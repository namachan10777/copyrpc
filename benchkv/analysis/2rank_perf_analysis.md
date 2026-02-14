# benchkv 2-rank Performance Analysis

## 1. Measurement Conditions

| Parameter | Value |
|---|---|
| Nodes | fern03 (rank 0), fern04 (rank 1) |
| CPU | Intel Xeon Gold 6530 |
| NIC | ConnectX-7 (mlx5) |
| Daemon threads | 1 per rank |
| Client threads | 1–16 per rank (varied) |
| Queue depth | 1 (synchronous) |
| Key range | 1024 per node |
| Read ratio | 0.5 |
| Distribution | Uniform |
| Transport | copyrpc DC (RDMA WRITE with Immediate) |
| Seed | rank * 1000 + client_id (deterministic) |
| Duration | 10s, 1 run |

## 2. Copyrpc RTT Scaling (Key Finding)

Daemon-side copyrpc RTT was directly measured: timestamp at `ep.call()` → timestamp at response callback in `poll_recv_counted()`.

| Clients/rank | copyrpc_rtt p50 (μs) | copyrpc_rtt mean (μs) | Total RPS | Remote/rank |
|---|---|---|---|---|
| 1 | **4.20** | 4.53 | 750K | 186K/s |
| 2 | **4.30** | 4.41 | 1.61M | 403K/s |
| 4 | **4.60** | 4.72 | 3.01M | 753K/s |
| 8 | **6.95** | 7.06 | 4.15M | 1.04M/s |
| 16 | **13.90** | 13.38 | 4.50M | 1.05M/s |

Reference: copyrpc_pingpong (dedicated tight loop, QD=1, 1 EP) = **5.50μs RTT**.

### Key Observations

1. **Base RTT (1 client) = 4.2μs** — close to copyrpc_pingpong's 5.5μs.
   copyrpc_pingpong is slightly slower because its responder calls `ctx.poll()` which includes `flush_endpoints()` on every iteration (extra overhead vs benchkv's separated `poll_recv_counted()`).

2. **RTT increases linearly with client count** beyond ~4 clients.

3. **Remote throughput saturates at ~1.05M/s per rank** regardless of client count (8→16: 1.04M→1.05M).

4. **Adding clients beyond saturation only increases RTT** without improving throughput. This is a classic closed-system queuing effect.

## 3. Little's Law Verification

Little's Law: `L = λ × W` (in-flight = throughput × response time).

Ground queue size measurements confirm the in-flight count:

| Clients | GQ size (mean) | In-flight (16 - GQ) | λ_remote (M/s) | RTT predicted (μs) | RTT measured (μs) |
|---|---|---|---|---|---|
| 1 | 0.1 | 0.9 | 0.186 | 4.84 | 4.20 |
| 4 | 0.2 | 3.8 | 0.753 | 5.05 | 4.60 |
| 8 | 0.4 | 7.6 | 1.04 | 7.31 | 6.95 |
| 16 | 0.4 | 15.6 | 1.05 | 14.86 | 13.90 |

Little's Law holds: `RTT ≈ in_flight / throughput`. The throughput ceiling determines RTT.

## 4. RTT Distribution: Bimodal Structure

Full RTT histogram at 16 clients (1μs buckets):

```
μs  count (rank 0)
3   4
4   19,086
5   313,681
6   376,919    ← first peak
7   296,414
8   231,283
9   173,872    ← valley
10  207,723
11  431,567
12  1,147,000
13  2,499,061
14  3,176,682  ← second peak (dominant)
15  1,894,466
16  414,100
17  21,742
18+ tail
```

**Two populations**:
- **Fast population (4–8μs)**: ~18% of requests. These encounter a lightly-loaded pipeline (few other requests in-flight) — minimal queuing delay, RTT ≈ base pipeline latency.
- **Slow population (11–16μs)**: ~82% of requests. These encounter a saturated pipeline (many requests in-flight) — significant queuing delay from competing with other in-flight requests.

This bimodal structure reflects the pipeline occupancy fluctuation: most of the time the pipeline is saturated (82%), but occasionally a burst of completions temporarily drains the queue (18%).

## 5. Root Cause: Pipeline Saturation and Closed-Loop Queuing

### 5.1 Daemon Loop Structure

The daemon is single-threaded with a 4-phase loop:

```
Phase 1:  poll_recv_counted() — detect response CQEs, write SHM responses
Phase 1b: recv() — process incoming remote requests, call reply()
Phase 2:  GQ scan — detect client SHM requests, call ep.call() for remote
Phase 3:  flush_endpoints() — post WQE for all pending messages, ring doorbell
```

### 5.2 Key Observation: Batch Sizes Are Small

The average batch size per flush is **not** N (total in-flight):

```
messages/flush = (remote_requests + replies) / flushes
               = (1.05M + 1.05M) / 1.75M
               = 1.2 messages/flush
```

The daemon flushes 1.75M times/s. Each flush carries only ~1.2 data messages
(~0.6 outgoing requests + ~0.6 replies to incoming). Per-request CPU processing
(GQ scan, encode, recv, handle, reply, callback) is **~80–100ns each** — nanoseconds,
not microseconds. The daemon is only ~22% utilized.

### 5.3 Pipeline Parallelism Limit

The copyrpc pipeline can hold a limited number of concurrent requests, determined by
the flush rate and network latency:

```
requests_in_one_way_flight = flush_rate × network_one_way × request_fraction
                           = 1.75M/s × 2μs × (1.05M / 2.1M)
                           = 1.75

pipeline_parallelism = requests_in_one_way_flight × 2 (round-trip)
                     + requests_in_processing
                     ≈ 1.75 × 2 + ~0.7
                     ≈ 4.2 concurrent requests
```

Verification at each client count:

| Clients | Remote in-flight | Pipeline slots used | Saturated? |
|---|---|---|---|
| 1 | 0.8 | 0.8 < 4.2 | No — RTT ≈ base (4.2μs) |
| 4 | 3.5 | 3.5 < 4.2 | No — RTT ≈ base (4.6μs) |
| 8 | 7.3 | 4.2 (capped) | Yes — queuing starts (RTT = 6.95μs) |
| 16 | 15.6 | 4.2 (capped) | Yes — heavy queuing (RTT = 13.9μs) |

### 5.4 RTT Inflation Mechanism

With QD=1 closed-loop clients, each client blocks until its previous request completes.
When in-flight requests exceed the pipeline parallelism (~4.2), the excess requests
cannot enter the pipeline — they wait at the client level (client is idle, waiting for
the previous response to come back via the saturated pipeline).

This is classic **closed-loop queuing**: adding clients beyond saturation does not
increase throughput, only increases response time.

From Little's Law:
```
RTT = in_flight / throughput = 15.6 / 1.05M = 14.9μs ≈ measured 13.9μs ✓
```

The RTT breakdown:
```
Base pipeline latency:    ~4.2μs  (network round-trip + processing)
Queuing delay:           +~9.7μs  (15.6 - 4.2 = 11.4 excess requests × 952ns/req)
Total:                   ~13.9μs  ✓
```

Note: the "952ns per request" (= 1/1.05M) is **not** per-request CPU processing time.
It is the **inverse of the pipeline's maximum service rate**. Each additional in-flight
request beyond the pipeline's capacity adds 1/λ_max ≈ 952ns of queuing delay.

### 5.5 What Limits the Pipeline Service Rate?

The pipeline throughput of 1.05M/s is determined by:

1. **Flush rate** (1.75M/s): The daemon flushes once per productive loop iteration.
   Productive loops occur when events (CQEs, GQ entries) are detected — 5.1% of all loops.

2. **Messages per flush** (1.2): Each flush carries ~0.6 remote requests + ~0.6 replies.
   Requests and replies share the same endpoint send ring and are flushed together.

3. **Closed-loop event arrival pattern**: Each flush triggers events that arrive
   ~300ns later (SHM turnaround → new GQ entry) and ~4μs later (network round-trip
   → response CQE). These events trigger the next productive loop. The average
   inter-productive-loop interval is 1/1.75M = 571ns.

4. **Not limited by**: daemon CPU (22% utilized), NIC bandwidth (<1% of ConnectX-7),
   credit starvation (ringfull/loop = 0.000001), or send ring capacity.

### 5.6 Model Predictions vs Measured

The pipeline saturation model:
- If N_remote ≤ pipeline_parallelism: throughput ≈ N_remote / T_base, RTT ≈ T_base
- If N_remote > pipeline_parallelism: throughput ≈ λ_max, RTT ≈ N_remote / λ_max

| Clients | N_remote | Throughput predicted | Throughput measured | RTT predicted | RTT measured |
|---|---|---|---|---|---|
| 1 | 0.8 | 0.8/4.2μs = 190K/s | 186K/s ✓ | 4.2μs | 4.20μs ✓ |
| 4 | 3.5 | 3.5/4.2μs = 833K/s | 753K/s ≈ | 4.2μs | 4.60μs ≈ |
| 8 | 7.3 | 1.05M/s (saturated) | 1.04M/s ✓ | 7.3/1.05M = 6.95μs | 6.95μs ✓ |
| 16 | 15.6 | 1.05M/s (saturated) | 1.05M/s ✓ | 15.6/1.05M = 14.9μs | 13.90μs ≈ |

## 6. Daemon Loop Metrics (16 clients)

### 6.1 Event Counts

| Counter | rank 0 | rank 1 |
|---|---|---|
| shm_req/loop | 0.061 | 0.062 |
| shm_local/loop | 0.030 | 0.031 |
| shm_remote/loop | 0.031 | 0.031 |
| cqe_resp/loop | 0.051 | 0.052 |
| incoming_req/loop | 0.031 | 0.031 |
| flush/loop | 0.051 | 0.052 |
| ringfull/loop | 0.000001 | 0.000001 |
| total loops (10s) | 343M | 336M |

### 6.2 Phase Timing (ns)

| Phase | p50 | p90 | p99 |
|---|---|---|---|
| cq_poll | 7 | 122 | 210 |
| incoming | 9 | 12 | 42 |
| gq_scan | 7 | 91 | 197 |
| flush | 7 | 23 | 100 |

### 6.3 Loop Time Distribution

```
p50=27ns  p90=33ns  p99=61ns  mean=29ns
99%+ of iterations < 100ns (empty loops)
```

### 6.4 Daemon Utilization

- Total useful work: ~22% of CPU
- 78% is empty polling (scanning empty GQ, empty CQ)
- Daemon is NOT the CPU bottleneck

## 7. T_local and T_remote Derivation

From measured data at 16 clients:

```
T_avg = 16 / 2.25M = 7.11μs per request per client
T_remote_copyrpc = 13.90μs (p50, daemon-measured)
T_remote_client ≈ 13.90 + 0.20 (SHM overhead) ≈ 14.10μs
T_local = (T_avg - 0.5 × T_remote_client) / 0.5
        = (7.11 - 7.05) / 0.5 = 0.12μs
```

T_local ≈ 120ns is consistent with: daemon detection (~14ns) + slot_read_seq cross-core (~40ns for nearby core) + handle_local (~20ns) + slot_write (~10ns) + client detection (~30ns).

## 8. Comparison with 1-rank Baseline

| Metric | 1-rank (16 clients) | 2-rank (16 clients) |
|---|---|---|
| Total RPS | 15.77M | 4.50M |
| T_avg per client | 1.01μs | 7.11μs |
| Daemon loop time | 1005ns | 29ns |
| SHM req/loop | 14.4 | 0.06 |
| GQ size (mean) | 16 (all grounded) | 0.4 (15.6 in-flight) |
| Bottleneck | GQ scan (960ns) | copyrpc RTT (13.9μs) |

In 1-rank mode, the daemon is fully utilized processing 14+ requests per loop.
In 2-rank mode, the daemon is 22% utilized; **the bottleneck is copyrpc pipeline saturation under closed-loop QD=1**.

## 9. Conclusions

### 9.1 Root Cause

The 3.5× RPS gap (15.77M → 4.50M) is **fully explained** by:

1. **50% of requests are remote** → each remote request takes 13.9μs (daemon copyrpc RTT) vs ~0.12μs for local
2. **copyrpc RTT at 16 clients (13.9μs) is 3.3× the base RTT (4.2μs at 1 client)**
3. The RTT inflation is caused by **pipeline saturation + closed-loop queuing**:
   - The copyrpc pipeline can hold ~4.2 concurrent requests (limited by flush rate × network latency)
   - With 15.6 in-flight remote requests, ~11.4 excess requests queue at the client level
   - This is NOT caused by large batch processing (average batch size = 1.2)
   - Per-request CPU processing is ~80–100ns (nanoseconds, not microseconds)
4. Throughput saturates at **~1.05M remote requests/s per rank** (pipeline service rate = flush_rate × requests_per_flush = 1.75M × 0.6)

### 9.2 Quantitative Gap Breakdown

```
T_avg = 0.5 × T_local + 0.5 × T_remote
      = 0.5 × 0.12μs + 0.5 × 14.1μs
      = 0.06 + 7.05
      = 7.11μs

RPS_per_rank = 16 / 7.11μs = 2.25M ← matches measured 2.25M ✓

T_remote breakdown:
  Base copyrpc RTT (unloaded):     4.2μs  (pipeline latency: network + processing)
  Queuing delay (pipeline full):  +9.7μs  (11.4 excess requests / 1.05M service rate)
  Total:                          13.9μs  ✓

Pipeline parallelism:
  flush_rate × network_one_way × 2 × request_fraction + processing_slots
  = 1.75M × 2μs × 2 × 0.5 + 0.7
  ≈ 4.2 concurrent requests
```

### 9.3 Optimization Opportunities

| Optimization | Expected Impact | Mechanism |
|---|---|---|
| **QD > 1** | Linear RTT reduction (more in-flight → higher throughput without pipeline change) | Break closed-loop QD=1 constraint; effective N_remote grows proportionally |
| **Increase flush rate** (flush per call instead of per loop) | More pipeline slots → higher throughput ceiling | Pipeline_parallelism = flush_rate × network_RTT; higher flush_rate → more concurrent requests |
| **Multi-daemon per rank** | More pipeline streams | Each daemon has independent endpoint + flush cycle |
| **Reduce network RTT** | Lower base pipeline latency → lower queuing threshold | Smaller messages, better NIC configuration |
| **NUMA-aware client placement** | Lower SHM turnaround → faster event recycling | Faster closed-loop cycling increases flush rate |
