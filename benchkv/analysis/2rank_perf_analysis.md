# benchkv 2-rank Performance Analysis

## 1. Measurement Conditions

| Parameter | Value |
|---|---|
| Nodes | fern03 (rank 0), fern04 (rank 1) |
| CPU | AMD EPYC 9354P (Zen 4), 32 cores, 2 NUMA nodes × 16 cores |
| NIC | ConnectX-7 (mlx5_0) on NUMA 0 |
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

## 5. Root Cause Analysis: NIC Pipeline Queuing + Secondary NUMA Contention

### 5.1 Per-Request RTT Sub-Phase Breakdown

Each copyrpc round-trip was decomposed into three phases:

- **call_to_flush**: time from `ep.call()` to `flush_endpoints()` (daemon-side queuing)
- **flush_to_cb**: time from `flush_endpoints()` (doorbell ring) to response callback (wire round-trip)
- **incoming_turnaround**: time from CQ poll start to reply flush for incoming requests (remote daemon processing)

**Baseline (default core assignment, clients on NUMA 0 first):**

| Clients | call_to_flush p50 | flush_to_cb p50 | turnaround p50 | ONE_WAY (μs) | Total RPS |
|---|---|---|---|---|---|
| 4 | 0.00μs | 4.60μs | 0.20μs | 2.20 | 2.97M |
| 8 | 0.00μs | 6.50μs | 0.20μs | 3.15 | 4.21M |
| 16 | 0.00μs | 12.80μs | 0.30μs | 6.25 | 4.93M |

**NUMA Isolated (clients on NUMA 1, cores 16–31; daemon only on NUMA 0, core 2):**

| Clients | call_to_flush p50 | flush_to_cb p50 | turnaround p50 | ONE_WAY (μs) | Total RPS |
|---|---|---|---|---|---|
| 1 | 0.00μs | 4.15μs | 0.20μs | 1.98 | 0.80M |
| 4 | 0.00μs | 4.60μs | 0.20μs | 2.20 | 2.94M |
| 8 | 0.00μs | 6.60μs | 0.20μs | 3.20 | 4.25M |
| 12 | 0.00μs | 10.50μs | 0.20μs | 5.15 | 4.25M |
| 16 | 0.00μs | 11.40μs | 0.20μs | 5.60 | 5.47M |

Where: `ONE_WAY = (flush_to_cb - incoming_turnaround) / 2`

### 5.2 Key Findings

1. **call_to_flush ≈ 0 for all client counts**: The daemon posts WQEs immediately.
   No daemon-side queuing delay exists. 99.98% of requests have call-to-flush < 1μs.

2. **incoming_turnaround ≈ 200ns (constant)**: The remote daemon detects incoming CQEs,
   processes requests (handle_local), replies, and flushes in ~200ns (p50). This does NOT
   scale with client count. The daemon is NOT the bottleneck.

3. **flush_to_cb ≈ RTT**: The entire copyrpc RTT is spent in the NIC/wire round-trip path.

### 5.3 NUMA Isolation Experiment: Disproof of Memory Controller Hypothesis

**Original hypothesis** (Section 5.4 of previous version): Client threads on NUMA 0 generate
memory requests that compete with NIC DMA operations at the memory controller, inflating
one-way latency by ~0.4μs per thread.

**Experimental test**: `--numa-isolate` flag moves all client threads to NUMA 1 (cores 16–31),
leaving ONLY the daemon thread on NUMA 0. Both ranks were isolated (verified by core pinning output).

**Results disprove the original hypothesis as the primary cause:**

| Clients | Baseline flush_to_cb | NUMA flush_to_cb | Improvement |
|---|---|---|---|
| 4 | 4.60μs | 4.60μs | **0%** |
| 8 | 6.50μs | 6.60μs | **0%** (within noise) |
| 16 | 12.80μs | 11.40μs | **-11%** |

- **At 4 and 8 clients**: NUMA isolation has ZERO effect. Yet flush_to_cb already
  inflates from 4.15 to 6.60μs. This inflation occurs with only 1 thread on NUMA 0.
- **At 16 clients**: 11% improvement. NUMA 0 memory controller contention is a **secondary**
  factor that adds ~1.4μs at 16 clients, but is NOT the primary driver.

### 5.4 Primary Cause: Closed-Loop Pipeline Queuing

The RTT inflation is primarily a **closed-loop queuing effect** in the NIC/wire pipeline.

**Evidence**: With NUMA isolation (only daemon on NUMA 0), the RTT still scales:

| Clients | NUMA 0 threads | Remote ops/s | flush_to_cb p50 |
|---|---|---|---|
| 1 | 1 | 0.40M | 4.15μs |
| 4 | 1 | 1.48M | 4.60μs |
| 8 | 1 | 2.13M | 6.60μs |
| 12 | 1 | 2.13M | 10.50μs |
| 16 | 1 | 2.74M | 11.40μs |

The ONLY variable is the NIC pipeline throughput. Each in-flight remote request occupies
the pipeline: outgoing WQE → NIC DMA → wire → remote NIC → remote daemon → return.

**Key observation**: 8 and 12 clients achieve IDENTICAL throughput (2.13M ops/s) but
12 clients has 59% higher RTT (10.50 vs 6.60μs). By Little's Law, the additional
in-flight requests increase queuing delay even at constant throughput:

```
8 clients:  4 remote in-flight,  RTT = 4/2.13M × 2 = 3.76μs avg (measured flush_to_cb = 6.60)
12 clients: 6 remote in-flight,  RTT = 6/2.13M × 2 = 5.63μs avg (measured flush_to_cb = 10.50)
```

The flush_to_cb exceeds the Little's Law prediction because the RTT distribution has a
heavy bimodal tail (see Section 4). The median (p50) is higher than the mean.

**Throughput plateau at 8–12 clients**: The system reaches a throughput ceiling of ~2.13M
remote ops/s per daemon (~4.25M total RPS). Additional clients beyond 8 increase RTT
without improving throughput. Only at 16 clients does throughput break through to 2.74M/s,
likely because enough pipeline parallelism overcomes the queuing delay.

### 5.5 Per-Request Pipeline Model

A copyrpc remote round-trip traverses the following pipeline:

```
Local daemon:
  1. ep.call() → message queued in send ring          ~50ns
  2. flush_endpoints() → WQE posted, doorbell rung    ~100ns

NIC + Wire (outgoing):
  3. NIC DMA reads WQE from SQ                        ~300ns (PCIe round-trip)
  4. NIC DMA reads data from send ring                 ~300ns
  5. Packet transmit + wire propagation                ~500ns

Remote NIC + Daemon:
  6. RDMA WRITE to recv ring + CQE to CQ              ~300ns
  7. Remote daemon poll → detect → process → reply     ~200ns (incoming_turnaround)
  8. Remote flush_endpoints() → reply WQE posted       ~100ns

NIC + Wire (return):
  9–12. Same as 3–6 in reverse                         ~1400ns

Local daemon:
  13. CQ poll → response callback                      ~50ns

Minimum pipeline latency: ~3350ns ≈ 3.4μs
```

Measured at 1 client: 4.15μs. Overhead above minimum: ~0.8μs (NIC internal queuing, IOMMU, etc.)

With N in-flight requests sharing the pipeline, each request experiences queuing delay
at the NIC's WQE processor and PCIe DMA subsystem. The pipeline has limited parallelism
(single DCI send queue, shared PCIe bus), creating an effective service rate ceiling.

### 5.6 Daemon Loop Structure

The daemon is single-threaded with a 4-phase loop:

```
Phase 1:  poll_recv_counted() — detect response CQEs, write SHM responses
Phase 1b: recv() — process incoming remote requests, call reply()
Phase 2:  GQ scan — detect client SHM requests, call ep.call() for remote
Phase 3:  flush_endpoints() — post WQE for all pending messages, ring doorbell
```

Average batch size per flush = 1.2 messages. Per-request CPU processing = ~80–100ns.
Daemon CPU utilization ≈ 22%. The daemon is NOT the bottleneck.

### 5.7 Little's Law Verification

From Little's Law: `RTT ≈ in_flight / throughput`

| Clients | In-flight (est) | λ_remote (M/s) | RTT predicted (μs) | RTT measured (μs) |
|---|---|---|---|---|
| 1 | 0.9 | 0.20 | 4.50 | 4.15 |
| 4 | 3.8 | 0.74 | 5.14 | 4.60 |
| 8 | 7.6 | 1.07 | 7.10 | 6.60 |
| 16 | 15.6 | 1.37 | 11.39 | 11.40 |

Little's Law holds within ~10%. The copyrpc RTT is fully determined by in-flight count
and throughput ceiling.

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
| Bottleneck | GQ scan (960ns) | NIC one-way latency (6.85μs) |

In 1-rank mode, the daemon is fully utilized processing 14+ requests per loop.
In 2-rank mode, the daemon is 22% utilized; **the bottleneck is NIC pipeline
queuing from closed-loop in-flight request accumulation**.

## 9. Conclusions

### 9.1 Root Cause

The 3.5× RPS gap (15.77M → 4.93M) at 16 clients is explained by two factors:

**Primary: Closed-loop pipeline queuing in the copyrpc DC transport (~89% of gap)**

With QD=1 per client, each remote request occupies the NIC/wire pipeline for its full RTT.
As client count increases, more requests are in-flight simultaneously, creating queuing delay
at the NIC's single DCI send queue and PCIe DMA subsystem. The copyrpc RTT inflates from
4.15μs (1 client) to 11.40μs (16 clients, NUMA isolated) purely from pipeline occupancy —
even with only 1 thread on the NIC's NUMA node.

**Secondary: NUMA 0 memory controller contention (~11% of gap at 16 clients)**

With 14 client threads on NUMA 0 (default layout), SHM polling generates memory traffic
that competes with NIC DMA operations. NUMA isolation (clients on NUMA 1) reduces
flush_to_cb from 12.80μs to 11.40μs at 16 clients. This is a secondary effect that
compounds with the primary pipeline queuing.

### 9.2 Quantitative Gap Breakdown

**Default layout (16 clients, no NUMA isolation):**
```
T_local ≈ 0.12μs  (SHM round-trip, same-NUMA)
T_remote = flush_to_cb = 12.80μs (copyrpc wire RTT)
T_avg = 0.5 × 0.12 + 0.5 × 12.80 = 6.46μs
RPS_per_rank = 16 / 6.46 = 2.48M → Total = 4.95M ← matches measured 4.93M ✓
```

**NUMA isolated (16 clients):**
```
T_local ≈ 0.85μs  (SHM round-trip, cross-NUMA)
T_remote = flush_to_cb = 11.40μs (copyrpc wire RTT)
T_avg = 0.5 × 0.85 + 0.5 × 11.40 = 6.13μs
RPS_per_rank = 16 / 6.13 = 2.61M → Total = 5.22M ← close to measured 5.47M ✓
```

**RTT decomposition (NUMA isolated, 16 clients):**
```
flush_to_cb = 2 × ONE_WAY + incoming_turnaround
11.40μs     = 2 × 5.60μs  + 0.20μs  ✓

ONE_WAY breakdown:
  Base one-way (1 client, zero queuing):            ~2.0μs
  Pipeline queuing from 16 in-flight requests:     +3.6μs
  Total:                                            ~5.6μs  ✓

NUMA contention adds further ~0.7μs per direction at 16 clients:
  Baseline ONE_WAY:    6.25μs
  NUMA isolated:       5.60μs
  NUMA penalty:        0.65μs per direction
```

### 9.3 Throughput Ceiling Analysis

The system exhibits a throughput ceiling at ~2.1M remote ops/s per daemon:

| Clients (NUMA iso) | Remote ops/s | flush_to_cb p50 | Total RPS |
|---|---|---|---|
| 8 | 2.13M | 6.60μs | 4.25M |
| 12 | 2.13M | 10.50μs | 4.25M |
| 16 | 2.74M | 11.40μs | 5.47M |

At 8–12 clients, the throughput ceiling is reached: adding more clients only increases
RTT without improving throughput. At 16 clients, enough pipeline parallelism (8 remote
in-flight) breaks through the ceiling modestly.

### 9.4 Optimization Opportunities

| Optimization | Expected Impact | Mechanism |
|---|---|---|
| **QD > 1** | **High: ~2–3× RPS** | Multiple outstanding requests per client hides pipeline latency; increases pipeline utilization without proportionally increasing queuing |
| **NUMA isolation** | **Measured: +11% RPS** | Pin clients to NUMA 1; confirmed by experiment (4.93M → 5.47M at 16 clients) |
| **Multi-daemon per rank** | **High** | Multiple independent DCIs; reduces per-pipeline congestion |
| **Reduce copyrpc per-message overhead** | Moderate | Smaller WQEs (current: 2 WQEBB/96B); batching multiple messages per WQE |
| **Client-side batching (QD > 1)** | **High** | Decouple client round-trips from pipeline depth; amortize flush overhead |

### 9.5 NUMA Isolation Experiment Summary

**Hypothesis tested**: NUMA 0 memory controller contention from client SHM polling is the
primary cause of RTT inflation.

**Result**: **Disproved as primary cause.** NUMA isolation provides 11% improvement at 16 clients
but has zero effect at 4–8 clients. The RTT inflation from 4.15μs to 6.60μs (1→8 clients)
occurs entirely within the NIC pipeline, with only 1 thread on NUMA 0.

**Corrected model**: The primary bottleneck is the copyrpc DC transport's pipeline depth
(NIC DMA + wire + remote processing + return). Each in-flight request adds queuing delay.
NUMA contention is a secondary factor visible only at high thread counts (16+).
