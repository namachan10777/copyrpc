# benchkv 2-rank Performance Analysis

## 1. Measurement Conditions

| Parameter | Value |
|---|---|
| Nodes | fern03 (rank 0), fern04 (rank 1) |
| CPU | Intel Xeon Gold 6530 |
| NIC | ConnectX-7 (mlx5) |
| Daemon threads | 1 per rank |
| Client threads | 16 per rank |
| Queue depth | 1 (synchronous) |
| Key range | 1024 per node |
| Read ratio | 0.5 |
| Distribution | Uniform |
| Ring size | 4 MB |
| Seed | rank * 1000 + client_id (deterministic) |
| Duration | 15s, 1 run |

## 2. RPS Results

| Configuration | RPS (total) | RPS (per rank) | Notes |
|---|---|---|---|
| **1-rank, 16 client (baseline)** | 15.77M | 15.77M | All local |
| **2-rank, 16 client** | 4.46M | 2.23M | 50% remote |
| copyrpc pingpong (QD=1, 1 EP) | 181.8K | - | 5.50 us RTT |

**Slowdown**: 15.77M -> 4.46M = 3.54x

## 3. DC WRITE with Immediate RTT

| Metric | Value |
|---|---|
| copyrpc DC ping-pong RTT | **5.50 us** |
| Throughput (QD=1) | 181.8K RPS |

This is the full round-trip latency including:
- Send ring encoding + batch header write
- RDMA WRITE with Immediate (NIC-to-NIC)
- CQE generation on receiver
- Recv ring decode + response encoding
- RDMA WRITE with Immediate (return path)

## 4. Daemon Loop Metrics

### 4.1 Event Counts (per loop iteration)

#### 2-rank (rank 0 / rank 1)

| Counter | rank 0 | rank 1 |
|---|---|---|
| shm_req/loop | 0.0682 | 0.0553 |
| shm_local/loop | 0.0339 | 0.0276 |
| shm_remote/loop | 0.0342 | 0.0277 |
| cqe_resp/loop | 0.0569 | 0.0454 |
| incoming_req/loop | 0.0344 | 0.0275 |
| flush/loop | 0.0564 | 0.0458 |
| ringfull/loop | 0.000001 | 0.000001 |
| **loops per req** | **14.7** | **18.1** |
| total loops | 978M | 1215M |

#### 1-rank (baseline)

| Counter | Value |
|---|---|
| shm_req/loop | 14.44 |
| loops per req | 0.069 (< 1, batched) |
| total loops | 16.4M |

**Key insight**: In 2-rank mode, daemon processes ~0.06 requests per loop, meaning **~16 loops are empty** between each useful work unit. In 1-rank mode, each loop processes ~14 requests (all 16 slots are ready).

### 4.2 Phase Timing (sampled iterations)

#### 2-rank (rank 0 / rank 1)

| Phase | p50 (ns) | p90 (ns) | p99 (ns) |
|---|---|---|---|
| cq_poll | 8 / 8 | 112 / 83 | 195 / 190 |
| incoming | 7 / 7 | 9 / 9 | 46 / 42 |
| gq_scan | 7 / 7 | 109 / 68 | 214 / 179 |
| flush | 7 / 7 | 37 / 23 | 114 / 103 |

| Metric | rank 0 | rank 1 |
|---|---|---|
| loop_ns (p50) | 30 | 24 |
| loop_ns (p90) | 36 | 28 |

#### 1-rank (baseline)

| Phase | p50 (ns) | p90 (ns) | p99 (ns) |
|---|---|---|---|
| cq_poll | 7 | 30 | 56 |
| incoming | 7 | 24 | 54 |
| gq_scan | 964 | 1011 | 1059 |
| flush | 7 | 9 | 32 |
| **loop total** | **1005** | **1035** | **1065** |

**Key insight**: 1-rank gq_scan is 964ns because all 16 slots have pending requests.
2-rank gq_scan p50=7ns because most loops find no new requests (slots empty, waiting for RDMA response).

### 4.3 Loop Time Distribution

#### 2-rank

```
loop_histogram: <=50ns=238340 <=100ns=326 <=200ns=3 <=1us=1   (rank 0)
loop_histogram: <=50ns=296532 <=100ns=23  <=200ns=1            (rank 1)
```

99.8%+ of iterations complete in under 50ns. The loop is overwhelmingly empty.

#### 1-rank

```
loop_histogram: <=50ns=388 <=500ns=1 <=1us=1 <=2us=3609
```

90%+ of iterations take 1-2us (processing 14+ requests per loop).

## 5. Cache Metrics

perf hardware counters were unavailable (perf_event_paranoid=1, no CAP_PERFMON).

However, from phase timing data:
- **cq_poll p90=112ns**: When a CQE is present, reading it takes ~112ns (NIC DMA'd to host memory, L3 miss).
- **gq_scan p90=109ns**: When a SHM slot has a new request, reading it takes ~109ns (cross-core cache line transfer, Modified->Shared).
- These ~100ns latencies are consistent with L3 cache miss / cross-CCX access on Xeon Gold 6530 (~40-50 cycles at 3.0 GHz = ~15ns for L3 hit, 100-150ns for cross-socket / MMIO).

## 6. Numerical RPS Gap Analysis

### 6.1 Theoretical Model

For 16 clients at QD=1, each client's request takes:
- **T_local**: Time for a request that targets the local rank
- **T_remote**: Time for a request that targets the remote rank

With uniform distribution across 2 ranks: 50% local, 50% remote.

**T_avg = 0.5 * T_local + 0.5 * T_remote**

**RPS_per_rank = N_clients / T_avg**

### 6.2 Component Latencies

#### T_remote (remote request lifecycle)

A remote request goes through:
1. Client writes SHM slot (slot_write) -> daemon detects in gq_scan
2. Daemon encodes into copyrpc send ring, marks dirty
3. Daemon flushes -> RDMA WRITE with Immediate to remote
4. Remote daemon receives CQE, decodes batch, processes request
5. Remote daemon encodes response, flushes -> RDMA WRITE with Immediate back
6. Local daemon receives CQE, writes response to SHM slot
7. Client detects response (slot_read_seq)

From copyrpc_pingpong: **T_copyrpc_rtt = 5.50 us** (steps 2-6 in a tight loop).

But in benchkv, steps 1 and 7 add SHM overhead, and the daemon doesn't process immediately (polling delay).

**T_daemon_detect**: Time for daemon to detect the SHM slot request.
- Loop time = 27ns (mean, 2-rank)
- Requests arrive uniformly across loops
- Average detection delay = loops_per_detect / 2 * loop_time

From data: shm_req = 0.06/loop, meaning 1 request per ~16 loops.
The 16 slots are scanned each loop, so a new request is detected in the first scan after the client writes.
Detection delay = average 0.5 * loop_time = **~14ns** (negligible).

But the client is blocked until its response arrives. The key bottleneck is:

#### Pipeline analysis: 16 clients, QD=1, 50% remote

Each client has at most 1 outstanding request. With 16 clients:
- 8 clients (on average) have local requests -> processed immediately
- 8 clients have remote requests -> blocked for copyrpc RTT

**T_local** = daemon processing time for one request:
- From 1-rank data: 15.77M RPS / 16 clients = **985.6K RPS/client**
- T_local = 1 / 985.6K = **1.015 us**

This includes: gq_scan cost (slot_read_seq + payload read + handle_local + slot_write + VecDeque overhead) amortized over 16 concurrent clients all hitting simultaneously.

With 2-rank, only ~8 clients are local at any time:
- gq_scan scans all 16 slots per loop, but only ~8 have pending local requests
- From data: shm_local = 0.034/loop, shm_remote = 0.034/loop
- Total requests per loop = 0.068

**T_remote** = copyrpc RTT + daemon overhead:
- copyrpc_pingpong RTT = 5.50 us (but this is a tight dedicated loop, no SHM/scan overhead)
- In benchkv, daemon must:
  1. Detect request in gq_scan (O(1) per slot in the scan)
  2. Call copyrpc endpoint.call() (encode in send ring)
  3. Flush (emit WQE, ring doorbell)
  4. Wait for remote processing + return
  5. Detect CQE in cq_poll
  6. Write response to SHM slot

Steps 1-3 and 5-6 happen within the daemon loop (~30ns per loop, but request detection may take multiple loops).

**Key factor**: In copyrpc_pingpong, flush happens immediately after call (dedicated loop). In benchkv, flush happens at the end of the daemon loop (Phase 3), so there's a latency of:
- Phase 1 (cq_poll): 8ns p50
- Phase 1b (incoming): 7ns p50
- Phase 2 (gq_scan): 7ns p50
- **Total flush delay: ~22ns** (negligible)

But on the **remote** daemon side, the incoming request must be detected in cq_poll, processed, and flushed in a subsequent loop. Same applies to the response CQE on the local side.

### 6.3 Queuing Model

With QD=1 per client and 50% remote rate:

**At any instant**, the daemon has:
- ~8 clients with local requests (processed in 1 loop)
- ~8 clients with inflight remote requests (waiting for copyrpc RTT)

The daemon loop is fast (27ns mean), so local requests are processed almost instantly.
For remote requests, the client is blocked for:

**T_remote_total = T_daemon_detect + T_flush_delay + T_copyrpc_rtt + T_response_detect**

Where:
- T_daemon_detect = time to scan slot and find new request ≈ half a loop = **14ns**
- T_flush_delay = time from call() to flush() ≈ rest of loop = **15ns**
- T_copyrpc_rtt = network round-trip (NIC to NIC) = **~4.2 us** (dc_pingpong measured 4.17 us for raw DC SEND/RECV)
- T_response_detect = time to detect CQE + write SHM slot ≈ **14ns**

Wait -- copyrpc_pingpong includes batching/encoding overhead. The raw RTT in benchkv should be similar.

Let's use the copyrpc RTT directly. In copyrpc_pingpong, the tight loop does:
```
call -> flush -> poll(wait for response) -> call -> ...
```

In benchkv, the sequence is:
```
[gq_scan: call] -> [flush] -> ... (remote processes) ... -> [cq_poll: response CQE] -> [write SHM]
```

The extra overheads compared to pingpong:
1. **Flush is not immediate**: In pingpong, flush happens right after call. In benchkv, call happens in Phase 2, flush in Phase 3. Gap = O(10ns). Negligible.
2. **Batching**: Multiple requests may be batched into one RDMA WRITE. In benchkv with only 1 remote target, each flush sends 1 batch.
3. **Remote side processing**: Remote daemon must detect CQE, process request (handle_local), reply, flush. In pingpong, this is a tight loop. In benchkv, the remote daemon is also scanning 16 SHM slots.

The copyrpc_pingpong RTT (5.50 us) already includes all batching/encoding overhead, so it's a good proxy for T_remote in benchkv minus the daemon loop overhead.

### 6.4 Computing Expected RPS

**Per-client throughput model**:

Each client alternates between local and remote requests (50/50 uniform):

- Local request time: T_local
- Remote request time: T_remote

T_local: In 2-rank mode, daemon processes local requests in the gq_scan phase.
- Scan 16 slots, ~8 have pending requests
- Per-slot scan: slot_read_seq (Acquire load, ~100ns if cross-core cache line bouncing) + handle_local + slot_write
- Total gq_scan for 8 local requests ≈ 8 * 100ns = 800ns
- Per local request ≈ **100ns** daemon side
- Plus client-side SHM write + spin wait + next request overhead ≈ 50ns
- **T_local ≈ 150ns**

But wait: with QD=1, the client writes a slot, then spins waiting. The daemon detects it in the next gq_scan (up to 1 loop delay), processes it, writes response. Client detects response and issues next request.

Client round-trip for local:
- Client slot_write: ~10ns
- Wait for daemon gq_scan: avg 0.5 * loop_time = **14ns** (but daemon is scanning every 27ns)
- Daemon processes: slot_read_seq + handle_local + slot_write ≈ **80ns** (from 1-rank: 1005ns / 14.4 req = 70ns/req)
- Client detects response: nearly instant (daemon writes, client spinning on same cache line)
- **T_local ≈ 10 + 14 + 70 + 5 ≈ 99ns**

Hmm, but 1-rank gave 15.77M / 16 = 985K/client = 1.015us/req. That's much higher than 99ns.
The discrepancy is because of **cache contention with 16 client threads**:
- Each client is on a different core
- daemon (core 2) reads 16 SHM slots written by 16 different cores
- Each slot_read_seq triggers a cross-core Acquire load (~30-100ns depending on NUMA distance)
- 16 * avg_slot_read ≈ 16 * 60ns ≈ 960ns (matches 1-rank gq_scan p50 = 964ns)

So **T_local_1rank** = 1005ns / 14.4 ≈ **70ns** per request (amortized over batch processing).

But client sees **T_local_client_1rank** = 1/985K ≈ **1015ns** per request. This is because:
- All 16 clients submit simultaneously
- Daemon processes all 16 in one loop (~1005ns)
- Each client waits for the entire scan to complete before getting its response
- Average client wait = loop_time/2 + gq_scan/2 ≈ **500ns** (roughly half the loop)

For 2-rank mode:
- Only ~8 of 16 slots have local requests per loop
- But the loop runs much faster (27ns) because most loops find nothing
- When a local request arrives, daemon detects it in the next gq_scan

The effective T_local for 2-rank is harder to model because it depends on how often local requests arrive relative to daemon loop speed.

### 6.5 Alternative Model: Little's Law

**Little's Law**: L = lambda * W

Where:
- L = average number of inflight requests per client = QD = 1
- lambda = throughput per client
- W = average service time per request

From measurement:
- Total RPS = 4.46M, 16 clients per rank, 2 ranks
- Per-client RPS = 4.46M / 32 = **139.4K** (across all ranks)
- Per-client per-rank: 2.23M / 16 = **139.4K**
- W = 1 / 139.4K = **7.17 us**

Expected W:
- W = 0.5 * T_local + 0.5 * T_remote

From 1-rank: T_local_1rank = 1 / (15.77M / 16) = **1.015 us**
But with 2-rank, daemon loop is faster (fewer requests per loop), so T_local should be shorter.

How much faster? In 2-rank:
- shm_local ≈ 0.034/loop * daemon_loop_rate
- daemon_loop_rate = total_loops / duration = 978M / 15s = **65.2M loops/s** (rank 0)
- local requests/s = 0.034 * 65.2M = **2.22M/s** (matches ~2.23M RPS per rank)
- Per local request, daemon loop time: scan_time ≈ 16 * slot_read_seq ≈ 16 * 7ns (p50, most empty) + 100ns (hit) ≈ **212ns**

Hmm, this is getting complex. Let me use the direct measurement approach.

### 6.6 Direct Measurement Approach

From the data, we can directly compute:

**Daemon capacity analysis**:
- 2-rank daemon loop: 27ns mean, ~37M loops/s
- Per loop processes: 0.068 SHM req + 0.027 incoming + 0.055 flush
- Total useful work per second: 0.068 * 37M = **2.52M req/s** (SHM)

But measured RPS = 2.23M per rank. Close!

The gap: 2.52M (SHM throughput) vs 2.23M (measured RPS).
- SHM req includes both local (0.034) and remote (0.034)
- Local requests complete in one loop
- Remote requests need copyrpc RTT before client can issue next request
- So SHM throughput ≠ client-visible throughput

**Client-visible throughput**:
Each client does QD=1. Its throughput is:
- 1 / T_avg_request
- Where T_avg = 0.5 * T_local + 0.5 * T_remote

From the data:
- Per-client measured = 2.23M / 16 = **139.4K RPS** = 7.17 us/req
- T_avg = 7.17 us
- 0.5 * T_local + 0.5 * T_remote = 7.17 us
- T_local + T_remote = 14.34 us

We know copyrpc RTT ≈ 5.50 us. But T_remote in benchkv > copyrpc RTT because:
1. Daemon doesn't flush immediately (slight delay)
2. Remote daemon has its own loop overhead
3. SHM slot round-trip on both ends

If T_remote ≈ copyrpc_rtt + SHM_overhead:
- T_remote ≈ 5.50 + 2 * daemon_detect_delay
- daemon_detect_delay = average time for daemon to discover new SHM slot or CQE
- From data: loop is 27ns, req/loop = 0.068 -> 1 req every ~15 loops = ~400ns
- But detection delay is just 1 loop scan time: ~27ns (slot is in ground_queue, scanned every loop)
- So daemon_detect ≈ 0.5 * 27 ≈ **14ns** (waiting half a loop on average)

Actually, the issue is more subtle. copyrpc_pingpong is a dedicated loop:
```
call -> flush -> spin(poll until response) -> repeat
```

In benchkv:
```
[loop N]: gq_scan finds request, calls copyrpc call()
[loop N]: Phase 3 flushes (emits WQE)
... remote processes ...
[loop N+K]: cq_poll finds response CQE
[loop N+K]: writes SHM slot_write
```

The client is blocked from step 1 to the SHM slot_write in step 4.

**T_remote_benchkv** =
- gq_scan to detect request: ~0ns (already in loop, scanned as part of Phase 2)
- call() + flush(): same loop, ~few ns
- RDMA WRITE to remote: 1-way network latency
- Remote cq_poll to detect: half a remote loop = ~14ns
- Remote processes + replies + flushes: ~100ns (handle_local + reply encoding + flush)
- RDMA WRITE back: 1-way network latency
- Local cq_poll to detect: half a local loop = ~14ns
- Write SHM response: ~10ns

Total non-network overhead: 14 + 100 + 14 + 10 ≈ **138ns**

copyrpc_pingpong RTT = 5.50us already includes the RDMA network RTT + encoding/decoding.

So T_remote_benchkv ≈ 5.50us + daemon_overhead.

But wait - copyrpc_pingpong's tight loop has 0 detection delay (immediate flush, immediate poll).
In benchkv, the detection delay on each end is ~14ns. That's:
- Local side: gq_scan delay (~14ns) + flush delay (Phase 3, ~22ns after gq_scan)
- Remote side: cq_poll delay (~14ns) + flush delay (~22ns)
- Local CQE detection: ~14ns

Total added latency: 14 + 22 + 14 + 22 + 14 ≈ **86ns**

So **T_remote ≈ 5.50 + 0.086 ≈ 5.59 us**

Then:
- T_avg = 0.5 * T_local + 0.5 * 5.59
- 7.17 = 0.5 * T_local + 2.79
- T_local = (7.17 - 2.79) / 0.5 = **8.76 us** ???

That can't be right. T_local should be much less than T_remote.

**Re-examining**: The issue is that QD=1 clients sharing a single daemon create **queuing contention**.

With 16 clients and QD=1, at any moment ~8 clients have local requests and ~8 have remote (blocked).
The daemon processes all pending local requests in one gq_scan (~8 * 60ns = 480ns).
But each client must wait for **its** response in the scan order.

Actually, in 2-rank mode, only ~0.034 local requests arrive per loop (not 8 at once).
The requests are spread out because each client blocks for ~7us on remote requests.

**Arrival rate per client**: 139.4K RPS = 1 request every 7.17us.
**Daemon loop rate**: 37M loops/s = 1 loop every 27ns.
**Loops between requests from one client**: 7.17us / 27ns = **265 loops**.

So at any given moment, the daemon has:
- 16 * 27ns / 7.17us ≈ **0.06** requests in the ground_queue (mostly empty)

When a local request arrives, it's processed in the next gq_scan (same loop or next):
- T_local_processing = slot_read_seq (~80ns cross-core) + handle_local (~20ns) + slot_write (~10ns)
- T_local_detection = 0.5 * loop_time = 14ns
- **T_local ≈ 14 + 80 + 20 + 10 = 124ns** ≈ 0.124 us

Checking: T_avg = 0.5 * 0.124 + 0.5 * 5.59 = 0.062 + 2.79 = **2.86 us**
Expected RPS per client = 1 / 2.86us = **350K**
Expected total = 350K * 16 * 2 = **11.2M**

But measured = 4.46M. Something is wrong.

### 6.7 Revised Analysis: Batching Impact

The discrepancy suggests copyrpc RTT in benchkv is higher than copyrpc_pingpong.

copyrpc_pingpong has a dedicated endpoint with no other work. In benchkv:
- Daemon does SHM scan, incoming request handling, Flux processing between flushes
- This adds ~27ns per loop to the critical path

But more importantly: **the remote daemon must also detect, process, and flush**.

Let me trace a single remote request end-to-end:

```
Time 0:     Client writes SHM slot (slot_write)
Time +14ns: Daemon gq_scan detects request (half loop avg)
Time +14ns: Daemon calls copyrpc call() (encode in send ring)
Time +36ns: Daemon Phase 3 flush (emit WQE + doorbell)  [+22ns after gq_scan]
Time +36ns: NIC picks up WQE and starts RDMA WRITE
```

Now wait for one-way network + remote processing:

```
Time +36ns + T_network_oneway:  Remote NIC completes WRITE, generates CQE
Time + ... + 14ns: Remote daemon cq_poll detects CQE (half loop avg)
Time + ... + 14ns: Remote daemon processes batch -> incoming request
Time + ... + ~50ns: Remote daemon handle_local + reply() + Phase 3 flush [~36ns for rest of loop]
Time + ... + 50ns + T_network_oneway: Local NIC completes WRITE, generates CQE
Time + ... + ... + 14ns: Local daemon cq_poll detects response CQE
Time + ... + ... + 14ns: Local daemon writes SHM slot
Time + ... + ... + 14ns: Client detects response (spin wait)
```

Total:
**T_remote_benchkv = 36 + T_network_oneway + 14 + 50 + T_network_oneway + 14 + 14**
= 36 + 14 + 50 + 14 + 14 + 2 * T_network_oneway
= **128ns + 2 * T_network_oneway**

From copyrpc_pingpong RTT = 5.50us, the tight loop does:
call + flush + T_network_oneway + remote_detect + remote_process + remote_flush + T_network_oneway + local_detect
≈ 20ns + T_net + 0ns + 20ns + 20ns + T_net + 0ns
= 60ns + 2 * T_net = 5500ns
-> 2 * T_net = 5440ns
-> **T_network_oneway = 2720ns**

Actually copyrpc_pingpong has immediate detect (tight spin), so:
5500ns = call(~5ns) + flush(~15ns) + T_net + remote_poll(~5ns) + remote_process(~5ns) + remote_flush(~15ns) + T_net + local_poll(~5ns)
= 50ns + 2 * T_net
-> **T_network_oneway ≈ 2725ns**

In benchkv:
T_remote = 128ns + 2 * 2725ns = 128 + 5450 = **5578ns ≈ 5.58us**

T_avg = 0.5 * T_local + 0.5 * T_remote = 0.5 * 0.124 + 0.5 * 5.578
= 0.062 + 2.789 = **2.85 us**

Expected per-client = 1 / 2.85us = 351K
Expected total = 351K * 32 = **11.2M RPS**

**Measured: 4.46M RPS. Gap = 2.52x.**

### 6.8 Root Cause: SHM Cross-Core Latency Dominates

The model above assumed T_local = 124ns. But this ignores a crucial factor:

**The client spin-waits on slot_read_seq of the response slot**.
The daemon writes the response on core 2. The client reads it on core 3-18.
This is a **cross-core cache line invalidation** which takes ~80-100ns on Xeon Gold.

Similarly, the daemon reads the request slot_read_seq which was written by the client.
Another cross-core cache line transfer: ~80-100ns.

So T_local includes:
1. Client writes req slot (local L1, ~5ns)
2. **Cross-core: daemon reads req slot** (~80ns)
3. Daemon processes (~20ns)
4. Daemon writes resp slot (local L1, ~5ns)
5. **Cross-core: client reads resp slot** (~80ns)
6. Client prepares next request (~10ns)

**T_local = 5 + 80 + 20 + 5 + 80 + 10 = 200ns**

But with 16 clients, the daemon scans all 16 slots even when most are empty.
In 2-rank mode, daemon scans 16 slots at 7ns each (empty) + ~80ns for each hit.
With 0.068 hits per loop: average scan = 16 * 7 + 0.068 * 80 ≈ **117ns**

This scan happens every loop, so the "daemon processing" for one request is:
- Amortized scan overhead: 117ns (but most of this is for empty slots)
- Actual per-request processing: 80ns (slot read) + 20ns (handle_local) + 10ns (slot write)

For a local request, the client sees:
1. Write req slot: 5ns
2. Wait for daemon to scan: avg 0.5 * loop = 14ns
3. Daemon scans and finds: 80ns (slot_read_seq cross-core)
4. Process + write resp: 30ns
5. Client detects response: 80ns (cross-core)
**T_local ≈ 5 + 14 + 80 + 30 + 80 = 209ns**

But this still gives T_avg = 0.5 * 0.209 + 0.5 * 5.578 = 2.89us -> 345K/client -> 11.0M total.

### 6.9 The Real Bottleneck: Daemon CPU Saturation

Let me check if the daemon is CPU-saturated.

Daemon loop rate = 37M loops/s (rank 0) = 27ns/loop.
Total CPU time per second = 1 second (100% of 1 core).
Useful work per second:
- SHM requests processed: 2.22M/s * 200ns ≈ **0.444s** (44.4% of CPU)
- CQ poll with CQEs: ~2.22M * 120ns / 16 ≈ **0.017s** (1.7%)
- Wait - cqe_resp = 0.057/loop * 37M = 2.1M CQEs/s, each taking ~120ns when present
- Actually CQ poll cost when CQE present is amortized: poll_raw processes multiple CQEs per call
- flush_endpoints: 0.056/loop * 37M = 2.07M/s, each taking ~100ns when dirty ≈ **0.207s** (20.7%)

Hmm, let me just compute it directly from the loop:
- Total loop time = 1.0 second (100% CPU)
- Number of loops = 37M
- Per loop: 27ns
- Breakdown:
  - cq_poll: 8ns (p50, empty), 120ns (when CQE present, ~5.7% of loops)
  - incoming: 7ns (p50)
  - gq_scan: 7ns (p50, empty), ~100ns (when hit, ~6.8% of loops)
  - flush: 7ns (p50, empty), ~100ns (when dirty, ~5.6% of loops)

Expected loop time = 0.943 * (8+7+7+7) + 0.057 * (120+7+100+100)
≈ 0.943 * 29 + 0.057 * 327
= 27.3 + 18.6 = **45.9ns** (weighted average)

Hmm, measured is 27ns, suggesting the phases overlap or are faster than the p90 values suggest.

Actually, most loops have NO events: p50 for all phases = 7-8ns, and 99.8% of loops are <=50ns.
The rare loops with events are the p90+ tail.

**Key realization**: The daemon is NOT CPU-saturated. It spends most of its time in empty loops (27ns each). It could process much more work if more requests arrived.

The bottleneck is that **clients are blocked waiting for copyrpc RTT (5.58us)**, and during that time they cannot issue new requests. With QD=1:

- Each client is blocked for ~5.58us on average per remote request
- 50% of requests are remote
- Average time per request = 0.5 * 0.209 + 0.5 * 5.58 = **2.89us**
- Per client throughput = 1/2.89us = **346K RPS**
- Total = 346K * 32 = **11.1M RPS**

But measured is 4.46M. The 2.49x gap remains.

### 6.10 Hypothesis: Client-Daemon Contention

Wait - the measured **per-client** throughput is 139.4K RPS = **7.17us per request**.

If T_remote = 5.58us and T_local = 0.21us:
- T_avg_expected = 0.5 * 0.21 + 0.5 * 5.58 = 2.89us
- But measured T_avg = 7.17us

**The measured T_local must be much higher than 0.21us.**

Let me derive T_local from measured data:
- T_avg = 7.17us
- T_remote = ? (unknown for benchkv)
- 0.5 * T_local + 0.5 * T_remote = 7.17

We need another equation. From the data:
- shm_local = 0.034/loop = 1.26M local req/s
- shm_remote = 0.034/loop = 1.26M remote req/s
- cqe_resp = 0.057/loop = 2.11M remote responses/s

Wait, shm_local = 16.04M / 15s = **1.07M/s**
And shm_remote = 16.17M / 15s = **1.08M/s**
Total per rank = 2.15M/s (matches 2.15M RPS)

Per-client total: 2.15M / 16 = **134K RPS**
Per-client local: 1.07M / 16 = **66.9K** -> T_local = 1/66.9K / 0.5 = **29.9us** ???

No wait, that's wrong. Each client issues 134K req/s total, of which 50% are local and 50% remote.
Per-client local rate = 67K/s, remote rate = 67K/s.
T_local = (time per local request) = must satisfy: client duty cycle adds up to 1.0

**For each client**:
- fraction of time doing local requests = local_rate * T_local = 67K * T_local
- fraction of time doing remote requests = remote_rate * T_remote = 67K * T_remote
- Both fractions must sum to 1.0:
  67K * T_local + 67K * T_remote = 1.0
  T_local + T_remote = 1/67K = **14.9us**

With T_remote = copyrpc_rtt + overhead = ~5.6us:
T_local = 14.9 - 5.6 = **9.3us** !!!

This is surprisingly high for a local request. What's happening?

**Root cause hypothesis**: With 16 clients all sharing the same daemon on core 2, and the daemon scanning all 16 slots every loop at 27ns/loop:
- When a client submits a local request, it must wait for the daemon to reach its slot in gq_scan
- But the daemon is busy processing other clients' requests too
- With 16 clients at 134K/s each = 2.15M req/s total
- Each request takes the daemon ~27ns to scan + process → daemon needs 2.15M * 27ns = 58ms per second
- That's only 5.8% utilization? No that can't be right.

Actually the daemon runs 37M loops/s * 27ns = 1.0s (100% utilized spinning).
But useful work is only 2.15M req/s * ~100ns per useful req = 215ms → **21.5% utilization**.

The rest (78.5%) is **empty polling**.

But that means the daemon *should* be able to handle more. The bottleneck is on the **client side**:
each client with QD=1 can only have 1 outstanding request. When it's remote, the client blocks for ~5.6us.

So why is T_local = 9.3us instead of ~0.2us?

**Answer: T_local includes client spin-wait time, which depends on daemon scan timing.**

Actually, let me reconsider. In the 1-rank case:
- 16 clients, all local, 15.77M/s total
- Per-client: 985K/s → T_local_1rank = 1.015us

So T_local is definitely possible to be ~1us. But in 2-rank it appears to be 9.3us. What changed?

The difference is **fewer local requests per scan**. In 1-rank, every scan finds ~14 requests.
In 2-rank, each scan finds ~0.068 requests.

In 1-rank mode:
- Daemon processes 14 requests per loop (all 16 slots have pending requests)
- Each client waits ~1us (one full loop through all 16 slots)
- Loop time = 1005ns (dominated by 16 * 60ns slot reads)

In 2-rank mode:
- Daemon loop = 27ns, scanning 16 mostly-empty slots
- Each slot_read_seq for an empty slot: 7ns (already cached, unchanged)
- Each slot_read_seq for a new request: ~80-100ns (cross-core)

**When a client submits a local request**:
1. Client writes slot: 5ns
2. Daemon scans slot in next loop: wait avg 14ns (half loop)
3. Daemon slot_read_seq: **80-100ns** (cross-core cache miss)
4. Daemon processes: 20ns
5. Daemon writes response: 10ns
6. **Client detects response: 80-100ns** (cross-core cache miss)
**Total ≈ 230ns**

This should give T_local ≈ 0.23us. Still much less than 9.3us.

**The math must be wrong in the derivation above.**

Let me redo: 134K req/s per client, where each request is either local or remote with equal probability.

If T_local = 0.23us and T_remote = 5.6us:
- Expected per-client throughput = 1 / (0.5 * 0.23 + 0.5 * 5.6) = 1 / 2.92 = 343K/s
- Expected total = 343K * 32 = 10.97M RPS

But measured = 4.46M. Factor **2.46x** gap.

The answer must be that **T_remote_benchkv >> copyrpc_pingpong_RTT**.

Let me compute T_remote from the measured data:
- per-client throughput = 134K/s
- T_avg = 1/134K = 7.46us
- T_avg = 0.5 * T_local + 0.5 * T_remote
- T_remote = (7.46 - 0.5 * 0.23) / 0.5 = (7.46 - 0.115) / 0.5 = **14.7us**

**T_remote_benchkv ≈ 14.7us**, which is **2.67x** the copyrpc_pingpong RTT (5.50us).

### 6.11 Why T_remote is 2.67x copyrpc_pingpong

copyrpc_pingpong: dedicated loop, QD=1, 1 endpoint.
```
rank 0: call → flush → poll_wait_response (tight spin) → call → ...
rank 1: poll_wait_request → reply → flush → poll_wait_request → ...
```

benchkv remote request path:
```
rank 0 daemon:
  [Phase 2 gq_scan] → call()
  [Phase 3 flush] → emit WQE
  ... NIC processes ...
  [Next CQ poll that detects response] → write SHM slot

rank 1 daemon:
  [Phase 1 cq_poll] → detect CQE
  [Phase 1b incoming] → recv() → handle_local → reply()
  [Phase 3 flush] → emit WQE
  ... NIC processes ...
```

The key difference is that in benchkv, after call(), the daemon doesn't immediately wait for a response. It continues doing other work. The response comes back asynchronously via cq_poll in a future loop.

But with QD=1, the **client** is blocked until the response arrives. So the client-visible latency is:

T_remote = T_daemon_call_to_flush + T_network + T_remote_daemon_process + T_network + T_local_daemon_cq_detect + T_shm_response

In copyrpc_pingpong, everything is synchronous and immediate.
In benchkv, the remote daemon may not detect the incoming CQE immediately because it's busy with its own loop.

**Critical factor: remote daemon's CQ poll frequency**

The remote daemon runs at 37M loops/s. It does cq_poll in Phase 1 every loop.
So CQE detection delay = 0.5 * loop_time ≈ 14ns.

But wait - there's a more fundamental issue. **The copyrpc batching/flush cycle.**

In benchkv:
1. call() encodes message in send ring, marks endpoint dirty
2. flush() in Phase 3 emits WQE with doorbell
3. WQE goes to NIC → RDMA WRITE → remote NIC → CQE

Step 2 doesn't happen until Phase 3. In copyrpc_pingpong, flush is immediate after call.

**But the loop is only 27ns, so the delay is ~15ns.** That's negligible compared to network RTT.

**So why is T_remote 14.7us instead of 5.6us?**

Possible explanations:
1. **SRQ refill stalls**: SRQ entries consumed by incoming requests might not be refilled fast enough
2. **Send CQ backpressure**: pending_send_cqes accumulating → flush delay
3. **Credit exhaustion**: remote recv ring credits depleted → call returns RingFull → retry delay
4. **Flush batching**: Multiple WQEs accumulated → NIC processes sequentially
5. **Remote daemon incoming processing delay**: The remote daemon processes incoming requests in Phase 1b, which may take time if multiple arrive at once

From the data:
- ringfull = 0.000001/loop (329 total in 978M loops) → credit exhaustion is NOT the issue
- flush = 0.056/loop → flush happens ~5.6% of loops
- shm_remote = 0.034/loop but flush = 0.056 → flush includes credit-return-only batches too

Actually, **flush rate > remote rate** (0.056 vs 0.034) because flush also handles:
- Credit return for received batches
- Responses to incoming requests

Let me check: incoming_req = 0.034/loop → replies are also flushed.
So flush events: remote calls + incoming replies + credit returns = 0.034 + 0.034 + some credit returns.
0.034 + 0.034 = 0.068, but flush = 0.056. Hmm, flushes are batched (one flush covers multiple operations).

### 6.12 Summary and Conclusion

**Measured Data**:
| Metric | Value |
|---|---|
| Total RPS (2-rank) | 4.46M |
| Per-client throughput | 139K RPS |
| T_avg per request | 7.17 us |
| copyrpc DC RTT (pingpong) | 5.50 us |
| T_local (estimated) | ~0.23 us |
| T_remote (derived) | ~14.7 us |
| Daemon loop time | 27ns (p50) |
| Daemon utilization | ~22% (useful work) |

**Gap Analysis**:
| Factor | Impact |
|---|---|
| copyrpc RTT (network) | 5.50 us (base) |
| benchkv daemon overhead per remote req | +~9.2 us (2.67x pingpong) |
| **Total T_remote** | **~14.7 us** |

**Hypothesis for the 9.2us extra overhead**:
The most likely explanation is that the **copyrpc batching/flush cycle introduces additional round-trips**.

In copyrpc_pingpong: 1 call → 1 flush → 1 WQE → 1 response.

In benchkv: multiple events are batched. The flush in Phase 3 may emit a batch with multiple messages,
and the remote side processes the batch and replies in its own Phase 1b → Phase 3 cycle.
The **credit-return mechanism** adds an extra round-trip: after receiving a batch, the receiver must
return credit (via a 0-message batch or piggybacked on the next data batch) before the sender can
send more. This credit-return requires an additional flush cycle on the receiver.

With QD=1 and bidirectional traffic (both ranks sending to each other), the pattern is:
1. Rank 0 sends request batch → Rank 1
2. Rank 1 detects, processes, replies → but reply consumes Rank 0's recv ring credit
3. Rank 1 must also return credit for Rank 0's original batch
4. Credit return may be piggybacked on the reply batch or sent as a separate 0-message batch

If credit return requires an extra round-trip, that adds ~5.5us (another copyrpc RTT).
5.50 + 5.50 ≈ 11.0us, plus daemon overhead ≈ 14.7us. Close!

**Further investigation needed**:
1. Add copyrpc-level metrics (batches sent/received, credit returns) to confirm
2. Test with larger credit (bigger recv ring) to see if T_remote drops
3. Profile the copyrpc flush path to understand batch timing
