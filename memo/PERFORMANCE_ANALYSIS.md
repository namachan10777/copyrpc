# copyrpc Performance Analysis

**Date**: 2026-02-08
**Machine**: fern04, Intel Xeon Gold 6530 (Sapphire Rapids), 32 cores, ConnectX-7 (NUMA 0)
**OFED**: MLNX_OFED_LINUX-24.10-1.1.4.0

---

## 1. Executive Summary

本ドキュメントでは copyrpc スタックの3層 (ipc, inproc, copyrpc) それぞれのマイクロベンチマーク結果、
perf stat (IPC) および perf c2c (HITM) によるハードウェアカウンタ分析、
理論限界の推定、そして2ノードベンチマーク (benchkv) での性能ボトルネックを包括的に分析する。

**主要な結論:**
- **ipc** (共有メモリ): pingpong 631ns/op (1.58M RPS), 16クライアント並列で depth=4 時 19.6M RPS
- **inproc** (スレッド間): FastForward が90.2 Mops/sで最速。Lamport 47.6 Mops/s、Onesided 31.4 Mops/s
- **copyrpc** (RDMA): 8ep×256QD で 28.8 Mops/s (34.7ns/op)。1ep×1QD pingpong で 3.76µs
- **benchkv 2ノード**: copyrpc Meta ~9.5M RPS vs UCX-AM ~20.4M RPS。差分はRDMAフラッシュ遅延が主因

---

## 2. Layer 1: ipc (共有メモリ IPC)

### 2.1 Benchmark Results

| Benchmark | Config | Latency | Throughput |
|-----------|--------|---------|------------|
| pingpong (SyncClient) | 1 client, RING_DEPTH=8 | 631 ns/op | 1.58 Melem/s |
| multi_client (SyncClient) | 16 clients, depth=1, RING_DEPTH=64 | 3.05 µs/iter | 5.25 Melem/s (per-client 328K) |
| multi_client (AsyncClient) | 16 clients, depth=4, RING_DEPTH=64 | 814 ns/iter | 19.6 Melem/s (per-client 1.23M) |

### 2.2 Perf Stat (IPC & Cache)

**pingpong (1 client, ~20s run):**

| Counter | Value | Rate |
|---------|-------|------|
| cycles | 63.7B | |
| instructions | 121.8B | **IPC = 1.91** |
| cache-references | 57.5M | |
| cache-misses | 2.18M | 3.80% |
| L1-dcache-loads | 30.7B | |
| L1-dcache-load-misses | 29.7M | **0.10%** |
| LLC-loads | 26.5M | |
| LLC-load-misses | 409K | **1.54%** |

**multi_client (16 clients, ~15s run):**

| Counter | Value | Rate |
|---------|-------|------|
| cycles | 576.0B | |
| instructions | 317.4B | **IPC = 0.55** |
| cache-references | 1.07B | |
| cache-misses | 520.5M | **48.5%** |
| L1-dcache-loads | 125.3B | |
| L1-dcache-load-misses | 519.0M | **0.41%** |
| LLC-loads | 521.9M | |
| LLC-load-misses | 255.6M | **49.0%** |

### 2.3 HITM Analysis (perf c2c)

**pingpong (1 client):**

| Metric | Value |
|--------|-------|
| Total records | 103,079 |
| Load Local HITM | **3,114** |
| Load Remote HITM | 20 |
| Load Fill Buffer Hit | 1,692 |
| Load L1D hit | 43,645 (86.7%) |
| Load LLC hit | 4,980 |
| Total Shared Cache Lines | 372 |
| Load access blocked by address | **31,525** |

**分析:**
- IPC=1.91 はシングルクライアントで良好。共有メモリの valid/slab_key フィールドへの競合が主な HITM 源
- **address-blocked が 31,525 で非常に高い**: サーバーとクライアントが同一キャッシュラインの valid バイトを頻繁に read/write している
- **Multi-client の IPC=0.55**: 16スレッドがサーバーの poll() で全クライアントリングをスキャンするため、L1 ミスが増大。LLC miss rate 49% は深刻

### 2.4 Theoretical Limits

ipc の理論限界は **共有メモリキャッシュラインの転送レイテンシ** で決まる:

- **L1→L1 (同一NUMA)**: ~20ns (HITM経由)
- **L1→L2**: ~10ns
- 1 RPC roundtrip = client write valid → server read valid → server write resp valid → client read resp valid
- **最低 4 cache-line transfers** = 4 × 20ns = **80ns** (理論下限)
- 実測 631ns は理論の **7.9倍**: poll() のリング全体スキャン、slab lookup、deferred response 処理のオーバーヘッド

**Multi-client スケーリング:**
- 16 clients × 4 depth = 64 inflight requests
- Server が全クライアントリングを毎 poll() でスキャンするため、O(max_clients × ring_depth) のキャッシュ汚染
- depth=4 での 19.6M RPS は **1クライアントの 12.4倍** (理論最大16倍)

---

## 3. Layer 2: inproc (スレッド間通信)

### 3.1 Benchmark Results (raw_spsc_bench, 5秒間)

| Transport | Throughput (Mops/s) | ns/op (implied) |
|-----------|-------------------|-----------------|
| **FastForward** | **90.18** | **11.1** |
| Lamport | 47.60 | 21.0 |
| Onesided | 31.39 | 31.9 |

**Flux 4-node all-to-all (criterion, mesh.rs, 768 calls):**

| Transport | Total Time | Throughput (Kelem/s) |
|-----------|-----------|---------------------|
| flux/onesided | 4.34 ms | 176.9 |
| flux/fastforward | 5.25 ms | 146.2 |
| flux/lamport | 4.88 ms | 157.3 |
| mesh/std_mpsc | 6.20 ms | 495.2 (notify) |
| mesh/std_mpsc (call_reply) | 4.30 ms | 178.7 |

### 3.2 Perf Stat (IPC & Cache)

| Transport | Cycles | Instructions | **IPC** | Cache-ref | Cache-miss | L1 miss% | LLC miss% |
|-----------|--------|-------------|---------|-----------|------------|----------|-----------|
| **FastForward** | 39.9B | 61.2B | **1.53** | 1.27B | 277K (0.02%) | 2.39% | 0.02% |
| Lamport | 39.8B | 97.6B | **2.46** | 259M | 282K (0.11%) | 0.08% | 0.40% |
| Onesided | 30.9B | 68.0B | **2.20** | 214M | 279K (0.13%) | 0.09% | 0.51% |

### 3.3 HITM Analysis (perf c2c)

**Onesided:**

| Metric | Value |
|--------|-------|
| Total records | 101,595 |
| Load Local HITM | 783 |
| Load Remote HITM | **962** |
| Load Fill Buffer Hit | 8,716 |
| Load L1D hit | 20,309 (50.7%) |
| Load LLC hit | 7,999 (20.0%) |
| LLC Misses to Remote DRAM | **74.0%** |
| LLC Misses to Remote cache (HITM) | **24.8%** |

**FastForward:**

| Metric | Value |
|--------|-------|
| Total records | 37,395 |
| Load Local HITM | **5,583** |
| Load Remote HITM | 2 |
| Load Fill Buffer Hit | 8,683 (46.7%) |
| Load L1D hit | 4,306 (23.2%) |
| Load LLC hit | 5,596 (30.1%) |
| Load access blocked by address | **9,405** |

**Lamport:**

| Metric | Value |
|--------|-------|
| Total records | **94** (PEBS しきい値以下) |
| Load Local HITM | 1 |
| Load Remote HITM | 0 |
| Load L1D hit | 29 (78.4%) |
| Shared Cache Lines | 1 |

### 3.4 Transport Architecture vs Performance 分析

#### FastForward (90.2 Mops/s, IPC=1.53)
- **最高スループット** だがIPC は最低。これは「少ない命令で多くのメモリアクセスを発行する」設計を反映
- **Local HITM = 5,583** が全 transport 中最大: flat combining パターンにより、クライアントが書いたリクエストスロットをサーバーが即座に読むため、同一 NUMA 内のキャッシュライン所有権移動が頻発
- **L1 miss = 2.39%** (Lamport の30倍): リングバッファの head/tail ポインタがクロスコア共有
- **Fill Buffer Hit 46.7%**: ストアバッファからの forwarding が多い = write-after-write パターンが高速
- キャッシュライン転送コストを「バッチ処理」で償却: 1回の poll() で複数スロットを drain → per-message コスト低減

#### Lamport (47.6 Mops/s, IPC=2.46)
- **IPC最高**: 命令パイプラインが最もよく埋まっている
- **HITM実質ゼロ**: generation counter ベースの同期により、write-read 競合が構造的に発生しない
- **PEBS サンプル 94件のみ**: メモリアクセスがほぼ L1 完結 (78.4% L1 hit)。ldlat=30 のしきい値を超えるアクセスがほぼ皆無
- **Instructions = 97.6B** (FastForward の 1.6倍): generation check のためのより多くの比較・分岐命令
- **ボトルネック**: 命令数の多さと sync() の epoch 同期コスト

#### Onesided (31.4 Mops/s, IPC=2.20)
- **Remote HITM = 962**: cross-NUMA アクセスが発生 (core 30,31 がNUMA境界をまたぐ可能性)
- **LLC Misses to Remote DRAM = 74.0%**: ワーストケースのメモリアクセスパターン
- **最低スループット**: CLDEMOTE 最適化の恩恵はあるが、根本的な cache-line bouncing が重い

### 3.5 Theoretical Limits

**SPSC リングバッファの理論限界:**

Sapphire Rapids の L1→L1 transfer (same NUMA):
- **Best case**: ~15-20ns per cache line transfer
- 1 message = 1 write + 1 read of shared slot = **最低 2 cache-line transfers**
- 理論限界: 1 / (2 × 15ns) = **33.3 Mops/s**

FastForward が 90.2 Mops/s を達成しているのは:
- **バッチング**: 1回の sync で複数メッセージを publish → per-message の cache-line transfer を分割
- **パイプライン**: inflight=256 により、cache miss penalty を複数メッセージで隠蔽
- 実効的な per-message cache cost: ~11.1ns (1 cache-line transfer 未満)

---

## 4. Layer 3: copyrpc (RDMA)

### 4.1 Benchmark Results (Single Node Loopback)

| Config | Latency | Throughput |
|--------|---------|------------|
| 1ep × 1qd (pingpong) | **3.76 µs** | 265.4 Kelem/s |
| 1ep × 32qd (pipelined) | 169.9 ns/op | **5.89 Melem/s** |
| 8ep × 256qd (pipelined) | 34.7 ns/op | **28.8 Melem/s** |

### 4.2 Perf Stat

**8ep × 256qd, ~31s run:**

| Counter | Value | Rate |
|---------|-------|------|
| cycles | 206.8B | |
| instructions | 925.5B | **IPC = 4.48** |
| cache-references | 1.03B | |
| cache-misses | 8.84M | **0.86%** |
| L1-dcache-loads | 305.9B | |
| L1-dcache-load-misses | 473.1M | **0.15%** |
| LLC-loads | 32.9M | |
| LLC-load-misses | 1.07M | **3.24%** |

### 4.3 Analysis

- **IPC = 4.48** は全レイヤー中最高: RDMA の操作 (WQE 構築、CQE パース) が整数演算主体でキャッシュフレンドリー
- **Cache miss rate 0.86%** は非常に低い: RDMA ハードウェアがデータ転送を担い、CPU はメタデータ操作のみ
- **8ep × 256qd = 28.8 Mops/s**: NIC の WQE 処理能力に近づいている

### 4.4 Comparison: copyrpc vs erpc

| Benchmark | copyrpc | erpc |
|-----------|---------|------|
| Pingpong latency (32B) | **3.76 µs** | 4.51 µs |
| Pipelined 1ep × 32qd | **5.89 Mops/s** | 2.67 Mops/s (depth=16) |
| Pipelined 8ep × 256qd | **28.8 Mops/s** | 6.09 Mops/s (8qp×depth32) |
| Multi-session pipelined | 28.8 Mops/s | 6.09 Mops/s (8 sessions) |

copyrpc は erpc の **4.7倍** のスループットを達成 (同一ノードループバック)。
要因: copyrpc の RDMA WRITE WITH IMM + SPSC リングバッファは、erpc の REQ/RESP UD メッセージングより効率的。

### 4.5 Raw RDMA Operations (mlx5)

| Operation | Latency | Throughput |
|-----------|---------|------------|
| 32B inline SEND (throughput) | 50.7 ns/op | 19.7 Melem/s |
| 32B BlueFlame SEND (lat+tput) | 129.1 ns/op | 7.75 Melem/s |
| 32B inline WRITE_IMM (pingpong) | 56.4 ns/op | 17.7 Melem/s |
| UD 32B SEND (throughput) | 49.9 ns/op | 20.0 Melem/s |
| RC latency (32B) | **2.97 µs** | 336.6 Kelem/s |
| UD latency (32B) | **3.05 µs** | 328.0 Kelem/s |
| 1MB WRITE (normal) | 52.8 µs | 18.5 GiB/s |
| 1MB WRITE (relaxed) | 47.4 µs | 20.6 GiB/s |
| 1MB READ (normal) | 47.4 µs | 20.6 GiB/s |
| 1MB READ (relaxed) | 47.3 µs | 20.7 GiB/s |

**RDMA 理論限界:**
- RC pingpong: 2.97 µs → copyrpc pingpong 3.76 µs → **RDMA overhead = 0.79 µs (21%)**
- WRITE_IMM throughput: 17.7 Mops/s → copyrpc 8ep 28.8 Mops/s → 8 endpoint の並列で NIC を十分活用
- 1ep × 32qd: 5.89 Mops/s vs WRITE_IMM 17.7 Mops/s → **33% の NIC 利用率**。リングバッファのメタデータ処理とポーリングサイクルがオーバーヘッド

---

## 5. Multi-Node Analysis: benchkv (2 nodes, 2 daemons, 8 clients)

### 5.1 Results

| Backend | Run 1 | Run 2 | Run 3 | Average |
|---------|-------|-------|-------|---------|
| copyrpc Meta | 10.2M | 9.1M | 9.1M | **~9.5M RPS** |
| UCX-AM | 20.3M | 20.4M | 20.4M | **~20.4M RPS** |

UCX-AM が copyrpc Meta の **2.1倍** 高速。

### 5.2 Bottleneck Analysis

#### A. RDMA フラッシュ遅延 (最大要因)

copyrpc daemon イベントループの5フェーズ:
```
Phase 1: ipc poll/recv (ローカルクライアント)
Phase 2: Flux poll (スレッド間デリゲーション)
Phase 3: Flux recv
Phase 4: copyrpc send/poll/recv (RDMA) ← ここでフラッシュ
Phase 5: Flux response drain
```

- copyrpc は WQE をバッチで蓄積し、**次の poll() サイクルでまとめてフラッシュ** (doorbell batching)
- reply → RDMA 送信の間に1ポーリングサイクル分の遅延が入る
- UCX-AM は `worker.progress()` を reply 直後に毎回呼び出し、即座にフラッシュ

**影響の定量化:**
- benchkv の daemon ループは 5 フェーズ × 各 O(clients × endpoints) のイテレーション
- 1 ループサイクル ≈ 1-5 µs (Phase 4 のRDMAポーリングが支配的)
- QD=4 × 8 clients = 32 inflight → フラッシュ遅延で 1-2 µs の追加レイテンシ

#### B. Consumer Position の RDMA READ

copyrpc のリングバッファフロー制御:
- 送信側がリング空き領域 < 25% になると RDMA READ で consumer position を取得
- READ latency ≈ 3 µs → この間送信ブロック
- UCX-AM はメッセージベースで、この種のフロー制御オーバーヘッドなし

#### C. ipc Server の poll() スキャンコスト

2 daemons × 4 clients/daemon = 各デーモンが4クライアントリングをスキャン:
- per-client ring スキャン: valid byte を cache-line 単位でチェック
- Multi-client perf stat の IPC=0.55 が示すように、スキャンは cache-miss bound

### 5.3 Theoretical Throughput Calculation

**Per-daemon 理論限界 (copyrpc Meta):**

1 RPC の critical path:
```
Client → ipc → Daemon → copyrpc RDMA → Remote Daemon → copyrpc RDMA → Daemon → ipc → Client
```

各区間の latency:
| Segment | Latency (µs) |
|---------|--------------|
| ipc client→server | 0.63 |
| copyrpc send (batched) | 0.17 (1/5.89M × 1ep) |
| RDMA wire latency | ~1.5 |
| Remote daemon recv→reply | 0.63 + 0.17 |
| RDMA wire back | ~1.5 |
| Daemon recv→ipc reply | 0.17 + 0.63 |
| **Total round-trip** | **~5.3 µs** |

With QD=4 pipelining:
- Per-client: QD / RT = 4 / 5.3µs = **0.75M RPS**
- 8 clients × 2 nodes = 16 total clients
- **理論 total: 16 × 0.75M = 12.0M RPS**
- 実測 9.5M = **理論の 79%** (合理的。daemon ループのバッチング非効率 + 2 daemon 間 Flux overhead)

**UCX-AM の優位性:**
- 即時フラッシュにより RDMA wire latency 後の「次ポーリングサイクル待ち」を排除
- 推定 RT 削減: ~1-2µs → 4.3µs RT → per-client 0.93M → total 14.9M (理論)
- 実測 20.4M > 理論 14.9M: UCX-AM の内部パイプライン最適化が追加寄与

---

## 6. Cross-Layer Performance Stack

### 6.1 Per-Operation Cost Breakdown

| Layer | Pingpong (ns) | Pipeline (ns/op, best) | Pipeline (Mops/s, best) |
|-------|---------------|----------------------|----------------------|
| **RDMA raw** (RC) | 2,970 | 50.7 (SEND inline) | 19.7 |
| **copyrpc** (8ep×256qd) | 3,760 | 34.7 | 28.8 |
| **inproc** (FastForward) | — | 11.1 | 90.2 |
| **ipc** (1 client) | 631 | — | 1.58 |
| **ipc** (16 clients, depth=4) | — | 51.0 | 19.6 |

### 6.2 Software Overhead Per Layer

| Overhead | Source | Cost |
|----------|--------|------|
| copyrpc vs raw RDMA (pingpong) | リングバッファメタデータ + CQ polling | +790 ns (27%) |
| copyrpc vs raw RDMA (pipelined) | WQE batching amortization | **-16 ns** (パイプラインで raw より高速) |
| ipc + inproc (benchkv path) | SHM poll + Flux dispatch | ~1-2 µs |
| RDMA flush delay (copyrpc) | Deferred doorbell | ~1-2 µs per cycle |

### 6.3 IPC (Instructions Per Cycle) Summary

| Component | IPC | Characterization |
|-----------|-----|-----------------|
| copyrpc (8ep×256qd) | **4.48** | Integer-heavy WQE/CQE processing, excellent ILP |
| inproc Lamport | **2.46** | Branch-heavy generation checks, good ILP |
| inproc Onesided | **2.20** | Moderate contention, some stalls |
| ipc pingpong | **1.91** | SHM cache-line sharing causes some stalls |
| inproc FastForward | **1.53** | Memory-bound (high cache transfer rate) |
| ipc multi_client | **0.55** | Severely memory-bound (16-way contention) |

### 6.4 HITM Summary

| Component | Local HITM | Remote HITM | LLC Miss Rate | Characterization |
|-----------|-----------|-------------|---------------|-----------------|
| inproc FastForward | **5,583** | 2 | 0.02% | Flat combining → intense local bouncing |
| ipc pingpong | **3,114** | 20 | 1.54% | SHM valid byte contention |
| inproc Onesided | 783 | **962** | 0.51% | Cross-NUMA traffic |
| inproc Lamport | 1 | 0 | 0.40% | **Virtually contention-free** |

---

## 7. Key Findings and Recommendations

### 7.1 ipc

**現状の強み:**
- Slab-based OoO completion により、マルチクライアント depth>1 で 3.7倍のスループット向上 (5.25→19.6 Melem/s)
- L1 miss rate 0.10% (pingpong) は非常に良好

**改善候補:**
1. **Server poll() の選択的スキャン**: 全クライアントリングを毎回フルスキャンではなく、アクティブクライアントのみスキャン (bitmap)
2. **Multi-client の LLC miss 49% 削減**: per-client control block のサイズ削減 or prefetch hint

### 7.2 inproc

**現状の強み:**
- FastForward 90.2 Mops/s は理論 SPSC 限界の2.7倍 (バッチングによる超過)
- Lamport の HITM-free 設計は multi-node Flux で活きる

**改善候補:**
1. Flux の 4-node all-to-all で FastForward が Lamport より遅い (146K vs 157K elem/s): Flux のバリア同期が FastForward のバッチング利点を打ち消している

### 7.3 copyrpc

**現状の強み:**
- IPC=4.48、erpc 比 4.7倍のスループット
- 8 endpoint パイプラインで NIC を十分に活用 (28.8 Mops/s)

**改善候補:**
1. **即時フラッシュモード**: benchkv のような low-latency ワークロードでは deferred flush を無効化するオプション。期待効果: +50-100% throughput
2. **Consumer position 通知最適化**: RDMA READ ではなく WRITE WITH IMM でアウトバンド通知
3. **Daemon ループ Phase 4 の最適化**: send と poll を interleave して、フラッシュ待ち時間を他の処理で埋める

### 7.4 benchkv Architecture

**2ノード構成の理論限界:**
- 各ノード: 8 clients × QD=4 = 32 inflight
- Per-node 理論: 6.0M RPS (copyrpc Meta), 実測 4.75M RPS/node
- **効率: 79%** — 許容範囲だが、RDMA フラッシュ遅延と ipc poll() オーバーヘッドが主要ロス

**UCX-AM との差分要因:**

| 要因 | 推定影響 |
|------|---------|
| RDMA flush 遅延 (deferred doorbell) | **40-50%** |
| Consumer position READ | **10-15%** |
| Daemon ループ多段構成 | **5-10%** |
| 間接参照 (QPN→endpoint→ring) | **5%** |

---

## Appendix A: Raw Data

### A.1 inproc raw_spsc_bench (5秒間, cores 30,31)

```
Onesided:    Sent=156,938,556  Throughput=31.39 Mops/s
FastForward: Sent=450,908,164  Throughput=90.18 Mops/s
Lamport:     Sent=238,028,800  Throughput=47.60 Mops/s
```

### A.2 copyrpc criterion (single-node loopback)

```
8ep_256qd_32B:  34.7 ns/op  (28.8 Mops/s)
1ep_32qd_32B:  169.9 ns/op  (5.89 Mops/s)
1ep_1qd_32B:    3.76 µs/op  (265 Kops/s)
```

### A.3 erpc criterion (single-node loopback)

```
pingpong/32B:           4.51 µs  (222 Kops/s)
pipelined/32B_depth8:   691 ns   (1.45 Mops/s)
pipelined/32B_depth16:  375 ns   (2.67 Mops/s)
pipelined/32B_depth64:  161 ns   (6.21 Mops/s)
pipelined/32B_depth256: 152 ns   (6.57 Mops/s)
8qp_depth32:            201 ns   (4.97 Mops/s)
8sessions_B8:           164 ns   (6.09 Mops/s)
```

### A.4 mlx5 raw RDMA ops

```
send_inline/32B:       50.7 ns  (19.7 Mops/s)
blueflame/32B:        129.1 ns  (7.75 Mops/s)
latency/32B (RC):       2.97 µs (337 Kops/s)
ud_send/32B:           49.9 ns  (20.0 Mops/s)
ud_latency/32B:         3.05 µs (328 Kops/s)
write_imm/32B:         56.4 ns  (17.7 Mops/s)
write_1m/normal:       52.8 µs  (18.5 GiB/s)
write_1m/relaxed:      47.4 µs  (20.6 GiB/s)
read_1m/normal:        47.4 µs  (20.6 GiB/s)
read_1m/relaxed:       47.3 µs  (20.7 GiB/s)
```

### A.5 benchkv 2-node (2 daemons, 8 clients, QD=4)

```
copyrpc Meta:  avg 9.5M RPS  (4.75M/node)
UCX-AM:        avg 20.4M RPS (10.2M/node)
Ratio:         UCX-AM / Meta = 2.15x
```
