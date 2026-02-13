# Delegation Backend: CQE バッチング双安定性の分析

## 概要

Delegation backend の daemon ループにおいて、CQE (Completion Queue Entry) のバッチ処理が
**双安定性** (bistability) を示すことを発見した。同一コードが NP (ノード数) に応じて
高スループット均衡と低スループット均衡のいずれかに収束し、一度低均衡に入ると自然には回復しない。

## 実験条件

- flush_endpoints() + Phase 1/2 interleave、poll_budget なし
- QD=32, server_threads=1, client_threads=46
- duration=10s, runs=1
- fern04 クラスタ (InfiniBand HDR100, Intel Xeon Gold 6530)
- copyrpc ring_size=4MB (auto-adjusted)

## 測定結果

### スループット

| NP | total Mops | /rank | outstanding avg | CQE batch avg | loops/10s |
|----|-----------|-------|----------------|---------------|-----------|
| 2  | 15.26     | 7.63  | --             | 4.6           | 8.6M      |
| 8  | **8.79**  | **1.10** | 1,343       | **0.97**      | **14.0M** |
| 16 | **110.04**| **6.88** | 833          | **28.0**      | **2.3M**  |

NP=8 は NP=16 の 1/6 のスループットしか出ていないが、outstanding (RDMA inflight calls) は
むしろ NP=8 の方が高い (1,343 vs 833)。**パイプラインは正しく充填されている**。
問題は CQE のバッチング効率にある。

### Outstanding 時系列 (QD sampling, interval=1)

#### NP=8 (低均衡に崩壊)

| 指標 | 値 |
|------|-----|
| 初回バースト時刻 | 45,334 us |
| 初回バースト inflight | 544 → 640 (数us で飽和) |
| ピーク inflight | **1,472** @ 104,544 us |
| 定常状態 inflight | ~1,370-1,470 |
| CQE batch 分布 | extra=0: **62.0%**, extra=1: 32.4%, extra=2-10: 5.6%, extra>100: **0.037%** |
| 定常状態 CQE batch avg | 0.58-0.72 |

#### NP=16 (高均衡を維持)

| 指標 | 値 |
|------|-----|
| 初回バースト時刻 | 61,435 us |
| 初回バースト inflight | 575 → 608 (数us で飽和) |
| ピーク inflight | **1,455** @ 164,353 us |
| 定常状態 inflight | ~1,390-1,420 |
| CQE batch 分布 | extra=0: **93.1%**, extra=1: 0.76%, extra=2-10: 0.67%, extra>100: **3.68%** |
| 定常状態 CQE batch avg | 107-200 |

## 双安定性のメカニズム

### Daemon ループの構造

```
loop {
    Step A: IPC poll (クライアントからリクエスト受信)
    Step B: Delegation ring poll (Flux 経由のローカル転送)
    Step C: ctx.poll() → CQE drain + callback (remote response 受信)
    Step D: Phase 1 — delegation リクエストをリモートに call()
    Step E: Phase 2 — call_backlog drain (RingFull リトライ)
    Step F: flush_endpoints() (RDMA WRITE 発行)
    Step G: backlog drain (reply_backlog, call_backlog)
}
```

ループ 1 回で処理する CQE 数を `B` (batch size)、ループ 1 回の所要時間を `T_loop` とする。

### 正のフィードバックループ

```
大きい B → 長い T_loop (多くのリクエスト処理) → RDMA RTT 中に CQE が蓄積 → 大きい B
```

```
小さい B → 短い T_loop → CQE 蓄積が不十分 → 小さい B
```

この正のフィードバックにより、システムは 2 つの安定均衡点を持つ。

### 臨界バッチ閾値

RDMA RTT を `T_rtt ≈ 6 us` とする。

- **高均衡**: `T_loop >> T_rtt` → ループ間に複数の CQE が到着 → `B >> 1`
- **低均衡**: `T_loop << T_rtt` → ループ間にほぼ CQE が到着しない → `B ≈ 0-1`

臨界点は `T_loop ≈ T_rtt` となるバッチサイズ。NP=16 の場合:
- 高均衡: T_loop ≈ 4.3 us (batch avg 28), loops/10s = 2.3M
- 最小安定 batch ≈ T_rtt / T_per_item ≈ 6 us / 0.16 us ≈ **37 items**

batch が 37 を下回ると T_loop < T_rtt となり、低均衡への崩壊が始まる。

### 崩壊の時間発展 (NP=8 の場合)

```
t=0        : 初期バースト。7 EP に同期的に 640 call を発行
t=6-8 us   : 最初の CQE 到着。全 EP が同時期に応答 → 大バッチ (640 CQE)
t=10-50 us : 大バッチ処理 → 大量の新 call 発行 → 再び全 EP が同期的に応答
  ...
t=200 us   : リモート daemon の処理時間がわずかにばらつく
             → CQE 到着タイミングが分散 (wave dispersion)
t=300 us   : batch が 100-200 に低下
t=400 us   : batch が 50-100 に低下
t=500 us   : batch が臨界閾値 (~37) を下回る
             → T_loop < T_rtt → CQE 蓄積不足 → 崩壊加速
t=1 ms     : batch ≈ 1、T_loop ≈ 0.7 us、低均衡に固定
```

### なぜ NP=8 は崩壊し NP=16 は安定するか

#### EP 数とタイミング多様性

| NP | リモート EP 数 | CQE 到着パターン |
|----|-------------|---------------|
| 8  | 7           | 少数 EP → バースト同期しやすい → パルス的 CQE 到着 |
| 16 | 15          | 多数 EP → 到着タイミングが自然にばらける → 連続的 CQE 到着 |

NP=8 (7 EP) の問題:

1. **初期同期**: 全 7 EP に同時に call → 全 EP がほぼ同時に応答
2. **バースト処理**: daemon は 1 ループで全応答を処理 → 全 EP に同時に再 call
3. **位相ロック**: call→response→call のサイクルが全 EP で同期
4. **Wave dispersion**: リモート daemon の処理時間差により、サイクルごとに位相が少しずつずれる
5. **分散による弱体化**: 数百 us で同期が崩れ、CQE が分散して到着
6. **臨界割れ**: 1 ループあたりの CQE が閾値を下回り、崩壊

NP=16 (15 EP) の場合:

1. **初期同期は同じ**: 全 15 EP に同時に call
2. **多様な応答時間**: EP 数が多いため、リモート daemon 間の処理時間差が大きい
3. **位相分散が初期から大きい**: 15 EP の応答が自然に分散
4. **連続的な CQE 流入**: どの時点でもいくつかの EP から CQE が到着
5. **T_loop が常に十分大きい**: batch > 臨界閾値 → 高均衡を維持

### CQE 分布の二極化

NP=8 と NP=16 の CQE batch 分布は質的に異なる:

**NP=8 (低均衡)**:
- extra=0 が 62%、extra=1 が 32% → **94% のループで CQE ≤ 1**
- extra>100 は 0.037% → 極めて稀な大バッチ (周期的な全 CQ ドレイン)
- ほぼ全てのループが空振りか 1 CQE 処理

**NP=16 (高均衡)**:
- extra=0 が 93% → 一見多いが、これはループが非常に速い (0.7 us/loop) ため
- extra>100 が **3.68%** → NP=8 の **100 倍** の頻度で大バッチが発生
- 大バッチの平均サイズは ~300-700 → 1 回で大量処理
- **スループットの大部分は少数の大バッチループで生産される**

## poll_budget によるジレンマ

| 設定 | NP=8 | NP=16 | 説明 |
|------|------|-------|------|
| poll_budget=16 | **58.4M** (7.31/rank) | 24.7M (1.55/rank) | CQ 蓄積時間を確保 → NP=8 安定、NP=16 は制限 |
| poll_budget なし | 8.79M (1.10/rank) | **110.0M** (6.88/rank) | ループ高速化 → NP=16 は EP diversity で安定、NP=8 は崩壊 |

poll_budget は CQE 蓄積のための「人工的な待ち時間」として機能する。

- **NP=8 + poll_budget=16**: ループに ~16 us の追加待ちが入り、T_loop > T_rtt を保証 → 崩壊回避
- **NP=16 + poll_budget=16**: EP diversity で自然に安定するのに不要な待ちが入る → スループット制限
- **NP=8 + poll_budget なし**: T_loop ≈ 0.7 us << T_rtt → 崩壊
- **NP=16 + poll_budget なし**: EP diversity で T_loop が自然に大きい → 安定

## 今後の方針

### 1. Adaptive poll_budget

CQE batch が閾値を下回ったら poll_budget を動的に挿入する:

```rust
if cqe_batch < THRESHOLD {
    poll_count += 1;
    if poll_count >= adaptive_budget {
        poll_count = 0;
        // 通常処理に進む
    } else {
        continue; // CQE 蓄積のため再 poll
    }
} else {
    poll_count = 0;
}
```

利点: NP=8 では自動的に budget が有効化され崩壊を防止、NP=16 では常にバッチが大きいため budget が無効化。

### 2. 初期バースト制御

崩壊は初期の同期バーストから始まる。初期 call を EP ごとに時間差をつけて発行すれば、
位相ロックを防止できる可能性がある。

### 3. Daemon マルチスレッド化

daemon を複数スレッドに分割し、各スレッドが担当 EP のサブセットを処理すれば、
1 スレッドあたりの EP 数が減り、ループが短くなるが、スレッド間の CQE 到着パターンが独立化する。
ただし、これは直接的にはバッチング崩壊を解決しない（スレッドあたりの EP 数がさらに少なくなるため）。

### 4. CQ event-driven アプローチ

busy-poll ではなく、CQ completion channel (eventfd) を使い、CQE が到着してから処理する方式。
CQE 蓄積の必要性を根本的に排除するが、イベント通知のレイテンシが追加される。

## 制御系としての実装案 (自己同定付き Adaptive Poll Budget)

### 状態・観測・制御入力

- 制御入力: `u = effective_poll_budget` (追加 poll 回数)
- 観測:
  - `b_k`: ループ k の CQE batch size
  - `e_k`: ループ k が空振り (`b_k == 0`) なら 1, それ以外 0
  - `t_k`: ループ時間 [us]
  - `rtt_k`: RTT 推定値 [us] (`call_ts` と completion から EWMA)
- 正規化指標:
  - `rho_k = t_k / rtt_k` (ループ時間 / RTT)

`rho_k < 1` が長く続くと CQE 蓄積が不足し、低均衡へ崩壊しやすい。

### 制御目標

- 一次目標: `rho_k` を `rho_low..rho_high` の範囲に維持 (`0.8..1.2` 目安)
- 二次目標: 空振り率 `E` を抑える (`E < 0.7` 目安)
- 最小化目標: 不要な待機 (`u`) を減らし、NP=16 など高多様性ケースで headroom を確保

### オンライン自己同定 (起動時 warmup)

1. `u=0` で短時間測定し、`B0, E0, R0` を得る
2. `u=u_probe` (例 8) で短時間測定し、`B1, E1` を得る
3. 感度推定:
   - `dB_du = (B1 - B0) / u_probe`
   - `dE_du = (E1 - E0) / u_probe`
4. ゲイン自動設定:
   - `step_up = clamp(round(2 / max(dB_du, eps)), 1, 8)`
   - `step_down = max(step_up * 2, 2)`
   - `u_max = clamp(round(2.5 * R0), 8, 64)`

これにより NP/QD ごとの手動調整を減らす。

### 疑似コード

```rust
struct AdaptiveBudgetCtl {
    // filtered metrics
    b_ema: f64,      // cqe batch EWMA
    e_ema: f64,      // empty-loop ratio EWMA
    rtt_ema_us: f64, // RTT EWMA
    loop_ema_us: f64,

    // control
    u: u32,          // effective poll budget
    u_max: u32,
    step_up: u32,
    step_down: u32,

    // hysteresis counters
    low_cnt: u32,
    high_cnt: u32,
    hold_cnt: u32,

    // thresholds
    rho_low: f64,    // e.g. 0.8
    rho_high: f64,   // e.g. 1.2
    e_high: f64,     // e.g. 0.75
    e_low: f64,      // e.g. 0.40
    low_need: u32,   // e.g. 64 loops
    high_need: u32,  // e.g. 128 loops
    hold_min: u32,   // e.g. 32 loops
}

fn daemon_loop(mut ctl: AdaptiveBudgetCtl) {
    loop {
        // Step A: CQ poll (u + 1 回まで追加 poll)
        let mut cqe = 0u32;
        for _ in 0..=ctl.u {
            cqe += ctx.poll_once();
            if cqe > 0 {
                break; // CQE が来たら通常処理へ
            }
        }

        // Step B..: existing pipeline
        process_deleg_requests();
        drain_call_backlog();
        ctx.flush_endpoints();
        process_flux_and_ipc();

        // Step C: update metrics
        let loop_us = last_loop_elapsed_us();
        let rtt_us = sample_rtt_ewma_us(); // if unavailable, keep previous
        ctl.b_ema = ewma(ctl.b_ema, cqe as f64, 0.05);
        ctl.e_ema = ewma(ctl.e_ema, if cqe == 0 { 1.0 } else { 0.0 }, 0.05);
        ctl.loop_ema_us = ewma(ctl.loop_ema_us, loop_us, 0.05);
        ctl.rtt_ema_us = ewma(ctl.rtt_ema_us, rtt_us, 0.02);
        let rho = ctl.loop_ema_us / ctl.rtt_ema_us.max(0.5);

        // Step D: hysteresis + rate-limited adaptation
        if ctl.hold_cnt > 0 {
            ctl.hold_cnt -= 1;
            continue;
        }

        let low_cond = rho < ctl.rho_low || ctl.e_ema > ctl.e_high;
        let high_cond = rho > ctl.rho_high && ctl.e_ema < ctl.e_low;

        if low_cond {
            ctl.low_cnt += 1;
            ctl.high_cnt = 0;
            if ctl.low_cnt >= ctl.low_need {
                ctl.u = (ctl.u + ctl.step_up).min(ctl.u_max);
                ctl.low_cnt = 0;
                ctl.hold_cnt = ctl.hold_min;
            }
        } else if high_cond {
            ctl.high_cnt += 1;
            ctl.low_cnt = 0;
            if ctl.high_cnt >= ctl.high_need {
                ctl.u = ctl.u.saturating_sub(ctl.step_down);
                ctl.high_cnt = 0;
                ctl.hold_cnt = ctl.hold_min;
            }
        } else {
            ctl.low_cnt = 0;
            ctl.high_cnt = 0;
        }

        // Step E: collapse recovery
        if ctl.b_ema < 1.0 && ctl.e_ema > 0.9 {
            ctl.u = (ctl.u + ctl.step_up * 2).min(ctl.u_max);
            ctl.hold_cnt = ctl.hold_min;
        }
    }
}
```

### 期待される挙動

- NP=8 崩壊ケース: `rho < 1`, `e_ema` 上昇を検知し `u` を自動増加、低均衡から回復
- NP=16 高均衡ケース: `rho > 1`, `e_ema` 低位なら `u` を自動縮小、不要待機を削減
- QD 変化時: warmup 同定と緩やかな再同定で追従し、固定 `poll_budget` より頑健

## 関連研究メモ (引用候補)

### 1. Batching / Livelock / Polling 安定化

1. Jeffrey C. Mogul, K. K. Ramakrishnan, "Eliminating Receive Livelock in an Interrupt-driven Kernel", USENIX ATC 1996.
   - 受信処理の崩壊と budget/制御導入による安定化という点で最も近い。
   - URL: https://www.usenix.org/conference/usenix-1996-annual-technical-conference/eliminating-receive-livelock-interrupt-driven

2. Adam Belay et al., "IX: A Protected Dataplane Operating System for High Throughput and Low Latency", OSDI 2014.
   - 低レイテンシと高スループットの両立に bounded batching を使う設計。
   - URL: https://www.usenix.org/conference/osdi14/technical-sessions/presentation/belay

3. Eunyoung Jeong et al., "mTCP: a Highly Scalable User-level TCP Stack for Multicore Systems", NSDI 2014.
   - user-space network stackにおける batch/event 集約の効果。
   - URL: https://www.usenix.org/conference/nsdi14/technical-sessions/presentation/jeong

4. Luigi Rizzo, "netmap: A Novel Framework for Fast Packet I/O", USENIX ATC 2012.
   - batching で固定コストを償却する設計原理の引用先として有用。
   - URL: https://www.usenix.org/conference/atc12/technical-sessions/presentation/rizzo

### 2. 制御理論ベースの通信最適化 (AQM / 遅延制御)

1. RFC 8289, "The CoDel Active Queue Management Algorithm", 2018.
   - しきい値・ヒステリシス型制御の実運用仕様。
   - URL: https://www.rfc-editor.org/rfc/rfc8289.html

2. RFC 8033, "PIE: A Lightweight Control Scheme to Address the Bufferbloat Problem", 2017.
   - PI制御を通信キュー制御へ適用した実装指向の標準文書。
   - URL: https://www.rfc-editor.org/rfc/rfc8033.html

3. C. V. Hollot et al., "On Designing Improved Controllers for AQM Routers Supporting TCP Flows", INFOCOM 2001.
   - AQMの古典的PI制御設計。
   - URL: https://dblp.org/rec/conf/infocom/HollotMTG01a.html

4. R. Pan et al., "PIE: A Lightweight Control Scheme to Address the Bufferbloat Problem", IEEE HPSR 2013.
   - PIEの元論文。
   - URL: https://dblp.org/rec/conf/hpsr/PanNPPSBV13.html

### 3. RDMA/Datacenter congestion control (遅延・フィードバック)

1. Radhika Mittal et al., "TIMELY: RTT-based Congestion Control for the Datacenter", SIGCOMM 2015.
   - RTTフィードバック制御。`rho = T_loop / RTT` 型指標の妥当性議論に使いやすい。
   - URL: https://research.google/pubs/timely-rtt-based-congestion-control-for-the-datacenter/

2. Yibo Zhu et al., "Congestion Control for Large-Scale RDMA Deployments (DCQCN)", SIGCOMM 2015.
   - RDMA向け制御則と大規模運用知見。
   - URL: https://www.microsoft.com/en-us/research/publication/congestion-control-for-large-scale-rdma-deployments/

3. Mohan Kumar et al., "Swift: Delay is Simple and Effective for Congestion Control in the Datacenter", SIGCOMM 2020.
   - 遅延目標に基づく実用制御の代表例。
   - URL: https://research.google/pubs/swift-delay-is-simple-and-effective-for-congestion-control-in-the-datacenter/

4. Anirudh Arun, Hari Balakrishnan, "Copa: Practical Delay-Based Congestion Control", NSDI 2018.
   - delayベース制御の理論と実装。
   - URL: https://www.usenix.org/conference/nsdi18/presentation/arun

5. Mo Dong et al., "PCC Vivace: Online-Learning Congestion Control", NSDI 2018.
   - オンライン最適化制御の比較対象として有用。
   - URL: https://www.usenix.org/conference/nsdi18/presentation/dong

6. Neal Cardwell et al., "BBR: Congestion-Based Congestion Control", ACM Queue 2016.
   - モデルベース状態推定 + 制御入力という枠組みの引用先。
   - URL: https://research.google/pubs/bbr-congestion-based-congestion-control-2/

### 4. 双安定/しきい値系の理論参照

1. Gagan Malhotra et al., "A feedback fluid queue with two congestion control thresholds", Mathematical Methods of Operations Research 2009.
   - 2しきい値フィードバック系の安定性解析。双安定記述の理論背景に使える。
   - URL: https://doi.org/10.1007/s00186-008-0235-8

### このメモでの使い分け案

- 「現象説明 (batching崩壊, livelock-like)」: Mogul+Ramakrishnan, IX, mTCP, netmap
- 「制御則の妥当性 (adaptive budget, hysteresis, PI/AIMD)」: CoDel, PIE, Hollot, Pan
- 「RTT/遅延を状態量として使う根拠」: TIMELY, Swift, Copa
- 「RDMA系文脈」: DCQCN
- 「双安定/閾値系の理論裏付け」: Malhotra et al.

## 実装済みの緩和策

### 1. Reply Batch-Hold (固定値、`--batch-hold-us`)

CQE 応答をバッファし、count ≥ NP またはタイムアウトで一括 flush。
adaptive toggle: バッチが健全 (flushed ≥ batch_target) なら hold を自動無効化。

**結果** (QD=1, fixed batch-hold=10μs vs no-hold):

| NP | fixed hold | no-hold | 改善 | copyrpc-direct | ucx-am |
|----|-----------|---------|------|--------|--------|
| 2  | 14.85M | 13.79M | +8% | 2.75M | 3.53M |
| 4  | 21.08M | 21.72M | -3% | 6.59M | 6.62M |
| 8  | 11.17M | 8.16M | **+37%** | 13.55M | 12.51M |
| 16 | 20.01M | 14.38M | **+39%** | 26.31M | 10.15M |
| 32 | 31.69M | 27.24M | +16% | 54.27M | 9.61M |

- NP=8 で delegation < ucx-am/direct: collapse 完全には解消されず
- NP≥16 で delegation > ucx-am (ucx-am がスケールしない)

### 2. 動的 Hold: RTT-based (`--hold-rtt-multiplier`, 失敗)

Step E flush → Step A first CQE の遅延を EWMA で計測し、hold_ns = rtt × multiplier。

**結果 (NP=8)**: m=1.0→8.72M, m=1.5→9.16M, m=2.0→9.45M (fixed 11.17M に大敗)

**失敗原因**: RTT (~3-5μs) は「1往復の遅延」を測る。batch collapse 解消に必要なのは
「N-1ノードの応答が揃うまでの時間」で、これは response arrival spread に依存し RTT より長い。
multiplier を上げれば追いつくが、それでは RTT 計測の意味がない。

### 3. 動的 Hold: Arrival-Rate Feedback (`--adaptive-hold`, 有望)

flush 時に直接観測可能な特徴量で hold_ns を自己調整:

```
rate = flushed_count / hold_duration   (responses/ns)
hold_ns = batch_target / rate_ewma     (直接計算)
```

EWMA 平滑化 (α=0.125): `rate_ewma = 0.875 * rate_ewma + 0.125 * sample`
clamp: 0.5μs ≤ hold ≤ 100μs。初期値は `--batch-hold-us`。

**自己修正メカニズム**:
- hold 短すぎ → flushed < target → rate 低 → hold 増加
- hold 十分 → flushed ≥ target → rate 高 → hold 減少 (無駄な待ち削減)
- NP=2,4 (健全) → rate 非常に高 → hold ≈ 0 (自動無効化)
- NP=8+ (崩壊) → rate 低 → hold 自動増加

**NP sweep 結果** (job 568288):

| NP | Adaptive | Fixed 10μs | Diff | No-hold |
|----|----------|-----------|------|---------|
| 2  | 14.73M   | 14.67M    | +0.4% | 13.76M |
| 4  | 20.91M   | 21.43M    | -2.5% | 21.87M |
| 8  | 11.11M   | 10.64M    | **+4.4%** | 8.18M |
| 16 | 19.49M   | 20.15M    | -3.3% | 14.71M |
| 32 | 38.93M   | 31.30M    | **+24.4%** | 27.08M |

- NP=32: arrival rate が高く、固定 10μs は長すぎる → adaptive が最適 hold を発見 (+24.4%)
- NP=8: modest improvement (+4.4%)、RTT方式 (-22%) と比べ大幅改善
- NP=2,4: ノイズ範囲内 (batch collapse なし)
- NP=16: -3.3% の微減、hold 調整が NP=16 の複雑な dynamics に追いつかない可能性

## 試行して失敗したアプローチ

1. **Adaptive poll_budget** (制御系としての実装): 上記「制御系としての実装案」参照。
   コードは実装したが adaptive_budget.rs モジュールごと削除。batch-hold の方が単純で効果的。

2. **Send-side batching**: flush_endpoints() を遅延して RDMA 送信をバッチ化。
   +1.6% (NP=8)。応答がすぐ返るため burst 維持不可。

3. **Recv coalescing** (`--coalesce-rtt-us`): ループ先頭で CQE を待つ。
   QD=1 のパイプラインが stall (因果結合)。

4. **IPC poll skip**: step_f をアイドル時スキップ。NP=2,4 で IPC starvation。

5. **RTT-based hold**: 上記参照。RTT は応答バラつきの proxy として不適切。
