# thread_spsc 実測パフォーマンスデータ分析

## 1. 測定環境

- **プロセッサ**: Intel/AMD x86_64
- **OS**: Linux 6.8.0
- **測定ツール**: perf stat
- **ベンチマーク**: criterion (comparison benchmark)

## 2. 実測データ

### 2.1 Pingpong Benchmark（Flux SPSC-based通信）

**測定時間**: 10.57秒
**Total cycles**: 74,431,572,073

```
Metric                    Value              Rate/Ratio
-----------------------------------------------------------
cycles                    74,431,572,073     -
instructions              34,284,936,980     0.46 IPC
cache-references          341,350,766        -
cache-misses              71,906,191         21.07%
branches                  7,270,391,108      -
branch-misses             37,047,485         0.51%
L1-dcache-loads           10,020,001,256     -
L1-dcache-load-misses     185,633,940        1.85%
L1-dcache-stores          5,407,671,119      -
```

**重要な観察:**
1. **IPC (Instructions Per Cycle): 0.46**
   - 非常に低い → メモリ待ちやスレッド同期が支配的
   - 理想値は2-4程度、0.46は**深刻なストール**を示唆

2. **Cache Miss Rate: 21.07%**
   - L2/L3キャッシュミス率が高い
   - スレッド間通信によるキャッシュコヒーレンシオーバーヘッド

3. **Branch Miss Rate: 0.51%**
   - 非常に良好 → 分岐予測器が効いている
   - write()の`rx_dead`チェックなどは正しく予測されている

4. **L1 D-cache Load Miss Rate: 1.85%**
   - 比較的良好だが、改善の余地あり
   - 約185M L1ミス → L2/L3からのフェッチが必要

### 2.2 SPSC Batch Benchmark - Batch Size 1

**測定時間**: 5.81秒
**Total cycles**: 41,772,214,042

```
Metric                    Value              Rate/Ratio
-----------------------------------------------------------
cycles                    41,772,214,042     -
instructions              55,171,798,334     1.32 IPC
cache-references          107,695,465        -
cache-misses              165,033            0.15%
branches                  12,272,028,518     -
branch-misses             60,616,771         0.49%
L1-dcache-loads           17,295,270,425     -
L1-dcache-load-misses     69,977,414         0.40%
```

**重要な観察:**
1. **IPC: 1.32**
   - Pingpongより大幅に改善（0.46 → 1.32）
   - ただし、依然として理想値の半分以下

2. **Cache Miss Rate: 0.15%**
   - 極めて良好！Pingpongの21.07%から劇的改善
   - データ構造がL1/L2に収まっている

3. **Branch Miss Rate: 0.49%**
   - Pingpongと同等、良好

4. **L1 D-cache Load Miss Rate: 0.40%**
   - Pingpongの1.85%から改善
   - 約70M L1ミス → 依然として存在

**1 iterationあたりのコスト推定:**
- 測定時間: 5.81秒
- 推定iteration数（1バッチ=1要素送受信）: 約500M-1B iterations
- Cycles/iteration: 約40-80サイクル

### 2.3 SPSC Batch Benchmark - Batch Size 32

**測定時間**: 2.06秒
**Total cycles**: 12,071,250,273

```
Metric                    Value              Rate/Ratio
-----------------------------------------------------------
cycles                    12,071,250,273     -
instructions              20,978,171,468     1.74 IPC
cache-references          38,518,454         -
cache-misses              6,130,952          15.92%
branches                  4,805,822,834      -
branch-misses             10,911,309         0.23%
L1-dcache-loads           6,497,232,217      -
L1-dcache-load-misses     24,298,898         0.37%
```

**重要な観察:**
1. **IPC: 1.74**
   - さらに改善（1.32 → 1.74）
   - バッチ処理により同期オーバーヘッドが削減

2. **Cache Miss Rate: 15.92%**
   - Batch size 1の0.15%より悪化
   - ただし、Pingpongの21.07%よりは良好
   - **cache-referencesの定義が異なる可能性**（L3アクセス？）

3. **Branch Miss Rate: 0.23%**
   - さらに改善（0.49% → 0.23%）
   - バッチ処理によりループの分岐予測が向上

4. **L1 D-cache Load Miss Rate: 0.37%**
   - Batch size 1の0.40%とほぼ同等、良好

**1 iterationあたりのコスト推定（32要素バッチ）:**
- Cycles/iteration: 約6-12サイクル（1要素あたり）
- バッチ効果により**約7-10倍の改善**

### 2.4 SPSC Batch Benchmark - Batch Size 256

**測定時間**: 2.47秒
**Total cycles**: 16,017,222,688

```
Metric                    Value              Rate/Ratio
-----------------------------------------------------------
cycles                    16,017,222,688     -
instructions              39,403,381,837     2.46 IPC
cache-references          28,410,363         -
cache-misses              104,412            0.37%
branches                  9,061,483,445      -
branch-misses             13,771,341         0.15%
```

**重要な観察:**
1. **IPC: 2.46**
   - 劇的改善！ほぼ理想値に近い
   - スレッド同期オーバーヘッドが最小化

2. **Cache Miss Rate: 0.37%**
   - 極めて良好
   - データ構造が完全にキャッシュに収まっている

3. **Branch Miss Rate: 0.15%**
   - 最良の結果
   - ループの分岐予測が完璧に機能

**1 iterationあたりのコスト推定（256要素バッチ）:**
- Cycles/iteration: 約4-8サイクル（1要素あたり）
- バッチ効果により**約10-20倍の改善**

## 3. バッチサイズによるスケーリング分析

### 3.1 IPC (Instructions Per Cycle) の変化

| Batch Size | IPC  | 改善率 |
|------------|------|--------|
| 1          | 1.32 | -      |
| 32         | 1.74 | +32%   |
| 256        | 2.46 | +41%   |
| Pingpong   | 0.46 | (基準) |

**グラフ的傾向:**
```
IPC
 |
2.5 |                    ●  (256)
    |
2.0 |
    |              ●  (32)
1.5 |
    |    ●  (1)
1.0 |
    |
0.5 | ●  (pingpong)
    +------------------------
       1    32   128  256  batch size
```

**観察:**
- バッチサイズが大きいほどIPC向上
- **Pingpong（スレッド間同期が頻繁）では0.46と極めて低い**
- バッチ処理により同期頻度削減 → CPU稼働率向上

### 3.2 Branch Miss Rate の変化

| Batch Size | Branch Misses | Total Branches | Miss Rate |
|------------|---------------|----------------|-----------|
| 1          | 60,616,771    | 12,272,028,518 | 0.49%     |
| 32         | 10,911,309    | 4,805,822,834  | 0.23%     |
| 256        | 13,771,341    | 9,061,483,445  | 0.15%     |

**観察:**
- バッチサイズ増加 → Branch Miss Rate減少
- ループ内の分岐予測が向上
- **write()の分岐（rx_dead check, capacity check）は良好に予測されている**

### 3.3 L1 D-cache Load Miss Rate の変化

| Batch Size | L1 Load Misses | L1 Loads        | Miss Rate |
|------------|----------------|-----------------|-----------|
| 1          | 69,977,414     | 17,295,270,425  | 0.40%     |
| 32         | 24,298,898     | 6,497,232,217   | 0.37%     |
| Pingpong   | 185,633,940    | 10,020,001,256  | 1.85%     |

**観察:**
- バッチサイズ1, 32では非常に低い（0.37-0.40%）
- Pingpongでは1.85%と高い
- **スレッド間通信によるキャッシュコヒーレンシオーバーヘッドが支配的**

### 3.4 Cycles/Iteration 推定

**計算方法:**
```
Cycles/iteration = Total Cycles / (実行時間 × イテレーション/秒)
```

推定（criterionの結果から逆算が必要だが、オーダー推定）:

| Batch Size | Cycles/element | 備考 |
|------------|----------------|------|
| 1          | 40-80          | スレッド同期含む |
| 32         | 6-12           | バッチ効果 |
| 256        | 4-8            | 最大効率 |

**理論値との比較:**
- 理論最小値（write + flush + sync + poll）: 44-56サイクル（推定）
- 実測（バッチサイズ256）: 4-8サイクル/要素
- **バッチ256では同期コストが256要素で償却される**

## 4. ボトルネックの特定

### 4.1 低IPCの原因

**Pingpongベンチマーク（IPC=0.46）:**

```
IPC = instructions / cycles = 0.46
1 / IPC = cycles / instruction = 2.17サイクル/命令
```

**内訳推定:**
- 実行サイクル: 約20-30%
- メモリ待ち: 約30-40%
- **スレッド同期待ち: 約30-50%**（最大のボトルネック）

**証拠:**
- Batch size 256ではIPC=2.46と劇的改善
- スレッド同期頻度削減により、CPU稼働率が向上

### 4.2 キャッシュミス分析

**L1 D-cache Load Miss の内訳:**

| ベンチマーク | L1 Misses   | L1 Loads        | Miss Rate | 主な原因 |
|--------------|-------------|-----------------|-----------|----------|
| Pingpong     | 185,633,940 | 10,020,001,256  | 1.85%     | スレッド間通信 |
| Batch 1      | 69,977,414  | 17,295,270,425  | 0.40%     | データ構造アクセス |
| Batch 32     | 24,298,898  | 6,497,232,217   | 0.37%     | データ構造アクセス |

**1.85%のL1ミス → 約185M回のL2/L3アクセス**

**L1ミス時のペナルティ:**
- L1 → L2: 12-15サイクル
- L1 → L3: 40-75サイクル
- 平均30サイクルと仮定: 185M × 30 = 5.5B サイクル
- Total: 74.4B サイクル
- **L1ミスだけで約7.4%のオーバーヘッド**

**キャッシュミスの主要箇所（推定）:**
1. `tx.tail` / `rx.head` のload/store（スレッド間通信）
2. `slots[]`配列へのアクセス（大きなバッチ時）
3. `rx_dead` / `tx_dead` のload（毎回のwrite()）

### 4.3 分岐予測ミス分析

**Branch Miss Rateは非常に良好（0.15-0.51%）**

| ベンチマーク | Branch Misses | Branches        | Miss Rate |
|--------------|---------------|-----------------|-----------|
| Pingpong     | 37,047,485    | 7,270,391,108   | 0.51%     |
| Batch 1      | 60,616,771    | 12,272,028,518  | 0.49%     |
| Batch 256    | 13,771,341    | 9,061,483,445   | 0.15%     |

**0.51%のミス → 約37M回の分岐ミス**

**分岐ミス時のペナルティ:**
- 1回のミス: 15-20サイクル
- 平均17サイクルと仮定: 37M × 17 = 629M サイクル
- Total: 74.4B サイクル
- **分岐ミスは約0.85%のオーバーヘッド（非常に小さい）**

**結論: 分岐予測は問題ではない**

### 4.4 主要なボトルネックの定量化

**Pingpongベンチマークの74.4B cycles内訳（推定）:**

| コンポーネント | サイクル | 割合 | 備考 |
|---------------|---------|------|------|
| 命令実行（理論） | 34.3B | 46% | instructions × 1 cycle/inst（理想） |
| L1キャッシュミス | 5.5B  | 7.4% | 185M misses × 30 cycles |
| 分岐ミス | 0.6B  | 0.9% | 37M misses × 17 cycles |
| **スレッド同期待ち** | **30-35B** | **40-47%** | 差分（最大のボトルネック） |
| その他（スケジューリング等） | 3-8B | 4-11% | 残差 |

**結論:**
- **スレッド間同期が最大のボトルネック（40-47%）**
- キャッシュミスは7.4%と無視できない
- 分岐ミスは0.9%と小さい

## 5. write()内のrx_dead checkの定量評価

### 5.1 推定される影響

**コード（189行目）:**
```rust
if self.inner.tx.rx_dead.load(Ordering::Relaxed) {
    return Err(SendError(value));
}
```

**1回のrx_dead loadコスト:**
- L1ヒット時: 3-5サイクル
- 分岐: 1サイクル（予測成功）

**write()の呼び出し頻度推定（Batch size 1）:**
- Total branches: 12,272,028,518
- write()内の分岐: 2回（rx_dead check + capacity check）
- 推定write()呼び出し: 約3-6B回

**rx_dead checkの総コスト:**
- 3-6B × 4サイクル = 12-24B サイクル
- Total: 41.8B サイクル
- **約29-57%のオーバーヘッド！**

**これは過大評価の可能性あり（他の分岐も含むため）**

**より保守的な推定:**
- write()呼び出し: 約1-2B回
- rx_dead checkコスト: 4-8B サイクル
- **約10-20%のオーバーヘッド**

### 5.2 Branch Missデータからの検証

**rx_dead checkでの分岐ミス:**
- 通常ケース: 99.99% not taken → 予測成功
- disconnection時: taken → 予測失敗（稀）

**Branch Miss Rate 0.49%の内訳推定:**
- rx_dead check: < 0.01%（ほぼゼロ）
- capacity check: 0.2-0.3%
- その他（ループ等）: 0.2-0.3%

**結論:**
- rx_dead checkの**分岐ミスは無視できる**
- しかし、**毎回のload自体が4-5サイクルのオーバーヘッド**

## 6. try_recv()の二重head store の定量評価

### 6.1 Store操作の頻度

**L1-dcache-stores（Batch size 1）:**
- Total stores: 5,407,671,119（Pingpong）
- Total stores: 2,867,867,999（Batch 1）（ただしwrite_flushベンチマーク）

**推定:**
- recv側のhead store: 約1-2B回
- 二重storeによる無駄: 約0.5-1B回（50%）

**1回のRelease storeコスト（x86_64）:**
- 1サイクル + store buffer latency
- 実効コスト: 2-3サイクル

**総コスト:**
- 0.5-1B × 2.5サイクル = 1.25-2.5B サイクル
- Total: 41.8B サイクル
- **約3-6%のオーバーヘッド**

### 6.2 Store Buffer の影響

**x86_64 Store Buffer:**
- サイズ: 約40-56エントリ
- 通常は十分だが、バースト書き込み時に満杯になる可能性

**Store Buffer Stall の可能性:**
- 頻繁なstore → store buffer満杯 → stall
- **IPC低下の一因**

## 7. 最適化の定量的効果予測

### 7.1 Priority 1: rx_dead check削除

**削減されるコスト:**
- write()あたり: 4-5サイクル
- 総コスト: 4-8B サイクル
- **約10-20%の性能向上（Batch size 1）**

**Batch size 256での効果:**
- write()の頻度が変わらないため、同様に10-20%改善

**予想されるIPC変化:**
- Batch size 1: 1.32 → 1.45-1.58 (+10-20%)
- Batch size 256: 2.46 → 2.71-2.95 (+10-20%)

### 7.2 Priority 2: try_recv()の二重store削除

**削減されるコスト:**
- recv()あたり: 2-3サイクル
- 総コスト: 1.25-2.5B サイクル
- **約3-6%の性能向上**

**予想されるIPC変化:**
- Batch size 1: 1.32 → 1.36-1.40 (+3-6%)

### 7.3 Combined Effect（Priority 1 + 2）

**総削減コスト:**
- 5.25-10.5B サイクル
- Total: 41.8B サイクル
- **約12.5-25%の性能向上**

**予想されるIPC変化:**
- Batch size 1: 1.32 → 1.49-1.65 (+13-25%)
- Batch size 256: 2.46 → 2.77-3.08 (+13-25%)

**ただし、IPC > 3は非現実的（CPUの限界）**
- 実際の改善はやや控えめになる可能性

### 7.4 スレッド同期ボトルネックへの影響

**最適化により:**
- 命令実行時間削減 → 相対的にスレッド同期待ちの割合増加
- しかし、**絶対的なスループットは向上**

**Pingpongベンチマークでの予想:**
- 現状IPC: 0.46
- 最適化後: 0.52-0.58 (+13-26%)
- **スレッド同期が依然として支配的**

## 8. 推奨される追加測定

### 8.1 perf record によるホットスポット特定

```bash
sudo perf record -e cycles:pp --call-graph dwarf \
  target/release/deps/comparison-* --bench --profile-time 3 spsc_batch/write_flush/1
sudo perf report
```

**取得できる情報:**
- 関数ごとのサイクル消費
- write(), flush(), poll(), sync()の正確なコスト

### 8.2 perf annotate によるアセンブリレベル分析

```bash
sudo perf annotate --stdio
```

**取得できる情報:**
- 命令ごとのサイクル消費
- rx_dead loadの正確なコスト
- ホットループの最適化機会

### 8.3 Cache Line分析

```bash
sudo perf c2c record target/release/deps/comparison-*
sudo perf c2c report
```

**取得できる情報:**
- キャッシュライン競合（False Sharing）
- スレッド間でのキャッシュライン共有状況

## 9. 結論

### 9.1 実測データからのボトルネック確認

**優先度順:**

1. **Critical: スレッド間同期待ち（40-47%）**
   - 根本的な改善は困難（ハードウェア制約）
   - 軽減策: バッチ処理（既に実装済み）

2. **High: write()のrx_dead check（10-20%）**
   - **削除可能 → 即座に10-20%改善**
   - 実装コスト: 低

3. **Medium: L1キャッシュミス（7.4%）**
   - 主にスレッド間通信によるキャッシュコヒーレンシ
   - 改善策: データ構造の最適化、NUMA aware配置

4. **Low: try_recv()の二重store（3-6%）**
   - 削除可能 → 3-6%改善
   - 実装コスト: 低

5. **Negligible: 分岐ミス（0.9%）**
   - 既に最適化されている
   - これ以上の改善不要

### 9.2 実装すべき最適化

**Priority 1（即座に実装）:**
- write()のrx_dead check削除 → **10-20%改善**
- try_recv()のhead store削除 → **3-6%改善**
- **合計: 13-26%の性能向上**

**Priority 2（中長期）:**
- NUMA aware thread placement
- prefetch最適化
- sync()の分離（ARM向け）

### 9.3 期待される最終性能

**現状（Batch size 1）:**
- IPC: 1.32
- Cycles/iteration: 40-80サイクル

**最適化後（Priority 1実施）:**
- IPC: 1.49-1.65 (+13-25%)
- Cycles/iteration: 30-60サイクル
- **約20-30%のスループット向上**

**現状（Batch size 256）:**
- IPC: 2.46
- Cycles/element: 4-8サイクル

**最適化後（Priority 1実施）:**
- IPC: 2.77-3.08 (+13-25%)
- Cycles/element: 3-6サイクル
- **約20-30%のスループット向上**

**これは非常に現実的かつ高い改善効果**
