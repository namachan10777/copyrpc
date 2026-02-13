# Delegation: No-Batching 理論性能モデル

## 目的

`delegation-batching-bistability` で観測された低均衡(= batching 崩壊時)について、
IPC 単体ベンチのレイテンシ/スループットから「batching 抜き理論性能」を計算し、
実測と比較する。

## 記号

- `np`: ノード数 (rank 数)
- `r`: remote 比率 (`remote_targets / total_targets`)
- `RTT_remote`: remote RPC の往復遅延 [s]
- `QD_total`: システム全体の remote outstanding 数
- `mu_ipc`: IPC 単体の no-batching サービス率 [ops/s/rank]
- `X_no_batch_total`: batching 抜き理論 total throughput [ops/s]

## 理論式

### 1. IPC 処理能力上限

`mu_ipc` は IPC 単体ベンチ (`depth=1` あるいは pingpong) から得る。

### 2. remote 側の供給上限 (Little の法則)

remote 完了供給は

`X_remote_cap = QD_total / (r * RTT_remote)`

で上限化される。

- 1 件の完了に平均 `r * RTT_remote` の remote 待ちが乗る近似
- `QD_total` は同時に飛んでいる remote リクエスト数

### 3. no-batching 理論 total throughput

batching が無い場合、処理側/供給側の小さい方で決まる:

`X_no_batch_total = min(np * mu_ipc, X_remote_cap)`

本ケースの観測値では `X_remote_cap` が十分大きく、実質 `np * mu_ipc` 支配。

## 根拠

1. 低均衡では CQE batch が 1 近傍で、固定費償却がほぼ効かない。
2. そのため 1 件あたりコストは IPC 単体 `depth=1` の挙動に近似できる。
3. ただし remote が律速する場合があるため、`QD_total/RTT` 上限も併置する。
4. よって `min(処理能力, 供給能力)` の形を採用する。

## 実測値 (2026-02-13)

### IPC 単体ベンチ

- `cargo bench -p ipc --bench pingpong -- u64 --sample-size 30 --measurement-time 3 --warm-up-time 1`
  - `thrpt median ~= 1.2346 Melem/s`
- `cargo bench -p ipc --bench multi_client -- depth_1 --sample-size 30 --measurement-time 3 --warm-up-time 1`
  - `thrpt median ~= 12.455 Melem/s`

### Delegation 実測 (memo/delegation-batching-bistability.md)

- NP=8: total `8.79 Mops/s` (低均衡)
- NP=16: total `110.04 Mops/s` (高均衡)
- outstanding avg: NP=8 `1343`, NP=16 `833`
- `RTT_remote ~= 6 us`
- `r`: NP=8 `7/8`, NP=16 `15/16`

remote 供給上限:

- NP=8: `QD_total/(r*RTT) = 1343 / ((7/8)*6e-6) ~= 255.8 Mops/s`
- NP=16: `833 / ((15/16)*6e-6) ~= 148.1 Mops/s`

→ どちらも `np * mu_ipc` より大きく、`np * mu_ipc` が no-batching 理論値。

## 比較表

### A. pingpong 基準 (`mu_ipc = 1.2346 Mops/s/rank`)

| NP | no-batching 理論 total [Mops/s] | 実測 total [Mops/s] | 実測/理論 |
|---|---:|---:|---:|
| 8  | 9.8768  | 8.79   | 89.0% |
| 16 | 19.7536 | 110.04 | 557.1% |

解釈:

- NP=8 低均衡は no-batching 理論に近い。
- NP=16 は batching により no-batching 理論を大幅に超える。

### B. multi_client depth_1 基準 (`mu_ipc = 12.455 Mops/s/rank`, 参考)

| NP | no-batching 理論 total [Mops/s] | 実測 total [Mops/s] | 実測/理論 |
|---|---:|---:|---:|
| 8  | 99.64  | 8.79   | 8.82% |
| 16 | 199.28 | 110.04 | 55.22% |

注:

- `depth_1` は 16-client 並列条件で、純粋 pingpong と前提が異なる。
- 「no-batching 崩壊基準」としては pingpong 基準を主、depth_1 は補助指標とする。

## まとめ

- no-batching 理論は `X_no_batch_total = min(np * mu_ipc, QD_total/(r*RTT_remote))`。
- 本データでは remote 供給は十分で、`np * mu_ipc` が支配。
- NP=8 低均衡は no-batching 理論に近く、NP=16 高均衡は batching 効果で大幅増速。
