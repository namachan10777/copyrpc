# ScaleRPC 性能分析レポート

## 測定環境

- Platform: Linux 6.8.0-90-generic
- RDMA: Mellanox mlx5 driver
- Benchmark: criterion 0.5

## 現在の性能

### Latency (Single QP, 32B payload)

- **RTT Latency**: ~5.9µs
- **Throughput**: ~169K RPS

### Multi-QP Throughput (64 QP, 32B payload)

- **Throughput**: ~103K RPS

## perf分析結果

### ホットスポット (CPU時間の占有率)

| 関数 | Self % | 説明 |
|------|--------|------|
| `RpcServer::process` | 30.21% | リクエスト処理メインループ |
| `criterion::Bencher::iter_custom` | 20.57% | ベンチマーク測定オーバーヘッド |
| `vdso clock_gettime` | 11.42% | 時刻取得システムコール |
| `std::sys::backtrace` | 8.72% | バックトレース処理 |
| `Cq::try_next_cqe` | 6.66% | CQ完了エントリのポーリング |
| `Cq::poll` | 5.45% | CQポーリング処理 |

### 最適化履歴

1. **VirtualMapping: HashMap → Vec** (10.65% → ~2%)
   - Connection IDは連番なのでHashMapは不要
   - O(1)ルックアップで高速化

2. **Shared CQ導入** (40.5% → ~12%)
   - 1024 QP × 2 CQ = 2048 CQポーリング → 2 CQポーリング
   - 大幅なポーリングオーバーヘッド削減

3. **Group-based slot allocation**
   - 全16384スロットスキャン → アクティブグループの1024スロットのみ
   - メモリアクセス削減

4. **Zero-copy handler**
   - レスポンス毎のVec割り当てを削除
   - 効果は軽微（コピーはボトルネックではなかった）

5. **fastant::Instant**
   - std::time::Instant の代替
   - Linux/macOSではラッパーのため大きな効果なし

## ボトルネック分析

### 現在の主なボトルネック

1. **RpcServer::process (30%)**
   - リクエストポーリング
   - ハンドラ呼び出し
   - レスポンス送信

2. **CQポーリング (12%)**
   - `try_next_cqe`: 完了エントリの確認
   - `poll`: CQのドレイン

3. **時刻取得 (14%)**
   - criterion内部の測定
   - スケジューラのshould_switchチェック

## 今後の最適化候補

1. **CQポーリング最適化**
   - CQEバッチ処理
   - Adaptive polling

2. **should_switchの呼び出し頻度削減**
   - バッチ処理後のみチェック
   - 処理回数ベースのスケジューリング

3. **Doorbell batching強化**
   - より大きなバッチサイズ

## ベンチマーク実行方法

```bash
# 環境変数で設定可能
export SCALERPC_CLIENT_CORE=0
export SCALERPC_SERVER_CORE=1
export SCALERPC_NUM_QPS=64
export SCALERPC_NUM_GROUPS=4
export SCALERPC_MULTI_QP_PIPELINE_DEPTH=64

# Latency測定
cargo bench --bench pingpong -- scalerpc_latency

# Multi-QPスループット測定
cargo bench --bench pingpong -- scalerpc_multi_qp

# perf分析
sudo perf record -g -- ./target/release/deps/pingpong-* --bench scalerpc_latency --profile-time 3
sudo perf report --stdio --percent-limit 1
```
