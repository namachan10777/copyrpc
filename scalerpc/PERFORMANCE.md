# ScaleRPC 性能分析レポート

## 測定環境

- Platform: Linux 6.8.0-90-generic
- CPU: (perf stat results below)
- RDMA: Mellanox mlx5 driver
- Benchmark: criterion 0.5
- Timing: minstant (RDTSC-based)

## 現在の性能

### Latency (Single QP, 32B payload, 256 slots)

| メトリクス | 値 |
|-----------|-----|
| RTT Latency | **4.4µs** |
| Throughput | **226K RPS** |

### ハードウェアカウンタ (perf stat)

| カウンタ | 値 | 備考 |
|---------|-----|------|
| IPC | 1.65 | Instructions per Cycle |
| Cache miss率 | 12.52% | LLC cache misses |
| Branch miss率 | 0.05% | 非常に低い |
| L1-dcache miss率 | 0.12% | 非常に低い |

---

## perf分析結果 (256 slots)

### 関数レベルのホットスポット

| 関数 | Self % | Children % | 説明 |
|------|--------|------------|------|
| `RpcServer::process` | 30.30% | 31.76% | リクエスト処理メインループ |
| `criterion::iter_custom` | 16.18% | 35.43% | ベンチマーク測定オーバーヘッド |
| `std::sys::backtrace` | 16.06% | 16.42% | バックトレース処理 |
| `Cq::try_next_cqe` | 9.48% | 10.32% | CQ完了エントリのポーリング |
| `clock_gettime` (vdso) | 10.37% | - | criterion内部の時刻取得 |
| `Cq::poll` | - | 4.76% | CQポーリング処理 |

### RpcServer::process 内部のホットスポット (アセンブリレベル)

| CPU % | 命令 | 対応コード | 説明 |
|-------|------|-----------|------|
| **15.59%** | `cmpl $0x0,0xffc(%r14,%r8,1)` | `MessageTrailer::read_from` | valid flagチェック |
| **6.83%** | `cmp %rdi,(%rsi,%rdx,1)` | `VirtualMapping::get_connection` | マッピングルックアップ |
| **4.79%** | `je 1c7410` | - | valid=0の場合のジャンプ |
| **4.51%** | `cmp 0xf0(%rbx),%r13` | - | スロットインデックス境界チェック |
| **3.26%** | `mov (%rsi,%rdi,1),%r13` | - | スロットインデックス読み取り |
| **2.32%** | `imul %r13,%r8` | - | スロットアドレス計算 |
| **1.86%** | `mov (%rax),%rdx` | - | コネクションイテレーション |
| **1.84%** | `push %rbp` | - | 関数プロローグ |
| **1.38%** | `movzwl 0x1c(%r14),%ebp` | `RequestHeader::read_from` | ヘッダ読み取り |

### Cq::try_next_cqe 内部のホットスポット

| CPU % | 命令 | 説明 |
|-------|------|------|
| **15.09%** | `movzbl 0x3f(%rbx,%rax,1),%ebp` | CQE ownership bitチェック |
| **9.35%** | `je 1c65bf` | CQ状態チェック |
| **8.48%** | `push %r13` | 関数プロローグ |
| **7.63%** | `mov %r9d,%r8d` | インデックス計算 |
| **7.57%** | `jne 1c6799` | ownership不一致時のジャンプ |
| **7.14%** | `movzbl 0x30(%rsi),%r8d` | CQ状態読み取り |
| **6.94%** | `cmp $0x80,%r10` | CQEサイズチェック |
| **6.66%** | `imul %r10,%rax` | CQEアドレス計算 |
| **6.21%** | `movzbl 0x3c(%rsi),%ecx` | CQ consumer index読み取り |

---

## ボトルネック分析

### 1. Valid Flag チェック (15.59%)

```rust
// check_slot_for_request内
let (msg_len, valid) = unsafe { MessageTrailer::read_from(slot_ptr) };
if valid == 0 || msg_len == 0 { return None; }
```

**問題**: 各スロットの末尾（offset 0xffc = 4092）をポーリング。スロット間隔が4KBのため、キャッシュライン単位でのアクセスが非効率。

**最適化案**:
- Valid bitmap: スロット毎にトレイラーを読むのではなく、別途bitmapで管理
- RDMA WRITE with IMM: IMMデータでスロットIDを通知

### 2. CQポーリング (9.48% + 4.76% ≈ 14%)

CQE ownership bitのチェックとCQEの読み取りがホットパス。

**現状**: Shared CQにより2回のポーリングで済む（改善済み）

### 3. criterion オーバーヘッド (16% + 10% ≈ 26%)

ベンチマークフレームワーク自体のオーバーヘッド。実アプリケーションでは発生しない。

---

## 最適化履歴

| 最適化 | Before | After | 改善率 |
|--------|--------|-------|--------|
| VirtualMapping: HashMap → Vec | 10.65% overhead | ~2% | **80%削減** |
| Shared CQ導入 | 40.5% (2048 CQ polls) | ~14% (2 CQ polls) | **65%削減** |
| Group-based slot allocation | 全スロットスキャン | アクティブグループのみ | **75%削減** |
| Zero-copy handler | Vec allocation毎回 | スロット直接書き込み | 軽微 |
| minstant (RDTSC) | 28.9ns/call | 19.8ns/call | **31%削減** |
| Pool size: 1024→256 slots | 5.9µs latency | 4.4µs latency | **25%削減** |

---

## 今後の最適化候補

### 高優先度

1. **Valid bitmap**
   - 現在: 各スロットの0xffcをポーリング（15.59%）
   - 提案: 64ビットbitmapで64スロット分を1回のメモリアクセスでチェック
   - 期待効果: スロットスキャンオーバーヘッドを1/64に削減

2. **RDMA WRITE with IMM**
   - 現在: valid flagをポーリング
   - 提案: IMMデータでスロットIDを直接通知、CQEから取得
   - 期待効果: ポーリング完全削除

### 中優先度

3. **CQE batching**
   - 複数CQEを一度に処理
   - prefetch最適化

4. **Doorbell coalescing強化**
   - より大きなバッチサイズ

---

## ベンチマーク実行方法

```bash
# 環境変数で設定可能
export SCALERPC_CLIENT_CORE=0
export SCALERPC_SERVER_CORE=1
export SCALERPC_NUM_SLOTS=256        # Single QP用
export SCALERPC_NUM_QPS=64           # Multi-QP用
export SCALERPC_NUM_GROUPS=4
export SCALERPC_MULTI_QP_PIPELINE_DEPTH=64
export SCALERPC_SLOTS_PER_CONN=4

# Latency測定
cargo bench --bench pingpong -- scalerpc_latency

# Multi-QPスループット測定
cargo bench --bench pingpong -- scalerpc_multi_qp

# perf分析
cargo build --release --bench pingpong
sudo perf record -g -- ./target/release/deps/pingpong-* --bench scalerpc_latency --profile-time 5
sudo perf report --stdio --percent-limit 1

# アセンブリレベル分析
sudo perf annotate --stdio -s scalerpc::server::RpcServer::process

# ハードウェアカウンタ
sudo perf stat -e cycles,instructions,cache-misses,cache-references,branch-misses,branches,L1-dcache-load-misses,L1-dcache-loads -- ./target/release/deps/pingpong-* --bench scalerpc_latency --profile-time 3
```

---

## 参考: ScaleRPC論文との比較

| メトリクス | 論文値 | 現在の実装 | 備考 |
|-----------|--------|-----------|------|
| Latency | ~2µs | 4.4µs | 最適化余地あり |
| Throughput | 20M RPS | 226K RPS | Multi-QPで改善予定 |

主な差異要因:
- Valid flagポーリングのオーバーヘッド（論文ではIMMベース）
- ベンチマーク環境の違い（ネットワーク vs ループバック）
