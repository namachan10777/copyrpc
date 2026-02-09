# eRPC パフォーマンス分析 (2026-01)

## 測定環境

- ベンチマーク: `cargo bench --bench rpc_bench -- "32B_depth64"`
- 計測: `perf record -g --call-graph dwarf -F 997`
- 現行性能: **3.74 Mops/s** (depth64), **4.44μs** latency (32B)

## プロファイル結果サマリ

| カテゴリ | 関数群 | CPU % | 備考 |
|---------|--------|-------|------|
| **イベントループ** | `run_event_loop_once` | 37.89% | 本体 |
| **CQポーリング** | `MonoCq::poll`, `Cq::poll`, `try_next_cqe` | 7.59% | mlx5レイヤー |
| **ベンチマークオーバーヘッド** | `exp`, `rayon`, criterion | ~14% | 統計計算 |
| **時刻取得** | vdso `clock_gettime` | ~7% | タイムスタンプ |
| **送信** | `post_send_raw` | 3.38% | WQE作成・ポスト |
| **タイミングホイール** | `cancel`, `advance`, `insert` | 3.51% | RTO管理 |
| **ハッシュ操作** | `hash_one`, `HashMap::*` | 3.05% | req_num検索 |
| **メモリ管理** | `malloc`, `free`, `memmove` | 3.53% | ヒープ操作 |
| **受信** | `post_recv_raw` | 1.00% | バッファ再ポスト |
| **その他** | `call`, `reply`, etc. | ~5% | アプリケーションロジック |

## 詳細分析

### 1. イベントループ (`run_event_loop_once`) - 37.89%

最大のホットスポット。内部では以下の処理を実行:

```
run_event_loop_once
├── MonoCq::poll (recv CQ)           ~3%
├── Cq::poll (send CQ)               ~3%
├── TimingWheel::advance             ~1%
├── TimingWheel::cancel              ~2%
├── clock_gettime (vdso)             ~5%
└── process_recv / process_send      ~24%
```

**ボトルネック要因**:
- 毎ループで`Instant::now()`を2回呼び出し（時刻取得コスト）
- TimingWheel操作でハッシュマップアクセス
- 完了キューのポーリングループ

### 2. CQ ポーリング - 7.59%

```
MonoCq<Q,F>::poll           2.51%  (recv専用モノモーフィックCQ)
Cq::poll                    2.17%  (send用共有CQ)
Cq::try_next_cqe            1.99%  (CQE取得)
MonoCq::dispatch_cqe        0.92%  (コールバック発火)
```

MonoCqはrecv専用に最適化されているが、それでも相当なCPU時間を消費。
CQEの取得とdispatchは必須処理であり削減困難。

### 3. 時刻取得 (vdso) - ~7%

```
vdso 0xb00 (clock_gettime)  4.48%
clock_gettime@@GLIBC_2.17   0.37%
Timespec::sub_timespec      1.12%
Timespec::now               0.54%
current_time_us             0.24%
```

**用途**:
- タイミングホイールのadvance
- RTOタイムアウト計算
- CC (Congestion Control) の時間ベース判定

### 4. タイミングホイール - 3.51%

```
TimingWheel::cancel         1.97%  (レスポンス受信時にRTOキャンセル)
TimingWheel::advance        0.87%  (時間進行・タイムアウト検出)
TimingWheel::insert         0.43%  (リクエスト送信時にRTO設定)
```

cancel操作が最も重い。ハッシュマップからのエントリ削除を含む。

### 5. ハッシュ操作 - 3.05%

```
hash_one                    1.27%
Hasher::write               0.79%
HashMap::remove             0.57%
HashMap::insert             0.42%
```

req_numをキーとした保留リクエスト/レスポンスの追跡に使用。
SipHash (デフォルトハッシャー) は暗号学的強度があり若干重い。

### 6. メモリ管理 - 3.53%

```
_int_free                   1.04%
memmove_avx512              0.88%
_int_malloc                 0.57%
malloc                      0.52%
cfree                       0.52%
```

**残存するヒープ確保**:
- `recv_queue: VecDeque<IncomingRequest>` への追加
- `pending_response_bufs: Vec<(u32, usize)>` の操作
- Criterionベンチマーク自体のデータ収集

### 7. 送受信パス

```
post_send_raw               3.38%  (WQE構築・ポスト)
post_recv_raw               1.00%  (受信バッファ再ポスト)
call                        2.41%  (リクエスト発行)
reply                       0.71%  (レスポンス送信)
```

Zero-Copy最適化後は送受信自体は軽量。
`post_send_raw`はWQE構築・アドレスハンドル設定を含むため比較的重い。

## 今後の最適化候補

### 高効果 (期待改善: 10%+)

1. **時刻取得の削減**
   - `rdtsc` を使用してvdsoコールを削減
   - RTOチェック間隔を延ばす（毎ループ→Nループごと）
   - 時刻を一度取得し、ループ内で再利用

2. **タイミングホイールの軽量化**
   - ハッシュマップからスロット直接参照方式へ変更
   - cancelをO(1)にする（req_numでスロット直接アクセス）

3. **ハッシャーの変更**
   - `ahash` や `rustc-hash` (FxHash) に変更
   - SipHashより3-5倍高速

### 中効果 (期待改善: 5-10%)

4. **recv_queueの最適化**
   - VecDequeからリングバッファへ
   - 容量を固定してアロケーション排除

5. **CQポーリングのバッチ化**
   - 複数CQEを一度に処理
   - dispatch_cqe呼び出し回数削減

### 低効果 (期待改善: <5%)

6. **インライン化促進**
   - 頻繁に呼ばれる小関数に`#[inline(always)]`追加

7. **pending_response_bufsの固定配列化**
   - 試行済み・失敗（リクエスト/レスポンス混在が原因）

## 参考: 関数別詳細プロファイル

```
37.89%  erpc::rpc::Rpc<U>::run_event_loop_once
 7.74%  __ieee754_exp_fma                         [criterion統計]
 5.44%  rayon::iter::plumbing::bridge_producer_consumer::helper [criterion]
 4.59%  std::sys::backtrace::__rust_begin_short_backtrace
 4.48%  [vdso] clock_gettime
 4.40%  exp@@GLIBC_2.29                           [criterion統計]
 3.38%  erpc::transport::UdTransport::post_send_raw
 2.51%  mlx5::mono_cq::MonoCq<Q,F>::poll
 2.41%  erpc::rpc::Rpc<U>::call
 2.17%  mlx5::cq::Cq::poll
 1.99%  mlx5::cq::Cq::try_next_cqe
 1.97%  erpc::timing::TimingWheel::cancel
 1.27%  core::hash::BuildHasher::hash_one
 1.12%  std::sys::pal::unix::time::Timespec::sub_timespec
 1.04%  _int_free
 1.00%  erpc::transport::UdTransport::post_recv_raw
 0.92%  mlx5::mono_cq::MonoCq<Q,F>::dispatch_cqe
 0.88%  __memmove_avx512_unaligned_erms
 0.87%  erpc::timing::TimingWheel::advance
 0.79%  <core::hash::sip::Hasher<S> as core::hash::Hasher>::write
 0.73%  __math_check_oflow                        [criterion]
 0.71%  erpc::rpc::Rpc<U>::reply
```

## 試行済み最適化（効果なし）

### rdtsc系ライブラリによる時刻取得

`minstant` および `quanta` クレートを試行したが、いずれも **vdso経由の`clock_gettime`より遅い**結果となった。

| 実装 | スループット | 結果 |
|------|-------------|------|
| `SystemTime::now()` (vdso) | 3.72 Mops/s | ベースライン |
| `minstant::Instant::now().as_unix_nanos()` | 2.07 Mops/s | **-44%** |
| `minstant::Instant::now().elapsed()` | 2.00 Mops/s | **-46%** |
| `quanta::Clock::delta()` | 2.03 Mops/s | **-45%** |

**原因**: TSC→ナノ秒変換のオーバーヘッドがvdsoより重い。vdsoはカーネルがユーザー空間にマッピングした高速パスで、syscallなしで`clock_gettime`を実行できる。rdtscライブラリのキャリブレーション・変換処理がそれを上回るオーバーヘッドを追加する。

**教訓**: 時刻取得の最適化は、vdsoが十分高速なLinux環境では効果が薄い。他のボトルネック（タイミングホイール、ハッシュ操作）に注力すべき。

---

## 結論

現在の実装では、イベントループ自体が37.89%を占め、その内訳は:
- **時刻取得**: ~7% (vdso) - rdtsc置換は**効果なし**
- **CQポーリング**: ~7% (mlx5)
- **タイミングホイール**: ~3.5%
- **ハッシュ操作**: ~3%
- **残りの処理ロジック**: ~17%

最も効果的な最適化は:
1. ~~rdtscによる時刻取得の高速化~~ → **試行済み、効果なし**
2. FxHashへのハッシャー変更
3. タイミングホイールのO(1)化
4. 時刻取得回数の削減（ループ内で一度だけ取得）

これらの実装により、さらに10-20%のスループット向上が期待できる。
