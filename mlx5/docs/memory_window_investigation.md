# Memory Window (MW) 実装調査レポート

## 概要

Type 2 Memory Window (MW) のUMR WQE経由バインド機能を実装した際に発見した問題と、その調査結果をまとめる。

## 問題の症状

bind_mw (UMR WQE) を実行した後、同一QP上でRDMA WRITE/READ操作を行うと：
- CQEはsyndrome=0（成功）を返す
- しかしデータは実際には転送されない（宛先バッファが変更されない）

### 影響範囲

- bind_mw後の**すべての**RDMA WRITE/READ操作が影響を受ける
  - MWのrkeyを使用した場合だけでなく
  - MRのrkeyを使用した場合も同様に失敗
- SEND操作はUMR WQE後も正常に動作する
- bind_mwおよびlocal_invalidate操作自体は正常に完了する（CQE syndrome=0）

## 調査プロセス

### 1. 初期調査

WQEの内容を詳細にデバッグ出力し、以下を確認：
- ctrl_seg: opcode, ds_count, wqe_idx が正しい
- rdma_seg: remote_addr, rkey が正しい
- data_seg: addr, len, lkey が正しい
- doorbell: PI, WQEポインタが正しい

すべてのWQEフィールドは正しく設定されていた。

### 2. rdma-core調査

[rdma-core providers/mlx5/qp.c](https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/qp.c) を調査し、以下の仕様を発見：

#### fence要件

```c
// IBV_WR_BIND_MW または IBV_WR_LOCAL_INV の後
next_fence = MLX5_WQE_CTRL_INITIATOR_SMALL_FENCE;

// 次のWQE投入時
if (wr->send_flags & IBV_SEND_FENCE)
    fence = MLX5_WQE_CTRL_FENCE;
else
    fence = next_fence;
next_fence = 0;

ctrl->fm_ce_se = qp->sq_signal_bits | fence | ...
```

UMR操作の後、**次のWQE**に`INITIATOR_SMALL_FENCE`(0x20)を設定する必要がある。

#### qpn_mkeyフィールドの構成

mkey contextセグメントの`qpn_mkey`フィールド（offset 4-7）は：

```c
mkey->qpn_mkey = htobe32((rkey & 0xFF) | (qpn << 8));
```

- 下位8ビット: rkey tag (rkey & 0xFF)
- 上位24ビット: QPN << 8

### 3. fence修正後の再テスト

fence機構を実装したが、問題は解消しなかった。デバッグ出力で確認：

```
[DEBUG] write_ctrl: opcode=Umr fence=0x00 flags=0x0008 combined=0x0008
MW bound successfully
[DEBUG] write_ctrl: opcode=RdmaWrite fence=0x20 flags=0x0008 combined=0x0028
WRITE CQE: opcode=Req, syndrome=0
RDMA WRITE with MR rkey: CQE success but data mismatch!
```

fenceは正しく適用されている（0x0028 = COMPLETION | INITIATOR_SMALL_FENCE）が、データは転送されない。

### 4. 他のテストとの比較

| テスト | bind_mw前 | bind_mw後 | 結果 |
|--------|-----------|-----------|------|
| RDMA WRITE (MR rkey) | - | なし | 成功 |
| RDMA WRITE (MR rkey) | あり | あり | **失敗** |
| RDMA WRITE (MW rkey) | あり | あり | **失敗** |
| SEND | あり | あり | 成功 |
| 連続RDMA WRITE | なし | なし | 成功 |

## 実装した修正

### 1. INITIATOR_SMALL_FENCE フラグ追加

**ファイル**: `mlx5/src/wqe/mod.rs`

```rust
pub struct WqeFlags: u16 {
    /// Initiator small fence: wait for prior UMR (memory registration) operations.
    const INITIATOR_SMALL_FENCE = 0x0020;
    // ...
}
```

### 2. next_fence フィールド追加

**ファイル**: `mlx5/src/qp/mod.rs`

```rust
pub(crate) struct SendQueueState<Entry, TableType> {
    // ...
    /// Fence to apply to the next WQE (for UMR operation ordering).
    pub(super) next_fence: Cell<u8>,
    // ...
}
```

### 3. fence適用ロジック

**ファイル**: `mlx5/src/qp/builder.rs`

```rust
pub(super) fn write_ctrl_with_opmod(...) {
    // Apply fence from prior UMR operation
    let fence = self.sq.next_fence.get();
    self.sq.next_fence.set(0);

    // Combine passed flags with fence bits
    let flags_with_fence = WqeFlags::from_bits_truncate(flags.bits() | (fence as u16));
    // ...
}
```

BlueFlameパスにも同様の修正を適用。

### 4. UMR操作後のfence設定

**ファイル**: `mlx5/src/qp/builder.rs`

```rust
impl<'a, Entry> BindMwWqeBuilder<'a, Entry> {
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        // ...
        let sq = self.core.sq;
        let result = self.core.finish_internal();
        // Set fence for next WQE (required after UMR operations)
        sq.next_fence.set(WqeFlags::INITIATOR_SMALL_FENCE.bits() as u8);
        result
    }
}
```

`LocalInvalWqeBuilder`にも同様の修正を適用。

### 5. qpn_mkey構成の修正

**ファイル**: `mlx5/src/wqe/mod.rs`

```rust
pub unsafe fn write_mkey_context_seg_bind(
    ptr: *mut u8,
    mw_rkey: u32,
    qpn: u32,  // 追加
    // ...
) {
    // ...
    // For Type 2 MW bind:
    //   - Lower 8 bits: rkey tag (rkey & 0xFF)
    //   - Upper 24 bits: QPN << 8
    let qpn_mkey = (mw_rkey & 0xFF) | (qpn << 8);
    std::ptr::write_volatile(ptr.add(4) as *mut u32, qpn_mkey.to_be());
    // ...
}
```

## 変更ファイル一覧

| ファイル | 変更内容 |
|----------|----------|
| `mlx5/src/wqe/mod.rs` | `INITIATOR_SMALL_FENCE`追加、`MLX5_UMR_CTRL_CHECK_QPN`追加、`MLX5_MKEY_MASK_QPN`追加、`write_mkey_context_seg_bind`のqpn引数追加、`write_umr_ctrl_seg_invalidate`をCHECK_QPNに変更、`write_mkey_context_seg_invalidate`のqpn_mkey修正 |
| `mlx5/src/qp/mod.rs` | `SendQueueState`に`next_fence`フィールド追加、4箇所の初期化コード更新 |
| `mlx5/src/qp/builder.rs` | fence適用ロジック追加（WqeCore、BlueflameWqeCore、RoceBlueflameWqeCore）、UMR builder finishでnext_fence設定、klm_octwordsを2に変更 |
| `mlx5/tests/mw_tests.rs` | テストコメント更新、`test_mw_rdma_write`を`#[ignore]`に |

## 残存する問題

### Loopback環境での制限

UMR WQE後のRDMA WRITE/READ操作がloopback環境で動作しない問題は未解決。

#### 考えられる原因

1. **mlx5ハードウェア/ファームウェアの制限**
   - Loopback + UMRの組み合わせが完全にサポートされていない可能性
   - UCXも類似の問題を報告している（[openucx/ucx#7863](https://github.com/openucx/ucx/issues/7863)）

2. **DMA/キャッシュコヒーレンシの問題**
   - Loopback環境でのメモリ同期が正しく行われていない可能性

3. **UMR WQE構成の問題**
   - 他に必要なフィールドが欠けている可能性
   - ただしbind_mw/local_invalidate自体は成功する

## 追加調査 (公開情報ベース)

rdma-coreのmlx5実装を確認すると、Type 2 MWのUMR WQE構成にはいくつか必須のフィールドがあり、
現状の実装との差分が残っている可能性が高い。

### rdma-coreの実装から読み取れる必須要件

以下はrdma-core `providers/mlx5/qp.c` の実装に基づく。

1) **UMR controlのmkey_maskにQPNを含める**
   - Type 2 MW bindでは `MLX5_WQE_UMR_CTRL_MKEY_MASK_QPN` を必ず追加。
   - QPNを更新しないと、後続のRMAでQPNチェックが破綻する可能性がある。
   - 参考: `set_umr_control_seg()` で `type == IBV_MW_TYPE_2` の場合にQPN maskを追加。

2) **invalidate時はCHECK_QPNを使う**
   - Type 2 MW invalidateでは `MLX5_WQE_UMR_CTRL_FLAG_CHECK_QPN` を使用。
   - 現状はCHECK_FREEを使っているが、rdma-coreではinvalidate時はCHECK_QPN。

3) **KLM octowordsはALIGN(1,3)/2**
   - rdma-coreは `get_klm_octo(1) = ALIGN(1,3)/2 = 2` を設定。
   - 現状は `klm_octwords = 1` であり、KLMサイズ指定が不足している可能性がある。

4) **invalidate時のqpn_mkey**
   - bind_info->length == 0 (invalidate) の場合、qpn_mkeyの上位24bitは
     `0xFFFFFF` を設定する (QPN無効化)。
   - 現状は `qpn_mkey = mkey` のみで、rdma-coreと異なる。

### 具体的な差分 (実装との突合せ)

現状の実装との差分が根本原因になっている可能性が高い。
以下はrdma-coreに合わせるべき修正候補:

- `write_umr_ctrl_seg_bind` に `MLX5_MKEY_MASK_QPN` を追加
- `write_umr_ctrl_seg_invalidate` をCHECK_QPNに変更
- `klm_octwords` を `ALIGN(1,3)/2 = 2` に変更
- `write_mkey_context_seg_invalidate` の `qpn_mkey` を
  `0xFFFFFF00 | (mkey & 0xFF)` に合わせる

### 参考ソース

- rdma-core mlx5 QP実装:
  https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/qp.c
- rdma-core mlx5 UMR flag/mask定義:
  https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/mlx5dv.h

### 補足 (loopback制限について)

UCX側に「UMR + loopbackが未サポート」という明確な根拠は見つからなかった。
一方で、UMR WQE構成がrdma-coreと差分がある点は公開情報から明確で、
まずはこちらを修正・検証するのが最優先と考えられる。

### 今後の調査方針

1. 実環境（別ノード間）でのテスト
2. UCXのUMR実装との比較（別QPをUMR専用に使用）
3. mlx5カーネルドライバのUMR WQE処理の調査
4. NVIDIAサポートへの問い合わせ

## 追加修正の実施と結果 (2026-01-22)

上記「追加調査」セクションで特定した4つの差分について修正を実施した。

### 実施した修正

#### 1. mkey_maskにMLX5_MKEY_MASK_QPNを追加

**ファイル**: `mlx5/src/wqe/mod.rs`

```rust
// write_umr_ctrl_seg_bind() 内
let mask: u64 = mkey_mask::MLX5_MKEY_MASK_FREE
    | mkey_mask::MLX5_MKEY_MASK_KEY
    | mkey_mask::MLX5_MKEY_MASK_LEN
    | mkey_mask::MLX5_MKEY_MASK_START_ADDR
    | mkey_mask::MLX5_MKEY_MASK_QPN  // 追加
    | mkey_mask::MLX5_MKEY_MASK_ACCESS_LOCAL_WRITE
    | mkey_mask::MLX5_MKEY_MASK_ACCESS_REMOTE_READ
    | mkey_mask::MLX5_MKEY_MASK_ACCESS_REMOTE_WRITE
    | mkey_mask::MLX5_MKEY_MASK_ACCESS_ATOMIC;
```

#### 2. klm_octwordsを2に変更

**ファイル**: `mlx5/src/qp/builder.rs`

```rust
// bind_mw() 内
// KLM octowords calculation: ALIGN(nentries, 4) / 2
// For 1 entry: ALIGN(1, 4) / 2 = 4 / 2 = 2 (matching rdma-core's get_klm_octo)
let klm_octwords: u16 = 2;

// translations_octword_size in mkey context = get_klm_octo(nklms) = 2
let translations_octwords: u32 = 2;
```

#### 3. invalidate時のCHECK_QPNフラグ使用

**ファイル**: `mlx5/src/wqe/mod.rs`

```rust
// umr_flags モジュールに追加
/// Check QPN for Type 2 MW invalidate.
pub const MLX5_UMR_CTRL_CHECK_QPN: u8 = 1 << 4;

// write_umr_ctrl_seg_invalidate() 内
// flags: inline mode with CHECK_QPN for Type 2 MW invalidate (per rdma-core)
std::ptr::write_volatile(
    ptr,
    umr_flags::MLX5_UMR_CTRL_INLINE | umr_flags::MLX5_UMR_CTRL_CHECK_QPN,
);
```

#### 4. invalidate時のqpn_mkeyを修正

**ファイル**: `mlx5/src/wqe/mod.rs`

```rust
// write_mkey_context_seg_invalidate() 内
// For Type 2 MW invalidate: upper 24 bits = 0xFFFFFF (QPN invalidation marker)
// lower 8 bits = mkey tag (per rdma-core)
let qpn_mkey = 0xFFFFFF00 | (mkey & 0xFF);
std::ptr::write_volatile(ptr.add(4) as *mut u32, qpn_mkey.to_be());
```

### 修正後のテスト結果

```
mlx5 unit tests: 18 passed
mlx5 integration tests:
  - mw_tests: 7 passed, 1 ignored (test_mw_rdma_write)
  - その他: 全て passed (regression なし)
```

### test_mw_rdma_write の結果

**問題は解消されなかった。** 症状は修正前と同一：

```
MW bound successfully
WRITE CQE: opcode=Req, syndrome=0
Data mismatch after RDMA WRITE
  left: [0, 0, 0, ...] (宛先バッファは変更されず)
```

### 結論

rdma-coreとの差分として公開情報から特定できた修正をすべて適用したが、
loopback環境でのUMR後RDMA WRITE問題は解消しなかった。

残る可能性：
1. **mlx5ハードウェア/ファームウェアのloopback固有の制限**
2. **公開情報からは特定できないrdma-coreとの差分**
3. **DMA/キャッシュコヒーレンシの問題**

次のステップ：
- 実環境（別ノード間）でのテストを実施し、loopback固有の問題か確認
- NVIDIAサポートへの問い合わせ

## 追加調査の追記 (2026-01-23)

修正後も症状が変わらないため、rdma-core実装と再突合せを行い、
以下の新たな差分候補を特定した。

### 1. klm_octowordsの計算式が誤っている可能性

rdma-coreの`get_klm_octo()`は以下の実装:

```c
#define ALIGN(x, log_a) ((((x) + (1 << (log_a)) - 1)) & ~((1 << (log_a)) - 1))
static inline __be16 get_klm_octo(int nentries)
{
    return htobe16(ALIGN(nentries, 3) / 2);
}
```

このため、`nentries=1` の場合:
- `ALIGN(1, 3)` = 8
- `8 / 2` = **4**

つまり、**klm_octowordsは4が正**であり、現状の `2` は不一致。
KLMセグメントは64 bytesでパディングされるため、
HWが期待する長さとWQE上のKLM領域がずれている可能性がある。

**要修正候補**
- `klm_octwords` を `4` に変更
- `translations_octwords` も同値に合わせる (rdma-coreでは同値を使う)

### 2. UMR flag定義のbit位置が誤っている

`mlx5dv.h` の定義では:
- `MLX5_WQE_UMR_CTRL_FLAG_TRNSLATION_OFFSET = 1 << 4`
- `MLX5_WQE_UMR_CTRL_FLAG_CHECK_QPN = 1 << 3`

しかし現状の実装では `CHECK_QPN` を `1 << 4` としており、
**TRANSLATION_OFFSETと同じbitになってしまっている**。
そのため、invalidate時に本来のCHECK_QPNが立っていない可能性がある。

**要修正候補**
- `MLX5_UMR_CTRL_CHECK_QPN = 1 << 3` に修正

### 3. bind_mwの使用QPが誤っている可能性

Type 2 MWの `qpn_mkey` は、bindを投稿したQPのQPNを使う。
rdma-coreでは `ibqp->qp_num` をそのまま使用する。

Loopbackテストでは **QP1でbindし、QP1でRDMA WRITE** しているが、
実際のアクセス対象は **QP2側のバッファ** である。
この場合、**MWのQPNがQP1に固定され、QP2宛のRDMAアクセスが拒否される**
可能性がある。

**切り分け案**
- bind_mwをQP2側で実行し、QP1からRDMA WRITEする構成で再テスト
- これで動作するなら、bind_mwの使用方法が誤っていた可能性が高い

### 参考ソース (追加)

- rdma-core UMR KLM長計算:
  https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/qp.c
- rdma-core UMR flag定義:
  https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/mlx5dv.h

## テスト結果

```
mlx5 unit tests: 18 passed
mlx5 integration tests:
  - mw_tests: 7 passed, 1 ignored (test_mw_rdma_write)
  - その他: 全て passed
```

正常に動作するテスト：
- `test_mw_alloc_dealloc`: MW割り当て・解放
- `test_mw_multiple_alloc`: 複数MW割り当て
- `test_mw_bind_via_umr`: UMR WQEによるMWバインド
- `test_local_invalidate`: ローカル無効化
- `test_send_with_invalidate`: SEND with Invalidate
- `test_simple_rdma_write_sanity`: 基本RDMA WRITE（bind_mwなし）
- `test_two_consecutive_rdma_writes`: 連続RDMA WRITE（bind_mwなし）

## 参考資料

- [rdma-core providers/mlx5/qp.c](https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/qp.c)
- [rdma-core providers/mlx5/mlx5dv.h](https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/mlx5dv.h)
- [NVIDIA Memory Window Documentation](https://docs.nvidia.com/networking/display/rdmacore50/memory+window+(mw))
- [ibv_alloc_mw man page](https://man7.org/linux/man-pages/man3/ibv_alloc_mw.3.html)

---

*Last updated: 2026-01-22*

## 引き継ぎメモ (2026-01-23)

### 現象

- UMR bind後、**MW rkeyでの RDMA READ/WRITE が成功CQEにも関わらずデータ不一致**。
- **MR rkeyでの READ/WRITE は成功**（UMR後でも問題なし）。
- つまり「UMR後にQP全体が壊れる」ではなく **MW rkeyだけが無効**。

### 実施済み変更

- **QP2（Responder側）で bind → QP1からRMA** に修正
- **UMR後の fence** (`INITIATOR_SMALL_FENCE`) を適用
- **qpn_mkey** を `(rkey & 0xFF) | (qpn << 8)` に修正
- **translations_octword_size のオフセット**を 52 に修正
- **CHECK_QPN bit** を `1<<3` に修正
- **rkey更新**: `MemoryWindow::inc_rkey()` を追加し、bind時に
  `orig_rkey` と `new_rkey` を分離して使用
- **LOCAL_READ を常時付与**
- **log_page_size=12**（4KB）を mkey context に明示

### 最新のテスト結果

- `test_mw_after_bind_mr_rkey_write_minimal`: PASS  
- `test_mw_after_bind_mr_rkey_read_minimal`: PASS  
- `test_mw_rdma_write_minimal`（MW rkey WRITE）: FAIL  
- `test_mw_after_bind_mw_rkey_read_minimal`（MW rkey READ）: FAIL  

### Capログ

- `Device caps: raw=0x21361c36 MW2A=true MW2B=true`

### 追加テスト

`mlx5/tests/mw_tests.rs` に追加した最小テスト:
- `test_mw_rdma_write_minimal`
- `test_mw_after_bind_mr_rkey_write_minimal`
- `test_mw_after_bind_mr_rkey_read_minimal`
- `test_mw_after_bind_mw_rkey_read_minimal`

### 次の調査ポイント

1) **UCXでのMW/UMR使用例の調査**  
   - `src/uct/ib/mlx5/` と `test/gtest/` を中心に検索
2) **mkey contextの他フィールド検証**  
   - `flags_pd` の上位ビット/PDN構成  
   - `mkey_mask` に LOCAL_READ 追加の必要性  
   - Type2b専用のUMR設定差分

---

## 解決 (2026-01-24)

### 何が問題だったか

**UMRのKLM長指定がrdma-coreと不一致**だった。  
`klm_octwords` と `translations_octword_size` を `2` にしていたため、
rdma-coreの `get_klm_octo(1)` が返す **4** とズレていた。  
結果として **MW rkeyでのREAD/WRITEが成功CQEでもデータが反映されない**挙動を招いていた。

### 修正内容

- `klm_octwords` を **4** に修正  
- `translations_octword_size` も **4** に統一  
  - rdma-core の `get_klm_octo(1) = ALIGN(1, 3) / 2 = 4` に合わせる

**ファイル**: `mlx5/src/qp/builder.rs`

### 追加で発見した別件

`test_send_with_invalidate` で **RQ doorbellが不足**しており、
CQEがタイムアウトしていた。  
`post_recv()` 後に `ring_rq_doorbell()` を追加して解消。

**ファイル**: `mlx5/tests/mw_tests.rs`

### 検証結果

```
cargo test --release -p mlx5 --test mw_tests -- --nocapture
=> 12 passed; 0 failed; 0 ignored
```

最小テスト (`test_mw_*_minimal`) も含め **全テストがPASS** した。
