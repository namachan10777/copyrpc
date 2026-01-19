# CQE圧縮デバッグログ

## 問題の概要

CQE圧縮を有効化するとベンチマークがハングする。

## 環境

- `mlx5dv_create_cq`でCQE圧縮を有効化 → **成功**
- デバッグ出力: `[MonoCq] CQE compression enabled`

## 観測された動作

### CQE圧縮無効時
- 正常動作
- スループット: 30.9 Melem/s

### CQE圧縮有効時
- 約20個のCQEが処理された後ハング
- 全てのCQEは通常CQE（format=0）
- 圧縮CQE（format=3）は観測されなかった

```
[CQE] ci=0, opcode_raw=0x1, op_own=0x10, sw_owner=0, hw_owner=0
[CQE] ci=1, opcode_raw=0x1, op_own=0x10, sw_owner=0, hw_owner=0
...
[CQE] ci=19, opcode_raw=0x1, op_own=0x10, sw_owner=0, hw_owner=0
（ここでハング）
```

## op_ownバイトのレイアウト

```
[7:4] opcode
[3:2] format (0=通常, 3=圧縮ブロック)
[1]   reserved
[0]   owner
```

op_own=0x10の場合:
- opcode = 1 (RESP_RDMA_WRITE_IMM)
- format = 0 (通常CQE)
- owner = 0

## 圧縮CQE検出方法

### 現在のコード（間違い）
```rust
let opcode_raw = op_own >> 4;
if opcode_raw == 0x0f { // opcode == 15 をチェック
```

### UCXの正しい実装
```c
#define UCT_IB_MLX5_CQE_FORMAT_MASK 0x0c
if ((cqe->op_own & UCT_IB_MLX5_CQE_FORMAT_MASK) == UCT_IB_MLX5_CQE_FORMAT_MASK) {
    // format bits (bits 3:2) == 3 をチェック
}
```

## rdma-coreのCQE圧縮有効化処理

```c
if (mlx5cq_attr->comp_mask & MLX5DV_CQ_INIT_ATTR_MASK_COMPRESSED_CQE) {
    if (mctx->cqe_comp_caps.max_num &&
        (mlx5cq_attr->cqe_comp_res_format &
         mctx->cqe_comp_caps.supported_format)) {
        cmd_drv->cqe_comp_en = 1;
        cmd_drv->cqe_comp_res_format = mlx5cq_attr->cqe_comp_res_format;
    } else {
        // "CQE Compression is not supported"
        errno = EINVAL;
        goto err_db;
    }
}
```

## 調査すべき点

1. **CQ作成時**: CQE圧縮が有効化された場合、CQバッファの初期化に特別な処理が必要か？
2. **Doorbell更新**: CQE圧縮モードではdoorbell更新方法が異なるか？
3. **Owner bit計算**: CQE圧縮モードではowner bitの計算方法が異なるか？
4. **圧縮CQEが生成されない理由**: デバイスがサポートしていても、特定の条件でのみ圧縮されるか？

## UCXのCQEバッファ初期化

```c
void uct_ib_mlx5_fill_cq_buf(uct_ib_mlx5_cq_t *cq, unsigned cq_size)
{
    struct mlx5_cqe64 *cqe;
    int i;

    for (i = 0; i < cq_size; ++i) {
        cqe = uct_ib_mlx5_get_cqe(cq, i);
        cqe->op_own |= MLX5_CQE_OWNER_MASK;      // owner bit = 1
        cqe->op_own |= MLX5_CQE_INVALID << 4;   // opcode = INVALID
        cqe->signature = 0xff;                   // ★ 重要: signatureも初期化
    }
}
```

### 私たちのコードとの違い

**私たちのコード:**
```rust
const OP_OWN_INVALID: u8 = 0xf1; // opcode=INVALID(0xf), owner=1
std::ptr::write_volatile(op_own_ptr, OP_OWN_INVALID);
// signatureは設定していない
```

**UCXのコード:**
- op_own = 0xf1 (同じ)
- signature = 0xff (追加)

### signatureフィールドの役割

UCXのコメントより:
> "the signature field does not get copied from source to destination
> because of absence of the mlx5_cqe64::check_sum field on some systems."

CQE圧縮時にsignatureフィールドが特別な意味を持つ可能性あり。

## ★★★ 重大な発見: Owner bit判定方法の違い ★★★

UCXの`uct_ib_mlx5_init_cq_common`より：

```c
if (cq->zip) {
    // CQE圧縮が有効の場合
    cq->own_field_offset = ucs_offsetof(struct mlx5_cqe64, signature);  // オフセット62
    cq->own_mask = 0xff;                                                 // 全8ビット
} else {
    // 通常の場合
    cq->own_field_offset = ucs_offsetof(struct mlx5_cqe64, op_own);     // オフセット63
    cq->own_mask = MLX5_CQE_OWNER_MASK;                                  // bit 0のみ
}
```

### CQE64レイアウト（末尾）
- オフセット62: `signature` (圧縮時のowner判定に使用)
- オフセット63: `op_own` (通常時のowner判定に使用)

### 私たちのコード（問題あり）
```rust
// 常にオフセット63のbit 0を使用 - CQE圧縮時は間違い！
let op_own = unsafe { std::ptr::read_volatile(cqe_ptr.add(63)) };
let sw_owner = ((ci >> state.cqe_cnt_log2) & 1) as u8;
let hw_owner = op_own & 1;
if sw_owner != hw_owner {
    return None;
}
```

### UCXの方式（正しい）
CQE圧縮が有効の場合：
- **オフセット62**（signature）を読む
- **全8ビット**で比較
- sw_owner値も異なる計算方法

**これがCQE圧縮有効時のハング原因と考えられる！**

## UCXのowner判定コード詳細

`uct_ib_mlx5_cqe_is_hw_owned`関数より：

```c
// sw_it_count（ソフトウェア側のイテレーションカウント）
uint8_t sw_it_count = cqe_index >> cq->cq_length_log;

// CQE圧縮時
hw_it_count = ((uint8_t *)cqe)[cq->own_field_offset];  // オフセット62
return (sw_it_count ^ hw_it_count) & cq->own_mask;     // 0xffでマスク

// 通常時
return (sw_it_count ^ cqe->op_own) & MLX5_CQE_OWNER_MASK;  // bit 0のみ
```

### XOR比較の意味
- 結果が0 = owner bit一致 = ソフトウェアが所有（処理可能）
- 結果が非0 = owner bit不一致 = ハードウェアが所有（待機）

## 試行した修正と結果

### 修正1: signatureでのowner判定（失敗）

UCXの方式に従い、CQE圧縮有効時にsignatureフィールドでowner判定を行うように修正。

```rust
let is_hw_owned = if state.cqe_compression {
    let signature = unsafe { std::ptr::read_volatile(cqe_ptr.add(62)) };
    (sw_owner ^ signature) & 0xff != 0
} else {
    (sw_owner ^ op_own) & 0x01 != 0
};
```

**結果: ハングが継続**

デバッグ出力:
```
[CQE comp] ci=0, sw_owner=0x00, signature=0xff, is_hw_owned=true  // 初期化値
[CQE comp] ci=0, sw_owner=0x00, signature=0xe1, is_hw_owned=true  // HW書き込み後
```

### 問題の原因

CQE圧縮を有効にしても、**HWは通常CQE（format=0）を返し続ける**場合がある。

この場合:
- signatureフィールドには**チェックサム値**が入る（iteration countではない）
- signature=0xe1はチェックサム値
- sw_owner=0、signature=0xe1 → XOR = 0xe1 ≠ 0 → is_hw_owned=true（誤判定）

### 結論

CQE圧縮の有効化は「全てのCQEを圧縮する」わけではなく、「特定条件下でCQEを圧縮する可能性がある」という意味。通常CQEも混在して返されるため、owner判定は以下の複雑な処理が必要:

1. まずop_ownを読み、format bitsをチェック
2. format=3（圧縮CQE）の場合: signatureでowner判定、mini CQEを展開
3. format=0（通常CQE）の場合: op_ownのbit 0でowner判定

しかし、これには「読む前にowner判定が必要」という鶏と卵の問題がある。
UCXはこの問題をどう解決しているか、さらなる調査が必要。

### 現在の状態

- CQE圧縮は一時的に無効化
- テストは全て通過
- ベンチマーク: 30.7 Melem/s（CQE圧縮無効時）

## 今後の調査項目

1. UCXがformat=0と=3を混在して処理する方法
2. CQE圧縮が実際に発動する条件（連続CQE数、QP数など）
3. rdma-coreのmlx5プロバイダーでのCQE圧縮処理

## 追加調査（2026-01-19）

### 再現したハング現象

CQE圧縮を再度有効化してテストを行ったところ、新たな問題を発見。

#### wraparoundテストでの観測

```
Testing CQ wrap-around: 768 completions (CQ size = 256)
[CQE] ci=0, idx=0, op_own=0xf1, sig=0xff, sw_own=0, hw_own=1, format=0, is_hw_owned=true
[CQE] ci=0, idx=0, op_own=0x0c, sig=0x00, sw_own=0, hw_own=0, format=3, is_hw_owned=false
[COMPRESSED CQE] ci=0, idx=0, op_own=0x0c, mini_cqe_cnt=0x00, count=1, qpn=130904, title_opcode=Req
[CQE] ci=1, idx=1, op_own=0x00, sig=0x00, sw_own=0, hw_own=0, format=0, is_hw_owned=false
[CQE] ci=2, idx=2, op_own=0xf1, sig=0xff, sw_own=0, hw_own=1, format=0, is_hw_owned=true
（ci=2でハング - HWがCQEを書き込まない）
```

#### 発見事項

1. **ci=0で圧縮CQE（format=3）が生成された**
   - op_own=0x0c → opcode=0, format=3, owner=0
   - mini_cqe_cnt=0x00 → count=1（1つのmini CQEのみ）
   - Title CQEがない（ci=0が圧縮CQE）

2. **32個のWQEを投稿したが、2個のCQEしか生成されない**
   - ci=0: 圧縮CQE（count=1）
   - ci=1: 通常CQE（op_own=0x00）
   - ci=2以降: 初期化値のまま（op_own=0xf1）

3. **問題の根本原因**
   - HWはCQE圧縮を有効にすると、複数の完了を圧縮ブロックにまとめる
   - しかし、圧縮ブロックのmini_cqe_cnt=0x00は「1つのmini CQE」を示す
   - 32個の完了が1つのmini CQEに圧縮されるのは異常
   - 残りの完了がどこに行ったか不明

4. **Title CQEの不在**
   - UCXのドキュメントでは、圧縮CQEの直前にTitle CQEがあるべき
   - しかし、ci=0で圧縮CQEが生成され、Title CQEがない
   - これにより、展開に必要なopcode、QPN等の情報が欠落

### UCXとの比較

UCXでは以下の方式で圧縮CQEを処理している：

1. **poll_flags**によるowner判定の切り替え
   - `UCT_IB_MLX5_POLL_FLAG_CQE_ZIP`が設定されている場合、signatureフィールドで判定
   - 設定されていない場合、op_ownのビット0で判定

2. **初期化時のprogressハンドラ選択**
   - SRQトポロジやCQの圧縮状態に応じて適切なハンドラを選択
   - 圧縮が有効な場合は`cyclic_zip`モードを使用

3. **Title CQE管理**
   - 圧縮CQEの直前のCQEをTitle CQEとしてコピー保持
   - 連続した圧縮ブロックではTitle CQEを再利用

### 解決策（2026-01-19実装）

**問題の根本原因:**
1. CQE圧縮フォーマット（HASH, CSUM等）はRX（responder側）専用
2. TX CQに圧縮を有効にすると未定義動作になる
3. 圧縮モードでも通常CQE（format=0）が混在して返される
4. 通常CQEではsignatureにチェックサムが入り、iteration countではない

**実装した解決策:**

1. **RX CQ専用API `create_mono_cq_rx_compressed`を追加**
   - TX CQには使用しないことをドキュメントで明記

2. **format bitsに基づく動的なowner判定切り替え**
   ```rust
   const CQE_FORMAT_MASK: u8 = 0x0c;
   let is_compressed_format = (op_own & CQE_FORMAT_MASK) == CQE_FORMAT_MASK;

   let is_hw_owned = if state.cqe_compression && is_compressed_format {
       // 圧縮CQE（format=3）: signatureフィールドで判定
       let signature = unsafe { std::ptr::read_volatile(cqe_ptr.add(62)) };
       let sw_it_count = ((ci >> state.cqe_cnt_log2) & 0xff) as u8;
       (sw_it_count ^ signature) != 0
   } else {
       // 通常CQE（format=0）: op_own bit 0で判定
       let sw_owner = ((ci >> state.cqe_cnt_log2) & 1) as u8;
       let hw_owner = op_own & 1;
       sw_owner != hw_owner
   };
   ```

3. **ci=0での圧縮CQEガード**
   - Title CQEが存在しない状態（ci=0）で圧縮CQEが来た場合はスキップ

**テスト結果:**
- RX CQ圧縮テスト: 64件のRDMA WRITE IMMを正常に処理
- 全既存テスト: regression なし

### HW Capabilities確認（2026-01-19）

ConnectX-7 (MT4129), FW 28.39.1002での確認:
```
CQE Compression Caps:
  max_num: 64
  supported_format: 0x7 (HASH, CSUM対応)
```

**重要な発見**: `mlx5dv_query_device`呼び出し時に`comp_mask`に
`MLX5DV_CONTEXT_MASK_CQE_COMPRESION`を設定しないとcapsが返されない。

### 圧縮CQEが生成されない問題

CQ圧縮モードでの作成は成功するが、HWは圧縮CQE（format=3）を生成せず、
全て通常CQE（format=0）が返される。

**考えられる原因:**
1. **Strided RQ (MPRQ) が必要** - UCXでは主にStrided RQと組み合わせて使用
2. ファームウェアの内部閾値 - 圧縮発動条件がより厳しい
3. 完了の到着パターン - 十分な「バースト」が必要

### 今後の課題

1. **Strided RQ実装後に再テスト**
   - CQE圧縮はStrided RQとの組み合わせで効果を発揮する可能性が高い

2. **ベンチマークでの性能検証**
   - RX CQ圧縮有効時のスループット測定（現状はオーバーヘッドなし）

## 参考資料

- [rdma-core mlx5dv_create_cq](https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/man/mlx5dv_create_cq.3.md)
- [UCX ib_mlx5.inl](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/ib_mlx5.inl)
- [UCX ib_mlx5.h](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/ib_mlx5.h)
- [rdma-core verbs.c](https://github.com/linux-rdma/rdma-core/blob/master/providers/mlx5/verbs.c)
- [UCX ib_mlx5.c](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/ib_mlx5.c)
- [UCX rc_mlx5_iface.c](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/rc/rc_mlx5_iface.c)
