# UCX CQE圧縮処理の詳細分析

このドキュメントはUCX (Unified Communication X) のCQE圧縮実装を分析し、
copyrpcでの実装に必要な知見をまとめたものである。

## 1. 概要

CQE圧縮は、複数のCQEを1つの「圧縮ブロック」にまとめることで、PCIe帯域幅を削減する機能。
UCXでは以下の方式で実装されている：

1. **Title CQE**: 圧縮ブロックの直前にある通常CQE。共通情報を保持。
2. **圧縮CQE**: format=3のCQE。Mini CQE配列を含む。
3. **Mini CQE**: 各CQEの一意情報（wqe_counter, byte_cnt等）のみを保持する8バイト構造体。

## 2. データ構造

### 2.1 Mini CQE構造体

```c
typedef struct {
    uint16_t wqe_counter;    // WQEカウンター
    uint8_t  s_wqe_opcode;   // WQEオペコード (TX用)
    uint8_t  reserved2;
    uint32_t byte_cnt;       // バイトカウント
} uct_ib_mlx5_mini_cqe8_t;   // 8バイト
```

### 2.2 CQ Unzip状態構造体

```c
typedef struct {
    struct mlx5_cqe64       title;           // Title CQEのコピー
    uct_ib_mlx5_mini_cqe8_t mini_arr[7];     // Mini CQE配列（最大7個）
    uint32_t                block_size;       // 圧縮ブロックサイズ
    uint32_t                current_idx;      // 現在処理中のインデックス
    uint32_t                miniarr_cq_idx;   // 圧縮CQEのCQインデックス
    uint16_t                wqe_counter;      // Title CQEのWQEカウンター
    uint8_t                 title_cqe_valid;  // Title CQEが有効か
} uct_ib_mlx5_cq_unzip_t;
```

### 2.3 CQ構造体（圧縮関連フィールド）

```c
typedef struct uct_ib_mlx5_cq {
    // ... 基本フィールド ...
    int                    zip;              // 圧縮有効フラグ
    unsigned               own_field_offset; // Owner判定フィールドのオフセット
    unsigned               own_mask;         // Owner判定マスク
    uct_ib_mlx5_cq_unzip_t cq_unzip;        // 展開状態
} uct_ib_mlx5_cq_t;
```

## 3. Owner判定の仕組み

### 3.1 通常モード（zip=false）

```c
cq->own_field_offset = offsetof(struct mlx5_cqe64, op_own);  // 63
cq->own_mask         = MLX5_CQE_OWNER_MASK;                   // 0x01
```

- オフセット63の`op_own`フィールドを使用
- bit 0のみで比較

### 3.2 圧縮モード（zip=true）

```c
cq->own_field_offset = offsetof(struct mlx5_cqe64, signature); // 62
cq->own_mask         = 0xff;                                    // 全8ビット
```

- オフセット62の`signature`フィールドを使用
- 全8ビットで比較（iteration count）

### 3.3 Owner判定関数

```c
static inline int
uct_ib_mlx5_cqe_is_hw_owned(uct_ib_mlx5_cq_t *cq, struct mlx5_cqe64 *cqe,
                            unsigned cqe_index, int poll_flags)
{
    uint8_t sw_it_count = cqe_index >> cq->cq_length_log;
    uint8_t hw_it_count;

    if (poll_flags & UCT_IB_MLX5_POLL_FLAG_CQE_ZIP) {
        hw_it_count = ((uint8_t *)cqe)[cq->own_field_offset];
        return (sw_it_count ^ hw_it_count) & cq->own_mask;
    } else {
        return (sw_it_count ^ cqe->op_own) & MLX5_CQE_OWNER_MASK;
    }
}
```

**重要**: 圧縮モードでは`poll_flags`に`UCT_IB_MLX5_POLL_FLAG_CQE_ZIP`が必要。

## 4. CQE圧縮の検出と処理フロー

### 4.1 圧縮CQE検出

```c
#define UCT_IB_MLX5_CQE_FORMAT_MASK 0x0c

static inline int
uct_ib_mlx5_cqe_is_error_or_zipped(uint8_t op_own)
{
    static const uint8_t mask = UCT_IB_MLX5_CQE_FORMAT_MASK |
                                UCT_IB_MLX5_CQE_OP_OWN_ERR_MASK;
    return (op_own & mask) >= UCT_IB_MLX5_CQE_FORMAT_MASK;
}
```

- `op_own & 0x0c == 0x0c` で format=3（圧縮）を検出
- エラーCQEも同時にチェック

### 4.2 ポーリングフロー

```c
struct mlx5_cqe64 *uct_ib_mlx5_poll_cq(...)
{
    cqe = uct_ib_mlx5_get_cqe(cq, cq->cq_ci);

    // 1. Owner判定
    if (uct_ib_mlx5_cqe_is_hw_owned(cq, cqe, cq->cq_ci, poll_flags)) {
        return NULL;
    }

    ucs_memory_cpu_load_fence();

    // 2. 圧縮/エラーチェック
    if (uct_ib_mlx5_cqe_is_error_or_zipped(cqe->op_own)) {
        return check_cqe_cb(iface, cq, cqe, poll_flags);  // コールバックで処理
    }

    // 3. 通常CQE処理
    if (poll_flags & UCT_IB_MLX5_POLL_FLAG_CQE_ZIP) {
        cq->cq_unzip.title_cqe_valid = 0;  // Title CQE無効化
    }

    cq->cq_ci = cq->cq_ci + 1;
    return cqe;
}
```

### 4.3 圧縮CQEコールバック

```c
struct mlx5_cqe64 *
uct_rc_mlx5_iface_check_rx_completion(...)
{
    // 圧縮CQEかチェック
    if (uct_ib_mlx5_check_and_init_zipped(cq, cqe)) {
        ++cq->cq_ci;
        return uct_ib_mlx5_iface_cqe_unzip(cq);  // 展開して返す
    }

    // エラー処理...
}
```

## 5. 圧縮CQE展開処理

### 5.1 初期化（uct_ib_mlx5_iface_cqe_unzip_init）

```c
void uct_ib_mlx5_iface_cqe_unzip_init(uct_ib_mlx5_cq_t *cq)
{
    // Title CQEは圧縮CQEの1つ前（cq->cq_ci - 1）
    struct mlx5_cqe64 *title_cqe = uct_ib_mlx5_get_cqe(cq, cq->cq_ci - 1);
    struct mlx5_cqe64 *mini_cqe  = uct_ib_mlx5_get_cqe(cq, cq->cq_ci);

    if (cq->cq_unzip.title_cqe_valid == 0) {
        // 新しいTitle CQEをコピー
        memcpy(&cq_unzip->title, title_cqe, sizeof(cq_unzip->title));
        cq_unzip->wqe_counter        = ntohs(title_cqe->wqe_counter);
        cq->cq_unzip.title_cqe_valid = 1;
    } else {
        // 連続した圧縮ブロックの場合、WQEカウンターを更新
        cq_unzip->wqe_counter += cq_unzip->block_size;
    }

    // Mini CQE配列をコピー（最大7個）
    memcpy(&cq_unzip->mini_arr, mini_cqe, sizeof(mini_cqe8) * 7);

    // ブロックサイズ = (op_own >> 4) + 1
    cq_unzip->block_size = (mini_cqe->op_own >> 4) + 1;
    cq_unzip->miniarr_cq_idx = cq->cq_ci;
}
```

**重要ポイント**:
- Title CQEは圧縮CQEの**直前**のCQE
- ブロックサイズは圧縮CQEの`op_own >> 4`の値 + 1（1〜7）
- 連続した圧縮ブロックではTitle CQEを再利用

### 5.2 展開（uct_ib_mlx5_iface_cqe_unzip）

```c
struct mlx5_cqe64 *uct_ib_mlx5_iface_cqe_unzip(uct_ib_mlx5_cq_t *cq)
{
    uint8_t mini_cqe_idx = cq_unzip->current_idx % 7;
    struct mlx5_cqe64 *title_cqe      = &cq_unzip->title;
    uct_ib_mlx5_mini_cqe8_t *mini_cqe = &cq_unzip->mini_arr[mini_cqe_idx];

    cq_unzip->current_idx++;

    // Title CQEにMini CQEの一意情報を合成
    uct_ib_mlx5_iface_cqe_unzip_fill_unique(title_cqe, mini_cqe, cq_unzip);

    if (cq_unzip->current_idx < cq_unzip->block_size) {
        // 次のCQEスロットにマーカーを設定（次回のポーリング用）
        next_cqe = uct_ib_mlx5_get_cqe(cq, next_cqe_idx);
        next_cqe->op_own    = UCT_IB_MLX5_CQE_FORMAT_MASK;  // format=3
        next_cqe->signature = title_cqe->signature;
    } else {
        cq_unzip->current_idx = 0;  // 展開完了
    }

    return title_cqe;
}
```

**重要ポイント**:
- 展開中は`current_idx > 0`
- 次のCQEスロットに`op_own = 0x0c`（format=3）を設定して、次のポーリングで再度コールバックを呼ばせる
- Title CQEの`signature`を次のスロットにコピー（Owner判定用）

### 5.3 一意情報の合成

```c
static void
uct_ib_mlx5_iface_cqe_unzip_fill_unique(struct mlx5_cqe64 *cqe,
                                        uct_ib_mlx5_mini_cqe8_t *mini_cqe,
                                        uct_ib_mlx5_cq_unzip_t *cq_unzip)
{
    cqe->byte_cnt = mini_cqe->byte_cnt;

    if ((cqe->op_own >> 4) == MLX5_CQE_REQ) {
        // TX側: Mini CQEのwqe_counterとs_wqe_opcodeを使用
        cqe->wqe_counter  = mini_cqe->wqe_counter;
        cqe->sop_drop_qpn = (cqe->sop_drop_qpn & net_qpn_mask) |
                            htonl(mini_cqe->s_wqe_opcode << UCT_IB_QPN_ORDER);
    } else {
        // RX側: wqe_counterはインクリメンタル
        cqe->wqe_counter = htons(cq_unzip->wqe_counter + cq_unzip->current_idx);
    }
}
```

## 6. CQバッファ初期化

```c
void uct_ib_mlx5_fill_cq_buf(uct_ib_mlx5_cq_t *cq, unsigned cq_size)
{
    for (i = 0; i < cq_size; ++i) {
        cqe = uct_ib_mlx5_get_cqe(cq, i);
        cqe->op_own   |= MLX5_CQE_OWNER_MASK;      // owner = 1
        cqe->op_own   |= MLX5_CQE_INVALID << 4;    // opcode = INVALID
        cqe->signature = 0xff;                      // signature = 0xff
    }
}
```

初期状態:
- `op_own = 0xf1` (opcode=INVALID, owner=1)
- `signature = 0xff`

## 7. CQE圧縮が有効になる条件

### 7.1 CQ作成時の設定

```c
// DEVX API
if (attr->flags & UCT_IB_MLX5_CQ_CQE_ZIP) {
    UCT_IB_MLX5DV_SET(cqc, cqctx, cqe_comp_en, 1);
    UCT_IB_MLX5DV_SET(cqc, cqctx, cqe_comp_layout, 1);
}
```

### 7.2 デバイスサポートチェック

```c
if (init_attr->cqe_zip_sizes[dir] & attr.cqe_size) {
    attr.flags |= UCT_IB_MLX5_CQ_CQE_ZIP;
}
```

デバイスが特定のCQEサイズでの圧縮をサポートしているか確認。

## 8. 私たちの実装への示唆

### 8.1 現在の問題

CQE圧縮を有効にしても、HWは常に圧縮CQEを返すわけではない。
通常CQE（format=0）も混在して返されるため、UCXの方式では：

1. Owner判定は常にsignatureフィールドで行う（`zip=true`の場合）
2. しかし、通常CQEのsignatureはチェックサム値であり、iteration countではない

### 8.2 UCXの解決策

UCXは**ポーリング時にpoll_flagsで動的に判定方法を切り替える**：

```c
if (poll_flags & UCT_IB_MLX5_POLL_FLAG_CQE_ZIP) {
    // 圧縮モードのowner判定
} else {
    // 通常モードのowner判定
}
```

しかし、これでも「通常CQEでsignatureがiteration countでない」問題は残る。

### 8.3 実際の動作（推測）

UCXのコードを詳細に分析すると、以下の仕組みが見える：

1. **展開中のマーカー設定**: `uct_ib_mlx5_iface_cqe_unzip()`で次のCQEスロットに
   `op_own = 0x0c`と`signature = title->signature`を設定
2. **Title CQEのsignature**: HWが圧縮CQEを書き込む際、Title CQEの**signature**に
   iteration countを書き込む可能性
3. **圧縮CQEのチェーン**: 連続した圧縮ブロックはTitle CQEを共有し、
   signature（iteration count）も引き継がれる

### 8.4 実装方針

copyrpcでCQE圧縮をサポートするには：

1. **Owner判定の修正**:
   - 通常CQEではop_own bit 0で判定（現在の実装）
   - 圧縮CQE展開中はcurrent_idx > 0をチェック

2. **圧縮CQE検出**:
   - `(op_own & 0x0c) == 0x0c`でformat=3を検出

3. **展開状態の管理**:
   - `MiniCqeIterator`相当の構造体に`title_cqe_valid`を追加
   - 連続した圧縮ブロックでTitle CQEを再利用

4. **次スロットへのマーカー設定**:
   - 展開中は次のCQEスロットにformat=3とsignatureを設定

## 9. 参考資料

- [UCX ib_mlx5.h](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/ib_mlx5.h)
- [UCX ib_mlx5.inl](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/ib_mlx5.inl)
- [UCX ib_mlx5.c](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/ib_mlx5.c)
- [UCX rc_mlx5_iface.c](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/rc/rc_mlx5_iface.c)
- [UCX dv/ib_mlx5_dv.c](https://github.com/openucx/ucx/blob/master/src/uct/ib/mlx5/dv/ib_mlx5_dv.c)
