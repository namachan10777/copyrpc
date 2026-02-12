# DevX 化進捗メモ

## 完了済み (動作確認済み)
- **PRM モジュール** (`mlx5/src/prm.rs`): ビットフィールド操作、コマンド定数、QPC/CQC/SRQC オフセット
- **DevX RAII ラッパー** (`mlx5/src/devx.rs`): DevxUar, DevxUmem, DevxObj + Context メソッド
- **CQ DevX 化** (`mlx5/src/cq.rs`): ユーザ提供バッファ + UMEM、テスト全 pass
- **UD QP DevX 化** (`mlx5/src/ud/mod.rs`): テスト全 pass
- **DC (DCI/DCT) DevX 化** (`mlx5/src/dc/mod.rs`): DCI は SQ 操作含め動作確認済み
- **テスト/ベンチの利用箇所更新**: common.rs, mono_cq_tests.rs, rc_tests.rs, tm_tests.rs 等

## 未解決: RC QP の BAD_CTRL_WQE_INDEX (vendor_err=0x68)

### 症状
- DevX で作成した RC QP の最初の SQ 操作 (SEND, RDMA WRITE) で失敗
- wqe_counter=0 (最初の WQE で発生)
- DCI (SQ-only QP) は同じパターンで正常動作
- QP 作成・状態遷移 (RST→INIT→RTR→RTS) は全て成功

### 検証済み (問題なし)
1. PRM コマンドエンコーディング (hex dump で確認)
2. WQE 内容 (ctrl seg: opmod=0, wqe_idx=0, opcode=SEND, qpn 正常)
3. RQ stride 計算 (rdma-core 一致: stride=64, log_rq_stride=2)
4. バッファレイアウト (RQ offset 0, SQ offset rq_size)
5. DBREC / BF 書き込み
6. QP 状態遷移

### 試行済みで効果なし
1. DCI パターン (single UMEM, wq_umem_valid=1, dbr_umem_valid=1)
2. rdma-core dr_devx 完全一致パターン (別 DBREC UMEM, valid flags なし, pm_state=3)
3. RC QP with log_rq_size=0 → QP 作成拒否 (RC は RQ 必須)

### 次に試すべきこと
1. **`fre=1` と `rlky=1` を CREATE_QP に追加**
   - カーネルは全 QP に設定している
   - fre: firmware QPC bit offset = 0x454
   - rlky: firmware QPC bit offset = 0x5b
2. **verbs で作った QP の PRM コマンドをキャプチャして DevX と比較**
   - `MLX5_DEBUG` 環境変数 or strace でバイト列取得
3. **WQ "regular mode" vs "cyclic mode" の調査**
   - vendor_err 0x68 = "Wrong index when SQ/RQ in regular mode"

### カーネルが設定するが我々が未設定のフィールド
| フィールド | firmware offset | 値 | 備考 |
|-----------|----------------|---|------|
| fre | 0x454 | 1 | kernel QP は全て設定 |
| rlky | 0x5b | 1 | kernel QP は全て設定 |
| wq_signature | 0x20 | 0/1 | flag による |
| page_offset | 0x554 | 計算値 | サブページオフセット |
| xrcd | - | devr_id | 非SRQ QP にもデフォルト値 |
| srqn_rmpn_xrqn | - | devr_id | 非SRQ QP にもデフォルト値 |

### 参照ファイル
- rdma-core: `/work/home/internship2025/rdma-core/providers/mlx5/`
  - `dr_devx.c`: dr_devx_create_qp (DevX QP 作成)
  - `dr_send.c`: dr_create_rc_qp (RC QP + WQ 計算)
  - `qp.c`: mlx5_create_qp (verbs QP 作成)
  - `mlx5_ifc.h`: PRM フィールド定義
- カーネル: `/usr/src/mlnx-ofed-kernel-24.10.OFED.24.10.1.1.4.1/drivers/infiniband/hw/mlx5/qp.c`
