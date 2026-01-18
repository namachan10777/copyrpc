# TODO詳細計画書（mlx5）

本ドキュメントは`memo/TODO.md`の各項目について、既存実装とrdma-core/UCXの挙動を参考にした詳細計画をまとめる。
対象リポジトリの既存設計メモは`memo/wqe_table.md`、`mlx5/design.md`を前提とする。

参照候補（外部）:
- rdma-core: `providers/mlx5/cq.c`, `providers/mlx5/qp.c`, `providers/mlx5/srq.c`
- UCX: `src/uct/ib/mlx5/ib_mlx5.c`

---

## 1) 可変長WQEが正しく構築可能か（inline + wrap around） ✅ 完了

### 目的
- InlineやSGE数によってWQEBB数が変動するWQEを正しく構築し、リング末尾の折り返しでも破綻しないことを保証する。

### 背景/参考
- rdma-coreはWQEの`qpn_ds`からWQEサイズを復元して「どれだけBBを消費したか」を追跡している（`providers/mlx5/qp.c`）。
- UCXは「CQEでは最後のWQE indexしか分からず、正確なBB消費数をCQEだけからは復元できない」前提で、送信側に保守的な予約を入れる（`ib_mlx5.c`の`bb_max`）。
- 本実装はBuilderパターンでWQEを直接構築するため、構築開始時点でWQEBB数が未確定となる。

### 方針
- Builder内部で「現在のWQEサイズ（バイト/DS数）」を逐次更新し、`finish()`でWQEBB数を確定する。
- wrap-around時はWQEBB境界で分割し、必要なら「余分なBBをNOPで埋める」か「次のWQE開始位置へ進める」方針を選択する。
- WQEサイズの計算は`calc_wqebb_cnt()`の結果に一致するように統一する。

### 実装タスク
- Builderに「現在のオフセット（bytes）」と「最大許容サイズ」を保持させる。
- inline追加/SGE追加時に、`offset` + 追加サイズがWQEバッファ終端を跨ぐかを判定。
- wrap-around時の挙動を決める：
  - 方式A: 今のWQEをNOPで埋めて次のWQEに回す（CQEとwqe_counterの一致が保ちやすい）。
  - 方式B: WQEを2分割して書き込む（実装が複雑、rdma-core/UCXは基本的に分割しない）。
- `finish()`で確定したWQEBB数をSQの`advance_pi()`に渡す。
- 単体テスト: inlineサイズ境界、SGE数境界、リング末尾付近でのwrap。

### リスク
- WQEを分割書き込みすると、ハードウェアの想定するWQEレイアウトに反する可能性がある。
- 余分なNOP埋め方式は有効BB数が減るが安全性が高い。

### 検証
- CQEで返るwqe_counterが常にWQEの先頭を指し、WQEBB消費数と整合しているか。
- `mlx5/tests/*`に「リング末尾+inline」ケースを追加。

### 実装結果（2026-01-18）
- NOP埋め方式（方式A）を採用
- `slots_to_end()`, `post_nop()` を SendQueueState に追加
- 公開API: `slots_to_ring_end()`, `post_nop_to_ring_end()` を RcQp/DCI/UD に追加
- `finish_internal()` に `debug_assert` でラップアラウンド検出を追加

---

## 2) WQE builderでフィールド不足時にエラーを出す

### 目的
- 必須フィールド（例: opcodeに必要なセグメント）が未設定のまま`finish()`されるのを防ぐ。

### 背景/参考
- rdma-coreはopcodeに応じて必要なsegを埋め、未設定があればCQEエラーを誘発する恐れがある。
- builderを柔軟にしたいのは主にsge/inline周り。

### 方針
- 「sge/inline以外はインライン関数で書き込み → データ領域ハンドルを返す → sge/inline追加 → finish」の2段階APIに切り替える。
- 必須セグメント不足は「データ領域ハンドル取得時に必須固定セグメントを確定済みであること」を保証することで回避する。
- sge/inlineは可変長として「最低1つ必要 or 0可」の条件をopcodeごとに明示し、`finish()`で不足時に`io::Error`を返す。

### 実装タスク
- opcodeごとの「固定セグメント構築関数」を用意し、データ領域ハンドルを返す。
- データ領域ハンドルはsge/inline追加のみを許可する型にする。
- `finish()`で「sge/inline不足」時にエラー。

### 検証
- 各opcodeで必須フィールド不足時にエラーになるテスト。

---

## 3) DC Stream実装（Unordered）

### 目的
- DC Streamで順序が崩れる前提のWQE管理・CQE処理を実装する。

### 背景/参考
- `memo/wqe_table.md`と`mlx5/design.md`にある通り、DC Streamはunordered扱い。
- rdma-coreではSQ管理は`wqe_head`を使った順序前提が多いが、TM-SRQなどunorderedではbitmapなどを使って解放管理している。

### 方針
- Unordered用のWQE tableを導入（Optionエントリ + 全Signaled）。
- 空き判定は「悲観/楽観の2種」を持ち、厳密版はスキャン or ビットマップで算出。
- CQE毎に該当エントリのみ解放し、順序前提の一括解放はしない。
- TM-SRQのRQ側WQE tableがすでに同種の挙動なので、共通化する。

### 実装タスク
- `UnorderedWqeTable`実装（エントリ単位で解放）。
- DC Stream用のbuilder/queue型を追加。
- CQE処理で`wqe_counter`に対応するエントリのみ解放。
- `available()`と`scan_available()`を分ける。
- TM-SRQのRQ側実装と共通化できる形（モジュール/トレイト分離）にする。

### 検証
- DC StreamでのCQE out-of-orderを模したテスト（可能なら実機）。

---

## 4) WQE table と SQ/RQ の直交 API 設計

### 目的
- Ordered/UnorderedとDense/Sparse、Send/Recvの組合せを型で表現し、APIを整理する。

### 背景/参考
- `mlx5/design.md`で「キューとWQE tableの統合」の問題が記載。
- `memo/wqe_table.md`にOrdered/Unorderedの管理パターンが整理されている。

### 方針
- 型レベルで「順序保証」「シグナル戦略」「WQE table密度」を表現。
- `Queue<WqeTable, Ordering, SignalPolicy>`のような分離。

### 実装タスク
- 型パラメータとトレイトの設計。
- `OrderedDense`, `OrderedSparse`, `UnorderedAllSignal`などの具象型を用意。
- 既存SQ/RQ/SRQ/TM-SRQ/DCI/DC Streamへの割当てを整理。

### 検証
- 型で不正な組合せがコンパイルエラーになるか。

---

## 5) DC Streamの型制約（Stream ID設定可能性）

### 目的
- Unordered + Stream ID設定可能、Ordered系はStream ID設定不可、という制約を型で保証。

### 方針
- `StreamCapable`トレイトを`Unordered`系のみに実装。
- `builder.stream_id()`の実装を`StreamCapable`に限定。

### 実装タスク
- トレイト/ジェネリクス設計。
- `wqe_builder()`の型引数整理。

### 検証
- Ordered系で`stream_id()`が呼べないことのコンパイル確認。

---

## 6) atomic命令のインターフェース改善

### 目的
- 使いやすさと安全性を高める（型/引数順/サイズ確認）。

### 方針
- `compare-and-swap`, `fetch-add`を明示的APIに分割。
- `remote_addr`, `rkey`, `compare`, `swap`の順序統一。

### 実装タスク
- builder APIを分割・整理。
- ドキュメントと例の追加。

### 検証
- 既存テスト更新、型安全性向上の確認。

---

## 7) mlx5拡張atomic命令サポート

### 目的
- mlx5固有の拡張アトミック命令を扱えるようにする。

### 背景/参考
- UCXの`rc_mlx5.inl`に拡張atomicの実装があり、`MLX5_OPCODE_ATOMIC_MASKED_CS`と`MLX5_OPCODE_ATOMIC_MASKED_FA`を使用。
- 32/64bit向けにopmodで拡張サイズを指定（`UCT_IB_MLX5_OPMOD_EXT_ATOMIC(2/3)`相当）。
- rdma-coreの`providers/mlx5/wqe.h`に`struct mlx5_wqe_masked_atomic_seg`が定義されている。

### 方針
- masked compare-and-swap / masked fetch-and-add を追加する。
- 32bit/64bitの両方を扱えるAPIを用意し、opmodで拡張サイズを指定する。
- UCXと同様に、上位APIではAND/OR/XOR/SWAPをmasked atomicにマップ可能にする。
- UCXのレイアウトに合わせ、32bitは単一セグメント、64bitは必要に応じて2セグメントを使う設計にする。

### 実装タスク
- `mlx5/src/wqe/mod.rs`に`MLX5_OPCODE_ATOMIC_MASKED_CS`/`MLX5_OPCODE_ATOMIC_MASKED_FA`相当の定義を追加。
- 32/64bitのマスク付きCS/FA用セグメント構造体を追加（UCXの`uct_ib_mlx5_atomic_masked_*`に準拠）。
- Builder APIに `masked_compare_swap32/64`, `masked_fetch_add32/64` を追加。
- 上位APIでAND/OR/XOR/SWAPを提供する場合、UCX同様にmasked CS/FAに割り当てる。
- 64bit masked CSは「swap/compare」と「swap_mask/compare_mask」を2セグメントに分ける必要がある点を反映する。
- 32bit masked CSは`swap/compare/swap_mask/compare_mask`、masked FAは`add/field_boundary`の引数対応を明文化する。

### 検証
- 実機での動作確認（可能なら）。

---

## 8) ibverbs_sys削除（mlx5_sysのみ） ✅ 対応不要

### 目的
- 依存を簡素化し、メンテコストを下げる。

### 方針
- `ibverbs_sys`で参照している型・定数を`mlx5_sys`に移行。

### 実装タスク
- 依存箇所の洗い出し（`rg ibverbs_sys`）。
- `Cargo.toml`/`Cargo.lock`から削除。

### 検証
- ビルド確認。

### 調査結果（2026-01-18）
- mlx5クレートは既にmlx5_sysのみに依存しており、ibverbs_sysへの依存は存在しない
- 対応不要

---

## 9) RELAXED_ORDERING対応

### 目的
- 順序保証を外した場合でも正しくWQEBB解放・callbackが行える設計にする。

### 背景/参考
- `memo/TODO.md`に「Signaled+Fencedのみ更新、他は無視」案がある。

### 方針
- `UseRelaxedOrdering`の型タグで挙動を分岐。
- `Fence`付きのSignaledのみが解放/コールバック対象。
- 理由: RELAXED_ORDERINGではCQE順序が保証されないため、Fenceが無いCQEで`sq_ci`を進めると未完了WQEを誤って解放する可能性がある。Ordered実装を維持するにはFence到着時のみ`sq_ci`を進める必要がある。

### 実装タスク
- 型タグの導入。
- WQE build時に`Fence`の強制や警告をドキュメントに明記。
- CQE処理で「Relaxed Orderingモード時は特定条件のみ解放」。

### 検証
- FenceなしSignaledのCQEを無視するテスト。

---

## 10) Inline CQE parse ✅ 完了

### 目的
- Inline scatterされたCQEのpayloadを正しく読み出し、ユーザバッファに配置する。

### 背景/参考
- rdma-coreは`MLX5_INLINE_SCATTER_32/64`を判定してCQE内のinlineデータをコピーする（`providers/mlx5/cq.c`）。

### 方針
- CQE opcode/flagでinline有無を判定。
- 32/64byteのオフセット差を吸収してコピー。

### 実装タスク
- CQE構造体にinline scatter flagを追加。
- CQEパーサーにコピー処理を追加。

### 検証
- Inline受信時のdata一致テスト。

### 実装結果（2026-01-18）
- `CqeOpcode::InlineScatter32`, `InlineScatter64` を追加
- `CqeOpcode::is_inline_scatter()` メソッドを追加
- `Cqe` に `inline_data` フィールド（最大32バイト）を追加
- `Cqe::inline_data()` アクセサメソッドを追加
- TM-SRQ の `dispatch_cqe` で inline scatter オペコードをハンドリング

---

## 11) CQE compressionパース ✅ 完了

### 目的
- CQE圧縮形式を解釈して通常CQEとして扱えるようにする。

### 背景/参考
- UCXはmini CQE展開ロジックを持ち、`wqe_counter`を連番で補完する（`ib_mlx5.c`）。

### 方針
- まずCQE圧縮の有無を検知。
- mini CQEから`wqe_counter`を再構成し、通常CQE構造に展開。

### 実装タスク
- CQEヘッダ判定ロジック追加。
- mini CQE展開のためのテンポラリバッファと状態管理。

### 検証
- 圧縮CQEをエミュレートするユニットテスト（可能なら）。

### 実装結果（2026-01-18）
- `MiniCqeFormat` 列挙型（Responder/Requester/ResponderCsum/L3L4Hash）を追加
- `MiniCqe` 構造体とパース関数（responder/requester形式）を追加
- `CompressedCqeHeader` 構造体を追加
- `MiniCqeIterator` で mini CQE 配列を展開
- `CqState` に `pending_mini_cqes` と `title_opcode` を追加
- `try_next_cqe()` で圧縮CQE（opcode 0x0f）を検出し自動展開
- 64バイトCQE形式で最大7個の mini CQE をサポート

---

# 優先順位（TODOと同順）

1. ~~Inline WQEの折り返しを正しく実装~~ ✅ 完了
2. WQE builderでのエラー
3. ~~CQE compression~~ ✅ 完了
4. ~~Inline CQE parse~~ ✅ 完了
5. API直交性や型制約の確認
6. 拡張atomic
7. DC Stream
8. RELAXED Ordering
