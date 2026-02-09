# mlx5 design detail

## 目的
- WQE 構築を Builder パターンで最小コスト化する。
- CQE からユーザデータを取り出す処理を内部に隠蔽する。
- SQ/RQ/SRQ/DCI/TM-SRQ/DC-Stream の順序保証差分を吸収し、同一インターフェースで扱えるようにする。

## 非目的
- 通信プロトコル(RC/UD/…)の意味論を作り込むこと。
- CQE 解析の詳細(フィールドレベルのパース)をここで確定させること。

## 用語
- ring_size: WQE リングのサイズ。2 の冪に固定。
- wqe_index: ring 内の位置。`wqe_index = wqe_counter & (ring_size - 1)` を基本とする。
- wqe_counter: CQE に含まれるカウンタ。送信/受信の進行を示す。
- WQE table: ring_size と同じ長さのユーザデータ管理テーブル。

## 順序保証の整理
### 完全順序(Ordered)
以下は「ある wqe_counter が返ったらそれ以前は全て完了」が成立する。
- SQ(QP)
- RQ(QP)
- SRQ
- DCI

### 非順序(Unordered)
以下は stream-id や tag が異なると順序が成立しないと扱う。
- DC Stream (stream-id 間)
- TM-SRQ (tag 間)

補足: partial order を活かす最適化は非目的としてまずは unordered 扱いで統一する。

## ユーザデータ管理の基本方針
CQE から `wqe_counter` を取得し、`wqe_index` で WQE table を参照してユーザデータを返す。
リング長と同じサイズの WQE table を持つことで、NIC がユーザデータを管理しない前提を補う。

### パターン別実装方針
#### 1) Ordered + Dense + SendQueue
- 目的: signaled を間引いても正しい完了処理を行う。
- 実装:
  - すべての WQE に user_data を登録。
  - `last_cqe_counter` を保持し、CQE 受信時に `(last_cqe_counter, current]` の範囲を完了とみなす。
  - 完了範囲内の WQE table を順に取り出して callback。
  - CQE detail は `Option<CqeDetail>` で渡す。
  - signaled WQE のみ Some、非 signaled は None。
- 対象: QP(SQ), DCI

#### 2) Ordered + Sparse
- 目的: signaled のみ登録してテーブル更新コストを下げる。
- 実装:
  - signaled の WQE のみ WQE table に登録。
  - CQE 受信で `current - last_cqe_counter` の差分を使い、WQE バッファ上の `sq_ci` を進める。
  - CQE で得た `wqe_index` のみに user_data が存在する想定。
- 注意点:
  - user_data が無い範囲も `sq_ci` と inflight 更新が必要。
- 対象: QP(SQ), DCI

#### 3) Unordered + SendQueue
- 目的: 順序保証なしのキューでの完了を個別処理する。
- 実装:
  - 原則すべて signaled (CQE なしだと完了検知不能)。
  - WQE table は `wqe_index` 単位で完了処理。
  - 空き確認は WQE table の `Option` スロットで管理する(補助構造は持たない)。
  - 基本は Ordered と同様の楽観的空き容量を利用し、厳密な必要時のみフルスキャンする。
- 対象: DCI + Stream

#### 4) Recv Queue + Ordered
- 目的: すべて signaled の受信キューで順序保証を活かす。
- 実装:
  - Unordered と同様の table だが、`wqe_counter` 差分で厳密な空き容量も算出可。
- 対象: QP(RQ), SRQ

#### 5) Recv Queue + Unordered
- 目的: TM-SRQ の tag 非順序を扱う。
- 実装:
  - Unordered と同様に個別完了。
  - 厳密な空き容量は提供不可。
- 対象: TM-SRQ

## WQE table の設計
### テーブルエントリ
```
struct WqeEntry<T> {
    user_data: T,
    gen: u32,      // 再利用時の世代管理
    state: State,  // Free/InFlight/Completed など
}
```
- Ordered + Dense: `state` と `gen` は省略可能。
- Unordered: `Option<WqeEntry<T>>` で空き管理し、`gen` で ABA 的な再利用衝突を防ぐ。

### リングサイズ
- 2 の冪に固定し `wqe_index = wqe_counter & (ring_size - 1)` を使用する。

### wrap 対応
- `wqe_counter` の幅はデバイス仕様に合わせ、キュー作成時に確定する。
- その情報は CQE パース層で扱いを閉じ込める。
- `last_cqe_counter` は `wrapping_sub` で差分計算する。

## SQ/RQ バッファ管理
- `sq_pi`/`sq_ci`/`rq_pi`/`rq_ci` は CQE 受信で進める。
- Ordered + Dense:
  - `sq_ci` は `current - last_cqe_counter` で進める。
- Ordered + Sparse:
  - user_data 参照は signaled のみだが `sq_ci` は同様に進める。
- Unordered:
  - `sq_ci` は CQE 単位で前進しない場合があるため、
    個別で空き状態を管理。

## CQE ポーリングと関連付け
- CQ は SQ/RQ と独立であり、CQE から対象キューを解決する必要がある。
- CQ 共有時は `intmap` で QP/SRQ を弱参照で保持し、CQE から lookup。
- CQ 独占時は lookup を省略できる。

## Builder パターン
- Builder は「WQE 構築」「ユーザデータ登録」「doorbell 書き込み」の境界を分離する。
- Builder 側で Queue 種別(Ordered/Unordered, Dense/Sparse)を型で確定。
- 目標: コンパイル時に最適な管理方式が選択される。

## API の方向性
- QP 抽象は SQ/RQ を内包するため扱いが重い。
- 方向性: SQ/RQ/SRQ/DCI/TM-SRQ を個別型で表現し、
  Rc/所有権でライフタイムを表現する。

## エラー処理
- CQE error を検出した場合も `wqe_counter` は進む前提で扱う。
- 自動リトライは行わない。
- error CQE でも user_data と CQE detail を返し、該当エントリは完了として削除する。

## スレッドモデル
- CQ ポーラは単一スレッドを基本にする。
- キューへの post は複数スレッドを許す場合、
  WQE table と pi/ci の同期方式を設計する必要がある。

## 未決事項
- DCI Stream/TM-SRQ の partial order を活かす最適化有無。
- signaled 間引きの方針(固定間隔か、ユーザ指定か)。
- WQE table のエントリサイズ最適化(Inline user_data か、pointer 参照か)。
