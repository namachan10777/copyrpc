# copyrpc 設計・実装メモ

ソースコード (`copyrpc/`) の調査に基づく論文用メモ。

## 全体アーキテクチャ

RDMA WRITE with Immediate Dataを中心としたRPCフレームワーク。
SPSCリングバッファ上のバッチ転送、クレジットベースフロー制御、遅延バッチングにより高スループット・低レイテンシを実現。

```
Sender                              Receiver
  |                                   |
  | call()/reply() x N               |
  |   -> local send_ring に書き込み   |
  |                                   |
  | poll()                            |
  |   -> N個のメッセージをバッチ化     |
  |   -> 1回の RDMA WRITE+IMM ------>|
  |      [flow_meta + msg1..msgN]     |
  |      IMM = delta/32              |
  |                                   | recv_cq CQE
  |                                   | -> process_cqe()
  |                                   |    -> remote_consumer更新 (piggyback)
  |                                   |    -> peer_credit_balance加算
  |                                   |    -> request -> recv_stack
  |                                   |    -> response -> on_response callback
```

## ファイル構成

| ファイル | 役割 |
|---------|------|
| `src/lib.rs` (~700行) | Context, ContextBuilder, EndpointInner, Endpoint, RecvHandle, poll(), process_cqe() |
| `src/ring.rs` (~260行) | RingBufferState, RingBuffer, RemoteRingInfo, RemoteConsumer |
| `src/encoding.rs` (~230行) | ワイヤフォーマット: IMM encoding, ヘッダ, flow metadata, アラインメント |
| `src/error.rs` (~80行) | Error, CallError<U> |
| `src/bin/copyrpc_bench.rs` (~400行) | MPIベースのノード間ベンチマーク |

## コアデータ構造

### Context

全エンドポイントを管理する中央構造体。

- `srq`: 全エンドポイント共有のShared Receive Queue。受信はWRITE経由でリングに書き込まれるため、SRQには0長SGEをポストする
- `send_cq`: 送信側CQ（通常のCq）。WRITE+IMM送信完了（無視）とRDMA READ完了（remote consumer更新）を処理
- `recv_cq`: 受信側CQ（MonoCq）。WRITE+IMM到着時のCQEをここで受信。inlinedコールバックでvtableオーバーヘッドなし
- `cqe_buffer` / `send_cqe_buffer`: CQコールバックとpoll()処理の間のデカップリング用バッファ
- `endpoints`: FastMap<QPN → EndpointInner>。QPNをキーとしたO(1)ルックアップ（ハッシュなし）

### EndpointInner

各RPC接続の内部状態。

**リングバッファ**:
- `send_ring` / `recv_ring`: Box<[u8]>。サイズは2のべき乗
- 全てのポジションは仮想的な単調増加u64。実際のオフセットは `pos & (ring_size - 1)` で計算
- `send_ring_producer`: 次に書き込むバイト位置
- `recv_ring_producer`: CQEのIMMデルタから更新
- `last_recv_pos`: パース済み位置（コンシューマ位置）

**リモートコンシューマ追跡（2つの機構）**:
1. `remote_consumer: RemoteConsumer`: キャッシュ値。受信バッチのflow_metadataからのpiggback、またはRDMA READの結果で更新
2. RDMA READ基盤: `consumer_position` (AtomicU64, REMOTE_READ権限) を公開し、相手が直接READできる。piggbackが古くなった場合のフォールバック

**バッチング状態**:
- `flush_start_pos`: 未送信データの開始位置
- `meta_pos`: 現バッチのflow_metadataプレースホルダ位置（Noneならバッチ未開始）
- `batch_message_count`: 現バッチ内のメッセージ数

**フロー制御状態**:
- `resp_reservation` (R): 発行済みクレジット - 書き込み済みレスポンスのバイト数
- `max_resp_reservation`: Rの上限（デフォルト: send_ring_size / 4）
- `peer_credit_balance`: 相手から付与されたクレジット（リクエスト送信に使用）

**WQEシグナリング**:
- `unsignaled_count`: SIGNAL_INTERVAL (64) WQEごとにシグナル。SQ枯渇防止

**ペンディングコール**:
- `pending_calls: Slab<U>`: slabインデックス = call_id

## ワイヤフォーマット

### 定数

| 定数 | 値 | 意味 |
|-----|---|------|
| `ALIGNMENT` | 32 B | 全メッセージ・ポジションは32Bの倍数 |
| `HEADER_SIZE` | 12 B | メッセージヘッダ |
| `FLOW_METADATA_SIZE` | 32 B | バッチメタデータ |
| `RESPONSE_FLAG` | `0x8000_0000` | call_idのMSBでレスポンスを識別 |
| `WRAP_MESSAGE_COUNT` | `u32::MAX` | リングラップのセンチネル値 |

### Immediate Value (32bit)

```
encode_imm(delta) = (delta / 32) as u32
decode_imm(imm) = (imm as u64) * 32
```

バッチ全体のバイト数をエンコード。最大128GB (32 * 2^32) を表現可能。
受信側はこれで`recv_ring_producer`を進める。

### バッチレイアウト（recv ring上）

1回のRDMA WRITE+IMMで以下が書き込まれる:

```
+---------------------------+
| Flow Metadata (32B)       |
|   consumer_pos  (8B, LE)  |  送信側のrecv ring消費位置（piggyback）
|   credit_grant  (8B, LE)  |  相手に付与するクレジット（バイト数）
|   message_count (4B, LE)  |  後続メッセージ数
|   padding       (12B)     |
+---------------------------+
| Message 1                 |
|   call_id    (4B, LE)     |  MSB=0: request, MSB=1: response
|   piggyback  (4B, LE)     |  request: response_allowance_blocks (32B単位)
|   len        (4B, LE)     |  payload長
|   payload    (len B)      |
|   padding to 32B          |
+---------------------------+
| Message 2 ...             |
+---------------------------+
```

### パディング

```
padded_message_size(payload_len) = ceil((12 + payload_len) / 32) * 32
```

- 0B payload → 32B
- 20B payload → 32B
- 21B payload → 64B
- 52B payload → 64B

## フロー制御

### 基本不変条件

```
in_flight + 2 * R <= C
```

- `in_flight = send_ring_producer - remote_consumer`（転送中バイト数）
- `R = resp_reservation`（レスポンス予約バイト数）
- `C = remote_recv_ring.size`（リモートrecv ringの容量）

**2の係数の理由**: 双方が同時にレスポンス予約を持つ可能性がある。各側のRが受信側の使用済み空間に寄与するため、2*Rの予約が必要。

### クレジット付与の計算

```
max_by_invariant = (C - in_flight) / 2 - current_R
max_by_policy = max_resp_reservation - current_R
credit_grant = min(max_by_invariant, max_by_policy) & !(ALIGNMENT - 1)
```

バッチ確定時（fill_metadata()）に計算し、flow_metadataのcredit_grantフィールドに書き込む。
ローカルの`resp_reservation`は即座に増加。

### クレジットライフサイクル

**接続時**:
```
resp_reservation = min(max_resp_reservation, remote_ring_size / 4)
peer_credit_balance = remote.initial_credit
```
初期ハンドシェイク不要。情報交換だけで相互クレジットが成立。

**リクエスト送信時 (call())**:
```
internal_allowance = align_up(padded_message_size(response_allowance) + FLOW_METADATA_SIZE, 32)
if peer_credit_balance < internal_allowance → InsufficientCredit
peer_credit_balance -= internal_allowance
```
internal_allowanceにFLOW_METADATA_SIZE (32B)を含めるのは、レスポンダがwrap markerを出す最悪ケースに備えるため。

**バッチ受信時 (process_cqe())**:
```
remote_consumer.update(consumer_pos)   // piggyback
peer_credit_balance += credit_grant     // 新規クレジット
```

**レスポンス送信時 (reply())**:
```
resp_reservation -= response_allowance  // 予約解放
```

### デッドロック自由性

reply()にはリングフルチェックがない。これは安全:
1. call()時にpeer_credit_balanceからinternal_allowanceを確保済み
2. 受信側のresp_reservationがcredit_grantで増加済み
3. 不変条件 `in_flight + 2R <= C` により、Rバイトのレスポンス分は常に空きがある

## バッチング

### 遅延送信モデル

call() / reply()は即座にRDMA送信しない。ローカルsend_ringに書き込み、send_ring_producerを更新するのみ。
実際のRDMA送信はpoll()内のflush()で行われる。複数メッセージを1回のRDMA WRITE+IMMに集約。

### バッチライフサイクル

1. **開始 (ensure_metadata())**: meta_posがNoneなら、現在位置に32Bのプレースホルダを予約
2. **メッセージ追加**: call()/reply()がヘッダ+ペイロード+パディングを書き込み、batch_message_countをインクリメント
3. **確定・送信 (fill_metadata() + emit_wqe())**: poll()時にメタデータを確定し、WQEを発行

### リングラップ処理

handle_wrap_if_needed(total_size):
```
if offset + total_size < ring_size → ラップ不要
else:
  1. 現バッチを確定・送信
  2. wrap marker（message_count = WRAP_MESSAGE_COUNT）を発行
  3. producerをリング境界を越えた位置に進める
```

offset + total_size の比較は strict `<`。境界ちょうどのケースもラップ扱いにして、バッチのSGEがリング境界を跨がないようにする。

wrap markerにもcredit_grantが含まれるため、クレジット情報が失われることはない。

## CQポーリング設計

### poll()の6ステップ

1. **recv_cq (MonoCq) のポーリング**: CQEを読み、コールバック経由でcqe_bufferに蓄積
2. **受信CQE処理**: cqe_bufferをdrainし、各CQEに対してprocess_cqe()を実行
3. **SRQ再ポスト**: posted数が最大値の2/3を下回ったら一括再ポスト（バッチ化によるオーバーヘッド削減）
4. **send_cqのポーリング**: 送信完了CQEをsend_cqe_bufferに蓄積
5. **送信CQE処理**: READ完了（is_read=true）のみ処理。WRITE+IMM送信完了は無視（SQ CI進行のみ）
6. **全エンドポイントのflush + READ発行**: 未送信データをflush。needs_read()またはforce_readならRDMA READを発行

### MonoCq (mlx5/src/mono_cq.rs)

- CQハードウェアバッファに直接アクセス
- owner bitチェック → メモリバリア → Cqe構築
- dispatch_cqe(): FastMapでQPN→QPルックアップ → process_cqe()でEntry抽出 → inlinedコールバック
- プリフェッチ: 2番目以降のCQEをprefetchしてからprocessing
- 圧縮CQE (format=3) にも対応

### イベント集約の論文的意義

従来研究ではrecv buffer上のvalidフラグをポーリングする設計が提案されてきた。
本実装では複数QPのCQEを1つのCQに集約し、MonoCqでowner bitをチェックするだけで全QPの受信を検出。
ibverbsのspin lockを排除した直接アクセスにより、CQポーリングはmemory pollingと同等以上の性能。

ポーリング箇所が1つに集約されるため、QP数に対してポーリングコストがスケールしない。
NICのDDIOによりCQEはL3に直接書き込まれ、L3からの読み出しのみで完結。

## 接続セットアップ

### エンドポイント生成

1. send_ring / recv_ring を Box<[u8]> で確保（2のべき乗にラウンドアップ）
2. メモリ登録:
   - send_ring_mr: LOCAL_WRITE | REMOTE_WRITE
   - recv_ring_mr: LOCAL_WRITE | REMOTE_WRITE
   - consumer_position_mr: LOCAL_WRITE | REMOTE_READ (8B, 相手がREADする用)
   - read_buffer_mr: LOCAL_WRITE (8B, こちらがREADする先)
3. QP生成: RC QP with SRQ, send_cq(sq_callback付き), recv_cq(MonoCq)
4. FastMapにQPNで登録

### 情報交換

LocalEndpointInfo / RemoteEndpointInfo をout-of-band交換（MPI, mpsc等）。
含まれる情報: QPN, PSN, LID, recv_ring(addr, rkey, size), consumer_position(addr, rkey), initial_credit

### 初期クレジット設計

両側が `initial_credit = min(max_resp_reservation, ring_size / 4)` で開始。
接続情報の交換だけで相互クレジットが成立し、追加のハンドシェイクは不要。

## mlx5クレートとの統合

### QP型

```
CopyrpcQp = RcQpForMonoCqWithSrqAndSqCb<SrqEntry, SqCqCallback>
```

- RC (Reliable Connection) QP
- InfiniBandトランスポート
- OrderedWqeTable<SrqEntry>: SQ完了追跡
- SharedRq<SrqEntry>: SRQによる受信
- SqCqCallback: 送信CQコールバック（Box dyn）

### WQE発行

`emit_wqe!`マクロで以下を構築:

- **WRITE+IMM**: Control Seg (16B) + RDMA Seg (16B) + Data Seg (16B) = 48B (1 WQEBB)
- **READ**: 同構造、48B

unsignaled/signaled切り替え。SQリングラップ時はNOP WQEで埋めてから発行。

### ドアベル

`ring_sq_doorbell_bf()`: BlueFlameで全WQEを書き込み。低レイテンシ。

## エラーハンドリング

- `RingFull`: poll()後にリトライ可能
- `InsufficientCredit`: poll()後にリトライ可能（新規credit_grantを受信するため）
- CQEエラー: stderrにログ出力、スキップ

## 定数一覧

| 定数 | 値 | 意味 |
|-----|---|------|
| DEFAULT_RING_SIZE | 1MB (1 << 20) | デフォルトリングサイズ |
| DEFAULT_SRQ_SIZE | 1024 | デフォルトSRQ容量 |
| DEFAULT_CQ_SIZE | 4096 | デフォルトCQエントリ数 |
| SIGNAL_INTERVAL | 64 | WQEシグナリング間隔 |
| ALIGNMENT | 32 B | アラインメント |
| HEADER_SIZE | 12 B | メッセージヘッダ |
| FLOW_METADATA_SIZE | 32 B | バッチメタデータ |
| max_resp_reservation | ring_size / 4 | レスポンスクレジット上限 |
| max_send_wr | 256 | QP送信キュー深度 |
| max_rd_atomic | 4 | RDMA READ/Atomic同時実行数 |

## リクエストのIn-Order取り込みとレスポンスのOut-of-Order送信

### リクエスト取り込みはIn-Order

`process_cqe()`はバッチ内のメッセージを先頭から順に処理する（`for _ in 0..message_count` の逐次ループ）。
RC (Reliable Connection) トランスポートはCQEの配信順序を保証するため、異なるバッチ間でも順序が維持される。

この順序保証がフロー制御を成立させている:

1. **flow_metadataの逐次処理**: バッチ先頭の`flow_metadata`（`consumer_pos`, `credit_grant`）がメッセージ処理の前に読まれる。これにより、クレジット更新が後続メッセージの処理に先行する
2. **recv_ring_producerの単調増加**: CQEのIMMデルタで`recv_ring_producer`を進める処理は、バッチ到着順に実行される。In-Orderでなければリング位置の整合性が崩れる
3. **wrap markerの正しい処理**: `WRAP_MESSAGE_COUNT`センチネルはバッチ境界で発生し、In-Order処理でなければリングのラップ位置を正しく追跡できない

```rust
// lib.rs process_cqe() - 逐次処理
let flow = FlowMetadata::read(batch_ptr);
endpoint.remote_consumer.update(flow.consumer_pos);
endpoint.peer_credit_balance += flow.credit_grant;

for _ in 0..message_count {
    let header = Header::read(cursor);
    // request → recv_stack, response → on_response callback
}
```

### レスポンスはOut-of-Order

`RecvHandle::reply()`は任意のタイミングで呼び出せる。順序制約はない。

**コード上の根拠**:
- `RecvHandle`は`call_id`と`response_allowance`を保持するが、順序情報（シーケンス番号等）は持たない
- `reply()`はsend_ringに書き込むだけで、他のRecvHandleの状態を参照しない
- 受信側では`to_response_id(call_id)`（`call_id | RESPONSE_FLAG`）で元のcall_idを特定し、`pending_calls.try_remove(original_call_id)`でSlab上のエントリを直接削除する。Slabはインデックスアクセスなので順序無関係

```rust
// RecvHandle::reply() - 順序制約なし
pub fn reply(&self, endpoint: &mut EndpointInner<U>, payload: &[u8]) {
    let call_id = to_response_id(self.call_id);
    // send_ringに書き込むだけ。他のRecvHandleと独立
    endpoint.write_message(call_id, self.response_allowance as u32, payload);
    endpoint.resp_reservation -= self.response_allowance;
}

// 受信側 process_cqe() - call_idで直接ルックアップ
let original_call_id = from_response_id(header.call_id);
if let Some(user_data) = endpoint.pending_calls.try_remove(original_call_id as usize) {
    on_response(payload, user_data);
}
```

### 設計上の意図

- **In-Order取り込み**: フロー制御メタデータ（consumer_pos, credit_grant）のpiggybackが正しく機能するために必須。順序を崩すとクレジット計算が破綻する
- **Out-of-Orderレスポンス**: IOキューとしての自由度を確保。サーバ側が複数リクエストを受け取った後、処理完了順にレスポンスを返せる。NVMe SQのようなセマンティクス
- **デッドロック自由性との関係**: reply()にリングフルチェックがないのは、call()時にcredit予約済みだから。Out-of-Orderでもこの不変条件は保たれる（予約はcall_id単位、順序無関係）

## mlx5マイクロアーキテクチャレベル最適化

ibverbsをバイパスし、mlx5 NICハードウェアに直接アクセスすることで実現するマイクロアーキレベルの最適化群。

### BlueFlame (BF) 最適化

通常のドアベルモデルでは、WQEをホストメモリに書き込んだ後、ドアベルレジスタにProducer Indexを書き込んでNICに通知し、NICがDMAでWQEを読む。BlueFlameはWQEデータ自体をNICのBFレジスタにMMIO書き込みし、DMA読み取り1往復分（~200-400ns）を削減する。

**3つのドアベルモード**:

1. **ring_doorbell()**: BFレジスタにCtrl Segの先頭8Bのみ書き込み。NICはここからopcode/QPN/WQE indexを読み、残りはDMA。最小レイテンシ削減
2. **ring_doorbell_bf()**: BFレジスタにWQE全体（最大bf_size=64-256B）を`mlx5_bf_copy!`で書き込み。NICのDMA読み取りが完全に不要。最大削減~400-800ns
3. **BlueframeBatch**: スタック上の256Bバッファに複数WQEを蓄積し、`finish()`で1回のBF書き込みに集約。ドアベル固定コスト（sfence + MMIO）を複数WQEで償却

**BFダブルバッファリング** (`wqe/emit.rs`):
```rust
self.bf_offset.set(bf_offset ^ self.bf_size);
```
連続するドアベルが異なる物理アドレスに書き込まれ、PCIe Write Combiningによる合併を防止。

**mlx5_bf_copy!マクロ** (`barrier.rs`):
- x86: `_mm512_stream_si512` (AVX-512非テンポラルストア) で64Bを1命令で書き込み。キャッシュ汚染なし、1 PCIeトランザクションで完結
- ARM: `stnp` (Store Non-Temporal Pair) で32Bずつ×2回

**メモリバリア**:
- `mmio_flush_writes!()`: x86では`sfence`。WCバッファをフラッシュし、SQ書き込みがメモリに到達してからドアベルが見えるよう保証
- `udma_to_device_barrier!()`: x86ではTSOにより無操作。ARM64では`dmb oshst`

### Inline最適化

ペイロードをWQE内に直接埋め込み、NICがホストメモリからペイロードをDMA読み取りする往復を削減。

**Inlineヘッダーフォーマット** (`wqe/mod.rs`):
```
[bit31=1(inline flag)][bit30-0=byte_count]
[payload...][padding to 16B boundary]
```

**WQEレイアウト比較**:
```
通常(SGE):  [Ctrl:16B][DataSeg:16B → DMAでペイロード取得]  合計48B + DMA
Inline:     [Ctrl:16B][Header:4B][Payload:N][Pad]          合計=16+ceil((4+N)/16)*16, DMA不要
```

デフォルト `max_inline_data=64B`。SEND WQEなら1 WQEBB (64B) に収まるサイズ。

**ハイブリッドモード（Inline + SGE）**: WRITE_IMMでは、Inlineにヘッダ（小サイズ）、SGEにボディ（大サイズ）を分離可能:
```
[Ctrl:16B][RDMA:16B][InlineHeader:4B][HeaderPayload:N][Pad][DataSeg:16B]
```

**BlueFlame + Inline併用**: BlueframeBatchのスタックバッファにInlineデータを構築 → `finish()`でWQE+ペイロードが単一MMIO書き込みで送信。NICのDMA読み取りが完全に不要。

### Scatter-to-CQE（受信側Inline）

`enable_scatter_to_cqe`フラグ有効時、小さい受信データ（<32B for 64B CQE）をCQE内に直接埋め込み。受信バッファへのDMA書き込みが不要。

CQEオフセット0-31に受信データが格納され、`op_own`フィールドのscatterフラグビットで検出。

## CQポーリングの本質: 受信イベント直列化のNICオフロード

### 問題の構造

複数QPから非同期に到着するメッセージを、CPUで処理するには直列化が必要。
直列化の方法は2つ:

1. **Memory polling**: 各QPの受信バッファにvalidフラグを設置し、CPUが全QPをラウンドロビンでポーリング。直列化はCPUが行う
2. **CQ polling**: NICが全QPの完了イベントを1つのCQに書き込む。直列化はNICが行う

Memory pollingではQP数に比例してポーリングコストが増加する。各QPバッファのキャッシュラインをタッチする必要があり、QP数が多いとL1/L2キャッシュを圧迫する。

CQ pollingの本質は、**受信イベントの直列化をCPUではなくNICにオフロードする**こと。NICは元々全QPの受信処理を行っており、CQEの書き込みは追加コストがほぼゼロ。一方CPUは、1つのCQバッファの先頭だけを監視すれば全QPの受信を検出できる。

### なぜibverbsではCQ pollingが不利に見えたか

従来の比較（HERD等）ではibverbs `ibv_poll_cq()`経由のCQ pollingが遅かった。しかしこれはibverbs実装の問題:

1. **spin lock**: ibverbsの`ibv_poll_cq()`は内部でスレッドセーフティのためにspin lockを取得。ポーリングのホットパスにロックが入る
2. **コピー**: CQEをハードウェアバッファから`ibv_wc`構造体にコピー。メモリ帯域幅を消費
3. **間接呼び出し**: `ibv_poll_cq()`は関数ポインタ経由のディスパッチ

これらのオーバーヘッドがCQ pollingの性能を歪めていた。

### MonoCqによる真のCQ polling性能

本実装ではibverbsをバイパスし、CQハードウェアバッファに直接アクセス:

1. **owner bitチェック** (`mono_cq.rs`): CQEの63バイト目の最下位ビットを`read_volatile`で読むだけ。ロックなし、コピーなし
2. **メモリバリア**: owner bit確認後、`udma_from_device_barrier!()`（x86ではcompiler fenceのみ、ハードウェアバリア不要）でCQEデータの可視性を保証
3. **DDIO**: NICはCQEをL3キャッシュに直接書き込む（Direct Data I/O）。CPUはL3からの読み出しのみで完結し、メインメモリアクセスは発生しない
4. **プリフェッチ**: 2番目以降のCQEに対して`prefetch_for_read!`を発行。dispatch_cqe()の処理時間で次のCQEのメモリレイテンシを隠蔽

**結果**: CQ pollingの1回あたりのコストは、実質的にowner bitのキャッシュライン読み出し1回。QP数に対してスケールせず、memory pollingと同等以上の性能を達成。

### MonoCqの単形化による最適化

`MonoCq<Q, F>`はジェネリック型パラメータにより、コンパイル時に全てのコールバックがinline化:

- `Q: CompletionSource` → `process_cqe()`がinline化。vtableオーバーヘッドゼロ
- `F: Fn(Cqe, Q::Entry)` → コールバックがinline化。関数ポインタ呼び出しなし
- `FastMap<Weak<Q>>` → QPNをキーとしたO(1)ルックアップ。ハッシュ計算なし

対照的にibverbsの`ibv_poll_cq()`は:
- `ibv_cq`→`mlx5_cq`のキャスト、関数ポインタ経由のディスパッチ
- `ibv_wc`構造体への全フィールドコピー
- QPNからアプリケーションコンテキストへのルックアップはアプリ側の責任（通常HashMap）

### 圧縮CQE対応

大量の完了を効率処理するための圧縮CQE (format=3) にも対応。1つの64B CQEに最大8個のミニCQE（各8B）を格納。`MiniCqeIterator`でイテレーション。CQ メモリ帯域幅を最大~87.5%削減。

## 論文での記述ポイント

1. **バッチングによるWQE消費削減**: call()/reply()は送信を遅延し、poll()で一括WRITE+IMM。WQE1個で複数メッセージを転送
2. **piggbackによるフロー制御メッセージの排除**: consumer_posとcredit_grantを全バッチに埋め込み、専用のACKメッセージが不要
3. **クレジット予約によるデッドロック自由性**: `in_flight + 2R <= C` の不変条件。reply()にリングフルチェックが不要な理由
4. **CQイベント集約**: 複数QPを1CQに集約。MonoCqでowner bitを直接チェック。QP数に対してポーリングコストがスケールしない
5. **ibverbsバイパス**: spin lock排除とコピー削減。CQ pollingがmemory pollingと同等以上の性能を発揮
6. **FastMapによるO(1)ディスパッチ**: QPNをキーとしたハッシュなしルックアップ。CQE処理の高速化
7. **SRQ共有**: 全エンドポイントで1つのSRQ。0長SGE（データはWRITEでリングに到着するため）
8. **RDMA READフォールバック**: piggbackが古くなった場合（RingFull時等）にremote consumerをREADで直接取得
9. **In-Order取り込み / Out-of-Orderレスポンス**: フロー制御のためにリクエストはIn-Orderで処理しつつ、IOキューの自由度のためにレスポンスはOut-of-Order。NVMe SQライクなセマンティクス
10. **BlueFlame最適化**: 3モード（最小8B/フルWQE/バッチ）。AVX-512非テンポラルストアでキャッシュ汚染なしの64B MMIO書き込み。ダブルバッファリングでPCIe WC合併を防止
11. **Inline最適化**: ペイロードをWQE内に埋め込みDMA削減。BF+Inlineの併用でWQE+ペイロードが単一MMIO書き込みで完結
12. **CQポーリングの本質**: 受信イベントの直列化をCPUからNICにオフロード。ibverbsのspin lock/コピー/間接呼び出しが従来のCQ polling vs memory polling比較を歪めていた。MonoCqの直接アクセスにより真の性能を回復
