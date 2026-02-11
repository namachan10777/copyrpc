# Delegation Architecture: Technical Reference

本文書は copyrpc ベースの分散KVベンチマーク (benchkv) における **Delegation バックエンド** のアーキテクチャ、データフロー、および構成要素のアルゴリズムを網羅的に記述する。

---

## 1. アーキテクチャ概要

Delegation バックエンドは、リモートリクエストの送信パスを短縮するために設計された。
従来の Meta バックエンドと比較し、ipc + Flux の2段委任を除去し、クライアントが共有メモリ MPSC リングに直接書き込む。

### 1.1 Meta バックエンド (従来)

**リモート送信パス** (3ホップ):
Client が ipc でローカルの Daemon#N に送信し、Daemon#N が Flux で Daemon#0 に転送し、Daemon#0 が copyrpc 経由で RDMA 送信する。

**リモート受信パス**:
リモートノードから RDMA で到着したリクエストを copyrpc が Daemon#0 で受信し、Flux で対象シャードの Daemon#N に転送する。Daemon#N が KV 操作を実行し、Flux で Daemon#0 に応答を返し、Daemon#0 が copyrpc reply で RDMA 応答する。

### 1.2 Delegation バックエンド (新)

**リモート送信パス** (1ホップ):
Client が delegationrpc の共有 MPSC リングに fetch_add で直接書き込み、Daemon#0 が copyrpc 経由で RDMA 送信する。ipc と Flux を完全にバイパスする。

**リモート受信パス**:
Meta と同一。リモートから copyrpc で受信後、Flux 経由で対象 Daemon#N に転送して処理する。

**ローカルパス**:
Meta と同一。Client が ipc でキーのシャードオーナーである Daemon#N に直接送信し、Daemon#N が KV 操作を実行して ipc で応答する。

### 1.3 ノード内スレッド構成

各 MPI rank (ノード) は以下のスレッドで構成される:

**Daemon#0** は 4 つのサブシステムを保持する:
- **copyrpc Context**: 全リモートrank との RDMA エンドポイント (N-1 個) を管理する。RDMA WRITE+IMM による双方向メッセージング。
- **delegationrpc Server**: `/dev/shm` 上の共有 MPSC リング。全クライアントスレッドがリモートリクエストをここに書き込む。
- **ipc Server**: シャード#0 用のローカル RPC。per-client SPSC リングベース。
- **Flux ノード**: Daemon#1..K との inter-daemon 通信チャネル。受信リモートリクエストのシャード転送に使用。

**Daemon#1..K** (ワーカー) は 2 つのサブシステムのみ:
- **ipc Server**: 各ワーカーが担当するシャード用。
- **Flux ノード**: Daemon#0 からの転送リクエストを受信し、応答を返す。

**Client Thread #0..C** は 2 つの通信パスを持つ:
- **ipc Client [0..K-1]**: 各 Daemon への接続 (K 個)。ローカルリクエストに使用。
- **delegationrpc Client**: Daemon#0 の共有リングへの書き込み。リモートリクエストに使用。

### 1.4 copyrpc エンドポイント配置の違い

|            | Meta                            | Delegation                       |
| ---------- | ------------------------------- | -------------------------------- |
| Daemon#d   | `rank % D == d` のリモートrankを担当 | Daemon#0 のみ: 全リモートrank        |
| EP 数/rank | D 個の daemon に分散              | Daemon#0 に N-1 個集約             |
| MPI 交換   | 全 daemon 参加                   | Daemon#0 のみ参加                  |

Meta では copyrpc エンドポイントが `rank % D == d` のルールで各 daemon に分散される。Delegation では Daemon#0 が全 N-1 個のリモートランクのエンドポイントを保持する。これにより MPI 交換が簡素化されるが、Daemon#0 が RDMA のボトルネックになる可能性がある (NIC の高スループットで緩和される)。

---

## 2. delegationrpc プロトコル

delegationrpc は RDMA 非依存の共有メモリ MPSC リング + per-client レスポンススロットで構成される。

### 2.1 共有メモリレイアウト

全領域は `/dev/shm` 上に配置される。magic は `0x444C_4752_5043_5631` ("DLGRPCV1")。

全体構成は 4 つの領域からなる。先頭に 128B の Header、続いて 128B の Ring Control、その後に `ring_depth` 個の Request Ring Slot、最後に `max_clients * resp_depth` 個の Response Slot が並ぶ。

#### Header (128 バイト, オフセット 0)

Header は 2 キャッシュライン分 (128B) を占有する。

| オフセット | サイズ | 型 | フィールド | 初期値 |
| --- | --- | --- | --- | --- |
| 0 | 8B | u64 | magic | `0x444C_4752_5043_5631` |
| 8 | 4B | u32 | version | 1 |
| 12 | 4B | u32 | max_clients | 作成時指定 |
| 16 | 4B | u32 | ring_depth | 作成時指定 (2のべき乗) |
| 20 | 4B | u32 | resp_depth | 作成時指定 (2のべき乗) |
| 24 | 4B | AtomicU32 | next_client_id | 0 |
| 28 | 1B | AtomicBool | server_alive | true |
| 29-127 | 99B | - | padding | - |

#### Ring Control (128 バイト, オフセット 128)

head と tail を別キャッシュラインに配置し、false sharing を防止する。

| オフセット | サイズ | 型 | フィールド | 書き込み者 | 読み取り者 |
| --- | --- | --- | --- | --- | --- |
| 128 | 8B | AtomicU64 | head | Client (fetch_add) | Server (poll) |
| 136-191 | 56B | - | padding | - | - |
| 192 | 8B | AtomicU64 | tail | Server (advance_tail) | Client (call) |
| 200-255 | 56B | - | padding | - | - |

**head**: クライアントが `fetch_add(1, Relaxed)` でインクリメントする。論理的なスロット予約位置を表す。

**tail**: サーバが `store(cursor, Release)` で更新する。消費済み位置を示し、クライアントのフロー制御スピンループを解除する。

#### Request Ring Slot (各 64B アラインメント, オフセット 256 以降)

各スロットは `align_up(16 + size_of::<Req>(), 64)` バイト。

| スロット内オフセット | サイズ | 型 | フィールド | 説明 |
| --- | --- | --- | --- | --- |
| +0 | 1B | AtomicU8 | committed | 0=未コミット, 1=コミット済み |
| +1 | 3B | - | padding | - |
| +4 | 4B | u32 | client_id | クライアント識別子 |
| +8 | 4B | u32 | resp_slot_idx | レスポンススロット番号 |
| +12 | 4B | - | padding | - |
| +16 | ?B | Req | payload | リクエストデータ (Copy型) |

物理インデックスは `pos & (ring_depth - 1)` で計算する (ring_depth は 2 のべき乗)。

#### Response Slot (各 64B アラインメント)

各スロットは `align_up(8 + size_of::<Resp>(), 64)` バイト。

| スロット内オフセット | サイズ | 型 | フィールド | 説明 |
| --- | --- | --- | --- | --- |
| +0 | 1B | AtomicU8 | valid | 0=未到着, 1=到着 |
| +1 | 7B | - | padding | - |
| +8 | ?B | Resp | payload | レスポンスデータ (Copy型) |

レスポンス領域は Request Ring の直後に配置される。各クライアントは `resp_depth` 個の専用スロットを持つ。スロットアドレスは `resp_area_offset + (client_id * resp_depth + slot_idx) * resp_slot_size` で計算される。

### 2.2 MPSC リングプロトコル

#### Client call() — 2-phase commit

1. `server_alive.load(Acquire)` でサーバ生存確認。false なら `CallError::ServerDisconnected` を返す。

2. レスポンススロットを round-robin で割り当てる: `resp_slot_idx = next_resp_slot; next_resp_slot = (next_resp_slot + 1) & resp_mask`。

3. `pos = head.fetch_add(1, Relaxed)` でスロット位置を確保する。fetch_add は Relaxed で十分。この時点ではデータ可視性の同期は不要で、ユニークな位置の確保のみが目的。

4. tail ベースのフロー制御: `pos.wrapping_sub(tail.load(Acquire)) < ring_depth` が成立するまでスピンする。リングが満杯の場合、サーバが advance_tail() で tail を進めるまで待機する。

5. スロットに `write_volatile` で `client_id`, `resp_slot_idx`, `payload` を書き込む。

6. `fence(Release)` で全書き込みをフラッシュし、`committed.store(1, Release)` でサーバにコミットを通知する。

7. `in_flight` カウンタをインクリメントする。

**メモリオーダリングの保証**: `fence(Release)` + `store(Release)` により、committed=1 が可視になった時点で payload の書き込みは必ず完了している。サーバの `committed.load(Acquire)` とリリース-アクワイアペアを形成する。

#### Server poll() — sequential scan

1. `head = ring_head.load(Acquire)` でスナップショットを取得し、`cached_head` に保存する。

2. `cursor` から `head` まで順にスロットを走査する。各スロットで `committed.load(Acquire)` をチェックし、committed=0 (ホール) を検出したら走査を停止する。committed=1 のスロットをカウントする。

3. 連続してコミットされたエントリの数を返す。

**ホール処理**: クライアント A が pos=N を確保し、クライアント B が pos=N+1 を確保した場合、B が先にコミットしても、サーバは pos=N (ホール) で停止する。次回の poll() で A がコミットすると、N と N+1 の両方が処理される。

#### Server recv() — entry 取り出し

1. `cursor >= cached_head` なら `None` を返す。

2. 現在の cursor 位置のスロットで `committed.load(Acquire)` をチェックする。0 (ホール) なら `None` を返す。

3. `read_volatile` で `client_id`, `resp_slot_idx`, `payload` を読み取る。

4. `committed.store(0, Release)` でスロットを再利用可能にし、`cursor` をインクリメントする。

5. `RecvEntry { client_id, resp_slot_idx, payload }` を返す。

#### Server advance_tail() — フロー制御解放

`tail.store(cursor, Release)` で現在の cursor 位置を tail に反映する。これによりクライアントのスピンループが解除され、新たなスロットへの書き込みが可能になる。

#### Server reply() — レスポンス直接書き込み

1. `resp_slot_ptr(client_id, resp_slot_idx)` でスロットアドレスを計算する。

2. `write_volatile(resp_payload, resp)` でレスポンスデータを書き込む。

3. `fence(Release)` で可視化した後、`valid.store(1, Release)` でクライアントに通知する。

#### Client poll() — レスポンス読み取り

`resp_depth` 個の全スロットを走査する。各スロットで `valid.load(Acquire)` をチェックし、valid != 0 なら `read_volatile` でレスポンスを読み取り、`valid.store(0, Release)` でスロットを解放する。`in_flight` をデクリメントし、`on_response` コールバックを呼び出す。

### 2.3 ipc との比較

| 側面 | delegationrpc | ipc |
| --- | --- | --- |
| リング型 | 共有 MPSC リング (1本) | per-client SPSC リング (N本) |
| スロット確保 | 全 client が head に fetch_add | 各 client が自分の ring に fetch_add |
| サーバスキャン | 1本のリングを走査 | 全 SPSC リングを flat combining |
| レスポンス | per-client round-robin スロット | per-client SPSC レスポンスリング |
| バックプレッシャ | tail ベース (client spin) | ring ベース (per-client) |
| head 競合 | 全 client が 1 本に競合 | 分散 (SPSC なので競合なし) |
| スケーラビリティ | < 8 client で最良 | client 数に依存しにくい |
| メモリ使用量 | コンパクト (1リング) | client * ring_depth 分 |

---

## 3. copyrpc アルゴリズム

copyrpc は RDMA (InfiniBand) を用いた低遅延 RPC フレームワーク。各エンドポイントは 1-to-1 の RC (Reliable Connected) QP で接続される。

### 3.1 アーキテクチャ

**Context** は copyrpc の中心構造体で、以下のコンポーネントを管理する:

- **Protection Domain (PD)**: 全エンドポイントで共有される RDMA 保護ドメイン。
- **Send CQ (Standard CQ)**: 送信完了の追跡に使用。signaled WQE の完了を検出する。
- **Recv CQ (MonoCq)**: 受信 CQE のポーリング用。インラインコールバック方式で CQE を即座に処理する。
- **Shared SRQ (Shared Receive Queue)**: 全 QP で共有。事前に recv バッファを post しておき、任意の QP からの WRITE+IMM を受信可能にする。
- **Endpoint Registry (FastMap<QPN, Endpoint>)**: QP 番号からエンドポイントへの O(1) ルックアップ。CQE 処理時に QPN で対象エンドポイントを特定する。

各 Endpoint は 1 つの RC QP を保有し、リモートの対応するエンドポイントと 1-to-1 で接続される。

### 3.2 リングバッファ管理

各エンドポイントは **send ring** と **recv ring** を持つ。いずれも `Box<[u8]>` で確保され、RDMA Memory Region として登録済み。デフォルトサイズは各 1MB。

#### 仮想位置 (Virtual Position)

全リング操作は **仮想位置** (単調増加する u64) で管理する。物理オフセットは `virtual_pos & (ring_size - 1)` で計算される (ring_size は 2 のべき乗)。仮想位置を使うことで wrap-around 検出が不要になり、デルタ計算 (end - start) が自然に行える。

#### Send Ring の状態変数

- **send_ring_producer**: 次の書き込み位置 (仮想)。call() / reply() でメッセージを書き込むたびに進む。
- **flush_start_pos**: 未送信データの開始位置 (仮想)。emit_raw() で RDMA WRITE を発行すると producer まで進む。
- **meta_pos**: 現在のバッチのフロー制御メタデータが配置される位置 (仮想)。prepare_next_batch() で予約される。
- **batch_message_count**: 現バッチ内のメッセージ数。

不変条件: `flush_start_pos <= meta_pos <= send_ring_producer`

#### Recv Ring の状態変数

- **recv_ring_producer**: 受信 CQE の immediate value から更新される (仮想)。ピアが WRITE+IMM で書き込んだデータの終端を示す。
- **last_recv_pos**: 消費済み位置 (仮想)。メッセージを 1 つ処理するたびに padded_message_size 分進む。
- **last_notified_recv_pos**: 最後にフロー制御メタデータの piggyback で通知した recv 位置 (仮想)。ring_credit_return の計算に使用。

### 3.3 メッセージエンコーディング

#### メッセージヘッダ (12 バイト)

各メッセージの先頭 12 バイトは以下のフィールドで構成される:

| オフセット | サイズ | フィールド | 説明 |
| --- | --- | --- | --- |
| 0 | 4B | call_id | MSB=0: リクエスト、MSB=1: レスポンス |
| 4 | 4B | piggyback | 送信者の consumer position (ALIGNMENT 単位) |
| 8 | 4B | payload_len | ペイロードバイト数 |

- リクエスト: `call_id = slab_key` (0 から 2^31-1)
- レスポンス: `call_id = slab_key | 0x80000000` (MSB をセット)
- piggyback フィールド: リクエスト送信時は `response_allowance / ALIGNMENT` をエンコードする。これにより受信側はレスポンス用のスペースを把握する。

#### パディング

メッセージ全体 (ヘッダ + ペイロード) は ALIGNMENT=32B 境界にパディングされる: `padded_size = align_up(12 + payload_len, 32)`。例えば payload=0 は 32B、payload=20 は 32B、payload=21 は 64B になる。

#### フロー制御メタデータ (32 バイト / バッチ)

各バッチの先頭に 32B のメタデータが配置される:

| オフセット | サイズ | フィールド | 説明 |
| --- | --- | --- | --- |
| 0 | 8B | ring_credit_return | 前回通知以降に消費したバイト数 |
| 8 | 8B | credit_grant | ピアに付与する新規クレジット (バイト) |
| 16 | 4B | message_count | バッチ内メッセージ数。`u32::MAX` はラップインジケータ |
| 20 | 12B | padding | ゼロ埋め |

#### Immediate Value (RDMA WRITE+IMM の 32-bit 値)

RDMA WRITE+IMM の immediate value には、書き込んだデータのバイト数デルタをエンコードする: `encode_imm(delta) = (delta / ALIGNMENT) as u32`、`decode_imm(imm) = (imm as u64) * ALIGNMENT`。ALIGNMENT=32 で割ることで、32-bit に最大 128GB のデルタを表現可能。

### 3.4 クレジット管理 (フロー制御)

copyrpc は **双方向クレジットベース** のフロー制御を行う。

#### コアの不変条件

`(producer - consumer) + 2 * R <= C`

ここで:
- `producer - consumer` は in-flight バイト数 (ローカルの send ring producer からリモートの消費位置を引いたもの)
- `R` は resp_reservation (ピアの応答用に予約したバイト数)
- `C` はリモートの recv ring サイズ
- 係数 2 はメタデータ + ラップ用の安全マージン

この不変条件により、リモートの recv ring がオーバーフローしないことを保証する。

#### クレジット付与の計算

`fill_metadata()` 内で毎バッチ計算される:

```
in_flight = producer - remote_consumer_pos
max_by_invariant = (remote_ring.size - in_flight) / 2 - current_resp_reservation
max_by_policy = max_resp_reservation - current_resp_reservation
grant = min(max_by_invariant, max_by_policy) & !(ALIGNMENT - 1)
```

`max_by_invariant` は不変条件から導かれる上限、`max_by_policy` はエンドポイント設定の上限。両方を満たす最小値をとり、ALIGNMENT 境界にアラインダウンする。

#### call() 時のクレジット消費

```
internal_allowance = align_up(padded_message_size(response_allowance) + 32, 32)

if peer_credit_balance < internal_allowance:
    flush, return InsufficientCredit

peer_credit_balance -= internal_allowance
```

`internal_allowance` は応答メッセージサイズ (padded) + フロー制御メタデータ (32B) を含む。これにより、ピアが応答を送信する際に十分なスペースが保証される。

#### reply() 時の予約解放

`resp_reservation -= response_allowance`。応答送信時にはクレジット不変条件により十分なスペースが保証されているため、RingFull エラーは発生しない。

#### クレジットの更新サイクル

送信側が call() を実行すると peer_credit_balance から allowance を差し引く。flush() でバッチが RDMA WRITE+IMM として送信され、メタデータに ring_credit_return と credit_grant が含まれる。受信側は poll() でこのメタデータを解読し、remote_consumer_pos に ring_credit_return を加算し、peer_credit_balance に credit_grant を加算する。これにより送信側のクレジットが回復する。

### 3.5 RDMA WRITE+IMM 操作

copyrpc の全データ転送は **RDMA WRITE with Immediate** で行われる。

#### WQE 構成

リモートの recv ring に直接書き込み、immediate value で位置デルタを通知する。リモートアドレスは `remote_recv_ring.addr + (start_pos & (remote_size - 1))`、リモートキーは `remote_recv_ring.rkey`、immediate value は `encode_imm(end_pos - start_pos)` である。

#### インライン最適化

| データサイズ | モード | 構成 |
| --- | --- | --- |
| 220B 以下 | Fully Inline | Ctrl + RDMA hdr + inline data |
| 220B 超 | Inline + SGE | Ctrl + RDMA hdr + 204B inline + SGE (残り) |

Fully Inline モードではメモリ読み取りが不要で、BlueFlame doorbell により NIC のキューバッファに直接書き込む。220B を超える場合は先頭 204B をインラインで、残りを SGE (Scatter/Gather Element) で send_ring_mr から読み取る。

#### シグナリング戦略

`SIGNAL_INTERVAL = 64`。unsignaled_count が 64 以上になると signaled WQE を発行し (CQE が生成される)、カウンタをリセットする。max_send_wr=256 に対し 64 WQE ごとにシグナルすることで SQ (Send Queue) の枯渇を防止する。

### 3.6 バッチ管理

#### バッチのライフサイクル

**prepare_next_batch()**: `meta_pos` を現在の producer 位置に設定し (32B メタデータプレースホルダ予約)、producer を FLOW_METADATA_SIZE (32B) 分進める。batch_message_count を 0 にリセットする。

**call() / reply()** (N 回): 各メッセージのヘッダ + ペイロードを producer 位置に書き込み、producer を padded_message_size 分進める。batch_message_count をインクリメントする。

**flush()**: まず `fill_metadata()` で ring_credit_return と credit_grant を計算し、meta_pos にメタデータを書き込む。次に `emit_raw()` で `[flush_start_pos, producer)` の範囲を RDMA WRITE+IMM として送信し、doorbell を ring する。最後に `prepare_next_batch()` で次のバッチを準備する。

#### ラップ処理

send ring の物理端に達し、バッチが ring 境界をまたぐ場合、連続 SGE が構成できなくなるため proactive wrap を行う。`meta_offset + batch_span >= ring_size` の条件で検出される。

ラップ処理は以下の手順で行う: (1) 現バッチを emit_raw() で送信する。(2) `message_count = u32::MAX` のラップメタデータを書き込む。(3) 残りのリングスペースを WRITE+IMM で送信する。(4) 新バッチを ring 先頭 (オフセット 0) から開始する。

受信側は `message_count == u32::MAX` を検出すると、次のリングサイクル先頭にスキップする。

### 3.7 poll() — CQE 処理

Context::poll() は以下の順序で実行される:

**ステップ 1: Recv CQ polling (MonoCq)**。CQE ごとに `process_cqe_static()` を呼び出す。まず CQE の QPN から FastMap で対象 Endpoint をルックアップする。次に `delta = decode_imm(cqe.imm)` で書き込みバイト数を計算し、`recv_ring_producer += delta` を更新する。フロー制御メタデータを解読し、`remote_consumer_pos += ring_credit_return`、`peer_credit_balance += credit_grant` を更新する。`message_count == u32::MAX` ならサイクルスキップする。そうでなければ `message_count` 個のメッセージを順に処理する。各メッセージについて、`is_response(call_id)` が true なら Slab から user_data を取り出して `on_response` コールバックを呼び出す。false (リクエスト) なら recv_stack に push する。Recv CQ のポーリング完了後、SRQ の posted 数が 2/3 * max_wr を下回っていれば不足分を repost する。

**ステップ 2: Send CQ polling**。送信完了を検出するが、通常は空のハンドラで処理される (完了追跡のみ)。

**ステップ 3: 全エンドポイント flush()**。各エンドポイントの未送信バッチを emit_raw() で RDMA WRITE+IMM として送信し、doorbell を ring する。

#### recv() — リクエスト取り出し

`pub fn recv(&self) -> Option<RecvHandle<U>>`

poll() で recv_stack に積まれたリクエストを 1 つずつ取り出す。RecvHandle は以下を保持する:
- `call_id`: リクエスト ID (レスポンス送信時に使用)
- `data()`: ペイロードスライス (recv ring 内の参照)
- `response_allowance`: ピアが予約したレスポンス用バイト数
- `reply(data)`: レスポンス送信メソッド。クレジット不変条件によりスペースが保証されている。

### 3.8 接続確立

#### エンドポイント交換

```rust
LocalEndpointInfo {
    qp_number: u32,           // RC QP 番号
    recv_ring_addr: u64,      // recv ring の RDMA 登録アドレス
    recv_ring_rkey: u32,      // リモートアクセスキー
    recv_ring_size: u64,      // バッファサイズ (2のべき乗)
    initial_credit: u64,      // 初期レスポンスクレジット (max_resp_reservation)
}
```

両側が LocalEndpointInfo を交換し、connect_ex() で RC QP を RTS 状態に遷移させる。

#### 初期化フロー

**Context::build()**: PD を作成し、SRQ を作成して recv バッファを pre-post し、Send CQ と Recv CQ を作成する。`srq_posted = srq_max_wr` に初期化する。

**ctx.create_endpoint(config)**: send_ring と recv_ring を `Box<[u8]>` で確保し、RDMA Memory Region として登録する。RC QP を INIT 状態で作成し、pending_calls 用の `Slab<U>` を初期化する。

**endpoint.local_info()**: ローカルの QPN、recv ring アドレス、rkey、サイズ、初期クレジットを返す。これを MPI や帯域外チャネルでピアと交換する。

**endpoint.connect_ex(remote_info)**: リモートの recv ring 情報を設定する。`resp_reservation = min(max_resp_reservation, remote_size / 4)` で初期予約を計算し、`peer_credit_balance = remote.initial_credit` で初期クレジットを設定する。QP を INIT → RTR → RTS に遷移させ、`prepare_next_batch()` で最初のバッチを準備する。

### 3.9 Raw RDMA Read/Write

copyrpc は RPC メッセージ以外に、raw RDMA Read/Write をサポートする (copyrpc-fs で使用)。

`post_rdma_read()` / `post_rdma_write()` は SGE ベースの RDMA 操作を直接発行する。sentinel QPN (`u32::MAX`) を使って Send CQ の CQE を通常の RPC 完了と区別し、`raw_rdma_done` カウンタを更新する。`raw_rdma_pending()` は `posted - done` で in-flight 操作数を返す。

---

## 4. データフロー詳細

### 4.1 リモートリクエスト送信 (Client → Remote)

以下の 6 ステップでリモートリクエストが送信される:

**ステップ 1 — Client が delegationrpc に書き込み**: Client は `deleg_client.call(req)` を呼び出す。内部で `head.fetch_add(1, Relaxed)` によりスロット位置を確保し、tail ベースのフロー制御でスペースを待機した後、スロットに `client_id`, `resp_slot_idx`, payload を書き込み、`committed.store(1, Release)` でコミットする。

**ステップ 2 — Daemon#0 が delegationrpc を読み取り**: Daemon#0 のイベントループ Phase 2 で `deleg_server.poll()` + `deleg_server.recv()` により、コミットされたエントリを順に取得する。

**ステップ 3 — Daemon#0 が copyrpc call() を発行**: RecvEntry から `target_rank` を抽出し、対応する copyrpc エンドポイントに対して `ep.call(remote_req.as_bytes(), DelegToken { client_id, resp_slot_idx }, 0u64)` を呼び出す。DelegToken は copyrpc の user_data (Slab) に保存され、レスポンス到着時に使用される。

**ステップ 4 — copyrpc がバッチを RDMA WRITE+IMM で送信**: flush() 時にメッセージがバッチ化され、リモートの recv ring に RDMA WRITE+IMM として書き込まれる。immediate value でデルタを通知する。

**ステップ 5 — RDMA NIC がリモートに転送**: InfiniBand RC QP により信頼性のある配信が保証される。

**ステップ 6 — リモートノードの copyrpc が受信**: Recv CQ に CQE が到着し、process_cqe_static() でメッセージが解読され、recv_stack にリクエストとして push される。

DelegToken は copyrpc の user_data として Slab に保持される:

```rust
#[derive(Clone, Copy)]
#[repr(C)]
struct DelegToken {
    client_id: u32,      // delegationrpc のクライアント ID
    resp_slot_idx: u32,  // delegationrpc のレスポンススロット番号
}
```

### 4.2 リモートレスポンス受信 (Remote → Client)

以下の 5 ステップでリモートレスポンスがクライアントに返される:

**ステップ 1 — リモートノードが copyrpc reply() を実行**: recv_handle.reply(resp_bytes) により、応答メッセージが send ring に書き込まれる。flush() で RDMA WRITE+IMM として送信される。

**ステップ 2 — RDMA NIC がローカルノードに配信**: Recv CQ に CQE が到着する。

**ステップ 3 — Daemon#0 の copyrpc poll() がレスポンスを処理**: poll() のコールバック `on_response(DelegToken, &[u8])` が呼び出される。DelegToken に含まれる `client_id` と `resp_slot_idx` により、応答先の delegationrpc クライアントとスロットが特定される。

**ステップ 4 — Daemon#0 が delegationrpc reply() を実行**: `deleg_server.reply(token.client_id, token.resp_slot_idx, resp)` により、レスポンスデータがクライアントの response slot に `write_volatile` で書き込まれ、`valid.store(1, Release)` で通知される。

**ステップ 5 — Client が response slot をポーリング**: `deleg_client.poll()` で `valid.load(Acquire)` をチェックし、valid=1 ならレスポンスを読み取り、`valid.store(0, Release)` でスロットを解放する。

### 4.3 リモートリクエスト受信 + シャード転送

リモートから受信したリクエストが Daemon#0 のシャードでない場合、Flux 経由で対象 Daemon に転送する。全体パスは: Remote → copyrpc → Daemon#0 → Flux → Daemon#N → KV → Flux → Daemon#0 → copyrpc reply → Remote。

**ステップ 1**: Daemon#0 が `ctx.recv()` で RemoteRequest を受信する。

**ステップ 2**: `owner_of(key, num_daemons)` で対象シャードの daemon ID を計算する。自分 (daemon_id=0) でなければ Flux 経由で転送する。

**ステップ 3**: recv_handle を `pending_copyrpc_handles[idx]` に保存し、`flux.call(target_daemon, DelegatePayload::Req(req), idx)` で転送する。`idx` が Flux トークンとなり、応答時にどの recv_handle に対応するかを追跡する。

**ステップ 4**: Daemon#N が `flux.try_recv_raw()` でリクエストを受信する。

**ステップ 5**: Daemon#N が `handle_local()` で KV 操作を実行する。

**ステップ 6**: Daemon#N が `flux.reply(flux_token, DelegatePayload::Resp(resp))` で応答を返す。

**ステップ 7**: Daemon#0 が `flux.poll()` のコールバックで応答を受信する。`pending_copyrpc_handles[pending_idx]` から recv_handle を取り出し、`recv_handle.reply(remote_resp.as_bytes())` で copyrpc reply を送信する。

**ステップ 8**: copyrpc reply がリモートノードに RDMA で配信される。

### 4.4 ローカルリクエスト

ローカルリクエストは Meta バックエンドと同一パスを使用する。

**ステップ 1**: Client が `entry.rank == my_rank` を判定し、`ipc_clients[key % num_daemons].call(req, ())` で対象シャードの Daemon に直接送信する。

**ステップ 2**: Daemon#N が `ipc_server.poll()` → `recv()` でリクエストを受信し、`handle_local()` で KV 操作を実行し、`handle.reply(resp)` で応答する。

**ステップ 3**: Client が `ipc_client.poll()` で応答を受信する。

---

## 5. Daemon#0 イベントループ

Daemon#0 は 4 フェーズを繰り返す:

```rust
loop {
    if stop_flag.load(Relaxed) { break; }

    // Phase 1: copyrpc poll + recv (RDMA 完了処理 + 受信リクエスト)
    ctx.poll(|token: DelegToken, data| {
        let resp = RemoteResponse::from_bytes(data).response;
        deleg_server.reply(token.client_id, token.resp_slot_idx, resp);
    });
    while let Some(recv) = ctx.recv() {
        // 自シャード → handle_local、他シャード → Flux 転送
    }

    // Phase 2: delegationrpc poll (クライアントのリモートリクエスト)
    deleg_server.poll();
    while let Some(entry) = deleg_server.recv() {
        // copyrpc.call() でリモートに送信
    }
    deleg_server.advance_tail();

    // Phase 3: ipc poll (ローカルリクエスト、shard #0)
    ipc_server.poll_with_skip(&ipc_skip);
    while let Some(handle) = ipc_server.recv() {
        handle.reply(handle_local(&mut store, handle.data()));
    }

    // Phase 4: Flux poll + recv (daemon 間転送)
    flux.poll(|pending_idx, payload| {
        // Flux レスポンス → copyrpc reply
    });
    while let Some((_, token, DelegatePayload::Req(req))) = flux.try_recv_raw() {
        // 他 daemon からの転送 → handle_local → Flux reply
    }
}
```

### フェーズ順序の根拠

**Phase 1 (copyrpc)** を最初に実行する。RDMA 完了を処理してクレジットを回復し、Phase 2 の copyrpc 送信に必要なクレジットを確保する。また、受信リモートリクエストを Flux 経由で転送する。

**Phase 2 (delegationrpc)** でクライアントのリモートリクエストを処理する。Phase 1 で回復したクレジットを消費して copyrpc.call() を発行する。advance_tail() でリングスペースを解放する。

**Phase 3 (ipc)** でローカルリクエストを処理する。ローカルリクエストはリモートよりレイテンシが低く、ipc の SPSC リングにはバックプレッシャ機構があるため、優先度は低い。

**Phase 4 (Flux)** で daemon 間通信を処理する。Phase 1 の copyrpc recv で生じた Flux 転送のレスポンスが到着し、copyrpc reply として送信される。

### バックプレッシャ処理

#### copyrpc 送信リング満杯 (Phase 2)

copyrpc の call() が `RingFull` を返した場合、ctx.poll() を呼んで RDMA 完了を処理し、クレジットを回復してからリトライする:

```rust
loop {
    match ep.call(bytes, token, 0u64) {
        Ok(_) => break,
        Err(CallError::RingFull(returned)) => {
            ctx.poll(|token, data| { /* レスポンス処理 */ });
            pending_token = returned;
        }
        Err(_) => break,
    }
}
```

#### copyrpc 応答リング満杯 (Phase 4)

Flux レスポンスを copyrpc reply で送信する際にリング満杯になった場合、poll() で排出するが、そのネストした poll() で到着した delegationrpc 向けレスポンスは `nested_resp_buf` に一旦バッファリングし、ループ脱出後に処理する:

```rust
loop {
    match recv_handle.reply(bytes) {
        Ok(()) => break,
        Err(RingFull) => {
            ctx.poll(|token, data| {
                nested_resp_buf.push((token, resp));
            });
        }
        Err(_) => break,
    }
}
for (token, resp) in nested_resp_buf.drain(..) {
    deleg_server.reply(token.client_id, token.resp_slot_idx, resp);
}
```

---

## 6. Daemon Worker イベントループ

Daemon#1..K は copyrpc / delegationrpc を持たず、ipc と Flux のみで構成される:

```rust
loop {
    if stop_flag.load(Relaxed) { break; }

    // Phase 1: ipc (ローカルリクエスト、自分のシャード)
    ipc_server.poll_with_skip(&ipc_skip);
    while let Some(handle) = ipc_server.recv() {
        handle.reply(handle_local(&mut store, handle.data()));
    }

    // Phase 2: Flux (Daemon#0 からの転送)
    flux.poll(|_, _| {});
    while let Some((_, token, DelegatePayload::Req(req))) = flux.try_recv_raw() {
        let resp = handle_local(&mut store, &req);
        flux.reply(token, DelegatePayload::Resp(resp));
    }
}
```

Phase 1 で ipc 経由のローカルリクエスト (自分のシャードに対するもの) を処理する。Phase 2 で Daemon#0 から Flux 経由で転送されたリモートリクエストを処理し、応答を Flux reply で返す。

---

## 7. クライアントのデュアルパスルーティング

```rust
loop {
    if entry.rank == my_rank {
        // ローカル: キーのシャードオーナーに ipc で直接送信
        let daemon = (entry.key % num_daemons as u64) as usize;
        ipc_clients[daemon].call(make_request(entry), ())?;
    } else {
        // リモート: delegationrpc で Daemon#0 に委任
        deleg_client.call(make_request(entry))?;
    }

    // 両方をポーリング
    for c in &mut ipc_clients { total_n += c.poll()?; }
    total_n += deleg_client.poll(|_, _| {});

    // 完了分を再充填
    for _ in 0..total_n { /* 次のリクエスト送信 */ }
}
```

ルーティング判定は `entry.rank == my_rank` で行う。ローカルリクエストは `key % num_daemons` で対象シャードの Daemon を特定し、ipc で直接送信する。リモートリクエストは delegationrpc の共有リングに fetch_add で書き込み、Daemon#0 に委任する。ポーリングは ipc と delegationrpc の両方を行い、完了した分だけ次のリクエストを充填する (closed-loop)。

---

## 8. メッセージ型

### Request / Response (ipc / delegationrpc レイヤ)

```rust
#[repr(C)]
pub enum Request {
    MetaPut { rank: u32, key: u64, value: u64 },
    MetaGet { rank: u32, key: u64 },
}

#[repr(C)]
pub enum Response {
    MetaPutOk,
    MetaGetOk { value: u64 },
    MetaGetNotFound,
}
```

`rank` フィールドでターゲットランクを、`key` フィールドでキーハッシュを保持する。

### RemoteRequest / RemoteResponse (copyrpc レイヤ)

```rust
#[repr(C)]
pub struct RemoteRequest { pub request: Request }
#[repr(C)]
pub struct RemoteResponse { pub response: Response }
```

`as_bytes()` / `from_bytes()` で `&[u8]` とゼロコピー変換する。copyrpc の call() / reply() に渡すバイト列として使用される。

### DelegatePayload (Flux レイヤ)

```rust
#[repr(C)]
pub enum DelegatePayload {
    Req(Request),
    Resp(Response),
}
```

Daemon#0 から Daemon#N への転送に `DelegatePayload::Req` を、Daemon#N から Daemon#0 への応答に `DelegatePayload::Resp` を使用する。

---

## 9. オーケストレータ (run_delegation)

### 初期化シーケンス

**ステップ 1 — CPU アフィニティ割り当て**: `get_available_cores(device_index)` で利用可能コアを取得し、`assign_cores()` で daemon と client にコアを分配する。

**ステップ 2 — ワークロードパターン生成**: 各クライアントスレッド用のアクセスパターン (rank, key, is_read) を事前生成する。

**ステップ 3 — ipc Server 作成**: K 個の ipc Server を `/benchkv_deleg_ipc_{rank}_{daemon_id}` パスで作成する。各サーバは `num_clients` 個のクライアント接続と `queue_depth` の QD をサポートする。

**ステップ 4 — delegationrpc Server 作成**: 1 個の delegationrpc Server を `/benchkv_deleg_ring_{rank}` パスで作成する。ring_depth=1024、resp_depth=queue_depth。Daemon#0 専用。

**ステップ 5 — Flux ネットワーク作成**: K ノードの Flux を作成する。capacity=1024、max_inflight=256。ペイロードは `DelegatePayload`、トークンは `usize`。

**ステップ 6 — copyrpc セットアップチャネル** (size > 1 のとき): Daemon#0 用に 1 セットの mpsc チャネルを作成する。`my_remote_ranks` は全リモートrank (Meta の `rank % D == d` 分散ではなく集約)。

**ステップ 7 — Daemon スレッド起動**: Daemon#0 は `run_daemon_0()` で、Daemon#1..K は `run_daemon_worker()` で起動する。

**ステップ 8 — MPI エンドポイント交換** (size > 1、メインスレッド): Daemon#0 から local_infos を mpsc で受信し、各リモートランクと `mpi_util::exchange_bytes()` で交換し、remote_infos を Daemon#0 に送信する。

**ステップ 9 — Ready Barrier**: 全 daemon + メインスレッドが同期し、copyrpc セットアップ完了を保証する。

**ステップ 10 — MPI Barrier**: 全ランクがグローバル同期する。

**ステップ 11 — Client スレッド起動**: `run_delegation_client()` で各クライアントスレッドを起動する。ipc パスと delegationrpc パスを渡す。

**ステップ 12 — ベンチマークループ**: `runs` 回繰り返す。各 run で `world.barrier()` → `sleep(duration)` → 時刻記録。

**ステップ 13 — 停止**: `stop_flag.store(true, Release)` で全スレッドに停止を通知する。

**ステップ 14 — スレッド join + 結果集計**: daemon / client スレッドを join し、各 run の RPS を計算する。MPI all_reduce で全ランク合計を算出する。

---

## 10. 性能特性

### 実測値 (fern03-fern04, 2 nodes, 2 daemons, 4 clients, QD=4)

| バックエンド | 単一ノード (RPS) | 2 ノード合計 (RPS) |
| --- | --- | --- |
| Meta | 25.2M | 3.37M |
| Delegation | 21.0M | 4.76M (+41%) |

### 単一ノードの差 (21M vs 25.2M)

単一ノードではリモートリクエストが発生しないため delegationrpc は使われない。差は Daemon#0 が delegationrpc の poll() を追加で実行するオーバーヘッドに起因する。全リクエストがローカルなので Meta の方が有利。

### マルチノードの改善 (+41%)

リモートリクエストパスの短縮による改善。Meta では Client から copyrpc に到達するまでに ipc → Daemon#N → Flux → Daemon#0 の 3 ホップが必要だが、Delegation では Client → delegationrpc → Daemon#0 の 1 ホップで済む。fetch_add による共有リングへの直接書き込みが、ipc の SPSC 間接参照と Flux の N-to-N チャネルの 2 段委任を除去し、リモートリクエストのレイテンシを大幅に削減する。
