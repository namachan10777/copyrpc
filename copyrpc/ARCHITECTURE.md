# copyrpc アーキテクチャと動作詳細

## 概要

copyrpcは、RDMA WRITE+IMMとSPSCリングバッファを使用した高スループットRPCフレームワークです。

## コンポーネント

### Context

```
┌─────────────────────────────────────────────────────────────────┐
│                         Context                                  │
│  ┌─────────┐  ┌─────────┐  ┌────────────────────────────────┐   │
│  │ recv_cq │  │   SRQ   │  │  Endpoint Registry             │   │
│  │(MonoCq) │  │ (shared)│  │  HashMap<QPN, Endpoint>        │   │
│  └─────────┘  └─────────┘  └────────────────────────────────┘   │
│  ┌─────────┐  ┌─────────┐                                       │
│  │ send_cq │  │cqe_buf  │  recv_stack: Vec<RecvMessage>        │
│  │  (Cq)   │  │send_buf │                                       │
│  └─────────┘  └─────────┘                                       │
└─────────────────────────────────────────────────────────────────┘
```

- **recv_cq (MonoCq)**: WRITE+IMMの受信完了を検出。コールバックでcqe_bufferにCQEを格納。
- **send_cq (Cq)**: WRITE+IMMとREADの送信完了を検出。dispatch_cqeでsend_cqe_bufferに格納。
- **SRQ**: 全エンドポイントで共有するReceive Queue。
- **cqe_buffer**: recv_cqのコールバックが格納する受信CQEのバッファ。
- **send_cqe_buffer**: send_cqのdispatch_cqeが格納する送信CQEのバッファ。
- **recv_stack**: process_cqeで作成されたRecvMessageのスタック。

### Endpoint

```
┌────────────────────────────────────────┐
│           EndpointInner                │
│  ┌────────────┐  ┌─────────────────┐   │
│  │   RC QP    │  │  send_ring      │   │
│  │ (SRQ使用)  │  │  send_ring_mr   │   │
│  └────────────┘  │  producer/cons  │   │
│                  └─────────────────┘   │
│  ┌─────────────────┐                   │
│  │  recv_ring      │ remote_recv_ring  │
│  │  recv_ring_mr   │ remote_consumer   │
│  │  last_recv_pos  │                   │
│  │  recv_producer  │                   │
│  └─────────────────┘                   │
│  ┌─────────────────┐                   │
│  │ consumer_pos MR │ (for RDMA READ)   │
│  │ read_buffer MR  │                   │
│  │ read_inflight   │                   │
│  └─────────────────┘                   │
│  pending_calls: HashMap<call_id, U>    │
└────────────────────────────────────────┘
```

## データフロー

### 1. リクエスト送信 (call)

```
Client                                      Server
  │                                           │
  │  call(&data, user_data)                   │
  │    1. Check ring space                    │
  │    2. Allocate call_id                    │
  │    3. pending_calls.insert(call_id, U)   │
  │    4. encode_header in send_ring          │
  │    5. copy data to send_ring              │
  │    6. update send_ring_producer           │
  │                                           │
  │  poll()                                   │
  │    emit_wqe() + flush()                   │
  │    ─────── RDMA WRITE+IMM ───────>       │
  │                                           │
  │                                           │  recv_cq gets CQE
  │                                           │  callback: cqe_buffer.push()
  │                                           │
  │                                           │  poll()
  │                                           │    process_cqe()
  │                                           │      decode_header
  │                                           │      update remote_consumer (piggyback)
  │                                           │      push to recv_stack
  │                                           │    repost SRQ recvs
  │                                           │
  │                                           │  recv()
  │                                           │    pop from recv_stack
```

### 2. レスポンス送信 (reply)

```
Server                                      Client
  │                                           │
  │  RecvHandle::reply(&data)                 │
  │    1. Check ring space                    │
  │    2. to_response_id(call_id)             │
  │    3. encode_header in send_ring          │
  │    4. copy data to send_ring              │
  │    5. update send_ring_producer           │
  │                                           │
  │  poll()                                   │
  │    emit_wqe() + flush()                   │
  │    ─────── RDMA WRITE+IMM ───────>       │
  │                                           │
  │                                           │  recv_cq gets CQE
  │                                           │  callback: cqe_buffer.push()
  │                                           │
  │                                           │  poll()
  │                                           │    process_cqe()
  │                                           │      decode_header
  │                                           │      is_response(call_id) == true
  │                                           │      pending_calls.remove(call_id)
  │                                           │      on_response(user_data, &data)
  │                                           │    repost SRQ recvs
```

## poll() の詳細

```rust
pub fn poll(&self) {
    // 1. recv_cq をポーリング（MonoCq）
    //    コールバックが cqe_buffer にCQEを格納
    loop {
        let count = self.recv_cq.poll();
        if count == 0 { break; }
    }
    self.recv_cq.flush();

    // 2. cqe_buffer からCQEを取り出して処理
    let cqes = self.cqe_buffer.drain();
    for (cqe, _entry) in cqes {
        self.process_cqe(cqe, cqe.qp_num);  // qp_num でエンドポイント特定
    }

    // 3. SRQ に recv を再投稿
    //    処理したCQEの数だけ repost
    for _ in 0..num_cqes {
        self.srq.post_recv(SrqEntry { qpn: 0, is_read: false }, 0, 0, 0);
    }
    self.srq.ring_doorbell();

    // 4. send_cq をポーリング（通常のCq）
    //    dispatch_cqe が send_cqe_buffer に格納
    loop {
        let count = self.send_cq.poll();
        if count == 0 { break; }
    }
    self.send_cq.flush();

    // 5. send_cqe_buffer からREAD完了を処理
    for (cqe, entry) in send_cqes {
        if entry.is_read {
            // RDMA READ 完了 → remote_consumer を更新
            let consumer_value = u64::from_le_bytes(read_buffer);
            ep.remote_consumer.update(consumer_value);
            ep.read_inflight.set(false);
        }
        // WRITE+IMM 完了 (is_read=false) は無視
    }

    // 6. 全エンドポイントをフラッシュ
    for ep in endpoints {
        ep.flush();  // emit_wqe + doorbell
    }

    // 7. needs_read() なエンドポイントに RDMA READ を発行
    for ep in endpoints {
        if !ep.read_inflight && ep.needs_read() {
            // remote の consumer_position を読む
            qp.sq_wqe().read(...).finish_signaled(SrqEntry { qpn, is_read: true });
            ep.read_inflight = true;
        }
    }
}
```

## process_cqe() の詳細

```rust
fn process_cqe(&self, cqe: Cqe, qpn: u32) {
    // CQEのimm値からオフセット増分を取得
    let delta = decode_imm(cqe.imm);

    // recv_ring_producer を更新
    ep.recv_ring_producer += delta;

    // last_recv_pos から recv_ring_producer まで全メッセージを処理
    loop {
        if last_recv_pos >= recv_ring_producer { break; }

        // ヘッダーをデコード
        let (call_id, piggyback, payload_len) = decode_header(...);

        // piggyback で remote_consumer を更新
        ep.remote_consumer.update(piggyback);

        if is_padding_marker(call_id) {
            // パディング：last_recv_pos を進める
            last_recv_pos += remaining;
            continue;
        }

        if is_response(call_id) {
            // レスポンス：コールバック呼び出し
            let user_data = pending_calls.remove(original_call_id);
            on_response(user_data, &data);
        } else {
            // リクエスト：recv_stack に格納
            recv_stack.push(RecvMessage { ... });
        }

        last_recv_pos += padded_message_size(payload_len);
    }

    // consumer_position MR を更新（リモートからのREAD用）
    ep.consumer_position.store(last_recv_pos, Release);
}
```

## リングバッファのフロー制御

### Piggyback (通常の更新)

メッセージヘッダーに local_consumer を含めて送信：
```
[header: call_id(4) | piggyback(4) | payload_len(4)] [payload...] [padding]
```

受信側はヘッダーから piggyback を読んで remote_consumer を更新。

### RDMA READ (フォールバック)

RingFull 検出時に、リモートの consumer_position MR から直接読み取り：

```
needs_read() == true の条件:
    available_space < ring_size / 4

READ 発行:
    qp.read(remote_consumer_mr.addr, remote_consumer_mr.rkey)
    → read_buffer に結果が書かれる

READ 完了時:
    consumer_value = u64::from_le_bytes(read_buffer)
    ep.remote_consumer.update(consumer_value)
```

## SRQ の管理

### 初期化

```
// 各エンドポイント作成時に SRQ へ recv を投稿
let initial_recv_count = max_send_wr * 4;  // バッファを持たせる
for _ in 0..initial_recv_count {
    srq.post_recv(SrqEntry { qpn, is_read: false }, 0, 0, 0);
}
srq.ring_doorbell();
```

### 再投稿 (repost)

```
// poll() 内で、処理したCQEの数だけ再投稿
for _ in 0..num_cqes {
    srq.post_recv(SrqEntry { qpn: 0, is_read: false }, 0, 0, 0);
}
srq.ring_doorbell();
```

### SRQ 枯渇の問題

高速通信で repost が追いつかないと SRQ が枯渇し、RNR NAK (Receiver Not Ready) エラーが発生。
QPがエラー状態に入り、以降の通信が不可能になる。

対策：
1. SRQ 容量を十分大きくする
2. 初期 recv 投稿数を増やす
3. 送信ペースを調整する

## CQ の処理

### recv_cq (MonoCq)

- コールバック方式で CQE を処理
- `MonoCq::poll()` が CQE を検出すると、登録された QP の `CompletionSource::process_cqe()` を呼び、
  その結果でコールバックが呼ばれる
- コールバック内で cqe_buffer に格納

### send_cq (通常の Cq)

- `Cq::poll()` が CQE を検出すると、登録された QP の `CompletionTarget::dispatch_cqe()` を呼ぶ
- `dispatch_cqe()` 内で sq_callback が呼ばれ、send_cqe_buffer に格納
- is_read フラグで WRITE+IMM 完了と READ 完了を区別

## エラー処理

### RingFull

send_ring が一杯の場合、call() / reply() は `Error::RingFull` を返す。
呼び出し側は poll() を呼んでから再試行するか、待機する。

### SRQ エラー (RNR NAK)

受信側に recv バッファがない状態でデータが届くと、RNR NAK が発生。
リトライ回数を超えると QP がエラー状態になる。

## パフォーマンス分析

### ベンチマーク結果 (2026-01-23)

| ベンチマーク | スループット |
|-------------|-------------|
| send_recv_small (inline) | 21 Mrps |
| **copyrpc** | 19.8 Mrps |
| mlx5 pingpong (WRITE+IMM) | 17 Mrps |

### 現在のボトルネック (perf分析)

poll()が全体の68%を占めており、内訳は以下の通り：

#### 1. Rc/RefCell操作 (~13%)

```rust
// lib.rs:476 - endpoints.borrow() + .get() → Rc clone
if let Some(ep) = self.endpoints.borrow().get(entry.qpn) {

// lib.rs:477, 489 - ep.borrow() → RefCell借用チェック
let ep_ref = ep.borrow();

// lib.rs:488 - .iter() → 各Rcのclone
for (_, ep) in self.endpoints.borrow().iter() {
```

#### 2. 条件分岐 (~15%)

```rust
// lib.rs:471 - is_read分岐
if !entry.is_read { continue; }

// lib.rs:478 - read_inflight分岐
if ep_ref.read_inflight.get() { ... }

// lib.rs:499 - 3条件の分岐
if !inflight && (needs || force) { ... }
```

#### 3. リングバッファ計算 (~11%)

```rust
// process_cqe内 (インライン化済み) - 空き容量計算
// emit_wqe内 - producer - consumer の計算
let available = remote_ring.size - (producer - consumer);
```

#### 4. フラグチェック (~4%)

```rust
// lib.rs:495-497
let needs = ep_ref.needs_read();      // cmpb $0x0,0x134
let force = ep_ref.force_read.get();
let inflight = ep_ref.read_inflight.get();
```

#### 5. エンドポイントイテレーション (~4%)

```rust
// lib.rs:488 - 8エンドポイント毎回走査
for (_, ep) in self.endpoints.borrow().iter() {
```

### 最適化の履歴

1. **fastmap導入**: HashMap→Vec+offsetでO(1)ルックアップ
2. **inline化**: emit_wqe, flush, process_cqeに#[inline(always)]
3. **SRQバッチ補充**: 2/3を切ったらまとめて補充
4. **to_vec()削除**: レスポンスコールバックにスライスを直接渡す
5. **ループ統合**: flush + READ発行ループを1つに

### 今後の改善候補

- アクティブなエンドポイントだけをイテレートする仕組み
- Rc/RefCellを避けた設計（unsafe or arena allocation）
- flush不要なエンドポイントのスキップ
