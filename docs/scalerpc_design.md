# ScaleRPC Rust実装 設計ドキュメント

## 概要

ScaleRPC (EuroSys 2019) をRustで実装するための設計ドキュメント。
Reliable Connection (RC) 上でone-sided RDMA verbsを使用し、スケーラブルなRPCを実現する。

**論文**: "Scalable RDMA RPC on Reliable Connection with Efficient Resource Sharing"
**著者**: Youmin Chen, Youyou Lu, Jiwu Shu (Tsinghua University)
**会議**: EuroSys 2019

---

## 1. 問題と解決策

### 1.1 RDMAスケーラビリティ問題

RDMA Reliable Connection (RC) は接続数増加に伴い深刻な性能劣化を起こす。

| リソース | ボトルネック | 影響 |
|----------|--------------|------|
| NICキャッシュ | QPコンテキスト (QPC) キャッシュミス | PCIeラウンドトリップ増加 |
| CPUキャッシュ | メッセージバッファ分散 | キャッシュミス増加 |
| ホストメモリ | 接続ごとのバッファ確保 | メモリ消費の二次関数的増加 |

**具体的な数値**:
- ConnectX-3: 約400 QP でキャッシュ飽和
- ConnectX-5: キャッシュサイズ約2MB
- 512ノード時RC: 146.198 MB/ノード (vs 32ノード時: 8.869 MB)
- キャッシュミス率49.1%時: スループット96.6Gbps → 48Gbps に劣化

### 1.2 ScaleRPCの解決アプローチ

| 技術 | 目的 | 効果 |
|------|------|------|
| Connection Grouping | NICキャッシュスラッシング軽減 | アクティブ接続数の制限 |
| Virtualized Mapping | CPUキャッシュミス軽減 | メッセージプール共有 |

### 1.3 性能特性

- **メタデータアクセス**: 最大90%性能向上
- **SmallBankトランザクション**: 最大160%性能向上

---

## 2. アーキテクチャ概要

### 2.1 システム構成

```
┌─────────────────────────────────────────────────────────────┐
│                        Application                          │
├─────────────────────────────────────────────────────────────┤
│                      ScaleRPC Layer                         │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐   │
│  │  Connection   │  │  Virtualized  │  │    Worker     │   │
│  │   Grouping    │  │   Mapping     │  │  Scheduling   │   │
│  └───────────────┘  └───────────────┘  └───────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                    One-sided RDMA (RC)                      │
│         RDMA WRITE (request) / RDMA READ (response)         │
├─────────────────────────────────────────────────────────────┤
│                     mlx5 / ibverbs                          │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 通信モデル: One-sided RDMA RPC

ScaleRPCはone-sided RDMA操作を使用してRPCを実現する。

**リクエスト送信 (Client → Server)**:
```
Client: RDMA WRITE → Server's Request Buffer
```

**レスポンス取得 (Server → Client)**:
```
Option A: Client: RDMA READ ← Server's Response Buffer (Polling)
Option B: Server: RDMA WRITE → Client's Response Buffer
```

---

## 3. Connection Grouping

### 3.1 設計原理

NICキャッシュのスラッシングを防ぐため、同時にアクティブな接続数を制限する。

```rust
/// コネクショングループ
pub struct ConnectionGroup {
    group_id: usize,
    connections: Vec<Connection>,
    max_active: usize,        // グループ内最大アクティブ接続数
    active_count: AtomicUsize,
}

/// グループマネージャー
pub struct GroupManager {
    groups: Vec<ConnectionGroup>,
    num_groups: usize,
    connections_per_group: usize,
}
```

### 3.2 グループサイズ決定

NICキャッシュサイズに基づいてグループサイズを決定:

```rust
/// NICキャッシュパラメータ (ConnectX系)
const NIC_QPC_CACHE_ENTRIES: usize = 400;  // CX-3
// const NIC_QPC_CACHE_ENTRIES: usize = 1000; // CX-5 推定

/// グループサイズ計算
fn calculate_group_size(total_connections: usize, num_threads: usize) -> usize {
    // キャッシュ飽和を避けるため、キャッシュエントリの一部のみ使用
    let safe_cache_usage = NIC_QPC_CACHE_ENTRIES * 80 / 100;
    let per_thread_qps = safe_cache_usage / num_threads;
    per_thread_qps.min(total_connections / num_threads)
}
```

### 3.3 グループ選択アルゴリズム

```rust
impl GroupManager {
    /// Round-Robin方式でグループ選択
    pub fn select_group(&self, dest_node: NodeId) -> &ConnectionGroup {
        let group_idx = dest_node.0 as usize % self.num_groups;
        &self.groups[group_idx]
    }

    /// 負荷分散を考慮したグループ選択
    pub fn select_group_load_aware(&self) -> &ConnectionGroup {
        self.groups
            .iter()
            .min_by_key(|g| g.active_count.load(Ordering::Relaxed))
            .unwrap()
    }
}
```

### 3.4 接続のライフサイクル管理

```rust
impl ConnectionGroup {
    /// 接続をアクティブ化（セマフォ的動作）
    pub fn activate(&self) -> Result<ConnectionGuard, Error> {
        loop {
            let current = self.active_count.load(Ordering::Acquire);
            if current >= self.max_active {
                // バックオフまたはスピン
                std::hint::spin_loop();
                continue;
            }
            if self.active_count.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                return Ok(ConnectionGuard { group: self });
            }
        }
    }
}

/// RAIIガード - スコープ終了時に自動解放
pub struct ConnectionGuard<'a> {
    group: &'a ConnectionGroup,
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.group.active_count.fetch_sub(1, Ordering::Release);
    }
}
```

---

## 4. Virtualized Mapping

### 4.1 設計原理

複数の接続グループで単一のメッセージプールを共有し、CPUキャッシュ効率を向上させる。

```
従来方式 (Per-Connection Buffer):
┌─────────┐ ┌─────────┐ ┌─────────┐
│ Conn 0  │ │ Conn 1  │ │ Conn 2  │  ... (CPUキャッシュミス増加)
│ Buffer  │ │ Buffer  │ │ Buffer  │
└─────────┘ └─────────┘ └─────────┘

ScaleRPC方式 (Shared Message Pool):
┌─────────────────────────────────────────┐
│           Shared Message Pool           │  (CPUキャッシュ効率向上)
│  ┌─────┬─────┬─────┬─────┬─────┬─────┐ │
│  │Slot0│Slot1│Slot2│Slot3│Slot4│ ... │ │
│  └─────┴─────┴─────┴─────┴─────┴─────┘ │
└─────────────────────────────────────────┘
         ↑       ↑       ↑
       Conn0   Conn1   Conn2  (仮想マッピング)
```

### 4.2 メッセージプール設計

```rust
/// スロットの状態
#[repr(u8)]
pub enum SlotState {
    Free = 0,
    Reserved = 1,
    RequestPending = 2,
    ResponseReady = 3,
}

/// メッセージスロット
#[repr(C)]
pub struct MessageSlot {
    state: AtomicU8,
    owner_conn: AtomicU32,    // 現在このスロットを使用している接続ID
    request_len: u32,
    response_len: u32,
    data: [u8; SLOT_DATA_SIZE],
}

/// 共有メッセージプール
pub struct MessagePool {
    buffer: HugePageBuffer,    // HugePage確保
    slots: *mut MessageSlot,
    num_slots: usize,
    slot_size: usize,
    mr: MemoryRegion,          // RDMA Memory Region
    free_list: Mutex<Vec<usize>>,
}

const SLOT_DATA_SIZE: usize = 4096 - 16;  // MTUに合わせる
const DEFAULT_NUM_SLOTS: usize = 8192;
```

### 4.3 スロット割り当てアルゴリズム

```rust
impl MessagePool {
    /// スロット割り当て（ロックフリー試行 → フォールバック）
    pub fn allocate_slot(&self) -> Option<SlotHandle> {
        // Fast path: ロックフリーで空きスロット探索
        for i in 0..self.num_slots {
            let slot = unsafe { &*self.slots.add(i) };
            if slot.state.compare_exchange(
                SlotState::Free as u8,
                SlotState::Reserved as u8,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                return Some(SlotHandle { pool: self, index: i });
            }
        }

        // Slow path: free_listからpop
        let mut free_list = self.free_list.lock().unwrap();
        free_list.pop().map(|index| SlotHandle { pool: self, index })
    }

    /// スロット解放
    pub fn release_slot(&self, index: usize) {
        let slot = unsafe { &*self.slots.add(index) };
        slot.state.store(SlotState::Free as u8, Ordering::Release);

        let mut free_list = self.free_list.lock().unwrap();
        free_list.push(index);
    }
}
```

### 4.4 仮想マッピングテーブル

```rust
/// 接続からスロットへのマッピング
pub struct VirtualMapping {
    conn_to_slots: HashMap<ConnectionId, Vec<usize>>,
    slot_to_conn: Vec<Option<ConnectionId>>,
}

impl VirtualMapping {
    /// 接続にスロットをバインド
    pub fn bind(&mut self, conn_id: ConnectionId, slot_idx: usize) {
        self.conn_to_slots
            .entry(conn_id)
            .or_default()
            .push(slot_idx);
        self.slot_to_conn[slot_idx] = Some(conn_id);
    }

    /// 接続のスロットを全て解放
    pub fn unbind_all(&mut self, conn_id: ConnectionId) -> Vec<usize> {
        let slots = self.conn_to_slots.remove(&conn_id).unwrap_or_default();
        for &slot_idx in &slots {
            self.slot_to_conn[slot_idx] = None;
        }
        slots
    }
}
```

---

## 5. One-sided RDMA RPCプロトコル

### 5.1 リクエスト/レスポンスバッファ構造

```rust
/// リクエストヘッダー
#[repr(C, packed)]
pub struct RequestHeader {
    magic: u32,              // 0xDEADBEEF
    req_id: u64,             // リクエスト識別子
    rpc_type: u16,           // RPCタイプ
    payload_len: u16,        // ペイロード長
    client_slot_addr: u64,   // クライアントのレスポンススロットアドレス
    client_slot_rkey: u32,   // クライアントのrkey
}

/// レスポンスヘッダー
#[repr(C, packed)]
pub struct ResponseHeader {
    magic: u32,              // 0xBEEFDEAD
    req_id: u64,             // 対応するリクエストID
    status: u32,             // 0=成功, その他=エラーコード
    payload_len: u32,        // ペイロード長
}

const REQUEST_HEADER_SIZE: usize = 32;
const RESPONSE_HEADER_SIZE: usize = 20;
```

### 5.2 RPCフロー: クライアント側

```rust
impl RpcClient {
    /// RPC呼び出し
    pub async fn call(&self, rpc_type: u16, request: &[u8]) -> Result<Vec<u8>, Error> {
        // 1. ローカルスロット確保（レスポンス受信用）
        let local_slot = self.pool.allocate_slot()
            .ok_or(Error::NoSlot)?;

        // 2. 接続グループからコネクション取得
        let conn_guard = self.group_manager
            .select_group(self.server_node)
            .activate()?;
        let conn = conn_guard.connection();

        // 3. リクエストヘッダー構築
        let header = RequestHeader {
            magic: 0xDEADBEEF,
            req_id: self.next_req_id.fetch_add(1, Ordering::Relaxed),
            rpc_type,
            payload_len: request.len() as u16,
            client_slot_addr: local_slot.addr(),
            client_slot_rkey: local_slot.rkey(),
        };

        // 4. サーバーのリクエストバッファにRDMA WRITE
        let server_slot = conn.acquire_server_slot().await?;
        conn.rdma_write(
            server_slot.addr(),
            server_slot.rkey(),
            &[header.as_bytes(), request].concat(),
        ).await?;

        // 5. レスポンスをポーリング
        let response = self.poll_response(&local_slot, header.req_id).await?;

        Ok(response)
    }

    /// レスポンスポーリング（タイムアウト付き）
    async fn poll_response(&self, slot: &SlotHandle, req_id: u64) -> Result<Vec<u8>, Error> {
        let start = Instant::now();
        let timeout = Duration::from_millis(5000);

        loop {
            // スロットの状態チェック
            let header = slot.read_response_header();
            if header.magic == 0xBEEFDEAD && header.req_id == req_id {
                // レスポンス到着
                let payload = slot.read_payload(header.payload_len as usize);
                return Ok(payload);
            }

            if start.elapsed() > timeout {
                return Err(Error::Timeout);
            }

            // CPUスピン（短時間）→ イールド（長時間）
            if start.elapsed() < Duration::from_micros(100) {
                std::hint::spin_loop();
            } else {
                tokio::task::yield_now().await;
            }
        }
    }
}
```

### 5.3 RPCフロー: サーバー側

```rust
impl RpcServer {
    /// イベントループ
    pub async fn run(&self) {
        loop {
            // 1. リクエストバッファをポーリング
            for slot_idx in 0..self.pool.num_slots {
                let slot = self.pool.slot(slot_idx);

                // 新しいリクエストがあるかチェック
                let header = slot.read_request_header();
                if header.magic != 0xDEADBEEF {
                    continue;
                }

                // 2. リクエスト処理
                let request = slot.read_payload(header.payload_len as usize);
                let response = self.handle_rpc(header.rpc_type, &request).await;

                // 3. レスポンスをクライアントにRDMA WRITE
                let resp_header = ResponseHeader {
                    magic: 0xBEEFDEAD,
                    req_id: header.req_id,
                    status: 0,
                    payload_len: response.len() as u32,
                };

                self.rdma_write(
                    header.client_slot_addr,
                    header.client_slot_rkey,
                    &[resp_header.as_bytes(), &response].concat(),
                ).await?;

                // 4. スロットをクリア
                slot.clear();
            }
        }
    }
}
```

---

## 6. ワーカースケジューリング

### 6.1 ディスパッチスレッドモデル

FaSSTやeRPCと同様に、専用のディスパッチスレッドを使用する。

```rust
/// ワーカー構成
pub struct WorkerConfig {
    num_dispatch_threads: usize,  // ディスパッチスレッド数
    num_worker_threads: usize,    // ワーカースレッド数
    requests_per_batch: usize,    // バッチサイズ
}

/// ディスパッチスレッド
pub struct DispatchThread {
    id: usize,
    rx_queue: CQ,                 // 受信完了キュー
    worker_queues: Vec<mpsc::Sender<Request>>,
    current_worker: AtomicUsize,
}

impl DispatchThread {
    pub fn run(&self) {
        loop {
            // CQをポーリング
            let completions = self.rx_queue.poll(32);

            for wc in completions {
                let request = self.decode_request(&wc);

                // ワーカーにラウンドロビン配分
                let worker_idx = self.current_worker
                    .fetch_add(1, Ordering::Relaxed) % self.worker_queues.len();
                self.worker_queues[worker_idx]
                    .send(request)
                    .expect("worker queue full");
            }
        }
    }
}
```

### 6.2 コルーチンベースワーカー

```rust
/// ワーカースレッド（async runtime使用）
pub struct WorkerThread {
    id: usize,
    request_rx: mpsc::Receiver<Request>,
    response_tx: mpsc::Sender<Response>,
    handlers: HashMap<u16, Box<dyn RpcHandler>>,
}

impl WorkerThread {
    pub async fn run(&self) {
        while let Some(request) = self.request_rx.recv().await {
            // ハンドラー呼び出し
            let handler = self.handlers
                .get(&request.rpc_type)
                .expect("unknown RPC type");

            let response = handler.handle(request.payload).await;

            self.response_tx
                .send(Response {
                    req_id: request.req_id,
                    client_addr: request.client_slot_addr,
                    client_rkey: request.client_slot_rkey,
                    payload: response,
                })
                .await
                .expect("response queue full");
        }
    }
}
```

---

## 7. copyrpc/mlx5との統合

### 7.1 活用する既存コンポーネント

| コンポーネント | 用途 |
|----------------|------|
| `mlx5::rc::RcQp` | RC QP作成・管理 |
| `mlx5::cq::Cq` | 完了キューポーリング |
| `mlx5::mr::Mr` | メモリ領域登録 |
| `mlx5::wqe::emit_wqe!` | 高速WQE発行 |
| `mlx5::hugepage` | HugePageメモリ確保 |

### 7.2 新規モジュール構成

```
copyrpc/
├── src/
│   ├── scalerpc/
│   │   ├── mod.rs
│   │   ├── group.rs         # Connection Grouping
│   │   ├── pool.rs          # Message Pool
│   │   ├── mapping.rs       # Virtualized Mapping
│   │   ├── client.rs        # RPCクライアント
│   │   ├── server.rs        # RPCサーバー
│   │   ├── protocol.rs      # ヘッダー定義
│   │   └── worker.rs        # ワーカースケジューリング
```

### 7.3 RC QP設定

```rust
/// RC QP設定パラメータ
pub struct RcQpConfig {
    pub send_queue_size: u32,      // 128
    pub recv_queue_size: u32,      // 512
    pub max_send_sge: u32,         // 1
    pub max_recv_sge: u32,         // 1
    pub max_inline_data: u32,      // 60
    pub timeout: u8,               // 14 (約4秒)
    pub retry_cnt: u8,             // 7
    pub rnr_retry: u8,             // 7
}

impl Default for RcQpConfig {
    fn default() -> Self {
        Self {
            send_queue_size: 128,
            recv_queue_size: 512,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 60,
            timeout: 14,
            retry_cnt: 7,
            rnr_retry: 7,
        }
    }
}
```

---

## 8. RDMA操作の詳細

### 8.1 RDMA WRITE (リクエスト送信)

```rust
impl Connection {
    /// RDMA WRITEでリクエスト送信
    pub fn send_request(
        &self,
        remote_addr: u64,
        remote_rkey: u32,
        data: &[u8],
    ) -> Result<(), Error> {
        // インライン判定
        let inline = data.len() <= 60;

        emit_wqe!(
            self.qp,
            opcode: RDMA_WRITE,
            remote_addr: remote_addr,
            rkey: remote_rkey,
            local_addr: if inline { 0 } else { data.as_ptr() as u64 },
            lkey: if inline { 0 } else { self.mr.lkey() },
            len: data.len() as u32,
            inline_data: if inline { Some(data) } else { None },
            signaled: self.should_signal(),
        )?;

        // Doorbell ring
        self.qp.ring_doorbell();

        Ok(())
    }
}
```

### 8.2 RDMA READ (レスポンスポーリング)

```rust
impl RpcClient {
    /// RDMA READでレスポンス取得（オプション）
    pub fn fetch_response(
        &self,
        conn: &Connection,
        remote_addr: u64,
        remote_rkey: u32,
        local_buf: &mut [u8],
    ) -> Result<(), Error> {
        emit_wqe!(
            conn.qp,
            opcode: RDMA_READ,
            remote_addr: remote_addr,
            rkey: remote_rkey,
            local_addr: local_buf.as_ptr() as u64,
            lkey: self.mr.lkey(),
            len: local_buf.len() as u32,
            signaled: true,
        )?;

        conn.qp.ring_doorbell();

        // 完了待機
        let wc = conn.cq.poll_blocking()?;
        if wc.status != IBV_WC_SUCCESS {
            return Err(Error::RdmaError(wc.status));
        }

        Ok(())
    }
}
```

### 8.3 Doorbell Batching

```rust
/// Doorbell Batchingによる最適化
pub struct DoorbellBatcher {
    qp: *mut Qp,
    pending_wqes: usize,
    batch_threshold: usize,
}

impl DoorbellBatcher {
    pub fn post_wqe(&mut self, /* wqe params */) {
        // WQE書き込み（doorbellなし）
        self.pending_wqes += 1;

        // バッチ閾値に達したらdoorbell
        if self.pending_wqes >= self.batch_threshold {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        if self.pending_wqes > 0 {
            unsafe { (*self.qp).ring_doorbell() };
            self.pending_wqes = 0;
        }
    }
}
```

---

## 9. 定数とパラメータ

```rust
/// ScaleRPC設定定数
pub mod constants {
    // NICキャッシュ関連
    pub const NIC_QPC_CACHE_ENTRIES_CX3: usize = 400;
    pub const NIC_QPC_CACHE_ENTRIES_CX5: usize = 1000;
    pub const SAFE_CACHE_RATIO: f64 = 0.8;

    // グループ管理
    pub const DEFAULT_NUM_GROUPS: usize = 8;
    pub const MAX_ACTIVE_PER_GROUP: usize = 50;

    // メッセージプール
    pub const DEFAULT_SLOT_SIZE: usize = 4096;
    pub const DEFAULT_NUM_SLOTS: usize = 8192;
    pub const SLOT_HEADER_SIZE: usize = 64;

    // タイムアウト
    pub const RPC_TIMEOUT_MS: u64 = 5000;
    pub const POLL_SPIN_US: u64 = 100;

    // バッチング
    pub const DOORBELL_BATCH_SIZE: usize = 16;
    pub const COMPLETION_BATCH_SIZE: usize = 32;

    // キューサイズ
    pub const SEND_QUEUE_SIZE: u32 = 128;
    pub const RECV_QUEUE_SIZE: u32 = 512;
    pub const MAX_INLINE_DATA: u32 = 60;
}
```

---

## 10. 実装フェーズ

### Phase 1: 基盤

- [ ] `MessagePool` とHugePageアロケータ
- [ ] スロット管理（割り当て/解放）
- [ ] RDMA Memory Region登録

### Phase 2: Connection Grouping

- [ ] `ConnectionGroup` 構造体
- [ ] グループマネージャー
- [ ] アクティブ接続制限

### Phase 3: Virtualized Mapping

- [ ] 仮想マッピングテーブル
- [ ] 接続とスロットのバインディング

### Phase 4: RPCプロトコル

- [ ] リクエスト/レスポンスヘッダー
- [ ] RDMA WRITE送信
- [ ] レスポンスポーリング

### Phase 5: ワーカー管理

- [ ] ディスパッチスレッド
- [ ] ワーカースレッドプール
- [ ] RPC ハンドラー登録

### Phase 6: 最適化・テスト

- [ ] Doorbell Batching
- [ ] インライン送信
- [ ] ベンチマーク

---

## 11. eRPCとの比較

| 項目 | ScaleRPC | eRPC |
|------|----------|------|
| トランスポート | RC (Reliable Connection) | UD (Unreliable Datagram) |
| RDMA操作 | One-sided (WRITE/READ) | Two-sided (SEND/RECV) |
| 信頼性 | ハードウェア (RC) | ソフトウェア (Go-Back-N) |
| 輻輳制御 | なし | Timely |
| スケーラビリティ対策 | Connection Grouping | UD QP共有 |
| メモリ効率 | Virtualized Mapping | MsgBuffer |

---

## 12. 参照資料

### 論文
- [ScaleRPC (EuroSys 2019)](https://dl.acm.org/doi/10.1145/3302424.3303968)
- [Design Guidelines for High Performance RDMA Systems (ATC 2016)](https://www.usenix.org/conference/atc16/technical-sessions/presentation/kalia)
- [FaSST (OSDI 2016)](https://www.usenix.org/conference/osdi16/technical-sessions/presentation/kalia)
- [RFP: When RPC is Faster (EuroSys 2017)](https://blog.acolyer.org/2017/06/07/rfp-when-rpc-is-faster-than-server-bypass-with-rdma/)

### 既存copyrpcコード
- `mlx5/src/rc/mod.rs` - RC QP
- `mlx5/src/mr/mod.rs` - Memory Region
- `mlx5/src/wqe/emit.rs` - WQE発行

---

## 13. 検証方法

1. **マイクロベンチマーク**: 単一RPC往復レイテンシ測定
2. **スケーラビリティテスト**: 接続数増加時のスループット測定
3. **NICキャッシュ効果**: perf/rdma-coreでQPCキャッシュミス率測定
4. **比較評価**: eRPC実装との性能比較
