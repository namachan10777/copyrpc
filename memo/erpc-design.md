# eRPC Rust実装 設計ドキュメント

## 概要

eRPC (NSDI 2019 Best Paper) をRustで実装するための設計ドキュメント。
既存のcopyrpc/mlx5コードベースを活用し、高性能データセンターRPCを実現する。

**参照リポジトリ**: `/local/mnakano/ghq/github.com/erpc-io/eRPC`

---

## 1. eRPCの核心設計

### 1.1 設計原則

| 原則 | 説明 |
|------|------|
| CPU管理接続状態 | NICではなくCPUキャッシュで接続状態を管理 |
| クライアント駆動 | サーバーはクライアントのパケットに応答するのみ |
| ゼロコピー | 連続MsgBufferレイアウトでDMA効率化 |
| コモンケース最適化 | 小メッセージ・短ハンドラ・無輻輳を最適化 |

### 1.2 性能特性

- **スループット**: 1CPUコアあたり約1000万RPS
- **レイテンシ**: 2.3μs (中央値)
- **大メッセージ**: 75 Gbps (8MB時)
- **スケーラビリティ**: 20,000セッション/サーバー

---

## 2. パケットフォーマット

### 2.1 パケットヘッダー (16バイト)

```rust
#[repr(C, packed)]
pub struct PktHdr {
    pub req_type: u8,           // RPCリクエストタイプ
    pub msg_size: [u8; 3],      // メッセージサイズ (24ビット、最大16MB)
    pub dest_session_num: u16,  // 宛先セッション番号
    pub pkt_type_num: u16,      // パケットタイプ(2b) + パケット番号(14b)
    pub req_num: [u8; 6],       // リクエスト番号 (44ビット)
    pub magic: u8,              // デバッグ用 (4ビット)
}

#[repr(u8)]
pub enum PktType {
    Req = 0,     // リクエストデータ
    Rfr = 1,     // Request for Response
    ExplCr = 2,  // 明示的クレジット返却
    Resp = 3,    // レスポンスデータ
}
```

### 2.2 定数

```rust
const SESSION_CREDITS: usize = 8;    // セッションあたりのクレジット
const SESSION_REQ_WINDOW: usize = 8; // 同時リクエスト数
const MTU: usize = 4096;             // InfiniBand MTU
const MAX_MSG_SIZE: usize = 1 << 24; // 16MB
```

---

## 3. MsgBuffer設計

### 3.1 メモリレイアウト

```
┌────────────────────────────────────────────────────────┐
│  pkthdr_0  │     data (連続領域)     │ pkthdr_1..N-1  │
│   (16B)    │                         │    各16B       │
└────────────────────────────────────────────────────────┘
```

- パケット0: ヘッダー+データが連続（1 DMA読み取りで送信可能）
- パケット1+: ヘッダーは末尾に配置、データ領域の連続性を確保

### 3.2 実装

```rust
pub struct MsgBuffer {
    buffer: HugePageBuffer,     // HugePageバッキング
    max_data_size: usize,       // 最大データサイズ
    data_size: usize,           // 現在のデータサイズ
    num_pkts: usize,            // パケット数
    buf: *mut u8,               // データポインタ (pkthdr_0の直後)
}
```

---

## 4. セッション管理

### 4.1 セッション構造

```rust
pub struct Session {
    role: SessionRole,              // Client/Server
    state: SessionState,            // Connected/Disconnecting
    local_session_num: u16,
    remote_session_num: u16,
    sslot_arr: [SSlot; 8],          // リクエストスロット
    client_info: Option<ClientInfo>,
    remote_routing: RoutingInfo,    // AH + QPN
}

pub struct ClientInfo {
    credits: usize,                 // 利用可能クレジット
    free_slots: Vec<usize>,         // 空きスロット
    backlog: VecDeque<EnqReqArgs>,  // バックログ
    timely: Timely,                 // 輻輳制御
}
```

### 4.2 セッションスロット (SSlot)

```rust
pub struct SSlot {
    session: *mut Session,
    index: usize,
    tx_msgbuf: Option<*mut MsgBuffer>,
    cur_req_num: u64,
    info: SSlotInfo,  // Client or Server specific
}

pub struct ClientSSlotInfo {
    resp_msgbuf: *mut MsgBuffer,
    cont_func: ContFunc,           // 継続コールバック
    num_tx: usize,                 // 送信パケット数
    num_rx: usize,                 // 受信パケット数
    progress_tsc: u64,             // タイムアウト用
    tx_ts: [u64; 8],               // RTT計測用
}
```

---

## 5. クレジットベースフロー制御

### 5.1 動作

1. 各セッションは8クレジットを持つ
2. パケット送信でクレジット消費
3. CR (Credit Return) またはレスポンスでクレジット回復
4. クレジット0でバックログキューに入れる

### 5.2 実装ポイント

```rust
impl Session {
    fn consume_credit(&mut self) -> bool {
        if self.client_info.credits > 0 {
            self.client_info.credits -= 1;
            true
        } else {
            false
        }
    }

    fn bump_credits(&mut self) {
        self.client_info.credits += 1;
    }
}
```

---

## 6. Timely輻輳制御

### 6.1 パラメータ

```rust
const MIN_RATE: f64 = 15e6;      // 15 Mbps
const ADD_RATE: f64 = 5e6;       // 5 Mbps (加法的増加)
const BETA: f64 = 0.8;           // 乗法的減少係数
const T_LOW: f64 = 50.0;         // 50μs
const T_HIGH: f64 = 1000.0;      // 1000μs
const EWMA_ALPHA: f64 = 0.02;
```

### 6.2 レート更新ロジック

```rust
impl Timely {
    fn update_rate(&mut self, sample_rtt_us: f64) {
        self.rate = if sample_rtt_us < T_LOW {
            // 加法的増加
            self.rate + ADD_RATE
        } else if sample_rtt_us <= T_HIGH {
            // 混合モード
            self.mixed_mode_update(sample_rtt_us)
        } else {
            // 乗法的減少
            self.rate * (1.0 - BETA * (1.0 - T_HIGH / sample_rtt_us))
        };
        self.rate = self.rate.max(MIN_RATE).min(self.link_bw);
    }
}
```

---

## 7. 信頼性 (Go-Back-N)

### 7.1 再送信メカニズム

- **タイムアウト**: 5ms (`RPC_RTO_MS`)
- **検出**: `num_tx > num_rx` かつ進捗なしでタイムアウト
- **動作**: `num_tx = num_rx` にロールバックし、未確認パケットを再送

### 7.2 At-Most-Once保証

- `req_num`でリクエストを一意に識別
- サーバーは同一`req_num`のハンドラを2回実行しない

---

## 8. ワイヤプロトコル

### 8.1 小メッセージ (1パケット)

```
Client ──── REQ (pkt_num=0) ────► Server
       ◄─── RESP (pkt_num=0) ───
```

### 8.2 大メッセージ (マルチパケット)

```
Client ──── REQ pkt_0 ────────► Server
       ──── REQ pkt_1 ────────►
       ◄──── CR (pkt_0) ────────  (クレジット返却)
       ◄──── RESP pkt_0 ────────  (pkt_num = num_req_pkts - 1 + resp_idx)
       ──── RFR ────────────────► (残りレスポンス要求)
       ◄──── RESP pkt_1 ────────
```

---

## 9. copyrpc/mlx5との統合

### 9.1 変更点

| 項目 | 現在 (RC) | eRPC方式 (UD) |
|------|-----------|---------------|
| QP種別 | Reliable Connection | Unreliable Datagram |
| 信頼性 | ハードウェア | Go-Back-N (ソフトウェア) |
| フロー制御 | なし | クレジットベース |
| 輻輳制御 | なし | Timely |

### 9.2 活用する既存コンポーネント

- `mlx5::ud::UdQp` - UD QP作成・管理
- `mlx5::cq::{Cq, MonoCq}` - CQポーリング
- `mlx5::srq::Srq` - 共有受信キュー
- `mlx5::wqe::emit_wqe!` - 高速WQE発行

### 9.3 新規モジュール構成

```
copyrpc/
├── src/
│   ├── erpc/
│   │   ├── mod.rs
│   │   ├── pkthdr.rs       # パケットヘッダー
│   │   ├── msg_buffer.rs   # MsgBuffer
│   │   ├── session.rs      # セッション管理
│   │   ├── sslot.rs        # セッションスロット
│   │   ├── rpc.rs          # Rpcコンテキスト
│   │   ├── transport.rs    # UDトランスポート
│   │   ├── timely.rs       # 輻輳制御
│   │   └── reliability.rs  # Go-Back-N
```

---

## 10. 実装フェーズ

### Phase 1: 基盤

- [ ] `PktHdr` 構造体とエンコード/デコード
- [ ] `MsgBuffer` とHugePageアロケータ
- [ ] 基本的なテスト

### Phase 2: セッション管理

- [ ] `Session`/`SSlot` 構造体
- [ ] セッション確立プロトコル
- [ ] クレジットベースフロー制御

### Phase 3: データパス

- [ ] UD経由の送受信
- [ ] リクエスト/レスポンスワイヤプロトコル
- [ ] Go-Back-N再送信

### Phase 4: 輻輳制御

- [ ] Timely実装
- [ ] タイミングホイール（ペーシング）

### Phase 5: 最適化・テスト

- [ ] ベンチマーク
- [ ] BlueFlame最適化
- [ ] 統合テスト

---

## 11. 参照ファイル

### eRPCソースコード
- `/local/mnakano/ghq/github.com/erpc-io/eRPC/src/pkthdr.h` - パケットヘッダー
- `/local/mnakano/ghq/github.com/erpc-io/eRPC/src/session.h` - セッション構造
- `/local/mnakano/ghq/github.com/erpc-io/eRPC/src/sslot.h` - スロット構造
- `/local/mnakano/ghq/github.com/erpc-io/eRPC/src/rpc_impl/` - コア実装
- `/local/mnakano/ghq/github.com/erpc-io/eRPC/src/cc/timely.h` - 輻輳制御

### 既存copyrpcコード
- `/local/mnakano/ghq/github.com/namachan10777/copyrpc/mlx5/src/ud/mod.rs` - UD QP
- `/local/mnakano/ghq/github.com/namachan10777/copyrpc/mlx5/src/wqe/emit.rs` - WQE発行
- `/local/mnakano/ghq/github.com/namachan10777/copyrpc/copyrpc/src/lib.rs` - 既存RPC

---

## 12. C++実装の詳細解析

### 12.1 イベントループ構造

```
run_event_loop_do_one_st():
  1. handle_sm_rx_st()           # セッション管理パケット処理
  2. ev_loop_tsc_ = rdtsc()      # タイムスタンプ更新
  3. process_comps_st()          # RX完了処理
  4. process_credit_stall_queue_st()  # クレジット待機キュー
  5. process_wheel_st()          # タイミングホイール
  6. do_tx_burst_st()            # TX一括送信
  7. pkt_loss_scan_st()          # パケット損失検出（周期的）
```

### 12.2 受信処理フロー

```
process_comps_st():
  ├─ rx_burst() - トランスポートからパケット一括受信
  ├─ パケット検証ループ:
  │   ├─ check_magic() - マジックナンバー検証
  │   ├─ セッション存在・状態確認
  │   └─ パケットタイプ別処理:
  │       ├─ kReq  → process_small_req_st/process_large_req_one_st
  │       ├─ kResp → process_resp_one_st（RTT計測付き）
  │       ├─ kRFR  → process_rfr_st
  │       └─ kExplCR → process_expl_cr_st
  └─ post_recvs(num_pkts) - RECVディスクリプタ再ポスト
```

### 12.3 送信キック処理

**kick_req_st(SSlot*):**
```
1. credits_ チェック（フロー制御）
2. 送信パケット数 = min(credits, num_pkts - num_tx)
3. bypass判定:
   ├─ true:  直接 enqueue_pkt_tx_burst_st()
   └─ false: タイミングホイールへ enqueue_wheel_req_st()
```

### 12.4 セッション作成フロー

**クライアント側 (create_session_st):**
```
1. 引数検証 (dispatch()スレッド確認、リソース確認)
2. Session作成 (Role::kClient, state=kConnectInProgress)
3. send_sm_req_st() - UDP経由で接続要求
4. 戻り値: session_num
```

**サーバー側 (handle_connect_req_st):**
```
1. 重複接続検出 (conn_req_token_map_)
2. トランスポート検証 (MTU, リソース)
3. ルーティング情報解決
4. Session作成 (Role::kServer, state=kConnected)
5. pre_resp_msgbuf_ 事前割当 (8個)
6. sm_pkt_udp_tx_st(resp) - レスポンス送信
```

### 12.5 IBトランスポートパラメータ

| パラメータ | 値 | 説明 |
|-----------|-----|------|
| kMTU | 3840 (IB), 1024 (RoCE) | 最大転送単位 |
| kRQDepth | 512 | RECV Queue深さ |
| kSQDepth | 128 | SEND Queue深さ |
| kUnsigBatch | 64 | シグナリング間隔 |
| kMaxInline | 60 | インライン送信上限 |
| kRecvSlack | 32 | RECVバッチ閾値 |
| kQKey | 0x0205 | UD Queue Key |

### 12.6 パケット損失処理

```
pkt_loss_scan_st():
  for each SSlot in active_rpcs:
    if num_tx == num_rx:
      continue  # クレジット停滞
    if ev_loop_tsc - progress_tsc > rto_cycles:
      pkt_loss_retransmit_st(sslot)

pkt_loss_retransmit_st():
  1. delta = num_tx - num_rx
  2. credits += delta           # クレジット回復
  3. num_tx = num_rx            # ロールバック
  4. progress_tsc = ev_loop_tsc # タイムスタンプ更新
  5. kick_req_st() or kick_rfr_st()  # 再送信
```

### 12.7 タイミングホイール構造

```cpp
struct wheel_bkt_t {
    size_t num_entries_ : 3;      // エントリ数（最大5）
    size_t tx_tsc_ : 61;          // 送信予定TSC
    wheel_bkt_t *last_, *next_;   // チェーン
    wheel_ent_t entry_[5];        // パケット参照
};

kWheelSlotWidthUs = 0.5;          // 500ns/スロット
kWheelNumWslots = 2000001;        // 約1秒分
```

### 12.8 Rust実装時の注意点

| C++パターン | Rust対応 |
|------------|----------|
| 生ポインタ相互参照 | `Rc<RefCell<T>>` または arena allocator |
| ビットフィールド | `bitfield` crateまたは手動マスク |
| インライン送信判定 | `if msg.len() <= 60` |
| rdtsc()キャッシング | `Cell<u64>` でイベントループ内共有 |
| セレクティブシグナリング | カウンタで64パケットごとにシグナル |

**状態管理の複雑性:**
- SessionState: `ConnectInProgress → Connected → DisconnectInProgress`
- SSlot: `num_tx`, `num_rx`, `progress_tsc`, `in_wheel[]`, `wheel_count`

**パフォーマンス最適化:**
1. タイムスタンプキャッシング（ev_loop_tsc_再利用）
2. セレクティブシグナリング（64pkt/signal）
3. インライン送信（≤60B）
4. バッチ処理（tx_burst_item_t[]）
5. ホイール最適化（bucket容量5）

---

## 13. 検証方法

1. **ユニットテスト**: 各コンポーネント（PktHdr、MsgBuffer、Session等）の単体テスト
2. **統合テスト**: セッション確立→RPC往復→セッション切断のe2eテスト
3. **ベンチマーク**: `cargo bench --bench pingpong` で既存copyrpcと比較
4. **ストレステスト**: 複数セッション・パケットロス注入下での動作確認
