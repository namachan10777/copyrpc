# eRPC: 論文忠実な詳細設計擬似コード

対象論文: *Datacenter RPCs can be General and Fast* (NSDI 2019)  
対象範囲: 論文本文 Section 3-5 + Appendix A/B/C を実装仕様へ落とし込み

## 0. 忠実性ポリシー
- `論文明記`: 論文に直接書かれた仕様
- `実装補完`: 論文に必要十分な詳細がない箇所を、論文の制約を破らない最小限の補完で明示

## 1. 設計ゴールと必須制約
- `論文明記` 非同期 RPC、at-most-once、クライアント駆動プロトコル
- `論文明記` 小サイズ RPC の common case 最適化を最優先
- `論文明記` セッション単位 credit による flow control
- `論文明記` 損失時は go-back-N、再送時は state rollback
- `論文明記` 輻輳制御は Timely (RTT-based) + rate limiter (Carousel)
- `論文明記` 再送/障害時を除き unsignaled TX で高速化

## 2. 定数・パラメータ
- `MAX_INFLIGHT_PER_SESSION = 8` (デフォルト同時 outstanding RPC 数)
- `TIMELY_LOW_THRESHOLD_US = 50` (Timely bypass 閾値)
- `RTO_MS = 5` (再送タイムアウト、実験設定)
- `SMALL_CTRL_PKT_SIZE = 16B` (`CR`, `RFR`)
- `TXQ_ENTRIES = 64`, `TX_CQ_ENTRIES = 64` (Appendix A)
- `RQ_STRIDE = 512` (multi-packet RQ descriptor の既定利用形態)
- `RX_CQ_ENTRIES = 8` (overrun 許容ポーリング)

## 3. データ構造
```text
enum PktType { REQ_DATA, RESP_DATA, CR, RFR }

struct PacketHeader {
  // 論文明記: transport header + request handler type + sequence numbers
  pkt_type: PktType
  session_id: u32                // 実装補完: 復元に必要
  req_type: u16                  // handler type
  seq_no: u32                    // message/packet sequence
  pkt_idx: u16                   // 実装補完: 多パケット再構成に必要
  pkt_cnt: u16                   // 実装補完: 多パケット再構成に必要
  slot_id: u16                   // 実装補完: out-of-order completion 管理
}

struct MsgBuf {
  // 論文明記: 1つの msgbuf は 1 message (multi-packet 可)
  // 論文明記: packet 1 は [header|data] 連続
  // 論文明記: packet >=2 は header 群を末尾に配置して data 連続性維持
  buf: dma_memory
  data_len: usize
}

struct Slot {
  in_use: bool
  req_msgbuf: MsgBuf
  resp_msgbuf: MsgBuf
  cont: Continuation

  req_pkt_total: u16
  req_pkt_next_to_send: u16
  req_pkt_acked: u16

  resp_pkt_total: u16
  resp_pkt_received: u16

  retransmit_deadline: time
  retransmitting: bool
  retransmit_in_rate_limiter: bool   // Appendix C
  completed: bool
}

struct Session {
  id: u32
  role: {CLIENT_MODE, SERVER_MODE}
  credits: i32                        // 論文明記: sendで消費、recvで補充
  credit_cap: i32                     // C
  slots[8]: Slot
  pending_reqs: queue<QueuedRequest>

  // client-side congestion control
  timely_state: TimelyState
  uncongested: bool
}

struct RpcEndpoint {
  rxq, txq
  rx_cq, tx_cq
  sessions: map<u32, Session>
  worker_pool: optional
  mgmt_thread: SessionMgmtThread
  rate_limiter: CarouselLikeLimiter
}
```

## 4. 不変条件
- `I1` continuation 実行時点で、当該 request msgbuf 参照が TX DMA queue / rate limiter に残っていない
- `I2` セッション内 outstanding packet 数は credit 制約を超えない (誤検知再送時の稀な逸脱は論文許容)
- `I3` サーバは同一 RPC request を二重実行しない (at-most-once)
- `I4` サーバ送信は client 駆動 (`CR`, `RESP` は受信 client packet への応答としてのみ進行)

## 5. 初期化
```text
function rpc_init(config):
  ep = RpcEndpoint()
  ep.txq = nic_create_txq(TXQ_ENTRIES)
  ep.tx_cq = nic_create_tx_cq(TX_CQ_ENTRIES)

  // Appendix A: multi-packet RQ + overrun RX CQ
  ep.rxq = nic_create_rxq_with_mprq(stride=RQ_STRIDE)
  ep.rx_cq = nic_create_rx_cq(entries=RX_CQ_ENTRIES, allow_overrun=true)

  ep.rate_limiter = carousel_init()
  ep.mgmt_thread.start()
  return ep
```

## 6. 送信側制御 (Client)

### 6.1 enqueue_request
```text
function enqueue_request(ep, session_id, req_msgbuf, resp_msgbuf, cont):
  s = ep.sessions[session_id]
  if no_free_slot(s):
    s.pending_reqs.push({req_msgbuf, resp_msgbuf, cont})
    return

  slot = alloc_slot(s)
  slot_init_for_new_rpc(slot, req_msgbuf, resp_msgbuf, cont)
  send_request_window(ep, s, slot)
```

### 6.2 request window 送信
```text
function send_request_window(ep, s, slot):
  // 論文明記: client begins by sending up to C request packets
  while slot.req_pkt_next_to_send < slot.req_pkt_total and s.credits > 0:
    pkt = make_req_data_pkt(s, slot, slot.req_pkt_next_to_send)
    post_data_or_rate_limited(ep, s, pkt)
    slot.req_pkt_next_to_send += 1
    s.credits -= 1

  arm_retransmission_timer(slot, RTO_MS)
```

### 6.3 congestion control (Timely + bypass)
```text
function post_data_or_rate_limited(ep, s, pkt):
  if s.uncongested:
    nic_post_send_unsignaled(ep.txq, pkt)        // common case
  else:
    ep.rate_limiter.enqueue(s.id, pkt)

function on_client_rx_feedback(s, rtt_sample):
  // bypass 1: Timely update skip
  if s.uncongested and rtt_sample < TIMELY_LOW_THRESHOLD_US:
    return

  timely_update_rate(s.timely_state, rtt_sample)  // Timely本体は論文[52]準拠
  s.uncongested = (timely_state_rate_is_max(s.timely_state))

function service_rate_limiter(ep):
  // bypass 2: uncongested session は limiter を使わない設計
  ep.rate_limiter.drain_ready_packets(nic_post_send_unsignaled)

// bypass 3: RTT timestamp は packet単位でなく batch単位
function event_loop_timestamp_once_per_batch():
  return rdtsc_or_clock_now()
```

## 7. 受信側制御 (Server)

### 7.1 request packet 処理
```text
function on_server_recv_req_data(ep, s, pkt):
  slot = locate_or_create_server_slot(s, pkt)
  append_packet_to_server_reassembly(slot, pkt)

  if not is_last_request_packet(pkt):
    cr = make_credit_return_pkt(s, slot)
    nic_post_send_unsignaled(ep.txq, cr)         // 明示 credit return
    return

  // 最終 request packet 到着: first response が暗黙 credit return を兼ねる
  req_msg = finalize_request_message(slot)

  if already_executed(req_msg.sequence_key):
    resp = replay_or_fetch_cached_response(req_msg.sequence_key)  // 実装補完
  else:
    resp = execute_handler_once(req_msg)       // dispatch mode or worker mode
    mark_executed(req_msg.sequence_key)

  send_first_response_packet(ep, s, slot, resp)
```

### 7.2 zero-copy request processing (common case)
```text
function dispatch_mode_handle_without_copy_if_possible(rx_pkt_buf):
  // 論文明記: NIC buffer descriptor を RQ に戻すまで eRPC が所有
  // single-packet + dispatch-mode では copy を省略可能
  handler(rx_pkt_buf.payload_view)
```

## 8. クライアント駆動 wire protocol

### 8.1 packet 受信ハンドラ
```text
function on_client_recv_packet(ep, s, pkt):
  slot = find_slot(s, pkt.slot_id)

  if pkt.type == CR:
    s.credits += 1
    try_send_more_req_data(ep, s, slot)
    return

  if pkt.type == RESP_DATA:
    // Appendix C: retransmitted copy が limiter 内にある間の response は drop
    if slot.retransmit_in_rate_limiter:
      drop(pkt)
      return

    ingest_response_data(slot, pkt)
    s.credits += 1                      // first resp による implicit credit return を含む
    on_client_rx_feedback(s, sample_rtt_for_pkt(pkt))

    if slot.resp_pkt_received == 1 and slot.resp_pkt_total > 1:
      // client-driven: 後続 response を引き出すため RFR を送る
      rfr = make_rfr_pkt(s, slot)
      post_data_or_rate_limited(ep, s, rfr)

    if response_complete(slot):
      complete_rpc_and_invoke_continuation(ep, s, slot)
    return
```

### 8.2 completion 処理
```text
function complete_rpc_and_invoke_continuation(ep, s, slot):
  cancel_retransmission_timer(slot)
  ensure_no_msgbuf_reference_before_cont(slot)   // I1
  slot.cont(success, slot.resp_msgbuf)
  free_slot(s, slot)
  if not s.pending_reqs.empty():
    req = s.pending_reqs.pop()
    enqueue_request(ep, s.id, req.req_msgbuf, req.resp_msgbuf, req.cont)
```

## 9. 損失処理・再送
```text
function on_retransmission_timeout(ep, s, slot):
  // 論文明記: go-back-N rollback
  rollback_wire_protocol_state(slot)
  reclaimed = count_rolled_back_unacked_packets(slot)
  s.credits += reclaimed
  slot.retransmitting = true

  // 論文明記: unsignaled TX のため、再送 packet queue 後に TX DMA queue flush
  enqueue_retransmission_packets(ep, s, slot)
  nic_flush_tx_dma_queue_blocking(ep.txq)   // ~2us cost, rare path

  // Appendix C: limiter 内再送参照が消えるまで response を drop
  if retransmission_enqueued_to_rate_limiter(slot):
    slot.retransmit_in_rate_limiter = true

  arm_retransmission_timer(slot, RTO_MS)
```

## 10. ノード障害処理 (Appendix B)
```text
thread SessionMgmtThread:
  loop:
    process_session_create_destroy_msgs()
    if detect_remote_node_failure(node):
      notify_dispatch_threads(node)

function on_dispatch_thread_remote_failure(ep, node):
  nic_flush_tx_dma_queue_blocking(ep.txq)

  for s in sessions_to_node(node):
    if s.role == CLIENT_MODE:
      wait_rate_limiter_packets_for_session_to_drain(s)
      for slot in inflight_slots(s):
        slot.cont(error_remote_failure, empty_resp)
        free_slot(s, slot)
    else: // SERVER_MODE
      wait_nonblocking_until_no_unreplied_handlers(s)
      free_session_resources(s)
```

## 11. サーバ/クライアント event loop
```text
function run_event_loop_once(ep):
  now = event_loop_timestamp_once_per_batch()

  rx_batch = poll_rx_packets_from_overrunning_cq(ep.rx_cq)
  for pkt in rx_batch:
    if is_server_session(pkt.session_id):
      on_server_recv_req_data(ep, session(pkt), pkt)
    else:
      on_client_recv_packet(ep, session(pkt), pkt)

  for s in ep.sessions where s.role == CLIENT_MODE:
    for slot in inflight_slots(s):
      if timer_expired(slot.retransmit_deadline, now):
        on_retransmission_timeout(ep, s, slot)

  service_rate_limiter(ep)
```

## 12. 実装補完が必要な未規定点
- Timely の式・係数は本論文本文では省略 (Timely 論文[52]の採用が前提)
- Carousel rate limiter 内部データ構造は本論文で省略
- packet header の完全フィールド定義は本文で省略 (最低限 `handler type`, `sequence numbers` は必須)
- at-most-once を満たす重複検出テーブル構造は本文で省略

## 13. 実装時チェックリスト
- 再送時 `TX DMA flush` を必ず実行 (unsignaled TX の安全条件)
- `CR` と `RFR` を 16B 制御 packet として実装
- セッション credit を `sendで減算 / recvで加算` に厳密一致
- `client-driven` を破る server 自律送信を禁止
- `single-packet request + dispatch handler` の zero-copy fast path を有効化
- RX CQ overrun と multi-packet RQ descriptor 前提を transport 初期化で有効化
