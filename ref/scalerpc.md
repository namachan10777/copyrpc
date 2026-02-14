# ScaleRPC: 論文忠実な詳細設計擬似コード

対象論文: *Scalable RDMA RPC on Reliable Connection with Efficient Resource Sharing* (EuroSys 2019)  
対象範囲: Section 3 を中心に、Section 4.2 (Global Synchronization) を実装仕様へ落とし込み

## 0. 忠実性ポリシー
- `論文明記`: 論文本文に直接記載された仕様
- `実装補完`: 論文が未規定な箇所を、論文制約を満たす最小限で補完

## 1. コア設計要点
- `論文明記` RC + one-sided `RDMA write` を基本 RPC primitive とする
- `論文明記` Connection Grouping: 同時にサービスする client 数を時分割で制限
- `論文明記` Virtualized Mapping: 1つの physical message pool を複数 group で共有
- `論文明記` Warmup: 次 group の要求を事前取り込みして context switch 待ちを隠蔽
- `論文明記` Priority-based scheduling: `Pi = Ti / Si` による group 動的再編

## 2. 定数・推奨既定値
- `DEFAULT_TIME_SLICE_US = 100` (評価既定)
- `DEFAULT_GROUP_SIZE = 40` (評価既定)
- `GROUP_SIZE_LEGAL_RANGE = [0.5, 1.5] * DEFAULT_GROUP_SIZE`
- `HUGEPAGE_SIZE = 2MB` (message pool 割り当て単位)

## 3. メッセージレイアウト
```text
// 論文明記: RDMA write は増加アドレス順で更新される前提
// 論文明記: right-aligned 3 fields
struct MessageBlock {
  Data[0..N-1]      // 可変長 payload
  MsgLen            // Data長
  Valid             // 新規メッセージ到着検出フラグ
}

// 受信判定:
// Valid が有効値に遷移した時点で Data/MsgLen の write 完了を保証する設計
```

## 4. データ構造
```text
enum ClientState { IDLE, WARMUP, PROCESS }

struct EndpointEntry {
  req_addr: u64
  batch_size: u32
  // 実装補完: generation/seq を入れると重複検出しやすい
}

struct ClientPerf {
  Ti: f64       // current slice throughput
  Si: f64       // average request size
  Pi: f64       // Ti / Si
}

struct GroupContext {
  group_id: u32
  client_ids: list<ClientId>
  offsets: map<ClientId, OffsetInPool>
  perf_counters: map<ClientId, ClientPerf>
  time_slice_us: u32
}

struct RpcServer {
  processing_pool: MessagePool
  warmup_pool: MessagePool
  endpoint_entries: array<EndpointEntry>    // E1..En

  groups: list<GroupContext>
  current_group_idx: usize
  scheduler: PriorityScheduler
  workers: list<WorkerThread>
}

struct RpcClient {
  id: ClientId
  state: ClientState
  assigned_group: GroupId
}
```

## 5. 不変条件
- `I1` 同一 time slice 中に request 送信可能なのは active group の client のみ
- `I2` physical message pool は常に `1 group 分` の容量しか持たない
- `I3` context switch 前に現 group の未処理 request を処理/通知してから切替
- `I4` `WARMUP -> PROCESS` は first response 受信でのみ遷移
- `I5` `PROCESS -> IDLE` は `context_switch_event` 受信でのみ遷移

## 6. サーバ初期化
```text
function server_init(config):
  s = RpcServer()
  s.processing_pool = alloc_and_register_hugepage_pool(HUGEPAGE_SIZE, for_one_group=true)
  s.warmup_pool = alloc_and_register_hugepage_pool(HUGEPAGE_SIZE, for_one_group=true)
  s.endpoint_entries = create_endpoint_entries(max_clients)

  s.groups = initial_grouping(clients, DEFAULT_GROUP_SIZE)
  for g in s.groups:
    g.time_slice_us = DEFAULT_TIME_SLICE_US

  start_worker_threads(s.workers, s.processing_pool)
  start_scheduler_thread(s.scheduler)
  return s
```

## 7. Priority-based grouping

### 7.1 優先度更新
```text
function update_client_priority(perf: ClientPerf):
  perf.Pi = perf.Ti / perf.Si
```

### 7.2 group 編成
```text
function regroup_by_priority(server):
  // 論文明記: 同優先度クラスを同 group に集約
  classes = bucketize_clients_by_priority(server.clients)

  new_groups = []
  for cls in classes_high_to_low:
    g = make_group_from_class(cls)
    // 論文明記: 高優先度ほど「少人数 + 長い time slice」
    g.target_size = smaller_size_for_higher_priority(cls)
    g.time_slice_us = longer_slice_for_higher_priority(cls)
    new_groups.append(g)

  server.groups = new_groups
```

### 7.3 lazy split / merge
```text
function lazy_adjust_group_sizes(server):
  lo = 0.5 * DEFAULT_GROUP_SIZE
  hi = 1.5 * DEFAULT_GROUP_SIZE

  for g in server.groups:
    if size(g.client_ids) < lo:
      merge_with_neighbor_group(g)
    else if size(g.client_ids) > hi:
      split_group(g)
```

## 8. context switch と virtualized mapping
```text
function context_switch(server):
  cur = server.groups[server.current_group_idx]

  notify_workers_to_drain_and_clear_suspended_requests(cur)

  // 論文明記: context_switch_event を response に piggyback
  for cid in cur.client_ids:
    if client_has_active_request(cid):
      piggyback_context_switch_event_in_next_response(cid)
    else:
      send_extra_rdma_write_context_switch_event(cid)  // 追加 write 比率は極小

  save_context_metadata(cur)           // ClientIDs, offsets, counters

  next_idx = (server.current_group_idx + 1) mod len(server.groups)
  next = server.groups[next_idx]
  load_context_metadata(next)

  // 論文明記: warmup pool <-> processing pool を入れ替える
  swap(server.processing_pool, server.warmup_pool)
  server.current_group_idx = next_idx
```

## 9. warmup パイプライン
```text
function client_post_warmup_tuple(client, local_req_addr, batch_size):
  // <req_addr, batch_size> を自身の EndpointEntry に RDMA write
  entry = server.endpoint_entries[client.id]
  rdma_write(entry, {local_req_addr, batch_size})

function scheduler_prepare_warmup_group(server):
  candidates = choose_clients_for_next_group(server.scheduler_policy)
  for cid in candidates:
    tuple = read_endpoint_entry(cid)
    // 論文明記: server が client memory から RDMA read で先読み
    rdma_read_from_client_into_pool(tuple.req_addr, tuple.batch_size, server.warmup_pool)
```

## 10. サーバ request 処理ループ
```text
thread worker_loop(server, thread_id):
  zone = processing_zone_owned_by(thread_id)
  while true:
    for block in zone.blocks:
      if block.Valid is not set:
        continue

      req_len = block.MsgLen
      req = block.Data[0:req_len]
      resp = invoke_rpc_handler(req)
      rdma_write_response_to_client(resp)

      clear_or_advance_block_state(block)   // 実装補完: 再利用管理
```

## 11. クライアント状態機械 (Figure 7 準拠)
```text
function on_client_connect(client):
  client.state = WARMUP

function client_submit_request(client, req_batch):
  if client.state == WARMUP:
    init_requests_locally(req_batch)                           // step 1
    client_post_warmup_tuple(client, addr(req_batch), size(req_batch))  // step 2
    return

  if client.state == PROCESS:
    rdma_write_requests_directly_to_processing_pool(req_batch)
    return

  if client.state == IDLE:
    // post_new_request? で WARMUP に戻る
    client.state = WARMUP
    client_submit_request(client, req_batch)

function on_client_receive_response(client, resp):
  if first_response_after_warmup(resp):
    client.state = PROCESS

  if has_context_switch_event(resp):
    client.state = IDLE
```

## 12. まとめ実行フロー (Section 3.4)
```text
loop every scheduling tick:
  scheduler_prepare_warmup_group(server)
  run_workers_on_processing_pool()
  if time_slice_elapsed(current_group):
    context_switch(server)
    update_priorities_and_regroup_if_needed(server)
    lazy_adjust_group_sizes(server)
```

## 13. API 層 (Section 3.5)
```text
SyncCall(req)           // 同期RPC
AsyncCall(req_batch)    // 非同期で複数要求を投げる
PollCompletion()        // 完了回収
```

非同期2 API (`AsyncCall`, `PollCompletion`) により、one-to-many で batch 投機送信する。

## 14. many-to-many 向け Global Synchronization (Section 4.2)

ScaleRPC 単体は各 RPCServer が独立に group switch すると、1 client が複数 server を跨ぐ時に stall しうる。  
そのため ScaleTX は NTP-like 同期を導入する。

```text
// Follower i
send_sync_to_time_server at Ti1
time_server_receives at Ti2
time_server_sends_resp at T3 with DeltaTi = (T3 - Ti2)
follower_receives_resp at Ti4

// 次回 context switch まで sleep する時間
Di = D - (Ti4 - Ti1 - DeltaTi) / 2
sleep(Di)  // follower
// time server は sleep(D)
```

`論文明記` 実装では global synchronization を `100ms` ごとに実行し、オーバーヘッドは小さい。

## 15. 既知の制約 (論文記載)
- grouping は throughput/平均遅延改善と引き換えに最大遅延を悪化させる場合がある
- 長時間 RPC は context switch にまたがる初回実行で失敗しうる  
  対応: 当該 call type 情報を記録し、以後は separate thread の legacy mode 実行
- クライアント独立実行 + サーバ/クライアント協調動作を前提

## 16. 実装補完が必要な未規定点
- `Valid` の有効値遷移方式 (toggle/epoch/seq) は本文未規定
- message block の再利用時クリア手順の厳密規約は本文未規定
- priority class の離散化方法・閾値は本文未規定
- context metadata の永続化形式は本文未規定

## 17. 実装時チェックリスト
- `Pi = Ti/Si` を毎スライス更新する
- group size が `[0.5, 1.5] * default` を外れたら lazy split/merge
- switch 時に `drain -> notify(context_switch_event) -> save/load metadata -> pool swap`
- `WARMUP` では endpoint entry へ `<req_addr, batch_size>` を書き、server RDMA read を待つ
- `PROCESS` では processing pool へ直接 RDMA write
- many-to-many では Global Synchronization を必須化
