# benchkv

分散KVストアベンチマーク。copyrpc + thread_channel + shm_spsc の3層通信スタックを評価する。

## CLI

```
benchkv [OPTIONS] <SUBCOMMAND>

OPTIONS:
  -d, --duration <SECS>           ベンチマーク実行時間 [default: 30]
  --interval-ms <MS>              エポック間隔（RPS記録単位） [default: 1000]
  --trim <N>                      ウォームアップ/クールダウンで除外するエポック数 [default: 3]
  -r, --runs <N>                  実行回数 [default: 3]
  -o, --output <FILE>             出力parquetファイル [default: "benchkv.parquet"]
  --device-index <N>              RDMAデバイスインデックス [default: 0]
  --port <N>                      RDMAポート [default: 1]
  --server-threads <N>            1ノードあたりのdaemonスレッド数 [default: 1]
  --client-threads <N>            1ノードあたりのクライアントスレッド数 [default: 1]
  --queue-depth <N>               クライアントあたりのin-flight数 (= shm_spsc slots_per_client, 2のべき乗) [default: 4]
  --key-range <N>                 ノードあたりのキー空間サイズ [default: 1024]
  --read-ratio <FLOAT>            read (get) の割合 [default: 0.5]
  --distribution <TYPE>           キー分布: uniform | zipfian [default: uniform]

SUBCOMMANDS:
  meta                            meta_put/meta_get ベンチマーク
  data                            data_put/data_get ベンチマーク（将来追加）
```

## メソッド

- `meta_put(rank: u32, key: u64, value: u64)` — メタデータ書き込み
- `meta_get(rank: u32, key: u64) -> Option<u64>` — メタデータ読み取り
- `data_put(rank: u32, key: u64, mr_info: BufferInfo)` — データ書き込み（copyrpcでネゴ → サーバがRDMA READ）
- `data_get(rank: u32, key: u64, mr_info: BufferInfo) -> bool` — データ読み取り（copyrpcでネゴ → サーバがRDMA WRITE）

rank, key は直接指定。クライアントがターゲットノード(rank)とノード内key(0..key_range)を直接指定する。

### ストレージバックエンド

各ノードに `[AtomicU64; key_range]` の配列を1本持つ。全daemonスレッドが共有アクセスする。
ベンチマークなのでストレージ層がボトルネックにならないよう、ロックフリー（Relaxed順序のAtomicU64）で実装。

## アーキテクチャ

### ノード構成（1 MPI rank = 1ノード）

```
┌─────────────────────────── Node (MPI rank) ──────────────────────────┐
│                                                                       │
│  Main Thread (管理専用)                                                │
│  ├── MPI初期化・接続確立                                                │
│  ├── Barrier同期・タイミング管理                                        │
│  └── エポックごとのRPS集計 → rank 0がparquet出力                         │
│                                                                       │
│  Daemon Threads (--server-threads個)                                  │
│  ┌───────────────┐ ┌───────────────┐     ┌───────────────┐           │
│  │ Daemon #0     │ │ Daemon #1     │ ... │ Daemon #N-1   │           │
│  │ (copyrpc兼任) │ │               │     │               │           │
│  │               │ │               │     │               │           │
│  │ shm_spsc Srv  │ │ shm_spsc Srv  │     │ shm_spsc Srv  │           │
│  │ Flux node     │ │ Flux node     │     │ Flux node     │           │
│  │ copyrpc ctx   │ │               │     │               │           │
│  └───────┬───────┘ └───────┬───────┘     └───────┬───────┘           │
│          │    thread_channel (Flux)               │                   │
│          └─────────────┼──────────────────────────┘                   │
│                                                                       │
│  Client Threads (--client-threads個)                                  │
│  ┌──────────┐ ┌──────────┐     ┌──────────┐                          │
│  │Client #0 │ │Client #1 │ ... │Client #M-1│                         │
│  │shm_spsc  │ │shm_spsc  │     │shm_spsc   │                         │
│  │Client    │ │Client    │     │Client     │                         │
│  └──────────┘ └──────────┘     └──────────┘                          │
│       │            │                 │                                 │
│       └────── shm_spsc ─────────────┘                                 │
│       クライアント→daemonスレッドの割り当て: client_id % server_threads   │
└───────────────────────────────────────────────────────────────────────┘
```

### 通信レイヤー

| レイヤー | 手段 | 用途 |
|---------|------|------|
| Client ↔ Daemon | shm_spsc | RPCリクエスト・レスポンス |
| Daemon ↔ Daemon（ノード内）| thread_channel (Flux) | リモートリクエストの委譲 |
| Daemon → Remote Daemon（ノード間）| copyrpc (RDMA) | ノード間KV操作。ノードペアあたり1接続 |
| Client ↔ Daemon（dataのみ）| /dev/shm 共有メモリ | 大きなデータのゼロコピー転送 |

### copyrpc接続構成

- ノードペアあたり1本のcopyrpc Endpoint接続
- Daemon #0 が copyrpc の `Context::poll()` を担当（兼任）
- 他のdaemonスレッドがリモートノードへのリクエストを処理する場合、thread_channel (Flux) でDaemon #0に委譲
- Daemon #0がcopyrpcでリモートノードに送信し、レスポンスをthread_channelで返す

### CPUアフィニティ（自動割り当て）

1. mlx5デバイスのNUMAノードを検出
2. Daemonスレッド: そのNUMAノードのコアに優先割り当て（コア0,1は除外）
3. Clientスレッド: 同じNUMAノードのコアに割り当て（可能な限り）
4. コア0,1はIRQ処理用のため常に除外

## リクエストフロー

### meta_put（リモートノード宛）の場合

```
Client                  Daemon #2              Daemon #0              Remote Daemon #0
  │                       │ (担当thread)        │ (copyrpc兼任)        │
  │── meta_put(rank,k,v) ─→│                    │                      │
  │   (shm_spsc)          │                     │                      │
  │                        │── delegate(rank,k,v)→│                     │
  │                        │   (thread_channel)   │                     │
  │                        │                      │── copyrpc call ────→│
  │                        │                      │                     │── write array[k]=v
  │                        │                      │←── copyrpc reply ──│
  │                        │←── response ─────────│                     │
  │                        │   (thread_channel)   │                     │
  │←── Ok ────────────────│                      │                      │
  │   (shm_spsc)          │                      │                      │
```

### meta_put（ローカルノード宛）の場合

```
Client                  Daemon #2
  │                       │
  │── meta_put(rank,k,v) ─→│
  │   (shm_spsc)          │── array[k].store(v, Relaxed)
  │←── Ok ────────────────│
  │   (shm_spsc)          │
```

## shm_spsc メッセージ型

```rust
/// クライアント→daemon リクエスト
#[derive(Clone, Copy)]
#[repr(C)]
enum Request {
    MetaPut { rank: u32, key: u64, value: u64 },
    MetaGet { rank: u32, key: u64 },
    // 将来追加:
    // DataPut { rank: u32, key: u64, offset: u64, len: u64 },
    // DataGet { rank: u32, key: u64, offset: u64, len: u64 },
}

/// daemon→クライアント レスポンス
#[derive(Clone, Copy)]
#[repr(C)]
enum Response {
    MetaPutOk,
    MetaGetOk { value: u64 },
    MetaGetNotFound,
    // 将来追加:
    // DataPutOk,
    // DataGetOk,
    // DataGetNotFound,
}
```

## thread_channel (Flux) メッセージ型

```rust
/// daemonスレッド間の委譲メッセージ
#[derive(Clone, Copy)]
#[repr(C)]
struct DelegateRequest {
    client_id: u32,          // レスポンスの返送先特定用
    slot_index: u32,         // shm_spscのスロット特定用（省略可、設計次第）
    request: Request,        // 元のクライアントリクエスト
}

#[derive(Clone, Copy)]
#[repr(C)]
struct DelegateResponse {
    client_id: u32,
    response: Response,
}
```

## copyrpc メッセージ型

```rust
/// ノード間RPCメッセージ（シリアライズ形式: そのままバイト列として送信）
#[derive(Clone, Copy)]
#[repr(C)]
struct RemoteRequest {
    call_id: u32,            // レスポンス対応付け用
    source_rank: u32,        // 送信元ノード
    request: Request,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct RemoteResponse {
    call_id: u32,
    response: Response,
}
```

## ベンチマーク測定（rpc_benchと同様）

### アクセスパターン事前生成

- ベンチマーク開始前に、クライアントごとのアクセスパターン（リクエスト列）を事前生成する
- **全エポックで同一のアクセスパターンを繰り返す**（生成コストとメモリ使用量の削減）
- 1エポック分のリクエスト列 = `(rank, key, is_read)` のタプル列
  - `is_read`: --read-ratio に基づきランダム決定
  - `key`: --distribution (uniform/zipfian) に基づき生成
  - `rank`: ターゲットノード（直接指定）
- パターン生成後、Barrier同期してからベンチマーク開始

### 測定フロー

1. MPI初期化 → 全ノードでdaemon/clientスレッド起動
2. アクセスパターン事前生成（クライアントごと、1エポック分）
3. Barrier同期 → ベンチマーク開始
4. 各クライアントスレッドが事前生成パターンを繰り返し実行（shm_spscのQD分パイプライン）
5. --interval-ms ごとにエポック区切りでRPS（完了リクエスト数）を記録
6. --duration 経過後、全クライアントがBarrierで停止
7. 先頭/末尾の --trim エポックを除外
8. rank 0 が全ノードの結果を集約 → parquet出力

### Parquet出力カラム

| カラム | 型 | 説明 |
|-------|-----|------|
| run | u32 | 実行回数のインデックス |
| rank | u32 | ノード番号 |
| client_id | u32 | クライアントスレッドID |
| epoch | u32 | エポック番号 |
| requests | u64 | そのエポックの完了リクエスト数 |
| duration_ns | u64 | そのエポックの実際の経過時間 |

### data_put/data_get（将来追加）

- クライアント接続時にdaemonが/dev/shmの共有メモリ領域をMR登録
- 1 daemonあたり1つの大きな/dev/shm領域を確保し、クライアントごとにオフセットで分割
- クライアントがバッファ位置を指定し、daemonがそのクライアントのバッファ情報からrkey, addr等を計算
- data_put: copyrpcでネゴ → リモートdaemonがRDMA READでクライアントのバッファから読み取り
- data_get: copyrpcでネゴ → リモートdaemonがRDMA WRITEでクライアントのバッファに書き込み

## 実装優先度

1. **Phase 1 (meta)**: meta_put/meta_get の E2E パイプライン
   - shm_spsc通信、thread_channel委譲、copyrpcノード間通信
   - YCSB風ワークロード生成（read-ratio, distribution）
   - 測定・集計・parquet出力
2. **Phase 2 (data)**: data_put/data_get の追加
   - /dev/shm共有メモリ + MR登録
   - RDMA READ/WRITE データ転送
