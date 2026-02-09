# copyrpc

RDMA (InfiniBand) を活用した高性能 RPC フレームワーク群。

## クレート構成

| レイヤー | クレート | 概要 |
|---------|---------|------|
| FFI バインディング | `mlx5_sys`, `ibverbs_sys` | MLX5 ドライバ・libibverbs へのバインディング |
| コアライブラリ | `mlx5` | RDMA 操作の抽象化層 |
| ユーティリティ | `fastmap` | 高速マップ実装 |
| ノード間 RPC | `copyrpc` | RDMA RC + SRQ ベースの RPC |
| プロセス間通信 | `ipc` | 共有メモリベースの RPC |
| スレッド間通信 | `inproc` | Flux/Mesh によるスレッド間通信 |
| 比較実装 | `erpc`, `scalerpc` | 他方式の RPC 実装 |
| ベンチマーク | `rpc_bench`, `benchkv` | 統一ベンチマーク |

## 共通 RPC インターフェース設計

`copyrpc`・`ipc`・`erpc` は同一の非同期 request-response パターンに従う。

### サーバ側: poll / recv / reply

```
poll() → recv() → [処理] → reply()
```

`recv()` はリクエストの出所を隠蔽する不透明トークン（`RecvHandle` / `RequestToken`）を返す。
トークン経由でリクエストデータを直接読み取り、コピーを回避する。
`reply()` はトークンの所有権を消費し、レスポンス書き込みとリソース解放を安全に行う。

リクエストの上書き防止は inflight 制限で実現する。
サーバがトークンを保持している間、対応するスロット/バッファは再利用されない。

```rust
// copyrpc (RDMA)
let handle: RecvHandle = ctx.recv()?;
let req = handle.data();                             // ゼロコピー読み取り
handle.reply(response_bytes)?;                       // 所有権消費

// ipc (共有メモリ)
let handle = server.recv()?;                         // RecvHandle 取得
let req = handle.data();                             // ゼロコピー読み取り
handle.reply(response);                              // 所有権消費（即座 reply）
// または遅延 reply:
let token = handle.into_token();                     // 非Copy トークン抽出
server.reply(token, response);                       // 後で reply
```

### クライアント側: call / poll + callback

```
call(req, user_data) → poll() → on_response(user_data, resp)
```

`call()` はリクエスト送信と同時に `user_data` を内部ストレージ（slab / 固定配列）に格納する。
`poll()` で完了を検出すると、インライン化されたコールバックを `user_data` とともに呼び出す。
user_data の生存期間はクレート内部で完結し、呼び出し側での追跡は不要。

```rust
// copyrpc (RDMA)
endpoint.call(request_bytes, user_data)?;           // slab に user_data 格納
ctx.poll();                                         // 完了検出 → on_response(user_data, &[u8])

// ipc (共有メモリ)
client.call(request, user_data)?;                   // 固定配列に user_data 格納
client.poll()?;                                     // 完了検出 → on_response(user_data, resp)
```

### 設計比較

| 側面 | copyrpc | ipc | erpc |
|------|---------|-----|------|
| トランスポート | RDMA RC + SRQ | POSIX 共有メモリ | RDMA UD |
| リクエスト形式 | `&[u8]` バイト列 | `Copy` 型構造体 | `&[u8]` バイト列 |
| サーバ受信 | `RecvHandle` (lifetime) | `RecvHandle` / `RequestToken` | `IncomingRequest` |
| データアクセス | `data()` 直接参照 | `data()` ゼロコピー参照 | `request.data()` |
| クライアント user_data | `Slab<U>` | `[Option<U>; 32]` | `Slab<U>` |
| レスポンスコールバック | `Fn(U, &[u8])` | `FnMut(U, Resp)` | `Fn(U, &[u8])` |
| overwrite 防止 | slab + CQ | bitmap + toggle bit | slab + slot window |

## inproc: MPSC/SPSC 研究カバレッジ（学術文献のみ）

inproc クレートに含まれる MPSC/SPSC 実装と、対応する代表的な研究・論文の対応表。
SPSC は call/response を双方向 SPSC 2 本で構成する前提の参照先も含む。
※本節は学術文献に紐づくもののみ記載（学術出典が確認できない実装は除外）。

### 実装済み（学術由来）

| カテゴリ | 実装 | 参考 |
|---|---|---|
| SPSC | `FastForwardTransport` | [FastForward for efficient pipeline parallelism: a cache-optimized concurrent lock-free queue (PPoPP 2008)](https://dblp.org/rec/conf/ppopp/GiacomoniMV08) |
| SPSC | `LamportTransport` | [Specifying Concurrent Program Modules (ACM TOPLAS 1983)](https://www.microsoft.com/en-us/research/publication/specifying-concurrent-program-modules/) |

### 未実装（研究候補; call/response に有用）

**MPSC**
- [Jiffy: A Fast, Memory Efficient, Wait-Free Multi-Producers Single-Consumer Queue (DISC 2020)](https://drops.dagstuhl.de/opus/volltexte/2020/13128)
- [A Scalable, Portable, and Memory-Efficient Lock-Free FIFO Queue (DISC 2019)](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.DISC.2019.28) ※MPMC だが単一 consumer に限定すれば MPSC として利用可能（推論）

**SPSC**
- [Single-Producer/Single-Consumer Queues on Shared Cache Multi-Core Systems (2010)](https://arxiv.org/abs/1012.1824) ※uSPSC など
- [Liberty Queues for EPIC Architectures (EPIC 2010)](https://liberty.cs.princeton.edu/Projects/AutoPar/)
- [B-Queue: Efficient and Practical Queuing for Fast Core-to-Core Communication (Int. J. Parallel Program. 2013)](https://dblp.org/rec/journals/ijpp/WangZTH13)
- [A lock-free, cache-efficient multi-core synchronization mechanism for line-rate network traffic monitoring (IPDPS 2010)](https://dblp.org/rec/conf/ipps/LeeBC10)
- [A lock-free, cache-efficient shared ring buffer for multi-core architectures (ANCS 2009)](https://dblp.org/rec/conf/ancs/LeeBC09)

### Intel TBB での実装可能性（比較）

- `oneapi::tbb::concurrent_queue` は複数スレッドが同時に push/pop できる unbounded FIFO。`try_pop` による非ブロッキング取得が基本。 [oneTBB Concurrent Queue Classes](https://uxlfoundation.github.io/oneTBB/main/tbb_userguide/Concurrent_Queue_Classes.html)
- `oneapi::tbb::concurrent_bounded_queue` は bounded FIFO で、capacity 設定と `push`/`pop` のブロッキング、`try_push` を提供。 [oneTBB Concurrent Queue Classes](https://uxlfoundation.github.io/oneTBB/main/tbb_userguide/Concurrent_Queue_Classes.html)
- 両者とも MPMC 設計なので、スレッド数を制限すれば MPSC/SPSC としても利用可能（推論）。
- call/response では「request 用 queue と response 用 queue をペアで持つ」構成が自然。MPSC サーバなら request は共有、response はクライアント別に分離する構成が可能（設計メモ）。
