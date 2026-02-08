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
let req = handle.with_data(|bytes| parse(bytes));  // ゼロコピー読み取り
handle.reply(response_bytes)?;                      // 所有権消費

// ipc (共有メモリ)
let (token, req) = server.recv()?;                  // Copy 型の直接読み取り
server.reply(token, response);                      // 所有権消費
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
| サーバ受信 | `RecvHandle` (lifetime) | `RequestToken` (opaque) | `IncomingRequest` |
| データアクセス | `with_data()` コールバック | `recv()` 戻り値 | `request.data()` |
| クライアント user_data | `Slab<U>` | `[Option<U>; 32]` | `Slab<U>` |
| レスポンスコールバック | `Fn(U, &[u8])` | `FnMut(U, Resp)` | `Fn(U, &[u8])` |
| overwrite 防止 | slab + CQ | bitmap + toggle bit | slab + slot window |
