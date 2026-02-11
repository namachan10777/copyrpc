# copyrpc-fs: RDMA分散ファイルシステム設計書

## 概要

CHFS/FINCHFS の設計をベースとした ad-hoc 並列分散ファイルシステム。
benchkv と同様のデーモン中継アーキテクチャを採用し、クライアントはライブラリとして提供する。

### 設計原則

1. **メタデータサーバなし**: CHFS と同様、consistent hashing で全データ・メタデータを分散
2. **デーモン中継**: benchkv パターン (client → ipc shm → daemon → copyrpc → remote)
3. **ゼロコピーDMA**: client-daemon 間バッファは /dev/shm、daemon 側で MR 登録し RDMA DMA
4. **Pmem 対応**: データストレージに DevDax (実験クラスタ) / FileRegion (フォールバック)
5. **最小限のFS機能**: create, open, read, write, stat, unlink, mkdir, readdir, close

## アーキテクチャ

### ノード構成

```
Node 0                                    Node 1
┌─────────────────────────────┐           ┌─────────────────────────────┐
│  Client (library)           │           │  Client (library)           │
│  ├─ ipc::Client[0]  ───────┤──shm──┐   │  ├─ ipc::Client[0]  ───────┤──shm──┐
│  └─ ipc::Client[1]  ───────┤──shm──┤   │  └─ ipc::Client[1]  ───────┤──shm──┤
│                             │       │   │                             │       │
│  Daemon 0 (ipc::Server) ◄──┤───────┘   │  Daemon 0 (ipc::Server) ◄──┤───────┘
│  ├─ PmemStore              │           │  ├─ PmemStore              │
│  ├─ copyrpc::Endpoint ─────┤═══RDMA════┤──┤─ copyrpc::Endpoint      │
│  └─ MR(client shm bufs)    │           │  └─ MR(client shm bufs)    │
│                             │           │                             │
│  Daemon 1 (ipc::Server) ◄──┤───────┘   │  Daemon 1 (ipc::Server) ◄──┤───────┘
│  ├─ PmemStore              │           │  ├─ PmemStore              │
│  ├─ copyrpc::Endpoint ─────┤═══RDMA════┤──┤─ copyrpc::Endpoint      │
│  └─ MR(client shm bufs)    │           │  └─ MR(client shm bufs)    │
└─────────────────────────────┘           └─────────────────────────────┘
```

### benchkv との差異

| 項目 | benchkv | copyrpc-fs |
|------|---------|------------|
| データモデル | 固定サイズ KV | 可変長チャンク (ファイル) |
| ルーティング | key % num_daemons | hash(path) % total_daemons |
| データ転送 | inline RPC payload | RDMA READ/WRITE via MR 登録済み shm |
| ストレージ | インメモリ配列 | Pmem (DevDax / FileRegion) |
| Flux 層 | あり (デーモン間委譲) | なし (各デーモンが独立に copyrpc endpoint を持つ) |

Flux 層を排除する理由: benchkv では全デーモンが copyrpc endpoint を共有していたが、
本設計では FINCHFS と同様に各デーモンが独立した copyrpc endpoint を持つ。
ルーティングはクライアント側で完結するため、デーモン間転送は不要。

## データモデル

### CHFS 方式: パスベースのチャンク分割

ファイルデータはチャンク (デフォルト 1 MiB) に分割し、各チャンクを独立に分散配置する。

```
ファイル /foo/bar.txt (3.5 MiB)
  → Chunk 0: key = hash("/foo/bar.txt", 0) → Daemon X
  → Chunk 1: key = hash("/foo/bar.txt", 1) → Daemon Y
  → Chunk 2: key = hash("/foo/bar.txt", 2) → Daemon Z
  → Chunk 3: key = hash("/foo/bar.txt", 3) → Daemon W  (0.5 MiB)
```

### キー設計

```rust
/// チャンクキー: ルーティングとストレージの両方に使用
#[repr(C)]
struct ChunkKey {
    path_hash: u64,      // koyama_hash(filename) — ルーティング用
    chunk_index: u32,    // チャンクインデックス
    _pad: u32,
}

/// ルーティング: (path_hash + chunk_index) % total_daemons
fn route(key: &ChunkKey, total_daemons: usize) -> DaemonId {
    ((key.path_hash as usize) + (key.chunk_index as usize)) % total_daemons
}
```

FINCHFS の number-aware hashing (koyama hash) を採用:
数値部分文字列を整数値として加算するため、連番ファイル名が均等に分散する。

### メタデータ

CHFS と同様、メタデータはチャンク 0 の先頭にインライン格納する。

```rust
#[repr(C)]
struct InodeHeader {
    mode: u32,          // S_IFREG | S_IFDIR | S_IFLNK | permissions
    uid: u32,
    gid: u32,
    size: u64,          // ファイル全体のサイズ
    chunk_size: u32,    // チャンクサイズ (バイト)
    _pad: u32,
    mtime: Timespec,
    ctime: Timespec,
}
// Chunk 0 value = [InodeHeader][data[0..chunk_size]]
// Chunk N value = [data[0..chunk_size]]  (N > 0, ヘッダなし)
```

### ディレクトリ

ディレクトリは `mode = S_IFDIR` の inode エントリ (チャンク 0 のみ、データなし) として格納。
`readdir` は全デーモンに対してプレフィクスマッチでスキャンする (CHFS 方式)。

## IPC プロトコル (Client ↔ Daemon)

既存の `ipc` クレートを拡張して使用する。

### メッセージ型

```rust
#[repr(C)]
enum FsRequest {
    Create { path_hash: u64, path_len: u16, mode: u32, chunk_size: u32 },
    Stat   { path_hash: u64, path_len: u16 },
    Read   { path_hash: u64, chunk_index: u32, offset: u32, len: u32 },
    Write  { path_hash: u64, chunk_index: u32, offset: u32, len: u32 },
    Unlink { path_hash: u64, path_len: u16 },
    Mkdir  { path_hash: u64, path_len: u16, mode: u32 },
    Readdir { path_hash: u64, path_len: u16 },
}

#[repr(C)]
enum FsResponse {
    Ok,
    StatOk { header: InodeHeader },
    ReadOk { len: u32 },
    ReaddirOk { count: u32 },
    NotFound,
    Error { code: i32 },
}
```

パス文字列は ipc の `extra_buffer` 経由で渡す。
Read/Write のデータペイロードも `extra_buffer` 経由。

### extra_buffer レイアウト

```
Client extra_buffer (per-client, daemon 側で MR 登録済み):
┌──────────────────────────────────────────┐
│ path area      (0 .. 4095)               │  パス文字列格納
├──────────────────────────────────────────┤
│ data area      (4096 .. extra_buf_size)   │  Read/Write データ
└──────────────────────────────────────────┘
```

- `Write`: クライアントが data area にデータ書き込み → FsRequest 送信 → daemon が処理
- `Read`: daemon がデータを data area に書き込み (ローカル or RDMA) → FsResponse 送信 → クライアントが読む

## RDMA データ転送 (Cross-Node)

### MR 登録戦略

```
Daemon 起動時:
  1. ipc::Server::create() で /dev/shm にサーバ作成
  2. 各クライアントの extra_buffer 領域を pd.register() で MR 登録
     → (rkey, addr, len) を取得
  3. クライアント接続ごとに MR 情報をテーブルに保持
```

### Write フロー (クロスノード)

```
Client (Node 0)                Daemon 0 (Node 0)              Daemon Y (Node 1)
    │                              │                              │
    │ 1. data → extra_buffer       │                              │
    │ 2. FsRequest::Write ──ipc──► │                              │
    │                              │ 3. route() → Node 1          │
    │                              │ 4. copyrpc::call()           │
    │                              │    payload: {key, offset,    │
    │                              │     len, client_rkey,        │
    │                              │     client_addr}             │
    │                              │ ─────────RDMA Send──────────►│
    │                              │                              │ 5. RDMA READ from
    │◄─────────────────────────────│◄─────────────────────────────│    client_addr/rkey
    │                              │                              │    → pmem store
    │                              │◄────────RDMA Reply───────────│ 6. reply(Ok)
    │◄──── FsResponse::Ok ─ipc────│                              │
```

ポイント: リモートデーモンがクライアントの /dev/shm バッファに直接 RDMA READ する。
これにより daemon 側のメモリコピーが不要になる。

### Read フロー (クロスノード)

```
Client (Node 0)                Daemon 0 (Node 0)              Daemon Y (Node 1)
    │                              │                              │
    │ 1. FsRequest::Read ──ipc──►  │                              │
    │                              │ 2. route() → Node 1          │
    │                              │ 3. copyrpc::call()           │
    │                              │    payload: {key, offset,    │
    │                              │     len, client_rkey,        │
    │                              │     client_addr}             │
    │                              │ ─────────RDMA Send──────────►│
    │                              │                              │ 4. pmem → read data
    │                              │                              │ 5. RDMA WRITE to
    │◄─────────────────────────────│◄─────────────────────────────│    client_addr/rkey
    │                              │◄────────RDMA Reply───────────│ 6. reply(ReadOk{len})
    │◄──FsResponse::ReadOk ─ipc───│                              │
    │ 7. data ← extra_buffer       │                              │
```

### ローカルアクセス (同一ノード)

ルーティング先が同一ノード内のデーモンの場合:
- Write: daemon が extra_buffer から直接 memcpy → pmem store → persist()
- Read: daemon が pmem から直接 memcpy → extra_buffer

RDMA 不要 (shm 経由で直接アクセス)。

## Pmem ストレージ

### PmemStore

```rust
struct PmemStore {
    region: Box<dyn PmemRegion>,  // DevDaxRegion or FileRegion
    // チャンクアロケーション用メタデータ
    chunk_table: HashMap<ChunkKey, ChunkLocation>,
    free_list: Vec<ChunkLocation>,
    chunk_size: usize,
}

struct ChunkLocation {
    offset: usize,     // region 内オフセット
    len: usize,        // 有効データ長
}

impl PmemStore {
    fn write_chunk(&mut self, key: &ChunkKey, offset: usize, data: &[u8]) {
        let loc = self.chunk_table.entry(*key)
            .or_insert_with(|| self.alloc_chunk());
        let dst = unsafe { self.region.as_ptr().add(loc.offset + offset) };
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len()) };
        unsafe { pmem::persist(dst, data.len()) };
        loc.len = loc.len.max(offset + data.len());
    }

    fn read_chunk(&self, key: &ChunkKey, offset: usize, buf: &mut [u8]) -> usize {
        let loc = match self.chunk_table.get(key) {
            Some(loc) => loc,
            None => return 0,
        };
        let readable = loc.len.saturating_sub(offset).min(buf.len());
        let src = unsafe { self.region.as_ptr().add(loc.offset + offset) };
        unsafe { std::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), readable) };
        readable
    }
}
```

DevDax がない環境では `FileRegion` にフォールバック:
```rust
let region: Box<dyn PmemRegion> = if Path::new("/dev/dax0.0").exists() {
    Box::new(unsafe { DevDaxRegion::open(Path::new("/dev/dax0.0"))? })
} else {
    Box::new(unsafe { FileRegion::create(Path::new("/tmp/fs_data"), capacity)? })
};
```

### RDMA 直接アクセス (Pmem → クライアント)

Read フローで、リモートデーモンが pmem のデータをクライアントの shm バッファに書き込む際:
1. pmem 領域も MR 登録済み (daemon 起動時)
2. リモートデーモンは pmem MR から SGE を構成し、RDMA WRITE でクライアントバッファへ直接転送
3. 中間バッファ不要 (pmem → NIC → クライアント shm)

```rust
// リモートデーモン側 (Read 処理)
let pmem_offset = chunk_location.offset + req.offset;
emit_wqe!(ctx, write {
    remote_addr: req.client_addr,   // クライアントの shm バッファ
    rkey: req.client_rkey,
    sge: {
        addr: pmem_mr.addr() + pmem_offset,
        len: req.len,
        lkey: pmem_mr.lkey(),
    },
});
```

## クライアントライブラリ API

```rust
pub struct FsClient {
    clients: Vec<ipc::Client<FsRequest, FsResponse, ...>>,
    num_daemons_per_node: usize,
    num_nodes: usize,
    chunk_size: usize,
}

impl FsClient {
    /// ローカルデーモン群に接続
    pub unsafe fn connect(
        shm_paths: &[String],       // daemon ごとの /dev/shm パス
        num_nodes: usize,
        chunk_size: usize,
    ) -> Result<Self, ConnectError>;

    /// ファイル作成
    pub fn create(&mut self, path: &str, mode: u32) -> Result<(), FsError>;

    /// ファイルオープン (クライアント側は fd テーブル管理のみ)
    pub fn open(&mut self, path: &str, flags: u32) -> Result<Fd, FsError>;

    /// pwrite (任意オフセット書き込み)
    pub fn pwrite(&mut self, fd: Fd, buf: &[u8], offset: u64) -> Result<usize, FsError>;

    /// pread (任意オフセット読み込み)
    pub fn pread(&mut self, fd: Fd, buf: &mut [u8], offset: u64) -> Result<usize, FsError>;

    /// stat
    pub fn stat(&mut self, path: &str) -> Result<InodeHeader, FsError>;

    /// unlink
    pub fn unlink(&mut self, path: &str) -> Result<(), FsError>;

    /// mkdir
    pub fn mkdir(&mut self, path: &str, mode: u32) -> Result<(), FsError>;

    /// readdir
    pub fn readdir(&mut self, path: &str) -> Result<Vec<DirEntry>, FsError>;

    /// close
    pub fn close(&mut self, fd: Fd) -> Result<(), FsError>;
}
```

### マルチチャンク Write の流れ

```rust
fn pwrite(&mut self, fd: Fd, buf: &[u8], offset: u64) -> Result<usize, FsError> {
    let chunk_size = self.chunk_size as u64;
    let mut written = 0;
    while written < buf.len() {
        let global_off = offset + written as u64;
        let chunk_idx = (global_off / chunk_size) as u32;
        let local_off = (global_off % chunk_size) as u32;
        let len = ((chunk_size - local_off as u64) as usize).min(buf.len() - written);

        let target = route(path_hash, chunk_idx, self.total_daemons());
        let client = &mut self.clients[target % self.num_daemons_per_node];

        // extra_buffer の data area にコピー
        let eb = client.extra_buffer();
        unsafe { ptr::copy_nonoverlapping(buf[written..].as_ptr(), eb.add(4096), len) };

        client.call(FsRequest::Write {
            path_hash, chunk_index: chunk_idx,
            offset: local_off, len: len as u32,
        }, ())?;

        written += len;
    }
    // 全応答を回収
    while self.pending() > 0 { self.poll_all()?; }
    Ok(written)
}
```

## クレート構成

```
copyrpc-fs/
├── Cargo.toml
├── src/
│   ├── lib.rs          # FsClient API
│   ├── message.rs      # FsRequest, FsResponse, InodeHeader
│   ├── routing.rs      # koyama_hash, route()
│   └── store.rs        # PmemStore
├── src/bin/
│   └── fsd.rs          # デーモンバイナリ (MPI 起動)
└── tests/
    └── integration.rs  # 2ノード統合テスト
```

### 依存関係

```
copyrpc-fs
├── ipc           # client-daemon 共有メモリ通信
├── copyrpc       # daemon 間 RDMA RPC
├── mlx5          # MR 登録 (pd.register)
├── pmem          # PmemRegion (DevDax / FileRegion)
├── fastmap       # 高速 HashMap
├── mpi           # デーモン起動・ノード発見 (fsd.rs のみ)
├── clap          # CLI (fsd.rs のみ)
└── nix           # mmap 等
```

## デーモンイベントループ

```rust
fn daemon_main(
    ipc_server: ipc::Server<FsRequest, FsResponse>,
    copyrpc_ctx: copyrpc::Context<PendingOp>,
    store: PmemStore,
    client_mrs: Vec<(u32, u64)>,  // (rkey, addr) per client
) {
    loop {
        // Phase 1: copyrpc completions (リモートからの応答)
        copyrpc_ctx.poll(&mut |pending_op, response_data| {
            match pending_op {
                PendingOp::ClientWrite { token } => {
                    ipc_server.reply(token, FsResponse::Ok);
                }
                PendingOp::ClientRead { token, len } => {
                    ipc_server.reply(token, FsResponse::ReadOk { len });
                }
            }
        });

        // Phase 2: copyrpc recv (リモートからのリクエスト)
        while let Some(req) = copyrpc_ctx.recv() {
            match decode_remote_request(req.data()) {
                RemoteWrite { key, offset, len, client_rkey, client_addr } => {
                    // RDMA READ: クライアントの shm → ローカルバッファ → pmem
                    // (または pmem MR に直接 RDMA READ)
                    store.write_from_rdma(&key, offset, len, client_rkey, client_addr);
                    req.reply(&encode_response(FsResponse::Ok));
                }
                RemoteRead { key, offset, len, client_rkey, client_addr } => {
                    // pmem → RDMA WRITE → クライアントの shm
                    let actual = store.read_to_rdma(&key, offset, len, client_rkey, client_addr);
                    req.reply(&encode_response(FsResponse::ReadOk { len: actual }));
                }
                // ... stat, create, unlink 等
            }
        }

        // Phase 3: ipc recv (ローカルクライアントからのリクエスト)
        ipc_server.poll();
        while let Some(req) = ipc_server.recv() {
            let target = route(&req.data());
            if is_local(target) {
                // ローカル処理: shm extra_buffer ↔ pmem (memcpy)
                handle_local(&store, &ipc_server, req);
            } else {
                // リモート転送: copyrpc 経由
                let (rkey, addr) = client_mrs[req.client_id()];
                let token = req.into_token();
                copyrpc_endpoint.call(
                    &encode_remote_request(req.data(), rkey, addr),
                    PendingOp::ClientWrite { token },
                    resp_allowance,
                );
            }
        }
    }
}
```

## readdir の実装

CHFS と同様、readdir は全デーモンに対してブロードキャストする必要がある。

```
Client: readdir("/foo")
  → 全 num_daemons_per_node 個のローカルデーモンに FsRequest::Readdir 送信
  → 各デーモンが:
    1. ローカル PmemStore をプレフィクスマッチでスキャン
    2. リモートデーモンにも readdir RPC を送信 (自分が担当するリモートデーモンのみ)
    3. 結果をマージして返答
  → クライアントが全デーモンの結果をマージ・重複排除
```

これは遅い操作だが、HPC ワークロードでは readdir 頻度は低い。

## 起動シーケンス

```bash
# 各ノードでデーモン起動 (MPI)
mpirun --hostfile hosts.txt -np 4 \        # 2ノード × 2デーモン/ノード
    copyrpc-fsd \
    --daemons-per-node 2 \
    --chunk-size 1048576 \
    --pmem-path /dev/dax0.0 \              # or --data-path /tmp/fs_data
    --shm-prefix /copyrpc-fs \
    --max-clients 32 \
    --queue-depth 8 \
    --extra-buffer-size 2097152            # 2 MiB (>= chunk_size)
```

起動フロー:
1. MPI_Init → rank, size 取得
2. PmemStore 初期化 (DevDax or FileRegion)
3. ipc::Server 作成 (per-daemon, /dev/shm)
4. copyrpc::Context 構築、endpoint 作成
5. MPI_Allgather で endpoint 情報交換
6. endpoint 接続
7. MPI_Barrier → イベントループ開始

## 制約・今後の課題

- **一貫性**: 同一ファイルへの並行書き込みは未定義動作 (CHFS と同様)
- **永続性**: pmem flush/drain でチャンク単位の永続性は保証。メタデータ (chunk_table) はインメモリのため、クラッシュ時に消失 (ad-hoc FS として許容)
- **ファイルサイズ上限**: chunk_table がインメモリなので、エントリ数に依存
- **readdir のスケーラビリティ**: 全ノードブロードキャスト。大規模時はボトルネックになりうる
- **symlink/hardlink**: 初期実装では省略
- **FUSE マウント**: 将来的に追加可能だが、初期はライブラリ API のみ
