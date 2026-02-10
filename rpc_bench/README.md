# rpc_bench ベンチマーク

RDMA (InfiniBand) ベースの RPC 実装比較ベンチマーク。
PBS ジョブスケジューラで実行する。

## シナリオ

### one-to-one (1 client + 1 server)

2 ノード (rank 0 = client, rank 1 = server) で 1-to-1 RPC スループットを測定。

**Systems:** copyrpc, erpc, rc-send, ucx-am
**Threads (T):** 1, 2, 4, 8
**Queue Depth (QD):** 1, 8, 256
**Endpoints (E):** 1, 2, 3, 4, 6, 8, 12, 16, ..., 4096 (E >= T かつ E % T == 0)

- ucx-am はシングルスレッドのみ (T=1)

### multi-client (1 server + N clients)

rank 0 = server、rank 1..N = clients で多対一スループットを測定。

**Systems:** copyrpc, erpc, ucx-am
**Clients (NC):**
- small: 1, 2, 3, 4, 6
- large: 8, 12, 16, 24, 32, 48, 64, 96, 128
**Queue Depth:** 32 (固定)

## 共通パラメータ

| パラメータ | 値 |
|-----------|-----|
| message_size | 32 bytes |
| duration | 10s |
| runs | 3 |
| affinity-mode | multinode |
| affinity-start | 47 |
| timeout | 120s |

## 実行方法 (Pegasus)

```bash
# one-to-one (2 ノード)
qsub rpc_bench/scripts/one_to_one.sh

# multi-client small (7 ノード)
qsub rpc_bench/scripts/multi_client_small.sh

# multi-client large (9 ノード)
qsub rpc_bench/scripts/multi_client_large.sh
```

## 可視化

```bash
uv run python rpc_bench/scripts/plot.py
```

## 出力ファイル

```
rpc_bench/
├── result/                         # Parquet 生データ (gitignore)
│   ├── one_to_one/                 # {system}_qd{QD}_t{T}_e{E}.parquet
│   └── multi_client/               # {system}_mc_nc{NC}.parquet
└── dist/                           # グラフ (git tracked)
    └── *.png
```

## Parquet スキーマ

| カラム | 型 | 説明 |
|--------|------|------|
| system | string | copyrpc, erpc, ucx-am, rc-send |
| mode | string | 1to1, multi_client |
| epoch_index | uint32 | エポック番号 |
| rps | float64 | Requests per second |
| message_size | uint64 | ペイロードサイズ (bytes) |
| endpoints | uint32 | QP 数 |
| inflight_per_ep | uint32 | EP あたり in-flight リクエスト数 |
| clients | uint32 | クライアント数 |
| threads | uint32 | ランクあたりスレッド数 |
| run_index | uint32 | 実行番号 |
| timestamp | int64 | Unix timestamp |
