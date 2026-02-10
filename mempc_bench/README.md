# mempc ベンチマーク

共有メモリベース MPSC 実装の QD (Queue Depth) sweep ベンチマーク基盤。

## シナリオ

### manyclient (1 server + N clients)

`server_client` バイナリを使用。1 つのサーバスレッドが N-1 クライアントからのリクエストを処理する。

**Rust transports:** onesided, fast-forward, fetch-add, scq, wcq, wcq-cas2, lcrq, lprq, bbq, jiffy
**TBB transports:** tbb_mpsc, tbb_spsc
**スレッド数 (n):** 2, 3, 5, 9, 17

### alltoall (N-thread 全対全)

`all_to_all` バイナリを使用。全スレッドが対等に通信する Flux パターン。

**Rust transports:** onesided, fast-forward, lamport, fetch-add, scq, wcq-cas2, wcq, bbq, jiffy
**TBB transports:** tbb_alltoall
**スレッド数 (n):** 2, 4, 8, 16

## 共通パラメータ

| パラメータ | 値 |
|-----------|-----|
| QD (inflight) | 1, 2, 4, 8, 16, 32, 64, 128, 256 |
| capacity | 1024 |
| duration | 5s |
| runs | 3 (bench), 1 (perf) |
| start-core | 31 |
| timeout | 60s (bench), 15s (perf) |

## 実行方法

```bash
# ビルド
cargo build --release --package mempc_bench
cd tbb_bench/build && make && cd ../..

# ベンチマーク (throughput のみ)
bash mempc_bench/scripts/run_manyclient_bench.sh
bash mempc_bench/scripts/run_alltoall_bench.sh

# ベンチマーク (throughput + perf stat)
bash mempc_bench/scripts/run_manyclient_perf.sh
bash mempc_bench/scripts/run_alltoall_perf.sh

# 可視化
uv run python mempc_bench/scripts/plot.py
```

## 出力ファイル

```
mempc_bench/
├── result/                         # 生データ (gitignore)
│   ├── manyclient/                 # {transport}_n{n}_qd{qd}.json
│   └── alltoall/                   # {transport}_n{n}_qd{qd}.json
└── dist/                           # 成果物 (git tracked)
    ├── manyclient_summary.json
    ├── alltoall_summary.json
    └── *.png                       # グラフ
```

## JSON フォーマット

```json
{
  "scenario": "manyclient",
  "transport": "onesided",
  "threads": 2,
  "clients": 1,
  "qd": 8,
  "capacity": 1024,
  "duration_secs": 5,
  "runs": 3,
  "median_mops": 27.51,
  "perf_duration_secs": 5,
  "perf_run_mops": 27.03,
  "perf_events": [ ... ]
}
```

`perf_*` フィールドは perf スクリプト実行時のみ含まれる。
