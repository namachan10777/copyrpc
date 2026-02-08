# Claude Code 開発ガイドライン

## プロジェクト概要

RDMA (InfiniBand) を活用した高性能RPCフレームワーク。

### クレート構成

| レイヤー | クレート | 概要 |
|---------|---------|------|
| FFIバインディング | `mlx5_sys`, `ibverbs_sys` | MLX5ドライバ・libibverbsへのバインディング |
| コアライブラリ | `mlx5` | RDMA操作の抽象化層 |
| ユーティリティ | `fastmap` | 高速マップ実装 |
| RPC実装 | `copyrpc` | メインRPC実装 (Flux/Meshアーキテクチャ) |
| RPC実装 | `erpc`, `scalerpc` | 比較用RPC実装 |
| スレッド間通信 | `inproc` | Flux/Meshのスレッド間版 (rtrb/omango/crossbeam対応) |
| プロセス間通信 | `ipc` | 共有メモリベースRPC |
| ベンチマーク | `rpc_bench` | 複数RPC実装の統一ベンチマーク |

### 主要な依存グラフ

```
mlx5_sys, fastmap → mlx5 → copyrpc, erpc, scalerpc → rpc_bench
```

## 実装方針

- 実装難易度が高くないタスクは **Claude Sonnet** (model: "sonnet") を使うこと。Opusは設計判断・デバッグ等の難易度の高いタスクに限定する。
- 独立したタスクは **subagentで並列実行** すること。例: 複数クレートのテスト修正、独立したファイルの変更など。
- 無駄なモックを作らない。この開発マシンにはIBデバイスが存在するので実ハードウェアでテストする。
- 後方互換性のために変更した仕様を温存しない。

## テスト・品質管理

コード変更時は必ず以下を実行してregressionがないか確認する。

```bash
# ユニットテスト
cargo test --package mlx5 --lib

# 統合テスト（要RDMAハードウェア）
cargo test --package mlx5 --test '*'

# clippy
cargo clippy
```

- 全てのテストが常にコンパイル可能な状態を維持すること。
- `#[allow]` は最後の手段。使う場合はユーザに確認を取る。
- ベンチマークが30秒以上かかる場合はハングの可能性が高い。

マルチノードでテストをしたい場合、fern03,fern04であれば/dev/omni.txtを使いMPI実行が出来る。
1 node = 1 rank前提でslotは一つずつ。SSH接続は`mnakano`ユーザで行うこと。
`/work/`はノードローカルなので、MPIで実験する際は`/home/mnakano`のNFSを使うか、rsyncでコピーを行う。

```bash
mpirun --hostfile ./dev/omni.txt -np 2 hostname
```

## 環境情報

- マシン: fern04, Intel Xeon Gold 6530, 32コア
- IB NICはNUMA 0に接続。コア0,1はIRQ処理用なのでbusy-pollingベンチマークでは避ける。
- MPIベンチマーク実行時: `mpirun --bind-to none`
