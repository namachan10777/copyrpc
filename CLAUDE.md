# Claude Code 開発ガイドライン

この開発マシンはIBデバイスが存在するのでIBが必要なテストも実行すること。
無駄なモックを作ってはならない。
後方互換性のために変更した仕様を温存してはいけない

## テスト

コード変更時はテストを全て実行してregressionがないか確認する。
常に全てのテストはコンパイル可能にすること。

```bash
# ユニットテスト
cargo test --package mlx5 --lib

# 統合テスト（要RDMAハードウェア）
cargo test --package mlx5 --test '*'

# ベンチマーク（要RDMAハードウェア）
cargo bench --bench pingpong
```
