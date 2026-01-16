# Claude Code 開発ガイドライン

## テスト

コード変更時はテストを実行してregressionがないか確認する。

```bash
# ユニットテスト
cargo test --package mlx5 --lib

# 統合テスト（要RDMAハードウェア）
cargo test --package mlx5 --test '*'

# ベンチマーク（要RDMAハードウェア）
cargo bench --bench pingpong
```
