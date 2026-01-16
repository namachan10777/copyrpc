# Claude Code 開発ガイドライン

## テスト

コード変更時はテストを実行してregressionがないか確認する。

```bash
# ユニットテスト
cargo test --package mlx5 --lib

# 統合テスト（要RDMAハードウェア）
cargo test --package mlx5 --test '*'

# ベンチマーク（要RDMAハードウェア）
# tasksetで物理コアのみを使用（HT無効化で安定した結果を得る）
taskset -c 0,2,4,6,8 cargo bench --bench pingpong
```
