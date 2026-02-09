QpInnerのsq_wqeの実装を参考に、DCI/SRQ/TM-SRQも実装を行う。
TM-SRQはcmd_wqeとrq_wqeでそれぞれエントリを投稿する。

また、全てにBlueFlame一括投入APIを作る。これは
`blueflame_sq_wqe`みたいな名前になる。これは直接Builderを返すのではなく、BlueFlameのアドレスへ書き込めるバージョンのWqeBuilderを作る構造体を返す。
```rust
let mut bf = qp.blueflame_sq_wqe();
bf.wqe().send(TxFlags::empty()).inline(&data).finish()?;
bf.wqe().send(TxFlags::empty()).inline(&data).finish()?;
bf.finish();
```
という感じ。Bf用のWqeBuilderはBf幅を超えるとエラーを返す。非BFはSQ fullでエラーを返す。
wqe indexは内部管理なので、finishの返す型は`Result<(), SubmissionError>`になる。
`SubmissionError`は今のところSqFullだけ。
