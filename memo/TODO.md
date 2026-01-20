- ~~可変長でのWQEが本当に正しく構築可能か~~
  - ~~inlineみたいなやつだね~~
  - ~~wrap aroundもある~~
  - ~~Builderパターンだと書き始めた時にどれくらいWQEBB使うかわからないのでちょっと複雑~~
  - **実装済み**: NOPで埋めてリング先頭にWQEを再配置する方式 (`finish_with_wrap_around`)
  - **テスト追加**: `test_rc_inline_wraparound`, `test_rc_inline_variable_size_wraparound`
- ~~WQE builderでフィールドが足りない時にエラーが出るかどうか~~
  - ~~sge/inlineのデータ以外は関数一発で作ってもいいかも。builderで柔軟に構築したいのはsge/inlineが本命だし~~
  - **実装済み**: 型状態パターンでコンパイル時にエラー (`ctrl_send`, `ctrl_rdma_write`, etc.)
- DC Stream実装
  - 他のSend系とか異なり、Unorderedなので実装が異なる
- WQE tableとSQ/RQは実は若干直行している。APIもそのようにするべき
  - Ordered+partial Signal: Sparse/Dense
    - DCI, QP(SQ), Tm-SRQ(CmdQueue)
  - Ordered+All signal: Ordered
    - QP(RQ), SRQ
  - Unordered+All signal: Unordered
    - DCI(DC Stream), Tm-SRQ(RQ)
- DC Streamを実装した時に、型レベルでUnordered+Stream ID設定可能と、Sparse/DenseでStream id設定不可能の二択なっているか
- atomic命令のインターフェース改善
- mlx5拡張atomic命令サポート
- ibverbs_sys削除（mlx5_sysだけでいいはず）
- RELAXED_ORDERING対応
  - 普通のSQ、DCIでもUnorderedは使用可能であるべき
  - また、実はSparse/DenseでもRELAXED_ORDERINGを使っても正しく動かせる方法がある
  - Signaled+Fencedの場合のみ更新して、他のTX CQEは無視すればいい。
  - これも型レベルでの保証かな
  - 構築時のジェネリクスで指定する。UseRelaxedOrdering型を指定すると、ジェネリクスによってRelaxed Orderingが存在するバージョンのFlagを使える。ただし、これを指定するとSparse/DenseではFenceが付かないとWQEBBの解放処理やcallbackを呼ぶ処理が行われないことに注意させる（これはドキュメントに明記かな）
- Inline CQE parse
- CQE compressionパース（RX CQ用に実装済み: memo/cqe_compression_debug.md参照）
  - `create_mono_cq_rx_compressed`でRX CQ専用の圧縮モードをサポート
  - CQE圧縮を有効にしても通常CQEが混在するため、format bitsによる動的判定を実装
  - format=3（圧縮CQE）: signatureフィールドでowner判定
  - format=0（通常CQE）: op_own bit 0でowner判定
  - HW caps確認: ConnectX-7でmax_num=64, HASH/CSUMフォーマット対応
  - 残課題: **Strided RQが必要な可能性** - 通常RQでは圧縮CQEが生成されない
- 可能な限り`mlx5_`系の関数を使う。

# 優先順位

1. ~~Inline WQEの折り返しを正しく実装~~ ✅ 実装・テスト済み
2. ~~WQE builderでのエラー~~ ✅ 型状態パターンで実装済み
3. ~~CQE compression~~ （RX CQ用は実装完了、圧縮CQE生成条件テスト残）
4. Inline CQE parse
5. API直行性や型制約の確認
6. 拡張atomic
7. DC Stream
8. RELAXED Ordering
