# mlx5基本コンセプト

WQE構築についてはBuilderパターンで最小コストで構築可能にする。
また、CQEに対応するユーザデータ管理も内部ロジックに隠蔽する（微妙に制約とかがややこしいので）。
基本的にユーザデータはNICは管理しないため、CQEのwqe counterをリングサイズでmodを取ることで投入時のwqe_indexを引けることを利用する。
リングサイズと同サイズのWQE tableを用意し、CQEの`(wqe counter % ring_size)`で投入時のwqe_indexに格納されたユーザデータを取得すれば良い。

ここで重要なのがまずSQ(QP)/RQ(QP)/SRQ/TMSRQ/DCI/DCS(DCI Stream)で順序保証があったりなかったりすること。

以下のものは順序保証が存在する。これはあるwqe_counterが返った場合、それ以前のものは全て完了している保証がある。

- SQ(QP)
- RQ(QP)
- SRQ
- DCI

よってこれらの順序保証ありのものについては、WQE counterでinflightを厳密に数えられる。

一方で、DC Streamではstream-idが異なれば順序保証がない。また、TM-SRQでもTagが異なれば順序保証がない。これらは実はPartial Orderではあるが、
簡単のためOrderingなしとして扱う。

## ユーザデータ管理

幾つかの管理パターンが考えられる。

- Ordered + Dense + SendQueue
  - 順序保証つきのキューの実装
  - SignaledでないリクエストについてはCQEが現れないが、順序保証があることを利用すると前回処理したエントリから今回のエントリまでの間の全てのWQEが完了したことがわかる。
  - よって、全てのエントリにUser Dataを格納して、Signalを間引いても正しく処理可能
  - QP(SQ), DCI
- Ordered + Sparse
  - 順序保証つきのキューの実装。
  - Signaledでないリクエストについてはそもそもエントリ登録をしない。これは最適化のため
  - QP(SQ), DCI
- Unordered + SendQueue
  - 順序保証なしのキューの実装。
  - 順序保証がないので実質的に全てSignaled必須。個別にエントリを呼び出す
  - また、空き確認についてもWQE table上で使用する位置が解放されているかで確認する
  - 順序保証がないのでscanでの厳密な空き容量と楽観的な空き容量を両方取れると便利そう
  - DCI+Stream
- Recv Queue + Ordered
  - そもそも全部SignaledなのでUnordered + SendQueueと同様の全部管理の実装で良い。
  - ただし、厳密な空き容量を提供出来るのは異なる
  - QP(RQ), SRQ
- Recv Queue + Unordered
  - そもそも全部SignaledなのでUnordered + SendQueueと同様の全部管理の実装で良い。
  - ただし、厳密な空き容量は提供できない。
  - TM-SRQ

これらを見ると分かるように、まずWQEバッファ管理とwqe table管理は完全には切り離せない。
そして、Dense/Sparseのように両方提供可能な場合もある。
SQのDense/Sparseは一長一短なのでジェネリクスでどちらか片方を選べるようにする。それ以外は一意に決まる。

## Polling

これがあるので少し複雑。CQEはCQのPollingで取得するが、
このCQはSQ/RQとは別物。なのでCQE→SQ/RQ...をまず解決し、次にwqe_counterでwqe tableとsq_ciなどのWQEバッファ処理を行う必要がある。
CQが共有されている場合は上記のように、intmapでWeakなQPやSRQへのポインタを保持して解決するべきである。
CQを特定のキューに占有させる場合は不要ではある。

## 抽象化の方向性

QPは抽象としては実は微妙。
SQ, RQ, SRQ...って感じに分けられないかな？Rcとか所有権を使っていい感じに。
