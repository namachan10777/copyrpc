InfinibandのCQEはそれ単体ではユーザメタデータを含めることが出来ない。
管理はCQEのwqe_counterを使用する。これは投入時のwqe indexであり、キューサイズでのmodを取ることで投入時のindexを取得可能である。
このmod計算の高速化のために、キューサイズは2の冪乗としてマスクとのbit andで計算するべきである。

このwqe indexに基づくwqe tableは、キューサイズと同じだけの配列を使って実装される。
一般的なRC、SRQ、Streamを使わないDCにおいては、WQE投入順序とCQE可視化順序が揃う。
よって、最後に観測したwqe_counterから現在のwqe_counterまで全て完了と見做せる。
この間のwqe tableに記録された値をコールバックで渡せば良い。
この場合、最適化を考えると2パターンありえる。

- Signaledの場合のみWQE table entry有効
  - Signaledのフラグの代わりにOptionでEntryに入れる値をWQE builderで指定し、SomeならSignaledかつWQE entryに書き込み
  - SignaledなEntryのみをcallback
- Signaled以外でもWQE table entry有効
  - CQEが可視化された場合、wqe_counterによってそれまでのWQE entryを全て順序通りcallbackに渡して呼び出す。
  
また、DC Stream有効化時のDC SQ、TM-SRQは順序保証がない（正確には、同一Streamでは順序保証が存在する。ただし、複雑になりすぎるので扱わない）。
こちらはWQE buffer自体が歯抜けになる可能性があるため、WQE table entryを利用してWQE書き込み可能性を判断するのが適切である。
WQEエントリをOptionにし、全てをSignaled強制にし、CQEによって当該エントリの中身をNoneに置き換える。
書き込み可能かどうかは現在のwqe indexに対応するテーブルのエントリがNoneであるかどうかで判別する。
availableは厳密性を持たない、`optimistic_available()`（こちらは順序保証のあるキューと同様。ただし、歯抜けがありえるので実際にはこの値より小さい可能性がある）。
と、`scan_to_calcurate_exact_available()`（確実だが遅い）があると良いだろう。

これらのインターフェースを総合し、キューとWQE tableの統合を考える。
そうすると、
- Signaled flag + WQE table entry value
  - 順序保証 + 全てのWQE table entryがcallbackで処理
- `Option`で表現されたWQE table entry value
  - 順序保証 + Signaledなtable entryのみcallback
- WQE table entry valueのみ
  - 順序保証なし
をWQE投入インターフェースで分ける必要が出てくる。
これはtraitを使うことでコードの共通化が可能である。
