# WqeBuilder設計

基本的に中間データは書かず、バッファへの直接書き込みでオーバーヘッド回避。
また、すべて`#[inline]`を付与してオーバーヘッド回避。

基本的な方針として、可能な限りimpl trait等を用いてユーザに見せる型を最小限にすること。

```rust
dci
  .sq_wqe()
  .av(dc_key, dctn, dlid)
  .sq_wqe()
  .send(Some(imm), TxFlags::empty())
  .sge(addr, len, lkey)
  .inline(&data)
  .signal(entry)
  .finish();
```

## AddressVector直行性

QPではAV不要だが、DCIおよびRoCEでのQP/DCIにはAddressVectorが必要。
また、AVは全てのVerbで必要なので、まず`sq_wqe()`の返り値で制御する。
この段階でRoCE/Infinibandの差分が吸収される。一方、RawPacketはそもそもVerbの種類が異なる。

- Infiniband RC QP (AVなし)
  - `sq_wqe() -> RdmaSqWqeBuilder`
- Infiniband DCI(AVあり)
  - `sq_wqe() -> DcSqWqeBuilder`
  - `DcSqWqeBuilder::av(dc_key, dctn, dlid) -> RdmaWqeBuilder`
- RoCE QP/DCI (AVあり)
  - `sq_wqe() -> RoceSqWqeBuilder`
  - `RoceSqWqeBuilder::av(dc_key, dctn, dlid) -> RdmaWqeBuilder`
 
これはそれぞれtraitとなる。具体型は隠蔽し、`impl trait`で返すことでAPIをシンプルにする。

## Verb派生

- `RdmaSqWqeBuilder`
  - `send(flag: TxFlags) -> SendWqeBuilder`
    - `signal(entry)`
    - `imm()`
    - `inline(&data)`
    - `sge(addr, len, lkey)`
    - `finish()`
    - sge/inlineは合計一個以上
  - `write() -> WriteWqeBuilder`
    - `signal(entry)`
    - `imm()`
    - `rdma(addr, len, rkey)`
    - `inline(&data)`
    - `sge(addr, len, lkey)`
    - `finish()`
    - sge/inlineは合計一個以上
  - `read() -> ReadWqeBuilder`
    - `signal(entry)`
    - `rdma(addr, len, rkey)`
    - `sge(addr, len, lkey)`
    - `finish()`
    - SGEは一個しか指定出来ないことに注意
  - `cas`
    - `signal(entry)`
    - `rdma(addr, len, rkey)`
    - `data(...)` ここ適当に埋めて
    - `finish()`
  - `fech_add`
    - `signal(entry)`
    - `rdma(addr, len, rkey)`
    - `data(...)` ここ適当に埋めて
    - `finish()`
  - `masked_cas`
    - `signal(entry)`
    - `rdma(addr, len, rkey)`
    - `data(...)` ここ適当に埋めて
    - `finish()`
  - `masked_fetch_add`
    - `signal(entry)`
    - `rdma(addr, len, rkey)`
    - `data(...)` ここ適当に埋めて
    - `finish()`

それぞれ派生のBuilderを返す。
それぞれ初期は`WriteWqeBuilder<(), ()>`のようなimpl traitを返し、
method chainで埋めていくと`WriteWqeBuilder<Has, Has>`の様な型に遷移させていって使用可能なメソッドを制限することで安全性を保つ。
sq_ciなどの操作はfinishで行い、finishを呼ばない限りabort（上書き）されることで不整合を防ぐ。wrap-aroundに関してはセマンティクスは壊れないので途中で操作しても問題ない。
メソッドの順番は適当なので構築しやすい様に整理しておいて。あと途中のBuilderはignoreすると警告が出るようにattributeをつけておく。

## Ordered/Unordered直行性

OrderedではSignalは選択可能だが、UnorderedではSignalは必須とする。これはWqeTableを型パラメータとして伝播させればいい。

## BlueFlame直行性

これらのWqe Operationは全てBlueFlame最適化が可能である。
そしてBlueFlameは複数Wqeの押し込みが可能。
まず`sq_wqe`に加えて、`sq_blueflame_wqe`を用意する。これは`SqBlueFlameWqeBuilder`を返す。
`wqe(&mut self)`を持ち、これから得られるBuilderを使うとBlueFlame上に直接Wqeを構築出来る。
最後は`finish(self)`でBlueFlameを押し込み直接Doorbellを鳴らす。
この時、実体としての方は通常のWqeBuilderとは別物として、BlueFlameに収まらなかった場合は`finish`でエラーを返し、
自分が書いた領域をクリアする処理を追加しておく。これで、`finish`がエラーならそこで`SqBlueFlameWqeBuilder::finish()`を呼んで書けたところまでNICに投入することが出来る。

## RQについて

基本的に同様の実装とする。
