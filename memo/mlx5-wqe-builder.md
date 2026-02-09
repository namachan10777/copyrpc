# WqeBuilder設計

基本的に中間データは書かず、バッファへの直接書き込みでオーバーヘッド回避。
また、すべて`#[inline]`を付与してオーバーヘッド回避。

```rust
struct RoceSqWqeBuilder {}
struct IbSqWqeBuilder {}

impl Qp<Roce> {
    pub fn sq_wqe(&self) -> RoceSqWqeBuilder;
}

impl Qp<Ib> {
    pub fn sq_wqe(&self) -> IbSqWqeBuilder;
}

struct SendWqeBuilder<Entry, WqeTable, Av, DataMarker, Entry> {}

impl RoceSqWqeBuilder {
    pub fn send(&self, imm: Option<u32>, flags: u32) -> SendWqeBuilder<RoceAv, ()>;
}

impl<Entry, WqeTable, Av, DataMarker> SendWqeBuilder<Entry, WqeTable, Av, DataMarker> {
    pub fn inline(&self, data: &[u8]) -> SendWqeBuilder<Entry, WqeTable, Av, HasData, Entry>;
    pub fn sge(&self, lkey: u32, addr: u64, length: u32) -> SendWqeBuilder<Entry, WqeTable, Av, HasData, Entry>;
}

impl<Entry, WqeTable, Av> SendWqeBuilder<Entry, WqeTable, Av, HasData> {
    pub fn finish_signaled(self, entry: Entry);
}

impl<Entry, Av> SendWqeBuilder<Entry, OrderedWqeTable, Av, HasData> {
    pub fn finish_unsignaled(self);
}
```
