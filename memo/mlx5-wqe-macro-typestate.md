# WQE macro + typestate design (draft)

## 背景
`mlx5` の WQE 構築は builder パターンを使っているが、現状は `&mut self` が必要になる。
WQE バッファは内部的に `UnsafeCell` を使えば `&self` から安全に書けるため、
macro で WQE 構築 API を生成しつつ「必須フィールドが全て設定済みか」を型システムで検査したい。

今回の前提は「macro で一発で WQE 全体を書いて CI 更新まで完了する」ことであり、
その場合は builder のように段階的な書き込みをしないため、
"予約トークン" を使わなくても安全性を保てる可能性がある。
そこで **コード生成なしで型安全性をどう担保するか** を中心に整理する。

## 目標
- `&self` から WQE を構築できる (内部は `UnsafeCell` による直接書き込み)。
- macro で WQE の setter 群と finish を生成する。
- 必須フィールド未設定のまま `finish` できないことを型で保証する。

## 主要アイデア
### 1) "一発書き込み" macro で `&self` を成立させる
macro が **WQE 全体を書き切る関数** を生成すれば、段階的な builder は不要。
`&self` で安全にするためには、以下を満たす必要がある:
- 1 回の呼び出しで WQE 書き込みと CI 更新まで完結する。
- WQE の各セグメントを書き込み順に固定する (バリア等も内部で完結)。
- 外部から WQE バッファに直接触れさせない (macro が唯一の入口)。

イメージ:

```rust
impl Sq {
    pub fn post_send(&self, args: SendArgs) -> Result<WqeHandle, SubmissionError> {
        // 空き確認 + wqe_idx 決定
        // WQE 全体を一発で書き込む
        // CI 更新 + doorbell
    }
}
```

この場合、線形トークンは「必須」ではない。
安全性は "WQE を一発で構築して commit まで完結させる" という設計制約で担保する。

### 2) typestate を const generics で表現する
必須フィールドの "設定済み/未設定" をビットで管理する。

```rust
struct Assert<const CHECK: bool>;
trait True {}
impl True for Assert<true> {}

const REQ_CTRL: u32 = 1 << 0;
const REQ_AV:   u32 = 1 << 1;
const REQ_DATA: u32 = 1 << 2;
const REQUIRED: u32 = REQ_CTRL | REQ_AV | REQ_DATA;

pub struct WqeBuilder<'a, const MASK: u32> {
    _not_used: PhantomData<&'a ()>,
}

impl<'a, const MASK: u32> WqeBuilder<'a, MASK> {
    pub fn ctrl(self, params: CtrlSegParams) -> WqeBuilder<'a, { MASK | REQ_CTRL }> { ... }
    pub fn av(self, av: &RoceAv) -> WqeBuilder<'a, { MASK | REQ_AV }> { ... }
    pub fn data_inline(self, data: &[u8]) -> WqeBuilder<'a, { MASK | REQ_DATA }> { ... }
}

impl<'a, const MASK: u32> WqeBuilder<'a, MASK>
where
    Assert<{ (MASK & REQUIRED) == REQUIRED }>: True,
{
    pub fn finish(self) -> WqeHandle { ... }
}
```

`finish` が `REQUIRED` を満たす型だけに実装されるので、
必須フィールド不足はコンパイルエラーになる。

### 3) macro で "一発書き込み" 関数を生成
macro が `post_send()` のような **全フィールド受け取り関数** を生成し、
`args` 型の構築を typestate でガードする。

```
#[derive(Clone, Copy)]
pub struct SendArgs<const MASK: u32> { ... }

impl SendArgs<0> {
    pub fn new() -> Self { ... }
}

impl<const MASK: u32> SendArgs<MASK> {
    pub fn ctrl(self, p: CtrlSegParams) -> SendArgs<{ MASK | REQ_CTRL }> { ... }
    pub fn av(self, av: RoceAv) -> SendArgs<{ MASK | REQ_AV }> { ... }
    pub fn data(self, data: InlineData) -> SendArgs<{ MASK | REQ_DATA }> { ... }
}

impl<const MASK: u32> SendArgs<MASK>
where
    Assert<{ (MASK & REQUIRED) == REQUIRED }>: True,
{
    fn into_raw(self) -> RawSendArgs { ... }
}

impl Sq {
    pub fn post_send(&self, args: SendArgs<{ REQUIRED }>) -> Result<WqeHandle, SubmissionError> {
        // args は必須フィールドが揃っている型でしか呼べない
    }
}
```

builder ではなく `args` を段階的に組み上げる方式なので、
WQE 構築自体は "一発書き込み" の関数で完結させられる。

## codegen を使わずに型安全性を整合させる方法 (まとめ)
1) `SendArgs<const MASK: u32>` のような型を手書きで用意する。
2) setter メソッドは `MASK | REQ_*` へ遷移する型を返す。
3) `into_raw()` や `post_send()` を `REQUIRED` を満たす型にだけ実装する。
4) 実際の WQE 書き込みは `post_send()` の内部で一発で完結させる。

macro を使うなら setter/required の boilerplate を減らせるが、
**型安全性の要点は const generics + where 句** なので codegen なしでも成立する。

## "一発書き込み" 方式の利点/注意
利点:
- `&self` のまま WQE を構築できる (`UnsafeCell` 前提)。
- builder の `&mut self` 不要で API が簡潔。

注意:
- WQE 書き込み順や ds_count 計算などを 1 関数内に閉じ込める必要がある。
- 途中でエラーが出た場合に "書きかけ WQE" を NIC に見せない設計が必須。
  (書き込み後に必ず CI 更新を行うなどの順序を厳密化)
