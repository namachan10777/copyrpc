# mlx5 WQE builder の &self 化設計メモ

## 背景
- 現状の WQE builder は `sq_wqe(&mut self)` など `&mut self` を要求するため、
  `Rc<RefCell<Qp>>` での運用が必要になり、`RefCell::borrow_mut()` のオーバーヘッドが発生する。
- WQE 構築は CPU 上で連続実行され、同一 SQ に対して同時に 2 つの builder が動かない前提なら
  `&self` で提供しても問題ない。

## 目標
- `&self` で WQE builder を開始できる API を提供し、`RefCell` を避ける。
- 競合や二重 builder を防止する仕組みは、必要最小限のコストで提供する。

## 観察
- `SendQueueState` は `Cell` を使っており、`&SendQueueState` から内部状態を進められる。
- したがって `&mut self` が必要なのは「同一 SQ に対して複数 builder を同時に走らせない」ための
  排他の意味合いが大きい。

## デザインパターン候補

### 1) スコープ型 (closure lending) パターン
`&self` で builder を作り、クロージャ内でのみ有効にする。
HRTB (Higher-Rank Trait Bound) を使うことで builder が外に漏れないようにする。

```rust
pub fn with_sq_wqe<R>(
    &self,
    f: impl for<'a> FnOnce(SqWqeEntryPoint<'a, SqEntry, NoAv>) -> io::Result<R>,
) -> io::Result<R> {
    let _guard = self.sq_borrow_guard()?; // 省略可能な排他ガード
    let sq = self.sq()?;
    f(SqWqeEntryPoint::new(sq, NoAv))
}
```

メリット:
- `Rc<Qp>` で十分になり `RefCell` が不要。
- builder が外に逃げない (直列実行が保証される)。

デメリット:
- クロージャの中で `self` を再利用して再帰的に `with_sq_wqe` を呼ぶと危険。
  そのため軽量な排他ガードを合わせるのが無難。

### 2) 明示的ガード (token/guard) パターン
`&self` から専用トークンを得て、それを持っている間のみ builder を作る。

```rust
pub fn sq_borrow(&self) -> io::Result<SqBorrow<'_>> { ... }

impl<'a> SqBorrow<'a> {
    pub fn sq_wqe(&self) -> io::Result<SqWqeEntryPoint<'a, SqEntry, NoAv>> { ... }
}
```

メリット:
- API 側で「排他を持っていること」を可視化できる。
- `Drop` で解放できるため直感的。

デメリット:
- 呼び出しがやや冗長。
- 実装は closure lending とほぼ同等で、最終的には軽量ガードが必要。

### 3) `unsafe` fast path
同時 builder が絶対起きない場所だけ `unsafe` API を使う。

```rust
pub unsafe fn sq_wqe_unchecked(&self) -> SqWqeEntryPoint<'_, SqEntry, NoAv> { ... }
```

メリット:
- 追加の分岐やガードを完全に排除できる。

デメリット:
- API 利用者の責務が大きく、誤用時の UB リスクが高い。

## 排他ガードの実装案
RefCell ほど重くない単純なフラグで十分。

```rust
struct BorrowFlag(Cell<bool>);

impl BorrowFlag {
    fn try_borrow(&self) -> io::Result<BorrowGuard<'_>> {
        if self.0.get() {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "builder already active"));
        }
        self.0.set(true);
        Ok(BorrowGuard { flag: self })
    }
}

struct BorrowGuard<'a> { flag: &'a BorrowFlag }

impl Drop for BorrowGuard<'_> {
    fn drop(&mut self) { self.flag.0.set(false); }
}
```

- `RefCell` と違って `borrow_mut` カウントや panic がないため軽量。
- `#[cfg(debug_assertions)]` でのみ有効にする選択肢もある。

## 推奨案
1) `with_sq_wqe` / `with_rq_wqe` / `with_ud_sq_wqe` のような closure lending API を追加する。
2) 内部で軽量ガード (BorrowFlag) を使い、同時 builder を防ぐ。
3) 既存の `&mut self` API は残し、互換性維持 + 移行パスを用意する。
4) 性能重視ユーザ向けに `unsafe fn sq_wqe_unchecked` を提供するのは検討余地あり。

## API 例 (案)

```rust
// IB
qp.with_sq_wqe(|wqe| {
    wqe.write(flags, remote_addr, rkey)?
        .sge(local_addr, len, lkey)
        .finish_signaled(entry)
})?;

// RoCE
qp.with_sq_wqe_roce(&grh, |wqe| {
    wqe.send(flags)?
        .inline(&data)?
        .finish_signaled(entry)
})?;
```

## 影響範囲 (想定)
- `Rc<RefCell<Qp>>` が不要になり、`BuildResult` の戻り型を `Rc<Qp>` に変更できる可能性。
- `mono_cq` / `cq` の登録パスで `RefCell` が不要か要再検討。

## リスクと注意点
- 再入や並行実行は SQ/RQ バッファ破壊につながるため、軽量ガードは必須。
- `&self` を返す API は「同時に 2 つ builder を作る」ことがコンパイル時に防げない。
  ガードで防ぎ、設計ドキュメントで明確に制約を示す必要がある。

