# thread_spsc ボトルネック分析

## 1. アーキテクチャ概要

現在の実装は以下の構造：

```
TxBlock (64B aligned)
├─ tail: AtomicUsize        // Writer's tail position (visible to reader after flush)
└─ rx_dead: AtomicBool      // Reader disconnection flag

RxBlock (64B aligned)
├─ head: AtomicUsize        // Reader's head position (visible to writer)
└─ tx_dead: AtomicBool      // Writer disconnection flag

Sender
├─ head_cache: usize        // Cached value of rx.head to reduce atomic reads
└─ pending_tail: usize      // Local tail (not visible until flush())

Receiver
├─ local_tail: usize        // Local view of tx.tail (updated by sync())
└─ local_head: usize        // Local head (not visible until sync())
```

## 2. キャッシュライン競合分析

### 2.1 現在の分離状況

**良い点:**
- TxBlockとRxBlockは64バイトアライメントで完全に分離
- 各ブロックは独立したキャッシュラインに配置される

**問題点:**

#### A. TxBlock内での競合
```rust
#[repr(C, align(64))]
struct TxBlock {
    tail: AtomicUsize,      // Offset 0, 8 bytes
    rx_dead: AtomicBool,    // Offset 8, 1 byte
}
```
- `tail`と`rx_dead`が同じキャッシュライン上に存在
- Senderが`rx_dead.load()`を**毎回のwrite()で実行**している（189行目）
- これによりキャッシュライン全体がSenderのL1キャッシュに常駐
- **問題**: dropやdisconnection検出以外では不要な読み取り

#### B. RxBlock内での競合
```rust
#[repr(C, align(64))]
struct RxBlock {
    head: AtomicUsize,      // Offset 0, 8 bytes
    tx_dead: AtomicBool,    // Offset 8, 1 byte
}
```
- 同様に`head`と`tx_dead`が同じキャッシュライン上

#### C. slots配列とメタデータの配置
- `Inner`構造体内で`slots`はBoxedスライスとして別領域に配置される
- これは正しい設計（メタデータとデータを分離）

### 2.2 偽共有 (False Sharing) のシナリオ

**現状**: TxBlockとRxBlockが分離されているため、主要な偽共有は**発生していない**

**潜在的問題**:
- 同一キャッシュライン内の`tail`/`head`と`rx_dead`/`tx_dead`の配置により、
  disconnection checkがflush/syncのキャッシュトラフィックに影響を与える可能性がある
- ただし、実際の影響は軽微（disconnection flagは読み取り専用が多い）

## 3. Memory Ordering コスト分析

### 3.1 クリティカルパス上のアトミック操作

#### Sender側（write + flush のペア）

```rust
// write() - 189行目
self.inner.tx.rx_dead.load(Ordering::Relaxed)  // Cost: ~3-5 cycles (L1 hit時)
```
**問題**: 毎回のwrite()で実行される不要なチェック
- 通常ケースではReceiverは生存している
- disconnection時のみ必要な操作を毎回実行している

```rust
// write() - 198行目（キャッシュミス時のみ）
self.head_cache = self.inner.rx.head.load(Ordering::Acquire);
```
**コスト**: Acquire fence
- x86_64: MOV命令 + compiler barrier（ハードウェアfenceなし）
- ARM: MOV + LDAR (load-acquire) - ~数十サイクル
- **重要**: Acquireによりこの読み取り以降のメモリアクセスが順序保証される

```rust
// flush() - 222行目
self.inner.tx.tail.store(self.pending_tail, Ordering::Release);
```
**コスト**: Release fence
- x86_64: MOV命令 + compiler barrier（ハードウェアfenceなし）
- ARM: STLR (store-release) - ~数十サイクル
- **重要**: Releaseによりこの書き込み以前のメモリアクセスが順序保証される

#### Receiver側（sync + poll のループ）

```rust
// sync() - 271行目
self.local_tail = self.inner.tx.tail.load(Ordering::Acquire);
```
**コスト**: Acquire fence（上記と同様）

```rust
// sync() - 273行目
self.inner.rx.head.store(self.local_head, Ordering::Release);
```
**コスト**: Release fence（上記と同様）

#### try_recv() の追加コスト

```rust
// try_recv() - 287行目, 297行目
self.inner.rx.head.store(self.local_head, Ordering::Release);
```
**問題**: poll()成功時に**即座に**headをstoreしている
- sync()との**二重書き込み**が発生
- 287行目でstore → 293行目でload（sync内でload） → store
- **無駄なアトミック操作の典型例**

### 3.2 x86_64 vs ARM でのコスト差

| 操作 | x86_64 (TSO) | ARM (Weak) |
|------|-------------|------------|
| Acquire load | MOV + compiler barrier | LDAR (~10-40 cycles) |
| Release store | MOV + compiler barrier | STLR (~10-40 cycles) |
| Relaxed load | MOV (~3-5 cycles) | LDR (~3-5 cycles) |

**結論**: ARMでは**Acquire/Releaseのコストが非常に高い**

## 4. 命令レイテンシ分析

### 4.1 クリティカルパス上の命令シーケンス

#### write()のホットパス（キャッシュヒット時）

```rust
// Check rx_dead
if self.inner.tx.rx_dead.load(Ordering::Relaxed) {  // 1. LOAD (3-5 cycles)
    return Err(SendError(value));                    // 2. Branch (mispred: ~15-20 cycles)
}

let next_tail = (self.pending_tail + 1) & self.inner.mask;  // 3. ADD + AND (1-2 cycles)

if next_tail == self.head_cache {                    // 4. CMP (1 cycle)
    // Cache miss path (cold)                        // 5. Branch (predict not taken)
}

// Write value
unsafe {
    let slot = &*self.inner.slots[self.pending_tail].get();  // 6. Array access (~2-3 cycles)
    std::ptr::write(slot.as_ptr() as *mut T, value);         // 7. STORE (1 cycle + store buffer)
}

self.pending_tail = next_tail;                       // 8. MOV (1 cycle)
```

**合計（キャッシュヒット時）**: 約10-15サイクル + store buffer latency

**ボトルネック**:
1. `rx_dead` の不要なload（毎回3-5サイクル）
2. 分岐予測ミスのリスク（disconnection checkとcapacity check）

#### flush()

```rust
self.inner.tx.tail.store(self.pending_tail, Ordering::Release);
```
- x86_64: 1 MOV命令（1サイクル）+ store buffer
- ARM: 1 STLR命令（10-40サイクル）

#### poll()のホットパス（データあり時）

```rust
if self.local_head == self.local_tail {              // 1. CMP (1 cycle)
    self.sync();                                      // 2. Sync (cold path)
    if self.local_head == self.local_tail {           // 3. CMP (1 cycle)
        return None;
    }
}

let value = unsafe {
    let slot = &*self.inner.slots[self.local_head].get();  // 4. Array access (2-3 cycles)
    std::ptr::read(slot.as_ptr() as *const T)               // 5. LOAD (3-5 cycles, L1 hit時)
};

self.local_head = (self.local_head + 1) & self.inner.mask;  // 6. ADD + AND (1-2 cycles)
```

**合計（データあり時）**: 約8-12サイクル

#### sync()

```rust
self.local_tail = self.inner.tx.tail.load(Ordering::Acquire);    // LOAD + fence
self.inner.rx.head.store(self.local_head, Ordering::Release);    // STORE + fence
```

- x86_64: 2サイクル（compiler barrierのみ、ハードウェアfenceなし）
- ARM: 20-80サイクル（LDAR + STLR）

### 4.2 分岐予測の影響

#### write()の分岐

```rust
if self.inner.tx.rx_dead.load(Ordering::Relaxed) {  // 予測: taken率 < 0.01%
    return Err(SendError(value));
}
```
**問題**:
- 分岐予測器は"not taken"を学習
- disconnection時には必ずミスプレディクション
- ただし、通常ケースでは正しく予測される

```rust
if next_tail == self.head_cache {  // 予測: taken率は利用率依存
    self.head_cache = self.inner.rx.head.load(Ordering::Acquire);
    if next_tail == self.head_cache {
        return Err(SendError(value));
    }
}
```
**問題**:
- チャネルがほぼ空の場合: "not taken"が正解（予測成功）
- チャネルが満杯に近い場合: "taken"が頻発（キャッシュミス）
- **2段階の分岐**により予測が複雑化

#### poll()の分岐

```rust
if self.local_head == self.local_tail {  // 予測: データ到着率依存
    self.sync();
    if self.local_head == self.local_tail {
        return None;
    }
}
```
**問題**:
- バースト性のあるトラフィックでは予測成功
- ストリーミング的なトラフィックでも予測成功
- ただし、**sync()の呼び出し自体が重い**（特にARM）

## 5. 主要なボトルネック総括

### 5.1 Critical: 不要なアトミック操作

**問題1: write()内のrx_dead check（毎回実行）**
```rust
// 189行目
if self.inner.tx.rx_dead.load(Ordering::Relaxed) {
```
- **コスト**: 3-5サイクル/write
- **頻度**: 100%
- **必要性**: disconnection時のみ（< 0.01%）
- **改善案**:
  - Periodic check（N回に1回）
  - flush()時のみcheck
  - または完全に削除してdisconnection時のエラーハンドリングに任せる

**問題2: try_recv()内の二重head store**
```rust
// 287, 297行目
self.inner.rx.head.store(self.local_head, Ordering::Release);
```
- poll()成功後に即座にstoreしているが、その後のsync()でも再度storeされる
- **無駄なRelease fence操作**

### 5.2 High: ARM環境でのAcquire/Releaseコスト

```rust
// sync()内の2つのアトミック操作
self.local_tail = self.inner.tx.tail.load(Ordering::Acquire);    // LDAR: 10-40 cycles
self.inner.rx.head.store(self.local_head, Ordering::Release);    // STLR: 10-40 cycles
```

- **コスト**: 20-80サイクル/sync
- x86では negligible だが、ARMでは**支配的なコスト**
- **改善案**:
  - Read/Writeの分離（load-onlyとstore-onlyのsync）
  - Batch処理でsync頻度を削減

### 5.3 Medium: 2段階分岐によるキャッシュミス検出

```rust
// write() - 196-203行目
if next_tail == self.head_cache {
    self.head_cache = self.inner.rx.head.load(Ordering::Acquire);
    if next_tail == self.head_cache {
        return Err(SendError(value));
    }
}
```

- **コスト**:
  - キャッシュヒット時: 1サイクル（分岐予測成功時）
  - キャッシュミス時: Acquire load + 分岐
- **問題**: 2段階の分岐により命令パイプラインが複雑化
- **改善案**: なし（これが最適な実装）

### 5.4 Low: キャッシュライン内のrx_dead/tx_dead配置

- TxBlock/RxBlock内で`tail`/`head`と同じキャッシュラインに配置
- ただし、読み取り専用が多いため実際の影響は軽微

## 6. 最適化の優先順位

### Priority 1: write()内のrx_dead check削除/最適化
- **効果**: 3-5 cycles/write 削減
- **実装難易度**: Low
- **リスク**: disconnection検出が遅れる（許容可能）

### Priority 2: try_recv()のhead store最適化
- **効果**: 1 Release store削減
- **実装難易度**: Low
- **リスク**: None

### Priority 3: sync()の分離（read-only / write-only）
- **効果**: ARMで10-40 cycles削減（条件付き）
- **実装難易度**: Medium
- **リスク**: API設計の複雑化

### Priority 4: Batch API利用の徹底
- **効果**: sync/flush頻度削減により全体コスト削減
- **実装難易度**: Low（既に実装済み）
- **リスク**: None（ユーザーコードの変更が必要）

## 7. ベンチマーク予測

### 現状のコスト見積もり（x86_64、1往復あたり）

| 操作 | コスト（cycles） | 頻度 |
|------|----------------|------|
| write() | 10-15 | 1x |
| flush() | 1-2 | 1x |
| sync() | 2-5 | 1x |
| poll() | 8-12 | 1x |
| **合計** | **21-34 cycles** | - |

### 最適化後の見積もり（Priority 1, 2適用）

| 操作 | コスト（cycles） | 頻度 |
|------|----------------|------|
| write() | 6-10 (-4~-5) | 1x |
| flush() | 1-2 | 1x |
| sync() | 2-5 | 1x |
| poll() | 8-12 | 1x |
| **合計** | **17-29 cycles** | - |

**改善率**: 約15-20%（x86_64）

### ARM環境での予測

| 操作 | 現状 | 最適化後 | 改善 |
|------|------|---------|------|
| write() | 15-20 | 10-15 | -5 |
| flush() | 10-40 | 10-40 | 0 |
| sync() | 20-80 | 20-80 | 0 |
| poll() | 10-15 | 10-15 | 0 |
| **合計** | **55-155** | **50-150** | **~3-5%** |

**重要**: ARMでは**Acquire/Releaseのコストが支配的**なため、Priority 3の最適化が必須

## 8. 結論

**主要なボトルネック**:
1. write()内の不要な`rx_dead`チェック（毎回3-5サイクル）
2. try_recv()内の二重head store（不要なRelease fence）
3. ARM環境でのsync()のAcquire/Releaseコスト（20-80サイクル）

**推奨される最適化**:
1. `rx_dead`チェックの削除またはperiodic check化
2. try_recv()のhead store最適化
3. （ARM向け）sync()のread-only/write-only分離
4. バッチAPIの積極的な利用推奨

**期待される効果**:
- x86_64: 15-20%の性能向上
- ARM: Priority 1, 2のみでは3-5%、Priority 3も含めれば10-15%の性能向上
