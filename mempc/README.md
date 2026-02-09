# mempc

共有メモリベースの高性能キュー実装。

## 参考文献

### BBQ (Block-based Bounded Queue)

- **論文**: Jiawei Wang, Diogo Behrens, Ming Fu, Lilith Oberhauser, Jonas Oberhauser. *"BBQ: A Block-based Bounded Queue for Exchanging Data and Profiling"*. USENIX ATC 2022.
- **PDF**: https://www.usenix.org/system/files/atc22-wang-jiawei.pdf
- **種別**: SPSC / MPSC / MPMC
- **手法**: リングバッファをブロック単位で管理し、producer/consumerがブロック単位でアクセスすることでキャッシュライン競合を最小化。SPSCでは既存実装 (Linux kernel, DPDK, Boost, Folly) に対して11.3x-42.4xのスループット向上を達成。

### SCQ (Scalable Circular Queue)

- **論文**: Ruslan Nikolaev. *"A Scalable, Portable, and Memory-Efficient Lock-Free FIFO Queue"*. DISC 2019.
- **PDF**: https://drops.dagstuhl.de/storage/00lipics/lipics-vol146-disc2019/LIPIcs.DISC.2019.28/LIPIcs.DISC.2019.28.pdf
- **arXiv**: https://arxiv.org/abs/1908.04511
- **実装**: https://github.com/rusnikola/lfqueue
- **種別**: MPMC (lock-free)
- **手法**: FAA (fetch-and-add) でHead/Tailを更新し、各スロットで単一幅CASのみ使用。LCRQと異なりdouble-width CAS (CAS2) を必要とせず、ABA-safeかつメモリ効率が高い。外部のメモリ回収機構やアロケータ不要。

### wCQ (Wait-free Circular Queue)

- **論文**: Ruslan Nikolaev. *"wCQ: A Fast Wait-Free Queue with Bounded Memory Usage"*. SPAA 2022.
- **PDF**: https://arxiv.org/pdf/2201.02179
- **arXiv**: https://arxiv.org/abs/2201.02179
- **実装**: https://github.com/rusnikola/wfqueue
- **種別**: MPMC (wait-free)
- **手法**: SCQをベースにfast-path-slow-pathで wait-free性を実現。slow pathでは全アクティブスレッドがstuckしたスレッドをassistし、SCQのlock-free保証により最終的に一つのスレッドが成功する。double-width CAS使用 (x86, AArch64対応)。メモリ使用量は有界。

### Jiffy

- **論文**: Dolev Adas, Roy Friedman. *"Jiffy: A Fast, Memory Efficient, Wait-Free Multi-Producers Single-Consumer Queue"*. DISC 2020 (brief announcement) / arXiv full version.
- **PDF**: https://arxiv.org/pdf/2010.14189
- **arXiv**: https://arxiv.org/abs/2010.14189
- **種別**: MPSC (wait-free)
- **手法**: バッファの連結リストで構成。enqueueは2番目のエントリ挿入時に次バッファを事前確保しCASで追加するため、バッファ末尾到達時にほぼ競合なし。dequeueはatomic操作を一切使わない。ノードあたりのメタデータは2ビットフラグのみで、空になったバッファは即座に解放。最大128スレッドで次善の実装比50%高いスループット、メモリ消費90%削減。

### LCRQ (Linked Concurrent Ring Queue)

- **論文**: Adam Morrison, Yehuda Afek. *"Fast Concurrent Queues for x86 Processors"*. PPoPP 2013.
- **PDF**: https://www.cs.tau.ac.il/~mad/publications/ppopp2013-x86queues.pdf
- **種別**: MPMC (lock-free)
- **手法**: Michael-Scott連結リストの各ノードをCRQ (Concurrent Ring Queue) で構成。CRQはFAA命令でスレッドをスロットに分散させ、並列enqueue/dequeueを実現。満杯またはlivelock発生時にCRQをcloseし新CRQを末尾に追加。x86のdouble-width CAS (CMPXCHG16B) を使用。

### LPRQ (Linked Portable Relaxed Queue)

- **論文**: Raed Romanov, Nikita Koval. *"The State-of-the-Art LCRQ Concurrent Queue Algorithm Does NOT Require CAS2"*. PPoPP 2023.
- **PDF**: https://nikitakoval.org/publications/ppopp23-lprq.pdf
- **ACM DL**: https://dl.acm.org/doi/10.1145/3572848.3577485
- **種別**: MPMC (lock-free)
- **手法**: LCRQからdouble-width CAS (CAS2) 依存を除去し、標準的なCASとFAA のみで同期を実現したポータブル版。Java, Kotlin, Go等CAS2が利用できない言語でも実装可能。性能はLCRQと同等で、CAS2不使用の既存手法に対して最大1.6x高速。

## ローカルPDF

`papers/` ディレクトリに論文PDFを保存済み (.gitignore対象):

| ファイル | 論文 |
|---------|------|
| `bbq-atc22.pdf` | BBQ (ATC 2022) |
| `scq-disc19.pdf` | SCQ (DISC 2019) |
| `wcq-arxiv.pdf` | wCQ (arXiv / SPAA 2022) |
| `jiffy-arxiv.pdf` | Jiffy (arXiv / DISC 2020) |
| `lcrq-ppopp13.pdf` | LCRQ (PPoPP 2013) |
| `lprq-ppopp23.pdf` | LPRQ (PPoPP 2023) |
