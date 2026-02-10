//! Lock-free ABA-free MPSC using wCQ CAS2 variant (Nikolaev, lfring_cas2.h).
//!
//! Design:
//! - SCQD (dual rings): aq (allocated queue) + fq (free queue)
//!   - fq starts full with indices 0..half-1
//!   - Caller: dequeue from fq → write data → enqueue into aq
//!   - Server: dequeue from aq → read data → enqueue into fq
//! - Per-caller SPSC response rings (common::ResponseRing)
//!
//! Uses 128-bit CAS (CMPXCHG16B on x86-64) via portable-atomic.
//! Each entry is a u128 pair: (entry_high, pointer_low).
//! - entry_high encodes cycle + state flags (bit 0x1 = has data, bit 0x2 = safe/dequeued)
//! - pointer_low carries the data slot index

use crate::common::{CachePadded, ResponseRing};
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use portable_atomic::{AtomicU128, Ordering as POrdering};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// CAS2 Entry Helpers
// ============================================================================

/// Pack entry (high 64) and pointer (low 64) into u128.
#[inline(always)]
fn pair(entry: u64, pointer: u64) -> u128 {
    ((entry as u128) << 64) | (pointer as u128)
}

/// Extract entry (high 64 bits) from u128.
#[inline(always)]
fn entry_of(p: u128) -> u64 {
    (p >> 64) as u64
}

/// Extract pointer (low 64 bits) from u128.
#[inline(always)]
fn pointer_of(p: u128) -> u64 {
    p as u64
}

/// Signed comparison.
#[inline(always)]
fn scmp(a: u64, b: u64) -> i64 {
    a.wrapping_sub(b) as i64
}

/// threshold4(n) = 2*n - 1
/// Reference: lfring_cas2.h __lfring_threshold4(n) = 2*n - 1
#[inline(always)]
fn threshold4(n: u64) -> i64 {
    (2 * n - 1) as i64
}

/// LFRING_MIN for 128-bit entries: log2(64/16) = 2
const LFRING_MIN: usize = 2;

/// Access the entry portion (upper 64 bits) of an AtomicU128 on little-endian.
/// The reference (lfring_cas2.h) uses `__lfring_array_entry()` for 64-bit
/// load/CAS on the entry half — critical for dequeue correctness with
/// concurrent enqueuers since it avoids spurious CAS failures from pointer changes.
///
/// # Safety
/// Requires little-endian platform with lock-free AtomicU128 (x86-64).
#[cfg(target_endian = "little")]
#[inline(always)]
unsafe fn entry_ref(atom: &AtomicU128) -> &AtomicU64 {
    unsafe { &*((atom as *const AtomicU128 as *const u8).add(8) as *const AtomicU64) }
}

/// Map index to physical position for cache-line distribution.
#[inline(always)]
fn lfring_map(idx: u64, order: usize, n: u64) -> usize {
    let full_order = order + 1;
    if full_order <= LFRING_MIN {
        return (idx & (n - 1)) as usize;
    }
    (((idx & (n - 1)) >> (full_order - LFRING_MIN)) | ((idx << LFRING_MIN) & (n - 1))) as usize
}

// ============================================================================
// wCQ CAS2 Ring Inner
// ============================================================================

#[allow(dead_code)]
struct WcqCas2RingInner {
    entries: Box<[AtomicU128]>,
    head: CachePadded<AtomicU64>,
    threshold: CachePadded<AtomicU64>, // signed via cast
    tail: CachePadded<AtomicU64>,
    order: usize,
    n: u64,
}

unsafe impl Send for WcqCas2RingInner {}
unsafe impl Sync for WcqCas2RingInner {}

#[allow(dead_code)]
impl WcqCas2RingInner {
    /// Create an empty ring (for aq).
    /// head=n, tail=n, all entries=0, threshold=-1
    fn new_empty(order: usize) -> Self {
        let half = 1u64 << order;
        let n = half * 2;

        let entries: Vec<AtomicU128> = (0..n).map(|_| AtomicU128::new(0)).collect();

        Self {
            entries: entries.into_boxed_slice(),
            head: CachePadded::new(AtomicU64::new(n)),
            threshold: CachePadded::new(AtomicU64::new(u64::MAX)), // -1 as signed
            tail: CachePadded::new(AtomicU64::new(n)),
            order,
            n,
        }
    }

    /// Create a full ring pre-loaded with indices 0..half-1.
    /// head=n, tail=n+half, entries[map(n+i)] = pair(n|0x1, i)
    #[allow(dead_code)]
    fn new_full(order: usize) -> Self {
        let half = 1u64 << order;
        let n = half * 2;

        let entries: Vec<AtomicU128> = (0..n).map(|_| AtomicU128::new(0)).collect();
        let ring = Self {
            entries: entries.into_boxed_slice(),
            head: CachePadded::new(AtomicU64::new(n)),
            threshold: CachePadded::new(AtomicU64::new(threshold4(n) as u64)),
            tail: CachePadded::new(AtomicU64::new(n + half)),
            order,
            n,
        };

        // Pre-fill: simulate half enqueue operations starting at tail=n.
        // Each enqueue at position (n+i) writes pair(tcycle | 0x1, i)
        // tcycle = (n+i) & ~(n-1) = n (since i < half < n)
        let tcycle = n; // n & ~(n-1) = n for power-of-two n
        for i in 0..half {
            let tidx = lfring_map(n + i, order, n);
            ring.entries[tidx].store(pair(tcycle | 0x1, i), POrdering::Relaxed);
        }

        ring
    }

    /// Enqueue a pointer (data slot index) into this ring.
    ///
    /// Reference: lfring_cas2.h `__lfring_enqueue`.
    fn enqueue(&self, ptr: u64, nonempty: bool) {
        let n = self.n;
        let n_mask = !(n - 1);

        loop {
            let tail = self.tail.fetch_add(1, Ordering::AcqRel);
            let tcycle = tail & n_mask;
            let tidx = lfring_map(tail, self.order, n);
            let mut p = self.entries[tidx].load(POrdering::Acquire);

            loop {
                let entry = entry_of(p);
                let ecycle = entry & n_mask;

                if scmp(ecycle, tcycle) < 0
                    && (entry == ecycle
                        || (entry == (ecycle | 0x2) && self.head.load(Ordering::Acquire) <= tail))
                {
                    match self.entries[tidx].compare_exchange_weak(
                        p,
                        pair(tcycle | 0x1, ptr),
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    ) {
                        Ok(_) => {
                            if !nonempty {
                                let thresh = threshold4(n) as u64;
                                if self.threshold.load(Ordering::Relaxed) != thresh {
                                    self.threshold.store(thresh, Ordering::Relaxed);
                                }
                            }
                            return;
                        }
                        Err(current) => {
                            p = current;
                            continue; // retry with same tail
                        }
                    }
                }
                break; // try next tail position
            }
        }
    }

    /// Enqueue a pointer (single-producer optimized version).
    /// ONLY safe when a single thread enqueues into this ring.
    #[allow(dead_code)]
    fn enqueue_single_producer(&self, ptr: u64, nonempty: bool) {
        let n = self.n;
        let n_mask = !(n - 1);

        loop {
            let tail = self.tail.load(Ordering::Relaxed);
            self.tail.store(tail.wrapping_add(1), Ordering::Release);
            let tcycle = tail & n_mask;
            let tidx = lfring_map(tail, self.order, n);
            let mut p = self.entries[tidx].load(POrdering::Acquire);

            loop {
                let entry = entry_of(p);
                let ecycle = entry & n_mask;

                if scmp(ecycle, tcycle) < 0
                    && (entry == ecycle
                        || (entry == (ecycle | 0x2) && self.head.load(Ordering::Acquire) <= tail))
                {
                    match self.entries[tidx].compare_exchange_weak(
                        p,
                        pair(tcycle | 0x1, ptr),
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    ) {
                        Ok(_) => {
                            if !nonempty {
                                let thresh = threshold4(n) as u64;
                                if self.threshold.load(Ordering::Relaxed) != thresh {
                                    self.threshold.store(thresh, Ordering::Relaxed);
                                }
                            }
                            return;
                        }
                        Err(current) => {
                            p = current;
                            continue; // retry with same tail
                        }
                    }
                }
                break; // try next tail position
            }
        }
    }

    /// Dequeue a pointer from this ring. Returns None if empty.
    ///
    /// Uses 64-bit load/CAS on entry only (matching lfring_cas2.h).
    #[allow(dead_code)]
    fn dequeue(&self, nonempty: bool) -> Option<u64> {
        let n = self.n;
        let n_mask = !(n - 1);

        if !nonempty && (self.threshold.load(Ordering::Relaxed) as i64) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::AcqRel);
            let hcycle = head & n_mask;
            let hidx = lfring_map(head, self.order, n);

            let entry_atom = unsafe { entry_ref(&self.entries[hidx]) };
            let mut entry = entry_atom.load(Ordering::Acquire);

            loop {
                let ecycle = entry & n_mask;
                if ecycle == hcycle {
                    // Found matching entry — consume it.
                    let mask = pair(!0x1u64, 0);
                    let old = self.entries[hidx].fetch_and(mask, POrdering::AcqRel);
                    return Some(pointer_of(old));
                }

                let entry_new;
                if (entry & !0x2u64) != ecycle {
                    entry_new = entry | 0x2;
                    if entry == entry_new {
                        break;
                    }
                } else {
                    entry_new = hcycle | (entry & 0x2);
                }

                if scmp(ecycle, hcycle) >= 0 {
                    break;
                }

                match entry_atom.compare_exchange_weak(
                    entry,
                    entry_new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => {
                        entry = current;
                    }
                }
            }

            // Handle potential empty queue
            if !nonempty {
                let tail = self.tail.load(Ordering::Acquire);
                if scmp(tail, head + 1) <= 0 {
                    self.catchup(tail, head + 1);
                    self.threshold.fetch_sub(1, Ordering::AcqRel);
                    return None;
                }
                if (self.threshold.fetch_sub(1, Ordering::AcqRel) as i64) <= 0 {
                    return None;
                }
            }
        }
    }

    /// Dequeue a pointer from this ring (single-consumer optimized version).
    /// ONLY safe when a single thread dequeues from this ring.
    fn dequeue_single_consumer(&self, nonempty: bool) -> Option<u64> {
        let n = self.n;
        let n_mask = !(n - 1);

        if !nonempty && (self.threshold.load(Ordering::Relaxed) as i64) < 0 {
            return None;
        }

        loop {
            let head = self.head.load(Ordering::Relaxed);
            self.head.store(head.wrapping_add(1), Ordering::Release);
            let hcycle = head & n_mask;
            let hidx = lfring_map(head, self.order, n);

            let entry_atom = unsafe { entry_ref(&self.entries[hidx]) };
            let mut entry = entry_atom.load(Ordering::Acquire);

            loop {
                let ecycle = entry & n_mask;
                if ecycle == hcycle {
                    let mask = pair(!0x1u64, 0);
                    let old = self.entries[hidx].fetch_and(mask, POrdering::AcqRel);
                    return Some(pointer_of(old));
                }

                let entry_new;
                if (entry & !0x2u64) != ecycle {
                    entry_new = entry | 0x2;
                    if entry == entry_new {
                        break;
                    }
                } else {
                    entry_new = hcycle | (entry & 0x2);
                }

                if scmp(ecycle, hcycle) >= 0 {
                    break;
                }

                match entry_atom.compare_exchange_weak(
                    entry,
                    entry_new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => {
                        entry = current;
                    }
                }
            }

            // Handle potential empty queue
            if !nonempty {
                let tail = self.tail.load(Ordering::Acquire);
                if scmp(tail, head + 1) <= 0 {
                    self.catchup(tail, head + 1);
                    self.threshold.fetch_sub(1, Ordering::AcqRel);
                    return None;
                }
                if (self.threshold.fetch_sub(1, Ordering::AcqRel) as i64) <= 0 {
                    return None;
                }
            }
        }
    }

    fn catchup(&self, mut tail: u64, mut head: u64) {
        while self
            .tail
            .compare_exchange_weak(tail, head, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            head = self.head.load(Ordering::Acquire);
            tail = self.tail.load(Ordering::Acquire);
            if scmp(tail, head) >= 0 {
                break;
            }
        }
    }
}

// ============================================================================
// Vyukov MPSC Ring (replaces CAS2 ring for aq in SCQD)
// ============================================================================
//
// CAS2's dequeue scans past entries whose enqueuers haven't completed their
// CAS yet — unlike SCQ which has a 10000-retry mechanism to detect pending
// enqueuers via the n-bit flag. CAS2's 128-bit CAS is atomic with no
// intermediate state, so the dequeuer can't tell if an enqueuer is pending.
// This causes head inflation identical to the fq problem.
//
// A Vyukov ring avoids this entirely: the consumer only advances when an
// item is actually ready (seq == tail + 1). No scanning, no head inflation.

#[repr(C, align(64))]
struct AqSlot {
    seq: AtomicU64,
    value: UnsafeCell<u64>,
}

unsafe impl Send for AqSlot {}
unsafe impl Sync for AqSlot {}

struct AqRing {
    buffer: Box<[AqSlot]>,
    /// Producer position (callers FAA).
    head: CachePadded<AtomicU64>,
    /// Consumer position (server load+store).
    tail: CachePadded<AtomicU64>,
    mask: u64,
    capacity: u64,
}

unsafe impl Send for AqRing {}
unsafe impl Sync for AqRing {}

impl AqRing {
    fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let buffer: Vec<AqSlot> = (0..capacity)
            .map(|i| AqSlot {
                seq: AtomicU64::new(i as u64),
                value: UnsafeCell::new(0),
            })
            .collect();
        Self {
            buffer: buffer.into_boxed_slice(),
            head: CachePadded::new(AtomicU64::new(0)),
            tail: CachePadded::new(AtomicU64::new(0)),
            mask: (capacity - 1) as u64,
            capacity: capacity as u64,
        }
    }

    /// Enqueue a slot index (multiple producers).
    /// Spins until the slot is available for this position.
    fn enqueue(&self, value: u64) {
        let pos = self.head.fetch_add(1, Ordering::Relaxed);
        let idx = (pos & self.mask) as usize;
        let slot = &self.buffer[idx];

        loop {
            if slot.seq.load(Ordering::Acquire) == pos {
                break;
            }
            std::hint::spin_loop();
        }

        unsafe { *slot.value.get() = value }
        slot.seq.store(pos + 1, Ordering::Release);
    }

    /// Try to dequeue a slot index (single consumer).
    fn try_dequeue(&self) -> Option<u64> {
        let tail = self.tail.load(Ordering::Relaxed);
        let idx = (tail & self.mask) as usize;
        let slot = &self.buffer[idx];

        if slot.seq.load(Ordering::Acquire) != tail + 1 {
            return None;
        }

        let value = unsafe { *slot.value.get() };
        slot.seq.store(tail + self.capacity, Ordering::Release);
        self.tail.store(tail + 1, Ordering::Release);

        Some(value)
    }

    fn available(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail) as usize
    }
}

// ============================================================================
// Free Slot Bitmap (replaces CAS2 ring for fq in SCQD)
// ============================================================================
//
// CAS2's monotonically-advancing head counter causes "head inflation" when
// multiple consumers scan an empty queue — the head advances past the
// producer's cycle, making freshly-enqueued items permanently unreachable.
//
// A bitmap avoids this entirely: callers CAS individual bits (no shared
// counter), so failed allocations have zero side-effects.

struct FreeSlotBitmap {
    words: Box<[AtomicU64]>,
    capacity: usize,
}

impl FreeSlotBitmap {
    /// Create bitmap with all `capacity` slots marked free (bits = 1).
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        let num_words = capacity.div_ceil(64);
        let mut words: Vec<AtomicU64> = (0..num_words).map(|_| AtomicU64::new(u64::MAX)).collect();
        // Clear bits beyond capacity in the last word.
        let last_bits = capacity % 64;
        if last_bits > 0 {
            words[num_words - 1] = AtomicU64::new((1u64 << last_bits) - 1);
        }
        Self {
            words: words.into_boxed_slice(),
            capacity,
        }
    }

    /// Try to allocate a free slot. Returns None if all slots are in use.
    fn try_alloc(&self) -> Option<usize> {
        for (word_idx, word) in self.words.iter().enumerate() {
            let mut val = word.load(Ordering::Acquire);
            loop {
                if val == 0 {
                    break;
                }
                let bit = val.trailing_zeros() as usize;
                let idx = word_idx * 64 + bit;
                if idx >= self.capacity {
                    break;
                }
                match word.compare_exchange_weak(
                    val,
                    val & !(1u64 << bit),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(idx),
                    Err(new_val) => val = new_val,
                }
            }
        }
        None
    }

    /// Free a slot back to the bitmap.
    fn free(&self, idx: usize) {
        debug_assert!(idx < self.capacity);
        let word_idx = idx / 64;
        let bit = idx % 64;
        self.words[word_idx].fetch_or(1u64 << bit, Ordering::Release);
    }
}

// ============================================================================
// Data Slot
// ============================================================================

struct DataSlot<T> {
    caller_id: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

// ============================================================================
// wCQ CAS2 Request Ring (SCQD: aq + fq + data_slots)
// ============================================================================

struct WcqCas2RequestRing<T> {
    aq: AqRing,
    fq: FreeSlotBitmap,
    data_slots: Box<[DataSlot<T>]>,
    sender_count: AtomicUsize,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for WcqCas2RequestRing<T> {}
unsafe impl<T: Send> Sync for WcqCas2RequestRing<T> {}

impl<T> WcqCas2RequestRing<T> {
    fn new(capacity: usize, num_senders: usize) -> Self {
        assert!(capacity.is_power_of_two());

        let data_slots: Vec<DataSlot<T>> = (0..capacity)
            .map(|_| DataSlot {
                caller_id: UnsafeCell::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            aq: AqRing::new(capacity),
            fq: FreeSlotBitmap::new(capacity),
            data_slots: data_slots.into_boxed_slice(),
            sender_count: AtomicUsize::new(num_senders),
            rx_alive: AtomicBool::new(true),
        }
    }

    fn try_push(&self, caller_id: usize, data: T) -> Result<u64, CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(data));
        }

        let slot_idx = match self.fq.try_alloc() {
            Some(idx) => idx,
            None => return Err(CallError::Full(data)),
        };

        unsafe {
            *self.data_slots[slot_idx].caller_id.get() = caller_id;
            (*self.data_slots[slot_idx].data.get()).write(data);
        }

        self.aq.enqueue(slot_idx as u64);
        Ok(slot_idx as u64)
    }

    fn try_pop(&self) -> Option<(usize, T, u64)> {
        let slot_idx = self.aq.try_dequeue()? as usize;

        let caller_id = unsafe { *self.data_slots[slot_idx].caller_id.get() };
        let data = unsafe { (*self.data_slots[slot_idx].data.get()).assume_init_read() };

        self.fq.free(slot_idx);
        Some((caller_id, data, slot_idx as u64))
    }

    fn available(&self) -> usize {
        self.aq.available()
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// WcqCas2Caller
// ============================================================================

pub struct WcqCas2Caller<Req, Resp> {
    caller_id: usize,
    req_ring: Arc<WcqCas2RequestRing<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for WcqCas2Caller<Req, Resp> {
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        self.req_ring.try_push(self.caller_id, req)?;

        let token = self.send_count;
        self.send_count += 1;
        self.inflight += 1;
        Ok(token)
    }

    fn sync(&mut self) {}

    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        let result = self.resp_ring.try_pop();
        if result.is_some() {
            self.inflight = self.inflight.saturating_sub(1);
        }
        result
    }

    fn pending_count(&self) -> usize {
        self.inflight
    }
}

impl<Req, Resp> Drop for WcqCas2Caller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// WcqCas2Server
// ============================================================================

pub struct WcqCas2Server<Req, Resp> {
    req_ring: Arc<WcqCas2RequestRing<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    recv_counts: Vec<u64>,
    disconnected: bool,
}

pub struct WcqCas2RecvRef<'a, Req, Resp> {
    server: &'a mut WcqCas2Server<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> WcqCas2RecvRef<'a, Req, Resp> {
    pub fn data(&self) -> &Req {
        &self.data
    }

    pub fn caller_id(&self) -> usize {
        self.caller_id
    }

    pub fn reply(self, resp: Resp) {
        let token = ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        };
        self.server.reply(token, resp);
    }

    pub fn into_token(self) -> ReplyToken {
        ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        }
    }
}

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for WcqCas2RecvRef<'_, Req, Resp> {
    #[inline]
    fn caller_id(&self) -> usize {
        self.caller_id
    }

    #[inline]
    fn data(&self) -> Req {
        self.data
    }

    #[inline]
    fn into_token(self) -> crate::ReplyToken {
        crate::ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        }
    }
}

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for WcqCas2Server<Req, Resp> {
    type RecvRef<'a>
        = WcqCas2RecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        self.req_ring.available() as u32
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data, _ring_token) = self.req_ring.try_pop()?;
        let slot_token = self.recv_counts[caller_id];
        self.recv_counts[caller_id] += 1;
        Some(WcqCas2RecvRef {
            server: self,
            caller_id,
            data,
            slot_token,
        })
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let resp_ring = &self.resp_rings[token.caller_id];
        while !resp_ring.try_push(token.slot_token, resp) {
            if !resp_ring.is_rx_alive() {
                return; // caller disconnected, discard response
            }
            std::hint::spin_loop();
        }
    }
}

impl<Req, Resp> Drop for WcqCas2Server<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_rx();
            for ring in &self.resp_rings {
                ring.disconnect_tx();
            }
        }
    }
}

// ============================================================================
// WcqCas2Mpsc Factory
// ============================================================================

pub struct WcqCas2Mpsc;

impl MpscChannel for WcqCas2Mpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = WcqCas2Caller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = WcqCas2Server<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two());
        assert!(max_callers > 0);

        let req_ring = Arc::new(WcqCas2RequestRing::new(ring_depth, max_callers));
        let resp_rings: Vec<_> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(ring_depth)))
            .collect();

        let callers = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| WcqCas2Caller {
                caller_id: id,
                req_ring: Arc::clone(&req_ring),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = WcqCas2Server {
            req_ring,
            resp_rings,
            recv_counts: vec![0u64; max_callers],
            disconnected: false,
        };

        (callers, server)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_single_caller() {
        let (mut callers, mut server) = WcqCas2Mpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        let tok = caller.call(42).unwrap();
        assert_eq!(tok, 0);
        assert_eq!(caller.pending_count(), 1);

        assert!(server.poll() >= 1);
        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 42);
        assert_eq!(recv.caller_id(), 0);

        recv.reply(84);

        let (resp_tok, resp) = caller.try_recv_response().unwrap();
        assert_eq!(resp_tok, tok);
        assert_eq!(resp, 84);
        assert_eq!(caller.pending_count(), 0);
    }

    #[test]
    fn multi_caller_concurrent() {
        let (mut callers, mut server) = WcqCas2Mpsc::create::<u64, u64>(4, 16, 8);

        for (i, caller) in callers.iter_mut().enumerate() {
            let val = (i as u64) * 100;
            caller.call(val).unwrap();
            caller.call(val + 1).unwrap();
        }

        assert!(server.poll() >= 8);

        let mut tokens = Vec::new();
        for _ in 0..8 {
            let recv = server.try_recv().unwrap();
            let caller_id = recv.caller_id();
            let val = *recv.data();
            let token = recv.into_token();
            tokens.push((caller_id, val, token));
        }

        for (_caller_id, val, token) in tokens {
            server.reply(token, val * 2);
        }

        for caller in callers.iter_mut() {
            let mut count = 0;
            while let Some((_, _resp)) = caller.try_recv_response() {
                count += 1;
            }
            assert_eq!(count, 2);
        }
    }

    #[test]
    fn ring_wraparound() {
        let (mut callers, mut server) = WcqCas2Mpsc::create::<u64, u64>(1, 8, 8);
        let caller = &mut callers[0];

        for round in 0u64..3 {
            for i in 0u64..8 {
                let val = round * 100 + i;
                caller.call(val).unwrap();
            }

            for _i in 0u64..8 {
                let recv = server.try_recv().unwrap();
                let val = *recv.data();
                recv.reply(val * 2);
            }

            for _ in 0..8 {
                let (_, resp) = caller.try_recv_response().unwrap();
                let _ = resp;
            }
        }
    }

    #[test]
    fn inflight_limit() {
        let (mut callers, _server) = WcqCas2Mpsc::create::<u64, u64>(1, 16, 4);
        let caller = &mut callers[0];

        for i in 0..4 {
            assert!(caller.call(i).is_ok());
        }

        match caller.call(100) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 100),
            _ => panic!("Expected InflightExceeded"),
        }
    }

    #[test]
    fn zero_copy_recv_ref() {
        let (mut callers, mut server) = WcqCas2Mpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        caller.call(999).unwrap();

        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 999);
        assert_eq!(*recv.data(), 999);
        recv.reply(1000);

        let (_, resp) = caller.try_recv_response().unwrap();
        assert_eq!(resp, 1000);
    }

    #[test]
    fn deferred_reply() {
        let (mut callers, mut server) = WcqCas2Mpsc::create::<u64, u64>(2, 16, 8);

        callers[0].call(10).unwrap();
        callers[1].call(20).unwrap();

        let recv1 = server.try_recv().unwrap();
        let data1 = *recv1.data();
        let tok1 = recv1.into_token();

        let recv2 = server.try_recv().unwrap();
        let data2 = *recv2.data();
        let tok2 = recv2.into_token();

        server.reply(tok2, data2 * 2);
        server.reply(tok1, data1 * 2);

        let (_, resp0) = callers[0].try_recv_response().unwrap();
        let (_, resp1) = callers[1].try_recv_response().unwrap();

        assert_eq!(resp0, 20);
        assert_eq!(resp1, 40);
    }

    #[test]
    fn full_ring() {
        let (mut callers, _server) = WcqCas2Mpsc::create::<u64, u64>(1, 4, 4);
        let caller = &mut callers[0];

        for i in 0..4 {
            assert!(caller.call(i).is_ok());
        }

        match caller.call(100) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 100),
            _ => panic!("Expected InflightExceeded"),
        }
    }

    #[test]
    fn disconnect_detection() {
        let (mut callers, server) = WcqCas2Mpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = WcqCas2Mpsc::create::<u64, u64>(8, 256, 32);

        const MSGS_PER_CALLER: usize = 100;

        let handles: Vec<_> = callers
            .into_iter()
            .enumerate()
            .map(|(id, mut caller)| {
                std::thread::spawn(move || {
                    let mut sent = 0;
                    while sent < MSGS_PER_CALLER {
                        let val = (id as u64) * 1000 + sent as u64;
                        match caller.call(val) {
                            Ok(_) => {
                                sent += 1;
                            }
                            Err(CallError::InflightExceeded(_)) => {
                                while caller.try_recv_response().is_some() {}
                            }
                            Err(CallError::Full(_)) => {
                                while caller.try_recv_response().is_some() {}
                                std::thread::yield_now();
                            }
                            Err(_) => {
                                std::thread::yield_now();
                            }
                        }
                    }

                    while caller.pending_count() > 0 {
                        while caller.try_recv_response().is_some() {}
                        std::thread::yield_now();
                    }

                    id
                })
            })
            .collect();

        let mut processed = 0;
        let expected = 8 * MSGS_PER_CALLER;

        while processed < expected {
            if let Some(recv) = server.try_recv() {
                let val = *recv.data();
                recv.reply(val + 1);
                processed += 1;
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(processed, expected);
    }
}
