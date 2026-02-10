//! Lock-free MPSC using SCQ (Scalable Circular Queue, Nikolaev DISC 2019).
//!
//! Design:
//! - SCQD (dual SCQ rings): aq (allocated queue) + fq (free queue)
//!   - fq starts full with indices 0..half-1
//!   - Caller: dequeue from fq → write data → enqueue into aq
//!   - Server: dequeue from aq → read data → enqueue into fq
//! - Per-caller SPSC response rings (common::ResponseRing)
//!
//! SCQ uses FAA for head/tail and single-width CAS per entry.
//! Entry encoding follows lfring.h reference implementation.

use crate::common::{CachePadded, ResponseRing};
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// SCQ Core Helpers
// ============================================================================

/// Signed comparison: (a - b) interpreted as signed.
#[inline(always)]
fn scmp(a: u64, b: u64) -> i64 {
    a.wrapping_sub(b) as i64
}

/// SCQ threshold for non-empty detection.
/// threshold3(half, n) = (half + n - 1) as signed.
#[inline(always)]
fn threshold3(half: u64, n: u64) -> i64 {
    (half + n - 1) as i64
}

/// LFRING_MIN = log2(cache_line_size / entry_size) = log2(64 / 8) = 3
const LFRING_MIN: usize = 3;

/// Map index to physical position for cache-line distribution.
/// Rotates bits so consecutive indices land on different cache lines.
#[inline(always)]
fn lfring_map(idx: u64, order: usize, n: u64) -> usize {
    let full_order = order + 1;
    if full_order <= LFRING_MIN {
        return (idx & (n - 1)) as usize;
    }
    (((idx & (n - 1)) >> (full_order - LFRING_MIN)) | ((idx << LFRING_MIN) & (n - 1))) as usize
}

// ============================================================================
// SCQ Ring Inner (single SCQ ring instance)
// ============================================================================

struct ScqRingInner {
    entries: Box<[AtomicU64]>,
    head: CachePadded<AtomicU64>,
    threshold: CachePadded<AtomicU64>, // signed via cast
    tail: CachePadded<AtomicU64>,
    order: usize,
    half: u64,
    n: u64,
}

unsafe impl Send for ScqRingInner {}
unsafe impl Sync for ScqRingInner {}

impl ScqRingInner {
    /// Create an empty SCQ ring (for aq: allocated queue).
    fn new_empty(order: usize) -> Self {
        let half = 1u64 << order;
        let n = half * 2;

        let entries: Vec<AtomicU64> = (0..n)
            .map(|_| AtomicU64::new(u64::MAX)) // -1 as unsigned
            .collect();

        Self {
            entries: entries.into_boxed_slice(),
            head: CachePadded::new(AtomicU64::new(0)),
            threshold: CachePadded::new(AtomicU64::new(u64::MAX)), // -1 as signed
            tail: CachePadded::new(AtomicU64::new(0)),
            order,
            half,
            n,
        }
    }

    /// Create a full SCQ ring (for fq: free queue) pre-loaded with indices 0..half-1.
    fn new_full(order: usize) -> Self {
        let half = 1u64 << order;
        let n = half * 2;
        let two_n_minus_1 = 2 * n - 1;

        let entries: Vec<AtomicU64> = (0..n).map(|_| AtomicU64::new(u64::MAX)).collect();
        let ring = Self {
            entries: entries.into_boxed_slice(),
            head: CachePadded::new(AtomicU64::new(0)),
            threshold: CachePadded::new(AtomicU64::new(threshold3(half, n) as u64)),
            tail: CachePadded::new(AtomicU64::new(half)),
            order,
            half,
            n,
        };

        // Pre-load indices 0..half-1 into the ring.
        // For index i at tail position i:
        //   tcycle = (i << 1) | (2*n - 1)
        //   eidx = i ^ (n - 1)
        //   entry = tcycle ^ eidx
        for i in 0..half {
            let tidx = lfring_map(i, order, n);
            let tcycle = (i << 1) | two_n_minus_1;
            let eidx = i ^ (n - 1);
            ring.entries[tidx].store(tcycle ^ eidx, Ordering::Relaxed);
        }

        // Remaining entries n/2..n-1 stay as -1 (empty).
        ring
    }

    /// Enqueue an index into this SCQ ring.
    /// For aq, this is called after writing data. For fq, this returns a slot.
    /// `nonempty`: if true, skip threshold update (caller guarantees non-empty).
    fn enqueue(&self, index: u64, nonempty: bool) {
        let n = self.n;
        let two_n_minus_1 = 2 * n - 1;
        let eidx = index ^ (n - 1);

        loop {
            let tail = self.tail.fetch_add(1, Ordering::AcqRel);
            let tcycle = (tail << 1) | two_n_minus_1;
            let tidx = lfring_map(tail, self.order, n);
            let mut entry = self.entries[tidx].load(Ordering::Acquire);

            loop {
                let ecycle = entry | two_n_minus_1;
                if scmp(ecycle, tcycle) < 0
                    && (entry == ecycle
                        || (entry == (ecycle ^ n) && self.head.load(Ordering::Acquire) <= tail))
                {
                    match self.entries[tidx].compare_exchange_weak(
                        entry,
                        tcycle ^ eidx,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            if !nonempty {
                                let thresh = threshold3(self.half, n) as u64;
                                if self.threshold.load(Ordering::Relaxed) != thresh {
                                    self.threshold.store(thresh, Ordering::Relaxed);
                                }
                            }
                            return;
                        }
                        Err(current) => {
                            entry = current;
                            continue; // retry with same tail
                        }
                    }
                }
                break; // try next tail position
            }
        }
    }

    /// Dequeue an index from this SCQ ring.
    /// Returns None if the ring is empty (only when !nonempty).
    fn dequeue(&self, nonempty: bool) -> Option<u64> {
        let n = self.n;
        let two_n_minus_1 = 2 * n - 1;

        if !nonempty && (self.threshold.load(Ordering::Relaxed) as i64) < 0 {
            return None;
        }

        loop {
            let head = self.head.fetch_add(1, Ordering::AcqRel);
            let hcycle = (head << 1) | two_n_minus_1;
            let hidx = lfring_map(head, self.order, n);
            let mut attempt = 0u32;

            'again: loop {
                let mut entry = self.entries[hidx].load(Ordering::Acquire);

                loop {
                    let ecycle = entry | two_n_minus_1;
                    if ecycle == hcycle {
                        // Found matching entry — consume it.
                        self.entries[hidx].fetch_or(n - 1, Ordering::AcqRel);
                        return Some(entry & (n - 1));
                    }

                    let entry_new;
                    if (entry | n) != ecycle {
                        entry_new = entry & !n;
                        if entry == entry_new {
                            break;
                        }
                    } else {
                        attempt += 1;
                        if attempt <= 10000 {
                            continue 'again;
                        }
                        entry_new = hcycle ^ (((!entry) & n) as u64);
                    }

                    if scmp(ecycle, hcycle) >= 0 {
                        break;
                    }

                    match self.entries[hidx].compare_exchange_weak(
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
                break;
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

    /// Dequeue an index from this SCQ ring (single-consumer optimized).
    /// This is identical to dequeue() but replaces FAA with load+store on head.
    /// ONLY safe when a single thread calls this method (e.g., server dequeuing from aq).
    fn dequeue_single_consumer(&self, nonempty: bool) -> Option<u64> {
        let n = self.n;
        let two_n_minus_1 = 2 * n - 1;

        if !nonempty && (self.threshold.load(Ordering::Relaxed) as i64) < 0 {
            return None;
        }

        loop {
            // Single-writer optimization: replace FAA with load+store
            let head = self.head.load(Ordering::Relaxed);
            self.head.store(head.wrapping_add(1), Ordering::Release);

            let hcycle = (head << 1) | two_n_minus_1;
            let hidx = lfring_map(head, self.order, n);
            let mut attempt = 0u32;

            'again: loop {
                let mut entry = self.entries[hidx].load(Ordering::Acquire);

                loop {
                    let ecycle = entry | two_n_minus_1;
                    if ecycle == hcycle {
                        // Found matching entry — consume it.
                        self.entries[hidx].fetch_or(n - 1, Ordering::AcqRel);
                        return Some(entry & (n - 1));
                    }

                    let entry_new;
                    if (entry | n) != ecycle {
                        entry_new = entry & !n;
                        if entry == entry_new {
                            break;
                        }
                    } else {
                        attempt += 1;
                        if attempt <= 10000 {
                            continue 'again;
                        }
                        entry_new = hcycle ^ (((!entry) & n) as u64);
                    }

                    if scmp(ecycle, hcycle) >= 0 {
                        break;
                    }

                    match self.entries[hidx].compare_exchange_weak(
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
                break;
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

    /// Enqueue an index into this SCQ ring (single-producer optimized).
    /// This is identical to enqueue() but replaces FAA with load+store on tail.
    /// ONLY safe when a single thread calls this method (e.g., server enqueuing into fq).
    fn enqueue_single_producer(&self, index: u64, nonempty: bool) {
        let n = self.n;
        let two_n_minus_1 = 2 * n - 1;
        let eidx = index ^ (n - 1);

        loop {
            // Single-writer optimization: replace FAA with load+store
            let tail = self.tail.load(Ordering::Relaxed);
            self.tail.store(tail.wrapping_add(1), Ordering::Release);

            let tcycle = (tail << 1) | two_n_minus_1;
            let tidx = lfring_map(tail, self.order, n);
            let mut entry = self.entries[tidx].load(Ordering::Acquire);

            loop {
                let ecycle = entry | two_n_minus_1;
                if scmp(ecycle, tcycle) < 0
                    && (entry == ecycle
                        || (entry == (ecycle ^ n) && self.head.load(Ordering::Acquire) <= tail))
                {
                    match self.entries[tidx].compare_exchange_weak(
                        entry,
                        tcycle ^ eidx,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            if !nonempty {
                                let thresh = threshold3(self.half, n) as u64;
                                if self.threshold.load(Ordering::Relaxed) != thresh {
                                    self.threshold.store(thresh, Ordering::Relaxed);
                                }
                            }
                            return;
                        }
                        Err(current) => {
                            entry = current;
                            continue; // retry with same tail
                        }
                    }
                }
                break; // try next tail position
            }
        }
    }

    /// Catchup: advance tail to match head when queue appears empty.
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
// Data Slot (holds request data + caller_id)
// ============================================================================

#[repr(C, align(64))]
struct DataSlot<T> {
    caller_id: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

// ============================================================================
// SCQ Request Ring (SCQD: aq + fq + data_slots)
// ============================================================================

struct ScqRequestRing<T> {
    /// Allocated queue: callers enqueue filled slot indices, server dequeues.
    aq: ScqRingInner,
    /// Free queue: server enqueues freed indices, callers dequeue to get free slots.
    fq: ScqRingInner,
    /// Data slots indexed by logical index (0..half).
    data_slots: Box<[DataSlot<T>]>,
    sender_count: AtomicUsize,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for ScqRequestRing<T> {}
unsafe impl<T: Send> Sync for ScqRequestRing<T> {}

impl<T> ScqRequestRing<T> {
    fn new(capacity: usize, num_senders: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let order = capacity.trailing_zeros() as usize;

        let data_slots: Vec<DataSlot<T>> = (0..capacity)
            .map(|_| DataSlot {
                caller_id: UnsafeCell::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            aq: ScqRingInner::new_empty(order),
            fq: ScqRingInner::new_full(order),
            data_slots: data_slots.into_boxed_slice(),
            sender_count: AtomicUsize::new(num_senders),
            rx_alive: AtomicBool::new(true),
        }
    }

    /// Caller: try to push a request.
    fn try_push(&self, caller_id: usize, data: T) -> Result<u64, CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(data));
        }

        // 1. Dequeue a free slot index from fq
        let slot_idx = match self.fq.dequeue(false) {
            Some(idx) => idx as usize,
            None => return Err(CallError::Full(data)),
        };

        // 2. Write data into data_slots[slot_idx]
        unsafe {
            *self.data_slots[slot_idx].caller_id.get() = caller_id;
            (*self.data_slots[slot_idx].data.get()).write(data);
        }

        // 3. Enqueue slot_idx into aq
        self.aq.enqueue(slot_idx as u64, false);

        Ok(slot_idx as u64)
    }

    /// Server: try to pop a request.
    fn try_pop(&self) -> Option<(usize, T, u64)> {
        // 1. Dequeue from aq (single-consumer optimized)
        let slot_idx = self.aq.dequeue_single_consumer(false)? as usize;

        // 2. Read data
        let caller_id = unsafe { *self.data_slots[slot_idx].caller_id.get() };
        let data = unsafe { (*self.data_slots[slot_idx].data.get()).assume_init_read() };

        // 3. Return slot to fq (single-producer optimized)
        self.fq.enqueue_single_producer(slot_idx as u64, false);

        Some((caller_id, data, slot_idx as u64))
    }

    fn available(&self) -> usize {
        let head = self.aq.head.load(Ordering::Acquire);
        let tail = self.aq.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head) as usize
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// ScqCaller
// ============================================================================

pub struct ScqCaller<Req, Resp> {
    caller_id: usize,
    req_ring: Arc<ScqRequestRing<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for ScqCaller<Req, Resp> {
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

    fn sync(&mut self) {
        // No-op: SCQ provides ordering via atomic operations.
    }

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

impl<Req, Resp> Drop for ScqCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// ScqServer
// ============================================================================

pub struct ScqServer<Req, Resp> {
    req_ring: Arc<ScqRequestRing<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    recv_counts: Vec<u64>,
    disconnected: bool,
}

pub struct ScqRecvRef<'a, Req, Resp> {
    server: &'a mut ScqServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> ScqRecvRef<'a, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for ScqRecvRef<'_, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for ScqServer<Req, Resp> {
    type RecvRef<'a>
        = ScqRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        self.req_ring.available() as u32
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data, _ring_token) = self.req_ring.try_pop()?;
        let slot_token = self.recv_counts[caller_id];
        self.recv_counts[caller_id] += 1;
        Some(ScqRecvRef {
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

impl<Req, Resp> Drop for ScqServer<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// ScqMpsc Factory
// ============================================================================

pub struct ScqMpsc;

impl MpscChannel for ScqMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = ScqCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = ScqServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two());
        assert!(max_callers > 0);

        let req_ring = Arc::new(ScqRequestRing::new(ring_depth, max_callers));
        let resp_rings: Vec<_> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(ring_depth)))
            .collect();

        let callers = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| ScqCaller {
                caller_id: id,
                req_ring: Arc::clone(&req_ring),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = ScqServer {
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
        let (mut callers, mut server) = ScqMpsc::create::<u64, u64>(1, 16, 16);
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
        let (mut callers, mut server) = ScqMpsc::create::<u64, u64>(4, 16, 8);

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
        let (mut callers, mut server) = ScqMpsc::create::<u64, u64>(1, 8, 8);
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
        let (mut callers, _server) = ScqMpsc::create::<u64, u64>(1, 16, 4);
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
        let (mut callers, mut server) = ScqMpsc::create::<u64, u64>(1, 16, 16);
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
        let (mut callers, mut server) = ScqMpsc::create::<u64, u64>(2, 16, 8);

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
        let (mut callers, _server) = ScqMpsc::create::<u64, u64>(1, 4, 4);
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
        let (mut callers, server) = ScqMpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = ScqMpsc::create::<u64, u64>(8, 256, 32);

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
