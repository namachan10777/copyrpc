//! Lock-free MPSC using BBQ (Block-based Bounded Queue, USENIX ATC 2022).
//!
//! Design:
//! - Single shared BBQ-MPSC request ring: all callers → server
//! - Per-caller SPSC response rings: server → each caller (valid-flag based, same as fetch_add)
//!
//! BBQ divides the ring buffer into B blocks of NE entries each.
//! Producers claim slots via fetch_add *within* a block, and only CAS on the
//! block-level head pointer when a block fills up (once every NE messages).
//! The consumer operates on a different block, eliminating cache-line contention
//! with producers.
//!
//! Block metadata:
//! - `allocated`: producers claim slots via fetch_add (intra-block)
//! - `committed`: sequential watermark (producer spin-waits for turn)
//! - `version`: ABA prevention; incremented when a block is recycled
//!
//! Note on committed watermark: The BBQ paper uses independent commit via fetch_add,
//! but this requires either block-granularity consumption (high latency for partial
//! blocks) or per-entry valid flags (extra atomic loads on consumer path). With few
//! producers (< ~8), sequential commit outperforms both alternatives because the
//! short waiting chain cost is lower than the overhead of per-entry visibility checks.

use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

const DEFAULT_BLOCK_COUNT: usize = 4;

// ============================================================================
// Cache-line padding
// ============================================================================

#[repr(C, align(64))]
struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

// ============================================================================
// BBQ Request Ring
// ============================================================================

#[repr(C)]
struct BbqEntry<T> {
    caller_id: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// Per-block metadata. Cache-line aligned to avoid false sharing between blocks.
#[repr(C, align(64))]
struct BbqBlock {
    /// Producers claim slots via fetch_add on this counter.
    allocated: AtomicU32,
    /// Sequential commit watermark: entry i is readable when committed > i.
    committed: AtomicU32,
    /// ABA-prevention version (incremented each time the block is recycled).
    version: AtomicU32,
}

struct BbqRequestRing<T> {
    entries: Box<[BbqEntry<T>]>,
    blocks: Box<[BbqBlock]>,
    /// Producer head: monotonically increasing block index.
    phead: CachePadded<AtomicUsize>,
    /// Consumer tail: monotonically increasing block index.
    ctail: CachePadded<AtomicUsize>,
    block_size: u32,
    #[allow(dead_code)]
    block_count: usize,
    block_mask: usize,
    /// log2(block_count) for replacing division with shift.
    block_count_shift: u32,
    /// log2(block_size) for replacing multiplication with shift.
    block_size_shift: u32,
    sender_count: AtomicUsize,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for BbqRequestRing<T> {}
unsafe impl<T: Send> Sync for BbqRequestRing<T> {}

impl<T> BbqRequestRing<T> {
    fn new(ring_depth: usize, block_count: usize, num_senders: usize) -> Self {
        assert!(ring_depth.is_power_of_two());
        assert!(block_count.is_power_of_two());
        assert!(ring_depth >= block_count);
        assert_eq!(ring_depth % block_count, 0);

        let block_size = (ring_depth / block_count) as u32;
        let block_count_shift = block_count.trailing_zeros();
        let block_size_shift = block_size.trailing_zeros();

        let entries: Vec<BbqEntry<T>> = (0..ring_depth)
            .map(|_| BbqEntry {
                caller_id: UnsafeCell::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        let blocks: Vec<BbqBlock> = (0..block_count)
            .map(|_| BbqBlock {
                allocated: AtomicU32::new(0),
                committed: AtomicU32::new(0),
                version: AtomicU32::new(0),
            })
            .collect();

        Self {
            entries: entries.into_boxed_slice(),
            blocks: blocks.into_boxed_slice(),
            phead: CachePadded::new(AtomicUsize::new(0)),
            ctail: CachePadded::new(AtomicUsize::new(0)),
            block_size,
            block_count,
            block_mask: block_count - 1,
            block_count_shift,
            block_size_shift,
            sender_count: AtomicUsize::new(num_senders),
            rx_alive: AtomicBool::new(true),
        }
    }

    fn try_push(&self, caller_id: usize, data: T) -> Result<(), CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(data));
        }

        loop {
            let phead = self.phead.load(Ordering::Acquire);
            let block_idx = phead & self.block_mask;
            let expected_version = (phead >> self.block_count_shift) as u32;
            let block = &self.blocks[block_idx];

            // Check version to see if this block is available for the current round
            let version = block.version.load(Ordering::Acquire);
            if version != expected_version {
                // Block not yet recycled by consumer → ring is full
                if !self.rx_alive.load(Ordering::Relaxed) {
                    return Err(CallError::Disconnected(data));
                }
                return Err(CallError::Full(data));
            }

            // Try to allocate a slot within this block
            let pos = block.allocated.fetch_add(1, Ordering::Relaxed);
            if pos >= self.block_size {
                // Block is full. Try to advance phead to the next block.
                let _ = self.phead.compare_exchange(
                    phead,
                    phead + 1,
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                // Retry from the top with the new phead
                continue;
            }

            // Write data into the entry
            let entry_idx = (block_idx << self.block_size_shift) + pos as usize;
            unsafe {
                *self.entries[entry_idx].caller_id.get() = caller_id;
                (*self.entries[entry_idx].data.get()).write(data);
            }

            // Sequential commit: spin until it's our turn, then advance watermark.
            // Note: BBQ paper uses fetch_add for independent commit, but sequential
            // commit performs better with few producers (< ~8) because it avoids
            // per-entry valid flags and allows the consumer to read entries as soon
            // as the watermark advances, without gaps.
            loop {
                if block.committed.load(Ordering::Acquire) == pos {
                    block.committed.store(pos + 1, Ordering::Release);
                    break;
                }
                if !self.rx_alive.load(Ordering::Relaxed) {
                    while block.committed.load(Ordering::Acquire) != pos {
                        std::hint::spin_loop();
                    }
                    block.committed.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
                std::hint::spin_loop();
            }

            return Ok(());
        }
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// Response Ring (SPSC, per-caller) — same as fetch_add
// ============================================================================

#[repr(C, align(64))]
struct RespSlot<T> {
    valid: AtomicBool,
    data: UnsafeCell<MaybeUninit<(u64, T)>>,
}

struct ResponseRing<T> {
    buffer: Box<[RespSlot<T>]>,
    write_pos: CachePadded<AtomicUsize>,
    read_pos: CachePadded<AtomicUsize>,
    mask: usize,
    tx_alive: AtomicBool,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for ResponseRing<T> {}
unsafe impl<T: Send> Sync for ResponseRing<T> {}

impl<T> ResponseRing<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let buffer = (0..capacity)
            .map(|_| RespSlot {
                valid: AtomicBool::new(false),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            buffer,
            write_pos: CachePadded::new(AtomicUsize::new(0)),
            read_pos: CachePadded::new(AtomicUsize::new(0)),
            mask: capacity - 1,
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        }
    }

    fn try_push(&self, token: u64, data: T) -> bool {
        if !self.rx_alive.load(Ordering::Acquire) {
            return false;
        }

        let pos = self.write_pos.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];

        if slot.valid.load(Ordering::Acquire) {
            return false;
        }

        unsafe {
            (*slot.data.get()).write((token, data));
        }

        slot.valid.store(true, Ordering::Release);
        self.write_pos.store(pos + 1, Ordering::Release);
        true
    }

    fn try_pop(&self) -> Option<(u64, T)> {
        let pos = self.read_pos.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];

        if !slot.valid.load(Ordering::Acquire) {
            return None;
        }

        let data = unsafe { (*slot.data.get()).assume_init_read() };

        slot.valid.store(false, Ordering::Release);
        self.read_pos.store(pos + 1, Ordering::Release);

        Some(data)
    }

    fn disconnect_tx(&self) {
        self.tx_alive.store(false, Ordering::Release);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// BbqCaller
// ============================================================================

pub struct BbqCaller<Req, Resp> {
    caller_id: usize,
    req_ring: Arc<BbqRequestRing<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for BbqCaller<Req, Resp> {
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
        // No-op: BBQ provides ordering via committed watermark
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

impl<Req, Resp> Drop for BbqCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// BbqServer
// ============================================================================

pub struct BbqServer<Req, Resp> {
    req_ring: Arc<BbqRequestRing<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    recv_counts: Vec<u64>,
    /// Current entry index within the current consumer block.
    consumer_entry_idx: u32,
    /// Cached committed watermark for the current block.
    cached_committed: u32,
    disconnected: bool,
}

pub struct BbqRecvRef<'a, Req, Resp> {
    server: &'a mut BbqServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> BbqRecvRef<'a, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for BbqRecvRef<'_, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> BbqServer<Req, Resp> {
    fn try_advance_block(&mut self) -> bool {
        let ring = &*self.req_ring;
        let ctail = ring.ctail.load(Ordering::Relaxed);
        let block_idx = ctail & ring.block_mask;
        let block = &ring.blocks[block_idx];

        // Check if the entire block has been consumed
        if self.consumer_entry_idx < ring.block_size {
            return false;
        }

        // Ensure all slots were allocated (block was fully used)
        let allocated = block.allocated.load(Ordering::Acquire);
        if allocated < ring.block_size {
            return false;
        }

        // Recycle the block: reset counters and bump version
        let next_version = ((ctail >> ring.block_count_shift) as u32) + 1;
        block.allocated.store(0, Ordering::Relaxed);
        block.committed.store(0, Ordering::Relaxed);
        block.version.store(next_version, Ordering::Release);

        // Advance consumer tail
        ring.ctail.store(ctail + 1, Ordering::Release);

        // Reset per-block consumer state
        self.consumer_entry_idx = 0;
        self.cached_committed = 0;

        true
    }
}

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for BbqServer<Req, Resp> {
    type RecvRef<'a>
        = BbqRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        // Try to recycle completed blocks first
        while self.try_advance_block() {}

        let ring = &*self.req_ring;
        let ctail = ring.ctail.load(Ordering::Relaxed);
        let block_idx = ctail & ring.block_mask;
        let block = &ring.blocks[block_idx];

        let committed = block.committed.load(Ordering::Acquire);
        self.cached_committed = committed;

        committed.saturating_sub(self.consumer_entry_idx)
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        // Try to advance to the next block if current is exhausted
        self.try_advance_block();

        let ring = &*self.req_ring;
        let ctail = ring.ctail.load(Ordering::Relaxed);
        let block_idx = ctail & ring.block_mask;
        let block = &ring.blocks[block_idx];

        // Refresh committed if needed
        if self.consumer_entry_idx >= self.cached_committed {
            let committed = block.committed.load(Ordering::Acquire);
            self.cached_committed = committed;
            if self.consumer_entry_idx >= committed {
                return None;
            }
        }

        let entry_idx = (block_idx << ring.block_size_shift) + self.consumer_entry_idx as usize;
        let caller_id = unsafe { *ring.entries[entry_idx].caller_id.get() };
        let data = unsafe { (*ring.entries[entry_idx].data.get()).assume_init_read() };

        self.consumer_entry_idx += 1;

        let slot_token = self.recv_counts[caller_id];
        self.recv_counts[caller_id] += 1;

        Some(BbqRecvRef {
            server: self,
            caller_id,
            data,
            slot_token,
        })
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let resp_ring = &self.resp_rings[token.caller_id];
        while !resp_ring.try_push(token.slot_token, resp) {
            std::hint::spin_loop();
        }
    }
}

impl<Req, Resp> Drop for BbqServer<Req, Resp> {
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
// BbqMpsc Factory
// ============================================================================

pub struct BbqMpsc;

impl MpscChannel for BbqMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = BbqCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = BbqServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two());
        assert!(max_callers > 0);
        assert!(ring_depth >= DEFAULT_BLOCK_COUNT);
        assert_eq!(ring_depth % DEFAULT_BLOCK_COUNT, 0);

        let req_ring = Arc::new(BbqRequestRing::new(
            ring_depth,
            DEFAULT_BLOCK_COUNT,
            max_callers,
        ));
        let resp_rings: Vec<_> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(ring_depth)))
            .collect();

        let callers = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| BbqCaller {
                caller_id: id,
                req_ring: Arc::clone(&req_ring),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = BbqServer {
            req_ring,
            resp_rings,
            recv_counts: vec![0u64; max_callers],
            consumer_entry_idx: 0,
            cached_committed: 0,
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
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(1, 16, 16);
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
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(4, 16, 8);

        for (i, caller) in callers.iter_mut().enumerate() {
            let val = (i as u64) * 100;
            caller.call(val).unwrap();
            caller.call(val + 1).unwrap();
        }

        assert!(server.poll() >= 1);

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
        // ring_depth=16, block_count=4, block_size=4
        // BBQ requires consumer to recycle blocks before producers can reuse them.
        // Test by filling/draining one block at a time across multiple rounds.
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        for round in 0u64..3 {
            for block in 0u64..4 {
                for i in 0u64..4 {
                    let val = round * 100 + block * 10 + i;
                    caller.call(val).unwrap();
                }

                for _ in 0..4 {
                    server.poll();
                    let recv = server.try_recv().unwrap();
                    let val = *recv.data();
                    recv.reply(val * 2);
                }

                for _ in 0..4 {
                    let (_, _resp) = caller.try_recv_response().unwrap();
                }
            }
        }
    }

    #[test]
    fn inflight_limit() {
        let (mut callers, _server) = BbqMpsc::create::<u64, u64>(1, 16, 4);
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
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        caller.call(999).unwrap();

        server.poll();
        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 999);
        assert_eq!(*recv.data(), 999);
        recv.reply(1000);

        let (_, resp) = caller.try_recv_response().unwrap();
        assert_eq!(resp, 1000);
    }

    #[test]
    fn deferred_reply() {
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(2, 16, 8);

        callers[0].call(10).unwrap();
        callers[1].call(20).unwrap();

        server.poll();
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
        let (mut callers, _server) = BbqMpsc::create::<u64, u64>(1, 8, 16);
        let caller = &mut callers[0];

        for i in 0u64..8 {
            assert!(caller.call(i).is_ok());
        }

        match caller.call(100) {
            Err(CallError::Full(val)) => assert_eq!(val, 100),
            other => panic!("Expected Full, got {:?}", other.map(|v| v)),
        }
    }

    #[test]
    fn disconnect_detection() {
        let (mut callers, server) = BbqMpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = BbqMpsc::create::<u64, u64>(8, 256, 32);

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
                            Err(CallError::InflightExceeded(_)) | Err(CallError::Full(_)) => {
                                while caller.try_recv_response().is_some() {}
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
            server.poll();
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

    #[test]
    fn block_advancement() {
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        for i in 0u64..4 {
            caller.call(i).unwrap();
        }

        for i in 0u64..4 {
            server.poll();
            let recv = server.try_recv().unwrap();
            assert_eq!(*recv.data(), i);
            recv.reply(i * 10);
        }

        for i in 10u64..14 {
            caller.call(i).unwrap();
        }

        for i in 10u64..14 {
            server.poll();
            let recv = server.try_recv().unwrap();
            assert_eq!(*recv.data(), i);
            recv.reply(i * 10);
        }

        for _ in 0..8 {
            let _ = caller.try_recv_response().unwrap();
        }
    }

    #[test]
    fn multi_block_sequential() {
        let (mut callers, mut server) = BbqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        for i in 0u64..12 {
            caller.call(i * 7 + 3).unwrap();
        }

        for i in 0u64..12 {
            server.poll();
            let recv = server.try_recv().unwrap();
            assert_eq!(*recv.data(), i * 7 + 3);
            recv.reply(i);
        }

        for i in 0u64..12 {
            let (_, resp) = caller.try_recv_response().unwrap();
            assert_eq!(resp, i);
        }
    }
}
