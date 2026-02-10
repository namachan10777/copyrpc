//! Jiffy wait-free MPSC queue (DISC 2020, Adas & Friedman).
//!
//! Key design:
//! - Linked list of fixed-size buffer arrays
//! - Enqueue: FAA on global tail → find buffer → write data → set flag (wait-free, no spin)
//! - Dequeue: volatile read of flag → read data (zero atomic operations on consumer)
//! - Pre-allocation: 2nd entry in tail buffer triggers CAS-based next buffer allocation
//! - 3-state slot flags: EMPTY(0), SET(1), HANDLED(2)
//! - HOL blocking on EMPTY: consumer waits for slow producers (required for RPC reliability)

use crate::common::CachePadded;
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// Constants
// ============================================================================

const EMPTY: u8 = 0;
const SET: u8 = 1;
const HANDLED: u8 = 2;

// ============================================================================
// JiffySlot
// ============================================================================

/// A single slot in a Jiffy buffer. Cache-line aligned.
#[repr(C, align(64))]
struct JiffySlot<T> {
    /// 3-state flag: EMPTY/SET/HANDLED
    state: AtomicU8,
    /// Caller ID that wrote this slot (valid when state == SET)
    caller_id: UnsafeCell<u16>,
    /// Per-caller token for response round-trip (valid when state == SET)
    caller_token: UnsafeCell<u64>,
    /// Request data (valid when state == SET)
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> JiffySlot<T> {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            caller_id: UnsafeCell::new(0),
            caller_token: UnsafeCell::new(0),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// ============================================================================
// JiffyBuffer
// ============================================================================

/// A fixed-size buffer node in the Jiffy linked list.
struct JiffyBuffer<T> {
    slots: Box<[JiffySlot<T>]>,
    next: AtomicPtr<JiffyBuffer<T>>,
    start_index: u64,
    capacity: usize,
}

impl<T> JiffyBuffer<T> {
    fn new(start_index: u64, capacity: usize) -> Self {
        let slots: Box<[JiffySlot<T>]> = (0..capacity).map(|_| JiffySlot::new()).collect();
        Self {
            slots,
            next: AtomicPtr::new(ptr::null_mut()),
            start_index,
            capacity,
        }
    }
}

// Safety: JiffyBuffer slots are synchronized through the state flag.
// Only one producer writes each slot (FAA gives unique positions).
// Only the single consumer reads completed slots.
unsafe impl<T: Send> Send for JiffyBuffer<T> {}
unsafe impl<T: Send> Sync for JiffyBuffer<T> {}

// ============================================================================
// JiffyQueue (shared state)
// ============================================================================

struct JiffyQueue<T> {
    /// Global enqueue counter. Producers FAA to claim a position.
    tail: CachePadded<AtomicU64>,
    /// Pointer to the tail buffer (hint for recent allocations).
    tail_buffer: AtomicPtr<JiffyBuffer<T>>,
    /// Pointer to the very first buffer (immutable, used as fallback).
    head_buffer: *mut JiffyBuffer<T>,
    /// Number of live senders.
    sender_count: AtomicUsize,
    /// Consumer alive flag.
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for JiffyQueue<T> {}
unsafe impl<T: Send> Sync for JiffyQueue<T> {}

impl<T> Drop for JiffyQueue<T> {
    fn drop(&mut self) {
        // Free the entire buffer chain starting from head_buffer.
        // This runs when the last Arc reference is dropped (all callers + server gone).
        let mut current = self.head_buffer;
        while !current.is_null() {
            let next = unsafe { (*current).next.load(Ordering::Relaxed) };
            unsafe { drop(Box::from_raw(current)) };
            current = next;
        }
    }
}

impl<T> JiffyQueue<T> {
    /// Find the buffer containing `pos`, creating new buffers as needed.
    fn find_or_create_buffer(&self, pos: u64) -> *mut JiffyBuffer<T> {
        let mut current = self.tail_buffer.load(Ordering::Acquire);
        // If tail_buffer hint is ahead of pos (due to pre-allocation), fall back to head.
        if pos < unsafe { &*current }.start_index {
            current = self.head_buffer;
        }
        loop {
            let buf = unsafe { &*current };
            if pos >= buf.start_index && pos < buf.start_index + buf.capacity as u64 {
                // Advance tail_buffer hint (best-effort, only forward)
                let old_tail = self.tail_buffer.load(Ordering::Relaxed);
                if unsafe { &*old_tail }.start_index < buf.start_index {
                    let _ = self.tail_buffer.compare_exchange(
                        old_tail,
                        current,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                }
                return current;
            }
            // Need the next buffer
            let next = buf.next.load(Ordering::Acquire);
            if !next.is_null() {
                current = next;
                continue;
            }
            // Allocate next buffer
            let new_start = buf.start_index + buf.capacity as u64;
            let new_buf = Box::into_raw(Box::new(JiffyBuffer::new(new_start, buf.capacity)));
            match buf.next.compare_exchange(
                ptr::null_mut(),
                new_buf,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    current = new_buf;
                }
                Err(existing) => {
                    // Another producer created it; drop ours
                    unsafe { drop(Box::from_raw(new_buf)) };
                    current = existing;
                }
            }
        }
    }

    /// Enqueue a request into the Jiffy queue.
    fn enqueue(&self, caller_id: u16, caller_token: u64, data: T) -> Result<(), T> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(data);
        }

        // Claim a global position
        let pos = self.tail.fetch_add(1, Ordering::Relaxed);

        // Find the buffer for this position
        let buf = self.find_or_create_buffer(pos);
        let buf_ref = unsafe { &*buf };
        let slot_idx = (pos - buf_ref.start_index) as usize;
        let slot = &buf_ref.slots[slot_idx];

        // Write data
        unsafe {
            *slot.caller_id.get() = caller_id;
            *slot.caller_token.get() = caller_token;
            (*slot.data.get()).write(data);
        }

        // Publish: EMPTY -> SET
        slot.state.store(SET, Ordering::Release);

        // Pre-allocate: when writing the 2nd slot of the tail buffer,
        // proactively create the next buffer (linked via buf.next only;
        // tail_buffer is NOT advanced here to avoid overshooting).
        if slot_idx == 1 && buf_ref.next.load(Ordering::Relaxed).is_null() {
            let new_start = buf_ref.start_index + buf_ref.capacity as u64;
            let new_buf = Box::into_raw(Box::new(JiffyBuffer::new(new_start, buf_ref.capacity)));
            if buf_ref
                .next
                .compare_exchange(
                    ptr::null_mut(),
                    new_buf,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                unsafe { drop(Box::from_raw(new_buf)) };
            }
        }

        Ok(())
    }
}

// ============================================================================
// JiffyConsumer (single-threaded consumer state)
// ============================================================================

struct JiffyConsumer<T> {
    read_pos: u64,
    head_buffer: *mut JiffyBuffer<T>,
    buffer_capacity: usize,
}

// Safety: JiffyConsumer is only used by the single server thread.
unsafe impl<T: Send> Send for JiffyConsumer<T> {}

impl<T: Copy> JiffyConsumer<T> {
    /// Try to dequeue one item. Returns (caller_id, data, caller_token).
    fn try_dequeue(&mut self) -> Option<(u16, T, u64)> {
        let buf = unsafe { &*self.head_buffer };
        let local_idx = (self.read_pos - buf.start_index) as usize;

        // Advance to next buffer if current is exhausted
        if local_idx >= self.buffer_capacity {
            let next = buf.next.load(Ordering::Acquire);
            if next.is_null() {
                return None;
            }
            self.head_buffer = next;
            return self.try_dequeue();
        }

        let slot = &buf.slots[local_idx];
        let state = slot.state.load(Ordering::Acquire);

        if state == SET {
            let caller_id = unsafe { *slot.caller_id.get() };
            let caller_token = unsafe { *slot.caller_token.get() };
            let data = unsafe { (*slot.data.get()).assume_init_read() };
            slot.state.store(HANDLED, Ordering::Relaxed);
            self.read_pos += 1;
            return Some((caller_id, data, caller_token));
        }

        // EMPTY: producer hasn't written yet (HOL blocking)
        None
    }

    /// Count consecutive available (SET) items from read_pos.
    fn count_available(&self) -> u32 {
        let mut count = 0u32;
        let mut pos = self.read_pos;
        let mut buf_ptr = self.head_buffer;

        loop {
            let buf = unsafe { &*buf_ptr };
            let local_idx = (pos - buf.start_index) as usize;

            if local_idx >= self.buffer_capacity {
                let next = buf.next.load(Ordering::Acquire);
                if next.is_null() {
                    break;
                }
                buf_ptr = next;
                continue;
            }

            let slot = &buf.slots[local_idx];
            if slot.state.load(Ordering::Acquire) != SET {
                break;
            }

            count += 1;
            pos += 1;
        }

        count
    }
}

// ============================================================================
// Response Ring (per-caller SPSC, copied from fetch_add pattern)
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
            return true; // Receiver disconnected: discard response
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
// JiffyCaller
// ============================================================================

pub struct JiffyCaller<Req, Resp> {
    caller_id: u16,
    queue: Arc<JiffyQueue<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for JiffyCaller<Req, Resp> {
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        match self.queue.enqueue(self.caller_id, self.send_count, req) {
            Ok(()) => {
                let token = self.send_count;
                self.send_count += 1;
                self.inflight += 1;
                Ok(token)
            }
            Err(data) => Err(CallError::Disconnected(data)),
        }
    }

    fn sync(&mut self) {
        // No-op: Jiffy has immediate visibility via atomic Release on slot state.
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

impl<Req, Resp> Drop for JiffyCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.queue.sender_count.fetch_sub(1, Ordering::AcqRel);
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// JiffyServer
// ============================================================================

pub struct JiffyServer<Req, Resp> {
    queue: Arc<JiffyQueue<Req>>,
    consumer: JiffyConsumer<Req>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    disconnected: bool,
}

// ============================================================================
// JiffyRecvRef
// ============================================================================

pub struct JiffyRecvRef<'a, Req, Resp> {
    server: &'a mut JiffyServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial + Send, Resp: Serial + Send> JiffyRecvRef<'a, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for JiffyRecvRef<'_, Req, Resp> {
    #[inline]
    fn caller_id(&self) -> usize {
        self.caller_id
    }

    #[inline]
    fn data(&self) -> Req {
        self.data
    }

    #[inline]
    fn into_token(self) -> ReplyToken {
        ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        }
    }
}

// ============================================================================
// MpscServer impl
// ============================================================================

impl<Req: Serial + Send, Resp: Serial + Send> MpscServer<Req, Resp> for JiffyServer<Req, Resp> {
    type RecvRef<'a>
        = JiffyRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        self.consumer.count_available()
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data, caller_token) = self.consumer.try_dequeue()?;
        Some(JiffyRecvRef {
            server: self,
            caller_id: caller_id as usize,
            data,
            slot_token: caller_token,
        })
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let resp_ring = &self.resp_rings[token.caller_id];
        while !resp_ring.try_push(token.slot_token, resp) {
            std::hint::spin_loop();
        }
    }
}

impl<Req, Resp> Drop for JiffyServer<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.queue.rx_alive.store(false, Ordering::Release);
            for ring in &self.resp_rings {
                ring.disconnect_tx();
            }
            // Buffer chain is freed by JiffyQueue::Drop when the last Arc is dropped.
        }
    }
}

// ============================================================================
// JiffyMpsc Factory
// ============================================================================

pub struct JiffyMpsc;

impl MpscChannel for JiffyMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = JiffyCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = JiffyServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(
            ring_depth >= 4,
            "ring_depth must be >= 4 for pre-allocation"
        );
        assert!(
            max_callers > 0 && max_callers <= u16::MAX as usize,
            "max_callers must fit in u16"
        );

        // Response ring capacity: next power of 2 >= ring_depth
        let resp_capacity = ring_depth.next_power_of_two();

        // Allocate initial Jiffy buffer
        let initial = Box::into_raw(Box::new(JiffyBuffer::new(0, ring_depth)));

        let queue = Arc::new(JiffyQueue {
            tail: CachePadded::new(AtomicU64::new(0)),
            tail_buffer: AtomicPtr::new(initial),
            head_buffer: initial,
            sender_count: AtomicUsize::new(max_callers),
            rx_alive: AtomicBool::new(true),
        });

        let resp_rings: Vec<Arc<ResponseRing<Resp>>> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(resp_capacity)))
            .collect();

        let callers: Vec<JiffyCaller<Req, Resp>> = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| JiffyCaller {
                caller_id: id as u16,
                queue: Arc::clone(&queue),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let consumer = JiffyConsumer {
            read_pos: 0,
            head_buffer: initial,
            buffer_capacity: ring_depth,
        };

        let server = JiffyServer {
            queue,
            consumer,
            resp_rings,
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
        let (mut callers, mut server) = JiffyMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        let tok = caller.call(42).unwrap();
        assert_eq!(tok, 0);
        assert_eq!(caller.pending_count(), 1);

        assert_eq!(server.poll(), 1);
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
    fn multi_caller() {
        let (mut callers, mut server) = JiffyMpsc::create::<u64, u64>(4, 16, 8);

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
    fn buffer_transition() {
        let (mut callers, mut server) = JiffyMpsc::create::<u64, u64>(1, 8, 8);
        let caller = &mut callers[0];

        for round in 0u64..3 {
            for i in 0u64..8 {
                let val = round * 100 + i;
                caller.call(val).unwrap();
            }

            for _ in 0..8 {
                assert!(server.poll() >= 1);
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
        let (mut callers, _server) = JiffyMpsc::create::<u64, u64>(1, 16, 4);
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
    fn deferred_reply() {
        let (mut callers, mut server) = JiffyMpsc::create::<u64, u64>(2, 16, 8);

        callers[0].call(10).unwrap();
        callers[1].call(20).unwrap();

        assert!(server.poll() >= 2);

        let recv1 = server.try_recv().unwrap();
        let data1 = *recv1.data();
        let tok1 = recv1.into_token();

        let recv2 = server.try_recv().unwrap();
        let data2 = *recv2.data();
        let tok2 = recv2.into_token();

        // Reply in reverse order
        server.reply(tok2, data2 * 2);
        server.reply(tok1, data1 * 2);

        let (_, resp0) = callers[0].try_recv_response().unwrap();
        let (_, resp1) = callers[1].try_recv_response().unwrap();

        assert_eq!(resp0, 20);
        assert_eq!(resp1, 40);
    }

    #[test]
    fn disconnect_detection() {
        let (mut callers, server) = JiffyMpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn many_buffer_transitions() {
        let (mut callers, mut server) = JiffyMpsc::create::<u64, u64>(1, 4, 4);
        let caller = &mut callers[0];

        for round in 0u64..20 {
            for i in 0u64..4 {
                caller.call(round * 100 + i).unwrap();
            }

            for _ in 0..4 {
                server.poll();
                let recv = server.try_recv().unwrap();
                let val = *recv.data();
                recv.reply(val + 1);
            }

            for _ in 0..4 {
                let (_, _resp) = caller.try_recv_response().unwrap();
            }
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = JiffyMpsc::create::<u64, u64>(8, 256, 32);

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
                            Ok(_) => sent += 1,
                            Err(CallError::InflightExceeded(_)) => {
                                while caller.try_recv_response().is_some() {}
                            }
                            Err(_) => std::thread::yield_now(),
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
    fn sustained_high_concurrency() {
        // Mimics server_client benchmark pattern: 5 clients sending continuously
        use std::sync::atomic::{AtomicBool, Ordering as AO};
        use std::sync::{Arc, Barrier};

        let num_clients = 5;
        let (callers, mut server) = JiffyMpsc::create::<u64, u64>(num_clients, 1024, 256);
        let barrier = Arc::new(Barrier::new(num_clients + 2));
        let stop = Arc::new(AtomicBool::new(false));

        // Server thread
        let server_barrier = Arc::clone(&barrier);
        let server_stop = Arc::clone(&stop);
        let server_handle = std::thread::spawn(move || {
            server_barrier.wait();
            loop {
                server.poll();
                loop {
                    let Some(recv) = server.try_recv() else { break };
                    recv.reply(42);
                }
                if server_stop.load(AO::Relaxed) {
                    break;
                }
            }
        });

        // Client threads
        let handles: Vec<_> = callers
            .into_iter()
            .map(|mut caller| {
                let b = Arc::clone(&barrier);
                let s = Arc::clone(&stop);
                std::thread::spawn(move || {
                    b.wait();
                    let mut completed = 0u64;
                    let mut iter = 0u32;
                    loop {
                        for _ in 0..32 {
                            if caller.pending_count() < 256 {
                                let _ = caller.call(42);
                            }
                        }
                        caller.sync();
                        while let Some(_) = caller.try_recv_response() {
                            completed += 1;
                        }
                        iter = iter.wrapping_add(1);
                        if iter & 0x3F == 0 && s.load(AO::Relaxed) {
                            break;
                        }
                    }
                    completed
                })
            })
            .collect();

        barrier.wait();
        std::thread::sleep(std::time::Duration::from_millis(500));
        stop.store(true, AO::Release);

        for h in handles {
            h.join().unwrap();
        }
        server_handle.join().unwrap();
    }

    #[test]
    fn token_round_trip() {
        let (mut callers, mut server) = JiffyMpsc::create::<u64, u64>(2, 16, 8);

        // Caller 0 sends 3 requests
        let t0 = callers[0].call(100).unwrap();
        let t1 = callers[0].call(200).unwrap();
        let t2 = callers[0].call(300).unwrap();
        assert_eq!((t0, t1, t2), (0, 1, 2));

        // Caller 1 sends 1 request
        let t3 = callers[1].call(999).unwrap();
        assert_eq!(t3, 0); // Per-caller counter

        server.poll();

        // Process all 4
        for _ in 0..4 {
            let recv = server.try_recv().unwrap();
            let data = *recv.data();
            recv.reply(data + 1);
        }

        // Caller 0: tokens 0, 1, 2
        let (tok, resp) = callers[0].try_recv_response().unwrap();
        assert_eq!((tok, resp), (0, 101));
        let (tok, resp) = callers[0].try_recv_response().unwrap();
        assert_eq!((tok, resp), (1, 201));
        let (tok, resp) = callers[0].try_recv_response().unwrap();
        assert_eq!((tok, resp), (2, 301));

        // Caller 1: token 0
        let (tok, resp) = callers[1].try_recv_response().unwrap();
        assert_eq!((tok, resp), (0, 1000));
    }
}
