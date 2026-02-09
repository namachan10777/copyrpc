//! Lock-free MPMC queue using only 64-bit CAS (LPRQ - Linked Portable Relaxed Queue).
//!
//! This is the portable version of LCRQ that avoids CAS2/CMPXCHG16B.
//! Request path: Callers → LPRQ shared queue → Server
//! Response path: Server → per-caller SPSC ResponseRing → Caller

use crate::common::{CachePadded, ResponseRing};
use crate::hazard::HazardDomain;
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscRecvRef, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// Constants
// ============================================================================

const COMMITTED_BIT: u64 = 1;
const CYCLE_SHIFT: u32 = 1;
const UNSAFE_BIT: u64 = 1 << 63;
const CRQ_CLOSED_BIT: u64 = 1 << 63;
const PATIENCE: u32 = 10;
const RECLAIM_THRESHOLD: usize = 64;

// ============================================================================
// State encoding helpers
// ============================================================================

#[inline(always)]
fn make_state(cycle: u64, committed: bool) -> u64 {
    (cycle << CYCLE_SHIFT) | (committed as u64)
}

#[inline(always)]
fn state_cycle(state: u64) -> u64 {
    (state & !UNSAFE_BIT) >> CYCLE_SHIFT
}

#[inline(always)]
fn state_is_committed(state: u64) -> bool {
    state & COMMITTED_BIT != 0
}

#[inline(always)]
fn state_is_unsafe(state: u64) -> bool {
    state & UNSAFE_BIT != 0
}

// ============================================================================
// LPRQ Slot
// ============================================================================

#[repr(C, align(128))]
struct LprqSlot<T: Serial> {
    state: AtomicU64,
    data: UnsafeCell<MaybeUninit<T>>,
    caller_id: UnsafeCell<usize>,
}

unsafe impl<T: Serial> Send for LprqSlot<T> {}
unsafe impl<T: Serial> Sync for LprqSlot<T> {}

impl<T: Serial> LprqSlot<T> {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(make_state(0, false)),
            data: UnsafeCell::new(MaybeUninit::uninit()),
            caller_id: UnsafeCell::new(0),
        }
    }
}

// ============================================================================
// CRQ Node
// ============================================================================

struct CrqNode<T: Serial> {
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    next: CachePadded<AtomicPtr<CrqNode<T>>>,
    ring: Box<[LprqSlot<T>]>,
    ring_size: usize,
    ring_mask: usize,
}

unsafe impl<T: Serial> Send for CrqNode<T> {}
unsafe impl<T: Serial> Sync for CrqNode<T> {}

impl<T: Serial> CrqNode<T> {
    fn new(ring_size: usize) -> Self {
        assert!(ring_size.is_power_of_two());
        let ring = (0..ring_size)
            .map(|_| LprqSlot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            head: CachePadded::new(AtomicU64::new(0)),
            tail: CachePadded::new(AtomicU64::new(0)),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            ring,
            ring_size,
            ring_mask: ring_size - 1,
        }
    }

    /// Try to enqueue into this CRQ node (multi-producer).
    fn enqueue(&self, val: T, caller_id: usize) -> Result<(), (T, usize)> {
        for _ in 0..PATIENCE {
            let t = self.tail.fetch_add(1, Ordering::Relaxed);
            if t & CRQ_CLOSED_BIT != 0 {
                return Err((val, caller_id));
            }

            let cycle = t / self.ring_size as u64;
            let idx = (t as usize) & self.ring_mask;
            let slot = &self.ring[idx];

            let state = slot.state.load(Ordering::Acquire);
            let s_cycle = state_cycle(state);

            // Slot is available if cycle matches or is from previous cycle, and not committed/unsafe
            if s_cycle <= cycle && !state_is_committed(state) && !state_is_unsafe(state) {
                // Try to claim this slot for our cycle
                let new_state = make_state(cycle, false);
                if slot
                    .state
                    .compare_exchange(state, new_state, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    // Claimed! Write data.
                    unsafe {
                        (*slot.data.get()).write(val);
                        *slot.caller_id.get() = caller_id;
                    }
                    // Commit
                    slot.state.store(make_state(cycle, true), Ordering::Release);
                    return Ok(());
                }
            }

            // If s_cycle > cycle or we failed CAS, we lost. Try next slot via next iteration.
        }

        // Close CRQ
        self.tail.fetch_or(CRQ_CLOSED_BIT, Ordering::Release);
        Err((val, caller_id))
    }

    /// Try to dequeue from this CRQ node (single consumer).
    fn dequeue(&self) -> Option<(usize, T)> {
        loop {
            let h = self.head.load(Ordering::Relaxed);
            let t = self.tail.load(Ordering::Relaxed) & !CRQ_CLOSED_BIT;
            if h >= t {
                return None;
            }

            let cycle = h / self.ring_size as u64;
            let idx = (h as usize) & self.ring_mask;
            let slot = &self.ring[idx];
            let state = slot.state.load(Ordering::Acquire);

            if state_cycle(state) == cycle && state_is_committed(state) {
                // Ready to read
                let data = unsafe { (*slot.data.get()).assume_init_read() };
                let caller_id = unsafe { *slot.caller_id.get() };
                // Reset slot for next cycle
                slot.state
                    .store(make_state(cycle + 1, false), Ordering::Release);
                self.head.store(h + 1, Ordering::Release);
                return Some((caller_id, data));
            }

            if state_cycle(state) == cycle && !state_is_committed(state) && !state_is_unsafe(state)
            {
                // Enqueuer claimed but hasn't committed yet. Spin briefly.
                for _ in 0..16 {
                    std::hint::spin_loop();
                    let s = slot.state.load(Ordering::Acquire);
                    if state_cycle(s) == cycle && state_is_committed(s) {
                        // Now ready
                        let data = unsafe { (*slot.data.get()).assume_init_read() };
                        let caller_id = unsafe { *slot.caller_id.get() };
                        slot.state
                            .store(make_state(cycle + 1, false), Ordering::Release);
                        self.head.store(h + 1, Ordering::Release);
                        return Some((caller_id, data));
                    }
                }
                // Still not committed. Mark unsafe and skip.
                let current_state = slot.state.load(Ordering::Acquire);
                if state_cycle(current_state) == cycle && !state_is_committed(current_state) {
                    let _ = slot.state.compare_exchange(
                        current_state,
                        current_state | UNSAFE_BIT,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    );
                }
                self.head.store(h + 1, Ordering::Release);
                continue; // Try next slot
            }

            if state_cycle(state) < cycle {
                // Slot from old cycle, enqueuer never arrived. Advance.
                let _ = slot.state.compare_exchange(
                    state,
                    make_state(cycle + 1, false),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                self.head.store(h + 1, Ordering::Release);
                continue; // Try next slot
            }

            // state_cycle > cycle, already consumed or unsafe - skip
            if state_is_unsafe(state) {
                self.head.store(h + 1, Ordering::Release);
                continue;
            }

            // Something's wrong, bail out
            return None;
        }
    }
}

// ============================================================================
// LinkedQueue (LPRQ)
// ============================================================================

struct LinkedQueue<T: Serial> {
    head: CachePadded<AtomicPtr<CrqNode<T>>>,
    tail: CachePadded<AtomicPtr<CrqNode<T>>>,
    ring_size: usize,
    rx_alive: AtomicBool,
    sender_count: AtomicUsize,
    hazard: Arc<HazardDomain>,
}

unsafe impl<T: Serial> Send for LinkedQueue<T> {}
unsafe impl<T: Serial> Sync for LinkedQueue<T> {}

impl<T: Serial> LinkedQueue<T> {
    fn new(ring_size: usize, num_senders: usize) -> Self {
        assert!(ring_size.is_power_of_two());
        let initial_node = Box::into_raw(Box::new(CrqNode::new(ring_size)));

        Self {
            head: CachePadded::new(AtomicPtr::new(initial_node)),
            tail: CachePadded::new(AtomicPtr::new(initial_node)),
            ring_size,
            rx_alive: AtomicBool::new(true),
            sender_count: AtomicUsize::new(num_senders),
            hazard: Arc::new(HazardDomain::new(num_senders)),
        }
    }

    /// Enqueue (caller side, multi-producer).
    fn enqueue(
        &self,
        hp_slot: usize,
        mut val: T,
        mut caller_id: usize,
    ) -> Result<(), CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(val));
        }

        loop {
            let tail_ptr = self.tail.load(Ordering::Acquire);
            self.hazard.protect(hp_slot, tail_ptr as *mut u8);
            if self.tail.load(Ordering::Acquire) != tail_ptr {
                continue;
            }

            // Help advance if next exists
            let next = unsafe { (*tail_ptr).next.load(Ordering::Acquire) };
            if !next.is_null() {
                let _ =
                    self.tail
                        .compare_exchange(tail_ptr, next, Ordering::AcqRel, Ordering::Relaxed);
                continue;
            }

            // Try to enqueue into current tail
            match unsafe { (*tail_ptr).enqueue(val, caller_id) } {
                Ok(()) => {
                    self.hazard.clear(hp_slot);
                    return Ok(());
                }
                Err((v, c)) => {
                    val = v;
                    caller_id = c;
                    // CRQ closed. Alloc new empty node, try to link.
                    let new = Box::into_raw(Box::new(CrqNode::new(self.ring_size)));
                    let next = unsafe { (*tail_ptr).next.load(Ordering::Acquire) };
                    if !next.is_null() {
                        unsafe { drop(Box::from_raw(new)) };
                        let _ = self.tail.compare_exchange(
                            tail_ptr,
                            next,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        );
                        continue;
                    }
                    if unsafe {
                        (*tail_ptr)
                            .next
                            .compare_exchange(
                                ptr::null_mut(),
                                new,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                    } {
                        let _ = self.tail.compare_exchange(
                            tail_ptr,
                            new,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        );
                    } else {
                        unsafe { drop(Box::from_raw(new)) };
                    }
                    // retry
                }
            }
        }
    }

    /// Dequeue (server, single consumer).
    fn dequeue(&self, retire_list: &mut Vec<*mut CrqNode<T>>) -> Option<(usize, T)> {
        loop {
            let head_ptr = self.head.load(Ordering::Acquire);
            match unsafe { (*head_ptr).dequeue() } {
                Some(result) => return Some(result),
                None => {
                    let next = unsafe { (*head_ptr).next.load(Ordering::Acquire) };
                    if next.is_null() {
                        return None;
                    }
                    // Advance head, retire old
                    self.head.store(next, Ordering::Release);
                    retire_list.push(head_ptr);
                    if retire_list.len() > RECLAIM_THRESHOLD {
                        // Convert to *mut u8 for hazard domain
                        let mut u8_retire_list: Vec<*mut u8> =
                            retire_list.iter().map(|&ptr| ptr as *mut u8).collect();
                        let unprotected = self.hazard.drain_unprotected(&mut u8_retire_list);
                        retire_list.clear();
                        for ptr in u8_retire_list {
                            retire_list.push(ptr as *mut CrqNode<T>);
                        }
                        for ptr in unprotected {
                            unsafe {
                                drop(Box::from_raw(ptr as *mut CrqNode<T>));
                            }
                        }
                    }
                }
            }
        }
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

impl<T: Serial> Drop for LinkedQueue<T> {
    fn drop(&mut self) {
        // Cleanup all nodes
        let mut current = self.head.load(Ordering::Acquire);
        while !current.is_null() {
            unsafe {
                let node = Box::from_raw(current);
                current = node.next.load(Ordering::Acquire);
            }
        }
    }
}

// ============================================================================
// LprqCaller
// ============================================================================

pub struct LprqCaller<Req: Serial, Resp: Serial> {
    caller_id: usize,
    hp_slot: usize,
    queue: Arc<LinkedQueue<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for LprqCaller<Req, Resp> {
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        self.queue.enqueue(self.hp_slot, req, self.caller_id)?;

        let token = self.send_count;
        self.send_count += 1;
        self.inflight += 1;
        Ok(token)
    }

    fn sync(&mut self) {
        // No-op: atomics provide ordering
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

impl<Req: Serial, Resp: Serial> Drop for LprqCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.queue.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// LprqServer
// ============================================================================

pub struct LprqServer<Req: Serial, Resp: Serial> {
    queue: Arc<LinkedQueue<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    recv_counts: Vec<u64>,
    retire_list: Vec<*mut CrqNode<Req>>,
    disconnected: bool,
}

// Safety: LprqServer is only accessed by a single consumer thread.
// The retire_list contains pointers to CrqNode<Req> which are safely deallocated.
unsafe impl<Req: Serial + Send, Resp: Serial + Send> Send for LprqServer<Req, Resp> {}

pub struct LprqRecvRef<'a, Req: Serial, Resp: Serial> {
    server: &'a mut LprqServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> LprqRecvRef<'a, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> MpscRecvRef<Req> for LprqRecvRef<'_, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for LprqServer<Req, Resp> {
    type RecvRef<'a>
        = LprqRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        // For LPRQ, checking availability without consuming is hard.
        // Return 0 and let try_recv do the actual work.
        // This is acceptable as poll is best-effort.
        0
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data) = self.queue.dequeue(&mut self.retire_list)?;
        let slot_token = self.recv_counts[caller_id];
        self.recv_counts[caller_id] += 1;
        Some(LprqRecvRef {
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
                return; // Client disconnected, drop response
            }
            std::hint::spin_loop();
        }
    }
}

impl<Req: Serial, Resp: Serial> Drop for LprqServer<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.queue.disconnect_rx();
            for ring in &self.resp_rings {
                ring.disconnect_tx();
            }
        }
        // Drain retire list
        let mut u8_retire_list: Vec<*mut u8> =
            self.retire_list.iter().map(|&ptr| ptr as *mut u8).collect();
        let unprotected = self.queue.hazard.drain_unprotected(&mut u8_retire_list);
        for ptr in unprotected {
            unsafe {
                drop(Box::from_raw(ptr as *mut CrqNode<Req>));
            }
        }
    }
}

// ============================================================================
// LprqMpsc Factory
// ============================================================================

pub struct LprqMpsc;

impl MpscChannel for LprqMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = LprqCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = LprqServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two());
        assert!(max_callers > 0);

        let queue = Arc::new(LinkedQueue::new(ring_depth, max_callers));
        let resp_rings: Vec<_> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(ring_depth)))
            .collect();

        let callers = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| LprqCaller {
                caller_id: id,
                hp_slot: id,
                queue: Arc::clone(&queue),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = LprqServer {
            queue,
            resp_rings,
            recv_counts: vec![0u64; max_callers],
            retire_list: Vec::new(),
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
        let (mut callers, mut server) = LprqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        // Send request
        let tok = caller.call(42).unwrap();
        assert_eq!(tok, 0);
        assert_eq!(caller.pending_count(), 1);

        // Server receives
        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 42);
        assert_eq!(recv.caller_id(), 0);

        // Server replies
        recv.reply(84);

        // Caller receives
        let (resp_tok, resp) = caller.try_recv_response().unwrap();
        assert_eq!(resp_tok, tok);
        assert_eq!(resp, 84);
        assert_eq!(caller.pending_count(), 0);
    }

    #[test]
    fn multi_caller_concurrent() {
        let (mut callers, mut server) = LprqMpsc::create::<u64, u64>(4, 16, 8);

        // Each caller sends 2 requests
        for (i, caller) in callers.iter_mut().enumerate() {
            let val = (i as u64) * 100;
            caller.call(val).unwrap();
            caller.call(val + 1).unwrap();
        }

        // Server processes all 8 requests
        let mut tokens = Vec::new();
        for _ in 0..8 {
            let recv = server.try_recv().unwrap();
            let caller_id = recv.caller_id();
            let val = *recv.data();
            let token = recv.into_token();
            tokens.push((caller_id, val, token));
        }

        // Reply to all
        for (_caller_id, val, token) in tokens {
            server.reply(token, val * 2);
        }

        // Each caller receives 2 responses
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
        let (mut callers, mut server) = LprqMpsc::create::<u64, u64>(1, 8, 8);
        let caller = &mut callers[0];

        // Fill and drain ring multiple times
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
        let (mut callers, _server) = LprqMpsc::create::<u64, u64>(1, 16, 4);
        let caller = &mut callers[0];

        // Send up to limit
        for i in 0..4 {
            assert!(caller.call(i).is_ok());
        }

        // 5th should fail
        match caller.call(100) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 100),
            _ => panic!("Expected InflightExceeded"),
        }
    }

    #[test]
    fn disconnect_detection() {
        let (mut callers, server) = LprqMpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        // Should detect disconnection
        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = LprqMpsc::create::<u64, u64>(8, 256, 32);

        const MSGS_PER_CALLER: usize = 100;

        // Spawn threads for callers
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
                                // Drain responses then retry
                                while caller.try_recv_response().is_some() {}
                            }
                            Err(_) => {
                                std::thread::yield_now();
                            }
                        }
                    }

                    // Drain remaining responses
                    while caller.pending_count() > 0 {
                        while caller.try_recv_response().is_some() {}
                        std::thread::yield_now();
                    }

                    id
                })
            })
            .collect();

        // Server processes all requests
        let mut processed = 0;
        let expected = 8 * MSGS_PER_CALLER;

        while processed < expected {
            if let Some(recv) = server.try_recv() {
                let val = *recv.data();
                recv.reply(val + 1);
                processed += 1;
            }
        }

        // Wait for all callers to finish
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(processed, expected);
    }

    #[test]
    fn deferred_reply() {
        let (mut callers, mut server) = LprqMpsc::create::<u64, u64>(2, 16, 8);

        // Send from both callers
        callers[0].call(10).unwrap();
        callers[1].call(20).unwrap();

        // Receive and defer replies
        let recv1 = server.try_recv().unwrap();
        let data1 = *recv1.data();
        let tok1 = recv1.into_token();

        let recv2 = server.try_recv().unwrap();
        let data2 = *recv2.data();
        let tok2 = recv2.into_token();

        // Reply in reverse order
        server.reply(tok2, data2 * 2);
        server.reply(tok1, data1 * 2);

        // Both callers receive their responses
        let (_, resp0) = callers[0].try_recv_response().unwrap();
        let (_, resp1) = callers[1].try_recv_response().unwrap();

        assert_eq!(resp0, 20);
        assert_eq!(resp1, 40);
    }

    #[test]
    fn crq_close_and_new_node() {
        // Use small ring to potentially force node transitions with concurrent access
        let (mut callers, mut server) = LprqMpsc::create::<u64, u64>(2, 16, 16);

        // Fill and drain ring multiple times
        for batch in 0u64..3 {
            for i in 0u64..16 {
                let val = batch * 16 + i;
                callers[0].call(val).unwrap();
            }

            // Server drains
            for _ in 0..16 {
                let recv = server.try_recv().unwrap();
                let val = *recv.data();
                recv.reply(val * 2);
            }

            // Client drains responses
            for _ in 0..16 {
                let _ = callers[0].try_recv_response().unwrap();
            }
        }
    }
}
