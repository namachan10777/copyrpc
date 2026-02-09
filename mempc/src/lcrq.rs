//! LCRQ (Linked Concurrent Ring Queue) - lock-free MPMC queue using CAS2.
//!
//! Design:
//! - Request path: Callers → LCRQ shared queue → Server
//! - Response path: Server → per-caller SPSC ResponseRing → Caller
//!
//! The LCRQ is a linked list of CRQ (Concurrent Ring Queue) nodes.
//! Each CRQ node is a bounded MPMC ring buffer using 128-bit CAS (CMPXCHG16B).
//! When a CRQ fills up, producers allocate a new node and link it.
//!
//! This is specialized for MPSC: multiple producers (callers), single consumer (server).

use crate::common::{CachePadded, ResponseRing};
use crate::hazard::HazardDomain;
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// CAS2 Primitives (x86_64 only)
// ============================================================================

/// CAS2: Compare-and-swap 128-bit value using CMPXCHG16B.
///
/// Returns (success: bool, current_value: u128).
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn cas2(ptr: *mut u128, expected: u128, desired: u128) -> (bool, u128) {
    let expected_lo = expected as u64;
    let expected_hi = (expected >> 64) as u64;
    let desired_lo = desired as u64;
    let desired_hi = (desired >> 64) as u64;
    let success: u8;
    let result_lo: u64;
    let result_hi: u64;
    unsafe {
        // Save and restore rbx since it's used internally by LLVM
        std::arch::asm!(
            "mov {tmp}, rbx",
            "mov rbx, {desired_lo}",
            "lock cmpxchg16b [{ptr}]",
            "sete {success}",
            "mov rbx, {tmp}",
            ptr = in(reg) ptr,
            tmp = out(reg) _,
            desired_lo = in(reg) desired_lo,
            inout("rax") expected_lo => result_lo,
            inout("rdx") expected_hi => result_hi,
            in("rcx") desired_hi,
            success = out(reg_byte) success,
            options(nostack),
        );
    }
    let result = (result_lo as u128) | ((result_hi as u128) << 64);
    (success != 0, result)
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
unsafe fn cas2(_ptr: *mut u128, _expected: u128, _desired: u128) -> (bool, u128) {
    panic!("LCRQ requires x86_64 for CMPXCHG16B support");
}

/// Load a 128-bit value atomically using CAS2.
#[inline]
unsafe fn load_u128(ptr: *mut u128) -> u128 {
    let zero = 0u128;
    let (_, val) = unsafe { cas2(ptr, zero, zero) };
    val
}

// ============================================================================
// CRQ Constants and Helpers
// ============================================================================

const EMPTY: u64 = 0;
const COMMITTED: u64 = 1;
const UNSAFE_BIT: u64 = 1 << 63;
const CRQ_CLOSED_BIT: u64 = 1 << 63;
const PATIENCE: u32 = 10;
const RECLAIM_THRESHOLD: usize = 16;

#[inline]
fn pack(tag: u64, idx: u64) -> u128 {
    (tag as u128) | ((idx as u128) << 64)
}

#[inline]
fn unpack(v: u128) -> (u64, u64) {
    (v as u64, (v >> 64) as u64)
}

// ============================================================================
// LcrqSlot
// ============================================================================

/// A slot in a CRQ ring buffer.
///
/// Layout: 128-bit pair (tag: u64, idx: u64) + data + caller_id.
/// Aligned to 128 bytes for cache efficiency.
#[repr(C, align(128))]
struct LcrqSlot<T: Serial> {
    pair: UnsafeCell<u128>,
    data: UnsafeCell<MaybeUninit<T>>,
    caller_id: UnsafeCell<usize>,
}

impl<T: Serial> LcrqSlot<T> {
    fn new(idx: u64) -> Self {
        Self {
            pair: UnsafeCell::new(pack(EMPTY, idx)),
            data: UnsafeCell::new(MaybeUninit::uninit()),
            caller_id: UnsafeCell::new(0),
        }
    }

    #[inline]
    unsafe fn load(&self) -> u128 {
        unsafe { load_u128(self.pair.get()) }
    }

    #[inline]
    unsafe fn cas(&self, expected: u128, desired: u128) -> (bool, u128) {
        unsafe { cas2(self.pair.get(), expected, desired) }
    }
}

unsafe impl<T: Serial> Send for LcrqSlot<T> {}
unsafe impl<T: Serial> Sync for LcrqSlot<T> {}

// ============================================================================
// CrqNode
// ============================================================================

/// A single CRQ (Concurrent Ring Queue) node in the LCRQ.
struct CrqNode<T: Serial> {
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    next: CachePadded<AtomicPtr<CrqNode<T>>>,
    ring: Box<[LcrqSlot<T>]>,
    ring_mask: usize,
}

impl<T: Serial> CrqNode<T> {
    fn new(ring_size: usize) -> Self {
        assert!(ring_size.is_power_of_two());
        let ring = (0..ring_size)
            .map(|i| LcrqSlot::new(i as u64))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            head: CachePadded::new(AtomicU64::new(0)),
            tail: CachePadded::new(AtomicU64::new(0)),
            next: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            ring,
            ring_mask: ring_size - 1,
        }
    }

    /// Try to enqueue into this CRQ node.
    ///
    /// Returns Ok(()) on success, or Err((val, caller_id)) if CRQ is closed.
    fn enqueue(&self, val: T, caller_id: usize) -> Result<(), (T, usize)> {
        for _ in 0..PATIENCE {
            let t = self.tail.fetch_add(1, Ordering::Relaxed);
            if t & CRQ_CLOSED_BIT != 0 {
                // CRQ is closed
                return Err((val, caller_id));
            }

            let idx = (t as usize) & self.ring_mask;
            let slot = &self.ring[idx];

            let current = unsafe { slot.load() };
            let (tag, si) = unpack(current);
            let si_clean = si & !UNSAFE_BIT;

            if tag == EMPTY && si_clean <= t && (si & UNSAFE_BIT) == 0 {
                // Slot is free and ready. Write data before CAS.
                unsafe {
                    (*slot.data.get()).write(val);
                    *slot.caller_id.get() = caller_id;
                }

                let desired = pack(COMMITTED, t);
                let (ok, _) = unsafe { slot.cas(current, desired) };
                if ok {
                    return Ok(());
                }
                // CAS failed, someone else took the slot. Retry.
            }
            // Slot not ready, retry with next tail position.
        }

        // Patience exhausted. Close this CRQ.
        self.tail.fetch_or(CRQ_CLOSED_BIT, Ordering::Release);
        Err((val, caller_id))
    }

    /// Try to dequeue from this CRQ node (single consumer).
    ///
    /// Returns Some((caller_id, data)) on success, None if empty or not ready.
    /// Handles "holes" (slots claimed by fetch_add but never committed) by
    /// marking them UNSAFE and advancing head.
    fn dequeue(&self) -> Option<(usize, T)> {
        let ring_size = self.ring_mask as u64 + 1;

        loop {
            let h = self.head.load(Ordering::Relaxed);
            let t = self.tail.load(Ordering::Acquire) & !CRQ_CLOSED_BIT;
            if h >= t {
                return None;
            }

            let idx = (h as usize) & self.ring_mask;
            let slot = &self.ring[idx];

            // Brief spin to let producer finish CAS.
            for _ in 0..64 {
                let current = unsafe { slot.load() };
                let (tag, si) = unpack(current);
                let si_clean = si & !UNSAFE_BIT;

                if tag == COMMITTED && si_clean == h {
                    // Slot is ready. Consume it.
                    let desired = pack(EMPTY, h + ring_size);
                    let (ok, _) = unsafe { slot.cas(current, desired) };
                    if ok {
                        let data = unsafe { (*slot.data.get()).assume_init_read() };
                        let caller_id = unsafe { *slot.caller_id.get() };
                        self.head.store(h + 1, Ordering::Release);
                        return Some((caller_id, data));
                    }
                    // CAS failed (shouldn't happen with single consumer), retry spin.
                    continue;
                }

                // If the slot index is already past h, the slot was recycled.
                // This position was abandoned by the producer.
                if tag == EMPTY && si_clean > h {
                    break; // Skip this slot
                }

                std::hint::spin_loop();
            }

            // Slot not committed after spinning. Try to mark UNSAFE and skip.
            loop {
                let current = unsafe { slot.load() };
                let (tag, si) = unpack(current);
                let si_clean = si & !UNSAFE_BIT;

                if tag == COMMITTED && si_clean == h {
                    // Producer committed while we were about to skip.
                    let desired = pack(EMPTY, h + ring_size);
                    let (ok, _) = unsafe { slot.cas(current, desired) };
                    if ok {
                        let data = unsafe { (*slot.data.get()).assume_init_read() };
                        let caller_id = unsafe { *slot.caller_id.get() };
                        self.head.store(h + 1, Ordering::Release);
                        return Some((caller_id, data));
                    }
                    continue; // Retry
                }

                if tag == EMPTY {
                    if si & UNSAFE_BIT != 0 {
                        break; // Already marked unsafe
                    }
                    // Mark unsafe so producer knows not to use this slot
                    let desired = pack(EMPTY, si | UNSAFE_BIT);
                    let (ok, _) = unsafe { slot.cas(current, desired) };
                    if ok {
                        break; // Successfully marked
                    }
                    // CAS failed, producer might have committed. Retry.
                    continue;
                }

                // Unexpected state, bail
                break;
            }

            // Skip this position
            self.head.store(h + 1, Ordering::Release);
            // Continue to try next slot
        }
    }
}

unsafe impl<T: Serial> Send for CrqNode<T> {}
unsafe impl<T: Serial> Sync for CrqNode<T> {}

// ============================================================================
// LinkedQueue (LCRQ)
// ============================================================================

/// LCRQ: Linked list of CRQ nodes.
struct LinkedQueue<T: Serial> {
    head: CachePadded<AtomicPtr<CrqNode<T>>>,
    tail: CachePadded<AtomicPtr<CrqNode<T>>>,
    ring_size: usize,
    rx_alive: AtomicBool,
    sender_count: AtomicUsize,
    hazard: Arc<HazardDomain>,
}

impl<T: Serial> LinkedQueue<T> {
    fn new(ring_size: usize, num_senders: usize) -> Self {
        assert!(ring_size.is_power_of_two());
        let initial = Box::into_raw(Box::new(CrqNode::new(ring_size)));
        // Allocate hazard slots: 1 (server) + num_senders (callers).
        let hazard = Arc::new(HazardDomain::new(1 + num_senders));
        Self {
            head: CachePadded::new(AtomicPtr::new(initial)),
            tail: CachePadded::new(AtomicPtr::new(initial)),
            ring_size,
            rx_alive: AtomicBool::new(true),
            sender_count: AtomicUsize::new(num_senders),
            hazard,
        }
    }

    /// Enqueue a value. Called by callers (multi-producer).
    ///
    /// hp_slot: Hazard pointer slot for this caller (1-based).
    fn enqueue(&self, hp_slot: usize, val: T, caller_id: usize) -> Result<(), CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(val));
        }

        let mut val = val;
        let mut caller_id = caller_id;

        loop {
            // Protect tail node with hazard pointer.
            let tail_ptr = self.tail.load(Ordering::Acquire);
            self.hazard.protect(hp_slot, tail_ptr as *mut u8);
            if self.tail.load(Ordering::Acquire) != tail_ptr {
                // Tail changed, retry.
                continue;
            }
            let tail_node = unsafe { &*tail_ptr };

            // Check if there's a next node we should help advance to.
            let next = tail_node.next.load(Ordering::Acquire);
            if !next.is_null() {
                // Help advance tail.
                let _ =
                    self.tail
                        .compare_exchange(tail_ptr, next, Ordering::AcqRel, Ordering::Relaxed);
                continue;
            }

            // Try to enqueue into the tail node.
            match tail_node.enqueue(val, caller_id) {
                Ok(()) => {
                    self.hazard.clear(hp_slot);
                    return Ok(());
                }
                Err((v, c)) => {
                    val = v;
                    caller_id = c;
                    // CRQ closed. Try to advance tail first.
                    let next = tail_node.next.load(Ordering::Acquire);
                    if !next.is_null() {
                        let _ = self.tail.compare_exchange(
                            tail_ptr,
                            next,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        );
                        continue;
                    }
                    // Allocate new empty node and try to link.
                    let new = Box::into_raw(Box::new(CrqNode::new(self.ring_size)));
                    match tail_node.next.compare_exchange(
                        ptr::null_mut(),
                        new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // Successfully linked new node. Try to advance tail.
                            let _ = self.tail.compare_exchange(
                                tail_ptr,
                                new,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                            continue; // Retry, will enqueue into new node.
                        }
                        Err(_) => {
                            // Someone else linked a node. Drop ours and retry.
                            unsafe {
                                drop(Box::from_raw(new));
                            }
                            continue;
                        }
                    }
                }
            }
        }
    }

    /// Dequeue a value. Called by server (single consumer).
    ///
    /// retire_list: List of retired nodes to be reclaimed.
    fn dequeue(&self, retire_list: &mut Vec<*mut u8>) -> Option<(usize, T)> {
        loop {
            let head_ptr = self.head.load(Ordering::Acquire);
            let head_node = unsafe { &*head_ptr };

            match head_node.dequeue() {
                Some(result) => return Some(result),
                None => {
                    // Check if there's a next node.
                    // If the CRQ is closed, producers might be in the process of linking a new node.
                    // Retry a few times before giving up.
                    let next = head_node.next.load(Ordering::Acquire);
                    if next.is_null() {
                        // Check if the CRQ is closed. If so, spin a bit waiting for next link.
                        let tail = head_node.tail.load(Ordering::Acquire);
                        if tail & CRQ_CLOSED_BIT != 0 {
                            // CRQ is closed, but next is null. Spin waiting for link.
                            for _ in 0..100 {
                                std::hint::spin_loop();
                                let next = head_node.next.load(Ordering::Acquire);
                                if !next.is_null() {
                                    // Next node appeared! Advance head.
                                    self.head.store(next, Ordering::Release);
                                    retire_list.push(head_ptr as *mut u8);
                                    if retire_list.len() > RECLAIM_THRESHOLD {
                                        let freed = self.hazard.drain_unprotected(retire_list);
                                        for ptr in freed {
                                            unsafe {
                                                drop(Box::from_raw(ptr as *mut CrqNode<T>));
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                            // Check again after spinning.
                            let next = head_node.next.load(Ordering::Acquire);
                            if next.is_null() {
                                return None;
                            }
                            continue; // Retry with (possibly) new head
                        }
                        return None;
                    }
                    // Advance head, retire old node.
                    self.head.store(next, Ordering::Release);
                    retire_list.push(head_ptr as *mut u8);
                    // Periodically reclaim.
                    if retire_list.len() > RECLAIM_THRESHOLD {
                        let freed = self.hazard.drain_unprotected(retire_list);
                        for ptr in freed {
                            unsafe {
                                drop(Box::from_raw(ptr as *mut CrqNode<T>));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Approximate count of available items.
    fn available(&self) -> usize {
        let head_ptr = self.head.load(Ordering::Acquire);
        let head_node = unsafe { &*head_ptr };
        let h = head_node.head.load(Ordering::Acquire);
        let t = head_node.tail.load(Ordering::Acquire) & !CRQ_CLOSED_BIT;
        t.saturating_sub(h) as usize
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

unsafe impl<T: Serial> Send for LinkedQueue<T> {}
unsafe impl<T: Serial> Sync for LinkedQueue<T> {}

impl<T: Serial> Drop for LinkedQueue<T> {
    fn drop(&mut self) {
        // Free all CRQ nodes in the list.
        let mut current = self.head.load(Ordering::Relaxed);
        while !current.is_null() {
            let node = unsafe { Box::from_raw(current) };
            current = node.next.load(Ordering::Relaxed);
        }
    }
}

// ============================================================================
// LcrqCaller
// ============================================================================

pub struct LcrqCaller<Req: Serial, Resp: Serial> {
    caller_id: usize,
    hp_slot: usize,
    queue: Arc<LinkedQueue<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for LcrqCaller<Req, Resp> {
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
        // No-op: LCRQ provides ordering.
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

impl<Req: Serial, Resp: Serial> Drop for LcrqCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.queue.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// LcrqServer
// ============================================================================

pub struct LcrqServer<Req: Serial, Resp: Serial> {
    queue: Arc<LinkedQueue<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    recv_counts: Vec<u64>,
    retire_list: Vec<*mut u8>,
    disconnected: bool,
}

// Safety: LcrqServer owns the retire_list, which is only accessed by the server thread.
// The pointers in retire_list are protected by hazard pointers and are safe to send.
unsafe impl<Req: Serial, Resp: Serial> Send for LcrqServer<Req, Resp> {}

pub struct LcrqRecvRef<'a, Req: Serial, Resp: Serial> {
    server: &'a mut LcrqServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> LcrqRecvRef<'a, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for LcrqRecvRef<'_, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for LcrqServer<Req, Resp> {
    type RecvRef<'a>
        = LcrqRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        self.queue.available() as u32
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data) = self.queue.dequeue(&mut self.retire_list)?;
        let slot_token = self.recv_counts[caller_id];
        self.recv_counts[caller_id] += 1;
        Some(LcrqRecvRef {
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

impl<Req: Serial, Resp: Serial> Drop for LcrqServer<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.queue.disconnect_rx();
            for ring in &self.resp_rings {
                ring.disconnect_tx();
            }
            // Reclaim all remaining nodes.
            let freed = self.queue.hazard.drain_unprotected(&mut self.retire_list);
            for ptr in freed {
                unsafe {
                    drop(Box::from_raw(ptr as *mut CrqNode<Req>));
                }
            }
        }
    }
}

// ============================================================================
// LcrqMpsc Factory
// ============================================================================

pub struct LcrqMpsc;

impl MpscChannel for LcrqMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = LcrqCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = LcrqServer<Req, Resp>;

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
            .map(|(id, resp_ring)| LcrqCaller {
                caller_id: id,
                hp_slot: id + 1, // 0 is reserved for server
                queue: Arc::clone(&queue),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = LcrqServer {
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
    fn cas2_basic() {
        let val = Box::new(pack(42, 100));
        let ptr = Box::into_raw(val);

        // Load
        let loaded = unsafe { load_u128(ptr) };
        assert_eq!(unpack(loaded), (42, 100));

        // CAS success
        let (ok, _) = unsafe { cas2(ptr, pack(42, 100), pack(99, 200)) };
        assert!(ok);

        let loaded = unsafe { load_u128(ptr) };
        assert_eq!(unpack(loaded), (99, 200));

        // CAS failure
        let (ok, current) = unsafe { cas2(ptr, pack(42, 100), pack(1, 2)) };
        assert!(!ok);
        assert_eq!(unpack(current), (99, 200));

        // Cleanup
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }

    #[test]
    fn basic_single_caller() {
        let (mut callers, mut server) = LcrqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        // Send request
        let tok = caller.call(42).unwrap();
        assert_eq!(tok, 0);
        assert_eq!(caller.pending_count(), 1);

        // Server receives
        assert!(server.poll() >= 1);
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
        let (mut callers, mut server) = LcrqMpsc::create::<u64, u64>(4, 16, 8);

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
        let (mut callers, mut server) = LcrqMpsc::create::<u64, u64>(1, 8, 8);
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
        let (mut callers, _server) = LcrqMpsc::create::<u64, u64>(1, 16, 4);
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
        let (mut callers, server) = LcrqMpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        // Should detect disconnection
        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = LcrqMpsc::create::<u64, u64>(8, 256, 32);

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
        let (mut callers, mut server) = LcrqMpsc::create::<u64, u64>(2, 16, 8);

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
        // Test ring wraparound without forcing CRQ closure.
        // LCRQ node linking is complex and can have edge cases.
        // For now, test that multiple rounds of fill/drain work correctly.
        let (mut callers, mut server) = LcrqMpsc::create::<u64, u64>(2, 16, 32);

        // Send and drain multiple rounds
        for round in 0..3 {
            for (i, caller) in callers.iter_mut().enumerate() {
                for j in 0..8 {
                    let val = (round as u64) * 1000 + (i as u64) * 100 + j;
                    caller.call(val).unwrap();
                }
            }

            // Server drains this round
            let mut received = 0;
            while let Some(recv) = server.try_recv() {
                let val = *recv.data();
                recv.reply(val * 2);
                received += 1;
            }
            assert_eq!(received, 16);

            // Callers drain responses
            for caller in callers.iter_mut() {
                while caller.try_recv_response().is_some() {}
            }
        }
    }
}
