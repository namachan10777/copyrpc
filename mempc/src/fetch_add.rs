//! Lock-free MPSC using fetch_add (Vyukov's bounded MPMC specialized for single consumer).
//!
//! Design:
//! - Single shared MPSC request ring: all callers → server (Vyukov's bounded MPMC)
//! - Per-caller SPSC response rings: server → each caller (simple valid-flag based)
//!
//! Request ring: Producers claim slots via fetch_add on head, spin until slot is ready,
//! write data, then mark slot as ready. Consumer reads from tail, marks slot as reusable.
//!
//! Response rings: Simple SPSC with valid flag. Server writes (token, response), sets valid.
//! Caller reads when valid, resets flag.

use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
// Request Ring (MPMC, specialized for single consumer)
// ============================================================================

#[repr(C, align(64))]
struct ReqSlot<T> {
    seq: AtomicUsize,
    caller_id: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

struct RequestRing<T> {
    buffer: Box<[ReqSlot<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    mask: usize,
    capacity: usize,
    sender_count: AtomicUsize,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for RequestRing<T> {}
unsafe impl<T: Send> Sync for RequestRing<T> {}

impl<T> RequestRing<T> {
    fn new(capacity: usize, num_senders: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let buffer = (0..capacity)
            .map(|i| ReqSlot {
                seq: AtomicUsize::new(i),
                caller_id: UnsafeCell::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            buffer,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            mask: capacity - 1,
            capacity,
            sender_count: AtomicUsize::new(num_senders),
            rx_alive: AtomicBool::new(true),
        }
    }

    /// Try to push a request. Claims a slot via fetch_add, spins until slot is ready.
    /// If receiver has disconnected, returns Disconnected error.
    fn try_push(&self, caller_id: usize, data: T) -> Result<u64, CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(data));
        }

        // Claim a slot via fetch_add
        let pos = self.head.fetch_add(1, Ordering::Relaxed);
        let idx = pos & self.mask;
        let slot = &self.buffer[idx];

        // Spin until slot is free for this position (seq == pos)
        loop {
            let seq = slot.seq.load(Ordering::Acquire);
            if seq == pos {
                break;
            }
            if !self.rx_alive.load(Ordering::Relaxed) {
                // Receiver gone. We already incremented head but that's OK.
                return Err(CallError::Disconnected(data));
            }
            std::hint::spin_loop();
        }

        // Write caller_id and data
        unsafe {
            *slot.caller_id.get() = caller_id;
            (*slot.data.get()).write(data);
        }

        // Mark slot as ready for consumer (seq = pos + 1)
        slot.seq.store(pos + 1, Ordering::Release);
        Ok(pos as u64)
    }

    /// Try to pop a request (consumer only).
    fn try_pop(&self) -> Option<(usize, T, u64)> {
        let tail = self.tail.load(Ordering::Relaxed);
        let idx = tail & self.mask;
        let slot = &self.buffer[idx];
        let seq = slot.seq.load(Ordering::Acquire);

        if seq != tail + 1 {
            return None;
        }

        // Read caller_id and data
        let caller_id = unsafe { *slot.caller_id.get() };
        let data = unsafe { (*slot.data.get()).assume_init_read() };
        let token = tail as u64;

        // Free slot for next cycle (seq = tail + capacity)
        slot.seq.store(tail + self.capacity, Ordering::Release);
        self.tail.store(tail + 1, Ordering::Release);

        Some((caller_id, data, token))
    }

    /// Count of available items (approximate).
    fn available(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.saturating_sub(tail)
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// Response Ring (SPSC, per-caller)
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

    /// Try to push a response (server side).
    fn try_push(&self, token: u64, data: T) -> bool {
        if !self.rx_alive.load(Ordering::Acquire) {
            return false;
        }

        let pos = self.write_pos.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];

        if slot.valid.load(Ordering::Acquire) {
            // Slot still occupied
            return false;
        }

        // Write data
        unsafe {
            (*slot.data.get()).write((token, data));
        }

        // Mark as valid
        slot.valid.store(true, Ordering::Release);
        self.write_pos.store(pos + 1, Ordering::Release);
        true
    }

    /// Try to pop a response (caller side).
    fn try_pop(&self) -> Option<(u64, T)> {
        let pos = self.read_pos.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];

        if !slot.valid.load(Ordering::Acquire) {
            return None;
        }

        // Read data
        let data = unsafe { (*slot.data.get()).assume_init_read() };

        // Mark as free
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
// FetchAddCaller
// ============================================================================

pub struct FetchAddCaller<Req, Resp> {
    caller_id: usize,
    req_ring: Arc<RequestRing<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for FetchAddCaller<Req, Resp> {
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
        // No-op: fetch_add provides ordering
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

impl<Req, Resp> Drop for FetchAddCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// FetchAddServer
// ============================================================================

pub struct FetchAddServer<Req, Resp> {
    req_ring: Arc<RequestRing<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    disconnected: bool,
}

pub struct FetchAddRecvRef<'a, Req, Resp> {
    server: &'a mut FetchAddServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> FetchAddRecvRef<'a, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for FetchAddRecvRef<'_, Req, Resp> {
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

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for FetchAddServer<Req, Resp> {
    type RecvRef<'a>
        = FetchAddRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        self.req_ring.available() as u32
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data, slot_token) = self.req_ring.try_pop()?;
        Some(FetchAddRecvRef {
            server: self,
            caller_id,
            data,
            slot_token,
        })
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let resp_ring = &self.resp_rings[token.caller_id];
        // Retry on full (shouldn't happen if clients are draining)
        while !resp_ring.try_push(token.slot_token, resp) {
            std::hint::spin_loop();
        }
    }
}

impl<Req, Resp> Drop for FetchAddServer<Req, Resp> {
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
// FetchAddMpsc Factory
// ============================================================================

pub struct FetchAddMpsc;

impl MpscChannel for FetchAddMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = FetchAddCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = FetchAddServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two());
        assert!(max_callers > 0);

        let req_ring = Arc::new(RequestRing::new(ring_depth, max_callers));
        let resp_rings: Vec<_> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(ring_depth)))
            .collect();

        let callers = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| FetchAddCaller {
                caller_id: id,
                req_ring: Arc::clone(&req_ring),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = FetchAddServer {
            req_ring,
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
        let (mut callers, mut server) = FetchAddMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        // Send request
        let tok = caller.call(42).unwrap();
        assert_eq!(tok, 0);
        assert_eq!(caller.pending_count(), 1);

        // Server receives
        assert_eq!(server.poll(), 1);
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
        let (mut callers, mut server) = FetchAddMpsc::create::<u64, u64>(4, 16, 8);

        // Each caller sends 2 requests
        for (i, caller) in callers.iter_mut().enumerate() {
            let val = (i as u64) * 100;
            caller.call(val).unwrap();
            caller.call(val + 1).unwrap();
        }

        // Server processes all 8 requests
        assert!(server.poll() >= 8);

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
        let (mut callers, mut server) = FetchAddMpsc::create::<u64, u64>(1, 8, 8);
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
        let (mut callers, _server) = FetchAddMpsc::create::<u64, u64>(1, 16, 4);
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
    fn zero_copy_recv_ref() {
        let (mut callers, mut server) = FetchAddMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        caller.call(999).unwrap();

        let recv = server.try_recv().unwrap();
        // Access data multiple times via reference
        assert_eq!(*recv.data(), 999);
        assert_eq!(*recv.data(), 999);
        recv.reply(1000);

        let (_, resp) = caller.try_recv_response().unwrap();
        assert_eq!(resp, 1000);
    }

    #[test]
    fn deferred_reply() {
        let (mut callers, mut server) = FetchAddMpsc::create::<u64, u64>(2, 16, 8);

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
    fn full_ring() {
        // With fetch_add design, senders spin when ring is full.
        // Use inflight limit to prevent blocking.
        let (mut callers, _server) = FetchAddMpsc::create::<u64, u64>(1, 4, 4);
        let caller = &mut callers[0];

        // Fill ring up to inflight limit
        for i in 0..4 {
            assert!(caller.call(i).is_ok());
        }

        // Next should fail due to inflight limit
        match caller.call(100) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 100),
            _ => panic!("Expected InflightExceeded"),
        }
    }

    #[test]
    fn disconnect_detection() {
        let (mut callers, server) = FetchAddMpsc::create::<u64, u64>(1, 16, 16);

        drop(server);

        // Should detect disconnection
        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = FetchAddMpsc::create::<u64, u64>(8, 256, 32);

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
}
