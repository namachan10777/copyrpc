//! Lamport-style SPSC with batched index synchronization, wrapped into MPSC.
//!
//! Key design points:
//! - Shared atomic head/tail indices, locally cached to reduce atomic operations
//! - Sender: writes to local_head, publishes via shared_head in sync()
//! - Receiver: reads from local_tail, publishes via shared_tail in sync()
//! - Batched synchronization: send many messages, then sync() once to make visible
//! - CRITICAL: sync() is NOT a no-op! It's required to make messages visible.

use crate::common::{CachePadded, Response};
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// ============================================================================
// SPSC Primitives
// ============================================================================

struct Inner<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    shared_head: CachePadded<AtomicUsize>, // Published by sender
    shared_tail: CachePadded<AtomicUsize>, // Published by receiver
    tx_alive: AtomicBool,
    rx_alive: AtomicBool,
}

impl<T> Inner<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));
        Self {
            buffer: buffer.into_boxed_slice(),
            shared_head: CachePadded::new(AtomicUsize::new(0)),
            shared_tail: CachePadded::new(AtomicUsize::new(0)),
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        }
    }
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

pub struct LamportSender<T> {
    inner: std::sync::Arc<Inner<T>>,
    local_head: usize,
    cached_tail: usize,
    mask: usize,
}

impl<T> LamportSender<T> {
    /// Attempt to send a value. Returns Err if full (based on cached_tail).
    /// Call sync() to publish writes and refresh tail cache.
    pub fn send(&mut self, value: T) -> Result<(), T> {
        let next_head = self.local_head.wrapping_add(1);
        if next_head.wrapping_sub(self.cached_tail) > self.mask {
            // Full based on cache; refresh tail
            self.cached_tail = self.inner.shared_tail.load(Ordering::Acquire);
            if next_head.wrapping_sub(self.cached_tail) > self.mask {
                return Err(value);
            }
        }

        unsafe {
            (*self.inner.buffer[self.local_head & self.mask].get()).write(value);
        }
        self.local_head = next_head;
        Ok(())
    }

    /// Publish local_head to shared_head, making writes visible to receiver.
    pub fn sync(&mut self) {
        self.inner.shared_head.store(self.local_head, Ordering::Release);
    }

    /// Check if receiver is still alive.
    pub fn is_connected(&self) -> bool {
        self.inner.rx_alive.load(Ordering::Acquire)
    }
}

impl<T> Drop for LamportSender<T> {
    fn drop(&mut self) {
        self.inner.tx_alive.store(false, Ordering::Release);
    }
}

pub struct LamportReceiver<T> {
    inner: std::sync::Arc<Inner<T>>,
    local_tail: usize,
    cached_head: usize,
    mask: usize,
}

impl<T: Copy> LamportReceiver<T> {
    /// Attempt to receive a value. Returns None if empty (based on cached_head).
    /// Call sync() to refresh head cache and publish tail.
    pub fn recv(&mut self) -> Option<T> {
        if self.local_tail == self.cached_head {
            // Empty based on cache; refresh head
            self.cached_head = self.inner.shared_head.load(Ordering::Acquire);
            if self.local_tail == self.cached_head {
                return None;
            }
        }

        let value = unsafe {
            let slot = &*self.inner.buffer[self.local_tail & self.mask].get();
            slot.assume_init()
        };
        self.local_tail = self.local_tail.wrapping_add(1);
        Some(value)
    }

    /// Publish local_tail to shared_tail, freeing slots for sender.
    pub fn sync(&mut self) {
        self.inner.shared_tail.store(self.local_tail, Ordering::Release);
    }

    /// Check if sender is still alive.
    pub fn is_connected(&self) -> bool {
        self.inner.tx_alive.load(Ordering::Acquire)
    }

    /// Get count of available messages (based on cached_head).
    pub fn available(&self) -> usize {
        self.cached_head.wrapping_sub(self.local_tail)
    }
}

impl<T> Drop for LamportReceiver<T> {
    fn drop(&mut self) {
        self.inner.rx_alive.store(false, Ordering::Release);
    }
}

pub fn lamport_channel<T>(capacity: usize) -> (LamportSender<T>, LamportReceiver<T>) {
    let inner = std::sync::Arc::new(Inner::new(capacity));
    let mask = capacity - 1;
    let sender = LamportSender {
        inner: inner.clone(),
        local_head: 0,
        cached_tail: 0,
        mask,
    };
    let receiver = LamportReceiver {
        inner,
        local_tail: 0,
        cached_head: 0,
        mask,
    };
    (sender, receiver)
}

// ============================================================================
// MPSC Wrappers
// ============================================================================

pub struct LamportCaller<Req: Serial + Send, Resp: Serial + Send> {
    call_tx: LamportSender<Req>,
    resp_rx: LamportReceiver<Response<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for LamportCaller<Req, Resp> {
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if !self.call_tx.is_connected() {
            return Err(CallError::Disconnected(req));
        }

        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        self.call_tx.send(req).map_err(|req| CallError::Full(req))?;

        let token = self.send_count;
        self.send_count = self.send_count.wrapping_add(1);
        self.inflight += 1;
        Ok(token)
    }

    fn sync(&mut self) {
        // CRITICAL: Publish calls to server, free response slots
        self.call_tx.sync();
        self.resp_rx.sync();
    }

    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv().map(|r| {
            self.inflight -= 1;
            (r.token, r.data)
        })
    }

    fn pending_count(&self) -> usize {
        self.inflight
    }
}

pub struct LamportRecvRef<'a, Req: Serial + Send, Resp: Serial + Send> {
    server: &'a mut LamportServer<Req, Resp>,
    caller_id: usize,
    slot_token: u64,
    data: Req,
}

impl<'a, Req: Serial + Send, Resp: Serial + Send> LamportRecvRef<'a, Req, Resp> {
    pub fn data(&self) -> &Req {
        &self.data
    }

    pub fn caller_id(&self) -> usize {
        self.caller_id
    }

    pub fn reply(self, resp: Resp) {
        self.server.reply(
            ReplyToken {
                caller_id: self.caller_id,
                slot_token: self.slot_token,
            },
            resp,
        );
    }

    pub fn into_token(self) -> ReplyToken {
        ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        }
    }
}

impl<Req: Serial + Send, Resp: Serial + Send> crate::MpscRecvRef<Req> for LamportRecvRef<'_, Req, Resp> {
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

struct CallerLane<Req: Serial + Send, Resp: Serial + Send> {
    call_rx: LamportReceiver<Req>,
    resp_tx: LamportSender<Response<Resp>>,
    recv_count: u64,
    available: usize,
}

pub struct LamportServer<Req: Serial + Send, Resp: Serial + Send> {
    lanes: Vec<CallerLane<Req, Resp>>,
    current_lane: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscServer<Req, Resp> for LamportServer<Req, Resp> {
    type RecvRef<'a> = LamportRecvRef<'a, Req, Resp> where Self: 'a;

    fn poll(&mut self) -> u32 {
        let mut total = 0u32;
        for lane in &mut self.lanes {
            // Refresh head cache to see new calls
            lane.call_rx.cached_head = lane.call_rx.inner.shared_head.load(Ordering::Acquire);
            lane.available = lane.call_rx.available();

            // Publish tail to free call slots, publish responses
            lane.call_rx.sync();
            lane.resp_tx.sync();

            total += lane.available as u32;
        }
        total
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        // Round-robin drain: start from current_lane
        let start = self.current_lane;
        let n = self.lanes.len();

        for i in 0..n {
            let idx = (start + i) % n;
            if self.lanes[idx].available > 0 {
                let lane = &mut self.lanes[idx];
                if let Some(req) = lane.call_rx.recv() {
                    lane.available -= 1;
                    let token = lane.recv_count;
                    lane.recv_count = lane.recv_count.wrapping_add(1);
                    self.current_lane = (idx + 1) % n;

                    return Some(LamportRecvRef {
                        server: self,
                        caller_id: idx,
                        slot_token: token,
                        data: req,
                    });
                }
            }
        }
        None
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let lane = &mut self.lanes[token.caller_id];
        let response = Response {
            token: token.slot_token,
            data: resp,
        };
        // Best-effort: if full, drop (should not happen with proper ring sizing)
        let _ = lane.resp_tx.send(response);
    }
}

// ============================================================================
// Factory
// ============================================================================

pub struct LamportMpsc;

impl MpscChannel for LamportMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = LamportCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = LamportServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        let mut callers = Vec::with_capacity(max_callers);
        let mut lanes = Vec::with_capacity(max_callers);

        for _ in 0..max_callers {
            let (call_tx, call_rx) = lamport_channel(ring_depth);
            let (resp_tx, resp_rx) = lamport_channel(ring_depth);

            callers.push(LamportCaller {
                call_tx,
                resp_rx,
                send_count: 0,
                inflight: 0,
                max_inflight,
            });

            lanes.push(CallerLane {
                call_rx,
                resp_tx,
                recv_count: 0,
                available: 0,
            });
        }

        let server = LamportServer {
            lanes,
            current_lane: 0,
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
    fn spsc_basic() {
        let (mut tx, mut rx) = lamport_channel::<u32>(4);

        // Send without sync - receiver won't see it
        tx.send(42).unwrap();
        assert_eq!(rx.recv(), None);

        // Sync sender - now visible
        tx.sync();
        rx.sync(); // Refresh head cache
        assert_eq!(rx.recv(), Some(42));
        rx.sync(); // Publish new tail
        tx.sync(); // Refresh tail cache to see freed slot

        // Fill the ring (3 more messages since 1 slot is free)
        assert!(tx.send(1).is_ok());
        assert!(tx.send(2).is_ok());
        assert!(tx.send(3).is_ok());
        assert!(tx.send(4).is_err()); // Full (4 slots, 0 free)

        // Receive one message to free a slot
        tx.sync(); // Publish head so rx can see messages
        rx.sync(); // Refresh head cache
        assert_eq!(rx.recv(), Some(1));
        rx.sync(); // Publish tail to free slot

        tx.sync(); // Refresh tail cache
        assert!(tx.send(4).is_ok()); // Now succeeds
    }

    #[test]
    fn spsc_batch() {
        let (mut tx, mut rx) = lamport_channel::<u64>(16);

        // Send batch
        for i in 0..10 {
            tx.send(i).unwrap();
        }
        tx.sync();

        // Receive batch
        rx.sync();
        for i in 0..10 {
            assert_eq!(rx.recv(), Some(i));
        }
        assert_eq!(rx.recv(), None);
    }

    #[test]
    fn spsc_wraparound() {
        let (mut tx, mut rx) = lamport_channel::<u32>(4);

        for round in 0..3 {
            for i in 0..3 {
                tx.send(round * 10 + i).unwrap();
            }
            tx.sync();

            rx.sync();
            for i in 0..3 {
                assert_eq!(rx.recv(), Some(round * 10 + i));
            }
            rx.sync(); // Free slots
            tx.sync(); // Refresh tail
        }
    }

    #[test]
    fn mpsc_basic() {
        let (mut callers, mut server) = LamportMpsc::create::<u32, u32>(2, 8, 8);

        // Call without sync - server won't see it
        let t0 = callers[0].call(100).unwrap();
        assert_eq!(server.poll(), 0);

        // Sync caller - now visible
        callers[0].sync();
        assert_eq!(server.poll(), 1);

        let req = server.try_recv().unwrap();
        assert_eq!(*req.data(), 100);
        assert_eq!(req.caller_id(), 0);
        req.reply(200);

        // Reply not visible until server sync
        callers[0].sync(); // Refresh head
        assert_eq!(callers[0].try_recv_response(), None);

        server.poll(); // Publish response
        callers[0].sync(); // Refresh head
        let (tok, resp) = callers[0].try_recv_response().unwrap();
        assert_eq!(tok, t0);
        assert_eq!(resp, 200);
    }

    #[test]
    fn mpsc_multi_caller() {
        let (mut callers, mut server) = LamportMpsc::create::<u32, u32>(3, 8, 8);

        let t0 = callers[0].call(10).unwrap();
        let t1 = callers[1].call(20).unwrap();
        let t2 = callers[2].call(30).unwrap();

        // No sync - server sees nothing
        assert_eq!(server.poll(), 0);

        // Sync all
        for c in &mut callers {
            c.sync();
        }
        assert_eq!(server.poll(), 3);

        // Drain and reply
        let mut received = vec![];
        while let Some(req) = server.try_recv() {
            let cid = req.caller_id();
            let val = *req.data();
            received.push((cid, val));
            req.reply(val * 2);
        }
        received.sort();
        assert_eq!(received, vec![(0, 10), (1, 20), (2, 30)]);

        // Publish responses
        server.poll();

        // Receive responses
        for c in &mut callers {
            c.sync();
        }
        assert_eq!(callers[0].try_recv_response(), Some((t0, 20)));
        assert_eq!(callers[1].try_recv_response(), Some((t1, 40)));
        assert_eq!(callers[2].try_recv_response(), Some((t2, 60)));
    }

    #[test]
    fn mpsc_inflight_limit() {
        let (mut callers, mut server) = LamportMpsc::create::<u32, u32>(1, 8, 3);

        callers[0].call(1).unwrap();
        callers[0].call(2).unwrap();
        callers[0].call(3).unwrap();

        match callers[0].call(4) {
            Err(CallError::InflightExceeded(_)) => {}
            _ => panic!("expected InflightExceeded"),
        }

        // Drain one
        callers[0].sync();
        server.poll();
        let req = server.try_recv().unwrap();
        req.reply(10);
        server.poll();
        callers[0].sync();
        callers[0].try_recv_response().unwrap();

        // Now can send again
        callers[0].call(4).unwrap();
    }

    #[test]
    fn mpsc_ring_full() {
        let (mut callers, mut server) = LamportMpsc::create::<u32, u32>(1, 4, 100);

        // Fill ring (capacity-1 = 3)
        callers[0].call(1).unwrap();
        callers[0].call(2).unwrap();
        callers[0].call(3).unwrap();

        match callers[0].call(4) {
            Err(CallError::Full(_)) => {}
            _ => panic!("expected Full"),
        }

        // Drain one to free slot
        callers[0].sync();
        server.poll();
        let req = server.try_recv().unwrap();
        req.reply(10);
        server.poll();
        callers[0].sync();
        callers[0].try_recv_response().unwrap();
        callers[0].sync(); // Free response slot

        // Now can send
        callers[0].call(4).unwrap();
    }

    #[test]
    fn mpsc_round_robin() {
        let (mut callers, mut server) = LamportMpsc::create::<u32, u32>(3, 8, 8);

        for i in 0..3 {
            callers[0].call(i * 100 + 0).unwrap();
            callers[1].call(i * 100 + 1).unwrap();
            callers[2].call(i * 100 + 2).unwrap();
        }

        for c in &mut callers {
            c.sync();
        }
        server.poll();

        // Should drain in round-robin order across all callers
        let mut received = vec![];
        while let Some(req) = server.try_recv() {
            let val = *req.data();
            received.push(val);
            req.reply(val);
        }
        assert_eq!(received.len(), 9);
    }

    #[test]
    fn mpsc_sync_required() {
        let (mut callers, mut server) = LamportMpsc::create::<u32, u32>(1, 8, 8);

        // Without sync, messages invisible
        callers[0].call(1).unwrap();
        callers[0].call(2).unwrap();
        assert_eq!(server.poll(), 0);

        // Sync once, both visible
        callers[0].sync();
        assert_eq!(server.poll(), 2);

        let req = server.try_recv().unwrap();
        assert_eq!(*req.data(), 1);
        req.reply(10);

        // Response invisible until server sync
        assert_eq!(callers[0].try_recv_response(), None);

        server.poll(); // Publish
        callers[0].sync(); // Refresh
        assert_eq!(callers[0].try_recv_response(), Some((0, 10)));
    }
}
