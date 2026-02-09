//! Producer-only write SPSC ring with generation counters, wrapped into MPSC.
//!
//! Key design:
//! - SPSC ring uses generation counters per slot (no shared atomics)
//! - Producer writes data + generation to same cache line
//! - Consumer is read-only (never writes back to channel)
//! - CLDEMOTE on first send after sync for cross-core latency reduction
//! - Inflight management prevents producer overwrite
//! - Response channel uses same SPSC ring with Response<T> wrapper

use crate::common::{DisconnectState, Response, cldemote};
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken, Serial};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ============================================================================
// Slot (public for SHM usage)
// ============================================================================

/// SPSC ring slot with generation counter. Cache-line aligned.
///
/// Used by `RawSender`/`RawReceiver` for the onesided protocol.
/// Can be placed in shared memory for IPC usage.
#[repr(C, align(64))]
pub struct Slot<T> {
    generation: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

// Safety: Only one thread writes (via RawSender), one reads (via RawReceiver)
unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

impl<T: Serial> Slot<T> {
    /// Initialize a slot in raw memory (sets generation = `usize::MAX`).
    ///
    /// # Safety
    /// `ptr` must point to a properly aligned, valid-for-write `Slot<T>`.
    #[inline]
    pub unsafe fn init(ptr: *mut Self) {
        unsafe {
            (*ptr).generation.get().write(usize::MAX);
            // data is left uninitialized (MaybeUninit)
        }
    }
}

// ============================================================================
// Raw SPSC primitives (no ownership, for SHM)
// ============================================================================

/// Raw SPSC sender for externally-managed memory (e.g. shared memory).
///
/// Operates on a raw `*mut Slot<T>` array without ownership or disconnect tracking.
/// The caller is responsible for memory lifetime, alignment, and slot initialization.
pub struct RawSender<T: Serial> {
    slots: *mut Slot<T>,
    mask: usize,
    head: usize,
    sends_since_publish: usize,
}

// Safety: RawSender only writes to its owned half of the ring
unsafe impl<T: Serial + Send> Send for RawSender<T> {}

impl<T: Serial> RawSender<T> {
    /// Create a new raw sender.
    ///
    /// # Safety
    /// - `slots` must point to `capacity` properly initialized `Slot<T>`s
    ///   (each with `generation = usize::MAX`)
    /// - `capacity` must be a power of 2
    /// - No other sender may operate on the same slots concurrently
    #[inline]
    pub unsafe fn new(slots: *mut Slot<T>, capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        Self {
            slots,
            mask: capacity - 1,
            head: 0,
            sends_since_publish: 0,
        }
    }

    /// Try to send a value. Returns the slot index on success.
    ///
    /// Always succeeds (no backpressure check). The caller must ensure
    /// the slot is not in use (via inflight management).
    #[inline]
    pub fn try_send(&mut self, value: T) -> Option<usize> {
        let slot_idx = self.head & self.mask;
        let slot = unsafe { &*self.slots.add(slot_idx) };

        // Write data first
        unsafe {
            (*slot.data.get()).write(value);
        }

        // Release fence: ensure data write completes before generation is visible
        std::sync::atomic::compiler_fence(Ordering::Release);

        // Write generation = head. Receiver checks generation == tail.
        unsafe {
            std::ptr::write_volatile(slot.generation.get(), self.head);
        }

        // CLDEMOTE on first send after publish
        if self.sends_since_publish == 0 {
            cldemote(slot.generation.get() as *const u8);
        }
        self.sends_since_publish += 1;

        let result = slot_idx;
        self.head = self.head.wrapping_add(1);
        Some(result)
    }

    /// Reset CLDEMOTE gate. Call after a batch of sends.
    #[inline]
    pub fn publish(&mut self) {
        self.sends_since_publish = 0;
    }

    /// Returns the current head position (for external inflight tracking).
    #[inline]
    pub fn head(&self) -> usize {
        self.head
    }
}

/// Raw SPSC receiver for externally-managed memory (e.g. shared memory).
///
/// Operates on a raw `*const Slot<T>` array without ownership or disconnect tracking.
/// The consumer is read-only (never writes back to the ring).
pub struct RawReceiver<T: Serial> {
    slots: *const Slot<T>,
    mask: usize,
    tail: usize,
}

// Safety: RawReceiver only reads from slots
unsafe impl<T: Serial + Send> Send for RawReceiver<T> {}

impl<T: Serial> RawReceiver<T> {
    /// Create a new raw receiver.
    ///
    /// # Safety
    /// - `slots` must point to `capacity` properly initialized `Slot<T>`s
    /// - `capacity` must be a power of 2
    /// - No other receiver may operate on the same slots concurrently
    #[inline]
    pub unsafe fn new(slots: *const Slot<T>, capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two());
        Self {
            slots,
            mask: capacity - 1,
            tail: 0,
        }
    }

    /// Try to receive a value. Returns `(slot_idx, value)` on success.
    ///
    /// Protocol: checks `generation == self.tail`. Consumer is read-only.
    #[inline]
    pub fn try_recv(&mut self) -> Option<(usize, T)> {
        let slot_idx = self.tail & self.mask;
        let slot = unsafe { &*self.slots.add(slot_idx) };

        let generation = unsafe { std::ptr::read_volatile(slot.generation.get()) };
        if generation != self.tail {
            return None;
        }

        // Acquire fence after reading generation
        std::sync::atomic::compiler_fence(Ordering::Acquire);

        let value = unsafe { std::ptr::read((*slot.data.get()).as_ptr()) };
        self.tail = self.tail.wrapping_add(1);
        Some((slot_idx, value))
    }

    /// Peek at available count (best-effort: 0 or 1).
    #[inline]
    pub fn available(&self) -> usize {
        let slot_idx = self.tail & self.mask;
        let slot = unsafe { &*self.slots.add(slot_idx) };
        let generation = unsafe { std::ptr::read_volatile(slot.generation.get()) };
        if generation == self.tail { 1 } else { 0 }
    }

    /// Count consecutive available slots from current tail, up to `max`.
    /// Does not advance the tail pointer.
    #[inline]
    pub fn count_available(&self, max: u32) -> u32 {
        let mut count = 0u32;
        let mut pos = self.tail;
        while count < max {
            let slot_idx = pos & self.mask;
            let slot = unsafe { &*self.slots.add(slot_idx) };
            let generation = unsafe { std::ptr::read_volatile(slot.generation.get()) };
            if generation != pos {
                break;
            }
            count += 1;
            pos = pos.wrapping_add(1);
        }
        count
    }
}

// ============================================================================
// Arc-backed SPSC (for intra-process usage)
// ============================================================================

struct Inner<T> {
    buffer: Box<[Slot<T>]>,
    disconnect: DisconnectState,
}

// Safety: Only one thread writes to each Slot via Sender, only one thread reads via Receiver
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

pub struct Sender<T: Serial> {
    inner: Arc<Inner<T>>,
    raw: RawSender<T>,
}

pub struct Receiver<T: Serial> {
    inner: Arc<Inner<T>>,
    raw: RawReceiver<T>,
}

impl<T: Serial> Sender<T> {
    /// Try to send a value. Returns the slot index on success.
    /// Returns `None` if receiver has been dropped.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Option<usize> {
        if !self.inner.disconnect.rx_alive.load(Ordering::Relaxed) {
            return None;
        }
        self.raw.try_send(value)
    }

    /// Reset CLDEMOTE gate.
    #[inline]
    pub fn publish(&mut self) {
        self.raw.publish();
    }

    /// Check if receiver is alive.
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner.disconnect.rx_alive.load(Ordering::Relaxed)
    }
}

impl<T: Serial> Receiver<T> {
    /// Try to receive a value. Returns (slot_idx, value) on success.
    #[inline]
    pub fn try_recv(&mut self) -> Option<(usize, T)> {
        self.raw.try_recv()
    }

    /// Peek at available count (best-effort: 0 or 1).
    #[inline]
    pub fn available(&self) -> usize {
        self.raw.available()
    }

    /// Check if sender is alive.
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner.disconnect.tx_alive.load(Ordering::Relaxed)
    }
}

impl<T: Serial> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.disconnect.tx_alive.store(false, Ordering::Release);
    }
}

impl<T: Serial> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.disconnect.rx_alive.store(false, Ordering::Release);
    }
}

fn create_spsc<T: Serial>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity.is_power_of_two(), "capacity must be power of 2");

    // Initial generation is usize::MAX so that no slot appears valid
    let buffer: Box<[Slot<T>]> = (0..capacity)
        .map(|_| Slot {
            generation: UnsafeCell::new(usize::MAX),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect();

    let inner = Arc::new(Inner {
        buffer,
        disconnect: DisconnectState {
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        },
    });

    // Safety: buffer lives as long as Arc<Inner>, capacity matches buffer length
    let raw_sender = unsafe {
        RawSender::new(
            inner.buffer.as_ptr() as *mut Slot<T>,
            capacity,
        )
    };
    let raw_receiver = unsafe {
        RawReceiver::new(
            inner.buffer.as_ptr(),
            capacity,
        )
    };

    let sender = Sender {
        inner: Arc::clone(&inner),
        raw: raw_sender,
    };

    let receiver = Receiver {
        inner,
        raw: raw_receiver,
    };

    (sender, receiver)
}

// ============================================================================
// MPSC wrappers
// ============================================================================

/// MPSC caller handle using onesided SPSC rings.
pub struct OnesidedCaller<Req: Serial, Resp: Serial> {
    call_tx: Sender<Req>,
    resp_rx: Receiver<Response<Resp>>,
    inflight: usize,
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for OnesidedCaller<Req, Resp> {
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if !self.call_tx.is_connected() {
            return Err(CallError::Disconnected(req));
        }

        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        match self.call_tx.try_send(req) {
            Some(slot_idx) => {
                self.inflight += 1;
                Ok(slot_idx as u64)
            }
            None => Err(CallError::Full(req)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        self.call_tx.publish();
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        let (_, resp) = self.resp_rx.try_recv()?;
        self.inflight -= 1;
        Some((resp.token, resp.data))
    }

    #[inline]
    fn pending_count(&self) -> usize {
        self.inflight
    }
}

/// MPSC server that manages N callers.
pub struct OnesidedServer<Req: Serial, Resp: Serial> {
    callers: Vec<CallerState<Req, Resp>>,
    scan_pos: usize,
}

struct CallerState<Req: Serial, Resp: Serial> {
    call_rx: Receiver<Req>,
    resp_tx: Sender<Response<Resp>>,
}

/// Zero-copy receive handle.
pub struct OnesidedRecvRef<'a, Req: Serial, Resp: Serial> {
    server: &'a mut OnesidedServer<Req, Resp>,
    caller_id: usize,
    slot_idx: usize,
    data: Req,
}

impl<'a, Req: Serial, Resp: Serial> OnesidedRecvRef<'a, Req, Resp> {
    /// Get a reference to the request data.
    #[inline]
    pub fn data(&self) -> &Req {
        &self.data
    }

    /// Get the caller ID.
    #[inline]
    pub fn caller_id(&self) -> usize {
        self.caller_id
    }

    /// Send a reply and consume the handle.
    #[inline]
    pub fn reply(self, resp: Resp) {
        let state = &mut self.server.callers[self.caller_id];
        let response = Response {
            token: self.slot_idx as u64,
            data: resp,
        };
        let _ = state.resp_tx.try_send(response);
    }

    /// Convert into a token for deferred reply.
    #[inline]
    pub fn into_token(self) -> ReplyToken {
        ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_idx as u64,
        }
    }
}

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for OnesidedRecvRef<'_, Req, Resp> {
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
            slot_token: self.slot_idx as u64,
        }
    }
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscServer<Req, Resp> for OnesidedServer<Req, Resp> {
    type RecvRef<'a> = OnesidedRecvRef<'a, Req, Resp> where Self: 'a;

    #[inline]
    fn poll(&mut self) -> u32 {
        let mut total = 0u32;
        for state in self.callers.iter() {
            total += state.call_rx.available() as u32;
        }
        total
    }

    #[inline]
    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let n = self.callers.len();

        for _ in 0..n {
            let caller_id = self.scan_pos % n;
            self.scan_pos += 1;

            let state = &mut self.callers[caller_id];
            if let Some((slot_idx, data)) = state.call_rx.try_recv() {
                return Some(OnesidedRecvRef {
                    server: self,
                    caller_id,
                    slot_idx,
                    data,
                });
            }
        }

        None
    }

    #[inline]
    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let state = &mut self.callers[token.caller_id];
        let response = Response {
            token: token.slot_token,
            data: resp,
        };
        let _ = state.resp_tx.try_send(response);
    }
}

/// Factory for onesided MPSC channels.
pub struct OnesidedMpsc;

impl MpscChannel for OnesidedMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = OnesidedCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = OnesidedServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two(), "ring_depth must be power of 2");

        let mut callers = Vec::with_capacity(max_callers);
        let mut server_states = Vec::with_capacity(max_callers);

        for _ in 0..max_callers {
            let (call_tx, call_rx) = create_spsc(ring_depth);
            let (resp_tx, resp_rx) = create_spsc(ring_depth);

            callers.push(OnesidedCaller {
                call_tx,
                resp_rx,
                inflight: 0,
                max_inflight,
            });

            server_states.push(CallerState { call_rx, resp_tx });
        }

        let server = OnesidedServer {
            callers: server_states,
            scan_pos: 0,
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

    // --- Raw SPSC tests ---

    #[test]
    fn raw_spsc_basic_send_recv() {
        let buffer: Box<[Slot<u64>]> = (0..4)
            .map(|_| Slot {
                generation: UnsafeCell::new(usize::MAX),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        let ptr = buffer.as_ptr() as *mut Slot<u64>;
        std::mem::forget(buffer); // prevent drop, we manage manually

        let mut tx = unsafe { RawSender::new(ptr, 4) };
        let mut rx = unsafe { RawReceiver::new(ptr, 4) };

        assert_eq!(tx.try_send(42), Some(0));
        assert_eq!(tx.try_send(43), Some(1));

        assert_eq!(rx.try_recv(), Some((0, 42)));
        assert_eq!(rx.try_recv(), Some((1, 43)));
        assert_eq!(rx.try_recv(), None);

        // Clean up
        unsafe { drop(Box::from_raw(std::slice::from_raw_parts_mut(ptr, 4))); }
    }

    #[test]
    fn raw_spsc_wraparound() {
        let buffer: Box<[Slot<u64>]> = (0..4)
            .map(|_| Slot {
                generation: UnsafeCell::new(usize::MAX),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        let ptr = buffer.as_ptr() as *mut Slot<u64>;
        std::mem::forget(buffer);

        let mut tx = unsafe { RawSender::new(ptr, 4) };
        let mut rx = unsafe { RawReceiver::new(ptr, 4) };

        for round in 0u64..3 {
            for i in 0u64..4 {
                let val = round * 100 + i;
                let slot_idx = (round * 4 + i) as usize & 3;
                assert_eq!(tx.try_send(val), Some(slot_idx));
            }
            for i in 0u64..4 {
                let val = round * 100 + i;
                let slot_idx = (round * 4 + i) as usize & 3;
                assert_eq!(rx.try_recv(), Some((slot_idx, val)));
            }
            assert_eq!(rx.try_recv(), None);
        }

        unsafe { drop(Box::from_raw(std::slice::from_raw_parts_mut(ptr, 4))); }
    }

    #[test]
    fn raw_spsc_count_available() {
        let buffer: Box<[Slot<u64>]> = (0..8)
            .map(|_| Slot {
                generation: UnsafeCell::new(usize::MAX),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        let ptr = buffer.as_ptr() as *mut Slot<u64>;
        std::mem::forget(buffer);

        let mut tx = unsafe { RawSender::new(ptr, 8) };
        let rx = unsafe { RawReceiver::new(ptr, 8) };

        assert_eq!(rx.count_available(8), 0);

        tx.try_send(10);
        tx.try_send(20);
        tx.try_send(30);

        assert_eq!(rx.count_available(8), 3);
        assert_eq!(rx.count_available(2), 2); // capped at max

        unsafe { drop(Box::from_raw(std::slice::from_raw_parts_mut(ptr, 8))); }
    }

    #[test]
    fn raw_spsc_slot_init() {
        // Test Slot::init on raw memory
        let layout = std::alloc::Layout::new::<Slot<u64>>();
        let ptr = unsafe { std::alloc::alloc(layout) as *mut Slot<u64> };
        assert!(!ptr.is_null());

        unsafe { Slot::<u64>::init(ptr); }

        // Verify generation is usize::MAX
        let generation = unsafe { std::ptr::read_volatile((*ptr).generation.get()) };
        assert_eq!(generation, usize::MAX);

        unsafe { std::alloc::dealloc(ptr as *mut u8, layout); }
    }

    // --- Arc-backed SPSC tests ---

    #[test]
    fn spsc_basic_send_recv() {
        let (mut tx, mut rx) = create_spsc::<u64>(4);

        assert_eq!(tx.try_send(42), Some(0));
        assert_eq!(tx.try_send(43), Some(1));

        assert_eq!(rx.try_recv(), Some((0, 42)));
        assert_eq!(rx.try_recv(), Some((1, 43)));
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn spsc_wraparound() {
        let (mut tx, mut rx) = create_spsc::<u64>(4);

        for round in 0u64..3 {
            for i in 0u64..4 {
                let val = round * 100 + i;
                let slot_idx = (round * 4 + i) as usize & 3;
                assert_eq!(tx.try_send(val), Some(slot_idx));
            }

            for i in 0u64..4 {
                let val = round * 100 + i;
                let slot_idx = (round * 4 + i) as usize & 3;
                assert_eq!(rx.try_recv(), Some((slot_idx, val)));
            }
            assert_eq!(rx.try_recv(), None);
        }
    }

    #[test]
    fn spsc_cldemote_gate() {
        let (mut tx, _rx) = create_spsc::<u64>(4);

        // First send after construction triggers CLDEMOTE
        assert_eq!(tx.raw.sends_since_publish, 0);
        tx.try_send(1);
        assert_eq!(tx.raw.sends_since_publish, 1);

        // Subsequent sends don't
        tx.try_send(2);
        assert_eq!(tx.raw.sends_since_publish, 2);

        // publish() resets gate
        tx.publish();
        assert_eq!(tx.raw.sends_since_publish, 0);
    }

    #[test]
    fn spsc_disconnect_tx_drop() {
        let (tx, mut rx) = create_spsc::<u64>(4);
        assert!(rx.is_connected());
        drop(tx);
        assert!(!rx.is_connected());
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn spsc_disconnect_rx_drop() {
        let (mut tx, rx) = create_spsc::<u64>(4);
        assert!(tx.is_connected());
        drop(rx);
        assert!(!tx.is_connected());
        assert_eq!(tx.try_send(42), None);
    }

    // --- MPSC tests ---

    #[test]
    fn mpsc_basic_call_reply() {
        let (mut callers, mut server) = OnesidedMpsc::create::<u64, u64>(2, 4, 4);

        // Caller 0 sends
        let token0 = callers[0].call(100).unwrap();
        callers[0].sync();

        // Server polls and receives
        assert_eq!(server.poll(), 1);
        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 100);
        assert_eq!(recv.caller_id(), 0);
        recv.reply(200);

        // Caller 0 receives response
        let (resp_token, resp) = callers[0].try_recv_response().unwrap();
        assert_eq!(resp_token, token0);
        assert_eq!(resp, 200);
    }

    #[test]
    fn mpsc_multi_caller() {
        let (mut callers, mut server) = OnesidedMpsc::create::<u64, u64>(3, 4, 4);

        // All callers send
        let t0 = callers[0].call(10).unwrap();
        let t1 = callers[1].call(20).unwrap();
        let t2 = callers[2].call(30).unwrap();

        for c in &mut callers {
            c.sync();
        }

        // Server processes all
        assert_eq!(server.poll(), 3);

        let mut received = vec![];
        for _ in 0..3 {
            let recv = server.try_recv().unwrap();
            let data = *recv.data();
            let cid = recv.caller_id();
            received.push((data, cid));
            recv.reply(data * 2);
        }

        received.sort();
        assert_eq!(received, vec![(10, 0), (20, 1), (30, 2)]);

        // Callers receive responses
        assert_eq!(callers[0].try_recv_response(), Some((t0, 20)));
        assert_eq!(callers[1].try_recv_response(), Some((t1, 40)));
        assert_eq!(callers[2].try_recv_response(), Some((t2, 60)));
    }

    #[test]
    fn mpsc_inflight_limit() {
        let (mut callers, _server) = OnesidedMpsc::create::<u64, u64>(1, 8, 3);

        let c = &mut callers[0];

        // Fill up to limit
        assert!(c.call(1).is_ok());
        assert!(c.call(2).is_ok());
        assert!(c.call(3).is_ok());
        assert_eq!(c.pending_count(), 3);

        // Exceeded
        match c.call(4) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 4),
            _ => panic!("expected InflightExceeded"),
        }
    }

    #[test]
    fn mpsc_zero_copy_recv() {
        let (mut callers, mut server) = OnesidedMpsc::create::<[u64; 8], u64>(1, 4, 4);

        let data = [1, 2, 3, 4, 5, 6, 7, 8];
        callers[0].call(data).unwrap();
        callers[0].sync();

        server.poll();
        let recv = server.try_recv().unwrap();

        // Zero-copy read
        assert_eq!(recv.data(), &data);
        recv.reply(99);
    }

    #[test]
    fn mpsc_deferred_reply() {
        let (mut callers, mut server) = OnesidedMpsc::create::<u64, u64>(2, 4, 4);

        callers[0].call(100).unwrap();
        callers[1].call(200).unwrap();

        for c in &mut callers {
            c.sync();
        }

        server.poll();

        // Collect tokens without replying
        let recv0 = server.try_recv().unwrap();
        let token0 = recv0.into_token();

        let recv1 = server.try_recv().unwrap();
        let token1 = recv1.into_token();

        // Deferred replies
        server.reply(token1, 2000);
        server.reply(token0, 1000);

        // Responses arrive (order may vary based on scan_pos)
        let mut responses = vec![
            callers[0].try_recv_response().unwrap().1,
            callers[1].try_recv_response().unwrap().1,
        ];
        responses.sort();
        assert_eq!(responses, vec![1000, 2000]);
    }

    #[test]
    fn mpsc_ring_wraparound() {
        let (mut callers, mut server) = OnesidedMpsc::create::<u64, u64>(1, 4, 8);

        let c = &mut callers[0];

        // Send and receive across multiple wrap cycles
        for round in 0u64..3 {
            for i in 0u64..4 {
                let val = round * 100 + i;
                c.call(val).unwrap();
            }
            c.sync();

            // poll() returns best-effort count (may undercount for onesided)
            assert!(server.poll() >= 1);

            for _ in 0..4 {
                // try_recv should successfully read all 4
                let recv = server.try_recv().unwrap();
                let data = *recv.data();
                recv.reply(data + 1000);
            }

            for _ in 0..4 {
                let (_, resp) = c.try_recv_response().unwrap();
                assert!(resp >= 1000);
            }
        }
    }

    #[test]
    fn mpsc_lane_drain_fairness() {
        let (mut callers, mut server) = OnesidedMpsc::create::<u64, u64>(3, 8, 8);

        // Caller 0 sends 4, caller 1 sends 2, caller 2 sends 1
        for _ in 0..4 { callers[0].call(0).unwrap(); }
        for _ in 0..2 { callers[1].call(1).unwrap(); }
        callers[2].call(2).unwrap();

        for c in &mut callers {
            c.sync();
        }

        // poll() returns best-effort count (onesided peeks 1 slot per caller)
        assert!(server.poll() >= 1);

        // Round-robin should visit all callers
        let mut caller_order = vec![];
        for _ in 0..7 {
            let recv = server.try_recv().unwrap();
            caller_order.push(recv.caller_id());
            recv.reply(0);
        }

        // Should have visited all 3 callers
        let mut unique: Vec<_> = caller_order.iter().copied().collect();
        unique.sort();
        unique.dedup();
        assert_eq!(unique, vec![0, 1, 2]);
    }
}
