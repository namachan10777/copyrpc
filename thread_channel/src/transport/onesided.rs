//! Onesided (producer-only write) transport implementation.
//!
//! This transport uses a producer-only write SPSC channel for both request
//! and response channels, achieving producer-only write semantics for all operations.
//!
//! Unlike Lamport, the consumer never writes back to the channel. Instead of
//! shared_tail (consumer → producer), inflight management ensures the producer
//! never overtakes the consumer. The producer publishes `shared_head` via sync()
//! so the consumer knows how far it can read.
//!
//! Token: uses monotonically increasing head counter masked to slot index.
//!
//! Store/Load characteristics:
//! - call(): producer writes data to buffer (local), advances local head
//! - sync(): producer publishes shared_head (single atomic store)
//! - recv(): consumer reads shared_head (atomic load), reads data from buffer
//! - reply(): producer writes data to buffer (local), advances local head

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::serial::Serial;

use super::common::{CachePadded, DisconnectState};
use super::{cldemote, Response, Transport, TransportEndpoint, TransportError};

// ============================================================================
// SPSC Implementation (Producer-only write, index-based)
// ============================================================================

/// Error returned when sending fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel is full")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}


/// Slot with embedded generation counter for adaptive visibility.
///
/// At low QD, the consumer checks `generation == tail` for immediate
/// detection without shared_head bouncing. At high QD, the consumer
/// uses cached_head from shared_head and ignores generation.
#[repr(C)]
struct Slot<T> {
    generation: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            generation: UnsafeCell::new(usize::MAX),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

/// Inner channel state.
struct Inner<T> {
    shared: DisconnectState,
    /// Producer head published via sync() (only producer writes)
    shared_head: CachePadded<AtomicUsize>,
    /// Ring buffer with embedded generation counters
    slots: Box<[Slot<T>]>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// The sending half of a producer-only write SPSC channel.
///
/// Data is written locally; `publish()` makes it visible to the receiver.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    /// Local head index (monotonically increasing, wraps via mask)
    head: usize,
    /// Bitmask for fast modulo
    mask: usize,
    /// Number of sends since last publish (0 = next send writes generation)
    sends_since_publish: usize,
}

unsafe impl<T: Send> Send for Sender<T> {}

/// The receiving half of a producer-only write SPSC channel.
///
/// This receiver is READ-ONLY on shared state - it never writes to the channel!
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    /// Local tail index (monotonically increasing, wraps via mask)
    tail: usize,
    /// Cached head from producer (reduces atomic reads)
    cached_head: usize,
    /// Bitmask for fast modulo
    mask: usize,
}

unsafe impl<T: Send> Send for Receiver<T> {}

/// Creates a new producer-only write SPSC channel.
///
/// The capacity will be rounded up to the next power of 2.
///
/// # Panics
/// Panics if `capacity` is 0.
pub fn channel<T: Serial>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "channel capacity must be greater than 0");

    let cap = capacity.next_power_of_two();
    let mask = cap - 1;

    let slots: Box<[Slot<T>]> = (0..cap).map(|_| Slot::new()).collect();

    let inner = Arc::new(Inner {
        shared: DisconnectState {
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        },
        shared_head: CachePadded::new(AtomicUsize::new(0)),
        slots,
    });

    let sender = Sender {
        inner: Arc::clone(&inner),
        head: 0,
        mask,
        sends_since_publish: 0,
    };

    let receiver = Receiver {
        inner,
        tail: 0,
        cached_head: 0,
        mask,
    };

    (sender, receiver)
}

impl<T: Serial> Sender<T> {
    /// Sends a value through the channel.
    ///
    /// Returns `Ok(slot_idx)` on success. The data is NOT visible to the
    /// receiver until `publish()` is called.
    ///
    /// # Safety
    /// Caller must ensure inflight count does not exceed capacity.
    #[inline]
    pub fn send(&mut self, value: T) -> Result<usize, SendError<T>> {
        let slot_idx = self.head & self.mask;
        let slot = &self.inner.slots[slot_idx];

        // Write the value
        unsafe {
            ptr::write((*slot.data.get()).as_mut_ptr(), value);
        }

        // Ensure data write completes before generation/head is advanced
        compiler_fence(Ordering::Release);

        if self.sends_since_publish == 0 {
            // First send after publish: write generation for immediate consumer visibility.
            // Generation and data are in the same Slot (same cache line), so the
            // consumer can detect + read in a single cache line transfer.
            unsafe {
                ptr::write_volatile(slot.generation.get(), self.head);
            }
            cldemote(slot as *const Slot<T> as *const u8);
        }

        self.head = self.head.wrapping_add(1);
        self.sends_since_publish += 1;

        Ok(slot_idx)
    }

    /// Sends a value through the channel without returning slot index.
    #[inline]
    pub fn send_no_idx(&mut self, value: T) -> Result<(), SendError<T>> {
        self.send(value)?;
        Ok(())
    }

    /// Publishes the current head, making all sent data visible to the receiver.
    #[inline]
    pub fn publish(&mut self) {
        if self.sends_since_publish > 0 {
            self.inner
                .shared_head
                .store(self.head, Ordering::Release);
            self.sends_since_publish = 0;
        }
    }

    /// Returns true if the receiver has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        !self.inner.shared.rx_alive.load(Ordering::Relaxed)
    }

    /// Returns the capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.shared.tx_alive.store(false, Ordering::Release);
    }
}

impl<T: Serial> Receiver<T> {
    /// Receives a value from the channel if available.
    ///
    /// Returns `Some((slot_idx, value))` if data is available.
    ///
    /// **This method is READ-ONLY on shared state** - it only reads shared_head!
    #[inline]
    pub fn recv(&mut self) -> Option<(usize, T)> {
        let slot_idx = self.tail & self.mask;
        self.recv_inner().map(|value| (slot_idx, value))
    }

    /// Receives a value from the channel without returning slot index.
    #[inline]
    pub fn recv_no_idx(&mut self) -> Option<T> {
        self.recv_inner()
    }

    /// Internal receive implementation (3-stage adaptive).
    ///
    /// 1. Fast path: tail < cached_head → data read only (high QD steady state)
    /// 2. Immediate path: generation check → distributed per-slot access (low QD)
    /// 3. Batch path: shared_head atomic load → batch discovery (high QD transition)
    #[inline]
    fn recv_inner(&mut self) -> Option<T> {
        // Fast path: cached_head tells us data is available
        if self.tail < self.cached_head {
            let slot_idx = self.tail & self.mask;
            let value = unsafe { ptr::read((*self.inner.slots[slot_idx].data.get()).as_ptr()) };
            self.tail = self.tail.wrapping_add(1);
            return Some(value);
        }

        let slot_idx = self.tail & self.mask;
        let slot = &self.inner.slots[slot_idx];

        // Immediate path: check generation counter (no shared_head bouncing).
        // Generation and data are in the same Slot → single cache line transfer.
        let generation = unsafe { ptr::read_volatile(slot.generation.get()) };
        if generation == self.tail {
            compiler_fence(Ordering::Acquire);
            let value = unsafe { ptr::read((*slot.data.get()).as_ptr()) };
            self.tail = self.tail.wrapping_add(1);
            return Some(value);
        }

        // Batch path: load shared_head
        self.cached_head = self.inner.shared_head.load(Ordering::Acquire);
        if self.tail < self.cached_head {
            let value = unsafe { ptr::read((*slot.data.get()).as_ptr()) };
            self.tail = self.tail.wrapping_add(1);
            return Some(value);
        }

        None
    }

    /// Returns true if the sender has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        !self.inner.shared.tx_alive.load(Ordering::Relaxed)
    }

}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.shared.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// Transport Implementation
// ============================================================================

/// A bidirectional endpoint using Onesided SPSC channels.
///
/// All channels use producer-only write semantics. The consumer never writes
/// back to the channel; instead, inflight management prevents overwrite.
/// `sync()` publishes pending sends via `shared_head` atomic store.
pub struct OnesidedEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    /// Channel for sending requests (this endpoint → peer)
    call_tx: Sender<Req>,
    /// Channel for receiving responses (peer → this endpoint)
    resp_rx: Receiver<Response<Resp>>,
    /// Channel for receiving requests (peer → this endpoint)
    call_rx: Receiver<Req>,
    /// Channel for sending responses (this endpoint → peer)
    resp_tx: Sender<Response<Resp>>,
    /// Current number of inflight requests
    inflight: usize,
    /// Maximum allowed inflight requests
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for OnesidedEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(TransportError::InflightExceeded(req));
        }
        match self.call_tx.send(req) {
            Ok(slot_idx) => {
                self.inflight += 1;
                Ok(slot_idx as u64)
            }
            Err(SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        // Publish pending calls and responses
        self.call_tx.publish();
        self.resp_tx.publish();
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv_no_idx().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx
            .recv()
            .map(|(slot_idx, req)| (slot_idx as u64, req))
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        match self.resp_tx.send_no_idx(response) {
            Ok(()) => Ok(()),
            Err(SendError(r)) => Err(TransportError::Full(r.data)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.call_tx.capacity()
    }

    #[inline]
    fn inflight(&self) -> usize {
        self.inflight
    }

    #[inline]
    fn max_inflight(&self) -> usize {
        self.max_inflight
    }
}

/// Onesided transport factory.
///
/// Uses producer-only write SPSC with batched index publishing via sync().
pub struct OnesidedTransport;

impl Transport for OnesidedTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = OnesidedEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
        max_inflight: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        // Endpoint A calls → Endpoint B receives
        let (call_a_to_b_tx, call_a_to_b_rx) = channel(capacity);
        // Endpoint B responds → Endpoint A receives response
        let (resp_b_to_a_tx, resp_b_to_a_rx) = channel(capacity);

        // Endpoint B calls → Endpoint A receives
        let (call_b_to_a_tx, call_b_to_a_rx) = channel(capacity);
        // Endpoint A responds → Endpoint B receives response
        let (resp_a_to_b_tx, resp_a_to_b_rx) = channel(capacity);

        (
            OnesidedEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
                inflight: 0,
                max_inflight,
            },
            OnesidedEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
                inflight: 0,
                max_inflight,
            },
        )
    }
}

// ============================================================================
// Variant: Immediate publish (index-based, per-send atomic store)
// ============================================================================

/// Endpoint that publishes shared_head on every call/reply (no batching).
/// Tests hypothesis: single shared_head creates cache line contention at high QD.
pub struct OnesidedImmediateEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    call_tx: Sender<Req>,
    resp_rx: Receiver<Response<Resp>>,
    call_rx: Receiver<Req>,
    resp_tx: Sender<Response<Resp>>,
    inflight: usize,
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for OnesidedImmediateEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(TransportError::InflightExceeded(req));
        }
        match self.call_tx.send(req) {
            Ok(slot_idx) => {
                self.call_tx.publish(); // Immediate publish
                self.inflight += 1;
                Ok(slot_idx as u64)
            }
            Err(SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        // Redundant but harmless — call/reply already published
        self.call_tx.publish();
        self.resp_tx.publish();
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv_no_idx().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx
            .recv()
            .map(|(slot_idx, req)| (slot_idx as u64, req))
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        match self.resp_tx.send_no_idx(response) {
            Ok(()) => {
                self.resp_tx.publish(); // Immediate publish
                Ok(())
            }
            Err(SendError(r)) => Err(TransportError::Full(r.data)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.call_tx.capacity()
    }

    #[inline]
    fn inflight(&self) -> usize {
        self.inflight
    }

    #[inline]
    fn max_inflight(&self) -> usize {
        self.max_inflight
    }
}

pub struct OnesidedImmediateTransport;

impl Transport for OnesidedImmediateTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = OnesidedImmediateEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
        max_inflight: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        let (call_a_to_b_tx, call_a_to_b_rx) = channel(capacity);
        let (resp_b_to_a_tx, resp_b_to_a_rx) = channel(capacity);
        let (call_b_to_a_tx, call_b_to_a_rx) = channel(capacity);
        let (resp_a_to_b_tx, resp_a_to_b_rx) = channel(capacity);

        (
            OnesidedImmediateEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
                inflight: 0,
                max_inflight,
            },
            OnesidedImmediateEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
                inflight: 0,
                max_inflight,
            },
        )
    }
}

// ============================================================================
// Variant: Validity flag (original design, distributed per-slot flags)
// ============================================================================

/// Slot with per-slot validity flag (original onesided design).
#[repr(C)]
struct ValiditySlot<T> {
    valid: UnsafeCell<bool>,
    _pad: [u8; 7],
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> ValiditySlot<T> {
    fn new() -> Self {
        Self {
            valid: UnsafeCell::new(false),
            _pad: [0; 7],
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

struct ValidityInner<T> {
    shared: DisconnectState,
    slots: Box<[ValiditySlot<T>]>,
}

unsafe impl<T: Send> Send for ValidityInner<T> {}
unsafe impl<T: Send> Sync for ValidityInner<T> {}

pub struct ValiditySender<T> {
    inner: Arc<ValidityInner<T>>,
    head: usize,
    mask: usize,
}

unsafe impl<T: Send> Send for ValiditySender<T> {}

pub struct ValidityReceiver<T> {
    inner: Arc<ValidityInner<T>>,
    tail: usize,
    mask: usize,
}

unsafe impl<T: Send> Send for ValidityReceiver<T> {}

fn validity_channel<T: Serial>(capacity: usize) -> (ValiditySender<T>, ValidityReceiver<T>) {
    assert!(capacity > 0);
    let cap = capacity.next_power_of_two();
    let mask = cap - 1;
    let slots: Box<[ValiditySlot<T>]> = (0..cap).map(|_| ValiditySlot::new()).collect();
    let inner = Arc::new(ValidityInner {
        shared: DisconnectState {
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        },
        slots,
    });
    (
        ValiditySender {
            inner: Arc::clone(&inner),
            head: 0,
            mask,
        },
        ValidityReceiver {
            inner,
            tail: 0,
            mask,
        },
    )
}

impl<T: Serial> ValiditySender<T> {
    #[inline]
    pub fn send(&mut self, value: T) -> Result<usize, SendError<T>> {
        let slot_idx = self.head;
        let slot = &self.inner.slots[slot_idx];
        // Sentinel: invalidate next slot
        let next_slot_idx = (self.head + 1) & self.mask;
        let next_slot = &self.inner.slots[next_slot_idx];
        unsafe {
            ptr::write_volatile(next_slot.valid.get(), false);
        }
        compiler_fence(Ordering::Release);
        // Write data
        unsafe {
            ptr::write((*slot.data.get()).as_mut_ptr(), value);
        }
        compiler_fence(Ordering::Release);
        // Mark valid
        unsafe {
            ptr::write_volatile(slot.valid.get(), true);
        }
        self.head = (self.head + 1) & self.mask;
        Ok(slot_idx)
    }

    #[inline]
    pub fn send_no_idx(&mut self, value: T) -> Result<(), SendError<T>> {
        self.send(value)?;
        Ok(())
    }

    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Drop for ValiditySender<T> {
    fn drop(&mut self) {
        self.inner.shared.tx_alive.store(false, Ordering::Release);
    }
}

impl<T: Serial> ValidityReceiver<T> {
    #[inline]
    pub fn recv(&mut self) -> Option<(usize, T)> {
        let slot_idx = self.tail;
        self.recv_inner().map(|value| (slot_idx, value))
    }

    #[inline]
    pub fn recv_no_idx(&mut self) -> Option<T> {
        self.recv_inner()
    }

    #[inline]
    fn recv_inner(&mut self) -> Option<T> {
        let slot = &self.inner.slots[self.tail];
        if !unsafe { ptr::read_volatile(slot.valid.get()) } {
            return None;
        }
        compiler_fence(Ordering::Acquire);
        let value = unsafe { ptr::read((*slot.data.get()).as_ptr()) };
        self.tail = (self.tail + 1) & self.mask;
        Some(value)
    }
}

impl<T> Drop for ValidityReceiver<T> {
    fn drop(&mut self) {
        self.inner.shared.rx_alive.store(false, Ordering::Release);
    }
}

/// Endpoint using validity-flag SPSC (original onesided design).
/// Tests hypothesis: distributed validity flags reduce cache contention.
pub struct OnesidedValidityEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    call_tx: ValiditySender<Req>,
    resp_rx: ValidityReceiver<Response<Resp>>,
    call_rx: ValidityReceiver<Req>,
    resp_tx: ValiditySender<Response<Resp>>,
    inflight: usize,
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for OnesidedValidityEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(TransportError::InflightExceeded(req));
        }
        match self.call_tx.send(req) {
            Ok(slot_idx) => {
                self.inflight += 1;
                Ok(slot_idx as u64)
            }
            Err(SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        // No-op: validity flags provide immediate visibility
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv_no_idx().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx
            .recv()
            .map(|(slot_idx, req)| (slot_idx as u64, req))
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        match self.resp_tx.send_no_idx(response) {
            Ok(()) => Ok(()),
            Err(SendError(r)) => Err(TransportError::Full(r.data)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.call_tx.capacity()
    }

    #[inline]
    fn inflight(&self) -> usize {
        self.inflight
    }

    #[inline]
    fn max_inflight(&self) -> usize {
        self.max_inflight
    }
}

pub struct OnesidedValidityTransport;

impl Transport for OnesidedValidityTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = OnesidedValidityEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
        max_inflight: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        let (call_a_to_b_tx, call_a_to_b_rx) = validity_channel(capacity);
        let (resp_b_to_a_tx, resp_b_to_a_rx) = validity_channel(capacity);
        let (call_b_to_a_tx, call_b_to_a_rx) = validity_channel(capacity);
        let (resp_a_to_b_tx, resp_a_to_b_rx) = validity_channel(capacity);

        (
            OnesidedValidityEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
                inflight: 0,
                max_inflight,
            },
            OnesidedValidityEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
                inflight: 0,
                max_inflight,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::fastforward;

    // ========================================================================
    // SPSC Tests
    // ========================================================================

    #[test]
    fn test_spsc_send_recv() {
        let (mut tx, mut rx) = channel::<u32>(4);

        // Send
        let slot0 = tx.send(100).unwrap();
        let slot1 = tx.send(200).unwrap();
        tx.publish(); // Make visible

        assert_eq!(slot0, 0);
        assert_eq!(slot1, 1);

        // Receive (read-only)
        let (idx, val) = rx.recv().unwrap();
        assert_eq!(idx, 0);
        assert_eq!(val, 100);

        let (idx, val) = rx.recv().unwrap();
        assert_eq!(idx, 1);
        assert_eq!(val, 200);

        // No more data
        assert!(rx.recv().is_none());

        // With inflight management, we can send more without explicit free_slot
        let slot2 = tx.send(300).unwrap();
        tx.publish();
        assert_eq!(slot2, 2);
    }

    #[test]
    fn test_spsc_consumer_is_readonly() {
        let (mut tx, mut rx) = channel::<u64>(4);

        // Send some values
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.publish();

        // Receive values
        let (_, _) = rx.recv().unwrap();
        let (_, _) = rx.recv().unwrap();

        // With inflight management (external), we can keep sending
        // The consumer doesn't write to the channel
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        tx.publish();

        // Receive all
        let (_, val) = rx.recv().unwrap();
        assert_eq!(val, 3);
        let (_, val) = rx.recv().unwrap();
        assert_eq!(val, 4);
    }

    #[test]
    fn test_spsc_threaded_with_inflight_limit() {
        use std::sync::Barrier;
        use std::thread;

        let (mut tx, mut rx) = channel::<u64>(256);
        let (mut resp_tx, mut resp_rx) = fastforward::channel::<(usize, u64)>(256);

        let barrier = Arc::new(Barrier::new(2));
        let iterations = 100_000u64;
        let inflight_max = 128usize;

        let barrier_clone = Arc::clone(&barrier);
        let producer = thread::spawn(move || {
            barrier_clone.wait();

            let mut inflight = 0usize;
            let mut sent = 0u64;
            let mut received = 0u64;

            while received < iterations {
                // Send requests (up to inflight_max in flight)
                while sent < iterations && inflight < inflight_max {
                    tx.send(sent).unwrap();
                    inflight += 1;
                    sent += 1;
                }
                tx.publish(); // Make batch visible

                // Receive responses (no free_slot needed)
                while let Some((_, resp_val)) = resp_rx.recv() {
                    assert_eq!(resp_val, received);
                    inflight -= 1;
                    received += 1;
                }
            }
        });

        let barrier_clone = Arc::clone(&barrier);
        let consumer = thread::spawn(move || {
            barrier_clone.wait();

            let mut processed = 0u64;

            while processed < iterations {
                // Receive requests (read-only!)
                while let Some((slot_idx, req_val)) = rx.recv() {
                    assert_eq!(req_val, processed);

                    // Send response with slot_idx
                    loop {
                        if resp_tx.send((slot_idx, req_val)).is_ok() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    processed += 1;
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    // ========================================================================
    // Transport Tests
    // ========================================================================

    #[test]
    fn test_call_reply() {
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8, 8);

        // Endpoint A sends request to B
        let token0 = endpoint_a.call(100).unwrap();
        let token1 = endpoint_a.call(200).unwrap();

        // Tokens are slot indices (0, 1, 2, ...)
        assert_eq!(token0, 0);
        assert_eq!(token1, 1);

        // sync() to make calls visible
        endpoint_a.sync();

        // Endpoint B receives requests
        endpoint_b.sync();
        let (t0, req0) = endpoint_b.recv().unwrap();
        assert_eq!(t0, 0);
        assert_eq!(req0, 100);

        let (t1, req1) = endpoint_b.recv().unwrap();
        assert_eq!(t1, 1);
        assert_eq!(req1, 200);

        assert!(endpoint_b.recv().is_none());

        // Endpoint B sends responses with the same tokens
        endpoint_b.reply(t0, req0 + 1000).unwrap();
        endpoint_b.reply(t1, req1 + 1000).unwrap();
        endpoint_b.sync(); // Publish responses

        // Endpoint A receives responses
        endpoint_a.sync();
        let (rt0, resp0) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(rt0, 0);
        assert_eq!(resp0, 1100);

        let (rt1, resp1) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(rt1, 1);
        assert_eq!(resp1, 1200);

        assert!(endpoint_a.try_recv_response().is_none());
    }

    #[test]
    fn test_bidirectional() {
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8, 8);

        // Both endpoints can call each other simultaneously
        let token_a = endpoint_a.call(100).unwrap();
        let token_b = endpoint_b.call(200).unwrap();

        // sync() to make calls visible
        endpoint_a.sync();
        endpoint_b.sync();

        // A receives B's call
        let (t, req) = endpoint_a.recv().unwrap();
        assert_eq!(req, 200);
        endpoint_a.reply(t, 201).unwrap();

        // B receives A's call
        let (t, req) = endpoint_b.recv().unwrap();
        assert_eq!(req, 100);
        endpoint_b.reply(t, 101).unwrap();

        // sync() to make responses visible
        endpoint_a.sync();
        endpoint_b.sync();

        // Both receive responses
        let (t, resp) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(t, token_a);
        assert_eq!(resp, 101);

        let (t, resp) = endpoint_b.try_recv_response().unwrap();
        assert_eq!(t, token_b);
        assert_eq!(resp, 201);
    }

    #[test]
    fn test_capacity() {
        let (endpoint_a, _endpoint_b) = OnesidedTransport::channel::<u32, u32>(8, 8);
        assert_eq!(endpoint_a.capacity(), 8);
    }

    #[test]
    fn test_wrap_around() {
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8, 8);

        for round in 0..3u32 {
            // Send 4 items
            for i in 0..4u32 {
                let token = endpoint_a.call(round * 4 + i).unwrap();
                // Token is slot_idx, wraps at capacity (8)
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token, "round={}, i={}", round, i);
            }
            endpoint_a.sync(); // Publish calls

            // Receive and reply
            endpoint_b.sync();
            for i in 0..4u32 {
                let (token, req) = endpoint_b.recv().unwrap();
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token);
                assert_eq!(req, round * 4 + i);
                endpoint_b.reply(token, req + 1000).unwrap();
            }
            endpoint_b.sync(); // Publish responses

            // Endpoint A receives responses
            endpoint_a.sync();
            for i in 0..4u32 {
                let (token, resp) = endpoint_a.try_recv_response().unwrap();
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token);
                assert_eq!(resp, round * 4 + i + 1000);
            }
        }
    }

    #[test]
    fn test_threaded() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u64, u64>(256, 256);

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                endpoint_b.sync();
                if let Some((token, data)) = endpoint_b.recv() {
                    endpoint_b.reply(token, data + 1000).unwrap();
                    endpoint_b.sync();
                    count += 1;
                }
            }
            // Drain remaining
            endpoint_b.sync();
            while let Some((token, data)) = endpoint_b.recv() {
                endpoint_b.reply(token, data + 1000).unwrap();
                endpoint_b.sync();
                count += 1;
            }
            count
        });

        let mut sent = 0u64;
        let mut received = 0u64;
        let iterations = 10000u64;
        let inflight_max = 128u64;

        while sent < iterations || received < sent {
            // Send some requests (limited by inflight)
            while sent < iterations && sent - received < inflight_max {
                if endpoint_a.call(sent).is_ok() {
                    sent += 1;
                } else {
                    break;
                }
            }

            // Receive responses
            endpoint_a.sync();
            while let Some((_token, resp)) = endpoint_a.try_recv_response() {
                assert!(resp >= 1000);
                received += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        let callee_count = handle.join().unwrap();

        assert_eq!(received, iterations);
        assert_eq!(callee_count, iterations);
    }
}
