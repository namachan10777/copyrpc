//! FastForward transport implementation.
//!
//! This transport uses the FastForward algorithm (PPoPP 2008) for both
//! request and response channels.
//!
//! Token generation: uses monotonically increasing counters (0, 1, 2, ...).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::serial::Serial;

use super::{Response, Transport, TransportEndpoint, TransportError};

// ============================================================================
// SPSC Implementation (FastForward algorithm)
// ============================================================================

/// Slip maintenance constants (FastForward PPoPP 2008 Section 3.4.1)
const CACHE_LINE_SIZE: usize = 64;
const ENTRY_SIZE: usize = 8; // pointer size
const ENTRIES_PER_LINE: usize = CACHE_LINE_SIZE / ENTRY_SIZE; // = 8

/// DANGER: slip lost threshold (2 cache lines)
const SLIP_DANGER: usize = ENTRIES_PER_LINE * 2; // = 16

/// GOOD: target slip (6 cache lines)
const SLIP_GOOD: usize = ENTRIES_PER_LINE * 6; // = 48

/// Slip adjustment frequency
const SLIP_CHECK_INTERVAL: usize = 64;

/// Error returned when sending fails because the receiver has disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sending on a disconnected channel")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned when receiving fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// The sender has disconnected.
    Disconnected,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Disconnected => write!(f, "receiving on a disconnected channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// FastForward style slot with validity flag.
///
/// The validity flag indicates whether the slot contains valid data:
/// - `true`: data is present and can be read
/// - `false`: slot is empty and can be written to
///
/// No atomics needed: x86-64 TSO guarantees aligned word access is atomic
/// at hardware level, and volatile prevents compiler reordering.
#[repr(C)]
struct Slot<T> {
    /// Validity flag: true = data present, false = empty
    /// Uses UnsafeCell + volatile access (not atomic)
    valid: UnsafeCell<bool>,
    /// Padding to separate validity flag from data
    _pad: [u8; 7],
    /// The actual data storage
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            valid: UnsafeCell::new(false),
            _pad: [0; 7],
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

/// Shared state for disconnect detection only.
///
/// This is the only shared state between sender and receiver.
/// The actual queue indices are local to each endpoint.
#[repr(C, align(64))]
struct Shared {
    /// True if sender is still alive
    tx_alive: AtomicBool,
    /// True if receiver is still alive
    rx_alive: AtomicBool,
}

/// Cache-padded atomic for producer head (slip measurement)
#[repr(C, align(64))]
struct CachePaddedAtomicUsize(AtomicUsize);

/// Shared channel state.
struct Inner<T> {
    shared: Shared,
    /// Producer head for distance calculation (slip maintenance)
    producer_head: CachePaddedAtomicUsize,
    slots: Box<[Slot<T>]>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// The sending half of a SPSC channel.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    /// Local head index (not shared with receiver)
    head: usize,
    /// Bitmask for fast modulo
    mask: usize,
}

unsafe impl<T: Send> Send for Sender<T> {}

/// The receiving half of a SPSC channel.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    /// Local tail index (not shared with sender)
    tail: usize,
    /// Bitmask for fast modulo
    mask: usize,
    /// Receive counter for slip check interval
    recv_count: usize,
    /// Whether initial slip has been established
    slip_initialized: bool,
}

unsafe impl<T: Send> Send for Receiver<T> {}

/// Creates a new SPSC channel with the given capacity.
///
/// The actual capacity will be rounded up to the next power of 2 for performance.
///
/// # Panics
/// Panics if `capacity` is 0.
pub fn channel<T: Serial>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "channel capacity must be greater than 0");

    // Round up to next power of 2
    let cap = capacity.next_power_of_two();
    let mask = cap - 1;

    let slots: Box<[Slot<T>]> = (0..cap).map(|_| Slot::new()).collect();

    let inner = Arc::new(Inner {
        shared: Shared {
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        },
        producer_head: CachePaddedAtomicUsize(AtomicUsize::new(0)),
        slots,
    });

    let sender = Sender {
        inner: Arc::clone(&inner),
        head: 0,
        mask,
    };

    let receiver = Receiver {
        inner,
        tail: 0,
        mask,
        recv_count: 0,
        slip_initialized: false,
    };

    (sender, receiver)
}

impl<T: Serial> Sender<T> {
    /// Sends a value through the channel.
    ///
    /// The value is immediately visible to the receiver after this call.
    ///
    /// Returns `Err(SendError(value))` if the channel is full.
    ///
    /// Note: This does not check for receiver disconnection for performance.
    /// Disconnection can be detected via `is_disconnected()` if needed.
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        let slot = &self.inner.slots[self.head];

        // Check if slot is empty (valid == false)
        // Volatile read prevents compiler from caching/reordering
        if unsafe { ptr::read_volatile(slot.valid.get()) } {
            // Slot is full - channel is full
            return Err(SendError(value));
        }

        // Write the value
        unsafe {
            ptr::write((*slot.data.get()).as_mut_ptr(), value);
        }

        // Compiler fence ensures data write is not reordered after flag write
        // x86-64 TSO guarantees hardware ordering; this is for the compiler only
        compiler_fence(Ordering::Release);

        // Volatile write to make data visible
        unsafe {
            ptr::write_volatile(slot.valid.get(), true);
        }

        // Advance head
        self.head = (self.head + 1) & self.mask;

        // Publish head for slip measurement (relaxed - no sync needed)
        self.inner
            .producer_head
            .0
            .store(self.head, Ordering::Relaxed);

        Ok(())
    }

    /// Returns true if the receiver has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        !self.inner.shared.rx_alive.load(Ordering::Relaxed)
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
    /// Returns `Some(value)` if data is available, `None` otherwise.
    #[inline]
    pub fn recv(&mut self) -> Option<T> {
        let slot = &self.inner.slots[self.tail];

        // Check if slot has data (valid == true)
        // Volatile read prevents compiler from caching/reordering
        if !unsafe { ptr::read_volatile(slot.valid.get()) } {
            // Slot is empty
            return None;
        }

        // Compiler fence ensures flag read happens before data read
        compiler_fence(Ordering::Acquire);

        // Read the value
        let value = unsafe { ptr::read((*slot.data.get()).as_ptr()) };

        // Compiler fence ensures data read completes before we mark slot empty
        compiler_fence(Ordering::Release);

        // Volatile write to mark slot as empty
        unsafe {
            ptr::write_volatile(slot.valid.get(), false);
        }

        // Advance tail
        self.tail = (self.tail + 1) & self.mask;

        Some(value)
    }

    /// Attempts to receive a value from this channel.
    ///
    /// Returns `Err(TryRecvError::Empty)` if the channel is empty.
    /// Returns `Err(TryRecvError::Disconnected)` if the sender has disconnected
    /// and the channel is empty.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(value) = self.recv() {
            return Ok(value);
        }

        // Empty, check if sender is dead
        if !self.inner.shared.tx_alive.load(Ordering::Acquire) {
            return Err(TryRecvError::Disconnected);
        }

        Err(TryRecvError::Empty)
    }

    /// Returns true if the sender has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        !self.inner.shared.tx_alive.load(Ordering::Relaxed)
    }

    /// Returns the distance (number of items) between producer and consumer.
    ///
    /// This is used for temporal slipping to ensure producer and consumer
    /// operate on different cache lines.
    #[inline]
    fn distance(&self) -> usize {
        let head = self.inner.producer_head.0.load(Ordering::Relaxed);
        head.wrapping_sub(self.tail) & self.mask
    }

    /// Establishes and maintains temporal slip.
    ///
    /// Temporal slipping (FastForward PPoPP 2008 Section 3.4.1) delays the
    /// consumer to ensure producer and consumer operate on different cache lines,
    /// reducing cache line bouncing.
    ///
    /// On first call: waits until producer is SLIP_GOOD entries ahead.
    /// On subsequent calls (every SLIP_CHECK_INTERVAL): if slip drops below
    /// SLIP_DANGER, waits until it recovers to SLIP_GOOD.
    #[inline]
    pub fn maintain_slip(&mut self) {
        self.recv_count = self.recv_count.wrapping_add(1);

        // Check only every SLIP_CHECK_INTERVAL iterations (after initialization)
        if self.slip_initialized && self.recv_count % SLIP_CHECK_INTERVAL != 0 {
            return;
        }

        let dist = self.distance();

        if !self.slip_initialized {
            // Initial slip establishment: wait until SLIP_GOOD
            while self.distance() < SLIP_GOOD {
                std::hint::spin_loop();
            }
            self.slip_initialized = true;
        } else if dist < SLIP_DANGER {
            // Slip recovery: wait until SLIP_GOOD (with progress check)
            let mut prev_dist = dist;
            while self.distance() < SLIP_GOOD {
                let curr_dist = self.distance();
                // Break if producer stopped making progress
                if curr_dist <= prev_dist && curr_dist > 0 {
                    break;
                }
                prev_dist = curr_dist;
                std::hint::spin_loop();
            }
        }
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

/// A bidirectional endpoint using FastForward SPSC channels.
///
/// Each endpoint contains:
/// - Outgoing request channel (this endpoint calls, peer responds)
/// - Incoming response channel (peer responds to this endpoint's calls)
/// - Incoming request channel (peer calls, this endpoint responds)
/// - Outgoing response channel (this endpoint responds to peer's calls)
pub struct FastForwardEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    /// Channel for sending requests (this endpoint → peer)
    call_tx: Sender<Req>,
    /// Channel for receiving responses (peer → this endpoint)
    resp_rx: Receiver<Response<Resp>>,
    /// Channel for receiving requests (peer → this endpoint)
    call_rx: Receiver<Req>,
    /// Channel for sending responses (this endpoint → peer)
    resp_tx: Sender<Response<Resp>>,
    /// Monotonically increasing counter for call tokens
    send_count: u64,
    /// Monotonically increasing counter for recv tokens
    recv_count: u64,
    /// Capacity
    capacity: usize,
    /// Current number of inflight requests
    inflight: usize,
    /// Maximum allowed inflight requests
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for FastForwardEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(TransportError::InflightExceeded(req));
        }
        match self.call_tx.send(req) {
            Ok(()) => {
                let token = self.send_count;
                self.send_count += 1;
                self.inflight += 1;
                Ok(token)
            }
            Err(SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        // No-op for FastForward: validity flags provide immediate visibility
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx.recv().map(|req| {
            let token = self.recv_count;
            self.recv_count += 1;
            (token, req)
        })
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        match self.resp_tx.send(response) {
            Ok(()) => Ok(()),
            Err(SendError(r)) => Err(TransportError::Full(r.data)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
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

/// FastForward transport factory.
///
/// Uses the FastForward algorithm with validity flags for minimal cache line thrashing.
/// Both request and response channels use the same algorithm.
pub struct FastForwardTransport;

impl Transport for FastForwardTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = FastForwardEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
        max_inflight: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        let actual_capacity = capacity.next_power_of_two();

        // Endpoint A calls → Endpoint B receives
        let (call_a_to_b_tx, call_a_to_b_rx) = channel(capacity);
        // Endpoint B responds → Endpoint A receives response
        let (resp_b_to_a_tx, resp_b_to_a_rx) = channel(capacity);

        // Endpoint B calls → Endpoint A receives
        let (call_b_to_a_tx, call_b_to_a_rx) = channel(capacity);
        // Endpoint A responds → Endpoint B receives response
        let (resp_a_to_b_tx, resp_a_to_b_rx) = channel(capacity);

        (
            FastForwardEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
                send_count: 0,
                recv_count: 0,
                capacity: actual_capacity,
                inflight: 0,
                max_inflight,
            },
            FastForwardEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
                send_count: 0,
                recv_count: 0,
                capacity: actual_capacity,
                inflight: 0,
                max_inflight,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // SPSC Tests
    // ========================================================================

    #[test]
    fn test_spsc_send_recv() {
        let (mut tx, mut rx) = channel::<u32>(4);

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_spsc_capacity() {
        let (mut tx, mut rx) = channel::<u32>(2);

        // FastForward uses validity flags, so full capacity is available
        // capacity=2 rounds up to 2, so can hold 2 items
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        // Third should fail (full)
        assert!(tx.send(3).is_err());

        // Receive to make room
        assert_eq!(rx.try_recv().unwrap(), 1);

        // Now can write again
        tx.send(3).unwrap();
    }

    #[test]
    fn test_spsc_sender_disconnect() {
        let (tx, mut rx) = channel::<u32>(4);

        drop(tx);

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
        assert!(rx.is_disconnected());
    }

    #[test]
    fn test_spsc_receiver_disconnect() {
        let (mut tx, rx) = channel::<u32>(4);

        drop(rx);

        // send() doesn't check for disconnection for performance
        assert!(tx.send(1).is_ok());
        // Disconnection can be detected via is_disconnected()
        assert!(tx.is_disconnected());
    }

    #[test]
    fn test_spsc_threaded() {
        let (mut tx, mut rx) = channel::<u64>(1024);

        let sender = std::thread::spawn(move || {
            for i in 0..10000 {
                loop {
                    match tx.send(i) {
                        Ok(()) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
        });

        let receiver = std::thread::spawn(move || {
            for i in 0..10000 {
                loop {
                    match rx.try_recv() {
                        Ok(v) => {
                            assert_eq!(v, i);
                            break;
                        }
                        Err(TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(e) => panic!("recv error: {:?}", e),
                    }
                }
            }
        });

        sender.join().unwrap();
        receiver.join().unwrap();
    }

    #[test]
    fn test_spsc_immediate_visibility() {
        let (mut tx, mut rx) = channel::<u32>(8);

        // Send values - they should be immediately visible
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();

        // Receiver can see all values immediately (no flush needed)
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(3));
        assert!(rx.recv().is_none());
    }

    #[test]
    fn test_spsc_recv_returns_none_when_empty() {
        let (mut tx, mut rx) = channel::<u32>(8);

        // Send and recv some values
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        // recv() returns values
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert!(rx.recv().is_none());
    }

    #[test]
    fn test_spsc_batch_send_recv() {
        let (mut tx, mut rx) = channel::<u64>(64);

        // Batch send
        for i in 0..32 {
            tx.send(i).unwrap();
        }

        // Batch receive
        let mut received = Vec::new();
        while let Some(v) = rx.recv() {
            received.push(v);
        }

        assert_eq!(received.len(), 32);
        for (i, v) in received.iter().enumerate() {
            assert_eq!(*v, i as u64);
        }
    }

    #[test]
    fn test_spsc_send_full() {
        let (mut tx, mut _rx) = channel::<u32>(2);

        // FastForward uses validity flags, so full capacity is available
        // capacity=2 rounds up to 2, so can hold 2 items
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        // Next send should fail with SendError
        assert!(matches!(tx.send(3), Err(SendError(3))));
    }

    #[test]
    fn test_spsc_threaded_batch() {
        let (mut tx, mut rx) = channel::<u64>(1024);
        const BATCH_SIZE: u64 = 100;
        const NUM_BATCHES: u64 = 100;

        let sender = std::thread::spawn(move || {
            for batch in 0..NUM_BATCHES {
                for i in 0..BATCH_SIZE {
                    let val = batch * BATCH_SIZE + i;
                    // Spin until we can send
                    loop {
                        match tx.send(val) {
                            Ok(()) => break,
                            Err(_) => std::hint::spin_loop(),
                        }
                    }
                }
            }
        });

        let receiver = std::thread::spawn(move || {
            let mut expected = 0u64;
            let total = BATCH_SIZE * NUM_BATCHES;
            while expected < total {
                while let Some(v) = rx.recv() {
                    assert_eq!(v, expected);
                    expected += 1;
                }
            }
        });

        sender.join().unwrap();
        receiver.join().unwrap();
    }

    #[test]
    fn test_spsc_fastforward_no_index_sharing() {
        // This test verifies that the FastForward implementation doesn't share indices
        // by checking that send/recv work correctly even under contention
        let (mut tx, mut rx) = channel::<u64>(256);

        let sender = std::thread::spawn(move || {
            for i in 0..100_000u64 {
                loop {
                    if tx.send(i).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        });

        let receiver = std::thread::spawn(move || {
            let mut expected = 0u64;
            while expected < 100_000 {
                if let Some(v) = rx.recv() {
                    assert_eq!(v, expected);
                    expected += 1;
                }
            }
        });

        sender.join().unwrap();
        receiver.join().unwrap();
    }

    #[test]
    fn test_spsc_full_then_consume_then_send() {
        let (mut tx, mut rx) = channel::<u32>(4);

        // FastForward uses validity flags, so full capacity is available
        // capacity=4 rounds up to 4, so can hold 4 items
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        assert!(tx.send(5).is_err()); // Should be full

        // Consume all
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(3));
        assert_eq!(rx.recv(), Some(4));
        assert!(rx.recv().is_none());

        // Should be able to send again
        tx.send(10).unwrap();
        tx.send(11).unwrap();
        tx.send(12).unwrap();
        tx.send(13).unwrap();
        assert!(tx.send(14).is_err());

        assert_eq!(rx.recv(), Some(10));
        assert_eq!(rx.recv(), Some(11));
        assert_eq!(rx.recv(), Some(12));
        assert_eq!(rx.recv(), Some(13));
        assert!(rx.recv().is_none());
    }

    // ========================================================================
    // Transport Tests
    // ========================================================================

    #[test]
    fn test_call_reply() {
        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u32, u32>(8, 8);

        // Endpoint A sends request to B
        let token0 = endpoint_a.call(100).unwrap();
        let token1 = endpoint_a.call(200).unwrap();

        assert_eq!(token0, 0);
        assert_eq!(token1, 1);

        // Endpoint B receives requests
        let (t0, req0) = endpoint_b.recv().unwrap();
        assert_eq!(t0, 0);
        assert_eq!(req0, 100);

        let (t1, req1) = endpoint_b.recv().unwrap();
        assert_eq!(t1, 1);
        assert_eq!(req1, 200);

        assert!(endpoint_b.recv().is_none());

        // Endpoint B sends responses
        endpoint_b.reply(t0, req0 + 1000).unwrap();
        endpoint_b.reply(t1, req1 + 1000).unwrap();

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
        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u32, u32>(8, 8);

        // Both endpoints can call each other simultaneously
        let token_a = endpoint_a.call(100).unwrap();
        let token_b = endpoint_b.call(200).unwrap();

        // A receives B's call
        let (t, req) = endpoint_a.recv().unwrap();
        assert_eq!(req, 200);
        endpoint_a.reply(t, 201).unwrap();

        // B receives A's call
        let (t, req) = endpoint_b.recv().unwrap();
        assert_eq!(req, 100);
        endpoint_b.reply(t, 101).unwrap();

        // Both receive responses
        endpoint_a.sync();
        let (t, resp) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(t, token_a);
        assert_eq!(resp, 101);

        endpoint_b.sync();
        let (t, resp) = endpoint_b.try_recv_response().unwrap();
        assert_eq!(t, token_b);
        assert_eq!(resp, 201);
    }

    #[test]
    fn test_capacity() {
        let (endpoint_a, _endpoint_b) = FastForwardTransport::channel::<u32, u32>(8, 8);
        assert_eq!(endpoint_a.capacity(), 8);
    }

    #[test]
    fn test_monotonic_tokens() {
        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u32, u32>(32, 20);

        // Tokens should be monotonically increasing
        for i in 0..10 {
            let token = endpoint_a.call(i).unwrap();
            assert_eq!(token, i as u64);
        }

        for i in 0..10 {
            let (token, value) = endpoint_b.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
            endpoint_b.reply(token, value).unwrap();
        }

        // After wrap, tokens continue increasing
        for i in 10..20 {
            let token = endpoint_a.call(i).unwrap();
            assert_eq!(token, i as u64);
        }

        // Receive responses from first batch
        endpoint_a.sync();
        for i in 0..10 {
            let (token, resp) = endpoint_a.try_recv_response().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(resp, i);
        }
    }

    #[test]
    fn test_threaded() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u64, u64>(64, 64);

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                if let Some((token, data)) = endpoint_b.recv() {
                    endpoint_b.reply(token, data + 1000).unwrap();
                    count += 1;
                }
            }
            // Drain remaining
            while let Some((token, data)) = endpoint_b.recv() {
                endpoint_b.reply(token, data + 1000).unwrap();
                count += 1;
            }
            count
        });

        let mut sent = 0u64;
        let mut received = 0u64;
        let iterations = 10000u64;

        while sent < iterations || received < sent {
            // Send some requests
            while sent < iterations && sent - received < 32 {
                if endpoint_a.call(sent).is_ok() {
                    sent += 1;
                } else {
                    break;
                }
            }

            // Receive responses
            endpoint_a.sync();
            while let Some((token, resp)) = endpoint_a.try_recv_response() {
                assert_eq!(token, received);
                assert_eq!(resp, received + 1000);
                received += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        let callee_count = handle.join().unwrap();

        assert_eq!(received, iterations);
        assert_eq!(callee_count, iterations);
    }
}
