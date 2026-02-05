//! Onesided (producer-only write) transport implementation.
//!
//! This transport uses a producer-only write SPSC channel for both request
//! and response channels, achieving producer-only write semantics for all operations.
//!
//! Token: uses slot_idx from the SPSC (0 to capacity-1, wraps around).
//!
//! Flow control: The slot_idx flows from caller → callee → caller.
//! When callee sends a response with slot_idx, caller knows that slot is consumed.
//!
//! Store/Load characteristics:
//! - call(): producer stores to request channel
//! - recv(): consumer loads from request channel (read-only)
//! - reply(): producer stores to response channel
//! - poll(): consumer loads from response channel (read-only)

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::serial::Serial;

use super::{Response, Transport, TransportEndpoint, TransportError};

// ============================================================================
// SPSC Implementation (Producer-only write pattern)
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

/// Error returned when sending fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel is full")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Producer-only write slot.
///
/// The validity flag is ONLY written by the producer:
/// - Set to `true` when sending a request
/// - Set to `false` when freeing (after response received)
#[repr(C)]
struct Slot<T> {
    /// Validity flag: true = data present, false = empty
    /// ONLY the producer writes this flag!
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

/// Shared state for disconnect detection.
#[repr(C, align(64))]
struct Shared {
    tx_alive: AtomicBool,
    rx_alive: AtomicBool,
}

/// Cache-padded atomic for producer head (slip measurement)
#[repr(C, align(64))]
struct CachePaddedAtomicUsize(AtomicUsize);

/// Inner channel state.
struct Inner<T> {
    shared: Shared,
    /// Producer head for distance calculation (slip maintenance)
    producer_head: CachePaddedAtomicUsize,
    slots: Box<[Slot<T>]>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// The sending half of a producer-only write SPSC channel.
///
/// This sender can:
/// - Send values (marking slots as valid)
/// - Free slots (marking them as invalid after response received)
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    /// Local head index for sending
    head: usize,
    /// Bitmask for fast modulo
    mask: usize,
}

unsafe impl<T: Send> Send for Sender<T> {}

/// The receiving half of a producer-only write SPSC channel.
///
/// This receiver is READ-ONLY - it never writes to the channel!
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    /// Local tail index for receiving
    tail: usize,
    /// Bitmask for fast modulo
    mask: usize,
    /// Receive counter for slip check interval
    recv_count: usize,
    /// Whether initial slip has been established
    slip_initialized: bool,
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
    /// Returns `Ok(slot_idx)` on success.
    ///
    /// # Safety
    /// Caller must ensure inflight count does not exceed capacity.
    /// With proper inflight management, head never overtakes tail,
    /// so overwriting slots is safe (consumer already read them).
    #[inline]
    pub fn send(&mut self, value: T) -> Result<usize, SendError<T>> {
        let slot_idx = self.send_inner(value)?;
        Ok(slot_idx)
    }

    /// Sends a value through the channel without returning slot index.
    ///
    /// This is a convenience method for cases where the slot index is not needed,
    /// such as response channels in RPC patterns.
    ///
    /// # Safety
    /// Same constraints as `send()` - caller must ensure inflight count does not exceed capacity.
    #[inline]
    pub fn send_no_idx(&mut self, value: T) -> Result<(), SendError<T>> {
        self.send_inner(value)?;
        Ok(())
    }

    /// Internal send implementation.
    #[inline]
    fn send_inner(&mut self, value: T) -> Result<usize, SendError<T>> {
        let slot_idx = self.head;
        let slot = &self.inner.slots[slot_idx];

        // FIRST: Invalidate the NEXT slot to create sentinel
        // This MUST happen before writing data to prevent race where consumer
        // reads old valid=true data before we place the sentinel
        let next_slot_idx = (self.head + 1) & self.mask;
        let next_slot = &self.inner.slots[next_slot_idx];
        unsafe {
            ptr::write_volatile(next_slot.valid.get(), false);
        }

        // Fence ensures sentinel is visible before we write data
        compiler_fence(Ordering::Release);

        // Write the value
        unsafe {
            ptr::write((*slot.data.get()).as_mut_ptr(), value);
        }

        // Fence ensures data write completes before valid flag
        compiler_fence(Ordering::Release);

        // Mark slot as valid (producer writes)
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

        Ok(slot_idx)
    }

    /// Frees a slot after receiving its response.
    ///
    /// DEPRECATED: With inflight constraint management, explicit slot freeing
    /// is no longer needed. Keeping for backward compatibility.
    #[inline]
    #[allow(dead_code)]
    pub fn free_slot(&mut self, slot_idx: usize) {
        debug_assert!(slot_idx <= self.mask, "slot_idx out of bounds");
        let slot = &self.inner.slots[slot_idx];

        // Mark slot as empty (producer writes)
        unsafe {
            ptr::write_volatile(slot.valid.get(), false);
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
    /// The `slot_idx` should be included in the response so the sender
    /// can free the slot.
    ///
    /// **This method is READ-ONLY** - it does not write to the channel!
    #[inline]
    pub fn recv(&mut self) -> Option<(usize, T)> {
        let slot_idx = self.tail;
        self.recv_inner().map(|value| (slot_idx, value))
    }

    /// Receives a value from the channel without returning slot index.
    ///
    /// This is a convenience method for cases where the slot index is not needed,
    /// such as response channels in RPC patterns.
    ///
    /// **This method is READ-ONLY** - it does not write to the channel!
    #[inline]
    pub fn recv_no_idx(&mut self) -> Option<T> {
        self.recv_inner()
    }

    /// Internal receive implementation.
    #[inline]
    fn recv_inner(&mut self) -> Option<T> {
        let slot = &self.inner.slots[self.tail];

        // Check if slot has data (consumer reads)
        if !unsafe { ptr::read_volatile(slot.valid.get()) } {
            return None;
        }

        // Compiler fence ensures flag read completes before data read
        compiler_fence(Ordering::Acquire);

        // Read the value (consumer reads, doesn't write anything!)
        let value = unsafe { ptr::read((*slot.data.get()).as_ptr()) };

        // Advance tail
        // NOTE: We do NOT mark the slot as invalid here!
        // The producer will invalidate it before writing new data.
        self.tail = (self.tail + 1) & self.mask;

        Some(value)
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

/// A bidirectional endpoint using Onesided SPSC channels.
///
/// Each endpoint contains:
/// - Outgoing request channel using producer-only write SPSC
/// - Incoming response channel using producer-only write SPSC (consumer read-only)
/// - Incoming request channel using producer-only write SPSC
/// - Outgoing response channel using producer-only write SPSC (consumer read-only)
///
/// All channels use producer-only write semantics, ensuring optimal cache behavior.
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
    fn poll(&mut self) -> Option<(u64, Resp)> {
        // Use recv_no_idx() since we don't need the slot index for responses
        self.resp_rx.recv_no_idx().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx.recv().map(|(slot_idx, req)| (slot_idx as u64, req))
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        // Use send_no_idx() since we don't need slot tracking for responses
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
/// Uses producer-only write SPSC for both requests and responses.
/// This is the default transport for Flux, optimized for RPC patterns where
/// all channels have producer-only write and consumer-only read semantics.
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
        assert_eq!(slot2, 2);
    }

    #[test]
    fn test_spsc_consumer_is_readonly() {
        let (mut tx, mut rx) = channel::<u64>(4);

        // Send some values
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        // Receive values (this should NOT modify the valid flags)
        let (_, _) = rx.recv().unwrap();
        let (_, _) = rx.recv().unwrap();

        // With inflight management (external), we can keep sending
        // The consumer doesn't write to the channel
        tx.send(3).unwrap();
        tx.send(4).unwrap();

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

        // Endpoint B receives requests
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

        // Endpoint A receives responses
        let (rt0, resp0) = endpoint_a.poll().unwrap();
        assert_eq!(rt0, 0);
        assert_eq!(resp0, 1100);

        let (rt1, resp1) = endpoint_a.poll().unwrap();
        assert_eq!(rt1, 1);
        assert_eq!(resp1, 1200);

        assert!(endpoint_a.poll().is_none());
    }

    #[test]
    fn test_bidirectional() {
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8, 8);

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
        let (t, resp) = endpoint_a.poll().unwrap();
        assert_eq!(t, token_a);
        assert_eq!(resp, 101);

        let (t, resp) = endpoint_b.poll().unwrap();
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
        // Test that tokens (slot indices) wrap around correctly
        // This SPSC uses sentinel, so use capacity=8 and send batches of 4
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8, 8);

        for round in 0..3u32 {
            // Send 4 items
            for i in 0..4u32 {
                let token = endpoint_a.call(round * 4 + i).unwrap();
                // Token is slot_idx, wraps at capacity (8)
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token, "round={}, i={}", round, i);
            }

            // Receive and reply
            for i in 0..4u32 {
                let (token, req) = endpoint_b.recv().unwrap();
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token);
                assert_eq!(req, round * 4 + i);
                endpoint_b.reply(token, req + 1000).unwrap();
            }

            // Endpoint A receives responses
            for i in 0..4u32 {
                let (token, resp) = endpoint_a.poll().unwrap();
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
            while let Some((_token, resp)) = endpoint_a.poll() {
                // Note: responses may not be in order due to batching
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
