//! Lamport-style transport implementation.
//!
//! This transport uses Lamport-style SPSC queues with batched index
//! synchronization for reduced atomic operations.
//!
//! Token generation: uses monotonically increasing counters (0, 1, 2, ...).
//!
//! sync(): performed automatically in poll() and recv() to publish/free indices.
//! For batched transports like this, sync is done at optimal times:
//! - poll() publishes call head and frees response slots
//! - recv() frees call slots and publishes response head

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::serial::Serial;

use super::{Response, Transport, TransportEndpoint, TransportError};

// ============================================================================
// SPSC Implementation (Lamport-style with batched index synchronization)
// ============================================================================

/// Error returned when sending fails because the channel is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel is full")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Cache-line padded atomic usize for avoiding false sharing.
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

/// Inner channel state shared between sender and receiver.
struct Inner<T> {
    /// Ring buffer for data
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Shared head index (written by sender during poll)
    shared_head: CachePadded<AtomicUsize>,
    /// Shared tail index (written by receiver during poll)
    shared_tail: CachePadded<AtomicUsize>,
    /// Disconnect detection
    tx_alive: AtomicBool,
    rx_alive: AtomicBool,
    /// Capacity (power of 2)
    capacity: usize,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// The sending half of a Lamport SPSC channel.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    /// Local head index (not shared until poll)
    local_head: usize,
    /// Cached tail from receiver (reduces atomic reads)
    cached_tail: usize,
    /// Bitmask for fast modulo
    mask: usize,
}

unsafe impl<T: Send> Send for Sender<T> {}

/// The receiving half of a Lamport SPSC channel.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    /// Local tail index (not shared until poll)
    local_tail: usize,
    /// Cached head from sender (reduces atomic reads)
    cached_head: usize,
    /// Bitmask for fast modulo
    mask: usize,
}

unsafe impl<T: Send> Send for Receiver<T> {}

/// Creates a new Lamport SPSC channel with the given capacity.
///
/// The actual capacity will be rounded up to the next power of 2.
///
/// # Panics
/// Panics if `capacity` is 0.
pub fn channel<T: Serial>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "channel capacity must be greater than 0");

    let cap = capacity.next_power_of_two();
    let mask = cap - 1;

    let buffer: Box<[UnsafeCell<MaybeUninit<T>>]> =
        (0..cap).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();

    let inner = Arc::new(Inner {
        buffer,
        shared_head: CachePadded::new(AtomicUsize::new(0)),
        shared_tail: CachePadded::new(AtomicUsize::new(0)),
        tx_alive: AtomicBool::new(true),
        rx_alive: AtomicBool::new(true),
        capacity: cap,
    });

    let sender = Sender {
        inner: Arc::clone(&inner),
        local_head: 0,
        cached_tail: 0,
        mask,
    };

    let receiver = Receiver {
        inner,
        local_tail: 0,
        cached_head: 0,
        mask,
    };

    (sender, receiver)
}

impl<T: Serial> Sender<T> {
    /// Sends a value through the channel.
    ///
    /// Returns `Ok(())` on success, `Err(SendError(value))` if the channel is full.
    ///
    /// NOTE: Call poll() after sending a batch to make items visible to receiver.
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        // Check if we have room using cached tail
        // head - tail gives number of items in queue
        // We can send if (local_head - cached_tail) < capacity
        if self.local_head.wrapping_sub(self.cached_tail) >= self.inner.capacity {
            // Cache miss - refresh cached_tail from shared
            self.cached_tail = self.inner.shared_tail.load(Ordering::Acquire);
            if self.local_head.wrapping_sub(self.cached_tail) >= self.inner.capacity {
                // Still full
                return Err(SendError(value));
            }
        }

        // Write the value
        let slot = self.local_head & self.mask;
        unsafe {
            ptr::write((*self.inner.buffer[slot].get()).as_mut_ptr(), value);
        }

        // Advance local head (no atomic store here - deferred to poll())
        self.local_head = self.local_head.wrapping_add(1);

        Ok(())
    }

    /// Polls to synchronize state.
    ///
    /// This publishes the current head to make sent items visible.
    /// With immediate publish in send(), this is a no-op but kept for API consistency.
    #[inline]
    pub fn poll(&mut self) {
        self.inner.shared_head.store(self.local_head, Ordering::Release);
    }

    /// Returns true if the receiver has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        !self.inner.rx_alive.load(Ordering::Relaxed)
    }

    /// Returns the capacity of the channel.
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.tx_alive.store(false, Ordering::Release);
    }
}

impl<T: Serial> Receiver<T> {
    /// Receives a value from the channel if available.
    ///
    /// Returns `Some(value)` if data is available, `None` if empty.
    ///
    /// NOTE: Call poll() after receiving a batch to free slots for sender.
    #[inline]
    pub fn recv(&mut self) -> Option<T> {
        // Check if we have items using cached head
        // head - tail gives number of items in queue
        if self.local_tail == self.cached_head {
            // Cache miss - refresh cached_head from shared
            self.cached_head = self.inner.shared_head.load(Ordering::Acquire);
            if self.local_tail == self.cached_head {
                // Still empty
                return None;
            }
        }

        // Read the value
        let slot = self.local_tail & self.mask;
        let value = unsafe { ptr::read((*self.inner.buffer[slot].get()).as_ptr()) };

        // Advance local tail (no atomic store here - deferred to poll())
        self.local_tail = self.local_tail.wrapping_add(1);

        Some(value)
    }

    /// Polls to synchronize state.
    ///
    /// Publishes current head (sender) or tail (receiver) to make changes visible.
    /// Call this after sending/receiving a batch.
    #[inline]
    pub fn poll(&mut self) {
        self.inner.shared_tail.store(self.local_tail, Ordering::Release);
    }

    /// Returns true if the sender has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        !self.inner.tx_alive.load(Ordering::Relaxed)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// Transport Implementation
// ============================================================================

/// A bidirectional endpoint using Lamport SPSC channels.
///
/// Each endpoint contains:
/// - Outgoing request channel (this endpoint calls, peer responds)
/// - Incoming response channel (peer responds to this endpoint's calls)
/// - Incoming request channel (peer calls, this endpoint responds)
/// - Outgoing response channel (this endpoint responds to peer's calls)
///
/// sync() is performed automatically in poll() and recv().
pub struct LamportEndpoint<Req: Serial + Send, Resp: Serial + Send> {
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

impl<Req: Serial + Send, Resp: Serial + Send> LamportEndpoint<Req, Resp> {
    /// Perform sync on call-related channels:
    /// - Publish call head (make sent calls visible to peer)
    /// - Free response slots (allow peer to send more responses)
    #[inline]
    fn sync_call(&mut self) {
        self.call_tx.poll();
        self.resp_rx.poll();
    }

    /// Perform sync on recv-related channels:
    /// - Free call slots (allow peer to send more calls)
    /// - Publish response head (make sent responses visible to peer)
    #[inline]
    fn sync_recv(&mut self) {
        self.call_rx.poll();
        self.resp_tx.poll();
    }
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for LamportEndpoint<Req, Resp>
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
        // Sync all channels: publish our calls/responses, free slots for peer
        self.sync_call();
        self.sync_recv();
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        // No sync - call sync() first
        self.resp_rx.recv().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        // No sync - call sync() first
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

/// Lamport transport factory.
///
/// Uses batched index synchronization to reduce atomic operations.
/// sync() is performed automatically in poll() and recv().
pub struct LamportTransport;

impl Transport for LamportTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = LamportEndpoint<Req, Resp>;

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
            LamportEndpoint {
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
            LamportEndpoint {
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
        tx.poll();  // Make visible

        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert!(rx.recv().is_none());
    }

    #[test]
    fn test_spsc_capacity() {
        let (mut tx, mut rx) = channel::<u32>(2);

        // capacity=2 rounds up to 2
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        // Third should fail (full)
        assert!(tx.send(3).is_err());

        tx.poll();  // Make visible
        // Receive to make room
        assert_eq!(rx.recv(), Some(1));
        rx.poll();  // Free slot

        // Now can send again
        tx.send(3).unwrap();
    }

    #[test]
    fn test_spsc_sender_disconnect() {
        let (tx, mut rx) = channel::<u32>(4);

        drop(tx);

        assert!(rx.recv().is_none());
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
            for i in 0..100_000u64 {
                loop {
                    match tx.send(i) {
                        Ok(()) => {
                            tx.poll();  // Make visible
                            break;
                        }
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
        });

        let receiver = std::thread::spawn(move || {
            let mut expected = 0u64;
            while expected < 100_000 {
                if let Some(v) = rx.recv() {
                    rx.poll();  // Free slot
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

        // Fill the channel
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        tx.poll();  // Make visible
        assert!(tx.send(5).is_err()); // Should be full

        // Consume all
        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert_eq!(rx.recv(), Some(3));
        assert_eq!(rx.recv(), Some(4));
        rx.poll();  // Free slots
        assert!(rx.recv().is_none());

        // Should be able to send again
        tx.send(10).unwrap();
        tx.send(11).unwrap();
        tx.send(12).unwrap();
        tx.send(13).unwrap();
        tx.poll();  // Make visible
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
        let (mut endpoint_a, mut endpoint_b) = LamportTransport::channel::<u32, u32>(8, 8);

        // Endpoint A sends requests
        let token0 = endpoint_a.call(100).unwrap();
        let token1 = endpoint_a.call(200).unwrap();

        assert_eq!(token0, 0);
        assert_eq!(token1, 1);

        // sync() makes calls visible to peer
        endpoint_a.sync();
        endpoint_b.sync();

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

        // sync() makes responses visible to peer
        endpoint_b.sync();

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
        let (mut endpoint_a, mut endpoint_b) = LamportTransport::channel::<u32, u32>(8, 8);

        // Both endpoints can call each other simultaneously
        let token_a = endpoint_a.call(100).unwrap();
        let token_b = endpoint_b.call(200).unwrap();

        // sync() makes calls visible
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

        // sync() makes responses visible
        endpoint_a.sync();
        endpoint_b.sync();

        // Now both can receive responses
        let (t, resp) = endpoint_b.try_recv_response().unwrap();
        assert_eq!(t, token_b);
        assert_eq!(resp, 201);

        let (t, resp) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(t, token_a);
        assert_eq!(resp, 101);
    }

    #[test]
    fn test_capacity() {
        let (endpoint_a, _endpoint_b) = LamportTransport::channel::<u32, u32>(8, 8);
        assert_eq!(endpoint_a.capacity(), 8);
    }

    #[test]
    fn test_monotonic_tokens() {
        let (mut endpoint_a, mut endpoint_b) = LamportTransport::channel::<u32, u32>(32, 20);

        // Tokens should be monotonically increasing
        for i in 0..10 {
            let token = endpoint_a.call(i).unwrap();
            assert_eq!(token, i as u64);
        }
        endpoint_a.sync();
        endpoint_b.sync();

        for i in 0..10 {
            let (token, value) = endpoint_b.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
            endpoint_b.reply(token, value).unwrap();
        }
        endpoint_b.sync();

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

        let (mut endpoint_a, mut endpoint_b) = LamportTransport::channel::<u64, u64>(256, 256);

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                // sync() makes peer's calls visible and our responses visible
                endpoint_b.sync();
                if let Some((token, data)) = endpoint_b.recv() {
                    endpoint_b.reply(token, data + 1000).unwrap();
                    count += 1;
                }
            }
            // Drain remaining
            endpoint_b.sync();
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
            while sent < iterations && sent - received < 64 {
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
