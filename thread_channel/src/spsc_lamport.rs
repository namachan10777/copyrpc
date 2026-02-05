//! Lamport-style SPSC bounded channel with batched index synchronization.
//!
//! This implementation reduces atomic operations by:
//! - Keeping local head/tail indices that are advanced without synchronization
//! - Only publishing to shared indices during poll() or when cache miss occurs
//! - Caching the remote index to reduce cross-thread reads

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::spsc::Serial;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let (mut tx, mut rx) = channel::<u32>(4);

        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.poll();  // Make visible

        assert_eq!(rx.recv(), Some(1));
        assert_eq!(rx.recv(), Some(2));
        assert!(rx.recv().is_none());
    }

    #[test]
    fn test_capacity() {
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
    fn test_sender_disconnect() {
        let (tx, mut rx) = channel::<u32>(4);

        drop(tx);

        assert!(rx.recv().is_none());
        assert!(rx.is_disconnected());
    }

    #[test]
    fn test_receiver_disconnect() {
        let (mut tx, rx) = channel::<u32>(4);

        drop(rx);

        // send() doesn't check for disconnection for performance
        assert!(tx.send(1).is_ok());
        // Disconnection can be detected via is_disconnected()
        assert!(tx.is_disconnected());
    }

    #[test]
    fn test_threaded() {
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
    fn test_full_then_consume_then_send() {
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
}
