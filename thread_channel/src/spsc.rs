//! Single-producer single-consumer bounded channel.
//!
//! This implementation is based on the FastForward algorithm (PPoPP 2008).
//! Key design principles:
//! - Head/tail indices are local to each thread (not shared)
//! - Slot validity flags determine empty/full status
//! - Minimizes cache line thrashing

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{compiler_fence, AtomicBool, Ordering};
use std::sync::Arc;

/// Marker trait for types that can be safely transmitted through the channel.
///
/// # Safety
/// Types implementing this trait must be `Copy` to ensure they can be safely
/// transmitted without ownership issues.
pub unsafe trait Serial: Copy {}

// Implement Serial for common types
unsafe impl Serial for u8 {}
unsafe impl Serial for u16 {}
unsafe impl Serial for u32 {}
unsafe impl Serial for u64 {}
unsafe impl Serial for u128 {}
unsafe impl Serial for usize {}
unsafe impl Serial for i8 {}
unsafe impl Serial for i16 {}
unsafe impl Serial for i32 {}
unsafe impl Serial for i64 {}
unsafe impl Serial for i128 {}
unsafe impl Serial for isize {}
unsafe impl Serial for f32 {}
unsafe impl Serial for f64 {}
unsafe impl Serial for bool {}
unsafe impl Serial for char {}
unsafe impl<T: Copy> Serial for Option<T> {}
unsafe impl<T: Copy, E: Copy> Serial for Result<T, E> {}
unsafe impl<T: Copy, const N: usize> Serial for [T; N] {}
unsafe impl<A: Copy, B: Copy> Serial for (A, B) {}
unsafe impl<A: Copy, B: Copy, C: Copy> Serial for (A, B, C) {}
unsafe impl<A: Copy, B: Copy, C: Copy, D: Copy> Serial for (A, B, C, D) {}

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

/// Shared channel state.
struct Inner<T> {
    shared: Shared,
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
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.shared.rx_alive.store(false, Ordering::Release);
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

        assert_eq!(rx.try_recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_capacity() {
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
    fn test_sender_disconnect() {
        let (tx, mut rx) = channel::<u32>(4);

        drop(tx);

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
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
    fn test_immediate_visibility() {
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
    fn test_recv_returns_none_when_empty() {
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
    fn test_batch_send_recv() {
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
    fn test_send_full() {
        let (mut tx, mut _rx) = channel::<u32>(2);

        // FastForward uses validity flags, so full capacity is available
        // capacity=2 rounds up to 2, so can hold 2 items
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        // Next send should fail with SendError
        assert!(matches!(tx.send(3), Err(SendError(3))));
    }

    #[test]
    fn test_threaded_batch() {
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
    fn test_fastforward_no_index_sharing() {
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
    fn test_full_then_consume_then_send() {
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
}
