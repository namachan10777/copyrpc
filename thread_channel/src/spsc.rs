//! Single-producer single-consumer bounded channel.
//!
//! This implementation is wait-free and uses local caching to minimize
//! atomic operations. The design separates the tx and rx blocks to avoid
//! false sharing.

use std::cell::UnsafeCell;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// Sender block - only accessed by the sender thread.
#[repr(C)]
struct TxBlock {
    /// Write position (only updated by sender).
    tail: AtomicUsize,
    /// Flag indicating the receiver has disconnected.
    rx_dead: AtomicBool,
}

/// Receiver block - only accessed by the receiver thread.
#[repr(C)]
struct RxBlock {
    /// Read position (only updated by receiver).
    head: AtomicUsize,
    /// Flag indicating the sender has disconnected.
    tx_dead: AtomicBool,
}

/// Shared channel state.
struct Inner<T> {
    tx: TxBlock,
    rx: RxBlock,
    cap: usize,
    slots: Box<[UnsafeCell<ManuallyDrop<MaybeUninit<T>>>]>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // No need to drop elements as T: Serial implies T: Copy
    }
}

/// The sending half of a SPSC channel.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    /// Cached value of head to reduce atomic reads.
    head_cache: usize,
}

unsafe impl<T: Send> Send for Sender<T> {}

/// The receiving half of a SPSC channel.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    /// Cached value of tail to reduce atomic reads.
    tail_cache: usize,
}

unsafe impl<T: Send> Send for Receiver<T> {}

/// Creates a new SPSC channel with the given capacity.
///
/// # Panics
/// Panics if `capacity` is 0.
pub fn channel<T: Serial>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "channel capacity must be greater than 0");

    let slots: Box<[_]> = (0..capacity)
        .map(|_| UnsafeCell::new(ManuallyDrop::new(MaybeUninit::uninit())))
        .collect();

    let inner = Arc::new(Inner {
        tx: TxBlock {
            tail: AtomicUsize::new(0),
            rx_dead: AtomicBool::new(false),
        },
        rx: RxBlock {
            head: AtomicUsize::new(0),
            tx_dead: AtomicBool::new(false),
        },
        cap: capacity,
        slots,
    });

    let sender = Sender {
        inner: Arc::clone(&inner),
        head_cache: 0,
    };

    let receiver = Receiver {
        inner,
        tail_cache: 0,
    };

    (sender, receiver)
}

impl<T: Serial> Sender<T> {
    /// Attempts to send a value on this channel.
    ///
    /// Returns `Err` if the receiver has disconnected.
    /// Returns `Ok(None)` if the value was sent successfully.
    /// Returns `Ok(Some(value))` if the channel is full.
    pub fn try_send(&mut self, value: T) -> Result<Option<T>, SendError<T>> {
        // Check if receiver is dead
        if self.inner.tx.rx_dead.load(Ordering::Relaxed) {
            return Err(SendError(value));
        }

        let tail = self.inner.tx.tail.load(Ordering::Relaxed);
        let next_tail = (tail + 1) % self.inner.cap;

        // Check if we have room using cached head
        if next_tail == self.head_cache {
            // Cache miss - reload head
            self.head_cache = self.inner.rx.head.load(Ordering::Acquire);
            if next_tail == self.head_cache {
                // Channel is full
                return Ok(Some(value));
            }
        }

        // Write the value
        unsafe {
            let slot = &*self.inner.slots[tail].get();
            std::ptr::write(slot.as_ptr() as *mut T, value);
        }

        // Update tail with Release ordering to ensure the write is visible
        self.inner.tx.tail.store(next_tail, Ordering::Release);

        Ok(None)
    }

    /// Sends a value, blocking until space is available.
    ///
    /// Returns `Err` if the receiver has disconnected.
    pub fn send(&mut self, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send(value) {
                Ok(None) => return Ok(()),
                Ok(Some(v)) => {
                    value = v;
                    std::hint::spin_loop();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns true if the receiver has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        self.inner.tx.rx_dead.load(Ordering::Relaxed)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.rx.tx_dead.store(true, Ordering::Release);
    }
}

impl<T: Serial> Receiver<T> {
    /// Attempts to receive a value from this channel.
    ///
    /// Returns `Err(TryRecvError::Empty)` if the channel is empty.
    /// Returns `Err(TryRecvError::Disconnected)` if the sender has disconnected
    /// and the channel is empty.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let head = self.inner.rx.head.load(Ordering::Relaxed);

        // Check if we have data using cached tail
        if head == self.tail_cache {
            // Cache miss - reload tail
            self.tail_cache = self.inner.tx.tail.load(Ordering::Acquire);
            if head == self.tail_cache {
                // Check if sender is dead
                if self.inner.rx.tx_dead.load(Ordering::Acquire) {
                    return Err(TryRecvError::Disconnected);
                }
                return Err(TryRecvError::Empty);
            }
        }

        // Read the value
        let value = unsafe {
            let slot = &*self.inner.slots[head].get();
            std::ptr::read(slot.as_ptr() as *const T)
        };

        // Update head with Release ordering
        let next_head = (head + 1) % self.inner.cap;
        self.inner.rx.head.store(next_head, Ordering::Release);

        Ok(value)
    }

    /// Receives a value, blocking until one is available.
    ///
    /// Returns `Err` if the sender has disconnected and the channel is empty.
    #[allow(dead_code)]
    pub fn recv(&mut self) -> Result<T, TryRecvError> {
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Empty) => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns true if the sender has disconnected.
    #[allow(dead_code)]
    pub fn is_disconnected(&self) -> bool {
        self.inner.rx.tx_dead.load(Ordering::Relaxed)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.tx.rx_dead.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let (mut tx, mut rx) = channel::<u32>(4);

        assert!(tx.try_send(1).unwrap().is_none());
        assert!(tx.try_send(2).unwrap().is_none());

        assert_eq!(rx.try_recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_capacity() {
        let (mut tx, mut rx) = channel::<u32>(2);

        // Can send one item (capacity - 1 due to ring buffer)
        assert!(tx.try_send(1).unwrap().is_none());
        // Second send should indicate full
        assert!(tx.try_send(2).unwrap().is_some());

        // Receive to make room
        assert_eq!(rx.try_recv().unwrap(), 1);

        // Now can send again
        assert!(tx.try_send(2).unwrap().is_none());
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

        assert!(matches!(tx.try_send(1), Err(SendError(1))));
        assert!(tx.is_disconnected());
    }

    #[test]
    fn test_threaded() {
        let (mut tx, mut rx) = channel::<u64>(1024);

        let sender = std::thread::spawn(move || {
            for i in 0..10000 {
                tx.send(i).unwrap();
            }
        });

        let receiver = std::thread::spawn(move || {
            for i in 0..10000 {
                assert_eq!(rx.recv().unwrap(), i);
            }
        });

        sender.join().unwrap();
        receiver.join().unwrap();
    }
}
