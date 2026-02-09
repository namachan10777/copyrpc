//! Lock-free bounded MPSC channel using `fetch_add` for slot acquisition.
//!
//! Based on Vyukov's bounded MPMC queue, specialized for single-consumer.
//! Each slot is cache-line padded to prevent false sharing between
//! concurrent producers writing to adjacent slots.

use std::cell::{Cell, UnsafeCell};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::mpsc::{MpscChannel, MpscChannelReceiver, MpscChannelSender, SendError, TryRecvError};
use mempc::common::CachePadded;

// ============================================================================
// Slot
// ============================================================================

/// A single slot in the ring buffer.
///
/// Each slot occupies its own cache line to prevent false sharing
/// between producers writing to adjacent positions concurrently.
#[repr(C, align(64))]
struct Slot<T> {
    /// Sequence number for coordination.
    /// - `seq == pos`: slot is free for producer at position `pos`
    /// - `seq == pos + 1`: slot contains data for consumer
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

// ============================================================================
// Inner (shared state)
// ============================================================================

struct Inner<T> {
    buffer: Box<[Slot<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    mask: usize,
    capacity: usize,
    sender_count: AtomicUsize,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0 && capacity.is_power_of_two());
        let buffer: Vec<Slot<T>> = (0..capacity)
            .map(|i| Slot {
                seq: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            buffer: buffer.into_boxed_slice(),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            mask: capacity - 1,
            capacity,
            sender_count: AtomicUsize::new(1),
            rx_alive: AtomicBool::new(true),
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // Drop any remaining items in the buffer
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        for pos in tail..head {
            let idx = pos & self.mask;
            let slot = &self.buffer[idx];
            if slot.seq.load(Ordering::Relaxed) == pos + 1 {
                unsafe { slot.data.get().read().assume_init_drop() };
            }
        }
    }
}

// ============================================================================
// Sender
// ============================================================================

/// The sending half of a `FetchAddMpsc` channel. Cloneable for multiple producers.
pub struct FetchAddSender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for FetchAddSender<T> {
    fn clone(&self) -> Self {
        self.inner.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for FetchAddSender<T> {
    fn drop(&mut self) {
        self.inner.sender_count.fetch_sub(1, Ordering::Release);
    }
}

unsafe impl<T: Send> Send for FetchAddSender<T> {}

impl<T: Send> MpscChannelSender<T> for FetchAddSender<T> {
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        let inner = &*self.inner;

        // Check if receiver is still alive before claiming a slot
        if !inner.rx_alive.load(Ordering::Acquire) {
            return Err(SendError(value));
        }

        // Claim a unique position via fetch_add
        let pos = inner.head.fetch_add(1, Ordering::Relaxed);
        let idx = pos & inner.mask;
        let slot = &inner.buffer[idx];

        // Spin until the slot is free for this position
        loop {
            let seq = slot.seq.load(Ordering::Acquire);
            if seq == pos {
                break;
            }
            // Check if receiver is still alive
            if !inner.rx_alive.load(Ordering::Relaxed) {
                // Receiver is gone. We've already incremented head,
                // but that's OK — no one will read this slot.
                return Err(SendError(value));
            }
            std::hint::spin_loop();
        }

        // Write data
        unsafe { slot.data.get().write(MaybeUninit::new(value)) };

        // Mark slot as full
        slot.seq.store(pos + 1, Ordering::Release);

        Ok(())
    }
}

// ============================================================================
// Receiver
// ============================================================================

/// The receiving half of a `FetchAddMpsc` channel. Single consumer only.
pub struct FetchAddReceiver<T> {
    inner: Arc<Inner<T>>,
    cached_tail: Cell<usize>,
}

unsafe impl<T: Send> Send for FetchAddReceiver<T> {}

impl<T> Drop for FetchAddReceiver<T> {
    fn drop(&mut self) {
        self.inner.rx_alive.store(false, Ordering::Release);
    }
}

impl<T: Send> MpscChannelReceiver<T> for FetchAddReceiver<T> {
    fn try_recv(&self) -> Result<T, TryRecvError> {
        let inner = &*self.inner;
        let pos = self.cached_tail.get();
        let idx = pos & inner.mask;
        let slot = &inner.buffer[idx];

        let seq = slot.seq.load(Ordering::Acquire);
        if seq != pos + 1 {
            // No data yet — check for disconnect
            if inner.sender_count.load(Ordering::Acquire) == 0 {
                // Double-check: there might be data that arrived before the last sender dropped
                let seq2 = slot.seq.load(Ordering::Acquire);
                if seq2 != pos + 1 {
                    return Err(TryRecvError::Disconnected);
                }
                // Fall through — data is available
            } else {
                return Err(TryRecvError::Empty);
            }
        }

        // Read data
        let value = unsafe { slot.data.get().read().assume_init() };

        // Free the slot for the next cycle
        slot.seq.store(pos + inner.capacity, Ordering::Release);

        // Advance tail
        self.cached_tail.set(pos + 1);
        inner.tail.store(pos + 1, Ordering::Relaxed);

        Ok(value)
    }

    fn recv(&self) -> Result<T, TryRecvError> {
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Empty) => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
    }
}

// ============================================================================
// FetchAddMpsc factory
// ============================================================================

/// Lock-free bounded MPSC channel using `fetch_add` for slot acquisition.
///
/// `CAP` must be a power of two. Default is 1024.
///
/// Use with `Mesh`: `create_mesh_with::<T, U, F, FetchAddMpsc>(n, callback)`
pub struct FetchAddMpsc<const CAP: usize = 1024>;

impl<const CAP: usize> MpscChannel for FetchAddMpsc<CAP> {
    type Sender<T: Send> = FetchAddSender<T>;
    type Receiver<T: Send> = FetchAddReceiver<T>;

    fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>) {
        let inner = Arc::new(Inner::new(CAP));
        let sender = FetchAddSender {
            inner: Arc::clone(&inner),
        };
        let receiver = FetchAddReceiver {
            inner,
            cached_tail: Cell::new(0),
        };
        (sender, receiver)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_send_recv() {
        let (tx, rx) = FetchAddMpsc::<16>::channel::<u64>();
        tx.send(42).unwrap();
        tx.send(43).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 42);
        assert_eq!(rx.try_recv().unwrap(), 43);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_multiple_senders() {
        let (tx, rx) = FetchAddMpsc::<64>::channel::<u64>();
        let tx2 = tx.clone();

        tx.send(1).unwrap();
        tx2.send(2).unwrap();

        let mut values = vec![rx.try_recv().unwrap(), rx.try_recv().unwrap()];
        values.sort();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn test_sender_disconnect() {
        let (tx, rx) = FetchAddMpsc::<16>::channel::<u64>();
        tx.send(1).unwrap();
        drop(tx);

        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn test_receiver_disconnect() {
        let (tx, rx) = FetchAddMpsc::<16>::channel::<u64>();
        drop(rx);
        assert!(matches!(tx.send(1), Err(SendError(1))));
    }

    #[test]
    fn test_fill_and_drain() {
        let (tx, rx) = FetchAddMpsc::<8>::channel::<u64>();

        // Fill all 8 slots
        for i in 0..8 {
            tx.send(i).unwrap();
        }

        // Drain all 8 slots
        for i in 0..8 {
            assert_eq!(rx.try_recv().unwrap(), i);
        }

        // Fill again (tests wraparound)
        for i in 8..16 {
            tx.send(i).unwrap();
        }
        for i in 8..16 {
            assert_eq!(rx.try_recv().unwrap(), i);
        }
    }

    #[test]
    fn test_threaded_multiple_producers() {
        let (tx, rx) = FetchAddMpsc::<1024>::channel::<u64>();
        let num_senders = 4;
        let msgs_per_sender = 1000u64;

        let handles: Vec<_> = (0..num_senders)
            .map(|i| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for j in 0..msgs_per_sender {
                        tx.send(i * msgs_per_sender + j).unwrap();
                    }
                })
            })
            .collect();

        drop(tx);

        let mut received = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(v) => received.push(v),
                Err(TryRecvError::Empty) => {
                    if handles.iter().all(|h| h.is_finished()) {
                        while let Ok(v) = rx.try_recv() {
                            received.push(v);
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(received.len(), (num_senders * msgs_per_sender) as usize);
    }

}
