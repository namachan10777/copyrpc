//! Producer-only write SPSC channel for RPC patterns.
//!
//! This implementation is optimized for call/response patterns where:
//! - Producer sends requests and frees slots when responses arrive
//! - Consumer only reads requests (never writes to the channel)
//!
//! Key insight: In RPC, receiving a response proves the request was consumed,
//! so the producer can free the slot instead of the consumer.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{compiler_fence, AtomicBool, Ordering};
use std::sync::Arc;

use crate::spsc::Serial;

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

/// Inner channel state.
struct Inner<T> {
    shared: Shared,
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
        slots,
    });

    let sender = Sender {
        inner: Arc::clone(&inner),
        head: 0,
        mask,
    };

    let receiver = Receiver { inner, tail: 0, mask };

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
        let slot = &self.inner.slots[slot_idx];

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

        Some((slot_idx, value))
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
    fn test_consumer_is_readonly() {
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
    fn test_threaded_with_inflight_limit() {
        use std::sync::Barrier;
        use std::thread;

        let (mut tx, mut rx) = channel::<u64>(256);
        let (mut resp_tx, mut resp_rx) = crate::spsc::channel::<(usize, u64)>(256);

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
}
