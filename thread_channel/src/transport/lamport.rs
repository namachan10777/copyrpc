//! Lamport-style transport implementation.
//!
//! This transport is based on the spsc_lamport module with batched index
//! synchronization for reduced atomic operations.
//!
//! Token generation: uses monotonically increasing counters (0, 1, 2, ...).
//! sync(): calls inner.poll() to publish indices.

use crate::spsc::Serial;
use crate::spsc_lamport;

use super::{Transport, TransportError, TransportReceiver, TransportSender};

/// Sender for Lamport transport.
pub struct LamportSender<T: Serial + Send> {
    inner: spsc_lamport::Sender<T>,
    /// Monotonically increasing send counter for token generation.
    send_count: u64,
    capacity: usize,
}

impl<T: Serial + Send> TransportSender<T> for LamportSender<T> {
    #[inline]
    fn send(&mut self, value: T) -> Result<u64, TransportError<T>> {
        match self.inner.send(value) {
            Ok(()) => {
                let token = self.send_count;
                self.send_count += 1;
                Ok(token)
            }
            Err(spsc_lamport::SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        self.inner.poll();
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Receiver for Lamport transport.
pub struct LamportReceiver<T: Serial + Send> {
    inner: spsc_lamport::Receiver<T>,
    /// Monotonically increasing receive counter for token generation.
    recv_count: u64,
}

impl<T: Serial + Send> TransportReceiver<T> for LamportReceiver<T> {
    #[inline]
    fn recv(&mut self) -> Option<(u64, T)> {
        self.inner.recv().map(|value| {
            let token = self.recv_count;
            self.recv_count += 1;
            (token, value)
        })
    }

    #[inline]
    fn sync(&mut self) {
        self.inner.poll();
    }
}

/// Lamport transport factory.
///
/// Uses batched index synchronization to reduce atomic operations.
/// Call `sync()` after sending/receiving batches to make changes visible.
pub struct LamportTransport;

impl Transport for LamportTransport {
    type Sender<T: Serial + Send> = LamportSender<T>;
    type Receiver<T: Serial + Send> = LamportReceiver<T>;

    fn channel<T: Serial + Send>(capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = spsc_lamport::channel(capacity);
        let actual_capacity = capacity.next_power_of_two();
        (
            LamportSender {
                inner: tx,
                send_count: 0,
                capacity: actual_capacity,
            },
            LamportReceiver {
                inner: rx,
                recv_count: 0,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let (mut tx, mut rx) = LamportTransport::channel::<u32>(8);

        let token0 = tx.send(100).unwrap();
        let token1 = tx.send(200).unwrap();
        tx.sync(); // Make visible

        assert_eq!(token0, 0);
        assert_eq!(token1, 1);

        let (t0, v0) = rx.recv().unwrap();
        assert_eq!(t0, 0);
        assert_eq!(v0, 100);

        let (t1, v1) = rx.recv().unwrap();
        assert_eq!(t1, 1);
        assert_eq!(v1, 200);
        rx.sync(); // Free slots

        assert!(rx.recv().is_none());
    }

    #[test]
    fn test_capacity() {
        let (tx, _rx) = LamportTransport::channel::<u32>(8);
        assert_eq!(tx.capacity(), 8);
    }

    #[test]
    fn test_sync_required() {
        let (mut tx, mut rx) = LamportTransport::channel::<u32>(8);

        // Without sync, receiver may not see the data
        tx.send(100).unwrap();
        // Data might not be visible yet (depends on implementation)

        tx.sync(); // Now make visible

        let (_, v) = rx.recv().unwrap();
        assert_eq!(v, 100);
    }

    #[test]
    fn test_monotonic_tokens() {
        let (mut tx, mut rx) = LamportTransport::channel::<u32>(16);

        // Tokens should be monotonically increasing
        for i in 0..10 {
            let token = tx.send(i).unwrap();
            assert_eq!(token, i as u64);
        }
        tx.sync();

        for i in 0..10 {
            let (token, value) = rx.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
        }
        rx.sync();

        // After wrap, tokens continue increasing
        for i in 10..20 {
            let token = tx.send(i).unwrap();
            assert_eq!(token, i as u64);
        }
        tx.sync();

        for i in 10..20 {
            let (token, value) = rx.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
        }
    }
}
