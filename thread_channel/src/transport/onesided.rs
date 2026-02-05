//! Onesided (producer-only write) transport implementation.
//!
//! This transport is based on the spsc_rpc module and is optimized for
//! call/response patterns where:
//! - Producer sends requests and frees slots when responses arrive
//! - Consumer only reads requests (never writes to the channel)
//!
//! Token generation: uses slot_idx directly (0 to capacity-1).

use crate::spsc::Serial;
use crate::spsc_rpc;

use super::{Transport, TransportError, TransportReceiver, TransportSender};

/// Sender for onesided transport.
pub struct OnesidedSender<T: Serial + Send> {
    inner: spsc_rpc::Sender<T>,
}

impl<T: Serial + Send> TransportSender<T> for OnesidedSender<T> {
    #[inline]
    fn send(&mut self, value: T) -> Result<u64, TransportError<T>> {
        match self.inner.send(value) {
            Ok(slot_idx) => Ok(slot_idx as u64),
            Err(spsc_rpc::SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

/// Receiver for onesided transport.
pub struct OnesidedReceiver<T: Serial + Send> {
    inner: spsc_rpc::Receiver<T>,
}

impl<T: Serial + Send> TransportReceiver<T> for OnesidedReceiver<T> {
    #[inline]
    fn recv(&mut self) -> Option<(u64, T)> {
        self.inner.recv().map(|(slot_idx, value)| (slot_idx as u64, value))
    }
}

/// Onesided transport factory (producer-only write pattern).
///
/// This is the default transport for Flux, optimized for RPC patterns.
pub struct OnesidedTransport;

impl Transport for OnesidedTransport {
    type Sender<T: Serial + Send> = OnesidedSender<T>;
    type Receiver<T: Serial + Send> = OnesidedReceiver<T>;

    fn channel<T: Serial + Send>(capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = spsc_rpc::channel(capacity);
        (OnesidedSender { inner: tx }, OnesidedReceiver { inner: rx })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let (mut tx, mut rx) = OnesidedTransport::channel::<u32>(8);

        let token0 = tx.send(100).unwrap();
        let token1 = tx.send(200).unwrap();

        assert_eq!(token0, 0);
        assert_eq!(token1, 1);

        let (t0, v0) = rx.recv().unwrap();
        assert_eq!(t0, 0);
        assert_eq!(v0, 100);

        let (t1, v1) = rx.recv().unwrap();
        assert_eq!(t1, 1);
        assert_eq!(v1, 200);

        assert!(rx.recv().is_none());
    }

    #[test]
    fn test_capacity() {
        let (tx, _rx) = OnesidedTransport::channel::<u32>(8);
        assert_eq!(tx.capacity(), 8);
    }

    #[test]
    fn test_wrap_around() {
        // spsc_rpc uses sentinel in next slot, so we cannot fill to full capacity.
        // Use capacity=8 but only send 4 items per round.
        let (mut tx, mut rx) = OnesidedTransport::channel::<u32>(8);

        // Test that tokens wrap correctly (slot_idx wraps at capacity)
        for round in 0..3u32 {
            // Send 4 items (less than capacity to avoid sentinel collision)
            for i in 0..4u32 {
                let token = tx.send(round * 4 + i).unwrap();
                // Token is slot_idx, which wraps at capacity (8)
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token, "round={}, i={}", round, i);
            }

            // Receive all 4 items
            for i in 0..4u32 {
                let result = rx.recv();
                assert!(result.is_some(), "Expected value at round {} index {}", round, i);
                let (token, value) = result.unwrap();
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token);
                assert_eq!(value, round * 4 + i);
            }
        }
    }
}
