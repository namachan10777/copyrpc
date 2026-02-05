//! FastForward transport implementation.
//!
//! This transport is based on the spsc module which implements the
//! FastForward algorithm (PPoPP 2008).
//!
//! Token generation: uses monotonically increasing counters (0, 1, 2, ...).

use crate::spsc;
use crate::spsc::Serial;

use super::{Transport, TransportError, TransportReceiver, TransportSender};

/// Sender for FastForward transport.
pub struct FastForwardSender<T: Serial + Send> {
    inner: spsc::Sender<T>,
    /// Monotonically increasing send counter for token generation.
    send_count: u64,
    capacity: usize,
}

impl<T: Serial + Send> TransportSender<T> for FastForwardSender<T> {
    #[inline]
    fn send(&mut self, value: T) -> Result<u64, TransportError<T>> {
        match self.inner.send(value) {
            Ok(()) => {
                let token = self.send_count;
                self.send_count += 1;
                Ok(token)
            }
            Err(spsc::SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Receiver for FastForward transport.
pub struct FastForwardReceiver<T: Serial + Send> {
    inner: spsc::Receiver<T>,
    /// Monotonically increasing receive counter for token generation.
    recv_count: u64,
}

impl<T: Serial + Send> TransportReceiver<T> for FastForwardReceiver<T> {
    #[inline]
    fn recv(&mut self) -> Option<(u64, T)> {
        self.inner.recv().map(|value| {
            let token = self.recv_count;
            self.recv_count += 1;
            (token, value)
        })
    }
}

/// FastForward transport factory.
///
/// Uses the FastForward algorithm with validity flags for minimal cache line thrashing.
pub struct FastForwardTransport;

impl Transport for FastForwardTransport {
    type Sender<T: Serial + Send> = FastForwardSender<T>;
    type Receiver<T: Serial + Send> = FastForwardReceiver<T>;

    fn channel<T: Serial + Send>(capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = spsc::channel(capacity);
        let actual_capacity = capacity.next_power_of_two();
        (
            FastForwardSender {
                inner: tx,
                send_count: 0,
                capacity: actual_capacity,
            },
            FastForwardReceiver {
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
        let (mut tx, mut rx) = FastForwardTransport::channel::<u32>(8);

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
        let (tx, _rx) = FastForwardTransport::channel::<u32>(8);
        assert_eq!(tx.capacity(), 8);
    }

    #[test]
    fn test_monotonic_tokens() {
        let (mut tx, mut rx) = FastForwardTransport::channel::<u32>(16);

        // Tokens should be monotonically increasing
        for i in 0..10 {
            let token = tx.send(i).unwrap();
            assert_eq!(token, i as u64);
        }

        for i in 0..10 {
            let (token, value) = rx.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
        }

        // After wrap, tokens continue increasing
        for i in 10..20 {
            let token = tx.send(i).unwrap();
            assert_eq!(token, i as u64);
        }

        for i in 10..20 {
            let (token, value) = rx.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
        }
    }
}
