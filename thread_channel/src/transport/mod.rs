//! Transport abstraction for bidirectional RPC channels.
//!
//! This module provides a unified interface for different SPSC-based RPC implementations:
//! - `FastForwardTransport`: Based on FastForward algorithm with validity flags
//! - `OnesidedTransport`: Producer-only write pattern for requests (based on spsc_rpc)
//! - `LamportTransport`: Lamport-style with batched index synchronization
//!
//! Transport abstracts a bidirectional RPC channel where a single endpoint can:
//! - Send requests via `call()` and receive responses via `poll()`
//! - Receive requests via `recv()` and send responses via `reply()`
//! - Tokens flow from one endpoint to the other for request/response matching

pub mod fastforward;
pub mod lamport;
pub mod onesided;

use crate::spsc::Serial;

/// Error returned when transport operations fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportError<T> {
    /// The channel is full.
    Full(T),
    /// The peer has disconnected.
    Disconnected(T),
}

impl<T> std::fmt::Display for TransportError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Full(_) => write!(f, "channel is full"),
            TransportError::Disconnected(_) => write!(f, "peer has disconnected"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for TransportError<T> {}

/// A bidirectional RPC endpoint.
///
/// Each endpoint can both send and receive requests/responses:
/// - `call()` sends a request and returns a token for response matching
/// - `poll()` receives responses to previous calls
/// - `recv()` receives requests from the peer
/// - `reply()` sends a response for a received request
///
/// For transports with batched synchronization (like Lamport), sync is performed
/// automatically at appropriate times (typically in poll/recv).
pub trait TransportEndpoint<Req: Serial + Send, Resp: Serial + Send>: Send {
    /// Sends a request and returns a token for response matching.
    ///
    /// The token will be included in the response when `poll()` returns.
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>>;

    /// Receives a response if available.
    ///
    /// Returns `Some((token, response))` where token matches the one from `call()`.
    /// For batched transports, this also synchronizes state.
    fn poll(&mut self) -> Option<(u64, Resp)>;

    /// Receives a request if available.
    ///
    /// Returns `Some((token, request))`. The token must be passed to `reply()`.
    /// For batched transports, this also synchronizes state.
    fn recv(&mut self) -> Option<(u64, Req)>;

    /// Sends a response for the given token.
    ///
    /// The token should be the one received from `recv()`.
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>>;

    /// Returns the capacity of the channel.
    fn capacity(&self) -> usize;
}

/// Factory trait for creating bidirectional RPC transport channels.
///
/// Creates pairs of connected endpoints where each endpoint can both
/// send and receive requests/responses.
pub trait Transport: Send + Sync + 'static {
    /// The endpoint type for this transport.
    type Endpoint<Req: Serial + Send, Resp: Serial + Send>: TransportEndpoint<Req, Resp>;

    /// Creates a new bidirectional RPC channel pair with the given capacity.
    ///
    /// Returns two connected endpoints. Each endpoint can:
    /// - Call the peer via `call()` and receive responses via `poll()`
    /// - Receive calls from the peer via `recv()` and respond via `reply()`
    ///
    /// The actual capacity may be rounded up to a power of 2.
    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>);
}

// Re-export transport types
pub use fastforward::FastForwardTransport;
pub use lamport::LamportTransport;
pub use onesided::OnesidedTransport;

/// Response wrapper that carries the token.
#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Response<T: Serial> {
    pub token: u64,
    pub data: T,
}

unsafe impl<T: Serial> Serial for Response<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    /// Generic test that verifies transport can complete 2 ring wraps with timeout.
    /// This catches deadlocks/hangs in sync logic.
    fn test_two_ring_wraps_with_timeout<T: Transport>() {
        let capacity = 8usize;
        let iterations = capacity * 2; // 2 ring wraps
        let timeout = Duration::from_secs(5);

        let (mut endpoint_a, mut endpoint_b) = T::channel::<u64, u64>(capacity);

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        // Responder thread
        let handle = thread::spawn(move || {
            let mut count = 0usize;
            while count < iterations && !stop2.load(Ordering::Relaxed) {
                // poll() does sync for Lamport
                endpoint_b.poll();
                if let Some((token, data)) = endpoint_b.recv() {
                    loop {
                        if endpoint_b.reply(token, data + 1000).is_ok() {
                            count += 1;
                            // Final sync after reply to make response visible
                            endpoint_b.poll();
                            break;
                        }
                        if stop2.load(Ordering::Relaxed) {
                            return count;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
            count
        });

        let start = std::time::Instant::now();
        let mut sent = 0usize;
        let mut received = 0usize;

        while received < iterations {
            if start.elapsed() > timeout {
                stop.store(true, Ordering::Relaxed);
                let responder_count = handle.join().unwrap();
                panic!(
                    "Timeout after {:?}! sent={}, received={}, responder={}",
                    timeout, sent, received, responder_count
                );
            }

            // Send requests (limit inflight to half capacity)
            while sent < iterations && sent - received < capacity / 2 {
                if endpoint_a.call(sent as u64).is_ok() {
                    sent += 1;
                } else {
                    break;
                }
            }

            // Receive responses (don't check token value, as it may wrap around)
            while let Some((_token, resp)) = endpoint_a.poll() {
                // Response should be data + 1000
                assert!(resp >= 1000);
                received += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        let responder_count = handle.join().unwrap();
        assert_eq!(received, iterations);
        assert_eq!(responder_count, iterations);
    }

    #[test]
    fn test_onesided_two_ring_wraps() {
        test_two_ring_wraps_with_timeout::<OnesidedTransport>();
    }

    #[test]
    fn test_fastforward_two_ring_wraps() {
        test_two_ring_wraps_with_timeout::<FastForwardTransport>();
    }

    #[test]
    fn test_lamport_two_ring_wraps() {
        test_two_ring_wraps_with_timeout::<LamportTransport>();
    }
}
