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
