//! Transport abstraction for SPSC channel implementations.
//!
//! This module provides a unified interface for different SPSC implementations:
//! - `FastForwardTransport`: Based on FastForward algorithm with validity flags
//! - `OnesidedTransport`: Producer-only write pattern (based on spsc_rpc)
//! - `LamportTransport`: Lamport-style with batched index synchronization
//!
//! Each transport provides `TransportSender` and `TransportReceiver` with
//! consistent APIs for sending values and receiving (token, value) pairs.

pub mod fastforward;
pub mod lamport;
pub mod onesided;

use crate::spsc::Serial;

/// Error returned when transport operations fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportError<T> {
    /// The channel is full.
    Full(T),
    /// The receiver has disconnected.
    Disconnected(T),
}

impl<T> std::fmt::Display for TransportError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Full(_) => write!(f, "channel is full"),
            TransportError::Disconnected(_) => write!(f, "receiver has disconnected"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for TransportError<T> {}

/// Sender half of a transport channel.
///
/// Provides methods to send values and optionally synchronize (for batched transports).
pub trait TransportSender<T: Serial + Send>: Send {
    /// Sends a value and returns a token for response matching.
    ///
    /// The token can be used to correlate responses with requests.
    fn send(&mut self, value: T) -> Result<u64, TransportError<T>>;

    /// Synchronizes state (for batched transports like Lamport).
    ///
    /// For non-batched transports, this is a no-op.
    fn sync(&mut self) {}

    /// Returns the capacity of the channel.
    fn capacity(&self) -> usize;
}

/// Receiver half of a transport channel.
///
/// Provides methods to receive (token, value) pairs and optionally synchronize.
pub trait TransportReceiver<T: Serial + Send>: Send {
    /// Receives a (token, value) pair if available.
    ///
    /// The token corresponds to the one returned by the sender's `send()`.
    fn recv(&mut self) -> Option<(u64, T)>;

    /// Synchronizes state (for batched transports like Lamport).
    ///
    /// For non-batched transports, this is a no-op.
    fn sync(&mut self) {}
}

/// Factory trait for creating transport channels.
///
/// Uses generic associated types (GAT) to allow different sender/receiver
/// types for different transport implementations.
pub trait Transport: Send + Sync + 'static {
    /// The sender type for this transport.
    type Sender<T: Serial + Send>: TransportSender<T>;
    /// The receiver type for this transport.
    type Receiver<T: Serial + Send>: TransportReceiver<T>;

    /// Creates a new channel pair with the given capacity.
    ///
    /// The actual capacity may be rounded up to a power of 2.
    fn channel<T: Serial + Send>(capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>);
}

// Re-export transport types
pub use fastforward::FastForwardTransport;
pub use lamport::LamportTransport;
pub use onesided::OnesidedTransport;
