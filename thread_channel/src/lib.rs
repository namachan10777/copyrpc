//! Thread-safe channel implementations for inter-thread communication.
//!
//! This crate provides:
//! - `flux`: SPSC-based n-to-n communication (separate queue per peer)
//! - `mesh`: MPSC-based n-to-n communication (single shared receive queue)
//! - `transport`: Transport trait abstraction for SPSC implementations

pub mod flux;
pub mod mesh;
pub mod peer_channel;
pub mod spsc;
pub mod spsc_lamport;
pub mod spsc_rpc;
pub mod transport;

pub(crate) mod mpsc;

pub use flux::{create_flux, create_flux_with_transport, Flux, RecvHandle};
pub use mesh::{create_mesh, Mesh};
pub use spsc::Serial;
pub use transport::{
    FastForwardTransport, LamportTransport, OnesidedTransport, Transport, TransportError,
    TransportReceiver, TransportSender,
};

/// A received message, distinguishing between notifications, requests, and responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivedMessage<T> {
    /// A one-way notification (no reply expected).
    Notify(T),
    /// A request that expects a reply.
    Request {
        /// The request number to use when replying.
        req_num: u64,
        /// The request data.
        data: T,
    },
    /// A response to a previous call.
    Response {
        /// The request number this responds to.
        req_num: u64,
        /// The response data.
        data: T,
    },
}

/// Error returned when sending fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// The target peer has disconnected.
    Disconnected(T),
    /// The channel to the target peer is full.
    Full(T),
    /// Invalid peer ID.
    InvalidPeer(T),
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Disconnected(_) => write!(f, "peer has disconnected"),
            SendError::Full(_) => write!(f, "channel is full"),
            SendError::InvalidPeer(_) => write!(f, "invalid peer ID"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned when receiving fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// All channels are empty.
    Empty,
    /// All senders have disconnected.
    Disconnected,
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Empty => write!(f, "all channels are empty"),
            RecvError::Disconnected => write!(f, "all senders have disconnected"),
        }
    }
}

impl std::error::Error for RecvError {}
