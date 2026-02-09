//! Thread-safe channel implementations for inter-thread communication.
//!
//! This crate provides:
//! - `flux`: SPSC-based n-to-n communication via `mempc::MpscChannel`
//! - `mesh`: MPSC-based n-to-n communication (single shared receive queue)

pub mod flux;
pub mod mesh;

pub mod mpsc;
pub mod mpsc_fetchadd;

pub use flux::{Flux, RecvHandle, create_flux, create_flux_with};
pub use mempc::ReplyToken;
pub use mempc::Serial;
pub use mempc::{FastForwardMpsc, FetchAddMpsc as MempcFetchAddMpsc, LamportMpsc, OnesidedMpsc};
pub use mesh::{Mesh, create_mesh, create_mesh_with};
pub use mpsc_fetchadd::FetchAddMpsc;

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
