//! Thread-safe channel implementations for inter-thread communication.
//!
//! This crate provides:
//! - `flux`: SPSC-based n-to-n communication (separate queue per peer)
//! - `mesh`: MPSC-based n-to-n communication (single shared receive queue)

pub mod flux;
pub mod mesh;

pub(crate) mod mpsc;
pub(crate) mod spsc;

pub use flux::{create_flux, Flux};
pub use mesh::{create_mesh, Mesh};
pub use spsc::Serial;

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
