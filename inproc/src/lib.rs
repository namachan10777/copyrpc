//! Thread-safe channel implementations for inter-thread communication.
//!
//! This crate provides SPSC-based n-to-n communication via `mempc::OnesidedMpsc`.

pub mod flux;

pub use flux::{Flux, RecvHandle, create_flux};
pub use mempc::ReplyToken;
pub use mempc::Serial;

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
