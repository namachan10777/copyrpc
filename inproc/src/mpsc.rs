//! Multi-producer single-consumer channel traits.
//!
//! This module provides abstract MPSC traits used by the `Mesh` network.
//! The concrete implementation is `FetchAddMpsc` in [`crate::mpsc_fetchadd`].

// ============================================================================
// Abstract MPSC Traits
// ============================================================================

/// Abstract MPSC channel sender trait.
pub trait MpscChannelSender<T>: Clone + Send {
    /// Sends a value on this channel.
    fn send(&self, value: T) -> Result<(), SendError<T>>;
}

/// Abstract MPSC channel receiver trait.
pub trait MpscChannelReceiver<T>: Send {
    /// Attempts to receive a value without blocking.
    fn try_recv(&self) -> Result<T, TryRecvError>;

    /// Receives a value, blocking until one is available.
    fn recv(&self) -> Result<T, TryRecvError>;
}

/// Factory trait for creating MPSC channels.
pub trait MpscChannel: 'static + Send + Sync {
    /// The sender type for this channel.
    type Sender<T: Send>: MpscChannelSender<T>;
    /// The receiver type for this channel.
    type Receiver<T: Send>: MpscChannelReceiver<T>;

    /// Creates a new MPSC channel.
    fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>);
}

// ============================================================================
// Error types
// ============================================================================

/// Error returned when sending fails because the receiver has disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sending on a disconnected channel")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned when receiving fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// All senders have disconnected.
    Disconnected,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Disconnected => write!(f, "receiving on a disconnected channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}
