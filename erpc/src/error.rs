//! Error types for eRPC.

use std::fmt;

/// Error type for eRPC operations.
#[derive(Debug)]
pub enum Error {
    /// IO error from underlying transport.
    Io(std::io::Error),
    /// Session not found.
    SessionNotFound(u16),
    /// Session already exists.
    SessionAlreadyExists(u16),
    /// Session is not connected.
    SessionNotConnected(u16),
    /// No available slots for requests.
    NoAvailableSlots,
    /// Request queue is full.
    RequestQueueFull,
    /// Invalid packet received.
    InvalidPacket,
    /// Invalid magic number in packet header.
    InvalidMagic { expected: u8, got: u8 },
    /// Invalid packet type.
    InvalidPacketType(u8),
    /// Message too large for MTU.
    MessageTooLarge { size: usize, max: usize },
    /// Credits exhausted.
    NoCredits,
    /// Maximum retries exceeded.
    MaxRetriesExceeded,
    /// Buffer too small.
    BufferTooSmall { required: usize, available: usize },
    /// Invalid configuration.
    InvalidConfig(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::SessionNotFound(id) => write!(f, "Session {} not found", id),
            Error::SessionAlreadyExists(id) => write!(f, "Session {} already exists", id),
            Error::SessionNotConnected(id) => write!(f, "Session {} is not connected", id),
            Error::NoAvailableSlots => write!(f, "No available slots for requests"),
            Error::RequestQueueFull => write!(f, "Request queue is full"),
            Error::InvalidPacket => write!(f, "Invalid packet received"),
            Error::InvalidMagic { expected, got } => {
                write!(f, "Invalid magic: expected {:#x}, got {:#x}", expected, got)
            }
            Error::InvalidPacketType(t) => write!(f, "Invalid packet type: {}", t),
            Error::MessageTooLarge { size, max } => {
                write!(f, "Message too large: {} bytes, max {} bytes", size, max)
            }
            Error::NoCredits => write!(f, "Credits exhausted"),
            Error::MaxRetriesExceeded => write!(f, "Maximum retries exceeded"),
            Error::BufferTooSmall { required, available } => {
                write!(
                    f,
                    "Buffer too small: required {} bytes, available {} bytes",
                    required, available
                )
            }
            Error::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<mlx5::wqe::SubmissionError> for Error {
    fn from(e: mlx5::wqe::SubmissionError) -> Self {
        Error::Io(std::io::Error::from(e))
    }
}

/// Result type for eRPC operations.
pub type Result<T> = std::result::Result<T, Error>;
