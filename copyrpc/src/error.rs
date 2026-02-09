//! Error types for copyrpc.

use std::io;

/// RPC operation errors.
#[derive(Debug)]
pub enum Error {
    /// IO error from the underlying mlx5 layer.
    Io(io::Error),
    /// Ring buffer is full, cannot send more data.
    RingFull,
    /// Remote consumer position not yet known (connection not established).
    RemoteConsumerUnknown,
    /// Call ID not found in pending calls.
    CallIdNotFound(u32),
    /// Invalid message header.
    InvalidHeader,
    /// Endpoint not found for the given QPN.
    EndpointNotFound(u32),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::RingFull => write!(f, "Ring buffer is full"),
            Error::RemoteConsumerUnknown => write!(f, "Remote consumer position unknown"),
            Error::CallIdNotFound(id) => write!(f, "Call ID {} not found", id),
            Error::InvalidHeader => write!(f, "Invalid message header"),
            Error::EndpointNotFound(qpn) => write!(f, "Endpoint not found for QPN {}", qpn),
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

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// Error from [`Endpoint::call()`] that returns `user_data` on transient failures.
#[derive(Debug)]
pub enum CallError<U> {
    /// Ring buffer is full. `user_data` is returned for retry.
    RingFull(U),
    /// Insufficient credit for response. `user_data` is returned for retry.
    InsufficientCredit(U),
    /// Fatal error. `user_data` is lost.
    Other(Error),
}

impl<U> CallError<U> {
    pub fn into_inner(self) -> Option<U> {
        match self {
            CallError::RingFull(u) => Some(u),
            CallError::InsufficientCredit(u) => Some(u),
            CallError::Other(_) => None,
        }
    }
}

impl<U: std::fmt::Debug> std::fmt::Display for CallError<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::RingFull(_) => write!(f, "Ring buffer is full"),
            CallError::InsufficientCredit(_) => write!(f, "Insufficient credit for response"),
            CallError::Other(e) => write!(f, "{}", e),
        }
    }
}

/// Result type for copyrpc operations.
pub type Result<T> = std::result::Result<T, Error>;
