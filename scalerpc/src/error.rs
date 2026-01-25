//! Error types for ScaleRPC.

use std::fmt;

/// Error type for ScaleRPC operations.
#[derive(Debug)]
pub enum Error {
    /// No free slots available in the message pool.
    NoFreeSlots,
    /// Invalid slot index.
    InvalidSlotIndex(usize),
    /// Slot is not in expected state.
    InvalidSlotState { expected: SlotState, actual: SlotState },
    /// IO error from underlying RDMA operations.
    Io(std::io::Error),
    /// Connection not found.
    ConnectionNotFound(usize),
    /// Group not found.
    GroupNotFound(usize),
    /// No available connection in group.
    NoAvailableConnection,
    /// Maximum active connections reached.
    MaxActiveReached,
    /// Protocol error.
    Protocol(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoFreeSlots => write!(f, "no free slots available"),
            Error::InvalidSlotIndex(idx) => write!(f, "invalid slot index: {}", idx),
            Error::InvalidSlotState { expected, actual } => {
                write!(f, "invalid slot state: expected {:?}, got {:?}", expected, actual)
            }
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::ConnectionNotFound(id) => write!(f, "connection not found: {}", id),
            Error::GroupNotFound(id) => write!(f, "group not found: {}", id),
            Error::NoAvailableConnection => write!(f, "no available connection"),
            Error::MaxActiveReached => write!(f, "maximum active connections reached"),
            Error::Protocol(msg) => write!(f, "protocol error: {}", msg),
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

/// Result type alias for ScaleRPC operations.
pub type Result<T> = std::result::Result<T, Error>;

/// State of a message slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotState {
    /// Slot is free and available for allocation.
    Free = 0,
    /// Slot is reserved by a client for sending a request.
    Reserved = 1,
    /// Request has been sent, waiting for response.
    RequestPending = 2,
    /// Response is ready to be read.
    ResponseReady = 3,
}

impl SlotState {
    /// Convert from u8 to SlotState.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(SlotState::Free),
            1 => Some(SlotState::Reserved),
            2 => Some(SlotState::RequestPending),
            3 => Some(SlotState::ResponseReady),
            _ => None,
        }
    }
}
