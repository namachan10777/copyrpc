//! copyrpc transport modules.
//!
//! - `rc`: Reliable Connection transport implementation
//! - `dc`: Dynamically Connected transport implementation
//! - `transport`: Shared traits for transport-agnostic call/poll/reply paths

#![allow(unsafe_op_in_unsafe_fn)]

pub mod dc;
pub mod encoding;
pub mod error;
pub mod rc;
pub mod ring;
pub mod transport;

// Backward compatibility: keep RC types at crate root.
pub use rc::*;
