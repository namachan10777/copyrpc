#![allow(unsafe_op_in_unsafe_fn)]

#[macro_use]
mod barrier;

pub mod cq;
pub mod dc;
pub mod device;
pub mod pd;
pub mod qp;
pub mod srq;
pub mod tm_srq;
pub mod types;
pub mod ud;
pub mod wqe;

// Re-export CQE types
pub use cq::{Cqe, CqeOpcode};

/// Trait for queues that can receive completion notifications from a CQ.
///
/// Implemented by RcQp, DenseRcQp, etc.
pub trait CompletionTarget {
    /// Get the QP number.
    fn qpn(&self) -> u32;

    /// Dispatch a CQE to this queue.
    ///
    /// Called by CompletionQueue::poll() when a CQE for this queue is received.
    fn dispatch_cqe(&self, cqe: Cqe);
}
