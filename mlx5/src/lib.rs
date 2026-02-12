//! # mlx5 - Low-level RDMA library for Mellanox ConnectX NICs
//!
//! This crate provides direct access to mlx5 hardware features via DevX,
//! enabling zero-copy RDMA operations with minimal latency.
//!
//! ## Design Philosophy
//!
//! ### Queue-Centric Model
//!
//! The fundamental abstraction is based on **queues**, not connections:
//!
//! - **Send Queue (SQ)**: Handles outbound requests (SEND, RDMA WRITE/READ)
//! - **Receive Queue (RQ)**: Handles inbound receives for a single QP
//! - **Shared Receive Queue (SRQ)**: Shared receive pool for multiple QPs
//! - **Completion Queue (CQ)**: Collects completion notifications
//!
//! Queue Pairs (QP) and DC Initiators (DCI) are primarily **access points** to
//! these queues, not the essential abstraction. The real work happens at the
//! SQ/RQ/SRQ level.
//!
//! ### Unified Request Submission API
//!
//! All queue types use a consistent builder pattern for posting Work Queue Entries:
//!
//! ```ignore
//! // Send Queue (via QP)
//! qp.wqe_builder(entry)?.ctrl(...).sge(...).finish();
//!
//! // Receive Queue (via QP)
//! qp.recv_builder(entry)?.sge(...).finish();
//!
//! // Shared Receive Queue
//! srq.recv_builder(entry)?.sge(...).finish();
//! ```
//!
//! The `entry` parameter is user-defined metadata that will be returned when
//! the operation completes, enabling correlation between requests and completions.
//!
//! ### Callback-Based Completion Handling
//!
//! Completions are delivered via registered callbacks, not manual polling.
//! QPs are created using the Builder pattern:
//!
//! ```ignore
//! // Create QP with completion callbacks using Builder pattern
//! let qp = ctx.rc_qp_builder::<u64, u64>(&pd, &config)
//!     .sq_cq(send_cq.clone(), |cqe, entry| { /* SQ completion */ })
//!     .rq_cq(recv_cq.clone(), |cqe, entry| { /* RQ completion */ })
//!     .build()?;
//!
//! // Poll CQ to dispatch completions to registered callbacks
//! send_cq.poll();
//! send_cq.flush();
//! ```
//!
//! This design:
//! - Eliminates the need to manually track outstanding requests
//! - Ensures entries are automatically associated with completions
//! - Supports both signaled and unsignaled operations
//!
//! ### QPN to Queue Mapping
//!
//! The CQ maintains a map from QP Number (QPN) to registered queues. When a
//! CQE arrives, the CQ looks up the QPN and dispatches to the appropriate
//! callback. Note that this mapping is surjective but not injective - multiple
//! QPNs may share the same SRQ, for example.
//!
//! ## Module Overview
//!
//! - [`cq`]: Completion Queue management and CQE parsing
//! - [`qp`]: Reliable Connected (RC) Queue Pairs
//! - [`ud`]: Unreliable Datagram (UD) Queue Pairs
//! - [`dc`]: Dynamically Connected (DC) transport (DCI/DCT)
//! - [`srq`]: Shared Receive Queues
//! - [`tm_srq`]: Tag Matching SRQ for hardware-accelerated message matching
//! - [`pd`]: Protection Domains and Memory Regions
//! - [`device`]: Device and Context management
//! - [`wqe`]: Work Queue Entry structures and builders

#![allow(unsafe_op_in_unsafe_fn)]

#[macro_use]
mod barrier;

pub(crate) mod builder_common;
pub mod cq;
pub mod dc;
pub mod device;
pub mod devx;
pub mod mono_cq;
pub mod pd;
pub(crate) mod prm;
pub mod qp;
pub mod srq;
pub mod tm_srq;
pub mod transport;
pub mod types;
pub mod ud;
pub mod wqe;

#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Trait for queues that can receive completion notifications from a CQ.
///
/// Implemented by RcQp, UdQp, Dci, TmSrq, etc.
pub trait CompletionTarget {
    /// Get the QP number.
    fn qpn(&self) -> u32;

    /// Dispatch a CQE to this queue.
    ///
    /// Called by Cq::poll() when a CQE for this queue is received.
    fn dispatch_cqe(&self, cqe: cq::Cqe);
}

/// Result type for QP/DCI builder's build() method.
///
/// Wraps the built queue in `Rc<RefCell<T>>` for shared ownership with interior mutability.
pub type BuildResult<T> = std::io::Result<std::rc::Rc<std::cell::RefCell<T>>>;
