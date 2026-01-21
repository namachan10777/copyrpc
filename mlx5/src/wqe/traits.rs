//! WQE Builder trait definitions.
//!
//! # Design Principles
//!
//! 1. **Direct writes**: No intermediate data, write directly to WQE buffers
//! 2. **Inlining**: All methods marked `#[inline]` for performance
//! 3. **Trait-free builders**: Only concrete impl blocks, no trait-based abstractions
//! 4. **Type-state for data**: `NoData`/`HasData` markers ensure compile-time safety
//!
//! # API Design
//!
//! Required fields are passed to verb methods directly:
//!
//! ```ignore
//! // IB RC QP
//! qp.sq_wqe()
//!     .send(flags)
//!     .sge(addr, len, lkey)
//!     .finish_signaled(entry)?;
//!
//! // WRITE: remote_addr, rkey passed to write()
//! qp.sq_wqe()
//!     .write(flags, remote_addr, rkey)
//!     .sge(addr, len, lkey)
//!     .finish_unsignaled()?;
//!
//! // CAS: all required fields passed to cas()
//! qp.sq_wqe()
//!     .cas(flags, remote_addr, rkey, swap, compare)
//!     .sge(addr, len, lkey)
//!     .finish_signaled(entry)?;
//! ```
//!
//! # finish_unsignaled vs finish_signaled
//!
//! - **Ordered + SQ**: `finish_unsignaled()` and `finish_signaled(entry)` both available
//! - **Unordered + SQ**: `finish_signaled(entry)` only
//! - **RQ**: `finish()` only (always signaled)
//!
//! # NoData/HasData State
//!
//! Builders start in `NoData` state. After calling `sge()` or `inline()`,
//! they transition to `HasData` state. Only `HasData` state allows `finish_*()` methods.

use crate::wqe::WqeFlags;

// =============================================================================
// Transport Type Tags
// =============================================================================

/// Transport type tag for InfiniBand.
///
/// InfiniBand uses LID-based addressing. RC QPs don't need AV for each WQE.
#[derive(Debug, Clone, Copy)]
pub struct InfiniBand;

/// Transport type tag for RoCE (RDMA over Converged Ethernet).
///
/// RoCE uses GID-based addressing and requires GRH in the AV.
#[derive(Debug, Clone, Copy)]
pub struct RoCE;

// =============================================================================
// Address Vector Trait
// =============================================================================

use crate::types::GrhAttr;
use crate::wqe::AddressVector;

/// Trait for Address Vector types.
///
/// This allows unified handling of IB (no AV) and RoCE (GRH-based AV).
pub trait Av: Copy {
    /// Size of the AV segment in bytes.
    const SIZE: usize;
    /// Number of data segments (SIZE / 16).
    const DS_COUNT: u8 = (Self::SIZE / 16) as u8;

    /// Write the AV segment to the WQE buffer.
    ///
    /// # Safety
    /// Caller must ensure ptr points to a valid WQE buffer with enough space.
    unsafe fn write_av(self, ptr: *mut u8);
}

/// No Address Vector (for InfiniBand RC QP).
#[derive(Debug, Clone, Copy)]
pub struct NoAv;

impl Av for NoAv {
    const SIZE: usize = 0;
    const DS_COUNT: u8 = 0;

    #[inline]
    unsafe fn write_av(self, _ptr: *mut u8) {
        // No AV segment for InfiniBand
    }
}

impl Av for &GrhAttr {
    const SIZE: usize = AddressVector::SIZE;
    const DS_COUNT: u8 = (AddressVector::SIZE / 16) as u8;

    #[inline]
    unsafe fn write_av(self, ptr: *mut u8) {
        // For RC QP RoCE, dc_key and dctn are not used (set to 0)
        AddressVector::write_roce(ptr, 0, 0, self);
    }
}

// =============================================================================
// Flags
// =============================================================================

/// Transmission flags for WQE operations.
///
/// This is an alias for `WqeFlags`. The `COMPLETION` flag is automatically
/// set when using `finish_signaled(entry)`, so users typically don't need to set it.
pub type TxFlags = WqeFlags;

// =============================================================================
// Data State Markers
// =============================================================================

/// Marker type indicating no data segment has been added yet.
///
/// Builders in this state cannot call `finish_*()` methods.
#[derive(Debug, Clone, Copy)]
pub struct NoData;

// NOTE: HasData is defined in mod.rs as a WqeState, re-exported here for consistency

// =============================================================================
// Receive Queue Traits
// =============================================================================

/// QP RQ builder.
///
/// All RQ WQEs generate completions, so entry is always required at construction.
#[must_use = "WQE builder must be finished"]
pub trait RqWqeBuilder<'a, Entry>: Sized {
    /// Add a scatter/gather entry for receive buffer.
    fn sge(self, addr: u64, len: u32, lkey: u32) -> Self;

    /// Finish the WQE construction.
    fn finish(self);
}

// =============================================================================
// TM-SRQ Traits (Tag Matching)
// =============================================================================

use crate::wqe::WqeHandle;

/// TM-SRQ Command Queue entry point.
#[must_use = "WQE builder must be finished"]
pub trait TmCmdWqeBuilder<'a, Entry>: Sized {
    /// Start building a TAG_ADD WQE.
    fn tag_add(self, index: u16, tag: u64) -> impl TmTagAddWqeBuilder<'a, Entry> + 'a;

    /// Start building a TAG_DEL WQE.
    fn tag_del(self, index: u16) -> impl TmTagDelWqeBuilder<'a, Entry> + 'a;
}

/// Tag Add WQE builder.
#[must_use = "WQE builder must be finished"]
pub trait TmTagAddWqeBuilder<'a, Entry>: Sized {
    /// Add a scatter/gather entry.
    fn sge(self, addr: u64, len: u32, lkey: u32) -> Self;

    /// Add inline data.
    fn inline(self, data: &[u8]) -> Self;

    /// Finish the WQE construction (unsignaled).
    fn finish_unsignaled(self) -> WqeHandle;

    /// Finish the WQE construction with completion signaling.
    fn finish_signaled(self, entry: Entry) -> WqeHandle;
}

/// Tag Del WQE builder.
#[must_use = "WQE builder must be finished"]
pub trait TmTagDelWqeBuilder<'a, Entry>: Sized {
    /// Finish the WQE construction (unsignaled).
    fn finish_unsignaled(self) -> WqeHandle;

    /// Finish the WQE construction with completion signaling.
    fn finish_signaled(self, entry: Entry) -> WqeHandle;
}

// =============================================================================
// SRQ RQ Traits
// =============================================================================

/// SRQ RQ builder - same interface as QP RQ.
#[must_use = "WQE builder must be finished"]
pub trait SrqRqWqeBuilder<'a, Entry>: RqWqeBuilder<'a, Entry> {}
