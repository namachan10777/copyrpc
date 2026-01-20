//! Transport type markers for InfiniBand vs RoCE.
//!
//! This module provides compile-time type safety for distinguishing between
//! InfiniBand and RoCE (RDMA over Converged Ethernet) transport types.
//!
//! # NOTE: RoCE support is untested (IB-only hardware environment)
//!
//! ## Usage
//!
//! ```ignore
//! use mlx5::transport::{InfiniBand, RoCE, Transport};
//!
//! // IB-specific QP
//! let qp: RcQp<InfiniBand, _, _> = ctx.create_rc_qp(&pd, &send_cq, &recv_cq, &config)?;
//! qp.connect(&IbRemoteQpInfo { qp_number: 123, psn: 0, lid: 1 }, ...)?;
//!
//! // RoCE-specific QP
//! let qp: RcQp<RoCE, _, _> = ctx.create_rc_qp(&pd, &send_cq, &recv_cq, &config)?;
//! qp.connect(&RoCERemoteQpInfo { qp_number: 123, psn: 0, grh: ... }, ...)?;
//! ```

use std::marker::PhantomData;

use crate::types::GrhAttr;

// =============================================================================
// Sealed Trait Pattern
// =============================================================================

mod sealed {
    /// Sealed trait to prevent external implementations of Transport.
    pub trait Sealed {}
}

// =============================================================================
// Transport Trait
// =============================================================================

/// Transport type marker trait.
///
/// This trait is sealed and cannot be implemented outside this crate.
/// Only `InfiniBand` and `RoCE` implement this trait.
pub trait Transport: sealed::Sealed + 'static + Copy + Default {
    /// Human-readable name for the transport type.
    const NAME: &'static str;
}

// =============================================================================
// Transport Markers
// =============================================================================

/// InfiniBand transport marker.
///
/// Use this type parameter for QPs operating over InfiniBand fabric.
/// InfiniBand uses LID (Local Identifier) for addressing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InfiniBand;

impl sealed::Sealed for InfiniBand {}
impl Transport for InfiniBand {
    const NAME: &'static str = "InfiniBand";
}

/// RoCE (RDMA over Converged Ethernet) transport marker.
///
/// Use this type parameter for QPs operating over RoCE v2.
/// RoCE uses GID (Global Identifier) and GRH for addressing.
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoCE;

impl sealed::Sealed for RoCE {}
impl Transport for RoCE {
    const NAME: &'static str = "RoCE";
}

// =============================================================================
// Remote QP Info Types
// =============================================================================

/// Remote QP information for InfiniBand transport.
///
/// Contains the addressing information needed to connect to a remote QP
/// over InfiniBand fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IbRemoteQpInfo {
    /// Remote QP number
    pub qp_number: u32,
    /// Remote packet sequence number
    pub packet_sequence_number: u32,
    /// Remote LID (Local Identifier)
    pub local_identifier: u16,
}

/// Remote QP information for RoCE transport.
///
/// Contains the addressing information needed to connect to a remote QP
/// over RoCE (RDMA over Converged Ethernet).
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoCERemoteQpInfo {
    /// Remote QP number
    pub qp_number: u32,
    /// Remote packet sequence number
    pub packet_sequence_number: u32,
    /// GRH (Global Route Header) attributes for RoCE addressing
    pub grh: GrhAttr,
}

// =============================================================================
// Remote DCT Info Types
// =============================================================================

/// Remote DCT information for InfiniBand transport.
///
/// Contains the addressing information needed to send to a remote DCT
/// over InfiniBand fabric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IbRemoteDctInfo {
    /// Remote DCT number
    pub dct_number: u32,
    /// DC key for access control
    pub dc_key: u64,
    /// Remote LID (Local Identifier)
    pub local_identifier: u16,
}

/// Remote DCT information for RoCE transport.
///
/// Contains the addressing information needed to send to a remote DCT
/// over RoCE (RDMA over Converged Ethernet).
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoCERemoteDctInfo {
    /// Remote DCT number
    pub dct_number: u32,
    /// DC key for access control
    pub dc_key: u64,
    /// GRH (Global Route Header) attributes for RoCE addressing
    pub grh: GrhAttr,
}

// =============================================================================
// Helper Types
// =============================================================================

/// Type-level marker for transport-specific operations.
///
/// Used internally to associate transport type with QP implementations.
#[derive(Debug, Clone, Copy)]
pub struct TransportMarker<T: Transport> {
    _marker: PhantomData<T>,
}

impl<T: Transport> Default for TransportMarker<T> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}
