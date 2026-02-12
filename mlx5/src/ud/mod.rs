//! UD (Unreliable Datagram) transport.
//!
//! UD provides connectionless datagram service. Each message is independently
//! addressed using an Address Handle (AH). Messages are limited to a single MTU.
//!
//! Key characteristics:
//! - Connectionless: No QP state machine, no connection setup
//! - Unreliable: No retransmission, no guaranteed delivery
//! - Unordered delivery: No network-level message delivery ordering between different senders
//!   (HCA completion ordering within a single QP is guaranteed, same as RC)
//! - One-to-many: Single QP can send to multiple destinations

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData};

use crate::BuildResult;
use crate::CompletionTarget;
use crate::builder_common::{MaybeMonoCqRegister, register_with_cqs};
use crate::cq::{Cq, Cqe};
use crate::device::Context;
use crate::devx::{DevxObj, DevxUmem};
use crate::pd::{AddressHandle, Pd};
use crate::prm;
use crate::qp::AlignedBuf;
use crate::srq::Srq;
use crate::transport::{InfiniBand, RoCE};
use crate::wqe::{
    DATA_SEG_SIZE, OrderedWqeTable, SubmissionError, WQEBB_SIZE,
    emit::{SendQueueState, UdEmitContext, bf_finish_rq},
    write_data_seg,
};

// =============================================================================
// UD Configuration
// =============================================================================

/// UD QP configuration.
#[derive(Debug, Clone)]
pub struct UdQpConfig {
    /// Maximum number of outstanding send WRs.
    pub max_send_wr: u32,
    /// Maximum number of outstanding receive WRs.
    pub max_recv_wr: u32,
    /// Maximum number of SGEs per send WR.
    pub max_send_sge: u32,
    /// Maximum number of SGEs per receive WR.
    pub max_recv_sge: u32,
    /// Maximum inline data size.
    pub max_inline_data: u32,
    /// Q_Key for this QP.
    pub qkey: u32,
}

impl Default for UdQpConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_recv_wr: 256,
            max_send_sge: 4,
            max_recv_sge: 1,
            max_inline_data: 64,
            qkey: 0x11111111,
        }
    }
}

/// UD QP buffer layout computed from config.
///
/// PRM-mandated layout: RQ at offset 0, SQ at offset rq_size.
pub(crate) struct UdBufferLayout {
    pub(crate) total_size: usize,
    pub(crate) sq_size: usize,
    pub(crate) rq_size: usize,
    pub(crate) rq_stride: usize,
    pub(crate) dbrec_offset: usize,
    pub(crate) log_sq_size: u32,
    pub(crate) log_rq_size: u32,
    pub(crate) log_rq_stride: u32,
}

impl UdQpConfig {
    /// Compute buffer layout for DevX UD QP creation.
    pub(crate) fn buffer_layout(&self, use_srq: bool) -> UdBufferLayout {
        let log_sq_size = if self.max_send_wr == 0 {
            0
        } else {
            (self.max_send_wr as usize).next_power_of_two().trailing_zeros()
        };
        let sq_size = (1usize << log_sq_size) * WQEBB_SIZE;

        let (rq_size, rq_stride, log_rq_size, log_rq_stride) = if use_srq {
            (0, 0, 0, 0)
        } else {
            let sge_count = self.max_recv_sge as usize + 1; // +1 for null terminator
            let stride = (sge_count * DATA_SEG_SIZE).next_power_of_two();
            let log_stride = (stride / 16).trailing_zeros();
            let log_rq = if self.max_recv_wr == 0 {
                0
            } else {
                (self.max_recv_wr as usize).next_power_of_two().trailing_zeros()
            };
            let rq_sz = (1usize << log_rq) * stride;
            (rq_sz, stride, log_rq, log_stride)
        };

        let dbrec_offset = (sq_size + rq_size + 63) & !63;
        let total_size = (dbrec_offset + 64 + 4095) & !4095;

        UdBufferLayout {
            total_size,
            sq_size,
            rq_size,
            rq_stride,
            dbrec_offset,
            log_sq_size,
            log_rq_size,
            log_rq_stride,
        }
    }
}

/// Resources created by DevX UD QP creation.
struct DevxUdQpResources {
    devx_obj: DevxObj,
    umem: DevxUmem,
    wq_buf: AlignedBuf,
    qpn: u32,
    layout: UdBufferLayout,
    bf_reg: *mut u8,
    bf_size: u32,
    dbrec: *mut u32,
    sq_buf: *mut u8,
    rq_buf: *mut u8,
}

/// Create a UD QP via DevX PRM CREATE_QP command.
///
/// # Safety
/// The caller must ensure `ctx`, `pd` are valid and the context was opened with DevX.
unsafe fn create_devx_ud_qp(
    ctx: &Context,
    pd: &Pd,
    send_cqn: u32,
    recv_cqn: u32,
    config: &UdQpConfig,
    srqn: Option<u32>,
) -> io::Result<DevxUdQpResources> {
    let layout = config.buffer_layout(srqn.is_some());

    let wq_buf = AlignedBuf::new(layout.total_size);
    let umem = ctx.register_umem(wq_buf.as_ptr(), layout.total_size, 7)?;
    let uar = ctx.devx_uar()?;

    // Build CREATE_QP PRM command (no PAS needed with UMEM mode)
    let cmd_in_size = prm::CREATE_QP_IN_BASE_SIZE;
    let mut cmd_in = vec![0u8; cmd_in_size];
    let mut cmd_out = vec![0u8; prm::CREATE_QP_OUT_SIZE];

    let umem_id = umem.umem_id();

    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_OPCODE.0,
        prm::CREATE_QP_IN_OPCODE.1,
        prm::MLX5_CMD_OP_CREATE_QP,
    );

    // WQ UMEM fields (work queue buffer memory)
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_VALID.0,
        prm::CREATE_QP_IN_WQ_UMEM_VALID.1,
        1,
    );
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_ID.0,
        prm::CREATE_QP_IN_WQ_UMEM_ID.1,
        umem_id,
    );
    prm::prm_set64(&mut cmd_in, prm::CREATE_QP_IN_WQ_UMEM_OFFSET, 0);

    let q = prm::CREATE_QP_IN_QPC_OFF;
    prm::prm_set(&mut cmd_in, q + prm::QPC_ST.0, prm::QPC_ST.1, prm::QP_ST_UD);
    prm::prm_set(&mut cmd_in, q + prm::QPC_PD.0, prm::QPC_PD.1, pd.pdn());
    prm::prm_set(&mut cmd_in, q + prm::QPC_UAR_PAGE.0, prm::QPC_UAR_PAGE.1, uar.page_id());
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_SQ_SIZE.0, prm::QPC_LOG_SQ_SIZE.1, layout.log_sq_size);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RQ_SIZE.0, prm::QPC_LOG_RQ_SIZE.1, layout.log_rq_size);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RQ_STRIDE.0, prm::QPC_LOG_RQ_STRIDE.1, layout.log_rq_stride);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_MSG_MAX.0, prm::QPC_LOG_MSG_MAX.1, 30);
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_SND.0, prm::QPC_CQN_SND.1, send_cqn);
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_RCV.0, prm::QPC_CQN_RCV.1, recv_cqn);

    // Set Q_KEY for UD
    prm::prm_set(&mut cmd_in, q + prm::QPC_Q_KEY.0, prm::QPC_Q_KEY.1, config.qkey);

    // DBR UMEM fields (doorbell record memory)
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_VALID.0, prm::QPC_DBR_UMEM_VALID.1, 1);
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_ID.0, prm::QPC_DBR_UMEM_ID.1, umem_id);
    prm::prm_set64(
        &mut cmd_in,
        q + prm::QPC_DBR_ADDR,
        layout.dbrec_offset as u64,
    );

    // SRQ
    if let Some(srqn) = srqn {
        prm::prm_set(&mut cmd_in, q + prm::QPC_RQ_TYPE.0, prm::QPC_RQ_TYPE.1, 1);
        prm::prm_set(&mut cmd_in, q + prm::QPC_SRQN_RMPN_XRQN.0, prm::QPC_SRQN_RMPN_XRQN.1, srqn);
    }

    let devx_obj = ctx.devx_obj_create(&cmd_in, &mut cmd_out)?;
    let qpn = prm::prm_get(&cmd_out, prm::CREATE_QP_OUT_QPN.0, prm::CREATE_QP_OUT_QPN.1);

    // PRM layout: RQ at offset 0, SQ at offset rq_size
    let rq_buf = wq_buf.as_ptr();
    let sq_buf = wq_buf.as_ptr().add(layout.rq_size);
    let dbrec = wq_buf.as_ptr().add(layout.dbrec_offset) as *mut u32;

    Ok(DevxUdQpResources {
        devx_obj,
        umem,
        wq_buf,
        qpn,
        layout,
        bf_reg: uar.reg_addr(),
        bf_size: 256,
        dbrec,
        sq_buf,
        rq_buf,
    })
}

/// UD QP state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdQpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Error,
}

// =============================================================================
// Send Queue State
// =============================================================================

// SendQueueState is now the unified SendQueueState from crate::wqe::emit.
// emit_ctx() is defined at QP-level (on UdQpIb/UdQpRoCE).

// =============================================================================
// Receive Queue State
// =============================================================================

/// MLX5 invalid lkey value used to mark end of SGE list.
#[allow(dead_code)]
const MLX5_INVALID_LKEY: u32 = 0x100;

/// Receive Queue state for UD QP.
///
/// Generic over `Entry`, the entry type stored in the WQE table.
/// Unlike SQ, all RQ WQEs generate completions (all signaled).
///
/// Uses `Cell<Option<Entry>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows.
pub(super) struct UdRecvQueueState<Entry> {
    pub(super) buf: *mut u8,
    pub(super) wqe_cnt: u16,
    pub(super) stride: u32,
    pub(super) pi: Cell<u16>,
    pub(super) ci: Cell<u16>,
    pub(super) dbrec: *mut u32,
    /// BlueFlame register pointer
    bf_reg: *mut u8,
    /// BlueFlame size
    bf_size: u32,
    /// Current BlueFlame offset (alternates between 0 and bf_size)
    bf_offset: Cell<u32>,
    /// Pointer to the first pending WQE (for ring_doorbell_bf)
    pending_start_ptr: Cell<Option<*mut u8>>,
    /// Number of pending WQEs (for ring_doorbell_bf)
    pending_wqe_count: Cell<u32>,
    /// Entry table (all WQEs are signaled, uses Cell for interior mutability)
    pub(super) table: Box<[Cell<Option<Entry>>]>,
}

impl<Entry> UdRecvQueueState<Entry> {
    pub(super) fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * (self.stride as usize);
        unsafe { self.buf.add(offset) }
    }

    /// Process a receive completion.
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        self.ci.set(wqe_idx.wrapping_add(1));
        let idx = (wqe_idx as usize) & ((self.wqe_cnt - 1) as usize);
        self.table[idx].take()
    }

    /// Get the number of available WQE slots.
    pub(super) fn available(&self) -> u32 {
        (self.wqe_cnt as u32) - (self.pi.get().wrapping_sub(self.ci.get()) as u32)
    }

    /// Ring the RQ doorbell (DBREC-only, no BF write).
    ///
    /// Updates DBREC[0] (RQ doorbell record) to notify the NIC of new RQ WQEs.
    /// RQ does not use BlueFlame writes (BF is SQ-only).
    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            // MLX5 PRM: dbrec[0] = RCV counter
            std::ptr::write_volatile(self.dbrec, (self.pi.get() as u32).to_be());
        }
        udma_to_device_barrier!();

        // Reset pending tracking
        self.pending_wqe_count.set(0);
        self.pending_start_ptr.set(None);
    }

    /// Ring the doorbell with BlueFlame write of all pending WQEs.
    ///
    /// Copies pending WQEs to BlueFlame register for higher throughput.
    /// Falls back to minimum doorbell if no pending WQEs.
    fn ring_doorbell_bf(&self) {
        let wqe_count = self.pending_wqe_count.get();
        if wqe_count == 0 {
            return;
        }

        mmio_flush_writes!();
        unsafe {
            // MLX5 PRM: dbrec[0] = RCV counter
            std::ptr::write_volatile(self.dbrec, (self.pi.get() as u32).to_be());
        }
        udma_to_device_barrier!();

        // Copy pending WQEs to BlueFlame register
        let bf_offset = self.bf_offset.get();
        let bf = unsafe { self.bf_reg.add(bf_offset as usize) };
        let copy_size = (wqe_count as usize * self.stride as usize).min(256);

        if let Some(start_ptr) = self.pending_start_ptr.get() {
            // Copy in 64-byte chunks
            let mut src = start_ptr;
            let mut dst = bf;
            let mut remaining = copy_size;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }
        }

        mmio_flush_writes!();
        self.bf_offset.set(bf_offset ^ self.bf_size);

        // Reset pending tracking
        self.pending_wqe_count.set(0);
        self.pending_start_ptr.set(None);
    }
}

// =============================================================================
// UD Receive Queue Type Markers
// =============================================================================

/// Marker type for UD QP with owned Receive Queue.
///
/// When a QP uses `UdOwnedRq`, it has its own dedicated receive queue and
/// the RQ-related methods (`post_recv`, `ring_rq_doorbell`, `blueflame_rq_batch`)
/// are available.
pub struct UdOwnedRq<Entry>(Option<UdRecvQueueState<Entry>>);

impl<Entry> UdOwnedRq<Entry> {
    pub(crate) fn new(rq: Option<UdRecvQueueState<Entry>>) -> Self {
        Self(rq)
    }

    pub(crate) fn as_ref(&self) -> Option<&UdRecvQueueState<Entry>> {
        self.0.as_ref()
    }
}

/// Marker type for UD QP with Shared Receive Queue (SRQ).
///
/// When a QP uses `UdSharedRq`, receive operations are handled through the SRQ
/// and the QP's own RQ-related methods are not available. Instead, use
/// `srq()` to access the underlying SRQ.
pub struct UdSharedRq<Entry>(Srq<Entry>);

impl<Entry> UdSharedRq<Entry> {
    pub(crate) fn new(srq: Srq<Entry>) -> Self {
        Self(srq)
    }

    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<Entry> {
        &self.0
    }
}

// =============================================================================
// UD Receive WQE Builder
// =============================================================================

/// Zero-copy WQE builder for UD receive operations.
///
/// Writes segments directly to the RQ buffer without intermediate copies.
pub struct UdRecvWqeBuilder<'a, Entry> {
    rq: &'a UdRecvQueueState<Entry>,
    entry: Entry,
    wqe_idx: u16,
}

impl<'a, Entry> UdRecvWqeBuilder<'a, Entry> {
    /// Add a data segment (SGE) for the receive buffer.
    ///
    /// # Safety
    /// The caller must ensure the buffer is registered and valid.
    pub fn sge(self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            let wqe_ptr = self.rq.get_wqe_ptr(self.wqe_idx);
            write_data_seg(wqe_ptr, len, lkey, addr);
        }
        self
    }

    /// Finish the receive WQE construction.
    ///
    /// Stores the entry in the table and advances the producer index.
    /// Call `ring_rq_doorbell()` after posting one or more WQEs to notify the HCA.
    pub fn finish(self) {
        let idx = (self.wqe_idx as usize) & ((self.rq.wqe_cnt - 1) as usize);
        self.rq.table[idx].set(Some(self.entry));
        self.rq.pi.set(self.rq.pi.get().wrapping_add(1));
    }
}

// =============================================================================
// UD Address Segment
// =============================================================================

/// UD Address Segment (48 bytes).
///
/// This segment specifies the destination for UD SEND operations.
/// It includes the AH index and remote QPN/Q_Key.
pub struct UdAddressSeg;

impl UdAddressSeg {
    /// Size of the UD address segment in bytes.
    /// mlx5_wqe_datagram_seg = mlx5_wqe_av = 48 bytes (no reserved header)
    pub const SIZE: usize = 48;

    /// Write the UD address segment from an Address Handle.
    ///
    /// # Safety
    /// The pointer must point to at least 48 bytes of writable memory.
    #[inline]
    pub unsafe fn write(ptr: *mut u8, ah: &AddressHandle, qkey: u32) {
        // mlx5_wqe_av format (48 bytes):
        // [0-7]:   key union (qkey at [0-3], reserved at [4-7])
        // [8-11]:  dqp_dct (destination QPN in [23:0] | MLX5_EXTENDED_UD_AV)
        // [12]:    stat_rate_sl (SL in [7:4], static_rate in [3:0])
        // [13]:    fl_mlid (force_lb in [7], source_lid[6:0])
        // [14-15]: rlid (remote LID, big-endian)
        // [16-19]: reserved0
        // [20-25]: rmac (remote MAC)
        // [26]:    tclass
        // [27]:    hop_limit
        // [28-31]: grh_gid_fl
        // [32-47]: rgid (remote GID)

        // Q_Key at offset 0
        std::ptr::write_volatile(ptr as *mut u32, qkey.to_be());
        // Reserved at offset 4
        std::ptr::write_volatile(ptr.add(4) as *mut u32, 0);

        // Remote QPN at offset 8 with MLX5_EXTENDED_UD_AV flag (bit 31=1)
        const MLX5_EXTENDED_UD_AV: u32 = 0x8000_0000;
        let dqp = (ah.qpn() & 0x00FF_FFFF) | MLX5_EXTENDED_UD_AV;
        std::ptr::write_volatile(ptr.add(8) as *mut u32, dqp.to_be());

        // stat_rate_sl at offset 12 (SL=0, rate=0)
        std::ptr::write_volatile(ptr.add(12), 0u8);
        // fl_mlid at offset 13 (force_lb=0, mlid=0)
        std::ptr::write_volatile(ptr.add(13), 0u8);

        // Remote LID at offset 14
        std::ptr::write_volatile(ptr.add(14) as *mut u16, ah.dlid().to_be());

        // Clear remaining fields (offset 16-47)
        std::ptr::write_bytes(ptr.add(16), 0, 32);
    }

    /// Write the UD address segment using raw values.
    ///
    /// This is used when you have the destination info directly.
    ///
    /// # Safety
    /// The pointer must point to at least 48 bytes of writable memory.
    #[inline]
    pub unsafe fn write_raw(ptr: *mut u8, remote_qpn: u32, qkey: u32, dlid: u16) {
        // Q_Key at offset 0
        std::ptr::write_volatile(ptr as *mut u32, qkey.to_be());
        // Reserved at offset 4
        std::ptr::write_volatile(ptr.add(4) as *mut u32, 0);

        // Remote QPN at offset 8 with MLX5_EXTENDED_UD_AV flag (bit 31=1)
        const MLX5_EXTENDED_UD_AV: u32 = 0x8000_0000;
        let dqp = (remote_qpn & 0x00FF_FFFF) | MLX5_EXTENDED_UD_AV;
        std::ptr::write_volatile(ptr.add(8) as *mut u32, dqp.to_be());

        // stat_rate_sl at offset 12
        std::ptr::write_volatile(ptr.add(12), 0u8);
        // fl_mlid at offset 13
        std::ptr::write_volatile(ptr.add(13), 0u8);

        // Remote LID at offset 14
        std::ptr::write_volatile(ptr.add(14) as *mut u16, dlid.to_be());

        // Clear remaining fields (offset 16-47)
        std::ptr::write_bytes(ptr.add(16), 0, 32);
    }
}

// =============================================================================
// UD QP
// =============================================================================

/// UD QP with owned RQ (InfiniBand).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = UdQp<
    SqEntry,
    RqEntry,
    InfiniBand,
    OrderedWqeTable<SqEntry>,
    UdOwnedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// UD QP with SRQ (InfiniBand).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type UdQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = UdQp<
    SqEntry,
    RqEntry,
    InfiniBand,
    OrderedWqeTable<SqEntry>,
    UdSharedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// UD QP with owned RQ (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
pub type UdQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = UdQp<
    SqEntry,
    RqEntry,
    RoCE,
    OrderedWqeTable<SqEntry>,
    UdOwnedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// UD QP with SRQ (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
pub type UdQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = UdQp<
    SqEntry,
    RqEntry,
    RoCE,
    OrderedWqeTable<SqEntry>,
    UdSharedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// Type alias for UD QP with MonoCq (no callback stored, InfiniBand).
///
/// When using MonoCq, callbacks are stored in the MonoCq, not in the UdQp.
/// Both SQ and RQ use the same Entry type with MonoCq.
pub type UdQpForMonoCq<Entry> =
    UdQp<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, UdOwnedRq<Entry>, (), ()>;

/// Type alias for UD QP with MonoCq (no callback stored, RoCE).
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
pub type UdQpForMonoCqRoCE<Entry> =
    UdQp<Entry, Entry, RoCE, OrderedWqeTable<Entry>, UdOwnedRq<Entry>, (), ()>;

/// Type alias for UD QP with SQ callback and RQ MonoCq (hybrid mode, InfiniBand).
///
/// This allows using a normal CQ with callback for SQ completions while using
/// MonoCq for RQ completions (for direct dispatch).
pub type UdQpForMonoCqWithSqCb<Entry, OnSq> =
    UdQp<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, UdOwnedRq<Entry>, OnSq, ()>;

/// UD (Unreliable Datagram) Queue Pair.
///
/// Provides connectionless datagram service. Each send operation requires
/// specifying the destination via an Address Handle.
///
/// Type parameter `SqEntry` is the entry type stored in SQ WQE table.
/// Type parameter `RqEntry` is the entry type stored in RQ WQE table.
/// Type parameter `Transport` is the transport type tag (`InfiniBand` or `RoCE`).
/// Type parameter `TableType` determines WQE table behavior.
/// Type parameter `Rq` is the receive queue type (`UdOwnedRq<RqEntry>` or `UdSharedRq<RqEntry>`).
/// Type parameter `OnSqComplete` is the SQ completion callback type.
/// Type parameter `OnRqComplete` is the RQ completion callback type.
pub struct UdQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    _devx_obj: DevxObj,
    _umem: DevxUmem,
    _wq_buf: AlignedBuf,
    qpn: u32,
    state: Cell<UdQpState>,
    qkey: u32,
    sq: Option<SendQueueState<SqEntry, TableType>>,
    rq: Rq,
    sq_callback: OnSqComplete,
    rq_callback: OnRqComplete,
    /// Weak reference to the send CQ for unregistration on drop.
    send_cq: Weak<Cq>,
    /// Weak reference to the recv CQ for unregistration on drop.
    recv_cq: Weak<Cq>,
    /// Keep the PD alive while this QP exists.
    _pd: Pd,
    /// Phantom for Transport and RqEntry types.
    _marker: std::marker::PhantomData<(Transport, RqEntry)>,
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> Drop
    for UdQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
{
    fn drop(&mut self) {
        let qpn = self.qpn();
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        if let Some(cq) = self.recv_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        // DevxObj handles destruction automatically
    }
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
    UdQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
{
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        self.qpn
    }

    /// Get the current QP state.
    pub fn state(&self) -> UdQpState {
        self.state.get()
    }

    /// Get the Q_Key.
    pub fn qkey(&self) -> u32 {
        self.qkey
    }

    /// Transition UD QP from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, pkey_index: u16) -> io::Result<()> {
        if self.state.get() != UdQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RESET state",
            ));
        }

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_RST2INIT_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        prm::prm_set(&mut cmd_in, q + prm::QPC_PM_STATE.0, prm::QPC_PM_STATE.1, 2); // migrated

        // For UD: set Q_KEY
        prm::prm_set(&mut cmd_in, q + prm::QPC_Q_KEY.0, prm::QPC_Q_KEY.1, self.qkey);

        // ADS: port
        let ads = q + prm::QPC_PRI_ADS;
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_PKEY_INDEX.0, prm::ADS_PKEY_INDEX.1, pkey_index as u32);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RST2INIT_QP")?;
        self.state.set(UdQpState::Init);
        Ok(())
    }

    /// Transition UD QP from INIT to RTR.
    pub fn modify_to_rtr(&mut self) -> io::Result<()> {
        if self.state.get() != UdQpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in INIT state",
            ));
        }

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_INIT2RTR_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "INIT2RTR_QP")?;
        self.state.set(UdQpState::Rtr);
        Ok(())
    }

    /// Transition UD QP from RTR to RTS.
    pub fn modify_to_rts(&mut self, sq_psn: u32) -> io::Result<()> {
        if self.state.get() != UdQpState::Rtr {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RTR state",
            ));
        }

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_RTR2RTS_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        prm::prm_set(&mut cmd_in, q + prm::QPC_NEXT_SEND_PSN.0, prm::QPC_NEXT_SEND_PSN.1, sq_psn);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RTR2RTS_QP")?;
        self.state.set(UdQpState::Rts);
        Ok(())
    }

    fn sq(&self) -> io::Result<&SendQueueState<SqEntry, TableType>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))
    }
}

// =============================================================================
// UD RQ BlueFlame Batch Builder
// =============================================================================

use crate::wqe::BLUEFLAME_BUFFER_SIZE;

/// RQ WQE size in bytes (single DataSeg).
const RQ_WQE_SIZE: usize = DATA_SEG_SIZE;

/// BlueFlame WQE batch builder for UD RQ.
///
/// Allows posting multiple receive WQEs efficiently using BlueFlame MMIO.
/// WQEs are accumulated in an internal buffer and submitted together
/// via BlueFlame doorbell when `finish()` is called.
///
/// RQ WQE size is 16 bytes (single DataSeg), so up to 16 WQEs can fit in a batch.
///
/// # Example
/// ```ignore
/// let mut batch = qp.blueflame_rq_batch()?;
/// batch.post(entry1, addr1, len1, lkey1)?;
/// batch.post(entry2, addr2, len2, lkey2)?;
/// batch.finish();
/// ```
pub struct UdRqBlueflameWqeBatch<'a, Entry> {
    rq: &'a UdRecvQueueState<Entry>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
    wqe_count: u16,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &'a Cell<u32>,
}

impl<'a, Entry> UdRqBlueflameWqeBatch<'a, Entry> {
    /// Create a new UD RQ BlueFlame batch builder.
    pub(crate) fn new(
        rq: &'a UdRecvQueueState<Entry>,
        bf_reg: *mut u8,
        bf_size: u32,
        bf_offset: &'a Cell<u32>,
    ) -> Self {
        Self {
            rq,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
            wqe_count: 0,
            bf_reg,
            bf_size,
            bf_offset,
        }
    }

    /// Post a receive WQE to the batch.
    ///
    /// # Arguments
    /// * `entry` - User entry to associate with this receive operation
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    ///
    /// # Errors
    /// Returns `RqFull` if the receive queue doesn't have enough space.
    /// Returns `BlueflameOverflow` if the batch buffer is full.
    #[inline]
    pub fn post(
        &mut self,
        entry: Entry,
        addr: u64,
        len: u32,
        lkey: u32,
    ) -> Result<(), SubmissionError> {
        if self.rq.available() <= self.wqe_count as u32 {
            return Err(SubmissionError::RqFull);
        }
        if self.offset + RQ_WQE_SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }

        // Write WQE to RQ buffer
        let wqe_idx = self.rq.pi.get().wrapping_add(self.wqe_count);
        unsafe {
            let wqe_ptr = self.rq.get_wqe_ptr(wqe_idx);
            write_data_seg(wqe_ptr, len, lkey, addr);

            // Also write to batch buffer for BlueFlame copy
            write_data_seg(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }

        // Store entry in table
        let idx = (wqe_idx as usize) & ((self.rq.wqe_cnt - 1) as usize);
        self.rq.table[idx].set(Some(entry));

        self.offset += RQ_WQE_SIZE;
        self.wqe_count += 1;
        Ok(())
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    ///
    /// This method updates the producer index, writes to the doorbell record,
    /// and copies the WQEs to the BlueFlame register.
    #[inline]
    pub fn finish(self) {
        if self.wqe_count == 0 {
            return; // No WQEs to submit
        }

        // Advance RQ producer index
        self.rq
            .pi
            .set(self.rq.pi.get().wrapping_add(self.wqe_count));

        unsafe {
            bf_finish_rq(
                self.rq.dbrec,
                self.rq.pi.get() as u32,
                self.bf_reg,
                self.bf_size,
                &self.bf_offset,
                &self.buffer,
                self.offset,
            );
        }
    }
}

// =============================================================================
// OwnedRq-specific methods
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete>
    UdQp<SqEntry, RqEntry, Transport, TableType, UdOwnedRq<RqEntry>, OnSqComplete, OnRqComplete>
{
    fn rq(&self) -> io::Result<&UdRecvQueueState<RqEntry>> {
        self.rq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))
    }

    /// Ring the RQ doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes to BlueFlame register.
    /// The NIC fetches remaining WQE data via DMA.
    pub fn ring_rq_doorbell(&self) {
        if let Some(rq) = self.rq.as_ref() {
            rq.ring_doorbell();
        }
    }

    /// Ring the RQ doorbell with BlueFlame write of all pending WQEs.
    ///
    /// Copies pending WQEs to BlueFlame register for higher throughput.
    /// Up to 256 bytes of WQE data can be copied in a single doorbell.
    pub fn ring_rq_doorbell_bf(&self) {
        if let Some(rq) = self.rq.as_ref() {
            rq.ring_doorbell_bf();
        }
    }
}

// =============================================================================
// SharedRq-specific methods
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete>
    UdQp<SqEntry, RqEntry, Transport, TableType, UdSharedRq<RqEntry>, OnSqComplete, OnRqComplete>
{
    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        self.rq.srq()
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
    UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
{
    /// Activate UD QP (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, sq_psn: u32) -> io::Result<()> {
        self.modify_to_init(port, 0)?;
        self.modify_to_rtr()?;
        self.modify_to_rts(sq_psn)?;
        Ok(())
    }

    /// Get an emit context for macro-based WQE emission.
    #[doc(hidden)]
    pub fn emit_ctx(&self) -> io::Result<UdEmitContext<'_, SqEntry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(UdEmitContext {
            buf: sq.buf,
            wqe_cnt: sq.wqe_cnt,
            sqn: sq.sqn,
            pi: &sq.pi,
            ci: &sq.ci,
            last_wqe: &sq.last_wqe,
            table: &sq.table,
        })
    }

    /// Post a receive WQE with a single scatter/gather entry.
    ///
    /// # Arguments
    /// * `entry` - User entry to associate with this receive operation
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    ///
    /// # Example
    /// ```ignore
    /// qp.post_recv(entry, addr, len, lkey)?;
    /// qp.ring_rq_doorbell();
    /// ```
    #[inline]
    pub fn post_recv(&self, entry: RqEntry, addr: u64, len: u32, lkey: u32) -> io::Result<()> {
        let rq = self.rq()?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }
        let wqe_idx = rq.pi.get();
        let wqe_ptr = rq.get_wqe_ptr(wqe_idx);
        unsafe {
            write_data_seg(wqe_ptr, len, lkey, addr);
        }
        let idx = (wqe_idx as usize) & ((rq.wqe_cnt - 1) as usize);
        rq.table[idx].set(Some(entry));
        rq.pi.set(rq.pi.get().wrapping_add(1));

        // Update pending tracking for ring_doorbell_bf
        if rq.pending_start_ptr.get().is_none() {
            rq.pending_start_ptr.set(Some(wqe_ptr));
        }
        rq.pending_wqe_count.set(rq.pending_wqe_count.get() + 1);

        Ok(())
    }

    /// Get a BlueFlame batch builder for low-latency RQ WQE submission.
    ///
    /// Multiple receive WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// RQ WQE size is 16 bytes (single DataSeg), so up to 16 WQEs can fit in a batch.
    ///
    /// # Example
    /// ```ignore
    /// let mut batch = qp.blueflame_rq_batch()?;
    /// batch.post(entry1, addr1, len1, lkey1)?;
    /// batch.post(entry2, addr2, len2, lkey2)?;
    /// batch.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_rq_batch(
        &self,
    ) -> Result<UdRqBlueflameWqeBatch<'_, RqEntry>, SubmissionError> {
        let rq = self.rq.as_ref().ok_or(SubmissionError::RqFull)?;
        let sq = self
            .sq
            .as_ref()
            .ok_or(SubmissionError::BlueflameNotAvailable)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(UdRqBlueflameWqeBatch::new(
            rq,
            sq.bf_reg,
            sq.bf_size,
            &sq.bf_offset,
        ))
    }

    /// Ring the SQ doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes (Control Segment) to BlueFlame register.
    /// The NIC fetches remaining WQE data via DMA.
    #[inline]
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell();
        }
    }

    /// Ring the SQ doorbell with BlueFlame write of entire WQE.
    ///
    /// Copies the last WQE (up to bf_size bytes) to the BlueFlame register.
    /// For WQEs larger than bf_size, only the first bf_size bytes are copied via BF,
    /// and the NIC reads the rest from host memory.
    #[inline]
    pub fn ring_sq_doorbell_bf(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell_bf();
        }
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
    UdQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
{
    /// Activate UD QP (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, sq_psn: u32) -> io::Result<()> {
        self.modify_to_init(port, 0)?;
        self.modify_to_rtr()?;
        self.modify_to_rts(sq_psn)?;
        Ok(())
    }

    /// Get an emit context for macro-based WQE emission.
    #[doc(hidden)]
    pub fn emit_ctx(&self) -> io::Result<UdEmitContext<'_, SqEntry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(UdEmitContext {
            buf: sq.buf,
            wqe_cnt: sq.wqe_cnt,
            sqn: sq.sqn,
            pi: &sq.pi,
            ci: &sq.ci,
            last_wqe: &sq.last_wqe,
            table: &sq.table,
        })
    }
}

// =============================================================================
// CompletionTarget Implementation (OwnedRq)
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget
    for UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion (responder)
            if let Some(rq) = self.rq.as_ref()
                && let Some(entry) = rq.process_completion(cqe.wqe_counter)
            {
                (self.rq_callback)(cqe, entry);
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// CompletionTarget Implementation (SharedRq/SRQ)
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget
    for UdQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion via SRQ (responder)
            if let Some(entry) = self.rq.srq().process_recv_completion(cqe.wqe_counter) {
                (self.rq_callback)(cqe, entry);
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// CompletionTarget impl for RoCE variants
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget
    for UdQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion (responder)
            if let Some(rq) = self.rq.as_ref()
                && let Some(entry) = rq.process_completion(cqe.wqe_counter)
            {
                (self.rq_callback)(cqe, entry);
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget
    for UdQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion via SRQ (responder)
            if let Some(entry) = self.rq.srq().process_recv_completion(cqe.wqe_counter) {
                (self.rq_callback)(cqe, entry);
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// UdQpBuilder - Builder Pattern for UD QP Creation
// =============================================================================

use crate::qp::{CqSet, NoCq};

/// Builder for UD Queue Pairs.
///
/// # Type Parameters
///
/// - `SqEntry`: Entry type stored in SQ WQE table
/// - `RqEntry`: Entry type stored in RQ WQE table
/// - `T`: Transport type (`InfiniBand` or `RoCE`)
/// - `Rq`: Receive queue type (`UdOwnedRq<RqEntry>` or `UdSharedRq<RqEntry>`)
/// - `SqCqState`: SQ CQ configuration state (`NoCq` or `CqSet`)
/// - `RqCqState`: RQ CQ configuration state (`NoCq` or `CqSet`)
/// - `OnSq`: SQ completion callback type
/// - `OnRq`: RQ completion callback type
/// - `SqMono`: MonoCq reference for SQ (default `()`)
/// - `RqMono`: MonoCq reference for RQ (default `()`)
pub struct UdQpBuilder<
    'a,
    SqEntry,
    RqEntry,
    T,
    Rq,
    SqCqState,
    RqCqState,
    OnSq,
    OnRq,
    SqMono = (),
    RqMono = (),
> {
    ctx: &'a Context,
    pd: &'a Pd,
    config: UdQpConfig,

    // CQ numbers (set via sq_cq/sq_mono_cq/rq_cq/rq_mono_cq)
    send_cqn: u32,
    recv_cqn: u32,

    // Weak references to normal CQs for registration (None for MonoCq)
    send_cq_weak: Option<Weak<Cq>>,
    recv_cq_weak: Option<Weak<Cq>>,

    // Callbacks (set via sq_cq/rq_cq, () for MonoCq)
    sq_callback: OnSq,
    rq_callback: OnRq,

    // MonoCq references for auto-registration at build() time
    sq_mono_cq_ref: SqMono,
    rq_mono_cq_ref: RqMono,

    // SRQ (set via with_srq())
    srq: Option<Rc<Srq<RqEntry>>>,

    _marker: std::marker::PhantomData<(SqEntry, RqEntry, T, Rq, SqCqState, RqCqState)>,
}

impl Context {
    /// Create a UD QP Builder.
    ///
    /// CQs are configured via builder methods:
    /// - `.sq_cq(cq, callback)` - Normal CQ with callback
    /// - `.sq_mono_cq(mono_cq)` - MonoCq (callback is on CQ side)
    /// - `.rq_cq(cq, callback)` / `.rq_mono_cq(mono_cq)` - Same for RQ
    ///
    /// # Example
    ///
    /// ```ignore
    /// let qp = ctx.ud_qp_builder::<u64, u64>(&pd, &config)
    ///     .sq_cq(send_cq.clone(), |cqe, entry| { /* SQ completion */ })
    ///     .rq_cq(recv_cq.clone(), |cqe, entry| { /* RQ completion */ })
    ///     .build()?;
    /// ```
    pub fn ud_qp_builder<'a, SqEntry, RqEntry>(
        &'a self,
        pd: &'a Pd,
        config: &UdQpConfig,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, InfiniBand, UdOwnedRq<RqEntry>, NoCq, NoCq, (), (), (), ()>
    {
        UdQpBuilder {
            ctx: self,
            pd,
            config: config.clone(),
            send_cqn: 0,
            recv_cqn: 0,
            send_cq_weak: None,
            recv_cq_weak: None,
            sq_callback: (),
            rq_callback: (),
            sq_mono_cq_ref: (),
            rq_mono_cq_ref: (),
            srq: None,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// SQ CQ Configuration Methods
// =============================================================================

impl<'a, SqEntry, RqEntry, T, Rq, RqCqState, OnRq, RqMono>
    UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, NoCq, RqCqState, (), OnRq, (), RqMono>
{
    /// Set normal CQ for SQ with callback.
    pub fn sq_cq<OnSq>(
        self,
        cq: Rc<Cq>,
        callback: OnSq,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, CqSet, RqCqState, OnSq, OnRq, (), RqMono>
    where
        OnSq: Fn(Cqe, SqEntry) + 'static,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: cq.cqn(),
            recv_cqn: self.recv_cqn,
            send_cq_weak: Some(Rc::downgrade(&cq)),
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: callback,
            rq_callback: self.rq_callback,
            sq_mono_cq_ref: (),
            rq_mono_cq_ref: self.rq_mono_cq_ref,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }

    /// Set MonoCq for SQ (callback is on CQ side).
    pub fn sq_mono_cq<Q>(
        self,
        mono_cq: &Rc<crate::mono_cq::MonoCq<Q>>,
    ) -> UdQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        T,
        Rq,
        CqSet,
        RqCqState,
        (),
        OnRq,
        Rc<crate::mono_cq::MonoCq<Q>>,
        RqMono,
    >
    where
        Q: crate::mono_cq::CompletionSource,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: mono_cq.cqn(),
            recv_cqn: self.recv_cqn,
            send_cq_weak: None,
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: (),
            rq_callback: self.rq_callback,
            sq_mono_cq_ref: Rc::clone(mono_cq),
            rq_mono_cq_ref: self.rq_mono_cq_ref,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// RQ CQ Configuration Methods
// =============================================================================

impl<'a, SqEntry, RqEntry, T, Rq, SqCqState, OnSq, SqMono>
    UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, NoCq, OnSq, (), SqMono, ()>
{
    /// Set normal CQ for RQ with callback.
    pub fn rq_cq<OnRq>(
        self,
        cq: Rc<Cq>,
        callback: OnRq,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, CqSet, OnSq, OnRq, SqMono, ()>
    where
        OnRq: Fn(Cqe, RqEntry) + 'static,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: self.send_cqn,
            recv_cqn: cq.cqn(),
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: Some(Rc::downgrade(&cq)),
            sq_callback: self.sq_callback,
            rq_callback: callback,
            sq_mono_cq_ref: self.sq_mono_cq_ref,
            rq_mono_cq_ref: (),
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }

    /// Set MonoCq for RQ (callback is on CQ side).
    pub fn rq_mono_cq<Q>(
        self,
        mono_cq: &Rc<crate::mono_cq::MonoCq<Q>>,
    ) -> UdQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        T,
        Rq,
        SqCqState,
        CqSet,
        OnSq,
        (),
        SqMono,
        Rc<crate::mono_cq::MonoCq<Q>>,
    >
    where
        Q: crate::mono_cq::CompletionSource,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: self.send_cqn,
            recv_cqn: mono_cq.cqn(),
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: None,
            sq_callback: self.sq_callback,
            rq_callback: (),
            sq_mono_cq_ref: self.sq_mono_cq_ref,
            rq_mono_cq_ref: Rc::clone(mono_cq),
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// Transport / RQ Type Transition Methods
// =============================================================================

impl<'a, SqEntry, RqEntry, T, Rq, SqCqState, RqCqState, OnSq, OnRq, SqMono, RqMono>
    UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, RqCqState, OnSq, OnRq, SqMono, RqMono>
{
    /// Switch to RoCE transport.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn for_roce(
        self,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, RoCE, Rq, SqCqState, RqCqState, OnSq, OnRq, SqMono, RqMono>
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: self.send_cqn,
            recv_cqn: self.recv_cqn,
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: self.sq_callback,
            rq_callback: self.rq_callback,
            sq_mono_cq_ref: self.sq_mono_cq_ref,
            rq_mono_cq_ref: self.rq_mono_cq_ref,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, SqEntry, RqEntry, T, SqCqState, RqCqState, OnSq, OnRq, SqMono, RqMono>
    UdQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        T,
        UdOwnedRq<RqEntry>,
        SqCqState,
        RqCqState,
        OnSq,
        OnRq,
        SqMono,
        RqMono,
    >
{
    /// Use Shared Receive Queue (SRQ) instead of owned RQ.
    pub fn with_srq(
        self,
        srq: Rc<Srq<RqEntry>>,
    ) -> UdQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        T,
        UdSharedRq<RqEntry>,
        SqCqState,
        RqCqState,
        OnSq,
        OnRq,
        SqMono,
        RqMono,
    > {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: self.send_cqn,
            recv_cqn: self.recv_cqn,
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: self.sq_callback,
            rq_callback: self.rq_callback,
            sq_mono_cq_ref: self.sq_mono_cq_ref,
            rq_mono_cq_ref: self.rq_mono_cq_ref,
            srq: Some(srq),
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// Build Methods - InfiniBand + OwnedRq
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        InfiniBand,
        UdOwnedRq<RqEntry>,
        CqSet,
        CqSet,
        OnSq,
        OnRq,
        (),
        (),
    >
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP.
    pub fn build(self) -> BuildResult<UdQpIb<SqEntry, RqEntry, OnSq, OnRq>> {
        unsafe {
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                None,
            )?;

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            // Initialize SQ
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            // Initialize RQ
            let rq_wqe_cnt = 1u16 << res.layout.log_rq_size;
            let rq = UdOwnedRq::new(Some(UdRecvQueueState {
                buf: res.rq_buf,
                wqe_cnt: rq_wqe_cnt,
                stride: res.layout.rq_stride as u32,
                pi: Cell::new(0),
                ci: Cell::new(0),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                pending_start_ptr: Cell::new(None),
                pending_wqe_count: Cell::new(0),
                table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
            }));

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq,
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal Cq
            register_with_cqs(qpn, &send_cq_for_register, &recv_cq_for_register, &qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - InfiniBand + OwnedRq + Hybrid (SQ: Cq+callback, RQ: MonoCq)
// =============================================================================

impl<'a, Entry, OnSq, RqMono>
    UdQpBuilder<'a, Entry, Entry, InfiniBand, UdOwnedRq<Entry>, CqSet, CqSet, OnSq, (), (), RqMono>
where
    Entry: Clone + 'static,
    OnSq: Fn(Cqe, Entry) + 'static,
    RqMono: MaybeMonoCqRegister<UdQpForMonoCqWithSqCb<Entry, OnSq>>,
{
    /// Build the UD QP in hybrid mode (SQ: normal CQ with callback, RQ: MonoCq).
    ///
    /// This variant is for when SQ uses normal CQ with callback and RQ uses MonoCq.
    /// SQ completions will trigger the SQ callback.
    /// RQ completions will be dispatched via MonoCq callback.
    pub fn build(self) -> BuildResult<UdQpForMonoCqWithSqCb<Entry, OnSq>> {
        unsafe {
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                None,
            )?;

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();

            // Initialize SQ
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            // Initialize RQ
            let rq_wqe_cnt = 1u16 << res.layout.log_rq_size;
            let rq = UdOwnedRq::new(Some(UdRecvQueueState {
                buf: res.rq_buf,
                wqe_cnt: rq_wqe_cnt,
                stride: res.layout.rq_stride as u32,
                pi: Cell::new(0),
                ci: Cell::new(0),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                pending_start_ptr: Cell::new(None),
                pending_wqe_count: Cell::new(0),
                table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
            }));

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq,
                sq_callback: self.sq_callback,
                rq_callback: (), // MonoCq callback is on CQ side
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: Weak::new(), // MonoCq doesn't need CQ registration
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with send CQ only (recv CQ is MonoCq)
            if let Some(send_cq) = &send_cq_for_register
                && let Some(cq) = send_cq.upgrade()
            {
                let weak: Weak<RefCell<dyn CompletionTarget>> =
                    Rc::downgrade(&(qp_rc.clone() as Rc<RefCell<dyn CompletionTarget>>));
                cq.register_queue(qpn, weak);
            }

            self.rq_mono_cq_ref.maybe_register(&qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - InfiniBand + SharedRq (SRQ)
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        InfiniBand,
        UdSharedRq<RqEntry>,
        CqSet,
        CqSet,
        OnSq,
        OnRq,
        (),
        (),
    >
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP with SRQ.
    pub fn build(self) -> BuildResult<UdQpIbWithSrq<SqEntry, RqEntry, OnSq, OnRq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        unsafe {
            let srqn = srq.srq_number()?;
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                Some(srqn),
            )?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            // Initialize SQ only (SRQ handles receives)
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq: UdSharedRq::new(srq_inner),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal Cq
            register_with_cqs(qpn, &send_cq_for_register, &recv_cq_for_register, &qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - RoCE + OwnedRq
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, RoCE, UdOwnedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq, (), ()>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP for RoCE.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<UdQpRoCE<SqEntry, RqEntry, OnSq, OnRq>> {
        unsafe {
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                None,
            )?;

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            // Initialize SQ
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            // Initialize RQ
            let rq_wqe_cnt = 1u16 << res.layout.log_rq_size;
            let rq = UdOwnedRq::new(Some(UdRecvQueueState {
                buf: res.rq_buf,
                wqe_cnt: rq_wqe_cnt,
                stride: res.layout.rq_stride as u32,
                pi: Cell::new(0),
                ci: Cell::new(0),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                pending_start_ptr: Cell::new(None),
                pending_wqe_count: Cell::new(0),
                table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
            }));

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq,
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal Cq
            register_with_cqs(qpn, &send_cq_for_register, &recv_cq_for_register, &qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - RoCE + SharedRq (SRQ)
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, RoCE, UdSharedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq, (), ()>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP with SRQ for RoCE.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<UdQpRoCEWithSrq<SqEntry, RqEntry, OnSq, OnRq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        unsafe {
            let srqn = srq.srq_number()?;
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                Some(srqn),
            )?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            // Initialize SQ only (SRQ handles receives)
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq: UdSharedRq::new(srq_inner),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            register_with_cqs(qpn, &send_cq_for_register, &recv_cq_for_register, &qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - MonoCq (OnSq = (), OnRq = ()) - InfiniBand + OwnedRq
// =============================================================================

impl<'a, Entry, SqMono, RqMono>
    UdQpBuilder<
        'a,
        Entry,
        Entry,
        InfiniBand,
        UdOwnedRq<Entry>,
        CqSet,
        CqSet,
        (),
        (),
        SqMono,
        RqMono,
    >
where
    Entry: 'static,
    SqMono: MaybeMonoCqRegister<UdQpForMonoCq<Entry>>,
    RqMono: MaybeMonoCqRegister<UdQpForMonoCq<Entry>>,
{
    /// Build the UD QP for use with MonoCq.
    ///
    /// When using MonoCq, callbacks are stored in the MonoCq, not in the UdQp.
    /// This method is available when both SQ and RQ use MonoCq.
    pub fn build(self) -> BuildResult<UdQpForMonoCq<Entry>> {
        unsafe {
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                None,
            )?;

            // Initialize SQ
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            // Initialize RQ
            let rq_wqe_cnt = 1u16 << res.layout.log_rq_size;
            let rq = UdOwnedRq::new(Some(UdRecvQueueState {
                buf: res.rq_buf,
                wqe_cnt: rq_wqe_cnt,
                stride: res.layout.rq_stride as u32,
                pi: Cell::new(0),
                ci: Cell::new(0),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                pending_start_ptr: Cell::new(None),
                pending_wqe_count: Cell::new(0),
                table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
            }));

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq,
                sq_callback: (),
                rq_callback: (),
                send_cq: Weak::new(), // MonoCq doesn't use Cq registration
                recv_cq: Weak::new(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            self.sq_mono_cq_ref.maybe_register(&qp_rc);
            self.rq_mono_cq_ref.maybe_register(&qp_rc);
            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - MonoCq (OnSq = (), OnRq = ()) - RoCE + OwnedRq
// =============================================================================

impl<'a, Entry, SqMono, RqMono>
    UdQpBuilder<'a, Entry, Entry, RoCE, UdOwnedRq<Entry>, CqSet, CqSet, (), (), SqMono, RqMono>
where
    Entry: 'static,
    SqMono: MaybeMonoCqRegister<UdQpForMonoCqRoCE<Entry>>,
    RqMono: MaybeMonoCqRegister<UdQpForMonoCqRoCE<Entry>>,
{
    /// Build the UD QP for use with MonoCq (RoCE).
    ///
    /// When using MonoCq, callbacks are stored in the MonoCq, not in the UdQp.
    /// This method is available when both SQ and RQ use MonoCq.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<UdQpForMonoCqRoCE<Entry>> {
        unsafe {
            let res = create_devx_ud_qp(
                self.ctx,
                self.pd,
                self.send_cqn,
                self.recv_cqn,
                &self.config,
                None,
            )?;

            // Initialize SQ
            let sq_wqe_cnt = 1u16 << res.layout.log_sq_size;
            let sq = Some(SendQueueState {
                buf: res.sq_buf,
                wqe_cnt: sq_wqe_cnt,
                sqn: res.qpn,
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(sq_wqe_cnt),
                _marker: PhantomData,
            });

            // Initialize RQ
            let rq_wqe_cnt = 1u16 << res.layout.log_rq_size;
            let rq = UdOwnedRq::new(Some(UdRecvQueueState {
                buf: res.rq_buf,
                wqe_cnt: rq_wqe_cnt,
                stride: res.layout.rq_stride as u32,
                pi: Cell::new(0),
                ci: Cell::new(0),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                pending_start_ptr: Cell::new(None),
                pending_wqe_count: Cell::new(0),
                table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
            }));

            let result = UdQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                sq,
                rq,
                sq_callback: (),
                rq_callback: (),
                send_cq: Weak::new(), // MonoCq doesn't use Cq registration
                recv_cq: Weak::new(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            self.sq_mono_cq_ref.maybe_register(&qp_rc);
            self.rq_mono_cq_ref.maybe_register(&qp_rc);
            Ok(qp_rc)
        }
    }
}

// =============================================================================
// CompletionSource Implementation for UdQpForMonoCq
// =============================================================================

use crate::mono_cq::CompletionSource;

/// CompletionSource for UdQpForMonoCqWithSqCb (covers both pure MonoCq and hybrid mode).
///
/// This implementation is used for:
/// - `UdQpForMonoCq<Entry>` (when OnSq = ()): Both SQ and RQ use MonoCq
/// - `UdQpForMonoCqWithSqCb<Entry, OnSq>` (when OnSq is a callback): SQ uses normal CQ, RQ uses MonoCq
///
/// In both cases, MonoCq handles RQ completions. For hybrid mode, SQ completions
/// are handled by the normal CQ via CompletionTarget::dispatch_cqe.
///
/// This implementation handles both SQ and RQ completions for MonoCq:
/// - In pure MonoCq mode (both SQ and RQ use MonoCq): processes both
/// - In hybrid mode (SQ uses normal Cq): only RQ completions arrive at MonoCq,
///   so the SQ branch won't be called (send completions go to normal Cq)
impl<Entry, OnSq> CompletionSource for UdQpForMonoCqWithSqCb<Entry, OnSq> {
    type Entry = Entry;

    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn process_cqe(&self, cqe: Cqe) -> Option<Entry> {
        if cqe.opcode.is_responder() {
            // RQ completion
            self.rq.as_ref()?.process_completion(cqe.wqe_counter)
        } else {
            // SQ completion
            self.sq.as_ref()?.process_completion(cqe.wqe_counter)
        }
    }
}

/// CompletionTarget for UdQpForMonoCqWithSqCb (hybrid mode: SQ callback, RQ MonoCq).
///
/// This implementation is for the normal CQ send side. It only handles SQ completions.
impl<Entry, OnSq> CompletionTarget for UdQpForMonoCqWithSqCb<Entry, OnSq>
where
    OnSq: Fn(Cqe, Entry),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if !cqe.opcode.is_responder() {
            // SQ completion - process and call callback
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
        // RQ completions are handled by MonoCq via CompletionSource, not here
    }
}
