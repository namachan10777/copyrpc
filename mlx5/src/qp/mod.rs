//! Queue Pair (QP) management.
//!
//! Queue Pairs are the fundamental communication endpoints in RDMA.
//! This module provides RC (Reliable Connection) QP creation using mlx5dv_create_qp.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::io;

use crate::BuildResult;
use crate::CompletionTarget;
use crate::builder_common::{MaybeMonoCqRegister, register_with_cqs, register_with_send_cq};
use crate::cq::{Cq, Cqe};
use crate::device::Context;
use crate::devx::{DevxObj, DevxUmem};
use crate::pd::Pd;
use crate::prm;
use crate::srq::Srq;
use crate::transport::{IbRemoteQpInfo, RoCERemoteQpInfo};
use crate::wqe::{
    DATA_SEG_SIZE,
    // Transport type tags
    InfiniBand,
    OrderedWqeTable,
    RoCE,
    SubmissionError,
    WQEBB_SIZE,
    // Macro-based emission
    emit::{EmitContext, SendQueueState, bf_finish_rq},
    write_data_seg,
};

/// RC QP configuration.
#[derive(Debug, Clone)]
pub struct RcQpConfig {
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
    /// Enable scatter-to-CQE for small received data.
    /// When enabled, received data ≤32 bytes is placed directly in CQE.
    pub enable_scatter_to_cqe: bool,
}

impl Default for RcQpConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_recv_wr: 256,
            max_send_sge: 4,
            max_recv_sge: 4,
            max_inline_data: 64,
            enable_scatter_to_cqe: false,
        }
    }
}

/// QP state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Error,
}

/// Page-aligned buffer (RAII).
pub(crate) struct AlignedBuf {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedBuf {
    pub(crate) fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "alloc_zeroed failed");
        Self { ptr, layout }
    }

    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub(crate) fn len(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) }
    }
}

/// QP buffer layout computed from config.
///
/// PRM-mandated layout: RQ at offset 0, SQ at offset rq_size.
/// (rdma-core: qp->rq.offset = 0; qp->sq.offset = rq_size)
pub(crate) struct QpBufferLayout {
    pub(crate) total_size: usize,
    pub(crate) sq_size: usize,
    pub(crate) rq_size: usize,
    pub(crate) rq_stride: usize,
    pub(crate) dbrec_offset: usize,
    pub(crate) log_sq_size: u32,
    pub(crate) log_rq_size: u32,
    pub(crate) log_rq_stride: u32,
}

impl RcQpConfig {
    /// Compute buffer layout for DevX QP creation.
    pub(crate) fn buffer_layout(&self, use_srq: bool) -> QpBufferLayout {
        let log_sq_size = if self.max_send_wr == 0 {
            0
        } else {
            (self.max_send_wr as usize).next_power_of_two().trailing_zeros()
        };
        let sq_size = (1usize << log_sq_size) * WQEBB_SIZE;

        let (rq_size, rq_stride, log_rq_size, log_rq_stride) = if use_srq {
            (0, 0, 0, 0)
        } else {
            let sge_count = (self.max_recv_sge as usize).max(1);
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

        QpBufferLayout {
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

/// Resources created by DevX QP creation.
struct DevxQpResources {
    devx_obj: DevxObj,
    umem: DevxUmem,
    wq_buf: AlignedBuf,
    dbr_umem: DevxUmem,
    dbr_buf: AlignedBuf,
    qpn: u32,
    layout: QpBufferLayout,
    bf_reg: *mut u8,
    bf_size: u32,
    dbrec: *mut u32,
    sq_buf: *mut u8,
    rq_buf: *mut u8,
}

/// Create an RC QP via DevX PRM CREATE_QP command.
///
/// Matches rdma-core's dr_devx_create_qp pattern:
/// - Separate DBREC UMEM (DBREC at offset 0)
/// - wq_umem_id without wq_umem_valid flag
/// - dbr_umem_id without dbr_umem_valid flag
/// - pm_state=3 in CREATE_QP
///
/// Buffer layout: WQ UMEM = [RQ | SQ], DBREC UMEM = [dbrec[0..1]]
///
/// # Safety
/// The caller must ensure `ctx`, `pd` are valid and the context was opened with DevX.
unsafe fn create_devx_rc_qp(
    ctx: &Context,
    pd: &Pd,
    send_cqn: u32,
    recv_cqn: u32,
    config: &RcQpConfig,
    srqn: Option<u32>,
) -> io::Result<DevxQpResources> {
    let layout = config.buffer_layout(srqn.is_some());

    // WQ buffer: [RQ | SQ] (no DBREC — separate UMEM)
    let wq_size = layout.sq_size + layout.rq_size;
    let wq_buf = AlignedBuf::new(if wq_size > 0 { wq_size } else { 4096 });
    let umem = ctx.register_umem(wq_buf.as_ptr(), wq_buf.len(), 7)?;
    let wq_umem_id = umem.umem_id();

    // Separate DBREC buffer (page-aligned, DBREC at offset 0)
    let dbr_buf = AlignedBuf::new(4096);
    let dbr_umem = ctx.register_umem(dbr_buf.as_ptr(), dbr_buf.len(), 7)?;
    let dbr_umem_id = dbr_umem.umem_id();

    let uar = ctx.devx_uar()?;

    // Build CREATE_QP PRM command (matching rdma-core dr_devx_create_qp)
    let cmd_in_size = prm::CREATE_QP_IN_BASE_SIZE;
    let mut cmd_in = vec![0u8; cmd_in_size];
    let mut cmd_out = vec![0u8; prm::CREATE_QP_OUT_SIZE];

    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_OPCODE.0,
        prm::CREATE_QP_IN_OPCODE.1,
        prm::MLX5_CMD_OP_CREATE_QP,
    );

    // WQ UMEM: only set wq_umem_id (NO wq_umem_valid, NO wq_umem_offset)
    // This matches rdma-core's dr_devx_create_qp pattern exactly.
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_ID.0,
        prm::CREATE_QP_IN_WQ_UMEM_ID.1,
        wq_umem_id,
    );

    let q = prm::CREATE_QP_IN_QPC_OFF;
    prm::prm_set(&mut cmd_in, q + prm::QPC_ST.0, prm::QPC_ST.1, prm::QP_ST_RC);
    // pm_state = MIGRATED (3) in CREATE_QP (matching rdma-core)
    prm::prm_set(&mut cmd_in, q + prm::QPC_PM_STATE.0, prm::QPC_PM_STATE.1, 3);
    prm::prm_set(&mut cmd_in, q + prm::QPC_PD.0, prm::QPC_PD.1, pd.pdn());
    prm::prm_set(&mut cmd_in, q + prm::QPC_UAR_PAGE.0, prm::QPC_UAR_PAGE.1, uar.page_id());
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_SQ_SIZE.0, prm::QPC_LOG_SQ_SIZE.1, layout.log_sq_size);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RQ_SIZE.0, prm::QPC_LOG_RQ_SIZE.1, layout.log_rq_size);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RQ_STRIDE.0, prm::QPC_LOG_RQ_STRIDE.1, layout.log_rq_stride);
    // NOTE: log_msg_max NOT set (rdma-core dr_devx doesn't set it)
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_SND.0, prm::QPC_CQN_SND.1, send_cqn);
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_RCV.0, prm::QPC_CQN_RCV.1, recv_cqn);

    // DBR UMEM: separate UMEM, DBREC at offset 0
    // Only set dbr_umem_id (NO dbr_umem_valid, NO dbr_addr)
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_ID.0, prm::QPC_DBR_UMEM_ID.1, dbr_umem_id);

    // SRQ
    if let Some(srqn) = srqn {
        prm::prm_set(&mut cmd_in, q + prm::QPC_RQ_TYPE.0, prm::QPC_RQ_TYPE.1, 1);
        prm::prm_set(&mut cmd_in, q + prm::QPC_SRQN_RMPN_XRQN.0, prm::QPC_SRQN_RMPN_XRQN.1, srqn);
    }

    // Debug: dump CREATE_QP PRM command
    eprintln!("=== RC QP CREATE_QP PRM command ({} bytes) ===", cmd_in.len());
    eprintln!("  layout: sq_size={}, rq_size={}, rq_stride={}, dbrec_offset={}, total={}",
        layout.sq_size, layout.rq_size, layout.rq_stride, layout.dbrec_offset, layout.total_size);
    eprintln!("  log: sq={}, rq={}, rq_stride={}", layout.log_sq_size, layout.log_rq_size, layout.log_rq_stride);
    eprintln!("  wq_umem_id={}, dbr_umem_id={}, uar_page={}, pd={}", wq_umem_id, dbr_umem_id, uar.page_id(), pd.pdn());
    eprintln!("  send_cqn={}, recv_cqn={}", send_cqn, recv_cqn);
    for i in (0..cmd_in.len()).step_by(16) {
        let end = (i + 16).min(cmd_in.len());
        let hex: Vec<String> = cmd_in[i..end].iter().map(|b| format!("{:02x}", b)).collect();
        if hex.iter().any(|s| s != "00") {
            eprintln!("  {:04x}: {}", i, hex.join(" "));
        }
    }

    let devx_obj = ctx.devx_obj_create(&cmd_in, &mut cmd_out)?;
    let qpn = prm::prm_get(&cmd_out, prm::CREATE_QP_OUT_QPN.0, prm::CREATE_QP_OUT_QPN.1);

    eprintln!("  QPN: 0x{:x}", qpn);
    eprintln!("  wq_buf ptr: {:p}, rq_size offset: 0x{:x}", wq_buf.as_ptr(), layout.rq_size);

    // PRM layout: RQ at offset 0, SQ at offset rq_size
    let rq_buf = wq_buf.as_ptr();
    let sq_buf = wq_buf.as_ptr().add(layout.rq_size);
    // DBREC at offset 0 in separate buffer (dbrec[0]=RCV, dbrec[1]=SND)
    let dbrec = dbr_buf.as_ptr() as *mut u32;

    Ok(DevxQpResources {
        devx_obj,
        umem,
        wq_buf,
        dbr_umem,
        dbr_buf,
        qpn,
        layout,
        bf_reg: uar.reg_addr(),
        bf_size: 256,
        dbrec,
        sq_buf,
        rq_buf,
    })
}

// =============================================================================
// Send Queue State
// =============================================================================

// SendQueueState is now defined in crate::wqe::emit and imported above.

impl<Entry> SendQueueState<Entry, OrderedWqeTable<Entry>> {
    /// Get an EmitContext for macro-based WQE emission.
    #[inline]
    pub(crate) fn emit_ctx(&self) -> EmitContext<'_, Entry> {
        EmitContext {
            buf: self.buf,
            wqe_cnt: self.wqe_cnt,
            sqn: self.sqn,
            pi: &self.pi,
            ci: &self.ci,
            last_wqe: &self.last_wqe,
            table: &self.table,
        }
    }
}

// =============================================================================
// Receive Queue State
// =============================================================================

/// Receive Queue state for direct WQE posting.
///
/// Generic over `Entry`, the entry type stored in the WQE table.
/// Unlike SQ, all RQ WQEs generate completions (all signaled),
/// so we use a simple table.
///
/// Uses `Cell<Option<Entry>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows.
pub(crate) struct ReceiveQueueState<Entry> {
    /// RQ buffer base address
    buf: *mut u8,
    /// Number of WQE slots
    wqe_cnt: u32,
    /// Stride (bytes per WQE slot)
    stride: u32,
    /// Producer index (next WQE slot)
    pub(super) pi: Cell<u16>,
    /// Consumer index (last completed WQE + 1)
    ci: Cell<u16>,
    /// Doorbell record pointer (dbrec[0] for RQ)
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

/// MLX5 invalid lkey value used to mark end of SGE list.
const MLX5_INVALID_LKEY: u32 = 0x100;

impl<Entry> ReceiveQueueState<Entry> {
    #[inline]
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx as u32) & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    /// Process a receive completion.
    ///
    /// Returns the entry associated with the completed WQE.
    #[inline]
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        self.ci.set(wqe_idx.wrapping_add(1));
        let idx = (wqe_idx as usize) & ((self.wqe_cnt - 1) as usize);
        self.table[idx].take()
    }

    /// Get the number of available WQE slots.
    #[inline]
    pub(super) fn available(&self) -> u32 {
        self.wqe_cnt - (self.pi.get().wrapping_sub(self.ci.get()) as u32)
    }

    #[inline]
    pub(super) fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    /// Ring the RQ doorbell (DBREC-only, no BF write).
    ///
    /// Updates DBREC[0] (RQ doorbell record) to notify the NIC of new RQ WQEs.
    /// RQ does not use BlueFlame writes (BF is SQ-only).
    #[inline]
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
    #[inline]
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
// Receive Queue Type Markers
// =============================================================================

/// Marker type for QP with owned Receive Queue.
///
/// When a QP uses `OwnedRq`, it has its own dedicated receive queue and
/// the RQ-related methods (`post_recv`, `ring_rq_doorbell`, `blueflame_rq_batch`)
/// are available.
pub struct OwnedRq<RqEntry>(Option<ReceiveQueueState<RqEntry>>);

impl<RqEntry> OwnedRq<RqEntry> {
    pub(crate) fn new(rq: Option<ReceiveQueueState<RqEntry>>) -> Self {
        Self(rq)
    }

    pub(crate) fn as_ref(&self) -> Option<&ReceiveQueueState<RqEntry>> {
        self.0.as_ref()
    }
}

/// Marker type for QP with Shared Receive Queue (SRQ).
///
/// When a QP uses `SharedRq`, receive operations are handled through the SRQ
/// and the QP's own RQ-related methods are not available. Instead, use
/// `srq()` to access the underlying SRQ.
pub struct SharedRq<RqEntry>(Srq<RqEntry>);

impl<RqEntry> SharedRq<RqEntry> {
    pub(crate) fn new(srq: Srq<RqEntry>) -> Self {
        Self(srq)
    }

    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        &self.0
    }
}

// =============================================================================
// RC QP
// =============================================================================

/// RC (Reliable Connection) Queue Pair with owned Receive Queue (InfiniBand).
///
/// Only signaled WQEs have entries stored in the WQE table.
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQp<
    SqEntry,
    RqEntry,
    InfiniBand,
    OrderedWqeTable<SqEntry>,
    OwnedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// RC (Reliable Connection) Queue Pair with SRQ (InfiniBand).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQp<
    SqEntry,
    RqEntry,
    InfiniBand,
    OrderedWqeTable<SqEntry>,
    SharedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// RC (Reliable Connection) Queue Pair with owned Receive Queue (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQp<
    SqEntry,
    RqEntry,
    RoCE,
    OrderedWqeTable<SqEntry>,
    OwnedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// RC (Reliable Connection) Queue Pair with SRQ (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQp<
    SqEntry,
    RqEntry,
    RoCE,
    OrderedWqeTable<SqEntry>,
    SharedRq<RqEntry>,
    OnSqComplete,
    OnRqComplete,
>;

/// Handle to RC QP (InfiniBand) wrapped in `Rc<RefCell<...>>`.
pub type RcQpIbHandle<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    Rc<RefCell<RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>;

/// Handle to RC QP with SRQ (InfiniBand) wrapped in `Rc<RefCell<...>>`.
pub type RcQpIbWithSrqHandle<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    Rc<RefCell<RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>;

/// Handle to RC QP (RoCE) wrapped in `Rc<RefCell<...>>`.
pub type RcQpRoCEHandle<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    Rc<RefCell<RcQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>;

/// Handle to RC QP with SRQ (RoCE) wrapped in `Rc<RefCell<...>>`.
pub type RcQpRoCEWithSrqHandle<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    Rc<RefCell<RcQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>;

/// RC (Reliable Connection) Queue Pair (internal implementation).
///
/// Created using mlx5dv_create_qp for direct hardware access.
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `Transport`: Transport type tag (`InfiniBand` or `RoCE`)
/// - `TableType`: WQE table behavior for the SQ (`OrderedWqeTable` or `UnorderedWqeTable`)
/// - `Rq`: Receive queue type (`OwnedRq<RqEntry>` or `SharedRq<RqEntry>`)
/// - `OnSqComplete`: SQ completion callback type
/// - `OnRqComplete`: RQ completion callback type
pub struct RcQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    /// DevX QP object. Must be dropped before UMEMs.
    _devx_obj: DevxObj,
    /// Registered UMEM for SQ/RQ buffers. Must be dropped after _devx_obj.
    _umem: DevxUmem,
    /// Page-aligned WQ buffer. Must be dropped after _umem.
    _wq_buf: AlignedBuf,
    /// Registered UMEM for DBREC. Must be dropped after _devx_obj.
    _dbr_umem: DevxUmem,
    /// Page-aligned DBREC buffer. Must be dropped after _dbr_umem.
    _dbr_buf: AlignedBuf,
    /// QP number (from HW).
    qpn: u32,
    state: Cell<QpState>,
    sq: Option<SendQueueState<SqEntry, TableType>>,
    rq: Rq,
    sq_callback: OnSqComplete,
    rq_callback: OnRqComplete,
    /// Weak reference to the send CQ for unregistration on drop
    send_cq: Weak<Cq>,
    /// Weak reference to the recv CQ for unregistration on drop
    recv_cq: Weak<Cq>,
    /// Keep the PD alive while this QP exists.
    _pd: Pd,
    /// Phantom data for transport type and RqEntry
    _marker: std::marker::PhantomData<(Transport, RqEntry)>,
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> Drop
    for RcQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
{
    fn drop(&mut self) {
        let qpn = self.qpn();
        // Unregister from both CQs before destroying QP.
        // DevxObj destruction is automatic via field drop order.
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        if let Some(cq) = self.recv_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
    }
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
{
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        self.qpn
    }

    /// Get the current QP state.
    pub fn state(&self) -> QpState {
        self.state.get()
    }

    /// Transition QP from RESET to INIT via DevX PRM.
    pub fn modify_to_init(&mut self, port: u8, access_flags: u32) -> io::Result<()> {
        if self.state.get() != QpState::Reset {
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
        let ads = q + prm::QPC_PRI_ADS;

        // PM state = migrated (required by FW)
        prm::prm_set(&mut cmd_in, q + prm::QPC_PM_STATE.0, prm::QPC_PM_STATE.1, 3);
        // Primary address path: port
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);
        // Access flags
        if access_flags & mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_READ != 0 {
            prm::prm_set(&mut cmd_in, q + prm::QPC_RRE.0, prm::QPC_RRE.1, 1);
        }
        if access_flags & mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE != 0 {
            prm::prm_set(&mut cmd_in, q + prm::QPC_RWE.0, prm::QPC_RWE.1, 1);
        }
        if access_flags & mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_ATOMIC != 0 {
            prm::prm_set(&mut cmd_in, q + prm::QPC_RAE.0, prm::QPC_RAE.1, 1);
            // FW requires atomic_mode when RAE changes (syndrome 0x69efe)
            prm::prm_set(
                &mut cmd_in,
                q + prm::QPC_ATOMIC_MODE.0,
                prm::QPC_ATOMIC_MODE.1,
                1, // UP_TO_8_BYTES (standard IB 64-bit atomics)
            );
        }

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RST2INIT")?;

        self.state.set(QpState::Init);
        Ok(())
    }

    /// Transition QP from RTR to RTS (Ready to Send) via DevX PRM.
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state.get() != QpState::Rtr {
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
        let ads = q + prm::QPC_PRI_ADS;

        // log2(max_rd_atomic), clamped
        let log_sra = if max_rd_atomic > 0 { (max_rd_atomic as u32).next_power_of_two().trailing_zeros() } else { 0 };
        prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_SRA_MAX.0, prm::QPC_LOG_SRA_MAX.1, log_sra);
        prm::prm_set(&mut cmd_in, q + prm::QPC_RETRY_COUNT.0, prm::QPC_RETRY_COUNT.1, 7);
        prm::prm_set(&mut cmd_in, q + prm::QPC_RNR_RETRY.0, prm::QPC_RNR_RETRY.1, 7);
        prm::prm_set(&mut cmd_in, q + prm::QPC_NEXT_SEND_PSN.0, prm::QPC_NEXT_SEND_PSN.1, local_psn);
        // ack_timeout in primary address path
        prm::prm_set(&mut cmd_in, ads + prm::ADS_ACK_TIMEOUT.0, prm::ADS_ACK_TIMEOUT.1, 14);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RTR2RTS")?;

        self.state.set(QpState::Rts);
        Ok(())
    }

    /// Transition QP to ERROR state via DevX PRM.
    ///
    /// This cleanly tears down the connection and flushes any pending work requests.
    /// Useful before destroying a QP to avoid crashes when the remote QP is already gone.
    pub fn modify_to_error(&self) -> io::Result<()> {
        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_2ERR_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "2ERR")?;

        self.state.set(QpState::Error);
        Ok(())
    }

    /// Get the number of available WQE slots in the send queue.
    pub fn send_queue_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    /// Ring the SQ doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes (Control Segment) to BlueFlame register.
    /// The NIC fetches remaining WQE data via DMA.
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
    pub fn ring_sq_doorbell_bf(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell_bf();
        }
    }
}

/// InfiniBand transport-specific methods.
impl<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, InfiniBand, TableType, Rq, OnSqComplete, OnRqComplete>
{
    /// Transition QP from INIT to RTR (Ready to Receive) via DevX PRM.
    ///
    /// Uses LID-based addressing for InfiniBand fabric.
    pub fn modify_to_rtr(
        &mut self,
        remote: &IbRemoteQpInfo,
        port: u8,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        if self.state.get() != QpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in INIT state",
            ));
        }

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_INIT2RTR_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        prm::prm_set(&mut cmd_in, q + prm::QPC_MTU.0, prm::QPC_MTU.1, 5); // MTU 4096
        prm::prm_set(&mut cmd_in, q + prm::QPC_REMOTE_QPN.0, prm::QPC_REMOTE_QPN.1, remote.qp_number);
        prm::prm_set(&mut cmd_in, q + prm::QPC_NEXT_RCV_PSN.0, prm::QPC_NEXT_RCV_PSN.1, remote.packet_sequence_number);

        let log_rra = if max_dest_rd_atomic > 0 {
            (max_dest_rd_atomic as u32).next_power_of_two().trailing_zeros()
        } else {
            0
        };
        prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RRA_MAX.0, prm::QPC_LOG_RRA_MAX.1, log_rra);
        prm::prm_set(&mut cmd_in, q + prm::QPC_MIN_RNR_NAK.0, prm::QPC_MIN_RNR_NAK.1, 12);

        // Primary address path
        let ads = q + prm::QPC_PRI_ADS;
        prm::prm_set(&mut cmd_in, ads + prm::ADS_RLID.0, prm::ADS_RLID.1, remote.local_identifier as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "INIT2RTR")?;

        self.state.set(QpState::Rtr);
        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS.
    pub fn connect(
        &mut self,
        remote: &IbRemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }
}

/// RoCE transport-specific methods.
impl<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, RoCE, TableType, Rq, OnSqComplete, OnRqComplete>
{
    /// Transition QP from INIT to RTR (Ready to Receive) via DevX PRM.
    ///
    /// Uses GID-based addressing for RoCE (RDMA over Converged Ethernet).
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn modify_to_rtr(
        &mut self,
        remote: &RoCERemoteQpInfo,
        port: u8,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        if self.state.get() != QpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in INIT state",
            ));
        }

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_INIT2RTR_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        prm::prm_set(&mut cmd_in, q + prm::QPC_MTU.0, prm::QPC_MTU.1, 5); // MTU 4096
        prm::prm_set(&mut cmd_in, q + prm::QPC_REMOTE_QPN.0, prm::QPC_REMOTE_QPN.1, remote.qp_number);
        prm::prm_set(&mut cmd_in, q + prm::QPC_NEXT_RCV_PSN.0, prm::QPC_NEXT_RCV_PSN.1, remote.packet_sequence_number);

        let log_rra = if max_dest_rd_atomic > 0 {
            (max_dest_rd_atomic as u32).next_power_of_two().trailing_zeros()
        } else {
            0
        };
        prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RRA_MAX.0, prm::QPC_LOG_RRA_MAX.1, log_rra);
        prm::prm_set(&mut cmd_in, q + prm::QPC_MIN_RNR_NAK.0, prm::QPC_MIN_RNR_NAK.1, 12);

        // Primary address path with GRH for RoCE
        let ads = q + prm::QPC_PRI_ADS;
        prm::prm_set(&mut cmd_in, ads + prm::ADS_GRH.0, prm::ADS_GRH.1, 1);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_SRC_ADDR_INDEX.0, prm::ADS_SRC_ADDR_INDEX.1, remote.grh.sgid_index as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_HOP_LIMIT.0, prm::ADS_HOP_LIMIT.1, remote.grh.hop_limit as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_TCLASS.0, prm::ADS_TCLASS.1, remote.grh.traffic_class as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_FLOW_LABEL.0, prm::ADS_FLOW_LABEL.1, remote.grh.flow_label);

        // RGID (128-bit destination GID)
        let rgid_off = ads + prm::ADS_RGID_RIP;
        let gid = &remote.grh.dgid.raw;
        for i in 0..4 {
            let val = u32::from_be_bytes([gid[i * 4], gid[i * 4 + 1], gid[i * 4 + 2], gid[i * 4 + 3]]);
            prm::prm_set(&mut cmd_in, rgid_off + i * 32, 32, val);
        }

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "INIT2RTR")?;

        self.state.set(QpState::Rtr);
        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn connect(
        &mut self,
        remote: &RoCERemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }
}

// init_direct_access_internal removed: SQ/RQ state is now initialized directly
// in build() from the DevX buffer layout.

// =============================================================================
// CompletionTarget impl for RcQp with OwnedRq
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget
    for RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
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
// CompletionTarget impl for RcQp with SharedRq (SRQ)
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget
    for RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
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
    for RcQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
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
    for RcQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
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
// ReceiveQueue methods (OwnedRq)
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, Transport, TableType, OwnedRq<RqEntry>, OnSqComplete, OnRqComplete>
{
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
// SRQ access methods (SharedRq)
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, Transport, TableType, SharedRq<RqEntry>, OnSqComplete, OnRqComplete>
{
    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        self.rq.srq()
    }
}

// =============================================================================
// Common methods
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
{
    /// Get the Send Queue Number (SQN).
    ///
    /// Returns None if direct access is not initialized.
    #[inline]
    pub fn sqn(&self) -> Option<u32> {
        self.sq.as_ref().map(|sq| sq.sqn)
    }
}

// =============================================================================
// Low-level SQ Access API for wqe_tx! macro
// =============================================================================

/// SQ slot information for direct WQE construction.
///
/// Contains the raw pointers and indices needed for constructing WQEs directly
/// without using the builder pattern.
#[derive(Debug, Clone, Copy)]
pub struct SqSlot {
    /// Pointer to the WQE buffer (64-byte aligned WQEBB).
    pub ptr: *mut u8,
    /// WQE index in the send queue.
    pub wqe_idx: u16,
    /// Send Queue Number.
    pub sqn: u32,
}

// =============================================================================
// CompletionSource impl for RcQp (for use with MonoCq)
// =============================================================================

use crate::mono_cq::CompletionSource;

impl<Entry> CompletionSource
    for RcQp<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, OwnedRq<Entry>, (), ()>
{
    type Entry = Entry;

    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
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

// =============================================================================
// RcQp without callback for MonoCq
// =============================================================================

/// RcQp type for use with MonoCq (no internal callback).
///
/// Uses a single Entry type for both SQ and RQ completions.
/// If you need different Entry types for SQ and RQ, use separate CQs.
pub type RcQpForMonoCq<Entry> =
    RcQp<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, OwnedRq<Entry>, (), ()>;

/// RcQp type for use with MonoCq with Shared Receive Queue (no internal callback).
///
/// Uses a single Entry type for both SQ and SRQ completions.
/// The RQ completions are processed via the SRQ.
pub type RcQpForMonoCqWithSrq<Entry> =
    RcQp<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, SharedRq<Entry>, (), ()>;

/// RcQp type for use with MonoCq with Shared Receive Queue (with SQ callback).
///
/// Uses a single Entry type for both SQ and SRQ completions.
/// The RQ completions are processed via the SRQ.
/// SQ completions trigger the OnSq callback.
pub type RcQpForMonoCqWithSrqAndSqCb<Entry, OnSq> =
    RcQp<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, SharedRq<Entry>, OnSq, ()>;

// =============================================================================
// CompletionSource impl for RcQp with SharedRq (for use with MonoCq)
// =============================================================================

// CompletionSource for RcQpForMonoCqWithSrqAndSqCb (also covers RcQpForMonoCqWithSrq when OnSq = ())
impl<Entry: Clone + 'static, OnSq> CompletionSource for RcQpForMonoCqWithSrqAndSqCb<Entry, OnSq> {
    type Entry = Entry;

    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
    }

    fn process_cqe(&self, cqe: Cqe) -> Option<Entry> {
        if cqe.opcode.is_responder() {
            // RQ completion via SRQ
            self.rq.srq().process_recv_completion(cqe.wqe_counter)
        } else {
            // SQ completion
            self.sq.as_ref()?.process_completion(cqe.wqe_counter)
        }
    }
}

// CompletionTarget impl for RcQpForMonoCqWithSrq (for send CQ polling)
//
// This implementation allows the QP to be registered with a normal Cq for SQ completions
// while using MonoCq for RQ completions via SRQ.
impl<Entry: Clone + 'static> CompletionTarget for RcQpForMonoCqWithSrq<Entry> {
    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if !cqe.opcode.is_responder() {
            // SQ completion - update ci to free SQ slots
            if let Some(sq) = self.sq.as_ref() {
                let _ = sq.process_completion(cqe.wqe_counter);
            }
        }
        // RQ completions are handled by MonoCq via CompletionSource, not this method
    }
}

// CompletionTarget impl for RcQpForMonoCqWithSrqAndSqCb (with SQ callback)
//
// This implementation calls the SQ callback when SQ completions arrive.
impl<Entry: Clone + 'static, OnSq: Fn(Cqe, Entry)> CompletionTarget
    for RcQpForMonoCqWithSrqAndSqCb<Entry, OnSq>
{
    fn qpn(&self) -> u32 {
        RcQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if !cqe.opcode.is_responder() {
            // SQ completion - get entry and call callback
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
        // RQ completions are handled by MonoCq via CompletionSource, not this method
    }
}

// =============================================================================
// RQ BlueFlame Batch Builder
// =============================================================================

use crate::wqe::BLUEFLAME_BUFFER_SIZE;

/// RQ WQE size in bytes (single DataSeg).
const RQ_WQE_SIZE: usize = DATA_SEG_SIZE;

/// BlueFlame WQE batch builder for RC RQ.
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
pub struct RqBlueflameWqeBatch<'a, Entry> {
    rq: &'a ReceiveQueueState<Entry>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
    wqe_count: u16,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &'a Cell<u32>,
}

impl<'a, Entry> RqBlueflameWqeBatch<'a, Entry> {
    /// Create a new RQ BlueFlame batch builder.
    pub(crate) fn new(
        rq: &'a ReceiveQueueState<Entry>,
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
    /// Returns `RqFull` if the receive queue is full.
    /// Returns `BlueflameOverflow` if the batch buffer is full.
    #[inline]
    pub fn post(
        &mut self,
        entry: Entry,
        addr: u64,
        len: u32,
        lkey: u32,
    ) -> Result<(), SubmissionError> {
        if self.rq.available() == 0 {
            return Err(SubmissionError::RqFull);
        }
        if self.offset + RQ_WQE_SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }

        // Write data segment to buffer
        unsafe {
            write_data_seg(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }

        // Store entry in RQ table
        let wqe_idx = self.rq.pi.get();
        let idx = (wqe_idx as usize) & (self.rq.table.len() - 1);
        self.rq.table[idx].set(Some(entry));

        // Advance state
        self.offset += RQ_WQE_SIZE;
        self.wqe_count += 1;
        self.rq.advance_pi(1);

        Ok(())
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    #[inline]
    pub fn finish(self) {
        if self.wqe_count == 0 {
            return; // No WQEs to submit
        }

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
// QP Entry Points (OwnedRq - RQ methods)
// =============================================================================

impl<SqEntry, RqEntry, Transport, OnSqComplete, OnRqComplete>
    RcQp<
        SqEntry,
        RqEntry,
        Transport,
        OrderedWqeTable<SqEntry>,
        OwnedRq<RqEntry>,
        OnSqComplete,
        OnRqComplete,
    >
{
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
        let rq = self
            .rq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }
        let wqe_idx = rq.pi.get();
        let wqe_ptr = rq.get_wqe_ptr(wqe_idx);
        unsafe {
            write_data_seg(wqe_ptr, len, lkey, addr);

            if rq.stride > DATA_SEG_SIZE as u32 {
                let sentinel_ptr = wqe_ptr.add(DATA_SEG_SIZE);
                let ptr32 = sentinel_ptr as *mut u32;
                std::ptr::write_volatile(ptr32, 0u32);
                std::ptr::write_volatile(ptr32.add(1), MLX5_INVALID_LKEY.to_be());
            }
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
    pub fn blueflame_rq_batch(&self) -> Result<RqBlueflameWqeBatch<'_, RqEntry>, SubmissionError> {
        let rq = self.rq.as_ref().ok_or(SubmissionError::RqFull)?;
        let sq = self
            .sq
            .as_ref()
            .ok_or(SubmissionError::BlueflameNotAvailable)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(RqBlueflameWqeBatch::new(
            rq,
            sq.bf_reg,
            sq.bf_size,
            &sq.bf_offset,
        ))
    }
}

// =============================================================================
// QP Entry Points (emit_ctx - IB)
// =============================================================================

impl<SqEntry, RqEntry, Rq, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, Rq, OnSqComplete, OnRqComplete>
{
    #[doc(hidden)]
    #[inline]
    pub fn emit_ctx(&self) -> io::Result<EmitContext<'_, SqEntry>> {
        let sq = self
            .sq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))?;
        Ok(sq.emit_ctx())
    }
}

// =============================================================================
// RcQpBuilder - Builder Pattern for QP Creation
// =============================================================================

/// Marker type indicating CQ is not yet set.
pub struct NoCq;

/// Marker type indicating CQ has been set.
pub struct CqSet;

/// Builder for RC Queue Pairs.
///
/// # MonoCq vs Cq
///
/// ## Cq (normal CQ)
/// - Dispatches to QP via `dyn CompletionTarget` trait object
/// - Dynamic dispatch prevents callback inlining
/// - Flexible (different QP types can share same CQ)
///
/// ## MonoCq
/// - Statically binds Entry type and Callback type via generics
/// - Monomorphization enables complete inlining
/// - Use for high-throughput, low-latency scenarios
///
/// Both are wrapped in `Rc<T>` and configured via builder methods.
///
/// # Type Parameters
///
/// - `SqEntry`: Entry type stored in SQ WQE table
/// - `RqEntry`: Entry type stored in RQ WQE table
/// - `T`: Transport type (`InfiniBand` or `RoCE`)
/// - `Rq`: Receive queue type (`OwnedRq<RqEntry>` or `SharedRq<RqEntry>`)
/// - `SqCqState`: SQ CQ configuration state (`NoCq` or `CqSet`)
/// - `RqCqState`: RQ CQ configuration state (`NoCq` or `CqSet`)
/// - `OnSq`: SQ completion callback type
/// - `OnRq`: RQ completion callback type
pub struct RcQpBuilder<
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
    config: RcQpConfig,

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
    /// Create an RC QP Builder.
    ///
    /// CQs are configured via builder methods:
    /// - `.sq_cq(cq, callback)` - Normal CQ with callback
    /// - `.sq_mono_cq(mono_cq)` - MonoCq (callback is on CQ side, already monomorphized)
    /// - `.rq_cq(cq, callback)` / `.rq_mono_cq(mono_cq)` - Same for RQ
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Both normal CQs with callbacks
    /// let qp = ctx.rc_qp_builder::<u64, u64>(&pd, &config)
    ///     .sq_cq(send_cq.clone(), |cqe, entry| { /* SQ completion */ })
    ///     .rq_cq(recv_cq.clone(), |cqe, entry| { /* RQ completion */ })
    ///     .build()?;
    ///
    /// // Both MonoCq (high-performance mode)
    /// let qp = ctx.rc_qp_builder::<Entry, Entry>(&pd, &config)
    ///     .sq_mono_cq(mono_cq.clone())
    ///     .rq_mono_cq(mono_cq.clone())
    ///     .build()?;
    ///
    /// // With SRQ
    /// let qp = ctx.rc_qp_builder::<u64, u64>(&pd, &config)
    ///     .with_srq(srq.clone())
    ///     .sq_cq(send_cq.clone(), sq_callback)
    ///     .rq_cq(recv_cq.clone(), rq_callback)
    ///     .build()?;
    ///
    /// // For RoCE
    /// let qp = ctx.rc_qp_builder::<u64, u64>(&pd, &config)
    ///     .for_roce()
    ///     .sq_cq(send_cq.clone(), sq_callback)
    ///     .rq_cq(recv_cq.clone(), rq_callback)
    ///     .build()?;
    /// ```
    pub fn rc_qp_builder<'a, SqEntry, RqEntry>(
        &'a self,
        pd: &'a Pd,
        config: &RcQpConfig,
    ) -> RcQpBuilder<'a, SqEntry, RqEntry, InfiniBand, OwnedRq<RqEntry>, NoCq, NoCq, (), (), (), ()>
    {
        RcQpBuilder {
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
    RcQpBuilder<'a, SqEntry, RqEntry, T, Rq, NoCq, RqCqState, (), OnRq, (), RqMono>
{
    /// Set normal CQ for SQ with callback.
    pub fn sq_cq<OnSq>(
        self,
        cq: Rc<Cq>,
        callback: OnSq,
    ) -> RcQpBuilder<'a, SqEntry, RqEntry, T, Rq, CqSet, RqCqState, OnSq, OnRq, (), RqMono>
    where
        OnSq: Fn(Cqe, SqEntry) + 'static,
    {
        RcQpBuilder {
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
    ) -> RcQpBuilder<
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
        RcQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: mono_cq.cqn(),
            recv_cqn: self.recv_cqn,
            send_cq_weak: None, // MonoCq doesn't need CQ registration
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
    RcQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, NoCq, OnSq, (), SqMono, ()>
{
    /// Set normal CQ for RQ with callback.
    pub fn rq_cq<OnRq>(
        self,
        cq: Rc<Cq>,
        callback: OnRq,
    ) -> RcQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, CqSet, OnSq, OnRq, SqMono, ()>
    where
        OnRq: Fn(Cqe, RqEntry) + 'static,
    {
        RcQpBuilder {
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
    ) -> RcQpBuilder<
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
        RcQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: self.send_cqn,
            recv_cqn: mono_cq.cqn(),
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: None, // MonoCq doesn't need CQ registration
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
    RcQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, RqCqState, OnSq, OnRq, SqMono, RqMono>
{
    /// Switch to RoCE transport.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn for_roce(
        self,
    ) -> RcQpBuilder<'a, SqEntry, RqEntry, RoCE, Rq, SqCqState, RqCqState, OnSq, OnRq, SqMono, RqMono>
    {
        RcQpBuilder {
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
    RcQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        T,
        OwnedRq<RqEntry>,
        SqCqState,
        RqCqState,
        OnSq,
        OnRq,
        SqMono,
        RqMono,
    >
{
    /// Use Shared Receive Queue (SRQ) instead of owned RQ.
    ///
    /// When using SRQ, receive operations are handled through the SRQ
    /// and the QP's own RQ-related methods are not available.
    pub fn with_srq(
        self,
        srq: Rc<Srq<RqEntry>>,
    ) -> RcQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        T,
        SharedRq<RqEntry>,
        SqCqState,
        RqCqState,
        OnSq,
        OnRq,
        SqMono,
        RqMono,
    > {
        RcQpBuilder {
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
// Build helper: create SQ state from DevX QP resources
// =============================================================================

fn make_sq_state<SqEntry>(res: &DevxQpResources) -> Option<SendQueueState<SqEntry, OrderedWqeTable<SqEntry>>> {
    let wqe_cnt = 1u16 << res.layout.log_sq_size;
    Some(SendQueueState {
        buf: res.sq_buf,
        wqe_cnt,
        sqn: res.qpn, // SQN = QPN for RC QP
        pi: Cell::new(0),
        ci: Cell::new(0),
        last_wqe: Cell::new(None),
        dbrec: res.dbrec,
        bf_reg: res.bf_reg,
        bf_size: res.bf_size,
        bf_offset: Cell::new(0),
        table: OrderedWqeTable::new(wqe_cnt),
        _marker: std::marker::PhantomData,
    })
}

fn make_rq_state<RqEntry>(res: &DevxQpResources) -> OwnedRq<RqEntry> {
    let rq_wqe_cnt = 1u32 << res.layout.log_rq_size;
    OwnedRq::new(Some(ReceiveQueueState {
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
    }))
}

// =============================================================================
// Build Methods - InfiniBand + OwnedRq
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    RcQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        InfiniBand,
        OwnedRq<RqEntry>,
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
    /// Build the RC QP via DevX.
    pub fn build(self) -> BuildResult<RcQpIb<SqEntry, RqEntry, OnSq, OnRq>> {
        unsafe {
            let res = create_devx_rc_qp(self.ctx, self.pd, self.send_cqn, self.recv_cqn, &self.config, None)?;

            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let sq = make_sq_state(&res);
            let rq = make_rq_state(&res);

            let result = RcQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                _dbr_umem: res.dbr_umem,
                _dbr_buf: res.dbr_buf,
                qpn: res.qpn,
                state: Cell::new(QpState::Reset),
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
            register_with_cqs(qpn, &send_cq_for_register, &recv_cq_for_register, &qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - InfiniBand + SharedRq (SRQ)
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    RcQpBuilder<
        'a,
        SqEntry,
        RqEntry,
        InfiniBand,
        SharedRq<RqEntry>,
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
    /// Build the RC QP with SRQ via DevX.
    pub fn build(self) -> BuildResult<RcQpIbWithSrq<SqEntry, RqEntry, OnSq, OnRq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        let srqn = srq.srq_number()?;

        unsafe {
            let res = create_devx_rc_qp(self.ctx, self.pd, self.send_cqn, self.recv_cqn, &self.config, Some(srqn))?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let sq = make_sq_state(&res);

            let result = RcQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                _dbr_umem: res.dbr_umem,
                _dbr_buf: res.dbr_buf,
                qpn: res.qpn,
                state: Cell::new(QpState::Reset),
                sq,
                rq: SharedRq::new(srq_inner),
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
// Build Methods - InfiniBand + SharedRq (SRQ) with MonoCq for RQ
// =============================================================================

impl<'a, Entry, OnSq, RqMono>
    RcQpBuilder<'a, Entry, Entry, InfiniBand, SharedRq<Entry>, CqSet, CqSet, OnSq, (), (), RqMono>
where
    Entry: Clone + 'static,
    OnSq: Fn(Cqe, Entry) + 'static,
    RqMono: MaybeMonoCqRegister<RcQpForMonoCqWithSrqAndSqCb<Entry, OnSq>>,
{
    /// Build the RC QP with SRQ using MonoCq for recv via DevX.
    pub fn build(self) -> BuildResult<RcQpForMonoCqWithSrqAndSqCb<Entry, OnSq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        let srqn = srq.srq_number()?;
        let sq_callback = self.sq_callback;

        unsafe {
            let res = create_devx_rc_qp(self.ctx, self.pd, self.send_cqn, self.recv_cqn, &self.config, Some(srqn))?;

            let srq_inner: Srq<Entry> = (**srq).clone();
            let send_cq_for_register = self.send_cq_weak.clone();

            let sq = make_sq_state(&res);

            let result = RcQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                _dbr_umem: res.dbr_umem,
                _dbr_buf: res.dbr_buf,
                qpn: res.qpn,
                state: Cell::new(QpState::Reset),
                sq,
                rq: SharedRq::new(srq_inner),
                sq_callback,
                rq_callback: (),
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: Weak::new(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();
            register_with_send_cq(qpn, &send_cq_for_register, &qp_rc);
            self.rq_mono_cq_ref.maybe_register(&qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - RoCE + OwnedRq
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    RcQpBuilder<'a, SqEntry, RqEntry, RoCE, OwnedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq, (), ()>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the RC QP for RoCE via DevX.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<RcQpRoCE<SqEntry, RqEntry, OnSq, OnRq>> {
        unsafe {
            let res = create_devx_rc_qp(self.ctx, self.pd, self.send_cqn, self.recv_cqn, &self.config, None)?;

            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let sq = make_sq_state(&res);
            let rq = make_rq_state(&res);

            let result = RcQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                _dbr_umem: res.dbr_umem,
                _dbr_buf: res.dbr_buf,
                qpn: res.qpn,
                state: Cell::new(QpState::Reset),
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
            register_with_cqs(qpn, &send_cq_for_register, &recv_cq_for_register, &qp_rc);

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - RoCE + SharedRq (SRQ)
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    RcQpBuilder<'a, SqEntry, RqEntry, RoCE, SharedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq, (), ()>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the RC QP with SRQ for RoCE via DevX.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<RcQpRoCEWithSrq<SqEntry, RqEntry, OnSq, OnRq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        let srqn = srq.srq_number()?;

        unsafe {
            let res = create_devx_rc_qp(self.ctx, self.pd, self.send_cqn, self.recv_cqn, &self.config, Some(srqn))?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let sq = make_sq_state(&res);

            let result = RcQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                _dbr_umem: res.dbr_umem,
                _dbr_buf: res.dbr_buf,
                qpn: res.qpn,
                state: Cell::new(QpState::Reset),
                sq,
                rq: SharedRq::new(srq_inner),
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
    RcQpBuilder<'a, Entry, Entry, InfiniBand, OwnedRq<Entry>, CqSet, CqSet, (), (), SqMono, RqMono>
where
    Entry: 'static,
    SqMono: MaybeMonoCqRegister<RcQpForMonoCq<Entry>>,
    RqMono: MaybeMonoCqRegister<RcQpForMonoCq<Entry>>,
{
    /// Build the RC QP for use with MonoCq via DevX.
    pub fn build(self) -> BuildResult<RcQpForMonoCq<Entry>> {
        unsafe {
            let res = create_devx_rc_qp(self.ctx, self.pd, self.send_cqn, self.recv_cqn, &self.config, None)?;

            let sq = make_sq_state(&res);
            let rq = make_rq_state(&res);

            let result = RcQp {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                _dbr_umem: res.dbr_umem,
                _dbr_buf: res.dbr_buf,
                qpn: res.qpn,
                state: Cell::new(QpState::Reset),
                sq,
                rq,
                sq_callback: (),
                rq_callback: (),
                send_cq: Weak::new(),
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
