//! Queue Pair (QP) management.
//!
//! Queue Pairs are the fundamental communication endpoints in RDMA.
//! This module provides RC (Reliable Connection) QP creation using mlx5dv_create_qp.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::BuildResult;
use crate::CompletionTarget;
use crate::builder_common::{MaybeMonoCqRegister, register_with_cqs, register_with_send_cq};
use crate::cq::{Cq, Cqe};
use crate::device::Context;
use crate::pd::Pd;
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

/// QP internal info obtained from mlx5dv_init_obj.
#[derive(Debug)]
pub(crate) struct QpInfo {
    /// Doorbell record pointer.
    pub(crate) dbrec: *mut u32,
    /// Send Queue buffer pointer.
    pub(crate) sq_buf: *mut u8,
    /// Send Queue WQE count.
    pub(crate) sq_wqe_cnt: u32,
    /// Send Queue stride (bytes per WQE slot).
    #[allow(dead_code)]
    pub(crate) sq_stride: u32,
    /// Receive Queue buffer pointer.
    pub(crate) rq_buf: *mut u8,
    /// Receive Queue WQE count.
    pub(crate) rq_wqe_cnt: u32,
    /// Receive Queue stride.
    pub(crate) rq_stride: u32,
    /// BlueFlame register pointer.
    pub(crate) bf_reg: *mut u8,
    /// BlueFlame size.
    pub(crate) bf_size: u32,
    /// Send Queue Number.
    pub(crate) sqn: u32,
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

    /// Ring the doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes to BlueFlame register.
    /// The NIC fetches remaining WQE data via DMA.
    /// Also resets pending WQE tracking.
    #[inline]
    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, (self.pi.get() as u32).to_be());
        }
        udma_to_device_barrier!();

        // Minimum 8-byte BF write (first 8 bytes of last WQE)
        let bf_offset = self.bf_offset.get();
        let bf = unsafe { self.bf_reg.add(bf_offset as usize) as *mut u64 };
        let last_wqe = self.get_wqe_ptr(self.pi.get().wrapping_sub(1));
        unsafe {
            std::ptr::write_volatile(bf, *(last_wqe as *const u64));
        }

        mmio_flush_writes!();
        self.bf_offset.set(bf_offset ^ self.bf_size);

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
    qp: NonNull<mlx5_sys::ibv_qp>,
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
        // Unregister from both CQs before destroying QP
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        if let Some(cq) = self.recv_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
    RcQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete>
{
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> QpState {
        self.state.get()
    }

    /// Get mlx5-specific QP information for direct WQE access.
    fn query_info(&self) -> io::Result<QpInfo> {
        unsafe {
            let mut dv_qp: MaybeUninit<mlx5_sys::mlx5dv_qp> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let dv_qp_ptr = dv_qp.as_mut_ptr();
            (*dv_qp_ptr).comp_mask =
                mlx5_sys::mlx5dv_qp_comp_mask_MLX5DV_QP_MASK_RAW_QP_HANDLES as u64;

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).qp.in_ = self.qp.as_ptr();
            (*obj_ptr).qp.out = dv_qp_ptr;

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_QP as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_qp = dv_qp.assume_init();
            let qp_num = (*self.qp.as_ptr()).qp_num;
            let sqn = if dv_qp.sqn != 0 { dv_qp.sqn } else { qp_num };

            Ok(QpInfo {
                dbrec: dv_qp.dbrec,
                sq_buf: dv_qp.sq.buf as *mut u8,
                sq_wqe_cnt: dv_qp.sq.wqe_cnt,
                sq_stride: dv_qp.sq.stride,
                rq_buf: dv_qp.rq.buf as *mut u8,
                rq_wqe_cnt: dv_qp.rq.wqe_cnt,
                rq_stride: dv_qp.rq.stride,
                bf_reg: dv_qp.bf.reg as *mut u8,
                bf_size: dv_qp.bf.size,
                sqn,
            })
        }
    }

    /// Transition QP from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, access_flags: u32) -> io::Result<()> {
        if self.state.get() != QpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RESET state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_INIT;
            attr.pkey_index = 0;
            attr.port_num = port;
            attr.qp_access_flags = access_flags;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PKEY_INDEX
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PORT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_ACCESS_FLAGS;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Init);
        Ok(())
    }

    /// Transition QP from RTR to RTS (Ready to Send).
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state.get() != QpState::Rtr {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RTR state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTS;
            attr.timeout = 14;
            attr.retry_cnt = 7;
            attr.rnr_retry = 7;
            attr.sq_psn = local_psn;
            attr.max_rd_atomic = max_rd_atomic;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_TIMEOUT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RETRY_CNT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RNR_RETRY
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_SQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_QP_RD_ATOMIC;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Rts);
        Ok(())
    }

    /// Transition QP to ERROR state.
    ///
    /// This cleanly tears down the connection and flushes any pending work requests.
    /// Useful before destroying a QP to avoid crashes when the remote QP is already gone.
    pub fn modify_to_error(&self) -> io::Result<()> {
        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_ERR;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

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
    /// Transition QP from INIT to RTR (Ready to Receive).
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

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.dest_qp_num = remote.qp_number;
            attr.rq_psn = remote.packet_sequence_number;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;
            attr.ah_attr.dlid = remote.local_identifier;
            attr.ah_attr.sl = 0;
            attr.ah_attr.src_path_bits = 0;
            attr.ah_attr.is_global = 0;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_DEST_QPN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

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
    /// Transition QP from INIT to RTR (Ready to Receive).
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

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.dest_qp_num = remote.qp_number;
            attr.rq_psn = remote.packet_sequence_number;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;

            // RoCE: use GRH (Global Route Header)
            attr.ah_attr.is_global = 1;
            attr.ah_attr.port_num = port;
            attr.ah_attr.dlid = 0; // Not used for RoCE

            // GRH configuration
            attr.ah_attr.grh.dgid.raw = remote.grh.dgid.raw;
            attr.ah_attr.grh.sgid_index = remote.grh.sgid_index;
            attr.ah_attr.grh.flow_label = remote.grh.flow_label;
            attr.ah_attr.grh.traffic_class = remote.grh.traffic_class;
            attr.ah_attr.grh.hop_limit = remote.grh.hop_limit;

            // Service level and source path bits
            attr.ah_attr.sl = 0;
            attr.ah_attr.src_path_bits = 0;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_DEST_QPN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

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

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
    RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
{
    /// Initialize direct queue access (internal implementation).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt;
        self.rq = OwnedRq::new(Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            pending_start_ptr: Cell::new(None),
            pending_wqe_count: Cell::new(0),
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        }));

        Ok(())
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
    RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
{
    /// Initialize direct queue access for SRQ variant (SQ only).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        // Note: RQ is not initialized for SRQ variant - SRQ handles receives
        Ok(())
    }
}

// =============================================================================
// RoCE init_direct_access_internal implementations
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
    RcQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
{
    /// Initialize direct queue access for RoCE (same as IB).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt;
        self.rq = OwnedRq::new(Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            pending_start_ptr: Cell::new(None),
            pending_wqe_count: Cell::new(0),
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        }));

        Ok(())
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
    RcQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
{
    /// Initialize direct queue access for RoCE with SRQ (SQ only).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        // Note: RQ is not initialized for SRQ variant - SRQ handles receives
        Ok(())
    }
}

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

    // CQ pointers (set via sq_cq/sq_mono_cq/rq_cq/rq_mono_cq)
    send_cq_ptr: *mut mlx5_sys::ibv_cq,
    recv_cq_ptr: *mut mlx5_sys::ibv_cq,

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
            send_cq_ptr: std::ptr::null_mut(),
            recv_cq_ptr: std::ptr::null_mut(),
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
            send_cq_ptr: cq.as_ptr(),
            recv_cq_ptr: self.recv_cq_ptr,
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
    pub fn sq_mono_cq<Q, F>(
        self,
        mono_cq: &Rc<crate::mono_cq::MonoCq<Q, F>>,
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
        Rc<crate::mono_cq::MonoCq<Q, F>>,
        RqMono,
    >
    where
        Q: crate::mono_cq::CompletionSource,
        F: Fn(Cqe, Q::Entry) + 'static,
    {
        RcQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: mono_cq.as_ptr(),
            recv_cq_ptr: self.recv_cq_ptr,
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
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: cq.as_ptr(),
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
    pub fn rq_mono_cq<Q, F>(
        self,
        mono_cq: &Rc<crate::mono_cq::MonoCq<Q, F>>,
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
        Rc<crate::mono_cq::MonoCq<Q, F>>,
    >
    where
        Q: crate::mono_cq::CompletionSource,
        F: Fn(Cqe, Q::Entry) + 'static,
    {
        RcQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: mono_cq.as_ptr(),
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
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: self.recv_cq_ptr,
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
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: self.recv_cq_ptr,
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
    /// Build the RC QP.
    ///
    /// Only available when both SQ and RQ CQs are configured.
    pub fn build(self) -> BuildResult<RcQpIb<SqEntry, RqEntry, OnSq, OnRq>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = self.config.max_recv_wr;
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = self.config.max_recv_sge;
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            if !self.config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Clone weak references before moving into RcQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = RcQp {
                qp,
                state: Cell::new(QpState::Reset),
                sq: None,
                rq: OwnedRq::new(None),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            RcQpIb::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal Cq
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
    /// Build the RC QP with SRQ.
    ///
    /// Only available when both SQ and RQ CQs are configured.
    pub fn build(self) -> BuildResult<RcQpIbWithSrq<SqEntry, RqEntry, OnSq, OnRq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            if !self.config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Clone the inner Srq from Rc<Srq<RqEntry>>
            let srq_inner: Srq<RqEntry> = (**srq).clone();

            // Clone weak references before moving into RcQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = RcQp {
                qp,
                state: Cell::new(QpState::Reset),
                sq: None,
                rq: SharedRq::new(srq_inner),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            RcQpIbWithSrq::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(
                &mut result,
            )?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal Cq
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
    /// Build the RC QP with SRQ using MonoCq for recv.
    ///
    /// This variant is for when RQ uses MonoCq (callback on CQ side, not QP).
    /// Only available when SQ uses normal CQ with callback and RQ uses MonoCq.
    /// The SQ callback will be invoked for SQ completions (e.g., RDMA READ).
    pub fn build(self) -> BuildResult<RcQpForMonoCqWithSrqAndSqCb<Entry, OnSq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        let sq_callback = self.sq_callback;

        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            if !self.config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Clone the inner Srq from Rc<Srq<Entry>>
            let srq_inner: Srq<Entry> = (**srq).clone();

            // Clone weak reference for CQ registration before moving into RcQp
            let send_cq_for_register = self.send_cq_weak.clone();

            let mut result = RcQp {
                qp,
                state: Cell::new(QpState::Reset),
                sq: None,
                rq: SharedRq::new(srq_inner),
                sq_callback,
                rq_callback: (),
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: Weak::new(), // MonoCq doesn't need CQ registration
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            RcQpForMonoCqWithSrqAndSqCb::<Entry, OnSq>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with send CQ for SQ completion processing
            register_with_send_cq(qpn, &send_cq_for_register, &qp_rc);

            // Auto-register with recv MonoCq
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
    /// Build the RC QP for RoCE.
    ///
    /// Only available when both SQ and RQ CQs are configured.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<RcQpRoCE<SqEntry, RqEntry, OnSq, OnRq>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = self.config.max_recv_wr;
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = self.config.max_recv_sge;
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            if !self.config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Clone weak references before moving into RcQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = RcQp {
                qp,
                state: Cell::new(QpState::Reset),
                sq: None,
                rq: OwnedRq::new(None),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            // Initialize direct access for RoCE (same as IB)
            RcQpRoCE::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(&mut result)?;

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
    RcQpBuilder<'a, SqEntry, RqEntry, RoCE, SharedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq, (), ()>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the RC QP with SRQ for RoCE.
    ///
    /// Only available when both SQ and RQ CQs are configured.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> BuildResult<RcQpRoCEWithSrq<SqEntry, RqEntry, OnSq, OnRq>> {
        let srq = self
            .srq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set"))?;

        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            if !self.config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();

            // Clone weak references before moving into RcQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = RcQp {
                qp,
                state: Cell::new(QpState::Reset),
                sq: None,
                rq: SharedRq::new(srq_inner),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_default(),
                recv_cq: self.recv_cq_weak.unwrap_or_default(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            RcQpRoCEWithSrq::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(
                &mut result,
            )?;

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
    /// Build the RC QP for use with MonoCq.
    ///
    /// When using MonoCq, callbacks are stored in the MonoCq, not in the RcQp.
    /// This method is available when both SQ and RQ use MonoCq.
    pub fn build(self) -> BuildResult<RcQpForMonoCq<Entry>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = self.config.max_recv_wr;
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = self.config.max_recv_sge;
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            if !self.config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = RcQp {
                qp,
                state: Cell::new(QpState::Reset),
                sq: None,
                rq: OwnedRq::new(None),
                sq_callback: (),
                rq_callback: (),
                send_cq: Weak::new(), // MonoCq doesn't use Cq registration
                recv_cq: Weak::new(),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            RcQpIb::<Entry, Entry, (), ()>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));

            // Auto-register with MonoCqs
            self.sq_mono_cq_ref.maybe_register(&qp_rc);
            self.rq_mono_cq_ref.maybe_register(&qp_rc);

            Ok(qp_rc)
        }
    }
}
