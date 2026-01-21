//! UD (Unreliable Datagram) transport.
//!
//! UD provides connectionless datagram service. Each message is independently
//! addressed using an Address Handle (AH). Messages are limited to a single MTU.
//!
//! Key characteristics:
//! - Connectionless: No QP state machine, no connection setup
//! - Unreliable: No retransmission, no guaranteed delivery
//! - Unordered: No message ordering guarantees
//! - One-to-many: Single QP can send to multiple destinations

pub mod builder;

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::{AddressHandle, Pd};
use crate::qp::QpInfo;
use crate::srq::Srq;
use crate::transport::{InfiniBand, RoCE};
use crate::wqe::{CTRL_SEG_SIZE, DATA_SEG_SIZE, OrderedWqeTable, WQEBB_SIZE, WqeOpcode, write_ctrl_seg, write_data_seg};

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

/// Send Queue state for UD QP.
///
/// Both table types use interior mutability (Cell) so no RefCell wrapper is needed.
pub(super) struct UdSendQueueState<Entry, TableType> {
    pub(super) buf: *mut u8,
    pub(super) wqe_cnt: u16,
    pub(super) sqn: u32,
    pub(super) pi: Cell<u16>,
    pub(super) ci: Cell<u16>,
    pub(super) last_wqe: Cell<Option<(*mut u8, usize)>>,
    pub(super) dbrec: *mut u32,
    pub(super) bf_reg: *mut u8,
    pub(super) bf_size: u32,
    pub(super) bf_offset: Cell<u32>,
    /// Maximum inline data size.
    pub(super) max_inline_data: u32,
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<Entry>>) so no RefCell needed.
    pub(super) table: TableType,
    _marker: PhantomData<Entry>,
}

impl<Entry, TableType> UdSendQueueState<Entry, TableType> {
    pub(super) fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    pub(super) fn max_inline_data(&self) -> u32 {
        self.max_inline_data
    }

    pub(super) fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    /// Returns the number of WQEBBs from current PI to the end of the ring buffer.
    pub(super) fn slots_to_end(&self) -> u16 {
        self.wqe_cnt - (self.pi.get() & (self.wqe_cnt - 1))
    }

    pub(super) fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    pub(super) fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Post a NOP WQE to fill the remaining slots until the ring end.
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots for the NOP WQE.
    pub(super) unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        debug_assert!(nop_wqebb_cnt >= 1, "NOP must be at least 1 WQEBB");
        debug_assert!(
            self.available() >= nop_wqebb_cnt,
            "Not enough slots for NOP"
        );

        let wqe_idx = self.pi.get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);

        // Write NOP control segment
        let ds_count = (nop_wqebb_cnt as u8) * 4;
        write_ctrl_seg(
            wqe_ptr,
            0, // opmod = 0 for NOP
            WqeOpcode::Nop as u8,
            wqe_idx,
            self.sqn,
            ds_count,
            0,
            0,
        );

        self.advance_pi(nop_wqebb_cnt);
        self.set_last_wqe(wqe_ptr, (nop_wqebb_cnt as usize) * WQEBB_SIZE);
    }

    fn ring_db(&self, wqe_ptr: *mut u8) {
        let bf = unsafe { self.bf_reg.add(self.bf_offset.get() as usize) as *mut u64 };
        let ctrl = wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset.set(self.bf_offset.get() ^ self.bf_size);
    }
}

impl<Entry> UdSendQueueState<Entry, OrderedWqeTable<Entry>> {
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        let entry = self.table.take(wqe_idx)?;
        // ci_delta is the accumulated PI value at completion
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }
}

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

    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, (self.pi.get() as u32).to_be());
        }
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
pub type UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    UdQp<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, UdOwnedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// UD QP with SRQ (InfiniBand).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type UdQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    UdQp<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, UdSharedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// UD QP with owned RQ (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
pub type UdQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    UdQp<SqEntry, RqEntry, RoCE, OrderedWqeTable<SqEntry>, UdOwnedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// UD QP with SRQ (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
pub type UdQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> =
    UdQp<SqEntry, RqEntry, RoCE, OrderedWqeTable<SqEntry>, UdSharedRq<RqEntry>, OnSqComplete, OnRqComplete>;

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
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: Cell<UdQpState>,
    qkey: u32,
    max_inline_data: u32,
    sq: Option<UdSendQueueState<SqEntry, TableType>>,
    rq: Rq,
    sq_callback: OnSqComplete,
    rq_callback: OnRqComplete,
    /// Weak reference to the send CQ for unregistration on drop.
    send_cq: Weak<CompletionQueue>,
    /// Weak reference to the recv CQ for unregistration on drop.
    recv_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this QP exists.
    _pd: Pd,
    /// Phantom for Transport and RqEntry types.
    _marker: std::marker::PhantomData<(Transport, RqEntry)>,
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> Drop for UdQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    fn drop(&mut self) {
        let qpn = self.qpn();
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

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> UdQp<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> UdQpState {
        self.state.get()
    }

    /// Get the Q_Key.
    pub fn qkey(&self) -> u32 {
        self.qkey
    }

    fn query_info(&self) -> io::Result<QpInfo> {
        unsafe {
            let mut dv_qp: MaybeUninit<mlx5_sys::mlx5dv_qp> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).qp.in_ = self.qp.as_ptr();
            (*obj_ptr).qp.out = dv_qp.as_mut_ptr();

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

    /// Transition UD QP from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, pkey_index: u16) -> io::Result<()> {
        if self.state.get() != UdQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RESET state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_INIT;
            attr.pkey_index = pkey_index;
            attr.port_num = port;
            attr.qkey = self.qkey;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PKEY_INDEX
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PORT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_QKEY;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

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

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

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

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTS;
            attr.sq_psn = sq_psn;

            let mask =
                mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE | mlx5_sys::ibv_qp_attr_mask_IBV_QP_SQ_PSN;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(UdQpState::Rts);
        Ok(())
    }

    fn sq(&self) -> io::Result<&UdSendQueueState<SqEntry, TableType>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))
    }
}

// =============================================================================
// OwnedRq-specific methods
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete> UdQp<SqEntry, RqEntry, Transport, TableType, UdOwnedRq<RqEntry>, OnSqComplete, OnRqComplete> {
    fn rq(&self) -> io::Result<&UdRecvQueueState<RqEntry>> {
        self.rq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))
    }

    /// Ring the receive queue doorbell.
    pub fn ring_rq_doorbell(&self) {
        if let Some(rq) = self.rq.as_ref() {
            rq.ring_doorbell();
        }
    }
}

// =============================================================================
// SharedRq-specific methods
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete> UdQp<SqEntry, RqEntry, Transport, TableType, UdSharedRq<RqEntry>, OnSqComplete, OnRqComplete> {
    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        self.rq.srq()
    }
}



impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Initialize direct queue access (internal implementation).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(UdSendQueueState {
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
            max_inline_data: self.max_inline_data,
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt as u16;
        self.rq = UdOwnedRq::new(Some(UdRecvQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        }));

        Ok(())
    }

    /// Activate UD QP (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, sq_psn: u32) -> io::Result<()> {
        self.modify_to_init(port, 0)?;
        self.modify_to_rtr()?;
        self.modify_to_rts(sq_psn)?;
        Ok(())
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Initialize direct queue access for SRQ variant (SQ only).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(UdSendQueueState {
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
            max_inline_data: self.max_inline_data,
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        // Note: RQ is not initialized for SRQ variant - SRQ handles receives
        Ok(())
    }

    /// Activate UD QP (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, sq_psn: u32) -> io::Result<()> {
        self.modify_to_init(port, 0)?;
        self.modify_to_rtr()?;
        self.modify_to_rts(sq_psn)?;
        Ok(())
    }

}

// =============================================================================
// CompletionTarget Implementation (OwnedRq)
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
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

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for UdQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
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
            if let Some(entry) = self.rq.srq().process_recv_completion(cqe.wqe_counter)
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
// RoCE init_direct_access_internal implementations
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Initialize direct queue access for RoCE (same as IB).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(UdSendQueueState {
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
            max_inline_data: self.max_inline_data,
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt as u16;
        self.rq = UdOwnedRq::new(Some(UdRecvQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        }));

        Ok(())
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Initialize direct queue access for RoCE with SRQ (SQ only).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(UdSendQueueState {
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
            max_inline_data: self.max_inline_data,
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        // Note: RQ is not initialized for SRQ variant - SRQ handles receives
        Ok(())
    }
}

// =============================================================================
// CompletionTarget impl for RoCE variants
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for UdQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
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

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for UdQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
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
            if let Some(entry) = self.rq.srq().process_recv_completion(cqe.wqe_counter)
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
// UdQpBuilder - Builder Pattern for UD QP Creation
// =============================================================================

use crate::qp::{NoCq, CqSet};

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
pub struct UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, RqCqState, OnSq, OnRq> {
    ctx: &'a Context,
    pd: &'a Pd,
    config: UdQpConfig,

    // CQ pointers (set via sq_cq/sq_mono_cq/rq_cq/rq_mono_cq)
    send_cq_ptr: *mut mlx5_sys::ibv_cq,
    recv_cq_ptr: *mut mlx5_sys::ibv_cq,

    // Weak references to normal CQs for registration (None for MonoCq)
    send_cq_weak: Option<Weak<CompletionQueue>>,
    recv_cq_weak: Option<Weak<CompletionQueue>>,

    // Callbacks (set via sq_cq/rq_cq, () for MonoCq)
    sq_callback: OnSq,
    rq_callback: OnRq,

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
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, InfiniBand, UdOwnedRq<RqEntry>, NoCq, NoCq, (), ()> {
        UdQpBuilder {
            ctx: self,
            pd,
            config: config.clone(),
            send_cq_ptr: std::ptr::null_mut(),
            recv_cq_ptr: std::ptr::null_mut(),
            send_cq_weak: None,
            recv_cq_weak: None,
            sq_callback: (),
            rq_callback: (),
            srq: None,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// SQ CQ Configuration Methods
// =============================================================================

impl<'a, SqEntry, RqEntry, T, Rq, RqCqState, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, NoCq, RqCqState, (), OnRq>
{
    /// Set normal CQ for SQ with callback.
    pub fn sq_cq<OnSq>(
        self,
        cq: Rc<CompletionQueue>,
        callback: OnSq,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, CqSet, RqCqState, OnSq, OnRq>
    where
        OnSq: Fn(Cqe, SqEntry) + 'static,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: cq.as_ptr(),
            recv_cq_ptr: self.recv_cq_ptr,
            send_cq_weak: Some(Rc::downgrade(&cq)),
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: callback,
            rq_callback: self.rq_callback,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }

    /// Set MonoCq for SQ (callback is on CQ side).
    pub fn sq_mono_cq<Q, F>(
        self,
        mono_cq: &Rc<crate::mono_cq::MonoCq<Q, F>>,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, CqSet, RqCqState, (), OnRq>
    where
        Q: crate::mono_cq::CompletionSource,
        F: Fn(Cqe, Q::Entry) + 'static,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: mono_cq.as_ptr(),
            recv_cq_ptr: self.recv_cq_ptr,
            send_cq_weak: None,
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: (),
            rq_callback: self.rq_callback,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// RQ CQ Configuration Methods
// =============================================================================

impl<'a, SqEntry, RqEntry, T, Rq, SqCqState, OnSq>
    UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, NoCq, OnSq, ()>
{
    /// Set normal CQ for RQ with callback.
    pub fn rq_cq<OnRq>(
        self,
        cq: Rc<CompletionQueue>,
        callback: OnRq,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, CqSet, OnSq, OnRq>
    where
        OnRq: Fn(Cqe, RqEntry) + 'static,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: cq.as_ptr(),
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: Some(Rc::downgrade(&cq)),
            sq_callback: self.sq_callback,
            rq_callback: callback,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }

    /// Set MonoCq for RQ (callback is on CQ side).
    pub fn rq_mono_cq<Q, F>(
        self,
        mono_cq: &Rc<crate::mono_cq::MonoCq<Q, F>>,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, CqSet, OnSq, ()>
    where
        Q: crate::mono_cq::CompletionSource,
        F: Fn(Cqe, Q::Entry) + 'static,
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: mono_cq.as_ptr(),
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: None,
            sq_callback: self.sq_callback,
            rq_callback: (),
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// Transport / RQ Type Transition Methods
// =============================================================================

impl<'a, SqEntry, RqEntry, T, Rq, SqCqState, RqCqState, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, T, Rq, SqCqState, RqCqState, OnSq, OnRq>
{
    /// Switch to RoCE transport.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn for_roce(
        self,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, RoCE, Rq, SqCqState, RqCqState, OnSq, OnRq> {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: self.recv_cq_ptr,
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: self.sq_callback,
            rq_callback: self.rq_callback,
            srq: self.srq,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, SqEntry, RqEntry, T, SqCqState, RqCqState, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, T, UdOwnedRq<RqEntry>, SqCqState, RqCqState, OnSq, OnRq>
{
    /// Use Shared Receive Queue (SRQ) instead of owned RQ.
    pub fn with_srq(
        self,
        srq: Rc<Srq<RqEntry>>,
    ) -> UdQpBuilder<'a, SqEntry, RqEntry, T, UdSharedRq<RqEntry>, SqCqState, RqCqState, OnSq, OnRq>
    {
        UdQpBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: self.send_cq_ptr,
            recv_cq_ptr: self.recv_cq_ptr,
            send_cq_weak: self.send_cq_weak,
            recv_cq_weak: self.recv_cq_weak,
            sq_callback: self.sq_callback,
            rq_callback: self.rq_callback,
            srq: Some(srq),
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// Build Methods - InfiniBand + OwnedRq
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, InfiniBand, UdOwnedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP.
    pub fn build(self) -> io::Result<Rc<RefCell<UdQpIb<SqEntry, RqEntry, OnSq, OnRq>>>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_UD;
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = UdQp {
                qp,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                max_inline_data: self.config.max_inline_data,
                sq: None,
                rq: UdOwnedRq::new(None),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_else(Weak::new),
                recv_cq: self.recv_cq_weak.unwrap_or_else(Weak::new),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            UdQpIb::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal CompletionQueue
            if let Some(cq) = send_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }
            if let Some(cq) = recv_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - InfiniBand + SharedRq (SRQ)
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, InfiniBand, UdSharedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP with SRQ.
    pub fn build(self) -> io::Result<Rc<RefCell<UdQpIbWithSrq<SqEntry, RqEntry, OnSq, OnRq>>>> {
        let srq = self.srq.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set")
        })?;

        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_UD;
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = UdQp {
                qp,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                max_inline_data: self.config.max_inline_data,
                sq: None,
                rq: UdSharedRq::new(srq_inner),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_else(Weak::new),
                recv_cq: self.recv_cq_weak.unwrap_or_else(Weak::new),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            UdQpIbWithSrq::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal CompletionQueue
            if let Some(cq) = send_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }
            if let Some(cq) = recv_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - RoCE + OwnedRq
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, RoCE, UdOwnedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP for RoCE.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> io::Result<Rc<RefCell<UdQpRoCE<SqEntry, RqEntry, OnSq, OnRq>>>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_UD;
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = UdQp {
                qp,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                max_inline_data: self.config.max_inline_data,
                sq: None,
                rq: UdOwnedRq::new(None),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_else(Weak::new),
                recv_cq: self.recv_cq_weak.unwrap_or_else(Weak::new),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            UdQpRoCE::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            // Register with CQs if using normal CompletionQueue
            if let Some(cq) = send_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }
            if let Some(cq) = recv_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }

            Ok(qp_rc)
        }
    }
}

// =============================================================================
// Build Methods - RoCE + SharedRq (SRQ)
// =============================================================================

impl<'a, SqEntry, RqEntry, OnSq, OnRq>
    UdQpBuilder<'a, SqEntry, RqEntry, RoCE, UdSharedRq<RqEntry>, CqSet, CqSet, OnSq, OnRq>
where
    SqEntry: 'static,
    RqEntry: 'static,
    OnSq: Fn(Cqe, SqEntry) + 'static,
    OnRq: Fn(Cqe, RqEntry) + 'static,
{
    /// Build the UD QP with SRQ for RoCE.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn build(self) -> io::Result<Rc<RefCell<UdQpRoCEWithSrq<SqEntry, RqEntry, OnSq, OnRq>>>> {
        let srq = self.srq.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "SRQ not set")
        })?;

        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_UD;
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let srq_inner: Srq<RqEntry> = (**srq).clone();

            // Clone weak references before moving into UdQp
            let send_cq_for_register = self.send_cq_weak.clone();
            let recv_cq_for_register = self.recv_cq_weak.clone();

            let mut result = UdQp {
                qp,
                state: Cell::new(UdQpState::Reset),
                qkey: self.config.qkey,
                max_inline_data: self.config.max_inline_data,
                sq: None,
                rq: UdSharedRq::new(srq_inner),
                sq_callback: self.sq_callback,
                rq_callback: self.rq_callback,
                send_cq: self.send_cq_weak.unwrap_or_else(Weak::new),
                recv_cq: self.recv_cq_weak.unwrap_or_else(Weak::new),
                _pd: self.pd.clone(),
                _marker: std::marker::PhantomData,
            };

            UdQpRoCEWithSrq::<SqEntry, RqEntry, OnSq, OnRq>::init_direct_access_internal(&mut result)?;

            let qp_rc = Rc::new(RefCell::new(result));
            let qpn = qp_rc.borrow().qpn();

            if let Some(cq) = send_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }
            if let Some(cq) = recv_cq_for_register.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
            }

            Ok(qp_rc)
        }
    }
}
