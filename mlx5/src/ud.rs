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

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::{AddressHandle, Pd};
use crate::qp::QpInfo;
use crate::srq::Srq;
use crate::wqe::{
    CtrlSeg, DataSeg, HasData, InlineHeader, NoData, OrderedWqeTable,
    SubmissionError, TxFlags, WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
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
struct UdSendQueueState<Entry, TableType> {
    buf: *mut u8,
    wqe_cnt: u16,
    sqn: u32,
    pi: Cell<u16>,
    ci: Cell<u16>,
    last_wqe: Cell<Option<(*mut u8, usize)>>,
    dbrec: *mut u32,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: Cell<u32>,
    /// Maximum inline data size.
    max_inline_data: u32,
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<Entry>>) so no RefCell needed.
    table: TableType,
    _marker: PhantomData<Entry>,
}

impl<Entry, TableType> UdSendQueueState<Entry, TableType> {
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    fn max_inline_data(&self) -> u32 {
        self.max_inline_data
    }

    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    /// Returns the number of WQEBBs from current PI to the end of the ring buffer.
    fn slots_to_end(&self) -> u16 {
        self.wqe_cnt - (self.pi.get() & (self.wqe_cnt - 1))
    }

    fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Post a NOP WQE to fill the remaining slots until the ring end.
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots for the NOP WQE.
    unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        debug_assert!(nop_wqebb_cnt >= 1, "NOP must be at least 1 WQEBB");
        debug_assert!(
            self.available() >= nop_wqebb_cnt,
            "Not enough slots for NOP"
        );

        let wqe_idx = self.pi.get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);

        // Write NOP control segment
        let ds_count = (nop_wqebb_cnt as u8) * 4;
        CtrlSeg::write(
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
struct UdRecvQueueState<Entry> {
    buf: *mut u8,
    wqe_cnt: u16,
    stride: u32,
    pi: Cell<u16>,
    ci: Cell<u16>,
    dbrec: *mut u32,
    /// Entry table (all WQEs are signaled, uses Cell for interior mutability)
    table: Box<[Cell<Option<Entry>>]>,
}

impl<Entry> UdRecvQueueState<Entry> {
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
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
    fn available(&self) -> u32 {
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
            DataSeg::write(wqe_ptr, len, lkey, addr);
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

/// Type alias for UD QP with ordered WQE table and owned RQ.
pub type UdQpWithTable<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = UdQp<SqEntry, RqEntry, OrderedWqeTable<SqEntry>, UdOwnedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// Type alias for UD QP with ordered WQE table and SRQ.
pub type UdQpWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = UdQp<SqEntry, RqEntry, OrderedWqeTable<SqEntry>, UdSharedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// UD (Unreliable Datagram) Queue Pair.
///
/// Provides connectionless datagram service. Each send operation requires
/// specifying the destination via an Address Handle.
///
/// Type parameter `SqEntry` is the entry type stored in SQ WQE table.
/// Type parameter `RqEntry` is the entry type stored in RQ WQE table.
/// Type parameter `TableType` determines WQE table behavior.
/// Type parameter `Rq` is the receive queue type (`UdOwnedRq<RqEntry>` or `UdSharedRq<RqEntry>`).
/// Type parameter `OnSqComplete` is the SQ completion callback type.
/// Type parameter `OnRqComplete` is the RQ completion callback type.
pub struct UdQp<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> {
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
    /// Phantom for RqEntry type.
    _marker: std::marker::PhantomData<RqEntry>,
}

impl Context {
    /// Create a UD QP.
    ///
    /// Only signaled WQEs have entries stored in the WQE table.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_ud_qp<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &UdQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<Rc<RefCell<UdQpWithTable<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>>
    where
        SqEntry: 'static,
        RqEntry: 'static,
        OnSqComplete: Fn(Cqe, SqEntry) + 'static,
        OnRqComplete: Fn(Cqe, RqEntry) + 'static,
    {
        let qp = self.create_ud_qp_raw(pd, send_cq, recv_cq, config, sq_callback, rq_callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    fn create_ud_qp_raw<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &UdQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<UdQpWithTable<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>
    where
        OnSqComplete: Fn(Cqe, SqEntry),
        OnRqComplete: Fn(Cqe, RqEntry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_UD;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = recv_cq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = config.max_recv_wr;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = config.max_recv_sge;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = UdQp {
                qp,
                state: Cell::new(UdQpState::Reset),
                qkey: config.qkey,
                max_inline_data: config.max_inline_data,
                sq: None,
                rq: UdOwnedRq::new(None),
                sq_callback,
                rq_callback,
                send_cq: Rc::downgrade(send_cq),
                recv_cq: Rc::downgrade(recv_cq),
                _pd: pd.clone(),
                _marker: std::marker::PhantomData,
            };

            // Auto-initialize direct access
            UdQpWithTable::<SqEntry, RqEntry, OnSqComplete, OnRqComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }

    /// Create a UD QP with SRQ.
    ///
    /// This variant uses a Shared Receive Queue (SRQ) for receive operations.
    /// The QP's own RQ-related methods (`post_recv`, `ring_rq_doorbell`, etc.)
    /// are not available. Instead, use the SRQ's methods for receive operations.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions
    /// * `recv_cq` - Completion Queue for receive completions
    /// * `srq` - Shared Receive Queue
    /// * `config` - QP configuration (max_recv_wr and max_recv_sge are ignored)
    /// * `sq_callback` - SQ completion callback
    /// * `rq_callback` - RQ completion callback
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    pub fn create_ud_qp_with_srq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        srq: &Srq<RqEntry>,
        config: &UdQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<Rc<RefCell<UdQpWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>>
    where
        SqEntry: 'static,
        RqEntry: 'static,
        OnSqComplete: Fn(Cqe, SqEntry) + 'static,
        OnRqComplete: Fn(Cqe, RqEntry) + 'static,
    {
        let qp = self.create_ud_qp_with_srq_raw(pd, send_cq, recv_cq, srq, config, sq_callback, rq_callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    fn create_ud_qp_with_srq_raw<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        srq: &Srq<RqEntry>,
        config: &UdQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<UdQpWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>
    where
        OnSqComplete: Fn(Cqe, SqEntry),
        OnRqComplete: Fn(Cqe, RqEntry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_UD;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = recv_cq.as_ptr();
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = UdQp {
                qp,
                state: Cell::new(UdQpState::Reset),
                qkey: config.qkey,
                max_inline_data: config.max_inline_data,
                sq: None,
                rq: UdSharedRq::new(srq.clone()),
                sq_callback,
                rq_callback,
                send_cq: Rc::downgrade(send_cq),
                recv_cq: Rc::downgrade(recv_cq),
                _pd: pd.clone(),
                _marker: std::marker::PhantomData,
            };

            // Initialize SQ direct access (no RQ for SRQ variant)
            UdQpWithSrq::<SqEntry, RqEntry, OnSqComplete, OnRqComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }
}

impl<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> Drop for UdQp<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> {
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

impl<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> UdQp<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> {
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

impl<SqEntry, RqEntry, TableType, OnSqComplete, OnRqComplete> UdQp<SqEntry, RqEntry, TableType, UdOwnedRq<RqEntry>, OnSqComplete, OnRqComplete> {
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

impl<SqEntry, RqEntry, TableType, OnSqComplete, OnRqComplete> UdQp<SqEntry, RqEntry, TableType, UdSharedRq<RqEntry>, OnSqComplete, OnRqComplete> {
    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        self.rq.srq()
    }
}



impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpWithTable<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
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

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
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

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for UdQpWithTable<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
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

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for UdQpWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
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
// New SQ WQE Builder API
// =============================================================================

/// Calculate maximum WQEBB count for UD SEND operation.
///
/// Layout: ctrl(16) + ud_addr(48) + inline_data(padded)
#[inline]
fn calc_ud_max_wqebb_send(max_inline_data: u32) -> u16 {
    let inline_padded = ((4 + max_inline_data as usize) + 15) & !15;
    let size = CtrlSeg::SIZE + UdAddressSeg::SIZE + inline_padded;
    calc_wqebb_cnt(size)
}

/// Internal WQE builder core for UD operations.
struct UdWqeCore<'a, Entry> {
    sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    signaled: bool,
    entry: Option<Entry>,
}

impl<'a, Entry> UdWqeCore<'a, Entry> {
    #[inline]
    fn new(sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>, entry: Option<Entry>) -> Self {
        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);
        Self {
            sq,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            signaled: entry.is_some(),
            entry,
        }
    }

    #[inline]
    fn available(&self) -> u16 {
        self.sq.available()
    }

    #[inline]
    fn max_inline_data(&self) -> u32 {
        self.sq.max_inline_data()
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                0,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_ud_av(&mut self, ah: &AddressHandle, qkey: u32) {
        unsafe {
            UdAddressSeg::write(self.wqe_ptr.add(self.offset), ah, qkey);
        }
        self.offset += UdAddressSeg::SIZE;
        self.ds_count += 3; // 48 bytes = 3 DS
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
    }

    #[inline]
    fn finish_internal(self) -> io::Result<WqeHandle> {
        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let slots_to_end = self.sq.slots_to_end();

        if wqebb_cnt > slots_to_end && slots_to_end < self.sq.wqe_cnt {
            return self.finish_with_wrap_around(wqebb_cnt, slots_to_end);
        }

        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(self.wqe_ptr);
            }
        }

        let wqe_idx = self.wqe_idx;
        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx,
            size: self.offset,
        })
    }

    #[cold]
    fn finish_with_wrap_around(self, wqebb_cnt: u16, slots_to_end: u16) -> io::Result<WqeHandle> {
        let total_needed = slots_to_end + wqebb_cnt;
        if self.sq.available() < total_needed {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let mut temp_buf = [0u8; 256];
        unsafe {
            std::ptr::copy_nonoverlapping(self.wqe_ptr, temp_buf.as_mut_ptr(), self.offset);
            self.sq.post_nop(slots_to_end);
        }

        let new_wqe_idx = self.sq.pi.get();
        let new_wqe_ptr = self.sq.get_wqe_ptr(new_wqe_idx);

        unsafe {
            std::ptr::copy_nonoverlapping(temp_buf.as_ptr(), new_wqe_ptr, self.offset);
            CtrlSeg::update_wqe_idx(new_wqe_ptr, new_wqe_idx);
            CtrlSeg::update_ds_cnt(new_wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(new_wqe_ptr);
            }
        }

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(new_wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(new_wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx: new_wqe_idx,
            size: self.offset,
        })
    }
}

// =============================================================================
// UD SQ WQE Entry Point
// =============================================================================

/// UD SQ WQE entry point.
///
/// Provides verb methods for UD operations (SEND only).
#[must_use = "WQE builder must be finished"]
pub struct UdSqWqeEntryPoint<'a, Entry> {
    core: UdWqeCore<'a, Entry>,
    ah: &'a AddressHandle,
    qkey: u32,
}

impl<'a, Entry> UdSqWqeEntryPoint<'a, Entry> {
    /// Create a new entry point.
    #[inline]
    pub(crate) fn new(
        sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>,
        ah: &'a AddressHandle,
        qkey: u32,
    ) -> Self {
        Self {
            core: UdWqeCore::new(sq, None),
            ah,
            qkey,
        }
    }

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: TxFlags) -> io::Result<UdSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_ud_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_ud_av(self.ah, self.qkey);
        Ok(UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(
        mut self,
        flags: TxFlags,
        imm: u32,
    ) -> io::Result<UdSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_ud_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_ud_av(self.ah, self.qkey);
        Ok(UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        })
    }
}

// =============================================================================
// UD Send WQE Builder
// =============================================================================

/// UD SEND WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct UdSendWqeBuilder<'a, Entry, DataState> {
    core: UdWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> UdSendWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> UdSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> UdSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> UdSendWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// sq_wqe / post_recv Methods
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpWithTable<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Get a SQ WQE builder for UD transport.
    ///
    /// # Example
    /// ```ignore
    /// qp.sq_wqe(&ah)?
    ///     .send(TxFlags::empty())?
    ///     .sge(addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// qp.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe<'a>(&'a self, ah: &'a AddressHandle) -> io::Result<UdSqWqeEntryPoint<'a, SqEntry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(UdSqWqeEntryPoint::new(sq, ah, self.qkey))
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
        unsafe {
            let wqe_ptr = rq.get_wqe_ptr(wqe_idx);
            DataSeg::write(wqe_ptr, len, lkey, addr);
        }
        let idx = (wqe_idx as usize) & ((rq.wqe_cnt - 1) as usize);
        rq.table[idx].set(Some(entry));
        rq.pi.set(rq.pi.get().wrapping_add(1));
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
    pub fn blueflame_rq_batch(&self) -> Result<UdRqBlueflameWqeBatch<'_, RqEntry>, SubmissionError> {
        let rq = self.rq.as_ref().ok_or(SubmissionError::RqFull)?;
        let sq = self.sq.as_ref().ok_or(SubmissionError::BlueflameNotAvailable)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(UdRqBlueflameWqeBatch::new(rq, sq.bf_reg, sq.bf_size, &sq.bf_offset))
    }

    /// Ring the send queue doorbell.
    ///
    /// Call this after posting one or more WQEs to notify the HCA.
    #[inline]
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            if let Some((wqe_ptr, _)) = sq.last_wqe.get() {
                sq.ring_db(wqe_ptr);
            }
        }
    }

    /// Get a BlueFlame batch builder for low-latency WQE submission.
    ///
    /// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// # Example
    /// ```ignore
    /// let mut bf = qp.blueflame_sq_wqe(&ah)?;
    /// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
    /// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
    /// bf.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_sq_wqe<'a>(&'a self, ah: &'a AddressHandle) -> Result<UdBlueflameWqeBatch<'a, SqEntry>, SubmissionError> {
        let sq = self.sq.as_ref().ok_or(SubmissionError::SqFull)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(UdBlueflameWqeBatch::new(sq, ah, self.qkey))
    }
}

// =============================================================================
// UD RQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame buffer size in bytes (256B doorbell window).
const BLUEFLAME_BUFFER_SIZE: usize = 256;

/// RQ WQE size in bytes (single DataSeg).
const RQ_WQE_SIZE: usize = DataSeg::SIZE;

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
    fn new(
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
    pub fn post(&mut self, entry: Entry, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
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
            DataSeg::write(wqe_ptr, len, lkey, addr);

            // Also write to batch buffer for BlueFlame copy
            DataSeg::write(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
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
        self.rq.pi.set(self.rq.pi.get().wrapping_add(self.wqe_count));

        mmio_flush_writes!();

        // Update doorbell record
        unsafe {
            std::ptr::write_volatile(self.rq.dbrec, (self.rq.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        // Copy buffer to BlueFlame register
        if self.bf_size > 0 {
            let bf_offset = self.bf_offset.get();
            let bf = unsafe { self.bf_reg.add(bf_offset as usize) };

            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.bf_offset.set(bf_offset ^ self.bf_size);
        }
    }
}

// =============================================================================
// UD SQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame WQE batch builder for UD QP.
///
/// Allows building multiple WQEs that fit within the BlueFlame buffer (256 bytes).
/// Each WQE is copied to a contiguous buffer and submitted via BlueFlame doorbell
/// when `finish()` is called.
pub struct UdBlueflameWqeBatch<'a, Entry> {
    sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>,
    ah: &'a AddressHandle,
    qkey: u32,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> UdBlueflameWqeBatch<'a, Entry> {
    fn new(sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>, ah: &'a AddressHandle, qkey: u32) -> Self {
        Self {
            sq,
            ah,
            qkey,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<UdBlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(UdBlueflameWqeEntryPoint { batch: self })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return;
        }

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.sq.bf_offset.set(bf_offset ^ self.sq.bf_size);
        }
    }
}

/// BlueFlame WQE entry point for UD.
#[must_use = "WQE builder must be finished"]
pub struct UdBlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut UdBlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> UdBlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: TxFlags) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = UdBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_ud_addr()?;
        Ok(UdBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: TxFlags, imm: u32) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = UdBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_ud_addr()?;
        Ok(UdBlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for UD BlueFlame WQE construction.
struct UdBlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut UdBlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> UdBlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut UdBlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
        if batch.offset + CtrlSeg::SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        Ok(Self {
            wqe_start: batch.offset,
            offset: batch.offset,
            batch,
            ds_count: 0,
            signaled: false,
        })
    }

    #[inline]
    fn remaining(&self) -> usize {
        BLUEFLAME_BUFFER_SIZE - self.offset
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: TxFlags, imm: u32) {
        let wqe_idx = self.batch.sq.pi.get();
        let flags = WqeFlags::from_bits_truncate(flags.bits());
        unsafe {
            CtrlSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset += CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_ud_addr(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < UdAddressSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            UdAddressSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                self.batch.ah,
                self.batch.qkey,
            );
        }
        self.offset += UdAddressSeg::SIZE;
        self.ds_count += 3;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DataSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            DataSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) -> Result<(), SubmissionError> {
        let padded_size = ((4 + data.len()) + 15) & !15;
        if self.remaining() < padded_size {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            let ptr = self.batch.buffer.as_mut_ptr().add(self.offset);
            InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
        }
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        Ok(())
    }

    #[inline]
    fn finish_internal(self, entry: Option<Entry>) -> Result<(), SubmissionError> {
        unsafe {
            CtrlSeg::update_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                CtrlSeg::set_completion_flag(
                    self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                );
            }
        }

        let wqe_size = self.offset - self.wqe_start;
        let wqebb_cnt = calc_wqebb_cnt(wqe_size);

        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// UD BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct UdBlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: UdBlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> UdBlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(UdBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(UdBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> UdBlueflameWqeBuilder<'b, 'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<Self, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(self)
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<Self, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(self)
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish(self) -> Result<(), SubmissionError> {
        self.core.finish_internal(None)
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(self, entry: Entry) -> Result<(), SubmissionError> {
        self.core.finish_internal(Some(entry))
    }
}
