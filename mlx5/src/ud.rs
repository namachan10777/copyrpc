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

use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::{AddressHandle, Pd};
use crate::qp::QpInfo;
use crate::wqe::{
    CtrlSeg, DataSeg, DenseWqeTable, InlineHeader, SparseWqeTable, WQEBB_SIZE, WqeFlags, WqeHandle,
    WqeOpcode, calc_wqebb_cnt,
};
use crate::CompletionTarget;

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
struct UdSendQueueState<T, Tab> {
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
    table: RefCell<Tab>,
    _marker: PhantomData<T>,
}

impl<T, Tab> UdSendQueueState<T, Tab> {
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Ring the doorbell using BlueFlame (low latency, single WQE).
    fn ring_blueflame(&self, wqe_ptr: *mut u8) {
        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        if self.bf_size > 0 {
            let bf = unsafe { self.bf_reg.add(self.bf_offset.get() as usize) };
            mlx5_bf_copy!(bf, wqe_ptr);
            mmio_flush_writes!();
            self.bf_offset.set(self.bf_offset.get() ^ self.bf_size);
        } else {
            self.ring_db(wqe_ptr);
        }
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

impl<T> UdSendQueueState<T, SparseWqeTable<T>> {
    fn process_completion_sparse(&self, wqe_idx: u16) -> Option<T> {
        self.ci.set(wqe_idx);
        self.table.borrow_mut().take(wqe_idx)
    }
}

impl<T> UdSendQueueState<T, DenseWqeTable<T>> {
    fn process_completions_dense<F>(&self, new_ci: u16, mut callback: F)
    where
        F: FnMut(u16, T),
    {
        for (idx, entry) in self.table.borrow_mut().drain_range(self.ci.get(), new_ci) {
            callback(idx, entry);
        }
        self.ci.set(new_ci);
    }
}

// =============================================================================
// Receive Queue State
// =============================================================================

/// MLX5 invalid lkey value used to mark end of SGE list.
const MLX5_INVALID_LKEY: u32 = 0x100;

/// Receive Queue state for UD QP.
///
/// Generic over `T`, the entry type stored in the WQE table.
/// Unlike SQ, all RQ WQEs generate completions (all signaled).
struct UdRecvQueueState<T> {
    buf: *mut u8,
    wqe_cnt: u16,
    stride: u32,
    pi: Cell<u16>,
    ci: Cell<u16>,
    dbrec: *mut u32,
    table: RefCell<Box<[Option<T>]>>,
}

impl<T> UdRecvQueueState<T> {
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * (self.stride as usize);
        unsafe { self.buf.add(offset) }
    }

    /// Process a receive completion.
    fn process_completion(&self, wqe_idx: u16) -> Option<T> {
        self.ci.set(wqe_idx.wrapping_add(1));
        let idx = (wqe_idx as usize) & ((self.wqe_cnt - 1) as usize);
        self.table.borrow_mut()[idx].take()
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
// UD Receive WQE Builder
// =============================================================================

/// Zero-copy WQE builder for UD receive operations.
///
/// Writes segments directly to the RQ buffer without intermediate copies.
pub struct UdRecvWqeBuilder<'a, T> {
    rq: &'a UdRecvQueueState<T>,
    entry: T,
    wqe_idx: u16,
}

impl<'a, T> UdRecvWqeBuilder<'a, T> {
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
        self.rq.table.borrow_mut()[idx] = Some(self.entry);
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
// UD WQE Builder
// =============================================================================

/// Zero-copy WQE builder for UD QP.
pub struct UdWqeBuilder<'a, T, Tab> {
    sq: &'a UdSendQueueState<T, Tab>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'a, T, Tab> UdWqeBuilder<'a, T, Tab> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
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
        self
    }

    /// Add UD address segment using an Address Handle.
    pub fn ud_av(mut self, ah: &AddressHandle, qkey: u32) -> Self {
        unsafe {
            UdAddressSeg::write(self.wqe_ptr.add(self.offset), ah, qkey);
        }
        self.offset += UdAddressSeg::SIZE;
        self.ds_count += 3; // 48 bytes = 3 DS
        self
    }

    /// Add UD address segment using raw values.
    pub fn ud_av_raw(mut self, remote_qpn: u32, qkey: u32, dlid: u16) -> Self {
        unsafe {
            UdAddressSeg::write_raw(self.wqe_ptr.add(self.offset), remote_qpn, qkey, dlid);
        }
        self.offset += UdAddressSeg::SIZE;
        self.ds_count += 3;
        self
    }

    /// Add a data segment (SGE).
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add inline data.
    pub fn inline_data(mut self, data: &[u8]) -> Self {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        self
    }

    fn finish_internal(self) -> WqeHandle {
        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
        }

        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let wqe_idx = self.wqe_idx;

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }

    fn finish_internal_with_blueflame(self) -> WqeHandle {
        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
        }

        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;

        self.sq.advance_pi(wqebb_cnt);
        self.sq.ring_blueflame(wqe_ptr);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }
}

// =============================================================================
// Sparse/Dense UD WQE Builders
// =============================================================================

/// Sparse UD WQE builder that stores entry on finish.
pub struct SparseUdWqeBuilder<'a, T> {
    inner: UdWqeBuilder<'a, T, SparseWqeTable<T>>,
    entry: Option<T>,
}

impl<'a, T> SparseUdWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add UD address segment.
    pub fn ud_av(mut self, ah: &AddressHandle, qkey: u32) -> Self {
        self.inner = self.inner.ud_av(ah, qkey);
        self
    }

    /// Add UD address segment using raw values.
    pub fn ud_av_raw(mut self, remote_qpn: u32, qkey: u32, dlid: u16) -> Self {
        self.inner = self.inner.ud_av_raw(remote_qpn, qkey, dlid);
        self
    }

    /// Add a data segment.
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.inner = self.inner.sge(addr, len, lkey);
        self
    }

    /// Add inline data.
    pub fn inline_data(mut self, data: &[u8]) -> Self {
        self.inner = self.inner.inline_data(data);
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        if let Some(entry) = self.entry {
            self.inner.sq.table.borrow_mut().store(wqe_idx, entry);
        }
        self.inner.finish_internal()
    }

    /// Finish the WQE construction with BlueFlame doorbell.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        if let Some(entry) = self.entry {
            self.inner.sq.table.borrow_mut().store(wqe_idx, entry);
        }
        self.inner.finish_internal_with_blueflame()
    }
}

/// Dense UD WQE builder that stores entry on finish.
pub struct DenseUdWqeBuilder<'a, T> {
    inner: UdWqeBuilder<'a, T, DenseWqeTable<T>>,
    entry: T,
}

impl<'a, T> DenseUdWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add UD address segment.
    pub fn ud_av(mut self, ah: &AddressHandle, qkey: u32) -> Self {
        self.inner = self.inner.ud_av(ah, qkey);
        self
    }

    /// Add UD address segment using raw values.
    pub fn ud_av_raw(mut self, remote_qpn: u32, qkey: u32, dlid: u16) -> Self {
        self.inner = self.inner.ud_av_raw(remote_qpn, qkey, dlid);
        self
    }

    /// Add a data segment.
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.inner = self.inner.sge(addr, len, lkey);
        self
    }

    /// Add inline data.
    pub fn inline_data(mut self, data: &[u8]) -> Self {
        self.inner = self.inner.inline_data(data);
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        self.inner.sq.table.borrow_mut().store(wqe_idx, self.entry);
        self.inner.finish_internal()
    }

    /// Finish the WQE construction with BlueFlame doorbell.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        self.inner.sq.table.borrow_mut().store(wqe_idx, self.entry);
        self.inner.finish_internal_with_blueflame()
    }
}

// =============================================================================
// UD QP
// =============================================================================

/// UD QP with sparse WQE table.
pub type UdQpSparseWqeTable<T, F> = UdQp<T, SparseWqeTable<T>, F>;

/// UD QP with dense WQE table.
pub type UdQpDenseWqeTable<T, F> = UdQp<T, DenseWqeTable<T>, F>;

/// UD (Unreliable Datagram) Queue Pair.
///
/// Provides connectionless datagram service. Each send operation requires
/// specifying the destination via an Address Handle.
///
/// Type parameter `T` is the entry type stored in both SQ and RQ WQE tables.
pub struct UdQp<T, Tab, F> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: Cell<UdQpState>,
    qkey: u32,
    sq: Option<UdSendQueueState<T, Tab>>,
    rq: Option<UdRecvQueueState<T>>,
    callback: F,
    /// Weak reference to the send CQ for unregistration on drop.
    send_cq: Weak<CompletionQueue>,
    /// Weak reference to the recv CQ for unregistration on drop.
    recv_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this QP exists.
    _pd: Pd,
}

impl Context {
    /// Create a UD QP with sparse WQE table.
    ///
    /// Only signaled WQEs have entries stored.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_ud_qp_sparse<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &UdQpConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<UdQpSparseWqeTable<T, F>>>>
    where
        T: 'static,
        F: Fn(Cqe, T) + 'static,
    {
        let qp = self.create_ud_qp_raw(pd, send_cq, recv_cq, config, callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    /// Create a UD QP with dense WQE table.
    ///
    /// Every WQE must have an entry stored.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_ud_qp_dense<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &UdQpConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<UdQpDenseWqeTable<T, F>>>>
    where
        T: 'static,
        F: Fn(Option<Cqe>, T) + 'static,
    {
        let qp = self.create_ud_qp_dense_raw(pd, send_cq, recv_cq, config, callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    fn create_ud_qp_raw<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &UdQpConfig,
        callback: F,
    ) -> io::Result<UdQpSparseWqeTable<T, F>>
    where
        F: Fn(Cqe, T),
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
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(UdQp {
                    qp,
                    state: Cell::new(UdQpState::Reset),
                    qkey: config.qkey,
                    sq: None,
                    rq: None,
                    callback,
                    send_cq: Rc::downgrade(send_cq),
                    recv_cq: Rc::downgrade(recv_cq),
                    _pd: pd.clone(),
                })
            })
        }
    }

    fn create_ud_qp_dense_raw<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &UdQpConfig,
        callback: F,
    ) -> io::Result<UdQpDenseWqeTable<T, F>>
    where
        F: Fn(Option<Cqe>, T),
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
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(UdQp {
                    qp,
                    state: Cell::new(UdQpState::Reset),
                    qkey: config.qkey,
                    sq: None,
                    rq: None,
                    callback,
                    send_cq: Rc::downgrade(send_cq),
                    recv_cq: Rc::downgrade(recv_cq),
                    _pd: pd.clone(),
                })
            })
        }
    }
}

impl<T, Tab, F> Drop for UdQp<T, Tab, F> {
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

impl<T, Tab, F> UdQp<T, Tab, F> {
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
                dbrec: dv_qp.dbrec as *mut u32,
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

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_SQ_PSN;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(UdQpState::Rts);
        Ok(())
    }

    fn sq(&self) -> io::Result<&UdSendQueueState<T, Tab>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    fn rq(&self) -> io::Result<&UdRecvQueueState<T>> {
        self.rq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    /// Ring the receive queue doorbell.
    pub fn ring_rq_doorbell(&self) {
        if let Some(rq) = self.rq.as_ref() {
            rq.ring_doorbell();
        }
    }

    /// Get a receive WQE builder for zero-copy WQE construction.
    ///
    /// The entry will be stored and returned via callback on RQ completion.
    pub fn recv_builder(&self, entry: T) -> io::Result<UdRecvWqeBuilder<'_, T>> {
        let rq = self.rq()?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }

        let wqe_idx = rq.pi.get();
        Ok(UdRecvWqeBuilder {
            rq,
            entry,
            wqe_idx,
        })
    }
}

impl<T, F> UdQpSparseWqeTable<T, F> {
    /// Initialize direct queue access.
    pub fn init_direct_access(&mut self) -> io::Result<()> {
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
            table: RefCell::new(SparseWqeTable::new(wqe_cnt)),
            _marker: PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt as u16;
        self.rq = Some(UdRecvQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: RefCell::new((0..rq_wqe_cnt).map(|_| None).collect()),
        });

        Ok(())
    }

    /// Activate UD QP (transition to RTS) and initialize direct access.
    pub fn activate(&mut self, port: u8, sq_psn: u32) -> io::Result<()> {
        self.modify_to_init(port, 0)?;
        self.modify_to_rtr()?;
        self.modify_to_rts(sq_psn)?;
        self.init_direct_access()?;
        Ok(())
    }

    /// Get a WQE builder for signaled operations.
    pub fn wqe_builder(&self, entry: T) -> io::Result<SparseUdWqeBuilder<'_, T>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(SparseUdWqeBuilder {
            inner: UdWqeBuilder {
                sq,
                wqe_ptr,
                wqe_idx,
                offset: 0,
                ds_count: 0,
                signaled: true,
            },
            entry: Some(entry),
        })
    }

    /// Get a WQE builder for unsignaled operations.
    pub fn wqe_builder_unsignaled(&self) -> io::Result<SparseUdWqeBuilder<'_, T>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(SparseUdWqeBuilder {
            inner: UdWqeBuilder {
                sq,
                wqe_ptr,
                wqe_idx,
                offset: 0,
                ds_count: 0,
                signaled: false,
            },
            entry: None,
        })
    }
}

impl<T, F> UdQpDenseWqeTable<T, F> {
    /// Initialize direct queue access.
    pub fn init_direct_access(&mut self) -> io::Result<()> {
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
            table: RefCell::new(DenseWqeTable::new(wqe_cnt)),
            _marker: PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt as u16;
        self.rq = Some(UdRecvQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: RefCell::new((0..rq_wqe_cnt).map(|_| None).collect()),
        });

        Ok(())
    }

    /// Activate UD QP (transition to RTS) and initialize direct access.
    pub fn activate(&mut self, port: u8, sq_psn: u32) -> io::Result<()> {
        self.modify_to_init(port, 0)?;
        self.modify_to_rtr()?;
        self.modify_to_rts(sq_psn)?;
        self.init_direct_access()?;
        Ok(())
    }

    /// Get a WQE builder.
    pub fn wqe_builder(&self, entry: T, signaled: bool) -> io::Result<DenseUdWqeBuilder<'_, T>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(DenseUdWqeBuilder {
            inner: UdWqeBuilder {
                sq,
                wqe_ptr,
                wqe_idx,
                offset: 0,
                ds_count: 0,
                signaled,
            },
            entry,
        })
    }
}

// =============================================================================
// CompletionTarget Implementation
// =============================================================================

impl<T, F> CompletionTarget for UdQpSparseWqeTable<T, F>
where
    F: Fn(Cqe, T),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion (responder)
            if let Some(rq) = self.rq.as_ref() {
                if let Some(entry) = rq.process_completion(cqe.wqe_counter) {
                    (self.callback)(cqe, entry);
                }
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref() {
                if let Some(entry) = sq.process_completion_sparse(cqe.wqe_counter) {
                    (self.callback)(cqe, entry);
                }
            }
        }
    }
}

impl<T, F> CompletionTarget for UdQpDenseWqeTable<T, F>
where
    F: Fn(Option<Cqe>, T),
{
    fn qpn(&self) -> u32 {
        UdQp::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion (responder) - all RQ WQEs are signaled
            if let Some(rq) = self.rq.as_ref() {
                if let Some(entry) = rq.process_completion(cqe.wqe_counter) {
                    (self.callback)(Some(cqe), entry);
                }
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref() {
                let signaled_idx = cqe.wqe_counter;
                sq.process_completions_dense(signaled_idx, |idx, entry| {
                    let cqe_opt = if idx == signaled_idx {
                        Some(cqe)
                    } else {
                        None
                    };
                    (self.callback)(cqe_opt, entry);
                });
            }
        }
    }
}
