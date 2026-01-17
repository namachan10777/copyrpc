//! Queue Pair (QP) management.
//!
//! Queue Pairs are the fundamental communication endpoints in RDMA.
//! This module provides RC (Reliable Connection) QP creation using mlx5dv_create_qp.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::Pd;
use crate::wqe::{
    AtomicSeg, CtrlSeg, DataSeg, DenseWqeTable, InlineHeader, RdmaSeg, SparseWqeTable, WQEBB_SIZE,
    WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
};
use crate::CompletionTarget;

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
}

impl Default for RcQpConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_recv_wr: 256,
            max_send_sge: 4,
            max_recv_sge: 4,
            max_inline_data: 64,
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

/// Remote QP information for connection.
#[derive(Debug, Clone)]
pub struct RemoteQpInfo {
    /// Remote QP number.
    pub qpn: u32,
    /// Remote packet sequence number.
    pub psn: u32,
    /// Remote LID (Local Identifier).
    pub lid: u16,
}

/// QP internal info obtained from mlx5dv_init_obj.
#[derive(Debug)]
pub struct QpInfo {
    /// Doorbell record pointer.
    pub dbrec: *mut u32,
    /// Send Queue buffer pointer.
    pub sq_buf: *mut u8,
    /// Send Queue WQE count.
    pub sq_wqe_cnt: u32,
    /// Send Queue stride (bytes per WQE slot).
    pub sq_stride: u32,
    /// Receive Queue buffer pointer.
    pub rq_buf: *mut u8,
    /// Receive Queue WQE count.
    pub rq_wqe_cnt: u32,
    /// Receive Queue stride.
    pub rq_stride: u32,
    /// BlueFlame register pointer.
    pub bf_reg: *mut u8,
    /// BlueFlame size.
    pub bf_size: u32,
    /// Send Queue Number.
    pub sqn: u32,
}

// =============================================================================
// Send Queue State
// =============================================================================

/// Send Queue state for direct WQE posting.
///
/// Generic over the table type `Tab` which determines dense vs sparse behavior.
/// Both table types use interior mutability (Cell) so no RefCell wrapper is needed.
pub(crate) struct SendQueueState<T, Tab> {
    /// SQ buffer base address
    buf: *mut u8,
    /// Number of WQEBBs (64-byte blocks)
    wqe_cnt: u16,
    /// SQ number
    sqn: u32,
    /// Producer index (next WQE slot)
    pi: Cell<u16>,
    /// Consumer index (last completed WQE)
    ci: Cell<u16>,
    /// Last posted WQE pointer and size (for BlueFlame)
    last_wqe: Cell<Option<(*mut u8, usize)>>,
    /// Doorbell record pointer
    dbrec: *mut u32,
    /// BlueFlame register pointer
    bf_reg: *mut u8,
    /// BlueFlame size (64 or 0 if not available)
    bf_size: u32,
    /// Current BlueFlame offset (alternates between 0 and bf_size)
    bf_offset: Cell<u32>,
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<T>>) so no RefCell needed.
    table: Tab,
    /// Phantom for entry type
    _marker: std::marker::PhantomData<T>,
}

impl<T, Tab> SendQueueState<T, Tab> {
    #[inline]
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    #[inline]
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    #[inline]
    fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    #[inline]
    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Ring the doorbell using regular doorbell write.
    #[inline]
    fn ring_doorbell(&self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
    }

    /// Ring the doorbell using BlueFlame (low latency, single WQE).
    #[inline]
    fn ring_blueflame(&self, wqe_ptr: *mut u8) {
        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        if self.bf_size > 0 {
            let bf_offset = self.bf_offset.get();
            let bf = unsafe { self.bf_reg.add(bf_offset as usize) };
            mlx5_bf_copy!(bf, wqe_ptr);
            mmio_flush_writes!();
            self.bf_offset.set(bf_offset ^ self.bf_size);
        } else {
            // Fallback to regular doorbell if BlueFlame not available
            self.ring_db(wqe_ptr);
        }
    }

    #[inline]
    fn ring_db(&self, wqe_ptr: *mut u8) {
        let bf_offset = self.bf_offset.get();
        let bf = unsafe { self.bf_reg.add(bf_offset as usize) as *mut u64 };
        let ctrl = wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset.set(bf_offset ^ self.bf_size);
    }
}

impl<T> SendQueueState<T, SparseWqeTable<T>> {
    #[inline]
    fn process_completion_sparse(&self, wqe_idx: u16) -> Option<T> {
        self.ci.set(wqe_idx);
        self.table.take(wqe_idx)
    }
}

impl<T> SendQueueState<T, DenseWqeTable<T>> {
    #[inline]
    fn process_completions_dense<F>(&self, new_ci: u16, mut callback: F)
    where
        F: FnMut(u16, T),
    {
        // Use take_range which only holds a shared reference to the table.
        // This allows callbacks to safely access the QP (e.g., post new WQEs)
        // without causing a RefCell borrow conflict.
        for (idx, entry) in self.table.take_range(self.ci.get(), new_ci) {
            callback(idx, entry);
        }
        self.ci.set(new_ci);
    }
}

// =============================================================================
// Receive Queue State
// =============================================================================

/// Receive Queue state for direct WQE posting.
///
/// Generic over `T`, the entry type stored in the WQE table.
/// Unlike SQ, all RQ WQEs generate completions (all signaled),
/// so we use a simple table without sparse/dense distinction.
///
/// Uses `Cell<Option<T>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows.
pub(crate) struct ReceiveQueueState<T> {
    /// RQ buffer base address
    buf: *mut u8,
    /// Number of WQE slots
    wqe_cnt: u32,
    /// Stride (bytes per WQE slot)
    stride: u32,
    /// Producer index (next WQE slot)
    pi: Cell<u16>,
    /// Consumer index (last completed WQE + 1)
    ci: Cell<u16>,
    /// Doorbell record pointer (dbrec[0] for RQ)
    dbrec: *mut u32,
    /// Entry table (all WQEs are signaled, uses Cell for interior mutability)
    table: Box<[Cell<Option<T>>]>,
}

/// MLX5 invalid lkey value used to mark end of SGE list.
const MLX5_INVALID_LKEY: u32 = 0x100;

impl<T> ReceiveQueueState<T> {
    #[inline]
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx as u32) & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    /// Process a receive completion.
    ///
    /// Returns the entry associated with the completed WQE.
    #[inline]
    fn process_completion(&self, wqe_idx: u16) -> Option<T> {
        self.ci.set(wqe_idx.wrapping_add(1));
        let idx = (wqe_idx as usize) & ((self.wqe_cnt - 1) as usize);
        self.table[idx].take()
    }

    /// Get the number of available WQE slots.
    #[inline]
    fn available(&self) -> u32 {
        self.wqe_cnt - (self.pi.get().wrapping_sub(self.ci.get()) as u32)
    }

    #[inline]
    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, (self.pi.get() as u32).to_be());
        }
    }
}

// =============================================================================
// Receive WQE Builder
// =============================================================================

/// Zero-copy WQE builder for receive operations.
///
/// Writes segments directly to the RQ buffer without intermediate copies.
/// Similar to the send WQE builder pattern, but for receive WQEs.
pub struct RecvWqeBuilder<'a, T> {
    rq: &'a ReceiveQueueState<T>,
    entry: T,
    wqe_idx: u16,
}

impl<'a, T> RecvWqeBuilder<'a, T> {
    /// Add a data segment (SGE) for the receive buffer.
    ///
    /// # Safety
    /// The caller must ensure the buffer is registered and valid.
    #[inline]
    pub fn sge(self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            let wqe_ptr = self.rq.get_wqe_ptr(self.wqe_idx);
            DataSeg::write(wqe_ptr, len, lkey, addr);

            // If there's room for another SGE, write a sentinel to mark end of list.
            // The sentinel has byte_count=0 and lkey=MLX5_INVALID_LKEY.
            if self.rq.stride > DataSeg::SIZE as u32 {
                let sentinel_ptr = wqe_ptr.add(DataSeg::SIZE);
                let ptr32 = sentinel_ptr as *mut u32;
                std::ptr::write_volatile(ptr32, 0u32); // byte_count = 0
                std::ptr::write_volatile(ptr32.add(1), MLX5_INVALID_LKEY.to_be()); // lkey = invalid
            }
        }
        self
    }

    /// Finish the receive WQE construction.
    ///
    /// Stores the entry in the table and advances the producer index.
    /// Call `ring_rq_doorbell()` after posting one or more WQEs to notify the HCA.
    #[inline]
    pub fn finish(self) {
        let idx = (self.wqe_idx as usize) & ((self.rq.wqe_cnt - 1) as usize);
        self.rq.table[idx].set(Some(self.entry));
        self.rq.pi.set(self.rq.pi.get().wrapping_add(1));
    }
}

// =============================================================================
// WQE Builder
// =============================================================================

/// Zero-copy WQE builder for RC QP.
///
/// Writes segments directly to the SQ buffer without intermediate copies.
pub struct WqeBuilder<'a, T, Tab> {
    sq: &'a SendQueueState<T, Tab>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    /// Whether SIGNALED flag is set
    signaled: bool,
}

impl<'a, T, Tab> WqeBuilder<'a, T, Tab> {
    /// Write the control segment.
    ///
    /// This must be the first segment in every WQE.
    #[inline]
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

    /// Add an RDMA segment (for WRITE/READ).
    #[inline]
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add a data segment (SGE).
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add inline data.
    #[inline]
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

    /// Get a mutable slice for inline data (zero-copy).
    ///
    /// Returns the builder and a slice that can be written to directly.
    #[inline]
    pub fn inline_slice(mut self, len: usize) -> (Self, &'a mut [u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            InlineHeader::write(ptr, len as u32)
        };
        let data_ptr = unsafe { self.wqe_ptr.add(self.offset + 4) };
        let slice = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        (self, slice)
    }

    /// Add an atomic Compare-and-Swap segment.
    ///
    /// The CAS operation atomically compares the 8-byte value at `remote_addr`
    /// (specified in the preceding RDMA segment) with `compare`. If equal,
    /// replaces it with `swap`. The original value is written to the local
    /// buffer (specified in the following data segment).
    ///
    /// WQE structure for atomic CAS:
    /// - Control segment (ctrl)
    /// - RDMA segment (rdma) - specifies remote address and rkey
    /// - Atomic segment (atomic_cas) - specifies swap and compare values
    /// - Data segment (sge) - specifies local buffer for result
    #[inline]
    pub fn atomic_cas(mut self, swap: u64, compare: u64) -> Self {
        unsafe {
            AtomicSeg::write_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add an atomic Fetch-and-Add segment.
    ///
    /// The FA operation atomically adds `add_value` to the 8-byte value at
    /// `remote_addr` (specified in the preceding RDMA segment). The original
    /// value is written to the local buffer (specified in the following data
    /// segment).
    ///
    /// WQE structure for atomic FA:
    /// - Control segment (ctrl)
    /// - RDMA segment (rdma) - specifies remote address and rkey
    /// - Atomic segment (atomic_fa) - specifies add value
    /// - Data segment (sge) - specifies local buffer for result
    #[inline]
    pub fn atomic_fa(mut self, add_value: u64) -> Self {
        unsafe {
            AtomicSeg::write_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Finish the WQE construction (internal).
    #[inline]
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

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. The doorbell is issued
    /// immediately, so no need to call `ring_sq_doorbell()` afterwards.
    #[inline]
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
// RC QP
// =============================================================================

/// RC (Reliable Connection) Queue Pair with sparse WQE table.
///
/// Only signaled WQEs have entries stored. Use this when you only need
/// to track completions for signaled WQEs.
///
/// Type parameters:
/// - `T`: Entry type stored in the WQE table
/// - `F`: Completion callback type `Fn(Cqe, T)`
///
/// For tracking all WQEs, use `DenseRcQp` instead.
pub type RcQp<T, F> = RcQpInner<T, SparseWqeTable<T>, F>;

/// RC (Reliable Connection) Queue Pair with dense WQE table.
///
/// Every WQE must have an entry stored. Use this when you need to track
/// all completions, including unsignaled WQEs.
///
/// Type parameters:
/// - `T`: Entry type stored in the WQE table
/// - `F`: Completion callback type `Fn(Option<Cqe>, T)`
///
/// For tracking only signaled WQEs, use `RcQp` instead.
pub type DenseRcQp<T, F> = RcQpInner<T, DenseWqeTable<T>, F>;

/// RC (Reliable Connection) Queue Pair (internal implementation).
///
/// Created using mlx5dv_create_qp for direct hardware access.
///
/// Type parameter `T` is the entry type stored in the WQE table (used for both SQ and RQ).
/// Type parameter `Tab` determines sparse vs dense table behavior for the SQ.
/// Type parameter `F` is the completion callback type.
pub struct RcQpInner<T, Tab, F> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: Cell<QpState>,
    sq: Option<SendQueueState<T, Tab>>,
    rq: Option<ReceiveQueueState<T>>,
    callback: F,
    /// Weak reference to the send CQ for unregistration on drop
    send_cq: Weak<CompletionQueue>,
    /// Weak reference to the recv CQ for unregistration on drop
    recv_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this QP exists.
    _pd: Pd,
}

impl Context {
    /// Create an RC Queue Pair with sparse WQE table using mlx5dv_create_qp.
    ///
    /// Only signaled WQEs have entries stored. The callback is invoked for each
    /// completion with the CQE and the entry stored at WQE submission.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions (wrapped in Rc for shared ownership)
    /// * `recv_cq` - Completion Queue for receive completions (wrapped in Rc for shared ownership)
    /// * `config` - QP configuration
    /// * `callback` - Completion callback `Fn(Cqe, T)` called for each signaled completion
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_rc_qp<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &RcQpConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<RcQp<T, F>>>>
    where
        T: 'static,
        F: Fn(Cqe, T) + 'static,
    {
        let qp = self.create_rc_qp_raw(pd, send_cq, recv_cq, config, callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        // Register this QP with both CQs for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    /// Create an RC Queue Pair with dense WQE table using mlx5dv_create_qp.
    ///
    /// Every WQE must have an entry stored. The callback is invoked for each
    /// completion: `Some(Cqe)` for signaled WQEs, `None` for unsignaled ones.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions (wrapped in Rc for shared ownership)
    /// * `recv_cq` - Completion Queue for receive completions (wrapped in Rc for shared ownership)
    /// * `config` - QP configuration
    /// * `callback` - Completion callback `Fn(Option<Cqe>, T)` called for each completion
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_dense_rc_qp<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &RcQpConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<DenseRcQp<T, F>>>>
    where
        T: 'static,
        F: Fn(Option<Cqe>, T) + 'static,
    {
        let qp = self.create_dense_rc_qp_raw(pd, send_cq, recv_cq, config, callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        // Register this QP with both CQs for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    fn create_rc_qp_raw<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &RcQpConfig,
        callback: F,
    ) -> io::Result<RcQp<T, F>>
    where
        F: Fn(Cqe, T),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
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
            // Disable scatter to CQE to ensure received data goes to the receive buffer,
            // not inline in the CQE. This is required for direct CQ polling to work correctly.
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS as u64;
            mlx5_attr.create_flags =
                mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(RcQpInner {
                    qp,
                    state: Cell::new(QpState::Reset),
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

    fn create_dense_rc_qp_raw<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &RcQpConfig,
        callback: F,
    ) -> io::Result<DenseRcQp<T, F>>
    where
        F: Fn(Option<Cqe>, T),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
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
            // Disable scatter to CQE to ensure received data goes to the receive buffer,
            // not inline in the CQE. This is required for direct CQ polling to work correctly.
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS as u64;
            mlx5_attr.create_flags =
                mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(RcQpInner {
                    qp,
                    state: Cell::new(QpState::Reset),
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

impl<T, Tab, F> Drop for RcQpInner<T, Tab, F> {
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

impl<T, Tab, F> RcQpInner<T, Tab, F> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the raw ibv_qp pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_qp {
        self.qp.as_ptr()
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

    /// Transition QP from INIT to RTR (Ready to Receive).
    pub fn modify_to_rtr(
        &mut self,
        remote: &RemoteQpInfo,
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
            attr.dest_qp_num = remote.qpn;
            attr.rq_psn = remote.psn;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;
            attr.ah_attr.dlid = remote.lid;
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

    fn sq(&self) -> io::Result<&SendQueueState<T, Tab>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    /// Get the number of available WQE slots.
    pub fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    /// Get the total SQ WQE count (hardware queue size).
    pub fn sq_wqe_cnt(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.wqe_cnt).unwrap_or(0)
    }

    /// Get the total RQ WQE count (hardware queue size).
    pub fn rq_wqe_cnt(&self) -> u32 {
        self.rq.as_ref().map(|rq| rq.wqe_cnt).unwrap_or(0)
    }

    /// Ring the SQ doorbell to notify HCA of new WQEs.
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell();
        }
    }
}

impl<T, F> RcQp<T, F> {
    /// Initialize direct queue access.
    ///
    /// Call this after the QP is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
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
            table: SparseWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt;
        self.rq = Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        });

        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS and initializes
    /// direct queue access.
    pub fn connect(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }

    /// Get a receive WQE builder for zero-copy WQE construction.
    ///
    /// The entry will be stored and returned via callback on RQ completion.
    pub fn recv_builder(&self, entry: T) -> io::Result<RecvWqeBuilder<'_, T>> {
        let rq = self.rq.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "direct access not initialized")
        })?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }

        let wqe_idx = rq.pi.get();
        Ok(RecvWqeBuilder {
            rq,
            entry,
            wqe_idx,
        })
    }

    /// Get a WQE builder for zero-copy WQE construction (signaled).
    ///
    /// The entry will be stored and returned via callback on completion.
    pub fn wqe_builder(&self, entry: T) -> io::Result<SparseWqeBuilder<'_, T>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(SparseWqeBuilder {
            inner: WqeBuilder {
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

    /// Get a WQE builder for zero-copy WQE construction (unsignaled).
    ///
    /// No entry is stored and no completion callback will be invoked.
    pub fn wqe_builder_unsignaled(&self) -> io::Result<SparseWqeBuilder<'_, T>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(SparseWqeBuilder {
            inner: WqeBuilder {
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

impl<T, F> DenseRcQp<T, F> {
    /// Initialize direct queue access.
    ///
    /// Call this after the QP is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
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
            table: DenseWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt;
        self.rq = Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        });

        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS and initializes
    /// direct queue access.
    pub fn connect(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }

    /// Get a receive WQE builder for zero-copy WQE construction.
    ///
    /// The entry will be stored and returned via callback on RQ completion.
    pub fn recv_builder(&self, entry: T) -> io::Result<RecvWqeBuilder<'_, T>> {
        let rq = self.rq.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "direct access not initialized")
        })?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }

        let wqe_idx = rq.pi.get();
        Ok(RecvWqeBuilder {
            rq,
            entry,
            wqe_idx,
        })
    }

    /// Get a WQE builder for zero-copy WQE construction.
    ///
    /// Every WQE must have an entry. The signaled flag controls whether
    /// a CQE is generated. The callback receives `Some(Cqe)` for signaled
    /// WQEs and `None` for unsignaled ones.
    pub fn wqe_builder(&self, entry: T, signaled: bool) -> io::Result<DenseWqeBuilder<'_, T>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(DenseWqeBuilder {
            inner: WqeBuilder {
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
// Sparse WQE Builder finish
// =============================================================================

/// Sparse WQE builder that stores entry on finish.
pub struct SparseWqeBuilder<'a, T> {
    inner: WqeBuilder<'a, T, SparseWqeTable<T>>,
    entry: Option<T>,
}

impl<'a, T> SparseWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add an RDMA segment.
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        self.inner = self.inner.rdma(remote_addr, rkey);
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

    /// Add an atomic Compare-and-Swap segment.
    pub fn atomic_cas(mut self, swap: u64, compare: u64) -> Self {
        self.inner = self.inner.atomic_cas(swap, compare);
        self
    }

    /// Add an atomic Fetch-and-Add segment.
    pub fn atomic_fa(mut self, add_value: u64) -> Self {
        self.inner = self.inner.atomic_fa(add_value);
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        if let Some(entry) = self.entry {
            self.inner.sq.table.store(wqe_idx, entry);
        }
        self.inner.finish_internal()
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_sq_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        if let Some(entry) = self.entry {
            self.inner.sq.table.store(wqe_idx, entry);
        }
        self.inner.finish_internal_with_blueflame()
    }
}

/// Dense WQE builder that stores entry on finish.
pub struct DenseWqeBuilder<'a, T> {
    inner: WqeBuilder<'a, T, DenseWqeTable<T>>,
    entry: T,
}

impl<'a, T> DenseWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add an RDMA segment.
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        self.inner = self.inner.rdma(remote_addr, rkey);
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

    /// Add an atomic Compare-and-Swap segment.
    pub fn atomic_cas(mut self, swap: u64, compare: u64) -> Self {
        self.inner = self.inner.atomic_cas(swap, compare);
        self
    }

    /// Add an atomic Fetch-and-Add segment.
    pub fn atomic_fa(mut self, add_value: u64) -> Self {
        self.inner = self.inner.atomic_fa(add_value);
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        self.inner.sq.table.store(wqe_idx, self.entry);
        self.inner.finish_internal()
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_sq_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        self.inner.sq.table.store(wqe_idx, self.entry);
        self.inner.finish_internal_with_blueflame()
    }
}

// =============================================================================
// CompletionTarget impl for RcQp (Sparse)
// =============================================================================

impl<T, F> CompletionTarget for RcQp<T, F>
where
    F: Fn(Cqe, T),
{
    fn qpn(&self) -> u32 {
        RcQpInner::qpn(self)
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

// =============================================================================
// CompletionTarget impl for DenseRcQp
// =============================================================================

impl<T, F> CompletionTarget for DenseRcQp<T, F>
where
    F: Fn(Option<Cqe>, T),
{
    fn qpn(&self) -> u32 {
        RcQpInner::qpn(self)
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

// =============================================================================
// ReceiveQueue methods
// =============================================================================

impl<T, Tab, F> RcQpInner<T, Tab, F> {
    /// Ring the RQ doorbell to notify HCA of new WQEs.
    pub fn ring_rq_doorbell(&self) {
        if let Some(rq) = self.rq.as_ref() {
            rq.ring_doorbell();
        }
    }

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

impl<T, Tab, F> RcQpInner<T, Tab, F> {
    /// Get a raw SQ slot for direct WQE construction.
    ///
    /// Returns `None` if the SQ is not initialized or full.
    ///
    /// # Safety
    /// The caller must:
    /// - Write a valid WQE to the returned pointer
    /// - Call `__sq_advance_pi()` after writing the WQE
    /// - Call `__sq_set_last_wqe()` or `__sq_ring_blueflame()` to notify the HCA
    #[inline]
    pub unsafe fn __sq_ptr(&self) -> Option<SqSlot> {
        let sq = self.sq.as_ref()?;
        if sq.available() == 0 {
            return None;
        }

        let wqe_idx = sq.pi.get();
        let ptr = sq.get_wqe_ptr(wqe_idx);

        Some(SqSlot {
            ptr,
            wqe_idx,
            sqn: sq.sqn,
        })
    }

    /// Advance the SQ producer index.
    ///
    /// # Safety
    /// The caller must have written a valid WQE to the SQ buffer before calling this.
    #[inline]
    pub unsafe fn __sq_advance_pi(&self, wqebb_cnt: u16) {
        if let Some(sq) = self.sq.as_ref() {
            sq.advance_pi(wqebb_cnt);
        }
    }

    /// Set the last WQE pointer and size for doorbell.
    ///
    /// Call `ring_sq_doorbell()` after this to notify the HCA.
    ///
    /// # Safety
    /// The caller must ensure `ptr` points to a valid WQE in the SQ buffer.
    #[inline]
    pub unsafe fn __sq_set_last_wqe(&self, ptr: *mut u8, size: usize) {
        if let Some(sq) = self.sq.as_ref() {
            sq.set_last_wqe(ptr, size);
        }
    }

    /// Ring the BlueFlame doorbell for low-latency WQE submission.
    ///
    /// This combines the doorbell record update and BlueFlame copy in a single operation.
    ///
    /// # Safety
    /// The caller must ensure `wqe_ptr` points to a valid WQE in the SQ buffer.
    #[inline]
    pub unsafe fn __sq_ring_blueflame(&self, wqe_ptr: *mut u8) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_blueflame(wqe_ptr);
        }
    }

    /// Get the BlueFlame buffer size.
    ///
    /// Returns 0 if BlueFlame is not available or SQ is not initialized.
    #[inline]
    pub fn __sq_bf_size(&self) -> u32 {
        self.sq.as_ref().map(|sq| sq.bf_size).unwrap_or(0)
    }
}

impl<T, F> RcQp<T, F> {
    /// Store an entry in the sparse WQE table.
    ///
    /// # Safety
    /// The caller must ensure `wqe_idx` corresponds to a valid pending WQE.
    #[inline]
    pub unsafe fn __sq_store_entry(&self, wqe_idx: u16, entry: T) {
        if let Some(sq) = self.sq.as_ref() {
            sq.table.store(wqe_idx, entry);
        }
    }
}

impl<T, F> DenseRcQp<T, F> {
    /// Store an entry in the dense WQE table.
    ///
    /// # Safety
    /// The caller must ensure `wqe_idx` corresponds to a valid pending WQE.
    #[inline]
    pub unsafe fn __sq_store_entry(&self, wqe_idx: u16, entry: T) {
        if let Some(sq) = self.sq.as_ref() {
            sq.table.store(wqe_idx, entry);
        }
    }
}
