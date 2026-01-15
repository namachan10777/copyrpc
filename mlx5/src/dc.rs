//! DC (Dynamically Connected) transport.
//!
//! DC provides scalable connectionless RDMA operations using:
//! - DCI (DC Initiator): Sends RDMA operations to DCTs
//! - DCT (DC Target): Receives RDMA operations via SRQ

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::Pd;
use crate::qp::QpInfo;
use crate::srq::Srq;
use crate::wqe::{
    AddressVector, CtrlSeg, DataSeg, DenseWqeTable, InlineHeader, RdmaSeg, SparseWqeTable,
    WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
};
use crate::CompletionTarget;

/// DCI configuration.
#[derive(Debug, Clone)]
pub struct DciConfig {
    /// Maximum number of outstanding send WRs.
    pub max_send_wr: u32,
    /// Maximum number of SGEs per send WR.
    pub max_send_sge: u32,
    /// Maximum inline data size.
    pub max_inline_data: u32,
}

impl Default for DciConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_send_sge: 4,
            max_inline_data: 64,
        }
    }
}

/// DCT configuration.
#[derive(Debug, Clone)]
pub struct DctConfig {
    /// DC key for this DCT.
    pub dc_key: u64,
}

impl Default for DctConfig {
    fn default() -> Self {
        Self { dc_key: 0 }
    }
}

/// DC QP state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DcQpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Error,
}

/// Remote DCT information for sending.
#[derive(Debug, Clone, Copy)]
pub struct RemoteDctInfo {
    /// DCT number.
    pub dctn: u32,
    /// DC key.
    pub dc_key: u64,
    /// Remote LID.
    pub lid: u16,
}

// =============================================================================
// DCI Send Queue State
// =============================================================================

/// Send Queue state for DCI.
///
/// Generic over the table type `Tab` which determines dense vs sparse behavior.
struct DciSendQueueState<T, Tab> {
    buf: *mut u8,
    wqe_cnt: u16,
    sqn: u32,
    pi: u16,
    ci: u16,
    last_wqe: Option<(*mut u8, usize)>,
    dbrec: *mut u32,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: u32,
    table: Tab,
    _marker: PhantomData<T>,
}

impl<T, Tab> DciSendQueueState<T, Tab> {
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.wrapping_sub(self.ci)
    }

    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    fn advance_pi(&mut self, count: u16) {
        self.pi = self.pi.wrapping_add(count);
    }

    fn set_last_wqe(&mut self, ptr: *mut u8, size: usize) {
        self.last_wqe = Some((ptr, size));
    }

    /// Ring the doorbell using regular doorbell write.
    fn ring_doorbell(&mut self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
    }

    /// Ring the doorbell using BlueFlame (low latency, single WQE).
    fn ring_blueflame(&mut self, wqe_ptr: *mut u8) {
        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        if self.bf_size > 0 {
            let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) };
            mlx5_bf_copy!(bf, wqe_ptr);
            mmio_flush_writes!();
            self.bf_offset ^= self.bf_size;
        } else {
            // Fallback to regular doorbell if BlueFlame not available
            self.ring_db(wqe_ptr);
        }
    }

    fn ring_db(&mut self, wqe_ptr: *mut u8) {
        let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) as *mut u64 };
        let ctrl = wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset ^= self.bf_size;
    }
}

impl<T> DciSendQueueState<T, SparseWqeTable<T>> {
    fn process_completion_sparse(&mut self, wqe_idx: u16) -> Option<T> {
        self.ci = wqe_idx;
        self.table.take(wqe_idx)
    }
}

impl<T> DciSendQueueState<T, DenseWqeTable<T>> {
    fn process_completions_dense<F>(&mut self, new_ci: u16, mut callback: F)
    where
        F: FnMut(u16, T),
    {
        for (idx, entry) in self.table.drain_range(self.ci, new_ci) {
            callback(idx, entry);
        }
        self.ci = new_ci;
    }
}

// =============================================================================
// DCI WQE Builder
// =============================================================================

/// Zero-copy WQE builder for DCI (internal implementation).
///
/// Similar to WqeBuilder but has Address Vector segment for DC operations.
pub struct DciWqeBuilder<'a, T, Tab> {
    sq: &'a mut DciSendQueueState<T, Tab>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    /// Whether SIGNALED flag is set
    signaled: bool,
}

impl<'a, T, Tab> DciWqeBuilder<'a, T, Tab> {
    /// Write the control segment.
    ///
    /// This must be the first segment in every WQE.
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

    /// Add an address vector (required for DC operations).
    ///
    /// This segment specifies the destination DCT.
    pub fn av(mut self, dc_key: u64, dctn: u32, dlid: u16) -> Self {
        unsafe {
            AddressVector::write(self.wqe_ptr.add(self.offset), dc_key, dctn, dlid);
        }
        self.offset += AddressVector::SIZE;
        self.ds_count += 3; // AV = 48 bytes = 3 DS
        self
    }

    /// Add an RDMA segment (for WRITE/READ).
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
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

    /// Get a mutable slice for inline data (zero-copy).
    ///
    /// Returns the builder and a slice that can be written to directly.
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

    /// Finish the WQE construction (internal).
    ///
    /// Stores WQE info for later doorbell via `ring_sq_doorbell()`.
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
// Sparse/Dense DCI WQE Builders
// =============================================================================

/// Sparse DCI WQE builder that stores entry on finish.
pub struct SparseDciWqeBuilder<'a, T> {
    inner: DciWqeBuilder<'a, T, SparseWqeTable<T>>,
    entry: Option<T>,
}

impl<'a, T> SparseDciWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add an address vector.
    pub fn av(mut self, dc_key: u64, dctn: u32, dlid: u16) -> Self {
        self.inner = self.inner.av(dc_key, dctn, dlid);
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

    /// Finish the WQE construction.
    ///
    /// The doorbell will be issued when `ring_sq_doorbell()` is called.
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

/// Dense DCI WQE builder that stores entry on finish.
pub struct DenseDciWqeBuilder<'a, T> {
    inner: DciWqeBuilder<'a, T, DenseWqeTable<T>>,
    entry: T,
}

impl<'a, T> DenseDciWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add an address vector.
    pub fn av(mut self, dc_key: u64, dctn: u32, dlid: u16) -> Self {
        self.inner = self.inner.av(dc_key, dctn, dlid);
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
// DCI
// =============================================================================

/// DCI with sparse WQE table.
///
/// Only signaled WQEs have entries stored. Use this when you only need
/// to track completions for signaled WQEs.
///
/// Type parameters:
/// - `T`: Entry type stored in the WQE table
/// - `F`: Completion callback type `Fn(Cqe, T)`
///
/// For tracking all WQEs, use `DciDenseWqeTable` instead.
pub type DciSparseWqeTable<T, F> = Dci<T, SparseWqeTable<T>, F>;

/// DCI with dense WQE table.
///
/// Every WQE must have an entry stored. Use this when you need to track
/// all completions, including unsignaled WQEs.
///
/// Type parameters:
/// - `T`: Entry type stored in the WQE table
/// - `F`: Completion callback type `Fn(Option<Cqe>, T)`
///
/// For tracking only signaled WQEs, use `DciSparseWqeTable` instead.
pub type DciDenseWqeTable<T, F> = Dci<T, DenseWqeTable<T>, F>;

/// DC Initiator (DCI).
///
/// Used for sending RDMA operations to DCTs.
/// Created using mlx5dv_create_qp with DC type.
///
/// Type parameter `T` is the entry type stored in the WQE table for tracking
/// in-flight operations. When a completion arrives, the associated entry is
/// returned via the callback.
/// Type parameter `Tab` determines sparse vs dense table behavior.
/// Type parameter `F` is the completion callback type.
pub struct Dci<T, Tab, F> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: DcQpState,
    sq: Option<DciSendQueueState<T, Tab>>,
    callback: F,
    /// Weak reference to the CQ for unregistration on drop
    send_cq: Weak<RefCell<CompletionQueue>>,
}

impl Context {
    /// Create a DCI (DC Initiator) with sparse WQE table using mlx5dv_create_qp.
    ///
    /// Only signaled WQEs have entries stored. The callback is invoked for each
    /// completion with the CQE and the entry stored at WQE submission.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions (will be modified to register this DCI)
    /// * `config` - DCI configuration
    /// * `callback` - Completion callback `Fn(Cqe, T)` called for each signaled completion
    ///
    /// # Errors
    /// Returns an error if the DCI cannot be created.
    pub fn create_dci_sparse<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<RefCell<CompletionQueue>>,
        config: &DciConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<DciSparseWqeTable<T, F>>>>
    where
        T: 'static,
        F: Fn(Cqe, T) + 'static,
    {
        let dci = self.create_dci_raw(pd, send_cq, config, callback)?;
        let dci_rc = Rc::new(RefCell::new(dci));
        let qpn = dci_rc.borrow().qpn();

        // Initialize CQ direct access and register this DCI
        send_cq.borrow_mut().init_direct_access()?;
        send_cq
            .borrow_mut()
            .register_queue(qpn, Rc::downgrade(&dci_rc) as _);

        Ok(dci_rc)
    }

    /// Create a DCI (DC Initiator) with dense WQE table using mlx5dv_create_qp.
    ///
    /// Every WQE must have an entry stored. The callback is invoked for each
    /// completion: `Some(Cqe)` for signaled WQEs, `None` for unsignaled ones.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions (will be modified to register this DCI)
    /// * `config` - DCI configuration
    /// * `callback` - Completion callback `Fn(Option<Cqe>, T)` called for each completion
    ///
    /// # Errors
    /// Returns an error if the DCI cannot be created.
    pub fn create_dci_dense<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<RefCell<CompletionQueue>>,
        config: &DciConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<DciDenseWqeTable<T, F>>>>
    where
        T: 'static,
        F: Fn(Option<Cqe>, T) + 'static,
    {
        let dci = self.create_dci_dense_raw(pd, send_cq, config, callback)?;
        let dci_rc = Rc::new(RefCell::new(dci));
        let qpn = dci_rc.borrow().qpn();

        // Initialize CQ direct access and register this DCI
        send_cq.borrow_mut().init_direct_access()?;
        send_cq
            .borrow_mut()
            .register_queue(qpn, Rc::downgrade(&dci_rc) as _);

        Ok(dci_rc)
    }

    fn create_dci_raw<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<RefCell<CompletionQueue>>,
        config: &DciConfig,
        callback: F,
    ) -> io::Result<DciSparseWqeTable<T, F>>
    where
        F: Fn(Cqe, T),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = send_cq.borrow().as_ptr();
            qp_attr.recv_cq = send_cq.borrow().as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_DC as u64;
            mlx5_attr.dc_init_attr.dc_type = mlx5_sys::mlx5dv_dc_type_MLX5DV_DCTYPE_DCI;
            mlx5_attr
                .dc_init_attr
                .__bindgen_anon_1
                .dci_streams
                .log_num_concurent = 0;
            mlx5_attr
                .dc_init_attr
                .__bindgen_anon_1
                .dci_streams
                .log_num_errored = 0;

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dci {
                    qp,
                    state: DcQpState::Reset,
                    sq: None,
                    callback,
                    send_cq: Rc::downgrade(send_cq),
                })
            })
        }
    }

    fn create_dci_dense_raw<T, F>(
        &self,
        pd: &Pd,
        send_cq: &Rc<RefCell<CompletionQueue>>,
        config: &DciConfig,
        callback: F,
    ) -> io::Result<DciDenseWqeTable<T, F>>
    where
        F: Fn(Option<Cqe>, T),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = send_cq.borrow().as_ptr();
            qp_attr.recv_cq = send_cq.borrow().as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_DC as u64;
            mlx5_attr.dc_init_attr.dc_type = mlx5_sys::mlx5dv_dc_type_MLX5DV_DCTYPE_DCI;
            mlx5_attr
                .dc_init_attr
                .__bindgen_anon_1
                .dci_streams
                .log_num_concurent = 0;
            mlx5_attr
                .dc_init_attr
                .__bindgen_anon_1
                .dci_streams
                .log_num_errored = 0;

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dci {
                    qp,
                    state: DcQpState::Reset,
                    sq: None,
                    callback,
                    send_cq: Rc::downgrade(send_cq),
                })
            })
        }
    }
}

impl<T, Tab, F> Drop for Dci<T, Tab, F> {
    fn drop(&mut self) {
        // Unregister from CQ before destroying QP
        if let Some(cq) = self.send_cq.upgrade() {
            cq.borrow_mut().unregister_queue(self.qpn());
        }
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<T, Tab, F> Dci<T, Tab, F> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> DcQpState {
        self.state
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

    /// Transition DCI from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8) -> io::Result<()> {
        if self.state != DcQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in RESET state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_INIT;
            attr.pkey_index = 0;
            attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PKEY_INDEX
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PORT;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Init;
        Ok(())
    }

    /// Transition DCI from INIT to RTR.
    pub fn modify_to_rtr(&mut self, port: u8) -> io::Result<()> {
        if self.state != DcQpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Rtr;
        Ok(())
    }

    /// Transition DCI from RTR to RTS.
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state != DcQpState::Rtr {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in RTR state",
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

        self.state = DcQpState::Rts;
        Ok(())
    }

    fn sq_mut(&mut self) -> io::Result<&mut DciSendQueueState<T, Tab>> {
        self.sq
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    fn ring_sq_doorbell(&mut self) {
        if let Some(sq) = self.sq.as_mut() {
            sq.ring_doorbell();
        }
    }
}

impl<T, F> DciSparseWqeTable<T, F> {
    /// Initialize direct queue access.
    ///
    /// Call this after the DCI is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(DciSendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: 0,
            ci: 0,
            last_wqe: None,
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: 0,
            table: SparseWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        Ok(())
    }

    /// Activate DCI (transition to RTS) and initialize direct access.
    pub fn activate(&mut self, port: u8, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        self.modify_to_init(port)?;
        self.modify_to_rtr(port)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }

    /// Get a WQE builder for zero-copy WQE construction (signaled).
    ///
    /// The entry will be stored and returned via callback on completion.
    pub fn wqe_builder(&mut self, entry: T) -> io::Result<SparseDciWqeBuilder<'_, T>> {
        let sq = self.sq_mut()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi;
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(SparseDciWqeBuilder {
            inner: DciWqeBuilder {
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
    pub fn wqe_builder_unsignaled(&mut self) -> io::Result<SparseDciWqeBuilder<'_, T>> {
        let sq = self.sq_mut()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi;
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(SparseDciWqeBuilder {
            inner: DciWqeBuilder {
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

impl<T, F> DciDenseWqeTable<T, F> {
    /// Initialize direct queue access.
    ///
    /// Call this after the DCI is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(DciSendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: 0,
            ci: 0,
            last_wqe: None,
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: 0,
            table: DenseWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        Ok(())
    }

    /// Activate DCI (transition to RTS) and initialize direct access.
    pub fn activate(&mut self, port: u8, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        self.modify_to_init(port)?;
        self.modify_to_rtr(port)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }

    /// Get a WQE builder for zero-copy WQE construction.
    ///
    /// Every WQE must have an entry. The signaled flag controls whether
    /// a CQE is generated.
    pub fn wqe_builder(&mut self, entry: T, signaled: bool) -> io::Result<DenseDciWqeBuilder<'_, T>> {
        let sq = self.sq_mut()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi;
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(DenseDciWqeBuilder {
            inner: DciWqeBuilder {
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
// CompletionTarget impl for DciSparseWqeTable
// =============================================================================

impl<T, F> CompletionTarget for DciSparseWqeTable<T, F>
where
    F: Fn(Cqe, T),
{
    fn qpn(&self) -> u32 {
        Dci::qpn(self)
    }

    fn dispatch_cqe(&mut self, cqe: Cqe) {
        if let Some(sq) = self.sq.as_mut() {
            if let Some(entry) = sq.process_completion_sparse(cqe.wqe_counter) {
                (self.callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// CompletionTarget impl for DciDenseWqeTable
// =============================================================================

impl<T, F> CompletionTarget for DciDenseWqeTable<T, F>
where
    F: Fn(Option<Cqe>, T),
{
    fn qpn(&self) -> u32 {
        Dci::qpn(self)
    }

    fn dispatch_cqe(&mut self, cqe: Cqe) {
        if let Some(sq) = self.sq.as_mut() {
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

// =============================================================================
// DCT
// =============================================================================

/// DC Target (DCT).
///
/// Receives RDMA operations from DCIs via an SRQ.
/// Created using mlx5dv_create_qp with DC type.
pub struct Dct {
    qp: NonNull<mlx5_sys::ibv_qp>,
    dc_key: u64,
    state: DcQpState,
}

impl Context {
    /// Create a DCT (DC Target) using mlx5dv_create_qp.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `srq` - Shared Receive Queue for incoming messages
    /// * `cq` - Completion Queue
    /// * `config` - DCT configuration
    ///
    /// # Errors
    /// Returns an error if the DCT cannot be created.
    pub fn create_dct(
        &self,
        pd: &Pd,
        srq: &Srq,
        cq: &CompletionQueue,
        config: &DctConfig,
    ) -> io::Result<Dct> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = cq.as_ptr();
            qp_attr.recv_cq = cq.as_ptr();
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_DC as u64;
            mlx5_attr.dc_init_attr.dc_type = mlx5_sys::mlx5dv_dc_type_MLX5DV_DCTYPE_DCT;
            mlx5_attr.dc_init_attr.__bindgen_anon_1.dct_access_key = config.dc_key;

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dct {
                    qp,
                    dc_key: config.dc_key,
                    state: DcQpState::Reset,
                })
            })
        }
    }
}

impl Drop for Dct {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl Dct {
    /// Get the DCT number.
    pub fn dctn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the DC key.
    pub fn dc_key(&self) -> u64 {
        self.dc_key
    }

    /// Get the current state.
    pub fn state(&self) -> DcQpState {
        self.state
    }

    /// Transition DCT from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, access_flags: u32) -> io::Result<()> {
        if self.state != DcQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCT must be in RESET state",
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

        self.state = DcQpState::Init;
        Ok(())
    }

    /// Transition DCT from INIT to RTR.
    pub fn modify_to_rtr(&mut self, port: u8, max_dest_rd_atomic: u8) -> io::Result<()> {
        if self.state != DcQpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCT must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Rtr;
        Ok(())
    }

    /// Activate DCT (transition to RTR).
    ///
    /// DCT does not need RTS state, RTR is sufficient for receiving.
    pub fn activate(
        &mut self,
        port: u8,
        access_flags: u32,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(port, max_dest_rd_atomic)?;
        Ok(())
    }

    /// Get remote DCT info for senders.
    pub fn remote_info(&self, lid: u16) -> RemoteDctInfo {
        RemoteDctInfo {
            dctn: self.dctn(),
            dc_key: self.dc_key,
            lid,
        }
    }
}
