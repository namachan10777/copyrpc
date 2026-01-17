//! DC (Dynamically Connected) transport.
//!
//! DC provides scalable connectionless RDMA operations using:
//! - DCI (DC Initiator): Sends RDMA operations to DCTs
//! - DCT (DC Target): Receives RDMA operations via SRQ

use std::cell::{Cell, RefCell};
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
    pub dct_number: u32,
    /// DC key.
    pub dc_key: u64,
    /// Remote LID (Local Identifier).
    pub local_identifier: u16,
}

// =============================================================================
// DCI Send Queue State
// =============================================================================

/// Send Queue state for DCI.
///
/// Generic over the table type `TableType` which determines dense vs sparse behavior.
/// Both table types use interior mutability (Cell) so no RefCell wrapper is needed.
struct DciSendQueueState<Entry, TableType> {
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
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<Entry>>) so no RefCell needed.
    table: TableType,
    _marker: PhantomData<Entry>,
}

impl<Entry, TableType> DciSendQueueState<Entry, TableType> {
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

    /// Ring the doorbell using regular doorbell write.
    fn ring_doorbell(&self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe.get() else {
            return;
        };
        self.last_wqe.set(None);

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
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
            // Fallback to regular doorbell if BlueFlame not available
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

impl<Entry> DciSendQueueState<Entry, SparseWqeTable<Entry>> {
    fn process_completion_sparse(&self, wqe_idx: u16) -> Option<Entry> {
        let entry = self.table.take(wqe_idx)?;
        // For sparse tables, ci_delta is the accumulated PI value at completion
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }
}

impl<Entry> DciSendQueueState<Entry, DenseWqeTable<Entry>> {
    fn process_completions_dense<F>(&self, new_ci: u16, mut callback: F)
    where
        F: FnMut(u16, Entry),
    {
        // Use take_range which only holds a shared reference to the table.
        // This allows callbacks to safely access the DCI (e.g., post new WQEs)
        // without causing a RefCell borrow conflict.
        for (idx, entry) in self.table.take_range(self.ci.get(), new_ci) {
            callback(idx, entry.data);
            // For dense tables, ci_delta is the number of WQEBBs for this WQE
            self.ci.set(self.ci.get().wrapping_add(entry.ci_delta));
        }
    }
}

// =============================================================================
// DCI WQE Builder
// =============================================================================

/// Zero-copy WQE builder for DCI (internal implementation).
///
/// Similar to WqeBuilder but has Address Vector segment for DC operations.
pub struct DciWqeBuilder<'a, Entry, TableType> {
    sq: &'a DciSendQueueState<Entry, TableType>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    /// Whether SIGNALED flag is set
    signaled: bool,
}

impl<'a, Entry, TableType> DciWqeBuilder<'a, Entry, TableType> {
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
pub struct SparseDciWqeBuilder<'a, Entry> {
    inner: DciWqeBuilder<'a, Entry, SparseWqeTable<Entry>>,
    entry: Option<Entry>,
}

impl<'a, Entry> SparseDciWqeBuilder<'a, Entry> {
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

        // Calculate ci_delta (accumulated PI value at completion) before advancing PI
        if let Some(entry) = self.entry {
            let wqebb_cnt = calc_wqebb_cnt(self.inner.offset);
            let ci_delta = self.inner.sq.pi.get().wrapping_add(wqebb_cnt);
            self.inner.sq.table.store(wqe_idx, entry, ci_delta);
        }
        self.inner.finish_internal()
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_sq_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;

        // Calculate ci_delta (accumulated PI value at completion) before advancing PI
        if let Some(entry) = self.entry {
            let wqebb_cnt = calc_wqebb_cnt(self.inner.offset);
            let ci_delta = self.inner.sq.pi.get().wrapping_add(wqebb_cnt);
            self.inner.sq.table.store(wqe_idx, entry, ci_delta);
        }
        self.inner.finish_internal_with_blueflame()
    }
}

/// Dense DCI WQE builder that stores entry on finish.
pub struct DenseDciWqeBuilder<'a, Entry> {
    inner: DciWqeBuilder<'a, Entry, DenseWqeTable<Entry>>,
    entry: Entry,
}

impl<'a, Entry> DenseDciWqeBuilder<'a, Entry> {
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
        let wqebb_cnt = calc_wqebb_cnt(self.inner.offset);
        // For dense tables, ci_delta is the number of WQEBBs for this WQE
        self.inner.sq.table.store(wqe_idx, self.entry, wqebb_cnt);
        self.inner.finish_internal()
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_sq_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        let wqebb_cnt = calc_wqebb_cnt(self.inner.offset);
        // For dense tables, ci_delta is the number of WQEBBs for this WQE
        self.inner.sq.table.store(wqe_idx, self.entry, wqebb_cnt);
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
/// - `Entry`: Entry type stored in the WQE table
/// - `OnComplete`: Completion callback type `Fn(Cqe, Entry)`
///
/// For tracking all WQEs, use `DciDenseWqeTable` instead.
pub type DciSparseWqeTable<Entry, OnComplete> = Dci<Entry, SparseWqeTable<Entry>, OnComplete>;

/// DCI with dense WQE table.
///
/// Every WQE must have an entry stored. Use this when you need to track
/// all completions, including unsignaled WQEs.
///
/// Type parameters:
/// - `Entry`: Entry type stored in the WQE table
/// - `OnComplete`: Completion callback type `Fn(Option<Cqe>, Entry)`
///
/// For tracking only signaled WQEs, use `DciSparseWqeTable` instead.
pub type DciDenseWqeTable<Entry, OnComplete> = Dci<Entry, DenseWqeTable<Entry>, OnComplete>;

/// DC Initiator (DCI).
///
/// Used for sending RDMA operations to DCTs.
/// Created using mlx5dv_create_qp with DC type.
///
/// Type parameter `Entry` is the entry type stored in the WQE table for tracking
/// in-flight operations. When a completion arrives, the associated entry is
/// returned via the callback.
/// Type parameter `TableType` determines sparse vs dense table behavior.
/// Type parameter `OnComplete` is the completion callback type.
pub struct Dci<Entry, TableType, OnComplete> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: Cell<DcQpState>,
    sq: Option<DciSendQueueState<Entry, TableType>>,
    callback: OnComplete,
    /// Weak reference to the CQ for unregistration on drop
    send_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this DCI exists.
    _pd: Pd,
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
    /// * `callback` - Completion callback `Fn(Cqe, Entry)` called for each signaled completion
    ///
    /// # Errors
    /// Returns an error if the DCI cannot be created.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_dci_sparse<Entry, OnComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        config: &DciConfig,
        callback: OnComplete,
    ) -> io::Result<Rc<RefCell<DciSparseWqeTable<Entry, OnComplete>>>>
    where
        Entry: 'static,
        OnComplete: Fn(Cqe, Entry) + 'static,
    {
        let dci = self.create_dci_raw(pd, send_cq, config, callback)?;
        let dci_rc = Rc::new(RefCell::new(dci));
        let qpn = dci_rc.borrow().qpn();

        // Register this DCI with the CQ for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&dci_rc) as _);

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
    /// * `callback` - Completion callback `Fn(Option<Cqe>, Entry)` called for each completion
    ///
    /// # Errors
    /// Returns an error if the DCI cannot be created.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_dci_dense<Entry, OnComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        config: &DciConfig,
        callback: OnComplete,
    ) -> io::Result<Rc<RefCell<DciDenseWqeTable<Entry, OnComplete>>>>
    where
        Entry: 'static,
        OnComplete: Fn(Option<Cqe>, Entry) + 'static,
    {
        let dci = self.create_dci_dense_raw(pd, send_cq, config, callback)?;
        let dci_rc = Rc::new(RefCell::new(dci));
        let qpn = dci_rc.borrow().qpn();

        // Register this DCI with the CQ for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&dci_rc) as _);

        Ok(dci_rc)
    }

    fn create_dci_raw<Entry, OnComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        config: &DciConfig,
        callback: OnComplete,
    ) -> io::Result<DciSparseWqeTable<Entry, OnComplete>>
    where
        OnComplete: Fn(Cqe, Entry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = send_cq.as_ptr();
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = Dci {
                qp,
                state: Cell::new(DcQpState::Reset),
                sq: None,
                callback,
                send_cq: Rc::downgrade(send_cq),
                _pd: pd.clone(),
            };

            // Auto-initialize direct access
            DciSparseWqeTable::<Entry, OnComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }

    fn create_dci_dense_raw<Entry, OnComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        config: &DciConfig,
        callback: OnComplete,
    ) -> io::Result<DciDenseWqeTable<Entry, OnComplete>>
    where
        OnComplete: Fn(Option<Cqe>, Entry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = send_cq.as_ptr();
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = Dci {
                qp,
                state: Cell::new(DcQpState::Reset),
                sq: None,
                callback,
                send_cq: Rc::downgrade(send_cq),
                _pd: pd.clone(),
            };

            // Auto-initialize direct access
            DciDenseWqeTable::<Entry, OnComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }
}

impl<Entry, TableType, OnComplete> Drop for Dci<Entry, TableType, OnComplete> {
    fn drop(&mut self) {
        // Unregister from CQ before destroying QP
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(self.qpn());
        }
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<Entry, TableType, OnComplete> Dci<Entry, TableType, OnComplete> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> DcQpState {
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

    /// Transition DCI from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Reset {
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

        self.state.set(DcQpState::Init);
        Ok(())
    }

    /// Transition DCI from INIT to RTR.
    pub fn modify_to_rtr(&mut self, port: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Init {
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

        self.state.set(DcQpState::Rtr);
        Ok(())
    }

    /// Transition DCI from RTR to RTS.
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Rtr {
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

        self.state.set(DcQpState::Rts);
        Ok(())
    }

    fn sq(&self) -> io::Result<&DciSendQueueState<Entry, TableType>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell();
        }
    }
}

impl<Entry, OnComplete> DciSparseWqeTable<Entry, OnComplete> {
    /// Initialize direct queue access (internal implementation).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(DciSendQueueState {
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
            _marker: PhantomData,
        });

        Ok(())
    }

    /// Initialize direct queue access.
    ///
    /// # Deprecated
    /// Direct access is now auto-initialized at DCI creation.
    /// This method is kept for backwards compatibility and is a no-op if already initialized.
    #[deprecated(note = "Direct access is now auto-initialized at creation")]
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        self.init_direct_access_internal()
    }

    /// Activate DCI (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        self.modify_to_init(port)?;
        self.modify_to_rtr(port)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }

    /// Get a WQE builder for zero-copy WQE construction (signaled).
    ///
    /// The entry will be stored and returned via callback on completion.
    pub fn wqe_builder(&self, entry: Entry) -> io::Result<SparseDciWqeBuilder<'_, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
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
    pub fn wqe_builder_unsignaled(&self) -> io::Result<SparseDciWqeBuilder<'_, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
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

impl<Entry, OnComplete> DciDenseWqeTable<Entry, OnComplete> {
    /// Initialize direct queue access (internal implementation).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(DciSendQueueState {
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
            _marker: PhantomData,
        });

        Ok(())
    }

    /// Initialize direct queue access.
    ///
    /// # Deprecated
    /// Direct access is now auto-initialized at DCI creation.
    /// This method is kept for backwards compatibility and is a no-op if already initialized.
    #[deprecated(note = "Direct access is now auto-initialized at creation")]
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        self.init_direct_access_internal()
    }

    /// Activate DCI (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        self.modify_to_init(port)?;
        self.modify_to_rtr(port)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }

    /// Get a WQE builder for zero-copy WQE construction.
    ///
    /// Every WQE must have an entry. The signaled flag controls whether
    /// a CQE is generated.
    pub fn wqe_builder(&self, entry: Entry, signaled: bool) -> io::Result<DenseDciWqeBuilder<'_, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi.get();
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

impl<Entry, OnComplete> CompletionTarget for DciSparseWqeTable<Entry, OnComplete>
where
    OnComplete: Fn(Cqe, Entry),
{
    fn qpn(&self) -> u32 {
        Dci::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if let Some(sq) = self.sq.as_ref() {
            if let Some(entry) = sq.process_completion_sparse(cqe.wqe_counter) {
                (self.callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// CompletionTarget impl for DciDenseWqeTable
// =============================================================================

impl<Entry, OnComplete> CompletionTarget for DciDenseWqeTable<Entry, OnComplete>
where
    OnComplete: Fn(Option<Cqe>, Entry),
{
    fn qpn(&self) -> u32 {
        Dci::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
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

// =============================================================================
// DCT
// =============================================================================

/// DC Target (DCT).
///
/// Receives RDMA operations from DCIs via an SRQ.
/// Created using mlx5dv_create_qp with DC type.
///
/// Type parameter `T` is the entry type stored in the SRQ for tracking
/// in-flight receives.
pub struct Dct<T> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    dc_key: u64,
    state: DcQpState,
    /// Keep the PD alive while this DCT exists.
    _pd: Pd,
    /// SRQ for receive processing.
    srq: Srq<T>,
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
    pub fn create_dct<T>(
        &self,
        pd: &Pd,
        srq: &Srq<T>,
        cq: &CompletionQueue,
        config: &DctConfig,
    ) -> io::Result<Dct<T>> {
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dct {
                    qp,
                    dc_key: config.dc_key,
                    state: DcQpState::Reset,
                    _pd: pd.clone(),
                    srq: srq.clone(),
                })
            })
        }
    }
}

impl<T> Drop for Dct<T> {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<T> Dct<T> {
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
    ///
    /// DCT requires STATE, PATH_MTU, MIN_RNR_TIMER, and AV attributes for RTR transition.
    pub fn modify_to_rtr(&mut self, port: u8, min_rnr_timer: u8) -> io::Result<()> {
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
            attr.min_rnr_timer = min_rnr_timer;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
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
        min_rnr_timer: u8,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(port, min_rnr_timer)?;
        Ok(())
    }

    /// Get remote DCT info for senders.
    pub fn remote_info(&self, local_identifier: u16) -> RemoteDctInfo {
        RemoteDctInfo {
            dct_number: self.dctn(),
            dc_key: self.dc_key,
            local_identifier,
        }
    }

    /// Get access to the SRQ.
    pub fn srq(&self) -> &Srq<T> {
        &self.srq
    }

    /// Process a receive completion and return the associated entry.
    ///
    /// Call this when a receive CQE for this DCT is received.
    ///
    /// # Arguments
    /// * `wqe_idx` - WQE index from the CQE (wqe_counter field)
    pub fn process_recv_completion(&self, wqe_idx: u16) -> Option<T> {
        self.srq.process_recv_completion(wqe_idx)
    }
}
