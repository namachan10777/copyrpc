//! Tag Matching SRQ (TM-SRQ) management.
//!
//! TM-SRQ enables hardware-accelerated tag matching for RPC-style messaging.
//! Incoming messages are matched to posted receive buffers based on 64-bit tags.
//!
//! # Direct Verbs Interface
//!
//! This implementation provides direct WQE posting for:
//! - Tag operations (add/remove) via the internal Command QP
//! - Unordered receive WQE posting to the SRQ
//!
//! # Example
//!
//! ```ignore
//! // Initialize direct access
//! tm_srq.init_direct_access()?;
//!
//! // Add a tagged receive using Command QP
//! let builder = tm_srq.cmd_wqe_builder(my_entry)?;
//! builder.ctrl(WqeOpcode::TagMatching, 0)
//!     .tag_add(index, tag, addr, len, lkey)
//!     .finish();
//! tm_srq.ring_cmd_doorbell();
//!
//! // Post unordered receive (for unexpected messages)
//! unsafe {
//!     tm_srq.post_unordered_recv(addr, len, lkey);
//!     tm_srq.ring_srq_doorbell();
//! }
//!
//! // Remove a tag using Command QP
//! let builder = tm_srq.cmd_wqe_builder(my_entry)?;
//! builder.ctrl(WqeOpcode::TagMatching, 0)
//!     .tag_del(index)
//!     .finish();
//! tm_srq.ring_cmd_doorbell();
//! ```

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{CompletionQueue, Cqe, CqeOpcode};
use crate::device::Context;
use crate::pd::Pd;
use crate::srq::SrqInfo;
use crate::wqe::{
    CtrlSeg, DataSeg, OrderedWqeTable, TmSeg, UnorderedWqeTable, WQEBB_SIZE, WqeHandle, WqeOpcode,
};

// =============================================================================
// TM-SRQ Completion Types
// =============================================================================

/// Completion type for TM-SRQ operations.
///
/// Distinguishes between Command QP completions (tag operations) and
/// RQ completions (unordered receive).
pub enum TmSrqCompletion<CmdEntry, RecvEntry> {
    /// Command QP completion (TAG_ADD/TAG_DEL).
    CmdQp(Cqe, CmdEntry),
    /// RQ completion (unordered receive).
    Recv(Cqe, RecvEntry),
    /// Tag match notification (no entry, use `cqe.app_info` for tag handle).
    TagMatch(Cqe),
    /// Error completion (entry if from Command QP).
    Error(Cqe, Option<CmdEntry>),
}

/// Offset from ibv_srq to cmd_qp pointer in mlx5_srq structure.
///
/// This is determined by the mlx5 provider's internal layout and may change
/// between rdma-core versions. The official API `ibv_post_srq_ops()` exists
/// but has verbs overhead. This hack provides direct WQE access for lower latency.
///
/// Tested with rdma-core v28+. If TM operations fail, verify this offset
/// against your rdma-core version's `struct mlx5_srq` in `providers/mlx5/mlx5.h`.
const CMD_QP_OFFSET: isize = 296;

/// TM-SRQ configuration.
#[derive(Debug, Clone)]
pub struct TmSrqConfig {
    /// Maximum number of outstanding receive WRs.
    pub max_wr: u32,
    /// Maximum number of SGEs per WR.
    pub max_sge: u32,
    /// Maximum number of tags that can be posted.
    pub max_num_tags: u32,
    /// Maximum number of outstanding TM operations.
    pub max_ops: u32,
}

impl Default for TmSrqConfig {
    fn default() -> Self {
        Self {
            max_wr: 1024,
            max_sge: 1,
            max_num_tags: 64,
            max_ops: 16,
        }
    }
}

// =============================================================================
// Command QP State
// =============================================================================

/// Command QP state for TM tag operations.
///
/// Generic over `TableType` to support both sparse (signaled-only) and dense (all WQE)
/// completion modes, similar to the main Send Queue implementation.
struct CmdQpState<Entry, TableType> {
    /// QP number.
    qpn: u32,
    /// Send queue buffer.
    sq_buf: *mut u8,
    /// Send queue WQE count (power of 2).
    sq_wqe_cnt: u16,
    /// Producer index.
    pi: Cell<u16>,
    /// Consumer index (for optimistic available calculation).
    ci: Cell<u16>,
    /// Doorbell record pointer.
    dbrec: *mut u32,
    /// BlueFlame register.
    bf_reg: *mut u8,
    /// BlueFlame size.
    bf_size: u32,
    /// BlueFlame offset.
    bf_offset: Cell<u32>,
    /// Last WQE pointer and size for doorbell.
    last_wqe: Cell<Option<(*mut u8, usize)>>,
    /// WQE table for tracking in-flight operations.
    table: TableType,
    /// Phantom for entry type.
    _marker: std::marker::PhantomData<Entry>,
}

impl<Entry, TableType> CmdQpState<Entry, TableType> {
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.sq_wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.sq_buf.add(offset) }
    }

    fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    fn optimistic_available(&self) -> u16 {
        self.sq_wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    /// Ring the doorbell using regular doorbell write.
    fn ring_doorbell(&self) {
        let Some((wqe_ptr, _)) = self.last_wqe.get() else {
            return;
        };
        self.last_wqe.set(None);

        mmio_flush_writes!();

        // Update doorbell record (dbrec[1] is SQ doorbell)
        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(wqe_ptr);
    }

    /// Ring the doorbell using BlueFlame (low latency, single WQE).
    fn ring_blueflame(&self, wqe_ptr: *mut u8) {
        mmio_flush_writes!();

        // Update doorbell record (dbrec[1] is SQ doorbell)
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

impl<Entry> CmdQpState<Entry, OrderedWqeTable<Entry>> {
    /// Process a single completion.
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        let entry = self.table.take(wqe_idx)?;
        // ci_delta is the accumulated PI value at completion
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }
}

// =============================================================================
// Command QP WQE Builder
// =============================================================================

/// WQE builder for Command QP tag operations.
pub struct CmdQpWqeBuilder<'a, T, Tab> {
    cmd_qp: &'a CmdQpState<T, Tab>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    entry: T,
    /// Whether the TM operation requests a CQE on completion.
    signaled: bool,
}

impl<'a, T, Tab> CmdQpWqeBuilder<'a, T, Tab> {
    /// Write the control segment.
    ///
    /// This must be the first segment in every WQE.
    /// For TM operations, CQ_UPDATE (0x08) must be set in addition to TM segment flags.
    pub fn ctrl(mut self, opcode: WqeOpcode, imm: u32, opmod: u8) -> Self {
        // MLX5_WQE_CTRL_CQ_UPDATE = 0x08 - required for TM operations
        const CQ_UPDATE: u8 = 0x08;
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                opmod, // NEW: pass opmod parameter
                opcode as u8,
                self.wqe_idx,
                self.cmd_qp.qpn,
                0,
                CQ_UPDATE, // TM operations require CQ_UPDATE in ctrl segment
                imm,
            );
        }
        self.offset = 16; // CtrlSeg::SIZE
        self.ds_count = 1;
        self
    }

    /// Add TAG_ADD segment with data segment.
    ///
    /// # Arguments
    /// * `index` - Tag index in the SRQ tag list
    /// * `tag` - Tag value to match against incoming messages
    /// * `addr` - Buffer address for received data
    /// * `len` - Buffer length
    /// * `lkey` - Local key for the memory region
    /// * `signaled` - Whether to request a CQE on completion
    pub fn tag_add(
        mut self,
        index: u16,
        tag: u64,
        addr: u64,
        len: u32,
        lkey: u32,
        signaled: bool,
    ) -> Self {
        unsafe {
            TmSeg::write_add(
                self.wqe_ptr.add(self.offset),
                index,
                index, // sw_cnt - same as index, per rdma-core
                tag,
                !0u64, // mask: all bits must match
                signaled,
            );
        }
        self.offset += TmSeg::SIZE;
        self.ds_count += 2; // TmSeg = 32 bytes = 2 DS

        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self.signaled = signaled;
        self
    }

    /// Add TAG_DEL segment.
    ///
    /// # Arguments
    /// * `index` - Tag index to remove
    /// * `signaled` - Whether to request a CQE on completion
    pub fn tag_del(mut self, index: u16, signaled: bool) -> Self {
        unsafe {
            TmSeg::write_del(self.wqe_ptr.add(self.offset), index, signaled);
        }
        self.offset += TmSeg::SIZE;
        self.ds_count += 2; // TmSeg = 32 bytes = 2 DS
        self.signaled = signaled;
        self
    }
}

impl<'a, T> CmdQpWqeBuilder<'a, T, OrderedWqeTable<T>> {
    /// Finish the WQE construction (sparse mode).
    ///
    /// The entry is stored only if signaled is true.
    /// The doorbell will be issued when `ring_cmd_doorbell()` is called.
    pub fn finish(self) -> WqeHandle {
        // Extract all fields before any moves
        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;
        let ds_count = self.ds_count;
        let offset = self.offset;
        let cmd_qp = self.cmd_qp;
        let signaled = self.signaled;
        let entry = self.entry;

        // For sparse tables, only store if signaled
        if signaled {
            // ci_delta is the accumulated PI value at completion
            let ci_delta = cmd_qp.pi.get().wrapping_add(1);
            cmd_qp.table.store(wqe_idx, entry, ci_delta);
        }

        // Inline finish_internal logic
        unsafe {
            std::ptr::write_volatile(wqe_ptr.add(7), ds_count);
        }
        cmd_qp.advance_pi(1);
        cmd_qp.set_last_wqe(wqe_ptr, offset);

        WqeHandle {
            wqe_idx,
            size: offset,
        }
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        // Extract all fields before any moves
        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;
        let ds_count = self.ds_count;
        let offset = self.offset;
        let cmd_qp = self.cmd_qp;
        let signaled = self.signaled;
        let entry = self.entry;

        // For sparse tables, only store if signaled
        if signaled {
            let ci_delta = cmd_qp.pi.get().wrapping_add(1);
            cmd_qp.table.store(wqe_idx, entry, ci_delta);
        }

        // Inline finish_internal_with_blueflame logic
        unsafe {
            std::ptr::write_volatile(wqe_ptr.add(7), ds_count);
        }
        cmd_qp.advance_pi(1);
        cmd_qp.ring_blueflame(wqe_ptr);

        WqeHandle {
            wqe_idx,
            size: offset,
        }
    }
}

// =============================================================================
// SRQ State
// =============================================================================

/// SRQ state for direct receive WQE posting.
struct TmSrqState<U> {
    /// SRQ buffer pointer.
    buf: *mut u8,
    /// Number of WQE slots (power of 2).
    wqe_cnt: u32,
    /// WQE stride.
    stride: u32,
    /// Producer index.
    head: Cell<u32>,
    /// Doorbell record pointer.
    dbrec: *mut u32,
    /// RQ WQE table for unordered completion tracking.
    table: UnorderedWqeTable<U>,
}

impl<U> TmSrqState<U> {
    fn get_wqe_ptr(&self, idx: u32) -> *mut u8 {
        let offset = (idx & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, self.head.get().to_be());
        }
    }

    fn process_completion(&self, wqe_idx: u16) -> Option<U> {
        self.table.take(wqe_idx)
    }
}

// =============================================================================
// RQ WQE Builder
// =============================================================================

/// WQE builder for RQ unordered receive operations.
///
/// Supports incremental WQEBB allocation for multi-segment WQEs.
pub struct RqWqeBuilder<'a, U> {
    srq_state: &'a TmSrqState<U>,
    wqe_ptr: *mut u8,
    wqe_idx: u32,
    wqebb_start: u16,
    wqebb_count: u16,
    offset: usize,
    entry: U,
}

impl<'a, U> RqWqeBuilder<'a, U> {
    /// Add a data segment to the WQE.
    ///
    /// Automatically extends WQEBB allocation if needed.
    pub fn data_seg(mut self, addr: u64, len: u32, lkey: u32) -> io::Result<Self> {
        let new_offset = self.offset + DataSeg::SIZE;
        let required_wqebbs = new_offset.div_ceil(WQEBB_SIZE) as u16;

        // Extend allocation if we need more WQEBBs
        while self.wqebb_count < required_wqebbs {
            if !self
                .srq_state
                .table
                .try_extend(self.wqebb_start, self.wqebb_count)
            {
                // Extension failed, rollback
                self.srq_state
                    .table
                    .release(self.wqebb_start, self.wqebb_count);
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
            }
            self.wqebb_count += 1;
        }

        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset = new_offset;
        Ok(self)
    }

    /// Finish the WQE construction and post it to the RQ.
    ///
    /// The doorbell should be rung via `ring_srq_doorbell()` after posting.
    pub fn finish(self) {
        self.srq_state.table.store(
            self.wqe_idx as u16,
            self.wqebb_start,
            self.wqebb_count,
            self.entry,
        );
        self.srq_state
            .head
            .set(self.srq_state.head.get().wrapping_add(1));
    }
}

// =============================================================================
// Tag Matching SRQ
// =============================================================================

/// Tag Matching Shared Receive Queue (inner implementation).
///
/// TM-SRQ provides hardware-accelerated tag matching. When a message arrives,
/// the hardware matches it against posted receive tags and delivers to the
/// corresponding buffer.
///
/// TM operations (add_tag, remove_tag) use the internal Command QP.
///
/// # Type Parameters
/// * `T` - Entry type for Command QP operations (TAG_ADD/TAG_DEL)
/// * `Tab` - WQE table type for Command QP (OrderedWqeTable)
/// * `U` - Entry type for RQ operations (unordered receive)
/// * `F` - Callback function type receiving `TmSrqCompletion<T, U>`
pub struct TagMatchingSrq<T, Tab, U, F> {
    srq: NonNull<mlx5_sys::ibv_srq>,
    /// Number of WQE slots (power of 2).
    #[allow(dead_code)]
    wqe_cnt: u32,
    /// Maximum number of tags.
    max_num_tags: u16,
    /// SRQ state for direct posting.
    srq_state: Option<TmSrqState<U>>,
    /// Command QP state for tag operations.
    cmd_qp: Option<CmdQpState<T, Tab>>,
    /// Callback for completion handling.
    callback: F,
    /// Weak reference to the CQ for unregistration on drop.
    send_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this TM-SRQ exists.
    _pd: Pd,
}

/// Tag Matching SRQ with ordered Command QP table (only signaled WQEs tracked).
pub type TmSrq<T, U, F> = TagMatchingSrq<T, OrderedWqeTable<T>, U, F>;

impl Context {
    /// Create a Tag Matching SRQ.
    ///
    /// Only signaled Command QP WQEs are tracked for completion handling.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `cq` - Completion Queue for TM operation completions
    /// * `config` - TM-SRQ configuration
    /// * `callback` - Callback for completions receiving `TmSrqCompletion<T, U>`
    ///
    /// # Errors
    /// Returns an error if the TM-SRQ cannot be created.
    pub fn create_tm_srq<T, U, F>(
        &self,
        pd: &Pd,
        cq: &Rc<CompletionQueue>,
        config: &TmSrqConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<TmSrq<T, U, F>>>>
    where
        T: 'static,
        U: 'static,
        F: Fn(TmSrqCompletion<T, U>) + 'static,
    {
        unsafe {
            // Create SRQ
            let (srq_nn, wqe_cnt) = Self::create_tm_srq_raw(self, pd, cq, config)?;

            // Query SRQ info for direct access
            let srq_info = query_srq_info(srq_nn)?;
            let cmd_info = query_cmd_qp_info_inner(srq_nn)?;

            let tm_srq = TagMatchingSrq {
                srq: srq_nn,
                wqe_cnt,
                max_num_tags: config.max_num_tags as u16,
                srq_state: Some(TmSrqState {
                    buf: srq_info.buf,
                    wqe_cnt,
                    stride: srq_info.stride,
                    head: Cell::new(0),
                    dbrec: srq_info.doorbell_record,
                    table: UnorderedWqeTable::new(wqe_cnt as u16),
                }),
                cmd_qp: Some(CmdQpState {
                    qpn: cmd_info.qpn,
                    sq_buf: cmd_info.sq_buf,
                    sq_wqe_cnt: cmd_info.sq_wqe_cnt,
                    pi: Cell::new(cmd_info.current_pi),
                    ci: Cell::new(cmd_info.current_pi),
                    dbrec: cmd_info.dbrec,
                    bf_reg: cmd_info.bf_reg,
                    bf_size: cmd_info.bf_size,
                    bf_offset: Cell::new(0),
                    last_wqe: Cell::new(None),
                    table: OrderedWqeTable::new(cmd_info.sq_wqe_cnt),
                    _marker: std::marker::PhantomData,
                }),
                callback,
                send_cq: Rc::downgrade(cq),
                _pd: pd.clone(),
            };

            let tm_srq_rc = Rc::new(RefCell::new(tm_srq));

            // Register with CQ
            cq.register_queue(cmd_info.qpn, Rc::downgrade(&tm_srq_rc) as _);

            Ok(tm_srq_rc)
        }
    }

    /// Internal helper to create the raw SRQ.
    unsafe fn create_tm_srq_raw(
        &self,
        pd: &Pd,
        cq: &Rc<CompletionQueue>,
        config: &TmSrqConfig,
    ) -> io::Result<(NonNull<mlx5_sys::ibv_srq>, u32)> {
        let mut attr: mlx5_sys::ibv_srq_init_attr_ex = MaybeUninit::zeroed().assume_init();
        attr.attr.max_wr = config.max_wr;
        attr.attr.max_sge = config.max_sge;
        attr.srq_type = mlx5_sys::ibv_srq_type_IBV_SRQT_TM;
        attr.pd = pd.as_ptr();
        attr.cq = cq.as_ptr();
        attr.tm_cap.max_num_tags = config.max_num_tags;
        attr.tm_cap.max_ops = config.max_ops;
        attr.comp_mask = mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_TYPE
            | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_PD
            | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_CQ
            | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_TM;

        let srq = mlx5_sys::ibv_create_srq_ex_ex(self.as_ptr(), &mut attr);
        let srq_nn = NonNull::new(srq).ok_or_else(io::Error::last_os_error)?;
        let wqe_cnt = config.max_wr.next_power_of_two();

        Ok((srq_nn, wqe_cnt))
    }
}

impl<T, Tab, U, F> Drop for TagMatchingSrq<T, Tab, U, F> {
    fn drop(&mut self) {
        // Unregister from CQ
        if let Some(cmd_qp) = &self.cmd_qp
            && let Some(cq) = self.send_cq.upgrade()
        {
            cq.unregister_queue(cmd_qp.qpn);
        }

        unsafe {
            mlx5_sys::ibv_destroy_srq(self.srq.as_ptr());
        }
    }
}

/// Internal helper to query Command QP info (common code).
struct CmdQpInfo {
    qpn: u32,
    sq_buf: *mut u8,
    sq_wqe_cnt: u16,
    current_pi: u16,
    dbrec: *mut u32,
    bf_reg: *mut u8,
    bf_size: u32,
}

/// Query SRQ info using mlx5dv_init_obj.
fn query_srq_info(srq: NonNull<mlx5_sys::ibv_srq>) -> io::Result<SrqInfo> {
    unsafe {
        let mut dv_srq: MaybeUninit<mlx5_sys::mlx5dv_srq> = MaybeUninit::zeroed();
        let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

        let obj_ptr = obj.as_mut_ptr();
        (*obj_ptr).srq.in_ = srq.as_ptr();
        (*obj_ptr).srq.out = dv_srq.as_mut_ptr();

        let ret =
            mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_SRQ as u64);
        if ret != 0 {
            return Err(io::Error::from_raw_os_error(-ret));
        }

        let dv_srq = dv_srq.assume_init();

        Ok(SrqInfo {
            buf: dv_srq.buf as *mut u8,
            doorbell_record: dv_srq.dbrec,
            stride: dv_srq.stride,
            srq_number: dv_srq.srqn,
        })
    }
}

fn query_cmd_qp_info_inner(srq: NonNull<mlx5_sys::ibv_srq>) -> io::Result<CmdQpInfo> {
    unsafe {
        // Get the internal Command QP from mlx5_srq structure
        let cmd_qp_ptr = {
            let ptr =
                (srq.as_ptr() as *const u8).offset(CMD_QP_OFFSET) as *const *mut mlx5_sys::ibv_qp;
            *ptr
        };

        if cmd_qp_ptr.is_null() {
            return Err(io::Error::other(
                "TM-SRQ cmd_qp is null (TM not supported?)",
            ));
        }

        let mut dv_qp: MaybeUninit<mlx5_sys::mlx5dv_qp> = MaybeUninit::zeroed();
        let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

        let obj_ptr = obj.as_mut_ptr();
        (*obj_ptr).qp.in_ = cmd_qp_ptr;
        (*obj_ptr).qp.out = dv_qp.as_mut_ptr();

        let ret =
            mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_QP as u64);
        if ret != 0 {
            return Err(io::Error::from_raw_os_error(-ret));
        }

        let dv_qp = dv_qp.assume_init();
        let qpn = (*cmd_qp_ptr).qp_num;
        let sq_wqe_cnt = dv_qp.sq.wqe_cnt as u16;

        // Read current PI from doorbell record (dbrec[1] is SQ doorbell)
        // The Command QP may have been used by rdma-core internally
        let current_pi = u32::from_be(std::ptr::read_volatile(dv_qp.dbrec.add(1))) as u16;

        Ok(CmdQpInfo {
            qpn,
            sq_buf: dv_qp.sq.buf as *mut u8,
            sq_wqe_cnt,
            current_pi,
            dbrec: dv_qp.dbrec,
            bf_reg: dv_qp.bf.reg as *mut u8,
            bf_size: dv_qp.bf.size,
        })
    }
}

impl<T, Tab, U, F> TagMatchingSrq<T, Tab, U, F> {
    /// Get the SRQ number.
    pub fn srq_number(&self) -> io::Result<u32> {
        query_srq_info(self.srq).map(|info| info.srq_number)
    }

    /// Get the maximum number of tags.
    pub fn max_num_tags(&self) -> u16 {
        self.max_num_tags
    }

    /// Get the raw ibv_srq pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_srq {
        self.srq.as_ptr()
    }

    fn cmd_qp(&self) -> &CmdQpState<T, Tab> {
        // Safe: srq_state is always Some after construction
        self.cmd_qp.as_ref().expect("cmd_qp not initialized")
    }

    fn srq_state(&self) -> &TmSrqState<U> {
        // Safe: srq_state is always Some after construction
        self.srq_state.as_ref().expect("srq_state not initialized")
    }

    // =========================================================================
    // Unordered Receive WQE Operations (via SRQ)
    // =========================================================================

    /// Get the number of available WQEBBs in the RQ.
    pub fn rq_available_wqebbs(&self) -> u16 {
        self.srq_state().table.available_wqebbs()
    }

    /// Get a WQE builder for RQ unordered receive operations.
    ///
    /// Entry is required for completion tracking.
    pub fn rq_wqe_builder(&self, entry: U) -> io::Result<RqWqeBuilder<'_, U>> {
        let srq_state = self.srq_state();
        let wqebb_start = srq_state
            .table
            .try_allocate_first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "RQ full"))?;

        let wqe_idx = srq_state.head.get();
        let wqe_ptr = srq_state.get_wqe_ptr(wqe_idx);

        // Clear Next Segment (16 bytes)
        unsafe { std::ptr::write_bytes(wqe_ptr, 0, 16) };

        Ok(RqWqeBuilder {
            srq_state,
            wqe_ptr,
            wqe_idx,
            wqebb_start,
            wqebb_count: 1,
            offset: 16, // Start after Next Segment
            entry,
        })
    }

    /// Post a simple unordered receive WQE with a single data segment.
    ///
    /// This is a convenience method for the common case.
    /// Call `ring_srq_doorbell()` after posting one or more receive WQEs.
    pub fn post_unordered_recv(&self, addr: u64, len: u32, lkey: u32, entry: U) -> io::Result<()> {
        self.rq_wqe_builder(entry)?
            .data_seg(addr, len, lkey)?
            .finish();
        Ok(())
    }

    /// Ring the SRQ doorbell to submit receive WQEs.
    pub fn ring_srq_doorbell(&self) {
        self.srq_state().ring_doorbell();
    }

    /// Process an RQ completion (internal use by dispatch_cqe).
    fn process_rq_completion(&self, wqe_idx: u16) -> Option<U> {
        self.srq_state().process_completion(wqe_idx)
    }

    // =========================================================================
    // Command QP Operations (for TM tag add/remove)
    // =========================================================================

    /// Get optimistic available count for Command QP.
    ///
    /// Based on pi - ci, but actual availability may be less due to gaps.
    pub fn cmd_optimistic_available(&self) -> u16 {
        self.cmd_qp().optimistic_available()
    }

    /// Ring the Command QP doorbell to submit TM operations.
    pub fn ring_cmd_doorbell(&self) {
        self.cmd_qp().ring_doorbell();
    }
}

// =============================================================================
// Command QP Methods
// =============================================================================

impl<T, U, F> TagMatchingSrq<T, OrderedWqeTable<T>, U, F> {
    /// Scan the table to get exact available count (slower).
    pub fn cmd_exact_available(&self) -> u16 {
        self.cmd_qp().table.count_available()
    }

    /// Check if a specific Command QP slot is available.
    pub fn cmd_is_slot_available(&self, idx: u16) -> bool {
        self.cmd_qp().table.is_available(idx)
    }

    /// Get a WQE builder for Command QP tag operations (sparse mode).
    pub fn cmd_wqe_builder(
        &self,
        entry: T,
    ) -> io::Result<CmdQpWqeBuilder<'_, T, OrderedWqeTable<T>>> {
        let cmd_qp = self.cmd_qp();
        if !cmd_qp.table.is_available(cmd_qp.pi.get()) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Command QP full"));
        }

        let wqe_idx = cmd_qp.pi.get();
        let wqe_ptr = cmd_qp.get_wqe_ptr(wqe_idx);

        Ok(CmdQpWqeBuilder {
            cmd_qp,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            entry,
            signaled: false,
        })
    }

    /// Process a Command QP completion (internal use by dispatch_cqe).
    fn process_cmd_completion(&self, wqe_idx: u16) -> Option<T> {
        self.cmd_qp().process_completion(wqe_idx)
    }
}

// =============================================================================
// CompletionTarget Implementation
// =============================================================================

impl<T, U, F> CompletionTarget for TmSrq<T, U, F>
where
    F: Fn(TmSrqCompletion<T, U>),
{
    fn qpn(&self) -> u32 {
        self.cmd_qp().qpn
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        match cqe.opcode {
            CqeOpcode::Req | CqeOpcode::TmFinish => {
                // Command QP completion (tag add/remove)
                // TmFinish is the success opcode for TM operations
                if let Some(entry) = self.process_cmd_completion(cqe.wqe_counter) {
                    (self.callback)(TmSrqCompletion::CmdQp(cqe, entry));
                }
            }
            CqeOpcode::TmMatchContext => {
                // Tag matching context match - incoming message matched a tag
                // app_info contains the tag handle
                (self.callback)(TmSrqCompletion::TagMatch(cqe));
            }
            CqeOpcode::RespSend
            | CqeOpcode::RespSendImm
            | CqeOpcode::RespSendInv
            | CqeOpcode::RespRdmaWriteImm
            | CqeOpcode::InlineScatter32
            | CqeOpcode::InlineScatter64 => {
                // RX queue completion (unordered receive)
                // InlineScatter opcodes indicate data is inlined in the CQE
                if let Some(entry) = self.process_rq_completion(cqe.wqe_counter) {
                    (self.callback)(TmSrqCompletion::Recv(cqe, entry));
                }
            }
            CqeOpcode::ReqErr | CqeOpcode::RespErr => {
                // Error completion - try to get entry if it's a Command QP error
                let entry = self.process_cmd_completion(cqe.wqe_counter);
                (self.callback)(TmSrqCompletion::Error(cqe, entry));
            }
        }
    }
}
