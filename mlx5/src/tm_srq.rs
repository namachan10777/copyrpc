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

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::{CompletionQueue, Cqe, CqeOpcode};
use crate::device::Context;
use crate::pd::Pd;
use crate::srq::SrqInfo;
use crate::wqe::{
    CtrlSeg, DataSeg, SparseWqeTable, TmSeg, WQEBB_SIZE, WqeHandle, WqeOpcode,
};
use crate::CompletionTarget;

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
struct CmdQpState<T> {
    /// QP number.
    qpn: u32,
    /// Send queue buffer.
    sq_buf: *mut u8,
    /// Send queue WQE count (power of 2).
    sq_wqe_cnt: u16,
    /// Producer index.
    pi: u16,
    /// Consumer index (for optimistic available calculation).
    ci: u16,
    /// Doorbell record pointer.
    dbrec: *mut u32,
    /// BlueFlame register.
    bf_reg: *mut u8,
    /// BlueFlame size.
    bf_size: u32,
    /// BlueFlame offset.
    bf_offset: u32,
    /// Last WQE pointer and size for doorbell.
    last_wqe: Option<(*mut u8, usize)>,
    /// WQE table for tracking in-flight operations.
    table: SparseWqeTable<T>,
}

impl<T> CmdQpState<T> {
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.sq_wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.sq_buf.add(offset) }
    }

    fn advance_pi(&mut self, count: u16) {
        self.pi = self.pi.wrapping_add(count);
    }

    fn set_last_wqe(&mut self, ptr: *mut u8, size: usize) {
        self.last_wqe = Some((ptr, size));
    }

    fn optimistic_available(&self) -> u16 {
        self.sq_wqe_cnt - self.pi.wrapping_sub(self.ci)
    }

    /// Ring the doorbell using regular doorbell write.
    fn ring_doorbell(&mut self) {
        let Some((wqe_ptr, _)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        // Update doorbell record (dbrec[1] is SQ doorbell)
        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(wqe_ptr);
    }

    /// Ring the doorbell using BlueFlame (low latency, single WQE).
    fn ring_blueflame(&mut self, wqe_ptr: *mut u8) {
        mmio_flush_writes!();

        // Update doorbell record (dbrec[1] is SQ doorbell)
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

    fn process_completion(&mut self, wqe_idx: u16) -> Option<T> {
        self.ci = self.ci.wrapping_add(1);
        self.table.take(wqe_idx)
    }
}

// =============================================================================
// Command QP WQE Builder
// =============================================================================

/// WQE builder for Command QP tag operations.
pub struct CmdQpWqeBuilder<'a, T> {
    cmd_qp: &'a mut CmdQpState<T>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    entry: T,
}

impl<'a, T> CmdQpWqeBuilder<'a, T> {
    /// Write the control segment.
    ///
    /// This must be the first segment in every WQE.
    /// For TM operations, CQ_UPDATE (0x08) must be set in addition to TM segment flags.
    pub fn ctrl(mut self, opcode: WqeOpcode, imm: u32) -> Self {
        // MLX5_WQE_CTRL_CQ_UPDATE = 0x08 - required for TM operations
        const CQ_UPDATE: u8 = 0x08;
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
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
    pub fn tag_add(
        mut self,
        index: u16,
        tag: u64,
        addr: u64,
        len: u32,
        lkey: u32,
    ) -> Self {
        unsafe {
            TmSeg::write_add(
                self.wqe_ptr.add(self.offset),
                index,
                index, // sw_cnt - same as index, per rdma-core
                tag,
                !0u64, // mask: all bits must match
                true,  // signaled
            );
        }
        self.offset += TmSeg::SIZE;
        self.ds_count += 2; // TmSeg = 32 bytes = 2 DS

        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add TAG_DEL segment.
    pub fn tag_del(mut self, index: u16) -> Self {
        unsafe {
            TmSeg::write_del(self.wqe_ptr.add(self.offset), index, true);
        }
        self.offset += TmSeg::SIZE;
        self.ds_count += 2; // TmSeg = 32 bytes = 2 DS
        self
    }

    /// Finish the WQE construction.
    ///
    /// The doorbell will be issued when `ring_cmd_doorbell()` is called.
    pub fn finish(self) -> WqeHandle {
        unsafe {
            // Update DS count in control segment
            std::ptr::write_volatile(self.wqe_ptr.add(7), self.ds_count);
        }

        let wqe_idx = self.wqe_idx;

        // Store entry in table (always signaled for unordered)
        self.cmd_qp.table.store(wqe_idx, self.entry);

        self.cmd_qp.advance_pi(1);
        self.cmd_qp.set_last_wqe(self.wqe_ptr, self.offset);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_cmd_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        unsafe {
            // Update DS count in control segment
            std::ptr::write_volatile(self.wqe_ptr.add(7), self.ds_count);
        }

        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;

        // Store entry in table (always signaled for unordered)
        self.cmd_qp.table.store(wqe_idx, self.entry);

        self.cmd_qp.advance_pi(1);
        self.cmd_qp.ring_blueflame(wqe_ptr);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }
}

// =============================================================================
// SRQ State
// =============================================================================

/// SRQ state for direct receive WQE posting.
struct TmSrqState {
    /// SRQ buffer pointer.
    buf: *mut u8,
    /// Number of WQE slots (power of 2).
    wqe_cnt: u32,
    /// WQE stride.
    stride: u32,
    /// Producer index.
    head: u32,
    /// Doorbell record pointer.
    dbrec: *mut u32,
}

impl TmSrqState {
    fn get_wqe_ptr(&self, idx: u32) -> *mut u8 {
        let offset = (idx & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    unsafe fn post(&mut self, addr: u64, len: u32, lkey: u32) {
        let wqe_ptr = self.get_wqe_ptr(self.head);

        // SRQ WQE format: Next Segment (16 bytes) + Data Segment (16 bytes)
        std::ptr::write_bytes(wqe_ptr, 0, 16);

        // Write Data Segment at offset 16
        DataSeg::write(wqe_ptr.add(16), len, lkey, addr);

        self.head = self.head.wrapping_add(1);
    }

    fn ring_doorbell(&mut self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, self.head.to_be());
        }
    }
}

// =============================================================================
// Tag Matching SRQ
// =============================================================================

/// Tag Matching Shared Receive Queue.
///
/// TM-SRQ provides hardware-accelerated tag matching. When a message arrives,
/// the hardware matches it against posted receive tags and delivers to the
/// corresponding buffer.
///
/// TM operations (add_tag, remove_tag) use the internal Command QP.
///
/// Type parameter `T` is the entry type stored in the WQE table for tracking
/// in-flight tag operations. Type parameter `F` is the callback function type
/// that receives `(Cqe, Option<T>)` - `Some(entry)` for Command QP completions,
/// `None` for RX queue completions.
pub struct TagMatchingSrq<T, F> {
    srq: NonNull<mlx5_sys::ibv_srq>,
    /// Number of WQE slots (power of 2).
    wqe_cnt: u32,
    /// Maximum number of tags.
    max_num_tags: u16,
    /// SRQ state for direct posting.
    srq_state: Option<TmSrqState>,
    /// Command QP state for tag operations.
    cmd_qp: Option<CmdQpState<T>>,
    /// Callback for completion handling.
    callback: F,
    /// Weak reference to the CQ for unregistration on drop.
    send_cq: Weak<RefCell<CompletionQueue>>,
    /// Keep the PD alive while this TM-SRQ exists.
    _pd: Pd,
}

impl Context {
    /// Create a Tag Matching SRQ.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `cq` - Completion Queue for TM operation completions
    /// * `config` - TM-SRQ configuration
    /// * `callback` - Callback for completions: `(Cqe, Option<T>)` where
    ///   `Some(entry)` for Command QP completions, `None` for RX completions
    ///
    /// # Errors
    /// Returns an error if the TM-SRQ cannot be created.
    pub fn create_tm_srq<T, F>(
        &self,
        pd: &Pd,
        cq: &Rc<RefCell<CompletionQueue>>,
        config: &TmSrqConfig,
        callback: F,
    ) -> io::Result<Rc<RefCell<TagMatchingSrq<T, F>>>>
    where
        T: 'static,
        F: Fn(Cqe, Option<T>) + 'static,
    {
        unsafe {
            let mut attr: mlx5_sys::ibv_srq_init_attr_ex = MaybeUninit::zeroed().assume_init();
            attr.attr.max_wr = config.max_wr;
            attr.attr.max_sge = config.max_sge;
            attr.srq_type = mlx5_sys::ibv_srq_type_IBV_SRQT_TM;
            attr.pd = pd.as_ptr();
            attr.cq = cq.borrow().as_ptr();
            attr.tm_cap.max_num_tags = config.max_num_tags;
            attr.tm_cap.max_ops = config.max_ops;
            attr.comp_mask = mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_TYPE
                | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_PD
                | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_CQ
                | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_TM;

            let srq = mlx5_sys::ibv_create_srq_ex_ex(self.as_ptr(), &mut attr);
            let srq_nn = NonNull::new(srq).ok_or_else(io::Error::last_os_error)?;

            let tm_srq = TagMatchingSrq {
                srq: srq_nn,
                wqe_cnt: config.max_wr.next_power_of_two(),
                max_num_tags: config.max_num_tags as u16,
                srq_state: None,
                cmd_qp: None,
                callback,
                send_cq: Rc::downgrade(cq),
                _pd: pd.clone(),
            };

            let tm_srq_rc = Rc::new(RefCell::new(tm_srq));

            // Register with CQ using Command QP's qpn (will be set after init_direct_access)
            // For now, we'll register after init_direct_access is called

            Ok(tm_srq_rc)
        }
    }
}

impl<T, F> Drop for TagMatchingSrq<T, F> {
    fn drop(&mut self) {
        // Unregister from CQ
        if let Some(cmd_qp) = &self.cmd_qp {
            if let Some(cq) = self.send_cq.upgrade() {
                cq.borrow_mut().unregister_queue(cmd_qp.qpn);
            }
        }

        unsafe {
            mlx5_sys::ibv_destroy_srq(self.srq.as_ptr());
        }
    }
}

impl<T, F> TagMatchingSrq<T, F> {
    /// Query SRQ info using mlx5dv_init_obj.
    fn query_srq_info(&self) -> io::Result<SrqInfo> {
        unsafe {
            let mut dv_srq: MaybeUninit<mlx5_sys::mlx5dv_srq> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).srq.in_ = self.srq.as_ptr();
            (*obj_ptr).srq.out = dv_srq.as_mut_ptr();

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_SRQ as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_srq = dv_srq.assume_init();

            Ok(SrqInfo {
                buf: dv_srq.buf as *mut u8,
                dbrec: dv_srq.dbrec as *mut u32,
                stride: dv_srq.stride,
                srqn: dv_srq.srqn,
            })
        }
    }

    /// Query Command QP info.
    fn query_cmd_qp_info(&self) -> io::Result<CmdQpState<T>> {
        unsafe {
            // Get the internal Command QP from mlx5_srq structure
            let cmd_qp_ptr = {
                let ptr = (self.srq.as_ptr() as *const u8).offset(CMD_QP_OFFSET)
                    as *const *mut mlx5_sys::ibv_qp;
                *ptr
            };

            if cmd_qp_ptr.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
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
            let dbrec = dv_qp.dbrec as *mut u32;
            let current_pi = u32::from_be(std::ptr::read_volatile(dbrec.add(1))) as u16;

            Ok(CmdQpState {
                qpn,
                sq_buf: dv_qp.sq.buf as *mut u8,
                sq_wqe_cnt,
                pi: current_pi,
                ci: current_pi, // Assume all previous WQEs completed
                dbrec: dv_qp.dbrec as *mut u32,
                bf_reg: dv_qp.bf.reg as *mut u8,
                bf_size: dv_qp.bf.size,
                bf_offset: 0,
                last_wqe: None,
                table: SparseWqeTable::new(sq_wqe_cnt),
            })
        }
    }

    /// Initialize direct access for TM-SRQ (internal implementation).
    fn init_direct_access_inner(&mut self) -> io::Result<u32> {
        let srq_info = self.query_srq_info()?;
        let cmd_qp = self.query_cmd_qp_info()?;
        let qpn = cmd_qp.qpn;

        self.srq_state = Some(TmSrqState {
            buf: srq_info.buf,
            wqe_cnt: self.wqe_cnt,
            stride: srq_info.stride,
            head: 0,
            dbrec: srq_info.dbrec,
        });

        self.cmd_qp = Some(cmd_qp);

        Ok(qpn)
    }

    /// Get the Command QP number (for CQ registration).
    pub(crate) fn cmd_qpn(&self) -> Option<u32> {
        self.cmd_qp.as_ref().map(|cq| cq.qpn)
    }

    /// Get the SRQ number.
    pub fn srqn(&self) -> io::Result<u32> {
        self.query_srq_info().map(|info| info.srqn)
    }

    /// Get the maximum number of tags.
    pub fn max_num_tags(&self) -> u16 {
        self.max_num_tags
    }

    /// Get the raw ibv_srq pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_srq {
        self.srq.as_ptr()
    }

    fn cmd_qp_mut(&mut self) -> io::Result<&mut CmdQpState<T>> {
        self.cmd_qp
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    // =========================================================================
    // Unordered Receive WQE Operations (via SRQ)
    // =========================================================================

    /// Post an unordered receive WQE for unexpected messages.
    ///
    /// Call `ring_srq_doorbell()` after posting one or more receive WQEs.
    ///
    /// # Safety
    /// - The buffer must be registered and valid
    /// - Direct access must be initialized
    pub unsafe fn post_unordered_recv(&mut self, addr: u64, len: u32, lkey: u32) {
        if let Some(state) = self.srq_state.as_mut() {
            state.post(addr, len, lkey);
        }
    }

    /// Ring the SRQ doorbell to submit receive WQEs.
    pub fn ring_srq_doorbell(&mut self) {
        if let Some(state) = self.srq_state.as_mut() {
            state.ring_doorbell();
        }
    }

    // =========================================================================
    // Command QP Operations (for TM tag add/remove)
    // =========================================================================

    /// Get optimistic available count for Command QP.
    ///
    /// Based on pi - ci, but actual availability may be less due to gaps.
    pub fn cmd_optimistic_available(&self) -> u16 {
        self.cmd_qp
            .as_ref()
            .map(|cq| cq.optimistic_available())
            .unwrap_or(0)
    }

    /// Scan the table to get exact available count (slower).
    pub fn cmd_exact_available(&self) -> u16 {
        self.cmd_qp
            .as_ref()
            .map(|cq| cq.table.count_available())
            .unwrap_or(0)
    }

    /// Check if a specific Command QP slot is available.
    pub fn cmd_is_slot_available(&self, idx: u16) -> bool {
        self.cmd_qp
            .as_ref()
            .map(|cq| cq.table.is_available(idx))
            .unwrap_or(false)
    }

    /// Get a WQE builder for Command QP tag operations.
    ///
    /// Entry is required (always signaled for Command QP).
    pub fn cmd_wqe_builder(&mut self, entry: T) -> io::Result<CmdQpWqeBuilder<'_, T>> {
        let cmd_qp = self.cmd_qp_mut()?;
        if !cmd_qp.table.is_available(cmd_qp.pi) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Command QP full"));
        }

        let wqe_idx = cmd_qp.pi;
        let wqe_ptr = cmd_qp.get_wqe_ptr(wqe_idx);

        Ok(CmdQpWqeBuilder {
            cmd_qp,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            entry,
        })
    }

    /// Ring the Command QP doorbell to submit TM operations.
    pub fn ring_cmd_doorbell(&mut self) {
        if let Some(cmd_qp) = self.cmd_qp.as_mut() {
            cmd_qp.ring_doorbell();
        }
    }

    /// Process a Command QP completion.
    ///
    /// Returns the entry that was stored at the given wqe_idx.
    pub fn process_cmd_completion(&mut self, wqe_idx: u16) -> Option<T> {
        self.cmd_qp
            .as_mut()
            .and_then(|cq| cq.process_completion(wqe_idx))
    }
}

impl<T, F> TagMatchingSrq<T, F>
where
    T: 'static,
    F: Fn(Cqe, Option<T>) + 'static,
{
    /// Initialize direct access for TM-SRQ and register with CQ.
    ///
    /// This initializes both the SRQ state for receive posting and the
    /// Command QP state for tag operations. Also registers with the CQ
    /// for automatic completion dispatch.
    pub fn init_direct_access(this: &Rc<RefCell<Self>>) -> io::Result<()> {
        let qpn = this.borrow_mut().init_direct_access_inner()?;

        // Register with CQ
        if let Some(cq) = this.borrow().send_cq.upgrade() {
            cq.borrow_mut()
                .register_queue(qpn, Rc::downgrade(this) as _);
        }

        Ok(())
    }
}

// =============================================================================
// CompletionTarget Implementation
// =============================================================================

impl<T, F> CompletionTarget for TagMatchingSrq<T, F>
where
    F: Fn(Cqe, Option<T>),
{
    fn qpn(&self) -> u32 {
        self.cmd_qp.as_ref().map(|cq| cq.qpn).unwrap_or(0)
    }

    fn dispatch_cqe(&mut self, cqe: Cqe) {
        match cqe.opcode {
            CqeOpcode::Req | CqeOpcode::TmFinish => {
                // Command QP completion (tag add/remove)
                // TmFinish is the success opcode for TM operations
                if let Some(entry) = self.process_cmd_completion(cqe.wqe_counter) {
                    (self.callback)(cqe, Some(entry));
                }
            }
            CqeOpcode::TmMatchContext => {
                // Tag matching context match - incoming message matched a tag
                // app_info contains the tag handle
                (self.callback)(cqe, None);
            }
            CqeOpcode::RespSend
            | CqeOpcode::RespSendImm
            | CqeOpcode::RespSendInv
            | CqeOpcode::RespRdmaWriteImm => {
                // RX queue completion (unordered receive)
                (self.callback)(cqe, None);
            }
            CqeOpcode::ReqErr | CqeOpcode::RespErr => {
                // Error completion - try to get entry if it's a Command QP error
                let entry = self.process_cmd_completion(cqe.wqe_counter);
                (self.callback)(cqe, entry);
            }
        }
    }
}
