//! Tag Matching SRQ (TM-SRQ) management.
//!
//! TM-SRQ enables hardware-accelerated tag matching for RPC-style messaging.
//! Incoming messages are matched to posted receive buffers based on 64-bit tags.
//!
//! # Direct Verbs Interface
//!
//! This implementation provides direct WQE posting for:
//! - Tag operations (add/remove) via the `emit_tm_wqe!` macro
//! - Unordered receive WQE posting to the SRQ
//!
//! # Example
//!
//! ```ignore
//! use mlx5::emit_tm_wqe;
//!
//! // Add a tagged receive using emit_tm_wqe! macro
//! let ctx = tm_srq.cmd_emit_ctx()?;
//! emit_tm_wqe!(&ctx, tag_add {
//!     index: tag_index,
//!     tag: tag_value,
//!     sge: { addr: buf_addr, len: buf_len, lkey: lkey },
//!     signaled: entry,
//! })?;
//! tm_srq.ring_cmd_doorbell();
//!
//! // Post unordered receive (for unexpected messages)
//! tm_srq.post_unordered_recv(addr, len, lkey, entry)?;
//! tm_srq.ring_srq_doorbell();
//!
//! // Remove a tag using emit_tm_wqe! macro
//! let ctx = tm_srq.cmd_emit_ctx()?;
//! emit_tm_wqe!(&ctx, tag_del {
//!     index: tag_index,
//!     signaled: entry,
//! })?;
//! tm_srq.ring_cmd_doorbell();
//! ```

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{Cq, Cqe, CqeOpcode};
use crate::device::Context;
use crate::pd::Pd;
use crate::srq::SrqInfo;
use crate::wqe::{OrderedWqeTable, UnorderedWqeTable, emit::{SqState, TmCmdEmitContext}};

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
/// Generic over `CmdTableType` to support both sparse (signaled-only) and dense (all WQE)
/// completion modes, similar to the main Send Queue implementation.
pub(super) struct CmdQpState<CmdEntry, CmdTableType> {
    /// QP number.
    pub(super) qpn: u32,
    /// Send queue buffer.
    pub(super) sq_buf: *mut u8,
    /// Send queue WQE count (power of 2).
    pub(super) sq_wqe_cnt: u16,
    /// Producer index.
    pub(super) pi: Cell<u16>,
    /// Consumer index (for optimistic available calculation).
    pub(super) ci: Cell<u16>,
    /// Doorbell record pointer.
    pub(super) dbrec: *mut u32,
    /// BlueFlame register.
    pub(super) bf_reg: *mut u8,
    /// BlueFlame size.
    pub(super) bf_size: u32,
    /// BlueFlame offset.
    pub(super) bf_offset: Cell<u32>,
    /// Last WQE pointer and size for doorbell.
    pub(super) last_wqe: Cell<Option<(*mut u8, usize)>>,
    /// WQE table for tracking in-flight operations.
    pub(super) table: CmdTableType,
    /// Phantom for entry type.
    pub(super) _marker: std::marker::PhantomData<CmdEntry>,
}

impl<CmdEntry, CmdTableType> CmdQpState<CmdEntry, CmdTableType> {
    pub(super) fn optimistic_available(&self) -> u16 {
        self.sq_wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    /// Ring the doorbell using regular doorbell write.
    pub(super) fn ring_doorbell(&self) {
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

impl<CmdEntry> CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>> {
    /// Process a single completion.
    fn process_completion(&self, wqe_idx: u16) -> Option<CmdEntry> {
        let entry = self.table.take(wqe_idx)?;
        // ci_delta is the accumulated PI value at completion
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }

    /// Get an emit context for macro-based WQE emission.
    ///
    /// Returns `Err` if the Command QP is full.
    #[doc(hidden)]
    pub fn emit_ctx(&self) -> io::Result<TmCmdEmitContext<'_, CmdEntry>> {
        if !self.table.is_available(self.pi.get()) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Command QP full"));
        }
        Ok(TmCmdEmitContext {
            buf: self.sq_buf,
            wqe_cnt: self.sq_wqe_cnt,
            qpn: self.qpn,
            pi: &self.pi,
            ci: &self.ci,
            last_wqe: &self.last_wqe,
            table: &self.table,
        })
    }
}

/// SqState implementation for CmdQpState.
impl<CmdEntry> SqState for CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>> {
    type Entry = CmdEntry;

    #[inline]
    fn sq_buf(&self) -> *mut u8 {
        self.sq_buf
    }

    #[inline]
    fn wqe_cnt(&self) -> u16 {
        self.sq_wqe_cnt
    }

    #[inline]
    fn sqn(&self) -> u32 {
        self.qpn
    }

    #[inline]
    fn pi(&self) -> &Cell<u16> {
        &self.pi
    }

    #[inline]
    fn ci(&self) -> &Cell<u16> {
        &self.ci
    }

    #[inline]
    fn last_wqe(&self) -> &Cell<Option<(*mut u8, usize)>> {
        &self.last_wqe
    }

    #[inline]
    fn table(&self) -> &OrderedWqeTable<CmdEntry> {
        &self.table
    }

    #[inline]
    fn dbrec(&self) -> *mut u32 {
        self.dbrec
    }

    #[inline]
    fn bf_reg(&self) -> *mut u8 {
        self.bf_reg
    }

    #[inline]
    fn bf_size(&self) -> u32 {
        self.bf_size
    }

    #[inline]
    fn bf_offset(&self) -> &Cell<u32> {
        &self.bf_offset
    }
}

// =============================================================================
// SRQ State
// =============================================================================

/// SRQ state for direct receive WQE posting.
pub(super) struct TmSrqState<RecvEntry> {
    /// SRQ buffer pointer.
    pub(super) buf: *mut u8,
    /// Number of WQE slots (power of 2).
    pub(super) wqe_cnt: u32,
    /// WQE stride.
    pub(super) stride: u32,
    /// Producer index.
    pub(super) head: Cell<u32>,
    /// Doorbell record pointer.
    pub(super) dbrec: *mut u32,
    /// BlueFlame register pointer (from CmdQp).
    bf_reg: *mut u8,
    /// BlueFlame size.
    bf_size: u32,
    /// Current BlueFlame offset (alternates between 0 and bf_size).
    bf_offset: Cell<u32>,
    /// Pointer to the first pending WQE (for ring_doorbell_bf).
    pending_start_ptr: Cell<Option<*mut u8>>,
    /// Number of pending WQEs (for ring_doorbell_bf).
    pending_wqe_count: Cell<u32>,
    /// RQ WQE table for unordered completion tracking.
    pub(super) table: UnorderedWqeTable<RecvEntry>,
}

impl<RecvEntry> TmSrqState<RecvEntry> {
    pub(super) fn get_wqe_ptr(&self, idx: u32) -> *mut u8 {
        let offset = (idx & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    /// Ring the doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes to BlueFlame register if configured.
    /// The NIC fetches remaining WQE data via DMA.
    fn ring_doorbell(&self) {
        unsafe {
            std::ptr::write_volatile(self.dbrec, self.head.get().to_be());
        }
        udma_to_device_barrier!();

        // Minimum 8-byte BF write if BF is configured
        if !self.bf_reg.is_null() && self.bf_size > 0 {
            let bf_offset = self.bf_offset.get();
            let bf = unsafe { self.bf_reg.add(bf_offset as usize) as *mut u64 };
            let last_wqe = self.get_wqe_ptr(self.head.get().wrapping_sub(1));
            unsafe {
                std::ptr::write_volatile(bf, *(last_wqe as *const u64));
            }
            mmio_flush_writes!();
            self.bf_offset.set(bf_offset ^ self.bf_size);
        } else {
            mmio_flush_writes!();
        }

        // Reset pending tracking
        self.pending_wqe_count.set(0);
        self.pending_start_ptr.set(None);
    }

    /// Ring the doorbell with BlueFlame write of all pending WQEs.
    ///
    /// Copies pending WQEs to BlueFlame register for higher throughput if configured.
    fn ring_doorbell_bf(&self) {
        let wqe_count = self.pending_wqe_count.get();
        if wqe_count == 0 {
            return;
        }

        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, self.head.get().to_be());
        }
        udma_to_device_barrier!();

        // Copy pending WQEs to BlueFlame register if BF is configured
        if !self.bf_reg.is_null() && self.bf_size > 0 {
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
                        src = src.add(64);
                        dst = dst.add(64);
                    }
                    remaining = remaining.saturating_sub(64);
                }
            }

            mmio_flush_writes!();
            self.bf_offset.set(bf_offset ^ self.bf_size);
        } else {
            mmio_flush_writes!();
        }

        // Reset pending tracking
        self.pending_wqe_count.set(0);
        self.pending_start_ptr.set(None);
    }

    fn process_completion(&self, wqe_idx: u16) -> Option<RecvEntry> {
        self.table.take(wqe_idx)
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
/// # Type Parameters
/// * `CmdEntry` - Entry type for Command QP operations (TAG_ADD/TAG_DEL)
/// * `RecvEntry` - Entry type for RQ operations (unordered receive)
/// * `OnComplete` - Callback function type receiving `TmSrqCompletion<CmdEntry, RecvEntry>`
pub struct TmSrq<CmdEntry, RecvEntry, OnComplete> {
    srq: NonNull<mlx5_sys::ibv_srq>,
    /// Number of WQE slots (power of 2).
    #[allow(dead_code)]
    wqe_cnt: u32,
    /// Maximum number of tags.
    max_num_tags: u16,
    /// SRQ state for direct posting.
    srq_state: Option<TmSrqState<RecvEntry>>,
    /// Command QP state for tag operations.
    cmd_qp: Option<CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>>>,
    /// Callback for completion handling.
    callback: OnComplete,
    /// Weak reference to the CQ for unregistration on drop.
    send_cq: Weak<Cq>,
    /// Keep the PD alive while this TM-SRQ exists.
    _pd: Pd,
}

impl Context {
    /// Create a Tag Matching SRQ.
    ///
    /// Only signaled Command QP WQEs are tracked for completion handling.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `cq` - Completion Queue for TM operation completions
    /// * `config` - TM-SRQ configuration
    /// * `callback` - Callback for completions receiving `TmSrqCompletion<CmdEntry, RecvEntry>`
    ///
    /// # Errors
    /// Returns an error if the TM-SRQ cannot be created.
    pub fn create_tm_srq<CmdEntry, RecvEntry, OnComplete>(
        &self,
        pd: &Pd,
        cq: &Rc<Cq>,
        config: &TmSrqConfig,
        callback: OnComplete,
    ) -> io::Result<Rc<RefCell<TmSrq<CmdEntry, RecvEntry, OnComplete>>>>
    where
        CmdEntry: 'static,
        RecvEntry: 'static,
        OnComplete: Fn(TmSrqCompletion<CmdEntry, RecvEntry>) + 'static,
    {
        unsafe {
            // Create SRQ
            let (srq_nn, wqe_cnt) = Self::create_tm_srq_raw(self, pd, cq, config)?;

            // Query SRQ info for direct access
            let srq_info = query_srq_info(srq_nn)?;
            let cmd_info = query_cmd_qp_info_inner(srq_nn)?;

            let tm_srq = TmSrq {
                srq: srq_nn,
                wqe_cnt,
                max_num_tags: config.max_num_tags as u16,
                srq_state: Some(TmSrqState {
                    buf: srq_info.buf,
                    wqe_cnt,
                    stride: srq_info.stride,
                    head: Cell::new(0),
                    dbrec: srq_info.doorbell_record,
                    bf_reg: cmd_info.bf_reg, // Use Command QP's BlueFlame register
                    bf_size: cmd_info.bf_size,
                    bf_offset: Cell::new(0),
                    pending_start_ptr: Cell::new(None),
                    pending_wqe_count: Cell::new(0),
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
        cq: &Rc<Cq>,
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

impl<CmdEntry, RecvEntry, OnComplete> Drop for TmSrq<CmdEntry, RecvEntry, OnComplete> {
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

impl<CmdEntry, RecvEntry, OnComplete> TmSrq<CmdEntry, RecvEntry, OnComplete> {
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

    fn cmd_qp(&self) -> &CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>> {
        // Safe: srq_state is always Some after construction
        self.cmd_qp.as_ref().expect("cmd_qp not initialized")
    }

    fn srq_state(&self) -> &TmSrqState<RecvEntry> {
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

    /// Post an unordered receive WQE with a single data segment.
    ///
    /// This posts a receive WQE to the SRQ for unexpected (unmatched) messages.
    /// Call `ring_srq_doorbell()` after posting one or more receive WQEs.
    ///
    /// # Arguments
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    /// * `entry` - User entry to associate with this receive operation
    pub fn post_unordered_recv(&self, addr: u64, len: u32, lkey: u32, entry: RecvEntry) -> io::Result<()> {
        use crate::wqe::write_data_seg;

        let srq_state = self.srq_state();

        // Allocate 1 WQEBB (Next Segment 16B + Data Segment 16B = 32B fits in 64B WQEBB)
        let wqebb_start = srq_state
            .table
            .try_allocate_first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "RQ full"))?;

        let wqe_idx = srq_state.head.get();
        let wqe_ptr = srq_state.get_wqe_ptr(wqe_idx);

        unsafe {
            // Clear Next Segment (16 bytes)
            std::ptr::write_bytes(wqe_ptr, 0, 16);

            // Write Data Segment after Next Segment (offset 16)
            write_data_seg(wqe_ptr.add(16), len, lkey, addr);
        }

        // Store entry in table and advance head
        srq_state.table.store(wqe_idx as u16, wqebb_start, 1, entry);
        srq_state.head.set(wqe_idx.wrapping_add(1));

        // Update pending tracking for ring_doorbell_bf
        if srq_state.pending_start_ptr.get().is_none() {
            srq_state.pending_start_ptr.set(Some(wqe_ptr));
        }
        srq_state.pending_wqe_count.set(srq_state.pending_wqe_count.get() + 1);

        Ok(())
    }

    /// Ring the SRQ doorbell to submit receive WQEs.
    /// Ring the SRQ doorbell with minimum BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes to BlueFlame register if configured.
    /// The NIC fetches remaining WQE data via DMA.
    pub fn ring_srq_doorbell(&self) {
        self.srq_state().ring_doorbell();
    }

    /// Ring the SRQ doorbell with BlueFlame write of all pending WQEs.
    ///
    /// Copies pending WQEs to BlueFlame register for higher throughput if configured.
    /// Up to 256 bytes of WQE data can be copied in a single doorbell.
    pub fn ring_srq_doorbell_bf(&self) {
        self.srq_state().ring_doorbell_bf();
    }

    /// Process an RQ completion (internal use by dispatch_cqe).
    fn process_rq_completion(&self, wqe_idx: u16) -> Option<RecvEntry> {
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

impl<CmdEntry, RecvEntry, OnComplete> TmSrq<CmdEntry, RecvEntry, OnComplete> {
    /// Scan the table to get exact available count (slower).
    pub fn cmd_exact_available(&self) -> u16 {
        self.cmd_qp().table.count_available()
    }

    /// Check if a specific Command QP slot is available.
    pub fn cmd_is_slot_available(&self, idx: u16) -> bool {
        self.cmd_qp().table.is_available(idx)
    }

    /// Process a Command QP completion (internal use by dispatch_cqe).
    fn process_cmd_completion(&self, wqe_idx: u16) -> Option<CmdEntry> {
        self.cmd_qp().process_completion(wqe_idx)
    }

    /// Get an emit context for macro-based Command QP WQE emission.
    ///
    /// This context can be used with the `emit_tm_wqe!` macro for tag operations.
    ///
    /// # Example
    /// ```ignore
    /// let ctx = tm_srq.cmd_emit_ctx()?;
    /// emit_tm_wqe!(&ctx, tag_add {
    ///     index: tag_index,
    ///     tag: tag_value,
    ///     sge: { addr: buf_addr, len: buf_len, lkey: lkey },
    ///     signaled: entry,
    /// })?;
    /// tm_srq.ring_cmd_doorbell();
    /// ```
    pub fn cmd_emit_ctx(&self) -> io::Result<TmCmdEmitContext<'_, CmdEntry>> {
        self.cmd_qp().emit_ctx()
    }
}

// =============================================================================
// CompletionTarget Implementation
// =============================================================================

impl<CmdEntry, RecvEntry, OnComplete> CompletionTarget for TmSrq<CmdEntry, RecvEntry, OnComplete>
where
    OnComplete: Fn(TmSrqCompletion<CmdEntry, RecvEntry>),
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
            | CqeOpcode::RespRdmaWriteImm => {
                // RX queue completion (unordered receive)
                // Note: scatter-to-CQE is detected via cqe.is_inline_scatter(), not opcode
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

