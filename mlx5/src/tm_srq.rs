//! Tag Matching SRQ (TM-SRQ) management.
//!
//! TM-SRQ enables hardware-accelerated tag matching for RPC-style messaging.
//! Incoming messages are matched to posted receive buffers based on 64-bit tags.
//!
//! # Direct Verbs Interface
//!
//! This implementation provides direct WQE posting for:
//! - Tag operations (add/remove) via the internal Command QP
//! - Receive WQE posting to the SRQ
//!
//! # Example
//!
//! ```ignore
//! // Initialize direct access
//! tm_srq.init_direct_access()?;
//!
//! // Add a tagged receive
//! unsafe {
//!     tm_srq.add_tag(0, tag, addr, len, lkey);
//!     tm_srq.ring_cmd_doorbell();
//! }
//!
//! // Post untagged receive (for unexpected messages)
//! unsafe {
//!     tm_srq.post_recv(addr, len, lkey);
//!     tm_srq.ring_doorbell();
//! }
//!
//! // Remove a tag
//! unsafe {
//!     tm_srq.remove_tag(handle);
//!     tm_srq.ring_cmd_doorbell();
//! }
//! ```

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::CompletionQueue;
use crate::device::Context;
use crate::pd::ProtectionDomain;
use crate::srq::SrqInfo;
use crate::wqe::{CtrlSeg, DataSeg, TmSeg, WQEBB_SIZE, WqeOpcode};

/// Offset from ibv_srq to cmd_qp pointer in mlx5_srq structure.
/// This is determined by the mlx5 provider's internal layout.
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
struct CmdQpState {
    /// QP number.
    qpn: u32,
    /// Send queue buffer.
    sq_buf: *mut u8,
    /// Send queue WQE count (power of 2).
    sq_wqe_cnt: u16,
    /// Producer index.
    pi: u16,
    /// Doorbell record pointer.
    dbrec: *mut u32,
    /// BlueFlame register.
    bf_reg: *mut u8,
    /// BlueFlame size.
    bf_size: u32,
    /// BlueFlame offset.
    bf_offset: u32,
    /// Last WQE pointer for doorbell.
    last_wqe: Option<*mut u8>,
}

impl CmdQpState {
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.sq_wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.sq_buf.add(offset) }
    }

    fn advance_pi(&mut self) {
        self.pi = self.pi.wrapping_add(1);
    }

    fn ring_doorbell(&mut self) {
        let Some(wqe_ptr) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        // Update doorbell record (dbrec[1] is SQ doorbell)
        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        // BlueFlame write
        if self.bf_size > 0 {
            let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) };
            mlx5_bf_copy!(bf, wqe_ptr);
            mmio_flush_writes!();
            self.bf_offset ^= self.bf_size;
        } else {
            // Regular doorbell
            let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) as *mut u64 };
            let ctrl = wqe_ptr as *const u64;
            unsafe {
                std::ptr::write_volatile(bf, *ctrl);
            }
            mmio_flush_writes!();
            self.bf_offset ^= self.bf_size;
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
pub struct TagMatchingSrq {
    srq: NonNull<mlx5_sys::ibv_srq>,
    /// Number of WQE slots (power of 2).
    wqe_cnt: u32,
    /// Maximum number of tags.
    max_num_tags: u16,
    /// SRQ state for direct posting.
    srq_state: Option<TmSrqState>,
    /// Command QP state for tag operations.
    cmd_qp: Option<CmdQpState>,
}

impl Context {
    /// Create a Tag Matching SRQ.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `cq` - Completion Queue for TM operation completions
    /// * `config` - TM-SRQ configuration
    ///
    /// # Errors
    /// Returns an error if the TM-SRQ cannot be created.
    pub fn create_tm_srq(
        &self,
        pd: &ProtectionDomain,
        cq: &CompletionQueue,
        config: &TmSrqConfig,
    ) -> io::Result<TagMatchingSrq> {
        unsafe {
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

            let srq = mlx5_sys::ibv_create_srq_ex_ex(self.ctx.as_ptr(), &mut attr);
            NonNull::new(srq).map_or(Err(io::Error::last_os_error()), |srq| {
                Ok(TagMatchingSrq {
                    srq,
                    wqe_cnt: config.max_wr.next_power_of_two(),
                    max_num_tags: config.max_num_tags as u16,
                    srq_state: None,
                    cmd_qp: None,
                })
            })
        }
    }
}

impl Drop for TagMatchingSrq {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_srq(self.srq.as_ptr());
        }
    }
}

impl TagMatchingSrq {
    /// Get the raw ibv_srq pointer.
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_srq {
        self.srq.as_ptr()
    }

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
    fn query_cmd_qp_info(&self) -> io::Result<CmdQpState> {
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

            Ok(CmdQpState {
                qpn,
                sq_buf: dv_qp.sq.buf as *mut u8,
                sq_wqe_cnt: dv_qp.sq.wqe_cnt as u16,
                pi: 0,
                dbrec: dv_qp.dbrec as *mut u32,
                bf_reg: dv_qp.bf.reg as *mut u8,
                bf_size: dv_qp.bf.size,
                bf_offset: 0,
                last_wqe: None,
            })
        }
    }

    /// Initialize direct access for TM-SRQ.
    ///
    /// This initializes both the SRQ state for receive posting and the
    /// Command QP state for tag operations.
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        let srq_info = self.query_srq_info()?;
        let cmd_qp = self.query_cmd_qp_info()?;

        self.srq_state = Some(TmSrqState {
            buf: srq_info.buf,
            wqe_cnt: self.wqe_cnt,
            stride: srq_info.stride,
            head: 0,
            dbrec: srq_info.dbrec,
        });

        self.cmd_qp = Some(cmd_qp);

        Ok(())
    }

    /// Get the SRQ number.
    pub fn srqn(&self) -> io::Result<u32> {
        self.query_srq_info().map(|info| info.srqn)
    }

    /// Get the maximum number of tags.
    pub fn max_num_tags(&self) -> u16 {
        self.max_num_tags
    }

    // =========================================================================
    // Tag Operations (via Command QP)
    // =========================================================================

    /// Add a tagged receive buffer.
    ///
    /// Posts a receive buffer with the specified tag. When a message with
    /// matching tag arrives, it will be delivered to this buffer.
    ///
    /// Call `ring_cmd_doorbell()` after posting one or more tag operations.
    ///
    /// # Arguments
    /// * `index` - Tag entry index (0 to max_num_tags-1)
    /// * `tag` - The 64-bit tag to match
    /// * `addr` - Buffer address
    /// * `len` - Buffer length
    /// * `lkey` - Local key for the buffer
    ///
    /// # Safety
    /// - The buffer must be registered and valid
    /// - Direct access must be initialized
    /// - Index must be valid
    pub unsafe fn add_tag(&mut self, index: u16, tag: u64, addr: u64, len: u32, lkey: u32) {
        let cmd_qp = self.cmd_qp.as_mut().expect("direct access not initialized");

        let wqe_idx = cmd_qp.pi;
        let wqe_ptr = cmd_qp.get_wqe_ptr(wqe_idx);

        // WQE layout for TAG_ADD:
        // - Control Segment (16 bytes)
        // - TM Segment (32 bytes)
        // - Data Segment (16 bytes)
        // Total: 64 bytes = 1 WQEBB, DS count = 4

        CtrlSeg::write(
            wqe_ptr,
            WqeOpcode::TagMatching as u8,
            wqe_idx,
            cmd_qp.qpn,
            4, // DS count
            0, // no completion
            0, // no imm
        );

        TmSeg::write_add(wqe_ptr.add(16), index, tag, !0u64, false);

        DataSeg::write(wqe_ptr.add(48), len, lkey, addr);

        cmd_qp.last_wqe = Some(wqe_ptr);
        cmd_qp.advance_pi();
    }

    /// Remove a previously added tag.
    ///
    /// Call `ring_cmd_doorbell()` after posting one or more tag operations.
    ///
    /// # Arguments
    /// * `index` - Tag entry index to remove
    ///
    /// # Safety
    /// - Direct access must be initialized
    /// - Index must be valid
    pub unsafe fn remove_tag(&mut self, index: u16) {
        let cmd_qp = self.cmd_qp.as_mut().expect("direct access not initialized");

        let wqe_idx = cmd_qp.pi;
        let wqe_ptr = cmd_qp.get_wqe_ptr(wqe_idx);

        // WQE layout for TAG_DEL:
        // - Control Segment (16 bytes)
        // - TM Segment (32 bytes)
        // Total: 48 bytes, DS count = 3

        CtrlSeg::write(
            wqe_ptr,
            WqeOpcode::TagMatching as u8,
            wqe_idx,
            cmd_qp.qpn,
            3, // DS count
            0, // no completion
            0, // no imm
        );

        TmSeg::write_del(wqe_ptr.add(16), index, false);

        cmd_qp.last_wqe = Some(wqe_ptr);
        cmd_qp.advance_pi();
    }

    /// Ring the Command QP doorbell to submit tag operations.
    pub fn ring_cmd_doorbell(&mut self) {
        if let Some(cmd_qp) = self.cmd_qp.as_mut() {
            cmd_qp.ring_doorbell();
        }
    }

    // =========================================================================
    // Receive WQE Operations (via SRQ)
    // =========================================================================

    /// Post a receive WQE for unexpected messages.
    ///
    /// Call `ring_doorbell()` after posting one or more receive WQEs.
    ///
    /// # Safety
    /// - The buffer must be registered and valid
    /// - Direct access must be initialized
    pub unsafe fn post_recv(&mut self, addr: u64, len: u32, lkey: u32) {
        if let Some(state) = self.srq_state.as_mut() {
            state.post(addr, len, lkey);
        }
    }

    /// Ring the SRQ doorbell to submit receive WQEs.
    pub fn ring_doorbell(&mut self) {
        if let Some(state) = self.srq_state.as_mut() {
            state.ring_doorbell();
        }
    }
}
