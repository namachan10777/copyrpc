//! Completion Queue (CQ) management.
//!
//! A Completion Queue is used to notify the application when work requests
//! have completed. CQs can be shared across multiple Queue Pairs.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Weak;
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::device::Context;
use crate::CompletionTarget;

// =============================================================================
// CQE Types
// =============================================================================

/// CQE opcode values.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqeOpcode {
    /// Requester completion (SQ)
    Req = 0x00,
    /// Responder RDMA write with immediate
    RespRdmaWriteImm = 0x01,
    /// Responder send
    RespSend = 0x02,
    /// Responder send with immediate
    RespSendImm = 0x03,
    /// Responder send with invalidate
    RespSendInv = 0x04,
    /// Tag Matching context match
    TmMatchContext = 0x05,
    /// Tag Matching operation finish (TM add/remove completed)
    TmFinish = 0x06,
    /// Requester error
    ReqErr = 0x0d,
    /// Responder error
    RespErr = 0x0e,
}

impl CqeOpcode {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(Self::Req),
            0x01 => Some(Self::RespRdmaWriteImm),
            0x02 => Some(Self::RespSend),
            0x03 => Some(Self::RespSendImm),
            0x04 => Some(Self::RespSendInv),
            0x05 => Some(Self::TmMatchContext),
            0x06 => Some(Self::TmFinish),
            0x0d => Some(Self::ReqErr),
            0x0e => Some(Self::RespErr),
            _ => None,
        }
    }
}

/// Parsed CQE (Completion Queue Entry).
#[derive(Debug, Clone, Copy)]
pub struct Cqe {
    /// Operation code
    pub opcode: CqeOpcode,
    /// WQE counter (index)
    pub wqe_counter: u16,
    /// QP number
    pub qp_num: u32,
    /// Byte count (for receive completions)
    pub byte_cnt: u32,
    /// Immediate data or invalidation rkey
    pub imm: u32,
    /// Error syndrome (0 = success)
    pub syndrome: u8,
    /// Vendor error syndrome (for error CQEs)
    pub vendor_err: u8,
    /// Application info (TM tag handle for Tag Matching operations)
    pub app_info: u16,
}

impl Cqe {
    /// Parse CQE from raw memory pointer.
    ///
    /// # Safety
    /// The pointer must point to a valid 64-byte CQE.
    ///
    /// CQE64 layout (success):
    /// - offset 36: imm_inval_pkey (4B, big-endian)
    /// - offset 44: byte_cnt (4B, big-endian)
    /// - offset 48-49: timestamp_h (2B)
    /// - offset 50-51: app_info (2B, big-endian) - TM tag handle for Tag Matching
    /// - offset 52-55: timestamp_l (4B)
    /// - offset 56: sop_drop_qpn (4B, big-endian) - QP number in [23:0]
    /// - offset 60: wqe_counter (2B, big-endian)
    /// - offset 63: op_own (1B) - opcode[7:4] | owner_bit[0]
    ///
    /// CQE64 layout (error - opcode 0x0d or 0x0e):
    /// - offset 54: vendor_err_synd (1B)
    /// - offset 55: syndrome (1B)
    /// - offset 56: s_wqe_opcode_qpn (4B, big-endian)
    /// - offset 60: wqe_counter (2B, big-endian)
    /// - offset 63: op_own (1B)
    pub(crate) unsafe fn from_ptr(ptr: *const u8) -> Self {
        let op_own = std::ptr::read_volatile(ptr.add(63));
        let opcode = CqeOpcode::from_u8(op_own >> 4).unwrap_or(CqeOpcode::ReqErr);

        let wqe_counter =
            u16::from_be(std::ptr::read_volatile(ptr.add(60) as *const u16));

        let qp_num =
            u32::from_be(std::ptr::read_volatile(ptr.add(56) as *const u32)) & 0x00FF_FFFF;

        let byte_cnt =
            u32::from_be(std::ptr::read_volatile(ptr.add(44) as *const u32));

        let imm =
            u32::from_be(std::ptr::read_volatile(ptr.add(36) as *const u32));

        // app_info at offset 50: contains TM tag handle for Tag Matching operations
        let app_info =
            u16::from_be(std::ptr::read_volatile(ptr.add(50) as *const u16));

        // Syndrome and vendor_err are only valid for error CQEs (opcode 0x0d=ReqErr or 0x0e=RespErr).
        // For success CQEs, offsets 54-55 are part of the timestamp field.
        let (vendor_err, syndrome) = if opcode == CqeOpcode::ReqErr || opcode == CqeOpcode::RespErr {
            (
                std::ptr::read_volatile(ptr.add(54)),
                std::ptr::read_volatile(ptr.add(55)),
            )
        } else {
            (0, 0)
        };

        Self {
            opcode,
            wqe_counter,
            qp_num,
            byte_cnt,
            imm,
            syndrome,
            vendor_err,
            app_info,
        }
    }
}

// =============================================================================
// CQ State
// =============================================================================

/// Internal CQ state for direct verbs polling.
pub(crate) struct CqState {
    /// CQ buffer base address
    buf: *mut u8,
    /// Number of CQEs (power of 2)
    cqe_cnt: u32,
    /// CQE size in bytes (64 or 128)
    cqe_size: u32,
    /// Doorbell record pointer
    dbrec: *mut u32,
    /// Consumer index
    ci: u32,
}

// =============================================================================
// Completion Queue
// =============================================================================

/// Completion Queue.
///
/// Used to receive completion notifications for send and receive operations.
/// Multiple QPs can share the same CQ.
pub struct CompletionQueue {
    cq: NonNull<mlx5_sys::ibv_cq>,
    state: Option<CqState>,
    /// Registered queues (QPN -> CompletionTarget)
    queues: HashMap<u32, Weak<RefCell<dyn CompletionTarget>>>,
}

impl Context {
    /// Create a Completion Queue using extended CQ API.
    ///
    /// Extended CQs support additional features required for TM-SRQ and other
    /// advanced operations.
    ///
    /// # Arguments
    /// * `cqe` - Minimum number of CQ entries (actual may be larger)
    ///
    /// # Errors
    /// Returns an error if the CQ cannot be created.
    pub fn create_cq(&self, cqe: i32) -> io::Result<CompletionQueue> {
        unsafe {
            let mut attr: mlx5_sys::ibv_cq_init_attr_ex = MaybeUninit::zeroed().assume_init();
            attr.cqe = cqe as u32;
            attr.cq_context = std::ptr::null_mut();
            attr.channel = std::ptr::null_mut();
            attr.comp_vector = 0;
            attr.wc_flags = 0;
            attr.comp_mask = 0;
            attr.flags = 0;

            let cq_ex = mlx5_sys::ibv_create_cq_ex_ex(self.ctx.as_ptr(), &mut attr);
            if cq_ex.is_null() {
                return Err(io::Error::last_os_error());
            }

            // ibv_cq_ex can be cast to ibv_cq (first fields are identical)
            let cq = cq_ex as *mut mlx5_sys::ibv_cq;
            Ok(CompletionQueue {
                cq: NonNull::new(cq).unwrap(),
                state: None,
                queues: HashMap::new(),
            })
        }
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_cq(self.cq.as_ptr());
        }
    }
}

impl CompletionQueue {
    /// Get the raw ibv_cq pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_cq {
        self.cq.as_ptr()
    }

    /// Initialize direct access for CQE polling.
    ///
    /// This is called automatically when a QP is registered.
    /// Can also be called manually if using `poll_one()` without registered QPs.
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        if self.state.is_some() {
            return Ok(());
        }

        unsafe {
            let mut dv_cq: MaybeUninit<mlx5_sys::mlx5dv_cq> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).cq.in_ = self.cq.as_ptr();
            (*obj_ptr).cq.out = dv_cq.as_mut_ptr();

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_CQ as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_cq = dv_cq.assume_init();

            self.state = Some(CqState {
                buf: dv_cq.buf as *mut u8,
                cqe_cnt: dv_cq.cqe_cnt,
                cqe_size: dv_cq.cqe_size,
                dbrec: dv_cq.dbrec as *mut u32,
                ci: 0,
            });

            Ok(())
        }
    }

    /// Register a queue for completion dispatch.
    ///
    /// Called automatically when a QP is created with this CQ.
    pub(crate) fn register_queue(&mut self, qpn: u32, queue: Weak<RefCell<dyn CompletionTarget>>) {
        self.queues.insert(qpn, queue);
    }

    /// Unregister a queue.
    ///
    /// Called automatically when a QP is dropped.
    pub(crate) fn unregister_queue(&mut self, qpn: u32) {
        self.queues.remove(&qpn);
    }

    /// Poll for completions and dispatch to registered queues.
    ///
    /// Returns the number of completions processed.
    pub fn poll(&mut self) -> usize {
        let mut count = 0;
        while let Some(cqe) = self.try_next_cqe() {
            if let Some(queue) = self.queues.get(&cqe.qp_num).and_then(Weak::upgrade) {
                queue.borrow_mut().dispatch_cqe(cqe);
            }
            count += 1;
        }
        count
    }

    /// Update the CQ doorbell record.
    ///
    /// Call this after processing completions to acknowledge them to the hardware.
    pub fn flush(&self) {
        if let Some(state) = &self.state {
            mmio_flush_writes!();
            unsafe {
                std::ptr::write_volatile(state.dbrec, (state.ci & 0x00FF_FFFF).to_be());
            }
        }
    }

    /// Poll for a single CQE without dispatching to callbacks.
    ///
    /// This is useful for testing or when you want to process CQEs manually.
    /// Returns None if no CQE is available.
    ///
    /// Note: When using this method, you should call `flush()` afterwards
    /// to acknowledge the completions to the hardware.
    pub fn poll_one(&mut self) -> Option<Cqe> {
        self.try_next_cqe()
    }

    /// Try to get the next CQE.
    ///
    /// Returns None if no CQE is available.
    fn try_next_cqe(&mut self) -> Option<Cqe> {
        let state = self.state.as_mut()?;

        let cqe_mask = state.cqe_cnt - 1;
        let idx = state.ci & cqe_mask;
        let cqe_ptr = unsafe { state.buf.add((idx as usize) * (state.cqe_size as usize)) };

        // Owner bit check
        let op_own = unsafe { std::ptr::read_volatile(cqe_ptr.add(63)) };
        let sw_owner = ((state.ci >> state.cqe_cnt.trailing_zeros()) & 1) as u8;
        let hw_owner = op_own & 1;

        // Check owner bit and invalid opcode
        if sw_owner != hw_owner || (op_own >> 4) == 0x0f {
            return None;
        }

        // After validating ownership, we need a load barrier to ensure subsequent
        // reads (including the receive buffer data) see the data written by hardware.
        // On x86, this is a compiler barrier only (TSO guarantees load-load ordering).
        // On ARM, this requires an explicit dmb ld instruction.
        udma_from_device_barrier!();

        let cqe = unsafe { Cqe::from_ptr(cqe_ptr) };
        state.ci = state.ci.wrapping_add(1);

        Some(cqe)
    }
}
