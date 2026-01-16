//! Completion Queue (CQ) management.
//!
//! A Completion Queue is used to notify the application when work requests
//! have completed. CQs can be shared across multiple Queue Pairs.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::device::Context;

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

    /// Returns true if this is a responder (RQ) completion.
    ///
    /// Responder completions indicate that data was received on the Receive Queue.
    /// This includes RDMA WRITE with immediate, SEND operations, and responder errors.
    pub fn is_responder(&self) -> bool {
        matches!(
            self,
            Self::RespRdmaWriteImm
                | Self::RespSend
                | Self::RespSendImm
                | Self::RespSendInv
                | Self::RespErr
        )
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

        let wqe_counter = u16::from_be(std::ptr::read_volatile(ptr.add(60) as *const u16));

        let qp_num = u32::from_be(std::ptr::read_volatile(ptr.add(56) as *const u32)) & 0x00FF_FFFF;

        let byte_cnt = u32::from_be(std::ptr::read_volatile(ptr.add(44) as *const u32));

        let imm = u32::from_be(std::ptr::read_volatile(ptr.add(36) as *const u32));

        // app_info at offset 50: contains TM tag handle for Tag Matching operations
        let app_info = u16::from_be(std::ptr::read_volatile(ptr.add(50) as *const u16));

        // Syndrome and vendor_err are only valid for error CQEs (opcode 0x0d=ReqErr or 0x0e=RespErr).
        // For success CQEs, offsets 54-55 are part of the timestamp field.
        let (vendor_err, syndrome) = if opcode == CqeOpcode::ReqErr || opcode == CqeOpcode::RespErr
        {
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
    ci: Cell<u32>,
}

// =============================================================================
// Completion Queue
// =============================================================================

/// Queue map type using rapidhash for fast lookups.
type QueueMap =
    HashMap<u32, Weak<RefCell<dyn CompletionTarget>>, BuildHasherDefault<rapidhash::RapidHasher>>;

/// Cached queue lookup result.
type CachedQueue = Option<(u32, Rc<RefCell<dyn CompletionTarget>>)>;

/// Completion Queue.
///
/// Used to receive completion notifications for send and receive operations.
/// Multiple QPs can share the same CQ.
pub struct CompletionQueue {
    cq: NonNull<mlx5_sys::ibv_cq>,
    /// Direct verbs state for CQE polling (always initialized).
    state: CqState,
    /// Registered queues for completion dispatch.
    queues: RefCell<QueueMap>,
    /// Cache for last looked up queue (QPN, Rc) to avoid HashMap lookup.
    last_queue_cache: RefCell<CachedQueue>,
    /// Keep the context alive while this CQ exists.
    _ctx: Context,
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

            let cq_ex = mlx5_sys::ibv_create_cq_ex_ex(self.as_ptr(), &mut attr);
            if cq_ex.is_null() {
                return Err(io::Error::last_os_error());
            }

            // ibv_cq_ex can be cast to ibv_cq (first fields are identical)
            let cq_ptr = cq_ex as *mut mlx5_sys::ibv_cq;

            // Initialize direct verbs access immediately
            let mut dv_cq: MaybeUninit<mlx5_sys::mlx5dv_cq> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).cq.in_ = cq_ptr;
            (*obj_ptr).cq.out = dv_cq.as_mut_ptr();

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_CQ as u64);
            if ret != 0 {
                mlx5_sys::ibv_destroy_cq(cq_ptr);
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_cq = dv_cq.assume_init();

            Ok(CompletionQueue {
                cq: NonNull::new(cq_ptr).unwrap(),
                state: CqState {
                    buf: dv_cq.buf as *mut u8,
                    cqe_cnt: dv_cq.cqe_cnt,
                    cqe_size: dv_cq.cqe_size,
                    dbrec: dv_cq.dbrec as *mut u32,
                    ci: Cell::new(0),
                },
                queues: RefCell::new(HashMap::with_hasher(BuildHasherDefault::default())),
                last_queue_cache: RefCell::new(None),
                _ctx: self.clone(),
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
    /// This is now a no-op since direct access is initialized at CQ creation.
    /// Kept for backward compatibility.
    #[deprecated(note = "Direct access is now initialized at CQ creation")]
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Register a queue for completion dispatch.
    ///
    /// Called automatically when a QP is created with this CQ.
    pub(crate) fn register_queue(&self, qpn: u32, queue: Weak<RefCell<dyn CompletionTarget>>) {
        self.queues.borrow_mut().insert(qpn, queue);
    }

    /// Unregister a queue.
    ///
    /// Called automatically when a QP is dropped.
    pub(crate) fn unregister_queue(&self, qpn: u32) {
        self.queues.borrow_mut().remove(&qpn);
        // Invalidate cache if it was for this QPN
        let mut cache = self.last_queue_cache.borrow_mut();
        if let Some((cached_qpn, _)) = cache.as_ref() {
            if *cached_qpn == qpn {
                *cache = None;
            }
        }
    }

    /// Find queue by QPN.
    #[inline]
    fn find_queue(&self, qpn: u32) -> Option<Rc<RefCell<dyn CompletionTarget>>> {
        // Fast path: check cache
        {
            let cache = self.last_queue_cache.borrow();
            if let Some((cached_qpn, cached_rc)) = cache.as_ref() {
                if *cached_qpn == qpn {
                    return Some(cached_rc.clone());
                }
            }
        }

        // Slow path: HashMap lookup
        let result = self.queues.borrow().get(&qpn).and_then(|w| w.upgrade());

        // Update cache
        if let Some(ref rc) = result {
            *self.last_queue_cache.borrow_mut() = Some((qpn, rc.clone()));
        }

        result
    }

    /// Poll for completions and dispatch to registered queues.
    ///
    /// Returns the number of completions processed.
    #[inline]
    pub fn poll(&self) -> usize {
        // First CQE: no prefetch (don't know if any CQE is ready)
        let Some(cqe) = self.try_next_cqe(false) else {
            return 0;
        };
        if let Some(queue) = self.find_queue(cqe.qp_num) {
            queue.borrow().dispatch_cqe(cqe);
        }

        // Subsequent CQEs: prefetch next slot (batch processing path)
        let mut count = 1;
        while let Some(cqe) = self.try_next_cqe(true) {
            if let Some(queue) = self.find_queue(cqe.qp_num) {
                queue.borrow().dispatch_cqe(cqe);
            }
            count += 1;
        }
        count
    }

    /// Update the CQ doorbell record.
    ///
    /// Call this after processing completions to acknowledge them to the hardware.
    /// Note: No sfence needed here - dbrec is in shared memory polled by NIC via DMA.
    /// The store will be visible to the device eventually (x86 TSO guarantees ordering).
    #[inline]
    pub fn flush(&self) {
        unsafe {
            std::ptr::write_volatile(
                self.state.dbrec,
                (self.state.ci.get() & 0x00FF_FFFF).to_be(),
            );
        }
    }

    /// Try to get the next CQE.
    ///
    /// Returns None if no CQE is available.
    #[inline]
    fn try_next_cqe(&self, prefetch_next: bool) -> Option<Cqe> {
        let state = &self.state;

        let ci = state.ci.get();
        let cqe_mask = state.cqe_cnt - 1;
        let idx = ci & cqe_mask;
        let cqe_size = state.cqe_size as usize;
        let cqe_ptr = unsafe { state.buf.add((idx as usize) * cqe_size) };

        // Owner bit check
        let op_own = unsafe { std::ptr::read_volatile(cqe_ptr.add(63)) };
        let sw_owner = ((ci >> state.cqe_cnt.trailing_zeros()) & 1) as u8;
        let hw_owner = op_own & 1;

        // Check owner bit and invalid opcode
        if sw_owner != hw_owner || (op_own >> 4) == 0x0f {
            return None;
        }

        // Prefetch next CQE slot if requested (for batch processing).
        // This overlaps prefetch with CQE parsing below.
        if prefetch_next {
            let next_idx = (idx + 1) & cqe_mask;
            let next_cqe_ptr = unsafe { state.buf.add((next_idx as usize) * cqe_size) };
            prefetch_for_read!(next_cqe_ptr);
        }

        // After validating ownership, we need a load barrier to ensure subsequent
        // reads (including the receive buffer data) see the data written by hardware.
        // On x86, this is a compiler barrier only (TSO guarantees load-load ordering).
        // On ARM, this requires an explicit dmb ld instruction.
        udma_from_device_barrier!();

        let cqe = unsafe { Cqe::from_ptr(cqe_ptr) };
        state.ci.set(ci.wrapping_add(1));

        Some(cqe)
    }
}
