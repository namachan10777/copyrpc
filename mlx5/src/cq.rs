//! Completion Queue (CQ) management.
//!
//! A Completion Queue is used to notify the application when work requests
//! have completed. CQs can be shared across multiple Queue Pairs.

use std::cell::{Cell, RefCell, UnsafeCell};
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
    /// Inline scatter 32 (data inlined in 32-byte CQE)
    InlineScatter32 = 0x12,
    /// Inline scatter 64 (data inlined in 64-byte CQE)
    InlineScatter64 = 0x13,
}

/// Mini CQE format values (for CQE compression).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiniCqeFormat {
    /// Responder (RQ) mini CQE format: contains wqe_counter, byte_cnt, checksum
    Responder = 0x00,
    /// Requester (SQ) mini CQE format: contains wqe_counter, byte_cnt
    Requester = 0x01,
    /// Enhanced responder format with checksum
    ResponderCsum = 0x02,
    /// L3/L4 hash format (for RSS)
    L3L4Hash = 0x03,
}

/// Mini CQE structure (8 bytes).
///
/// Used in CQE compression mode to pack multiple completions into a single CQE.
/// The exact layout depends on the mini CQE format.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MiniCqe {
    /// WQE counter (index) - computed from base + offset
    pub wqe_counter: u16,
    /// Byte count
    pub byte_cnt: u32,
    /// Checksum (for responder formats)
    pub checksum: u16,
}

impl MiniCqe {
    /// Parse a mini CQE from raw memory pointer (responder format).
    ///
    /// # Safety
    /// The pointer must point to valid 8-byte mini CQE data.
    ///
    /// Responder mini CQE layout (8 bytes):
    /// - offset 0-3: byte_cnt (4B, big-endian)
    /// - offset 4-5: checksum (2B, big-endian)
    /// - offset 6-7: reserved/stride_idx (2B)
    #[inline]
    pub unsafe fn from_ptr_responder(ptr: *const u8, wqe_counter: u16) -> Self {
        let byte_cnt = u32::from_be(std::ptr::read_volatile(ptr as *const u32));
        let checksum = u16::from_be(std::ptr::read_volatile(ptr.add(4) as *const u16));
        Self {
            wqe_counter,
            byte_cnt,
            checksum,
        }
    }

    /// Parse a mini CQE from raw memory pointer (requester format).
    ///
    /// # Safety
    /// The pointer must point to valid 8-byte mini CQE data.
    ///
    /// Requester mini CQE layout (8 bytes):
    /// - offset 0-3: s_wqe_info (4B, contains wqe_counter in [31:16])
    /// - offset 4-7: byte_cnt (4B, big-endian)
    #[inline]
    pub unsafe fn from_ptr_requester(ptr: *const u8) -> Self {
        let s_wqe_info = u32::from_be(std::ptr::read_volatile(ptr as *const u32));
        let wqe_counter = (s_wqe_info >> 16) as u16;
        let byte_cnt = u32::from_be(std::ptr::read_volatile(ptr.add(4) as *const u32));
        Self {
            wqe_counter,
            byte_cnt,
            checksum: 0,
        }
    }
}

/// Maximum number of mini CQEs in a compressed CQE.
/// For 64-byte CQE: (64 - 8) / 8 = 7 mini CQEs + 1 title (header) CQE
pub const MAX_MINI_CQES: usize = 7;

/// Compressed CQE header information.
#[derive(Debug, Clone, Copy)]
pub struct CompressedCqeHeader {
    /// Number of completions in this compressed CQE (1-8)
    pub count: u8,
    /// Mini CQE format
    pub format: MiniCqeFormat,
    /// Base WQE counter for responder mini CQEs
    pub base_wqe_counter: u16,
    /// QP number (from title CQE)
    pub qp_num: u32,
    /// Opcode for all mini CQEs (from title CQE)
    pub opcode: CqeOpcode,
}

impl CompressedCqeHeader {
    /// Parse compressed CQE header from raw memory pointer.
    ///
    /// # Safety
    /// The pointer must point to a valid 64-byte compressed CQE.
    ///
    /// Compressed CQE layout:
    /// - offset 0-55: mini CQE array (up to 7 mini CQEs, 8 bytes each)
    /// - offset 56: sop_drop_qpn (4B) - QP number in [23:0]
    /// - offset 60: wqe_counter (2B) - base counter for responder format
    /// - offset 62: mini_cqe_cnt (1B) - [7:6]=format, [5:2]=count-1, [1:0]=reserved
    /// - offset 63: op_own (1B) - opcode[7:4]=0x0f (compression marker), owner[0]
    #[inline]
    pub unsafe fn from_ptr(ptr: *const u8, title_opcode: CqeOpcode) -> Self {
        let qp_num = u32::from_be(std::ptr::read_volatile(ptr.add(56) as *const u32)) & 0x00FF_FFFF;
        let base_wqe_counter = u16::from_be(std::ptr::read_volatile(ptr.add(60) as *const u16));
        let mini_cqe_cnt = std::ptr::read_volatile(ptr.add(62));

        // format is in bits [7:6]
        let format_val = (mini_cqe_cnt >> 6) & 0x03;
        let format = match format_val {
            0 => MiniCqeFormat::Responder,
            1 => MiniCqeFormat::Requester,
            2 => MiniCqeFormat::ResponderCsum,
            _ => MiniCqeFormat::L3L4Hash,
        };

        // count-1 is in bits [5:2], so count = ((mini_cqe_cnt >> 2) & 0x0f) + 1
        let count = ((mini_cqe_cnt >> 2) & 0x0f) + 1;

        Self {
            count,
            format,
            base_wqe_counter,
            qp_num,
            opcode: title_opcode,
        }
    }
}

/// State for iterating over mini CQEs in a compressed CQE.
#[derive(Debug, Clone)]
pub struct MiniCqeIterator {
    /// Compressed CQE header information
    header: CompressedCqeHeader,
    /// Current index (0-based)
    current: u8,
    /// Pointer to mini CQE array in CQ buffer
    mini_cqe_ptr: *const u8,
}

impl MiniCqeIterator {
    /// Create a new iterator from a compressed CQE.
    ///
    /// # Safety
    /// The pointer must point to a valid compressed CQE.
    pub unsafe fn new(ptr: *const u8, title_opcode: CqeOpcode) -> Self {
        let header = CompressedCqeHeader::from_ptr(ptr, title_opcode);
        Self {
            header,
            current: 0,
            mini_cqe_ptr: ptr, // Mini CQEs start at offset 0
        }
    }

    /// Get the next mini CQE as a full Cqe.
    pub fn next(&mut self) -> Option<Cqe> {
        if self.current >= self.header.count {
            return None;
        }

        let cqe = unsafe {
            let mini_ptr = self.mini_cqe_ptr.add((self.current as usize) * 8);
            let mini = match self.header.format {
                MiniCqeFormat::Responder
                | MiniCqeFormat::ResponderCsum
                | MiniCqeFormat::L3L4Hash => {
                    // For responder format, wqe_counter is computed from base + offset
                    let wqe_counter = self
                        .header
                        .base_wqe_counter
                        .wrapping_add(self.current as u16);
                    MiniCqe::from_ptr_responder(mini_ptr, wqe_counter)
                }
                MiniCqeFormat::Requester => {
                    // For requester format, wqe_counter is in the mini CQE itself
                    MiniCqe::from_ptr_requester(mini_ptr)
                }
            };

            Cqe {
                opcode: self.header.opcode,
                wqe_counter: mini.wqe_counter,
                qp_num: self.header.qp_num,
                byte_cnt: mini.byte_cnt,
                imm: 0,
                syndrome: 0,
                vendor_err: 0,
                app_info: 0,
                inline_data: [0u8; MAX_INLINE_SCATTER_SIZE],
            }
        };

        self.current += 1;
        Some(cqe)
    }

    /// Check if there are more mini CQEs to process.
    pub fn has_more(&self) -> bool {
        self.current < self.header.count
    }
}

impl CqeOpcode {
    pub fn from_u8(v: u8) -> Option<Self> {
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
            0x12 => Some(Self::InlineScatter32),
            0x13 => Some(Self::InlineScatter64),
            _ => None,
        }
    }

    /// Returns true if this is a responder (RQ) completion.
    ///
    /// Responder completions indicate that data was received on the Receive Queue.
    /// This includes RDMA WRITE with immediate, SEND operations, responder errors,
    /// and inline scatter completions.
    pub fn is_responder(&self) -> bool {
        matches!(
            self,
            Self::RespRdmaWriteImm
                | Self::RespSend
                | Self::RespSendImm
                | Self::RespSendInv
                | Self::RespErr
                | Self::InlineScatter32
                | Self::InlineScatter64
        )
    }

    /// Returns true if this CQE contains inline scatter data.
    ///
    /// When scatter-to-CQE is enabled and the received data is small enough,
    /// the hardware places the data directly in the CQE instead of the receive buffer.
    /// Use [`Cqe::inline_data()`] to access the inlined data.
    pub fn is_inline_scatter(&self) -> bool {
        matches!(self, Self::InlineScatter32 | Self::InlineScatter64)
    }
}

/// Maximum inline scatter data size in bytes.
///
/// For 64-byte CQEs, up to 32 bytes can be inlined (offset 0-31).
/// For 32-byte CQEs (not currently supported), up to 16 bytes can be inlined.
pub const MAX_INLINE_SCATTER_SIZE: usize = 32;

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
    /// Inline scatter data buffer.
    /// Valid only when `opcode.is_inline_scatter()` returns true.
    /// The actual data length is `byte_cnt`.
    inline_data: [u8; MAX_INLINE_SCATTER_SIZE],
}

impl Cqe {
    /// Parse CQE from raw memory pointer.
    ///
    /// # Safety
    /// The pointer must point to a valid 64-byte CQE.
    ///
    /// CQE64 layout (success):
    /// - offset 0-31: inline scatter data (for InlineScatter32/64 opcodes)
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

        // Copy inline scatter data for InlineScatter32/64 opcodes.
        // For 64-byte CQEs, inline data is at offset 0-31 (up to 32 bytes).
        let mut inline_data = [0u8; MAX_INLINE_SCATTER_SIZE];
        if opcode.is_inline_scatter() {
            let copy_len = (byte_cnt as usize).min(MAX_INLINE_SCATTER_SIZE);
            std::ptr::copy_nonoverlapping(ptr, inline_data.as_mut_ptr(), copy_len);
        }

        Self {
            opcode,
            wqe_counter,
            qp_num,
            byte_cnt,
            imm,
            syndrome,
            vendor_err,
            app_info,
            inline_data,
        }
    }

    /// Get the inline scatter data if present.
    ///
    /// Returns `Some(&[u8])` if the CQE contains inline scatter data
    /// (opcode is `InlineScatter32` or `InlineScatter64`), `None` otherwise.
    ///
    /// The returned slice length is determined by `byte_cnt`, capped at
    /// [`MAX_INLINE_SCATTER_SIZE`].
    ///
    /// # Example
    /// ```ignore
    /// if let Some(data) = cqe.inline_data() {
    ///     // Process inline data directly without copying from receive buffer
    ///     process_data(data);
    /// } else {
    ///     // Data is in the receive buffer, not inlined
    ///     process_data(&recv_buffer[..cqe.byte_cnt as usize]);
    /// }
    /// ```
    pub fn inline_data(&self) -> Option<&[u8]> {
        if self.opcode.is_inline_scatter() {
            let len = (self.byte_cnt as usize).min(MAX_INLINE_SCATTER_SIZE);
            Some(&self.inline_data[..len])
        } else {
            None
        }
    }
}

impl Default for Cqe {
    fn default() -> Self {
        Self {
            opcode: CqeOpcode::Req,
            wqe_counter: 0,
            qp_num: 0,
            byte_cnt: 0,
            imm: 0,
            syndrome: 0,
            vendor_err: 0,
            app_info: 0,
            inline_data: [0u8; MAX_INLINE_SCATTER_SIZE],
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
    /// log2(cqe_cnt) - pre-computed for owner bit calculation
    cqe_cnt_log2: u32,
    /// CQE size in bytes (64 or 128)
    cqe_size: u32,
    /// Doorbell record pointer
    dbrec: *mut u32,
    /// Consumer index
    ci: Cell<u32>,
    /// Pending mini CQE iterator for compressed CQE expansion.
    /// Uses UnsafeCell for interior mutability without RefCell overhead.
    pending_mini_cqes: UnsafeCell<Option<MiniCqeIterator>>,
    /// Title CQE opcode for compressed CQE expansion.
    /// Read from the preceding "title" CQE that indicates the opcode for all mini CQEs.
    title_opcode: Cell<CqeOpcode>,
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
    ///
    /// Uses UnsafeCell instead of RefCell to avoid borrow checking overhead in the hot path.
    /// Safety: Single-threaded access is guaranteed by Rc (not Send), and there are no
    /// recursive calls through dispatch_cqe that would access this cache.
    last_queue_cache: UnsafeCell<CachedQueue>,
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

            // Initialize all CQEs to look like they are in HW ownership (UCX-style).
            // op_own byte layout: opcode[7:4] | reserved[3:1] | owner[0]
            // Set opcode = 0xf (INVALID) and owner = 1.
            // When CI = 0, sw_owner = 0, so CQEs with owner = 1 will be skipped.
            // This prevents reading garbage before HW writes valid CQEs.
            const OP_OWN_INVALID: u8 = 0xf1; // opcode=INVALID(0xf), owner=1
            let buf = dv_cq.buf as *mut u8;
            for i in 0..dv_cq.cqe_cnt {
                let cqe_ptr = buf.add((i as usize) * (dv_cq.cqe_size as usize));
                let op_own_ptr = cqe_ptr.add(63);
                std::ptr::write_volatile(op_own_ptr, OP_OWN_INVALID);
            }

            Ok(CompletionQueue {
                cq: NonNull::new(cq_ptr).unwrap(),
                state: CqState {
                    buf: dv_cq.buf as *mut u8,
                    cqe_cnt: dv_cq.cqe_cnt,
                    cqe_cnt_log2: dv_cq.cqe_cnt.trailing_zeros(),
                    cqe_size: dv_cq.cqe_size,
                    dbrec: dv_cq.dbrec as *mut u32,
                    ci: Cell::new(0),
                    pending_mini_cqes: UnsafeCell::new(None),
                    title_opcode: Cell::new(CqeOpcode::Req),
                },
                queues: RefCell::new(HashMap::with_hasher(BuildHasherDefault::default())),
                last_queue_cache: UnsafeCell::new(None),
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
        // Safety: Single-threaded access guaranteed by Rc (not Send).
        // unregister_queue is only called during QP drop, not from dispatch_cqe.
        let cache = unsafe { &mut *self.last_queue_cache.get() };
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
        // Safety: Single-threaded access guaranteed by Rc (not Send).
        // No recursive calls through dispatch_cqe access this cache.
        // The scope block ensures the immutable reference is dropped before
        // we create a mutable reference below.
        {
            let cache = unsafe { &*self.last_queue_cache.get() };
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
            // Safety: The immutable reference from the cache check above has been
            // dropped (scope ended), so we can safely create a mutable reference.
            unsafe {
                *self.last_queue_cache.get() = Some((qpn, rc.clone()));
            }
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
    ///
    /// This method handles both regular CQEs and compressed CQEs. When a compressed
    /// CQE is encountered, it expands the mini CQE array and returns each mini CQE
    /// as a full Cqe on subsequent calls.
    #[inline]
    fn try_next_cqe(&self, prefetch_next: bool) -> Option<Cqe> {
        let state = &self.state;

        // First, check if we have pending mini CQEs from a compressed CQE
        // Safety: Single-threaded access guaranteed by Rc (not Send).
        let pending = unsafe { &mut *state.pending_mini_cqes.get() };
        if let Some(iter) = pending {
            if let Some(cqe) = iter.next() {
                // If no more mini CQEs, clear the pending state
                if !iter.has_more() {
                    *pending = None;
                }
                return Some(cqe);
            }
            *pending = None;
        }

        let ci = state.ci.get();
        let cqe_mask = state.cqe_cnt - 1;
        let idx = ci & cqe_mask;
        let cqe_size = state.cqe_size as usize;
        let cqe_ptr = unsafe { state.buf.add((idx as usize) * cqe_size) };

        // Owner bit check
        let op_own = unsafe { std::ptr::read_volatile(cqe_ptr.add(63)) };
        let sw_owner = ((ci >> state.cqe_cnt_log2) & 1) as u8;
        let hw_owner = op_own & 1;

        // Check owner bit first
        if sw_owner != hw_owner {
            return None;
        }

        let opcode_raw = op_own >> 4;

        // Check for compressed CQE (opcode 0x0f is the compression marker)
        if opcode_raw == 0x0f {
            // After validating ownership, we need a load barrier
            udma_from_device_barrier!();

            // Create mini CQE iterator using the title opcode from previous CQE
            let title_opcode = state.title_opcode.get();
            let mut iter = unsafe { MiniCqeIterator::new(cqe_ptr, title_opcode) };

            state.ci.set(ci.wrapping_add(1));

            // Get the first mini CQE
            if let Some(cqe) = iter.next() {
                // Store remaining mini CQEs for subsequent calls
                if iter.has_more() {
                    *pending = Some(iter);
                }
                return Some(cqe);
            }
            // Empty compressed CQE (shouldn't happen, but handle gracefully)
            return None;
        }

        // Check for invalid/reserved opcode (0x0f was already handled as compression)
        // Other invalid opcodes should be treated as errors
        if CqeOpcode::from_u8(opcode_raw).is_none() {
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

        // Save the opcode as title opcode for potential compressed CQE following
        state.title_opcode.set(cqe.opcode);

        state.ci.set(ci.wrapping_add(1));

        Some(cqe)
    }
}
