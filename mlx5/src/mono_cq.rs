//! Monomorphic Completion Queue (MonoCq) for inlined callback dispatch.
//!
//! This module provides a generic CQ that eliminates vtable overhead by
//! statically knowing the queue type at compile time. The callback is passed
//! as an argument to `poll()`, enabling monomorphization without storing a
//! callback in the CQ struct.
//!
//! # Performance Benefits
//!
//! The standard `Cq::poll()` uses dynamic dispatch (`dyn CompletionTarget`)
//! which prevents inlining of the callback. `MonoCq<Q>` keeps type information,
//! enabling the compiler to inline both `process_cqe` and the callback.
//!
//! # Usage
//!
//! ```ignore
//! // Create MonoCq (no callback stored)
//! let mono_cq = ctx.create_mono_cq::<QpType>(256, &CqConfig::default())?;
//!
//! // Create QP using builder with MonoCq — registration is automatic
//! let qp = ctx.rc_qp_builder::<Entry, Entry>(&pd, &config)
//!     .sq_mono_cq(&mono_cq)
//!     .rq_mono_cq(&mono_cq)
//!     .build()?;
//!
//! // Poll with inlined dispatch — callback is monomorphized at call site
//! mono_cq.poll(|cqe, entry| {
//!     println!("Completed: {:?}", entry);
//! });
//! ```

use std::cell::{Cell, RefCell, UnsafeCell};
use std::ptr::NonNull;
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit};

use crate::cq::{
    CqConfig, CqModeration, Cqe, CqeCompressionFormat, CqeOpcode, CqeSize, MiniCqeIterator,
};
use crate::device::Context;
use fastmap::FastMap;

// =============================================================================
// CompletionSource Trait
// =============================================================================

/// Trait for queues that can process CQEs and return entries.
///
/// Unlike `CompletionTarget` which handles its own callback internally,
/// `CompletionSource` returns the entry to the caller, allowing the
/// callback to be stored on the CQ side for inlining.
///
/// # Design
///
/// MonoCq uses a single Entry type and single callback for all completions.
/// If you need different Entry types for SQ and RQ, use separate CQs.
pub trait CompletionSource {
    /// The entry type returned on completion.
    type Entry;

    /// Get the QP number.
    fn qpn(&self) -> u32;

    /// Process a CQE and return the associated entry.
    ///
    /// Returns `Some(entry)` if the CQE was successfully processed,
    /// `None` if no entry was found (e.g., for error CQEs).
    fn process_cqe(&self, cqe: Cqe) -> Option<Self::Entry>;
}

// =============================================================================
// CQ State (duplicated from cq.rs for independence)
// =============================================================================

/// Internal CQ state for direct verbs polling.
struct MonoCqState {
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
    pending_mini_cqes: UnsafeCell<Option<MiniCqeIterator>>,
    /// Title CQE opcode for compressed CQE expansion.
    title_opcode: Cell<CqeOpcode>,
    /// CQE compression enabled flag.
    /// When true, compressed CQEs (format=3) may be returned by HW.
    cqe_compression: bool,
    /// Counter for compressed CQEs detected (for debugging/monitoring).
    compressed_cqe_count: Cell<u64>,
}

// =============================================================================
// MonoCq
// =============================================================================

/// Monomorphic Completion Queue with inlined callback dispatch.
///
/// Unlike `Cq` which uses `dyn CompletionTarget` for dynamic dispatch,
/// `MonoCq<Q>` keeps the queue type `Q` as a generic, enabling the compiler
/// to inline processing. The callback is passed to `poll()` as a closure
/// argument, enabling monomorphization at the call site.
///
/// # Type Parameters
///
/// - `Q`: Queue type implementing `CompletionSource`
///
/// # Design
///
/// MonoCq uses a single Entry type for all completions (SQ and RQ).
/// If you need different Entry types for SQ vs RQ, use separate CQs.
///
/// # Constraints
///
/// All registered queues must be of the same type `Q`. For heterogeneous
/// queues, use the standard `Cq` instead.
pub struct MonoCq<Q>
where
    Q: CompletionSource,
{
    cq: NonNull<mlx5_sys::ibv_cq>,
    state: MonoCqState,
    /// Registered queues for completion dispatch.
    /// Uses FastMap for O(1) lookup without hash computation.
    queues: RefCell<FastMap<Weak<RefCell<Q>>>>,
    /// Keep the context alive while this CQ exists.
    _ctx: Context,
}

impl<Q> Drop for MonoCq<Q>
where
    Q: CompletionSource,
{
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_cq(self.cq.as_ptr());
        }
    }
}

impl Context {
    /// Create a Monomorphic Completion Queue with inlined callback dispatch.
    ///
    /// The callback is passed to `poll()` at the call site, enabling the
    /// compiler to inline the callback code via monomorphization.
    ///
    /// # Arguments
    /// * `cqe` - Minimum number of CQ entries (actual may be larger)
    /// * `config` - CQ configuration options (CQE size, compression, etc.)
    ///
    /// # Type Parameters
    /// * `Q` - Queue type implementing `CompletionSource`
    ///
    /// # CQE Compression
    /// When `config.compression_format` is Some:
    /// - CQE compression is only valid for RX (responder side) completions
    /// - Using compression for TX CQs will cause undefined behavior
    /// - Owner checking uses signature field (offset 62) with 0xff mask
    /// - Compressed CQEs (format=3) are expanded via MiniCqeIterator
    /// - Note: Actual compressed CQEs may only be generated with Strided RQ (MPRQ)
    ///
    /// # Errors
    /// Returns an error if the CQ cannot be created.
    pub fn create_mono_cq<Q>(&self, cqe: i32, config: &CqConfig) -> io::Result<MonoCq<Q>>
    where
        Q: CompletionSource,
    {
        // Check device support for CQE compression if requested
        if config.compression_format.is_some() {
            let mlx5_attr = self.query_mlx5_device()?;
            const CQE_128B_COMP: u64 =
                mlx5_sys::mlx5dv_context_flags_MLX5DV_CONTEXT_FLAGS_CQE_128B_COMP as u64;
            if (mlx5_attr.flags & CQE_128B_COMP) == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "CQE 128B compression not supported by device (MLX5DV_CONTEXT_FLAGS_CQE_128B_COMP)",
                ));
            }
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_cq_init_attr_ex = MaybeUninit::zeroed().assume_init();
            attr.cqe = cqe as u32;
            attr.cq_context = std::ptr::null_mut();
            attr.channel = std::ptr::null_mut();
            attr.comp_vector = 0;
            attr.wc_flags = 0;
            attr.comp_mask = 0;
            attr.flags = 0;

            let compression_format = config.compression_format;
            let use_custom_cqe_size = config.cqe_size != CqeSize::Size64;

            let (cq_ex, cqe_compression) = if compression_format.is_some() || use_custom_cqe_size {
                // Use mlx5dv_create_cq for MLX5-specific features
                let mut mlx5_attr: mlx5_sys::mlx5dv_cq_init_attr =
                    MaybeUninit::zeroed().assume_init();

                // Set CQE size
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_cq_init_attr_mask_MLX5DV_CQ_INIT_ATTR_MASK_CQE_SIZE as u64;
                mlx5_attr.cqe_size = config.cqe_size as u16;

                // Enable compression if requested
                // Note: CQE compression formats (HASH, CSUM, etc.) are responder-side only.
                // DO NOT use this for TX (send) CQs - it will cause hangs.
                if let Some(format) = compression_format {
                    mlx5_attr.comp_mask |=
                        mlx5_sys::mlx5dv_cq_init_attr_mask_MLX5DV_CQ_INIT_ATTR_MASK_COMPRESSED_CQE
                            as u64;
                    mlx5_attr.cqe_comp_res_format = match format {
                        CqeCompressionFormat::Hash => {
                            mlx5_sys::mlx5dv_cqe_comp_res_format_MLX5DV_CQE_RES_FORMAT_HASH as u8
                        }
                        CqeCompressionFormat::Csum => {
                            mlx5_sys::mlx5dv_cqe_comp_res_format_MLX5DV_CQE_RES_FORMAT_CSUM as u8
                        }
                        CqeCompressionFormat::CsumStridx => {
                            mlx5_sys::mlx5dv_cqe_comp_res_format_MLX5DV_CQE_RES_FORMAT_CSUM_STRIDX
                                as u8
                        }
                    };
                }

                let cq_ex = mlx5_sys::mlx5dv_create_cq(self.as_ptr(), &mut attr, &mut mlx5_attr);

                if cq_ex.is_null() {
                    return Err(io::Error::last_os_error());
                }
                (cq_ex, compression_format.is_some())
            } else {
                // Standard CQ without compression or custom CQE size
                (
                    mlx5_sys::ibv_create_cq_ex_ex(self.as_ptr(), &mut attr),
                    false,
                )
            };

            if cq_ex.is_null() {
                return Err(io::Error::last_os_error());
            }

            let cq_ptr = cq_ex as *mut mlx5_sys::ibv_cq;

            // Initialize direct verbs access
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
            // When CQE compression is enabled, ownership is tracked via signature field (offset 62).
            // When disabled, ownership is tracked via op_own field (offset 63, bit 0).
            const OP_OWN_INVALID: u8 = 0xf1; // opcode=INVALID(0xf), owner=1
            const SIGNATURE_INVALID: u8 = 0xff; // Initial signature for compression mode
            let buf = dv_cq.buf as *mut u8;
            let cqe64_offset: usize = if dv_cq.cqe_size == 128 { 64 } else { 0 };
            for i in 0..dv_cq.cqe_cnt {
                let cqe_ptr = buf.add((i as usize) * (dv_cq.cqe_size as usize));
                let cqe64_ptr = cqe_ptr.add(cqe64_offset);
                let signature_ptr = cqe64_ptr.add(62);
                let op_own_ptr = cqe64_ptr.add(63);
                std::ptr::write_volatile(signature_ptr, SIGNATURE_INVALID);
                std::ptr::write_volatile(op_own_ptr, OP_OWN_INVALID);
            }

            Ok(MonoCq {
                cq: NonNull::new(cq_ptr).unwrap(),
                state: MonoCqState {
                    buf: dv_cq.buf as *mut u8,
                    cqe_cnt: dv_cq.cqe_cnt,
                    cqe_cnt_log2: dv_cq.cqe_cnt.trailing_zeros(),
                    cqe_size: dv_cq.cqe_size,
                    dbrec: dv_cq.dbrec,
                    ci: Cell::new(0),
                    pending_mini_cqes: UnsafeCell::new(None),
                    title_opcode: Cell::new(CqeOpcode::Req),
                    cqe_compression,
                    compressed_cqe_count: Cell::new(0),
                },
                queues: RefCell::new(FastMap::new()),
                _ctx: self.clone(),
            })
        }
    }
}

impl<Q> MonoCq<Q>
where
    Q: CompletionSource,
{
    /// Get the raw ibv_cq pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_cq {
        self.cq.as_ptr()
    }

    /// Check if CQE compression is enabled for this CQ.
    pub fn is_compression_enabled(&self) -> bool {
        self.state.cqe_compression
    }

    /// Get the number of compressed CQEs detected.
    ///
    /// This counter is incremented each time a compressed CQE (format=3)
    /// is processed. Useful for debugging and monitoring.
    pub fn compressed_cqe_count(&self) -> u64 {
        self.state.compressed_cqe_count.get()
    }

    /// Register a queue for completion dispatch.
    ///
    /// Called automatically by the QP builder during `build()`.
    /// The queue must implement `CompletionSource` with same entry type.
    pub(crate) fn register(&self, qp: &Rc<RefCell<Q>>) {
        let qpn = qp.borrow().qpn();
        self.queues.borrow_mut().insert(qpn, Rc::downgrade(qp));
    }

    /// Unregister a queue.
    pub fn unregister(&self, qpn: u32) {
        self.queues.borrow_mut().remove(qpn);
    }

    /// Find queue by QPN using direct indexing.
    #[inline(always)]
    fn find_queue(&self, qpn: u32) -> Option<Rc<RefCell<Q>>> {
        self.queues
            .borrow()
            .get(qpn)
            .and_then(|weak| weak.upgrade())
    }

    /// Poll for completions and dispatch to registered queues with inlined callback.
    ///
    /// This is the main performance-critical method. Unlike `Cq::poll()`,
    /// both `process_cqe` and the callback are statically known and can be inlined.
    /// The callback is passed as a closure argument, enabling monomorphization
    /// at the call site without storing it in the struct.
    ///
    /// Returns the number of completions processed.
    #[inline(always)]
    pub fn poll(&self, mut callback: impl FnMut(Cqe, Q::Entry)) -> usize {
        // First CQE: no prefetch
        let Some(cqe) = self.try_next_cqe(false) else {
            return 0;
        };
        self.dispatch_cqe(cqe, &mut callback);

        // Subsequent CQEs: prefetch next slot
        let mut count = 1;
        while let Some(cqe) = self.try_next_cqe(true) {
            self.dispatch_cqe(cqe, &mut callback);
            count += 1;
        }
        count
    }

    /// Dispatch a CQE to the appropriate queue and callback.
    #[inline(always)]
    fn dispatch_cqe(&self, cqe: Cqe, callback: &mut impl FnMut(Cqe, Q::Entry)) {
        if let Some(queue) = self.find_queue(cqe.qp_num) {
            let qp = queue.borrow();
            if let Some(entry) = qp.process_cqe(cqe) {
                callback(cqe, entry);
            }
        }
    }

    /// Update the CQ doorbell record.
    ///
    /// Call this after processing completions to acknowledge them to the hardware.
    #[inline]
    pub fn flush(&self) {
        unsafe {
            std::ptr::write_volatile(
                self.state.dbrec,
                (self.state.ci.get() & 0x00FF_FFFF).to_be(),
            );
        }
    }

    /// Set CQ moderation parameters.
    ///
    /// Moderation reduces interrupt overhead by batching completions.
    /// The CQ will generate an interrupt when either threshold is reached:
    /// - `cq_count` CQEs have been generated, or
    /// - `cq_period` microseconds have elapsed since the first CQE
    ///
    /// # Arguments
    /// * `moderation` - Moderation settings (count and period thresholds)
    ///
    /// # Errors
    /// Returns an error if the moderation settings cannot be applied.
    pub fn set_moderation(&self, moderation: CqModeration) -> io::Result<()> {
        unsafe {
            let mut attr: mlx5_sys::ibv_modify_cq_attr = MaybeUninit::zeroed().assume_init();
            // IBV_CQ_ATTR_MODERATE = 1 (from libibverbs)
            const IBV_CQ_ATTR_MODERATE: u32 = 1;
            attr.attr_mask = IBV_CQ_ATTR_MODERATE;
            attr.moderate.cq_count = moderation.cq_count;
            attr.moderate.cq_period = moderation.cq_period;

            let ret = mlx5_sys::ibv_modify_cq_ex(self.cq.as_ptr(), &mut attr);
            if ret != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    /// Try to get the next CQE.
    ///
    /// This method handles both regular CQEs and compressed CQEs. When a compressed
    /// CQE is encountered, it expands the mini CQE array and returns each mini CQE
    /// as a full Cqe on subsequent calls.
    #[inline(always)]
    fn try_next_cqe(&self, prefetch_next: bool) -> Option<Cqe> {
        let state = &self.state;

        // Fast path: non-compression mode (most common case)
        if !state.cqe_compression {
            return self.try_next_cqe_simple(prefetch_next);
        }

        // Slow path: compression mode enabled
        self.try_next_cqe_compressed(prefetch_next)
    }

    /// Fast path for non-compression mode.
    /// Minimizes overhead for the common case where CQE compression is disabled.
    #[inline(always)]
    fn try_next_cqe_simple(&self, prefetch_next: bool) -> Option<Cqe> {
        let state = &self.state;
        let ci = state.ci.get();
        let cqe_mask = state.cqe_cnt - 1;
        let idx = ci & cqe_mask;
        let cqe_size = state.cqe_size as usize;
        let cqe_ptr = unsafe { state.buf.add((idx as usize) * cqe_size) };

        // For 128-byte CQEs, the CQE64 structure is in the second half (offset 64-127).
        // For 64-byte CQEs, it's at the beginning.
        let cqe64_offset = if cqe_size == 128 { 64 } else { 0 };
        let cqe64_ptr = unsafe { cqe_ptr.add(cqe64_offset) };

        // Owner bit check using op_own field bit 0
        let op_own = unsafe { std::ptr::read_volatile(cqe64_ptr.add(63)) };
        let sw_owner = ((ci >> state.cqe_cnt_log2) & 1) as u8;
        let hw_owner = op_own & 1;

        // Check owner bit and invalid opcode (opcode == 0x0f)
        if sw_owner != hw_owner || (op_own >> 4) == 0x0f {
            return None;
        }

        // Prefetch next CQE slot if requested (for batch processing).
        if prefetch_next {
            let next_idx = (idx + 1) & cqe_mask;
            let next_cqe_ptr = unsafe { state.buf.add((next_idx as usize) * cqe_size) };
            prefetch_for_read!(next_cqe_ptr);
        }

        // Load barrier after validating ownership
        udma_from_device_barrier!();

        let cqe = unsafe { Cqe::from_ptr(cqe64_ptr) };
        state.ci.set(ci.wrapping_add(1));

        Some(cqe)
    }

    /// Slow path for compression mode.
    /// Handles both normal CQEs and compressed CQEs (format=3).
    #[inline]
    fn try_next_cqe_compressed(&self, prefetch_next: bool) -> Option<Cqe> {
        let state = &self.state;

        // First, check if we have pending mini CQEs from a compressed CQE
        // Safety: Single-threaded access guaranteed by Rc (not Send).
        let pending = unsafe { &mut *state.pending_mini_cqes.get() };
        if let Some(iter) = pending {
            if let Some(cqe) = iter.next_mini_cqe() {
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

        // For 128-byte CQEs, the CQE64 structure is in the second half (offset 64-127).
        // For 64-byte CQEs, it's at the beginning.
        let cqe64_offset = if cqe_size == 128 { 64 } else { 0 };
        let cqe64_ptr = unsafe { cqe_ptr.add(cqe64_offset) };

        // Owner bit check - different based on CQE format
        // Reference: UCX uct_ib_mlx5_cqe_is_hw_owned() and uct_ib_mlx5_init_cq_common()
        let op_own = unsafe { std::ptr::read_volatile(cqe64_ptr.add(63)) };

        // Check for compressed CQE via format bits (bits 3:2)
        // format=3 (0x0c) indicates a compressed CQE block
        // Reference: UCX UCT_IB_MLX5_CQE_FORMAT_MASK in ib_mlx5.h
        const CQE_FORMAT_MASK: u8 = 0x0c;
        let is_compressed_format = (op_own & CQE_FORMAT_MASK) == CQE_FORMAT_MASK;

        let is_hw_owned = if is_compressed_format {
            // Compressed CQE (format=3): use signature field (offset 62) with full 8-bit comparison
            // For compressed CQEs, HW writes iteration count to signature field.
            // signature = iteration_count (0, 1, 2, ... wrapping at cq_size)
            let signature = unsafe { std::ptr::read_volatile(cqe64_ptr.add(62)) };
            let sw_it_count = ((ci >> state.cqe_cnt_log2) & 0xff) as u8;
            (sw_it_count ^ signature) != 0
        } else {
            // Normal CQE (format=0): use op_own field bit 0 only
            // Even in compression mode, normal CQEs have checksum in signature (not iteration count).
            let sw_owner = ((ci >> state.cqe_cnt_log2) & 1) as u8;
            let hw_owner = op_own & 1;
            sw_owner != hw_owner
        };

        if is_hw_owned {
            return None;
        }

        let opcode_raw = op_own >> 4;

        if is_compressed_format {
            // Compressed CQE must have a preceding title CQE (ci > 0)
            // This is because title CQE provides qp_num, opcode, and other metadata.
            // If ci == 0, there's no title CQE, which is invalid.
            if ci == 0 {
                // This should not happen with proper RX CQ usage.
                // Log and skip this CQE.
                return None;
            }

            // Increment compressed CQE counter
            state
                .compressed_cqe_count
                .set(state.compressed_cqe_count.get() + 1);

            // After validating ownership, we need a load barrier
            udma_from_device_barrier!();

            // Create mini CQE iterator using the title opcode from previous CQE
            let title_opcode = state.title_opcode.get();
            let mut iter = unsafe { MiniCqeIterator::new(cqe64_ptr, title_opcode) };

            state.ci.set(ci.wrapping_add(1));

            // Get the first mini CQE
            if let Some(cqe) = iter.next_mini_cqe() {
                // Store remaining mini CQEs for subsequent calls
                if iter.has_more() {
                    *pending = Some(iter);
                }
                return Some(cqe);
            }
            // Empty compressed CQE (shouldn't happen, but handle gracefully)
            return None;
        }

        // Check for invalid/reserved opcode
        CqeOpcode::from_u8(opcode_raw)?;

        // Prefetch next CQE slot if requested (for batch processing).
        if prefetch_next {
            let next_idx = (idx + 1) & cqe_mask;
            let next_cqe_ptr = unsafe { state.buf.add((next_idx as usize) * cqe_size) };
            prefetch_for_read!(next_cqe_ptr);
        }

        // Load barrier after validating ownership
        udma_from_device_barrier!();

        let cqe = unsafe { Cqe::from_ptr(cqe64_ptr) };

        // Save the opcode as title opcode for potential compressed CQE following
        state.title_opcode.set(cqe.opcode);

        state.ci.set(ci.wrapping_add(1));

        Some(cqe)
    }
}

// =============================================================================
// Type Aliases
// =============================================================================

use crate::qp::{RcQpForMonoCq, RcQpForMonoCqWithSrq};

/// MonoCq for RcQp (callback passed to poll).
///
/// Use with `create_rc_qp_for_mono_cq()` which creates QPs with `()` callback.
///
/// # Type Parameters
/// - `Entry`: Entry type for completions (same for SQ and RQ)
pub type MonoCqRc<Entry> = MonoCq<RcQpForMonoCq<Entry>>;

/// MonoCq for RcQp with Shared Receive Queue (callback passed to poll).
///
/// Use with `with_srq()` builder method to create QPs that share an SRQ.
///
/// # Type Parameters
/// - `Entry`: Entry type for completions (same for SQ and SRQ)
pub type MonoCqRcWithSrq<Entry> = MonoCq<RcQpForMonoCqWithSrq<Entry>>;
