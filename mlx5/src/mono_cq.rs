//! Monomorphic Completion Queue (MonoCq) for inlined callback dispatch.
//!
//! This module provides a generic CQ that eliminates vtable overhead by
//! statically knowing the queue type and callback type at compile time.
//!
//! # Performance Benefits
//!
//! The standard `CompletionQueue::poll()` uses dynamic dispatch (`dyn CompletionTarget`)
//! which prevents inlining of the callback. `MonoCq<Q, F>` keeps type information,
//! enabling the compiler to inline both `process_cqe` and the callback.
//!
//! # Usage
//!
//! ```ignore
//! // Create MonoCq with callback on the CQ side
//! let mono_cq = ctx.create_mono_cq(256, |cqe, entry| {
//!     // This callback is inlined!
//!     println!("Completed: {:?}", entry);
//! })?;
//!
//! // Create QP without callback (callback is on CQ)
//! let qp = ctx.create_rc_qp_for_mono_cq(&pd, &mono_cq, &recv_cq, &config)?;
//!
//! // Register the QP
//! mono_cq.register(&qp);
//!
//! // Poll with inlined dispatch
//! mono_cq.poll();
//! ```

use std::cell::{Cell, RefCell, UnsafeCell};
use std::ptr::NonNull;
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit};

use crate::cq::{Cqe, CqeOpcode, MiniCqeIterator};
use crate::device::Context;

// =============================================================================
// CompletionSource Trait
// =============================================================================

/// Trait for queues that can process CQEs and return entries.
///
/// Unlike `CompletionTarget` which handles its own callback internally,
/// `CompletionSource` returns the entry to the caller, allowing the
/// callback to be stored on the CQ side for inlining.
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
    /// Note: Currently disabled due to hang issues.
    cqe_compression: bool,
}

// =============================================================================
// MonoCq
// =============================================================================

/// Direct queue lookup vector using QPN offset.
/// QPNs are monotonically increasing, so we use base_qpn + offset mapping.
type MonoQueueVec<Q> = Vec<Option<Weak<RefCell<Q>>>>;

/// Monomorphic Completion Queue with inlined callback dispatch.
///
/// Unlike `CompletionQueue` which uses `dyn CompletionTarget` for dynamic dispatch,
/// `MonoCq<Q, F>` keeps the queue type `Q` and callback type `F` as generics,
/// enabling the compiler to inline `process_cqe` and the callback.
///
/// # Type Parameters
///
/// - `Q`: Queue type implementing `CompletionSource`
/// - `F`: Callback type `Fn(Cqe, Q::Entry)`
///
/// # Constraints
///
/// All registered queues must be of the same type `Q`. For heterogeneous
/// queues, use the standard `CompletionQueue` instead.
pub struct MonoCq<Q, F>
where
    Q: CompletionSource,
    F: Fn(Cqe, Q::Entry),
{
    cq: NonNull<mlx5_sys::ibv_cq>,
    state: MonoCqState,
    callback: F,
    /// Registered queues for completion dispatch.
    /// Uses direct indexing: queues[qpn - base_qpn]
    queues: RefCell<MonoQueueVec<Q>>,
    /// Base QPN for offset calculation (first registered QP).
    /// QPNs are monotonically increasing machine-wide.
    base_qpn: Cell<u32>,
    /// Keep the context alive while this CQ exists.
    _ctx: Context,
}

impl<Q, F> Drop for MonoCq<Q, F>
where
    Q: CompletionSource,
    F: Fn(Cqe, Q::Entry),
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
    /// The callback is stored on the CQ and called for each completion,
    /// enabling the compiler to inline the callback code.
    ///
    /// # Arguments
    /// * `cqe` - Minimum number of CQ entries (actual may be larger)
    /// * `callback` - Completion callback `Fn(Cqe, T)` called for each completion
    ///
    /// # Type Parameters
    /// * `Q` - Queue type implementing `CompletionSource`
    /// * `F` - Callback type
    ///
    /// # Errors
    /// Returns an error if the CQ cannot be created.
    pub fn create_mono_cq<Q, F>(&self, cqe: i32, callback: F) -> io::Result<MonoCq<Q, F>>
    where
        Q: CompletionSource,
        F: Fn(Cqe, Q::Entry),
    {
        self.create_mono_cq_internal(cqe, callback, false)
    }

    /// Create a Monomorphic Completion Queue for RX (receive) with CQE compression.
    ///
    /// CQE compression is only supported for RX (responder side) completions.
    /// Using this for TX CQs will cause undefined behavior.
    ///
    /// When compression is enabled:
    /// - Owner checking uses signature field (offset 62) with 0xff mask
    /// - Compressed CQEs (format=3) are expanded via MiniCqeIterator
    /// - Title CQE must precede compressed CQE (asserts cq_ci > 0)
    ///
    /// # Arguments
    /// * `cqe` - Minimum number of CQ entries (actual may be larger)
    /// * `callback` - Completion callback `Fn(Cqe, T)` called for each completion
    ///
    /// # Errors
    /// Returns an error if the CQ cannot be created or compression is not supported.
    pub fn create_mono_cq_rx_compressed<Q, F>(
        &self,
        cqe: i32,
        callback: F,
    ) -> io::Result<MonoCq<Q, F>>
    where
        Q: CompletionSource,
        F: Fn(Cqe, Q::Entry),
    {
        self.create_mono_cq_internal(cqe, callback, true)
    }

    /// Internal function to create MonoCq with optional compression.
    fn create_mono_cq_internal<Q, F>(
        &self,
        cqe: i32,
        callback: F,
        enable_compression: bool,
    ) -> io::Result<MonoCq<Q, F>>
    where
        Q: CompletionSource,
        F: Fn(Cqe, Q::Entry),
    {
        unsafe {
            let mut attr: mlx5_sys::ibv_cq_init_attr_ex = MaybeUninit::zeroed().assume_init();
            attr.cqe = cqe as u32;
            attr.cq_context = std::ptr::null_mut();
            attr.channel = std::ptr::null_mut();
            attr.comp_vector = 0;
            attr.wc_flags = 0;
            attr.comp_mask = 0;
            attr.flags = 0;

            let (cq_ex, cqe_compression) = if enable_compression {
                // Try to create CQ with CQE compression enabled (HASH format for RX)
                // Note: CQE compression formats (HASH, CSUM, etc.) are responder-side only.
                // DO NOT use this for TX (send) CQs - it will cause hangs.
                let mut mlx5_attr: mlx5_sys::mlx5dv_cq_init_attr =
                    MaybeUninit::zeroed().assume_init();
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_cq_init_attr_mask_MLX5DV_CQ_INIT_ATTR_MASK_COMPRESSED_CQE
                        as u64;
                mlx5_attr.cqe_comp_res_format =
                    mlx5_sys::mlx5dv_cqe_comp_res_format_MLX5DV_CQE_RES_FORMAT_HASH as u8;

                let cq_ex = mlx5_sys::mlx5dv_create_cq(self.as_ptr(), &mut attr, &mut mlx5_attr);

                if cq_ex.is_null() {
                    // Fallback to standard CQ if compression not supported
                    (
                        mlx5_sys::ibv_create_cq_ex_ex(self.as_ptr(), &mut attr),
                        false,
                    )
                } else {
                    (cq_ex, true)
                }
            } else {
                // Standard CQ without compression (for TX or when compression not needed)
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
            for i in 0..dv_cq.cqe_cnt {
                let cqe_ptr = buf.add((i as usize) * (dv_cq.cqe_size as usize));
                let signature_ptr = cqe_ptr.add(62);
                let op_own_ptr = cqe_ptr.add(63);
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
                    dbrec: dv_cq.dbrec as *mut u32,
                    ci: Cell::new(0),
                    pending_mini_cqes: UnsafeCell::new(None),
                    title_opcode: Cell::new(CqeOpcode::Req),
                    cqe_compression,
                },
                callback,
                queues: RefCell::new(Vec::new()),
                base_qpn: Cell::new(u32::MAX),
                _ctx: self.clone(),
            })
        }
    }
}

impl<Q, F> MonoCq<Q, F>
where
    Q: CompletionSource,
    F: Fn(Cqe, Q::Entry),
{
    /// Get the raw ibv_cq pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_cq {
        self.cq.as_ptr()
    }

    /// Register a queue for completion dispatch.
    ///
    /// The queue must implement `CompletionSource` with same entry type.
    pub fn register(&self, qp: &Rc<RefCell<Q>>) {
        let qpn = qp.borrow().qpn();
        let base_qpn = self.base_qpn.get();

        let mut queues = self.queues.borrow_mut();

        let offset = if base_qpn == u32::MAX {
            self.base_qpn.set(qpn);
            0
        } else if qpn >= base_qpn {
            qpn - base_qpn
        } else {
            eprintln!("Warning: QPN {} is less than base QPN {}", qpn, base_qpn);
            0
        } as usize;

        if offset >= queues.len() {
            queues.resize(offset + 1, None);
        }

        queues[offset] = Some(Rc::downgrade(qp));
    }

    /// Unregister a queue.
    pub fn unregister(&self, qpn: u32) {
        let base_qpn = self.base_qpn.get();
        if qpn >= base_qpn {
            let mut queues = self.queues.borrow_mut();
            let offset = (qpn - base_qpn) as usize;
            if offset < queues.len() {
                queues[offset] = None;
            }
        }
    }

    /// Find queue by QPN using direct indexing.
    #[inline]
    fn find_queue(&self, qpn: u32) -> Option<Rc<RefCell<Q>>> {
        let base_qpn = self.base_qpn.get();
        if qpn < base_qpn {
            return None;
        }

        let offset = (qpn - base_qpn) as usize;
        let queues = self.queues.borrow();

        queues
            .get(offset)
            .and_then(|w| w.as_ref().and_then(|weak| weak.upgrade()))
    }

    /// Poll for completions and dispatch to registered queues with inlined callback.
    ///
    /// This is the main performance-critical method. Unlike `CompletionQueue::poll()`,
    /// both `process_cqe` and the callback are statically known and can be inlined.
    ///
    /// Returns the number of completions processed.
    #[inline]
    pub fn poll(&self) -> usize {
        // First CQE: no prefetch
        let Some(cqe) = self.try_next_cqe(false) else {
            return 0;
        };
        if let Some(queue) = self.find_queue(cqe.qp_num) {
            let qp = queue.borrow();
            if let Some(entry) = qp.process_cqe(cqe) {
                (self.callback)(cqe, entry);
            }
        }

        // Subsequent CQEs: prefetch next slot
        let mut count = 1;
        while let Some(cqe) = self.try_next_cqe(true) {
            if let Some(queue) = self.find_queue(cqe.qp_num) {
                let qp = queue.borrow();
                if let Some(entry) = qp.process_cqe(cqe) {
                    (self.callback)(cqe, entry);
                }
            }
            count += 1;
        }
        count
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

    /// Try to get the next CQE.
    ///
    /// This method handles both regular CQEs and compressed CQEs. When a compressed
    /// CQE is encountered, it expands the mini CQE array and returns each mini CQE
    /// as a full Cqe on subsequent calls.
    #[inline]
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
    #[inline]
    fn try_next_cqe_simple(&self, prefetch_next: bool) -> Option<Cqe> {
        let state = &self.state;
        let ci = state.ci.get();
        let cqe_mask = state.cqe_cnt - 1;
        let idx = ci & cqe_mask;
        let cqe_size = state.cqe_size as usize;
        let cqe_ptr = unsafe { state.buf.add((idx as usize) * cqe_size) };

        // Owner bit check using op_own field bit 0
        let op_own = unsafe { std::ptr::read_volatile(cqe_ptr.add(63)) };
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

        let cqe = unsafe { Cqe::from_ptr(cqe_ptr) };
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

        // Owner bit check - different based on CQE format
        // Reference: UCX uct_ib_mlx5_cqe_is_hw_owned() and uct_ib_mlx5_init_cq_common()
        let op_own = unsafe { std::ptr::read_volatile(cqe_ptr.add(63)) };

        // Check for compressed CQE via format bits (bits 3:2)
        // format=3 (0x0c) indicates a compressed CQE block
        // Reference: UCX UCT_IB_MLX5_CQE_FORMAT_MASK in ib_mlx5.h
        const CQE_FORMAT_MASK: u8 = 0x0c;
        let is_compressed_format = (op_own & CQE_FORMAT_MASK) == CQE_FORMAT_MASK;

        let is_hw_owned = if is_compressed_format {
            // Compressed CQE (format=3): use signature field (offset 62) with full 8-bit comparison
            // For compressed CQEs, HW writes iteration count to signature field.
            // signature = iteration_count (0, 1, 2, ... wrapping at cq_size)
            let signature = unsafe { std::ptr::read_volatile(cqe_ptr.add(62)) };
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

        // Check for invalid/reserved opcode
        if CqeOpcode::from_u8(opcode_raw).is_none() {
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

        let cqe = unsafe { Cqe::from_ptr(cqe_ptr) };

        // Save the opcode as title opcode for potential compressed CQE following
        state.title_opcode.set(cqe.opcode);

        state.ci.set(ci.wrapping_add(1));

        Some(cqe)
    }
}

// =============================================================================
// Type Aliases
// =============================================================================

use crate::qp::RcQpForMonoCq;

/// MonoCq for RcQp without callback (callback on CQ side).
///
/// Use with `create_rc_qp_for_mono_cq()` which creates QPs with `()` callback.
pub type MonoCqRc<T, F> = MonoCq<RcQpForMonoCq<T>, F>;
