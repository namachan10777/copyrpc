//! Macro-based WQE emission for ILP-maximized direct buffer writes.
//!
//! This module provides the `emit_wqe!` macro for one-shot WQE construction,
//! eliminating &mut state updates for maximum instruction-level parallelism.

use std::cell::Cell;

use crate::types::GrhAttr;
use crate::wqe::{
    ADDRESS_VECTOR_SIZE, ATOMIC_SEG_SIZE, CTRL_SEG_SIZE, CtrlSegParams, DATA_SEG_SIZE,
    OrderedWqeTable, RDMA_SEG_SIZE, SubmissionError, WQEBB_SIZE, WqeFlags, WqeOpcode,
    calc_wqebb_cnt, copy_inline_data, inline_padded_size, write_address_vector_ib,
    write_atomic_seg_cas, write_atomic_seg_fa, write_ctrl_seg, write_data_seg, write_inline_header,
    write_rdma_seg,
};

// =============================================================================
// Parameter Structs for WQE Emission
// =============================================================================

/// Scatter/Gather Entry parameters for local memory access.
#[derive(Debug, Clone, Copy)]
pub struct SgeParams {
    /// Local buffer address.
    pub addr: u64,
    /// Data length in bytes.
    pub len: u32,
    /// Local memory region key.
    pub lkey: u32,
}

/// RDMA remote memory parameters.
#[derive(Debug, Clone, Copy)]
pub struct RdmaParams {
    /// Remote buffer address.
    pub remote_addr: u64,
    /// Remote memory region key.
    pub rkey: u32,
}

// =============================================================================
// SqState Trait - Core trait for direct QP access
// =============================================================================

/// Core trait providing access to Send Queue state.
///
/// Implemented directly on QP types to allow the `emit_wqe!` macro
/// to access SQ state without intermediate context structs.
pub trait SqState {
    /// The entry type stored in the WQE table.
    type Entry;

    /// Get the SQ buffer base address.
    fn sq_buf(&self) -> *mut u8;

    /// Get the number of WQEBBs (must be power of 2).
    fn wqe_cnt(&self) -> u16;

    /// Get the SQ number (QPN for RC/UD, SQN for DCI).
    fn sqn(&self) -> u32;

    /// Get reference to the producer index cell.
    fn pi(&self) -> &Cell<u16>;

    /// Get reference to the consumer index cell.
    fn ci(&self) -> &Cell<u16>;

    /// Get reference to the last WQE pointer and size cell.
    fn last_wqe(&self) -> &Cell<Option<(*mut u8, usize)>>;

    /// Get reference to the WQE table.
    fn table(&self) -> &OrderedWqeTable<Self::Entry>;

    /// Get the doorbell record pointer.
    fn dbrec(&self) -> *mut u32;

    /// Get the BlueFlame register pointer.
    fn bf_reg(&self) -> *mut u8;

    /// Get the BlueFlame buffer size.
    fn bf_size(&self) -> u32;

    /// Get reference to the BlueFlame offset cell.
    fn bf_offset(&self) -> &Cell<u32>;

    // =========================================================================
    // Default implementations
    // =========================================================================

    /// Get the number of available WQEBBs.
    #[inline]
    fn available(&self) -> u16 {
        self.wqe_cnt() - self.pi().get().wrapping_sub(self.ci().get())
    }

    /// Get pointer to WQE at given index.
    #[inline]
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        unsafe {
            self.sq_buf()
                .add(((idx & (self.wqe_cnt() - 1)) as usize) * 64)
        }
    }

    /// Get the number of slots from PI to the end of ring buffer.
    #[inline]
    fn slots_to_end(&self) -> u16 {
        self.wqe_cnt() - (self.pi().get() & (self.wqe_cnt() - 1))
    }

    /// Ring the SQ doorbell to notify HCA of new WQEs.
    #[inline]
    fn ring_sq_doorbell(&self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe().take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec().add(1), (self.pi().get() as u32).to_be());
        }

        udma_to_device_barrier!();

        // Ring doorbell via BlueFlame register
        let bf_offset = self.bf_offset().get();
        let bf = unsafe { self.bf_reg().add(bf_offset as usize) as *mut u64 };
        let ctrl = last_wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset().set(bf_offset ^ self.bf_size());
    }

    /// Advance the producer index.
    #[inline]
    fn advance_pi(&self, count: u16) {
        self.pi().set(self.pi().get().wrapping_add(count));
    }

    /// Set last WQE info for doorbell.
    #[inline]
    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe().set(Some((ptr, size)));
    }

    /// Post a NOP WQE to fill remaining slots.
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots.
    #[inline]
    unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        let wqe_idx = self.pi().get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);
        write_nop_wqe(wqe_ptr, wqe_idx, self.sqn(), nop_wqebb_cnt);
        self.advance_pi(nop_wqebb_cnt);
        self.set_last_wqe(wqe_ptr, (nop_wqebb_cnt as usize) * WQEBB_SIZE);
    }
}

// =============================================================================
// BlueframeBatch - Batched BlueFlame writes
// =============================================================================

/// BlueFlame batch for efficient low-latency WQE submission.
///
/// Accumulates WQEs in an internal buffer (up to 256 bytes) and
/// writes them all at once to the BlueFlame register on `finish()`.
///
/// # Example
/// ```ignore
/// let mut bf = qp.blueflame_batch()?;
/// emit_wqe_bf!(&mut bf, write { ... })?;
/// emit_wqe_bf!(&mut bf, write { ... })?;
/// bf.finish();  // Single BlueFlame write for all WQEs
/// ```
pub struct BlueframeBatch<'a, Q: SqState> {
    qp: &'a Q,
    buffer: [u8; 256],
    offset: usize,
    wqe_count: usize,
}

impl<'a, Q: SqState> BlueframeBatch<'a, Q> {
    /// Create a new BlueFlame batch.
    ///
    /// Returns an error if BlueFlame is not available on this device.
    #[inline]
    pub fn new(qp: &'a Q) -> Result<Self, SubmissionError> {
        if qp.bf_size() == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(Self {
            qp,
            buffer: [0u8; 256],
            offset: 0,
            wqe_count: 0,
        })
    }

    /// Get the buffer pointer for writing WQE data.
    #[inline]
    pub fn buffer_ptr(&mut self) -> *mut u8 {
        self.buffer.as_mut_ptr()
    }

    /// Get the current offset in the buffer.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Set the current offset in the buffer.
    #[inline]
    pub fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    /// Get the SQ number.
    #[inline]
    pub fn sqn(&self) -> u32 {
        self.qp.sqn()
    }

    /// Get the producer index cell.
    #[inline]
    pub fn pi(&self) -> &Cell<u16> {
        self.qp.pi()
    }

    /// Get the WQE table.
    #[inline]
    pub fn table(&self) -> &OrderedWqeTable<Q::Entry> {
        self.qp.table()
    }

    /// Get the number of WQEs in this batch.
    #[inline]
    pub fn wqe_count(&self) -> usize {
        self.wqe_count
    }

    /// Increment the WQE count.
    #[inline]
    pub fn inc_wqe_count(&mut self) {
        self.wqe_count += 1;
    }

    /// Get the remaining capacity in the buffer.
    #[inline]
    pub fn remaining(&self) -> usize {
        256 - self.offset
    }

    /// Get the WQE count in the buffer.
    #[inline]
    pub fn wqe_cnt(&self) -> u16 {
        self.qp.wqe_cnt()
    }

    /// Get the CI for available calculation.
    #[inline]
    pub fn ci(&self) -> &Cell<u16> {
        self.qp.ci()
    }

    /// Finish the batch and write to BlueFlame register.
    ///
    /// This writes all accumulated WQEs to the BlueFlame register in one
    /// operation, providing lower latency than individual doorbell writes.
    pub fn finish(self) {
        if self.wqe_count == 0 {
            return;
        }

        mmio_flush_writes!();

        // Update doorbell record
        unsafe {
            std::ptr::write_volatile(self.qp.dbrec().add(1), (self.qp.pi().get() as u32).to_be());
        }

        udma_to_device_barrier!();

        // Write to BlueFlame register
        let bf_offset = self.qp.bf_offset().get();
        let bf = unsafe { self.qp.bf_reg().add(bf_offset as usize) };

        // Copy the buffer to BlueFlame register
        // Safety: bf_reg is a valid MMIO region, buffer contains valid WQE data
        unsafe {
            mlx5_bf_copy!(bf, self.buffer.as_ptr());
        }

        mmio_flush_writes!();
        self.qp.bf_offset().set(bf_offset ^ self.qp.bf_size());
    }
}

// =============================================================================
// BlueFlame Finish Helpers
// =============================================================================

/// Copy buffer to BlueFlame register and toggle the BF offset.
///
/// This is the common BF copy logic shared across all BlueFlame batch finish methods.
/// Copies `data_len` bytes from `buffer` to the BF register in WQEBB (64B) chunks,
/// then toggles the BF offset for double-buffering.
///
/// # Safety
/// - `bf_reg` must be a valid MMIO BlueFlame register pointer
/// - `buffer` must contain at least `data_len` bytes of valid WQE data
/// - `data_len` must be > 0
#[inline]
pub(crate) unsafe fn bf_copy_and_toggle(
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &Cell<u32>,
    buffer: &[u8],
    data_len: usize,
) {
    if bf_size > 0 {
        let offset = bf_offset.get();
        let bf = bf_reg.add(offset as usize);

        let mut src = buffer.as_ptr();
        let mut dst = bf;
        let mut remaining = data_len;
        while remaining > 0 {
            mlx5_bf_copy!(dst, src);
            src = src.add(WQEBB_SIZE);
            dst = dst.add(WQEBB_SIZE);
            remaining = remaining.saturating_sub(WQEBB_SIZE);
        }

        mmio_flush_writes!();
        bf_offset.set(offset ^ bf_size);
    }
}

/// SQ BlueFlame batch finish: update dbrec[1] with PI, then copy buffer to BF register.
///
/// Common finish logic for all SQ BlueFlame batch types (RC, DCI, UD).
///
/// # Safety
/// - `dbrec` must point to a valid doorbell record (at least 2 u32 entries)
/// - `bf_reg` must be a valid MMIO BlueFlame register pointer
/// - `buffer` must contain at least `data_len` bytes of valid WQE data
#[inline]
#[allow(dead_code)]
pub(crate) unsafe fn bf_finish_sq(
    dbrec: *mut u32,
    pi: u16,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &Cell<u32>,
    buffer: &[u8],
    data_len: usize,
) {
    mmio_flush_writes!();

    std::ptr::write_volatile(dbrec.add(1), (pi as u32).to_be());

    udma_to_device_barrier!();

    bf_copy_and_toggle(bf_reg, bf_size, bf_offset, buffer, data_len);
}

/// RQ BlueFlame batch finish: update dbrec[0] with PI (as u32), then copy buffer to BF register.
///
/// Common finish logic for all RQ BlueFlame batch types.
///
/// # Safety
/// - `dbrec` must point to a valid doorbell record
/// - `bf_reg` must be a valid MMIO BlueFlame register pointer
/// - `buffer` must contain at least `data_len` bytes of valid WQE data
#[inline]
pub(crate) unsafe fn bf_finish_rq(
    dbrec: *mut u32,
    pi: u32,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &Cell<u32>,
    buffer: &[u8],
    data_len: usize,
) {
    mmio_flush_writes!();

    std::ptr::write_volatile(dbrec, pi.to_be());

    udma_to_device_barrier!();

    bf_copy_and_toggle(bf_reg, bf_size, bf_offset, buffer, data_len);
}

// =============================================================================
// SQ Capability Trait
// =============================================================================

// =============================================================================
// Unified Send Queue State
// =============================================================================

use std::marker::PhantomData;

/// Unified Send Queue state shared across all QP types (RC, UD, DCI, TM-SRQ cmd QP).
///
/// Generic over the entry type `Entry` and table type `TableType`.
/// Uses interior mutability (Cell) so no RefCell wrapper is needed.
pub(crate) struct SendQueueState<Entry, TableType> {
    /// SQ buffer base address.
    pub(crate) buf: *mut u8,
    /// Number of WQEBBs (64-byte blocks, power of 2).
    pub(crate) wqe_cnt: u16,
    /// SQ number (QPN for RC/UD, SQN for DCI, QPN for TM cmd QP).
    pub(crate) sqn: u32,
    /// Producer index (next WQE slot).
    pub(crate) pi: Cell<u16>,
    /// Consumer index (last completed WQE).
    pub(crate) ci: Cell<u16>,
    /// Last posted WQE pointer and size (for BlueFlame).
    pub(crate) last_wqe: Cell<Option<(*mut u8, usize)>>,
    /// Doorbell record pointer.
    pub(crate) dbrec: *mut u32,
    /// BlueFlame register pointer.
    pub(crate) bf_reg: *mut u8,
    /// BlueFlame size (64 or 0 if not available).
    pub(crate) bf_size: u32,
    /// Current BlueFlame offset (alternates between 0 and bf_size).
    pub(crate) bf_offset: Cell<u32>,
    /// WQE table for tracking in-flight operations.
    pub(crate) table: TableType,
    /// Phantom for entry type.
    pub(crate) _marker: PhantomData<Entry>,
}

impl<Entry, TableType> SendQueueState<Entry, TableType> {
    /// Get the number of available WQEBBs.
    #[inline]
    pub(crate) fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    /// Ring the doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes (Control Segment) to BlueFlame register.
    /// The NIC fetches remaining WQE data via DMA.
    #[inline]
    pub(crate) fn ring_doorbell(&self) {
        let Some((last_wqe_ptr, _wqe_size)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
    }

    /// Ring the doorbell with BlueFlame write of entire WQE.
    ///
    /// Copies the last WQE (up to bf_size bytes) to the BlueFlame register
    /// using non-temporal (streaming) stores.
    #[inline]
    pub(crate) fn ring_doorbell_bf(&self) {
        let Some((last_wqe_ptr, wqe_size)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        let bf_offset = self.bf_offset.get();
        let mut bf = unsafe { self.bf_reg.add(bf_offset as usize) };
        let mut src = last_wqe_ptr;
        let copy_size = wqe_size.min(self.bf_size as usize);
        let mut remaining = copy_size;

        while remaining > 0 {
            unsafe {
                mlx5_bf_copy!(bf, src);
                src = src.add(WQEBB_SIZE);
                bf = bf.add(WQEBB_SIZE);
            }
            remaining = remaining.saturating_sub(WQEBB_SIZE);
        }

        mmio_flush_writes!();
        self.bf_offset.set(bf_offset ^ self.bf_size);
    }

    /// Write 8-byte doorbell to BF register (minimum BF write).
    #[inline]
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

impl<Entry> SendQueueState<Entry, OrderedWqeTable<Entry>> {
    /// Process a single completion, updating CI from the stored ci_delta.
    #[inline]
    pub(crate) fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        let entry = self.table.take(wqe_idx)?;
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }
}

/// SqState implementation for the unified SendQueueState.
impl<Entry> SqState for SendQueueState<Entry, OrderedWqeTable<Entry>> {
    type Entry = Entry;

    #[inline]
    fn sq_buf(&self) -> *mut u8 {
        self.buf
    }

    #[inline]
    fn wqe_cnt(&self) -> u16 {
        self.wqe_cnt
    }

    #[inline]
    fn sqn(&self) -> u32 {
        self.sqn
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
    fn table(&self) -> &OrderedWqeTable<Entry> {
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
// Internal Helpers for WQE Writing
// =============================================================================

/// Write a NOP WQE to fill slots until ring end.
///
/// # Safety
/// Caller must ensure there are enough available slots and wqe_ptr is valid.
#[inline]
pub(crate) unsafe fn write_nop_wqe(wqe_ptr: *mut u8, wqe_idx: u16, sqn: u32, nop_wqebb_cnt: u16) {
    let ds_count = (nop_wqebb_cnt as u8) * 4;
    write_ctrl_seg(
        wqe_ptr,
        &CtrlSegParams {
            opmod: 0,
            opcode: WqeOpcode::Nop as u8,
            wqe_idx,
            qpn: sqn,
            ds_cnt: ds_count,
            flags: WqeFlags::empty(),
            imm: 0,
        },
    );
}

// =============================================================================
// WQE Emission Context
// =============================================================================

/// Context for WQE emission, providing access to SQ state and buffer.
///
/// This struct holds all the necessary state for emitting WQEs without
/// requiring &mut self on the SQ during construction.
pub struct EmitContext<'a, Entry> {
    /// SQ buffer base address
    pub buf: *mut u8,
    /// Number of WQEBBs (mask = wqe_cnt - 1)
    pub wqe_cnt: u16,
    /// SQ number
    pub sqn: u32,
    /// Producer index (next WQE slot)
    pub pi: &'a Cell<u16>,
    /// Consumer index (last completed WQE)
    pub ci: &'a Cell<u16>,
    /// Last posted WQE pointer and size
    pub last_wqe: &'a Cell<Option<(*mut u8, usize)>>,
    /// WQE table for entry storage
    pub table: &'a OrderedWqeTable<Entry>,
}

impl<'a, Entry> EmitContext<'a, Entry> {
    /// Get the number of available WQEBBs.
    #[inline]
    pub fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    /// Get pointer to WQE at given index.
    #[inline]
    pub fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    /// Get the number of slots from PI to the end of ring buffer.
    #[inline]
    pub fn slots_to_end(&self) -> u16 {
        self.wqe_cnt - (self.pi.get() & (self.wqe_cnt - 1))
    }

    /// Advance the producer index.
    #[inline]
    pub fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    /// Set last WQE info for doorbell.
    #[inline]
    pub fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Post a NOP WQE to fill remaining slots.
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots.
    #[inline]
    pub unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        let wqe_idx = self.pi.get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);
        write_nop_wqe(wqe_ptr, wqe_idx, self.sqn, nop_wqebb_cnt);
        self.advance_pi(nop_wqebb_cnt);
        self.set_last_wqe(wqe_ptr, (nop_wqebb_cnt as usize) * WQEBB_SIZE);
    }

    // SqState-compatible accessors
    #[inline]
    pub fn pi(&self) -> &Cell<u16> {
        self.pi
    }

    #[inline]
    pub fn ci(&self) -> &Cell<u16> {
        self.ci
    }

    #[inline]
    pub fn sqn(&self) -> u32 {
        self.sqn
    }

    #[inline]
    pub fn wqe_cnt(&self) -> u16 {
        self.wqe_cnt
    }

    #[inline]
    pub fn sq_buf(&self) -> *mut u8 {
        self.buf
    }

    #[inline]
    pub fn last_wqe(&self) -> &Cell<Option<(*mut u8, usize)>> {
        self.last_wqe
    }

    #[inline]
    pub fn table(&self) -> &OrderedWqeTable<Entry> {
        self.table
    }
}

impl<'a, Entry> SqState for EmitContext<'a, Entry> {
    type Entry = Entry;

    #[inline]
    fn sq_buf(&self) -> *mut u8 {
        self.buf
    }

    #[inline]
    fn wqe_cnt(&self) -> u16 {
        self.wqe_cnt
    }

    #[inline]
    fn sqn(&self) -> u32 {
        self.sqn
    }

    #[inline]
    fn pi(&self) -> &Cell<u16> {
        self.pi
    }

    #[inline]
    fn ci(&self) -> &Cell<u16> {
        self.ci
    }

    #[inline]
    fn last_wqe(&self) -> &Cell<Option<(*mut u8, usize)>> {
        self.last_wqe
    }

    #[inline]
    fn table(&self) -> &OrderedWqeTable<Self::Entry> {
        self.table
    }

    // BlueFlame fields are not available in EmitContext (legacy API)
    // These methods should not be called when using EmitContext
    fn dbrec(&self) -> *mut u32 {
        panic!("EmitContext does not support BlueFlame doorbell; use QP directly")
    }

    fn bf_reg(&self) -> *mut u8 {
        panic!("EmitContext does not support BlueFlame register; use QP directly")
    }

    fn bf_size(&self) -> u32 {
        panic!("EmitContext does not support BlueFlame; use QP directly")
    }

    fn bf_offset(&self) -> &Cell<u32> {
        panic!("EmitContext does not support BlueFlame; use QP directly")
    }
}

// =============================================================================
// WQE Result Structure
// =============================================================================

/// Result of emitting a WQE.
pub struct EmitResult {
    /// WQE pointer in the buffer
    pub wqe_ptr: *mut u8,
    /// WQE index
    pub wqe_idx: u16,
    /// Total WQE size in bytes
    pub wqe_size: usize,
    /// Number of WQEBBs consumed
    pub wqebb_cnt: u16,
}

// =============================================================================
// SEND WQE Wrap-Around Helper (Cold Path)
// =============================================================================

/// Wrap-around helper for SEND WQE (cold path).
///
/// Posts a NOP to fill remaining slots, then emits SEND at ring beginning.
#[cold]
#[inline]
pub fn emit_send_wrap<Q: SqState>(
    ctx: &Q,
    flags: WqeFlags,
    sge: SgeParams,
    signaled: bool,
    inline_data: Option<&[u8]>,
    imm: u32,
    opcode: WqeOpcode,
) -> Result<EmitResult, SubmissionError> {
    // Calculate WQE size
    let data_size = if let Some(data) = inline_data {
        ((4 + data.len()) + 15) & !15
    } else {
        DATA_SEG_SIZE
    };
    let wqe_size = CTRL_SEG_SIZE + data_size;
    let wqebb_cnt = calc_wqebb_cnt(wqe_size);

    let slots_to_end = ctx.slots_to_end();
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    // Post NOP to fill remaining slots
    unsafe {
        ctx.post_nop(slots_to_end);
    }

    // Now emit at the beginning of the ring
    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    let actual_flags = if signaled {
        flags | WqeFlags::COMPLETION
    } else {
        flags
    };

    let ds_count = (wqe_size / 16) as u8;

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: opcode as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: ds_count,
                flags: actual_flags,
                imm,
            },
        );

        if let Some(data) = inline_data {
            let ptr = wqe_ptr.add(CTRL_SEG_SIZE);
            write_inline_header(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
        } else {
            write_data_seg(wqe_ptr.add(CTRL_SEG_SIZE), sge.len, sge.lkey, sge.addr);
        }
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, wqe_size);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size,
        wqebb_cnt,
    })
}

// =============================================================================
// WRITE WQE Wrap-Around Helper (Cold Path)
// =============================================================================

/// Wrap-around helper for WRITE WQE (cold path).
#[cold]
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn emit_write_wrap<Q: SqState>(
    ctx: &Q,
    flags: WqeFlags,
    rdma: RdmaParams,
    sge: SgeParams,
    signaled: bool,
    inline_data: Option<&[u8]>,
    imm: u32,
    opcode: WqeOpcode,
) -> Result<EmitResult, SubmissionError> {
    // Calculate WQE size
    let data_size = match (inline_data, sge.len > 0) {
        (Some(data), true) => inline_padded_size(data.len()) + DATA_SEG_SIZE,
        (Some(data), false) => inline_padded_size(data.len()),
        (None, _) => DATA_SEG_SIZE,
    };
    let wqe_size = CTRL_SEG_SIZE + RDMA_SEG_SIZE + data_size;
    let wqebb_cnt = calc_wqebb_cnt(wqe_size);

    let slots_to_end = ctx.slots_to_end();
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    // Now emit at the beginning of the ring
    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    let actual_flags = if signaled {
        flags | WqeFlags::COMPLETION
    } else {
        flags
    };

    let ds_count = (wqe_size / 16) as u8;

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: opcode as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: ds_count,
                flags: actual_flags,
                imm,
            },
        );

        write_rdma_seg(wqe_ptr.add(CTRL_SEG_SIZE), rdma.remote_addr, rdma.rkey);

        let data_offset = CTRL_SEG_SIZE + RDMA_SEG_SIZE;
        if let Some(data) = inline_data {
            let ptr = wqe_ptr.add(data_offset);
            write_inline_header(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            if sge.len > 0 {
                let inline_padded = inline_padded_size(data.len());
                write_data_seg(
                    wqe_ptr.add(data_offset + inline_padded),
                    sge.len,
                    sge.lkey,
                    sge.addr,
                );
            }
        } else {
            write_data_seg(wqe_ptr.add(data_offset), sge.len, sge.lkey, sge.addr);
        }
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, wqe_size);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size,
        wqebb_cnt,
    })
}

// =============================================================================
// READ WQE Wrap-Around Helper (Cold Path)
// =============================================================================

/// Wrap-around helper for READ WQE (cold path).
#[cold]
pub fn emit_read_wrap<Q: SqState>(
    ctx: &Q,
    flags: WqeFlags,
    rdma: RdmaParams,
    sge: SgeParams,
    signaled: bool,
) -> Result<EmitResult, SubmissionError> {
    const WQE_SIZE: usize = CTRL_SEG_SIZE + RDMA_SEG_SIZE + DATA_SEG_SIZE;
    let wqebb_cnt = calc_wqebb_cnt(WQE_SIZE);

    let slots_to_end = ctx.slots_to_end();
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    let actual_flags = if signaled {
        flags | WqeFlags::COMPLETION
    } else {
        flags
    };

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: WqeOpcode::RdmaRead as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (WQE_SIZE / 16) as u8,
                flags: actual_flags,
                imm: 0,
            },
        );

        write_rdma_seg(wqe_ptr.add(CTRL_SEG_SIZE), rdma.remote_addr, rdma.rkey);
        write_data_seg(
            wqe_ptr.add(CTRL_SEG_SIZE + RDMA_SEG_SIZE),
            sge.len,
            sge.lkey,
            sge.addr,
        );
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, WQE_SIZE);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size: WQE_SIZE,
        wqebb_cnt,
    })
}

// =============================================================================
// CAS WQE Emission
// =============================================================================

// =============================================================================
// CAS WQE Wrap-Around Helper (Cold Path)
// =============================================================================

/// Wrap-around helper for CAS WQE (cold path).
#[cold]
#[allow(clippy::too_many_arguments)]
pub fn emit_cas_wrap<Q: SqState>(
    ctx: &Q,
    flags: WqeFlags,
    rdma: RdmaParams,
    swap: u64,
    compare: u64,
    sge_addr: u64,
    sge_lkey: u32,
    signaled: bool,
) -> Result<EmitResult, SubmissionError> {
    const WQE_SIZE: usize = CTRL_SEG_SIZE + RDMA_SEG_SIZE + ATOMIC_SEG_SIZE + DATA_SEG_SIZE;
    let wqebb_cnt = calc_wqebb_cnt(WQE_SIZE);

    let slots_to_end = ctx.slots_to_end();
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    let actual_flags = if signaled {
        flags | WqeFlags::COMPLETION
    } else {
        flags
    };

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: WqeOpcode::AtomicCs as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (WQE_SIZE / 16) as u8,
                flags: actual_flags,
                imm: 0,
            },
        );

        write_rdma_seg(wqe_ptr.add(CTRL_SEG_SIZE), rdma.remote_addr, rdma.rkey);
        write_atomic_seg_cas(wqe_ptr.add(CTRL_SEG_SIZE + RDMA_SEG_SIZE), swap, compare);
        write_data_seg(
            wqe_ptr.add(CTRL_SEG_SIZE + RDMA_SEG_SIZE + ATOMIC_SEG_SIZE),
            8, // Atomic result is always 8 bytes
            sge_lkey,
            sge_addr,
        );
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, WQE_SIZE);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size: WQE_SIZE,
        wqebb_cnt,
    })
}

// =============================================================================
// Fetch-and-Add WQE Wrap-Around Helper (Cold Path)
// =============================================================================

/// Wrap-around helper for Fetch-and-Add WQE (cold path).
#[cold]
pub fn emit_fetch_add_wrap<Q: SqState>(
    ctx: &Q,
    flags: WqeFlags,
    rdma: RdmaParams,
    add_value: u64,
    sge_addr: u64,
    sge_lkey: u32,
    signaled: bool,
) -> Result<EmitResult, SubmissionError> {
    const WQE_SIZE: usize = CTRL_SEG_SIZE + RDMA_SEG_SIZE + ATOMIC_SEG_SIZE + DATA_SEG_SIZE;
    let wqebb_cnt = calc_wqebb_cnt(WQE_SIZE);

    let slots_to_end = ctx.slots_to_end();
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    let actual_flags = if signaled {
        flags | WqeFlags::COMPLETION
    } else {
        flags
    };

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: WqeOpcode::AtomicFa as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (WQE_SIZE / 16) as u8,
                flags: actual_flags,
                imm: 0,
            },
        );

        write_rdma_seg(wqe_ptr.add(CTRL_SEG_SIZE), rdma.remote_addr, rdma.rkey);
        write_atomic_seg_fa(wqe_ptr.add(CTRL_SEG_SIZE + RDMA_SEG_SIZE), add_value);
        write_data_seg(
            wqe_ptr.add(CTRL_SEG_SIZE + RDMA_SEG_SIZE + ATOMIC_SEG_SIZE),
            8, // Atomic result is always 8 bytes
            sge_lkey,
            sge_addr,
        );
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, WQE_SIZE);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size: WQE_SIZE,
        wqebb_cnt,
    })
}

// =============================================================================
// emit_wqe! Macro - Direct Expansion Version
// =============================================================================

/// Emit a WQE using the macro-based API with direct buffer writes.
///
/// This macro directly expands to inline WQE construction code, eliminating
/// parameter struct construction and function call overhead for maximum
/// instruction-level parallelism.
///
/// # Syntax
///
/// ```ignore
/// // SEND with SGE
/// emit_wqe!(ctx, send {
///     flags: WqeFlags::FENCE,
///     sge: { addr: local_addr, len: 1024, lkey },
/// })?;
///
/// // SEND with inline data
/// emit_wqe!(ctx, send {
///     flags: WqeFlags::empty(),
///     inline: &payload,
///     signaled: entry,
/// })?;
///
/// // WRITE with SGE
/// emit_wqe!(ctx, write {
///     flags: WqeFlags::empty(),
///     remote_addr: dest,
///     rkey: rkey,
///     sge: { addr: local_addr, len: 64, lkey },
///     signaled: entry,
/// })?;
///
/// // READ
/// emit_wqe!(ctx, read {
///     flags: WqeFlags::FENCE,
///     remote_addr: remote_buf,
///     rkey: rkey,
///     sge: { addr: local, len: 4096, lkey },
///     signaled: entry,
/// })?;
///
/// // CAS
/// emit_wqe!(ctx, cas {
///     flags: WqeFlags::FENCE,
///     remote_addr: atomic_addr,
///     rkey: rkey,
///     swap: new_val,
///     compare: expected,
///     sge: { addr: result_buf, lkey },
///     signaled: entry,
/// })?;
/// ```
#[macro_export]
macro_rules! emit_wqe {
    // =========================================================================
    // SEND with SGE
    // =========================================================================
    ($ctx:expr, send {
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_send_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    false,
                    None,
                    0,
                    $crate::wqe::WqeOpcode::Send,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with SGE (signaled)
    ($ctx:expr, send {
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_send_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    true,
                    None,
                    0,
                    $crate::wqe::WqeOpcode::Send,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with inline
    ($ctx:expr, send {
        flags: $flags:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_send_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    false,
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::Send,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    let data_ptr = wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // SEND with inline (signaled)
    ($ctx:expr, send {
        flags: $flags:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_send_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    true,
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::Send,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    let data_ptr = wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // =========================================================================
    // WRITE with SGE
    // =========================================================================
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    false,
                    None,
                    0,
                    $crate::wqe::WqeOpcode::RdmaWrite,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // WRITE with SGE (signaled)
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    true,
                    None,
                    0,
                    $crate::wqe::WqeOpcode::RdmaWrite,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // WRITE with inline
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    false,
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::RdmaWrite,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let data_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // WRITE with inline (signaled)
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    true,
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::RdmaWrite,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let data_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // =========================================================================
    // WRITE_IMM with SGE
    // =========================================================================
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    false,
                    None,
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // WRITE_IMM with SGE (signaled)
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    true,
                    None,
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // WRITE_IMM with inline
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    false,
                    Some(data),
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags: $flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let data_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // WRITE_IMM with inline (signaled)
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    true,
                    Some(data),
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi().get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let data_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi().get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // WRITE_IMM with inline + SGE
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        inline: $data:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + inline_size
            + $crate::wqe::DATA_SEG_SIZE;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    false,
                    Some(data),
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags: $flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let data_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                    $crate::wqe::write_data_seg(data_ptr.add(inline_size), $len, $lkey, $addr);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // WRITE_IMM with inline + SGE (signaled)
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        inline: $data:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let inline_size = $crate::wqe::inline_padded_size(data.len());
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + inline_size
            + $crate::wqe::DATA_SEG_SIZE;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_write_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    true,
                    Some(data),
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi().get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let data_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                    $crate::wqe::write_inline_header(data_ptr, data.len() as u32);
                    $crate::wqe::copy_inline_data(data_ptr.add(4), data.as_ptr(), data.len());
                    $crate::wqe::write_data_seg(data_ptr.add(inline_size), $len, $lkey, $addr);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi().get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // =========================================================================
    // READ
    // =========================================================================
    ($ctx:expr, read {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_read_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    false,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaRead as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // READ (signaled)
    ($ctx:expr, read {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_read_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    true,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaRead as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // =========================================================================
    // CAS
    // =========================================================================
    ($ctx:expr, cas {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        swap: $swap:expr,
        compare: $compare:expr,
        sge: { addr: $addr:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + $crate::wqe::ATOMIC_SEG_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_cas_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $swap,
                    $compare,
                    $addr,
                    $lkey,
                    false,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::AtomicCs as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_atomic_seg_cas(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $swap,
                        $compare,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add(
                            $crate::wqe::CTRL_SEG_SIZE
                                + $crate::wqe::RDMA_SEG_SIZE
                                + $crate::wqe::ATOMIC_SEG_SIZE,
                        ),
                        8,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // CAS (signaled)
    ($ctx:expr, cas {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        swap: $swap:expr,
        compare: $compare:expr,
        sge: { addr: $addr:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + $crate::wqe::ATOMIC_SEG_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_cas_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $swap,
                    $compare,
                    $addr,
                    $lkey,
                    true,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::AtomicCs as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_atomic_seg_cas(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $swap,
                        $compare,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add(
                            $crate::wqe::CTRL_SEG_SIZE
                                + $crate::wqe::RDMA_SEG_SIZE
                                + $crate::wqe::ATOMIC_SEG_SIZE,
                        ),
                        8,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // =========================================================================
    // Fetch-and-Add
    // =========================================================================
    ($ctx:expr, fetch_add {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        add_value: $add:expr,
        sge: { addr: $addr:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + $crate::wqe::ATOMIC_SEG_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_fetch_add_wrap(
                    ctx,
                    $flags,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $add,
                    $addr,
                    $lkey,
                    false,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::AtomicFa as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_atomic_seg_fa(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $add,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add(
                            $crate::wqe::CTRL_SEG_SIZE
                                + $crate::wqe::RDMA_SEG_SIZE
                                + $crate::wqe::ATOMIC_SEG_SIZE,
                        ),
                        8,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // Fetch-and-Add (signaled)
    ($ctx:expr, fetch_add {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        add_value: $add:expr,
        sge: { addr: $addr:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + $crate::wqe::ATOMIC_SEG_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_fetch_add_wrap(
                    ctx,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $add,
                    $addr,
                    $lkey,
                    true,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::AtomicFa as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_atomic_seg_fa(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                        $add,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add(
                            $crate::wqe::CTRL_SEG_SIZE
                                + $crate::wqe::RDMA_SEG_SIZE
                                + $crate::wqe::ATOMIC_SEG_SIZE,
                        ),
                        8,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // =========================================================================
    // NOP
    // =========================================================================
    ($ctx:expr, nop {
        flags: $flags:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE;
        const WQEBB_CNT: u16 = 1;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe {
                ctx.sq_buf()
                    .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
            };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::Nop as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: 1,
                        flags: $flags,
                        imm: 0,
                    },
                );
            }

            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // NOP (signaled)
    ($ctx:expr, nop {
        flags: $flags:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE;
        const WQEBB_CNT: u16 = 1;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe {
                ctx.sq_buf()
                    .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
            };
            let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::Nop as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: 1,
                        flags,
                        imm: 0,
                    },
                );
            }

            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};
}

pub use emit_wqe;

// =============================================================================
// DCI Address Vector Types
// =============================================================================

/// InfiniBand Address Vector for DCI operations.
#[derive(Debug, Clone, Copy)]
pub struct DcAvIb {
    /// DC key.
    pub dc_key: u64,
    /// DC Target Number.
    pub dctn: u32,
    /// Destination Local Identifier.
    pub dlid: u16,
}

impl DcAvIb {
    /// Create a new InfiniBand DC Address Vector.
    pub fn new(dc_key: u64, dctn: u32, dlid: u16) -> Self {
        Self { dc_key, dctn, dlid }
    }
}

/// RoCE Address Vector for DCI operations.
#[derive(Debug, Clone)]
pub struct DcAvRoCE<'a> {
    /// DC key.
    pub dc_key: u64,
    /// DC Target Number.
    pub dctn: u32,
    /// GRH attributes.
    pub grh: &'a GrhAttr,
}

impl<'a> DcAvRoCE<'a> {
    /// Create a new RoCE DC Address Vector.
    pub fn new(dc_key: u64, dctn: u32, grh: &'a GrhAttr) -> Self {
        Self { dc_key, dctn, grh }
    }
}

/// Type alias for DCI EmitContext (identical to EmitContext).
pub type DciEmitContext<'a, Entry> = EmitContext<'a, Entry>;

// =============================================================================
// DCI WRITE WQE Emission
// =============================================================================
// DCI WQE Wrap-around Helpers (Direct Parameters)
// =============================================================================

/// Cold path: emit DCI WRITE WQE with wrap-around handling.
#[cold]
#[allow(clippy::too_many_arguments)]
pub fn emit_dci_write_wrap<'a, Entry>(
    ctx: &DciEmitContext<'a, Entry>,
    av: DcAvIb,
    flags: WqeFlags,
    rdma: RdmaParams,
    sge: SgeParams,
    inline_data: Option<&[u8]>,
    imm: u32,
    opcode: WqeOpcode,
    wqebb_cnt: u16,
    slots_to_end: u16,
) -> Result<EmitResult, SubmissionError> {
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    // Calculate WQE size again after wrap
    let data_size = if let Some(data) = inline_data {
        inline_padded_size(data.len())
    } else {
        DATA_SEG_SIZE
    };
    let wqe_size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + data_size;
    let wqebb_cnt = calc_wqebb_cnt(wqe_size);

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: opcode as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (wqe_size / 16) as u8,
                flags,
                imm,
            },
        );

        write_address_vector_ib(wqe_ptr.add(CTRL_SEG_SIZE), av.dc_key, av.dctn, av.dlid);

        let rdma_offset = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE;
        write_rdma_seg(wqe_ptr.add(rdma_offset), rdma.remote_addr, rdma.rkey);

        let data_offset = rdma_offset + RDMA_SEG_SIZE;
        if let Some(data) = inline_data {
            let ptr = wqe_ptr.add(data_offset);
            write_inline_header(ptr, data.len() as u32);
            copy_inline_data(ptr.add(4), data.as_ptr(), data.len());
        } else {
            write_data_seg(wqe_ptr.add(data_offset), sge.len, sge.lkey, sge.addr);
        }
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, wqe_size);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size,
        wqebb_cnt,
    })
}

/// Cold path: emit DCI READ WQE with wrap-around handling.
#[cold]
pub fn emit_dci_read_wrap<'a, Entry>(
    ctx: &DciEmitContext<'a, Entry>,
    av: DcAvIb,
    flags: WqeFlags,
    rdma: RdmaParams,
    sge: SgeParams,
    wqebb_cnt: u16,
    slots_to_end: u16,
) -> Result<EmitResult, SubmissionError> {
    const WQE_SIZE: usize = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + DATA_SEG_SIZE;

    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: WqeOpcode::RdmaRead as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (WQE_SIZE / 16) as u8,
                flags,
                imm: 0,
            },
        );

        write_address_vector_ib(wqe_ptr.add(CTRL_SEG_SIZE), av.dc_key, av.dctn, av.dlid);

        let rdma_offset = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE;
        write_rdma_seg(wqe_ptr.add(rdma_offset), rdma.remote_addr, rdma.rkey);
        write_data_seg(
            wqe_ptr.add(rdma_offset + RDMA_SEG_SIZE),
            sge.len,
            sge.lkey,
            sge.addr,
        );
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, WQE_SIZE);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size: WQE_SIZE,
        wqebb_cnt,
    })
}

/// Cold path: emit DCI SEND WQE with wrap-around handling.
#[cold]
#[allow(clippy::too_many_arguments)]
pub fn emit_dci_send_wrap<'a, Entry>(
    ctx: &DciEmitContext<'a, Entry>,
    av: DcAvIb,
    flags: WqeFlags,
    sge: SgeParams,
    inline_data: Option<&[u8]>,
    imm: u32,
    opcode: WqeOpcode,
    wqebb_cnt: u16,
    slots_to_end: u16,
) -> Result<EmitResult, SubmissionError> {
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    // Calculate WQE size again after wrap
    let data_size = if let Some(data) = inline_data {
        inline_padded_size(data.len())
    } else {
        DATA_SEG_SIZE
    };
    let wqe_size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + data_size;
    let wqebb_cnt = calc_wqebb_cnt(wqe_size);

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: opcode as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (wqe_size / 16) as u8,
                flags,
                imm,
            },
        );

        write_address_vector_ib(wqe_ptr.add(CTRL_SEG_SIZE), av.dc_key, av.dctn, av.dlid);

        let data_offset = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE;
        if let Some(data) = inline_data {
            let ptr = wqe_ptr.add(data_offset);
            write_inline_header(ptr, data.len() as u32);
            copy_inline_data(ptr.add(4), data.as_ptr(), data.len());
        } else {
            write_data_seg(wqe_ptr.add(data_offset), sge.len, sge.lkey, sge.addr);
        }
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, wqe_size);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size,
        wqebb_cnt,
    })
}

// =============================================================================
// UD Address Vector Types
// =============================================================================

/// InfiniBand Address Vector for UD operations.
///
/// UD Address Vector specifies the destination QPN, Q_Key, and DLID.
#[derive(Debug, Clone, Copy)]
pub struct UdAvIb {
    /// Remote Queue Pair Number.
    pub remote_qpn: u32,
    /// Queue Key.
    pub qkey: u32,
    /// Destination Local Identifier.
    pub dlid: u16,
}

impl UdAvIb {
    /// Create a new InfiniBand UD Address Vector.
    pub fn new(remote_qpn: u32, qkey: u32, dlid: u16) -> Self {
        Self {
            remote_qpn,
            qkey,
            dlid,
        }
    }
}

/// Write UD address vector to WQE buffer (InfiniBand).
///
/// # Safety
/// The pointer must point to at least 48 bytes of writable memory.
#[inline]
pub unsafe fn write_ud_address_vector_ib(ptr: *mut u8, av: &UdAvIb) {
    // mlx5_wqe_av format for UD (48 bytes):
    // [0-3]:   qkey (big-endian)
    // [4-7]:   reserved
    // [8-11]:  dqp_dct (destination QPN in [23:0] | MLX5_EXTENDED_UD_AV)
    // [12]:    stat_rate_sl (SL in [7:4], static_rate in [3:0])
    // [13]:    fl_mlid (force_lb in [7], source_lid[6:0])
    // [14-15]: rlid (remote LID, big-endian)
    // [16-47]: reserved/GRH fields

    // Q_Key at offset 0
    std::ptr::write_volatile(ptr as *mut u32, av.qkey.to_be());
    // Reserved at offset 4
    std::ptr::write_volatile(ptr.add(4) as *mut u32, 0);

    // Remote QPN at offset 8 with MLX5_EXTENDED_UD_AV flag (bit 31=1)
    const MLX5_EXTENDED_UD_AV: u32 = 0x8000_0000;
    let dqp = (av.remote_qpn & 0x00FF_FFFF) | MLX5_EXTENDED_UD_AV;
    std::ptr::write_volatile(ptr.add(8) as *mut u32, dqp.to_be());

    // stat_rate_sl at offset 12 (SL=0, rate=0)
    std::ptr::write_volatile(ptr.add(12), 0u8);
    // fl_mlid at offset 13 (force_lb=0, mlid=0)
    std::ptr::write_volatile(ptr.add(13), 0u8);

    // Remote LID at offset 14
    std::ptr::write_volatile(ptr.add(14) as *mut u16, av.dlid.to_be());

    // Clear remaining fields (offset 16-47)
    std::ptr::write_bytes(ptr.add(16), 0, 32);
}

/// Type alias for UD EmitContext (identical to EmitContext).
pub type UdEmitContext<'a, Entry> = EmitContext<'a, Entry>;

// =============================================================================
// UD SEND WQE Wrap-around Helper (Direct Parameters)
// =============================================================================

/// Cold path: emit UD SEND WQE with wrap-around handling.
#[cold]
#[allow(clippy::too_many_arguments)]
pub fn emit_ud_send_wrap<'a, Entry>(
    ctx: &UdEmitContext<'a, Entry>,
    av: UdAvIb,
    flags: WqeFlags,
    sge: SgeParams,
    inline_data: Option<&[u8]>,
    imm: u32,
    opcode: WqeOpcode,
    wqebb_cnt: u16,
    slots_to_end: u16,
) -> Result<EmitResult, SubmissionError> {
    let total_needed = slots_to_end + wqebb_cnt;
    if ctx.available() < total_needed {
        return Err(SubmissionError::SqFull);
    }

    unsafe {
        ctx.post_nop(slots_to_end);
    }

    // Calculate WQE size again after wrap
    let data_size = if let Some(data) = inline_data {
        inline_padded_size(data.len())
    } else {
        DATA_SEG_SIZE
    };
    let wqe_size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + data_size;
    let wqebb_cnt = calc_wqebb_cnt(wqe_size);

    let wqe_idx = ctx.pi().get();
    let wqe_ptr = ctx.get_wqe_ptr(wqe_idx);

    unsafe {
        write_ctrl_seg(
            wqe_ptr,
            &CtrlSegParams {
                opmod: 0,
                opcode: opcode as u8,
                wqe_idx,
                qpn: ctx.sqn(),
                ds_cnt: (wqe_size / 16) as u8,
                flags,
                imm,
            },
        );

        write_ud_address_vector_ib(wqe_ptr.add(CTRL_SEG_SIZE), &av);

        let data_offset = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE;
        if let Some(data) = inline_data {
            let ptr = wqe_ptr.add(data_offset);
            write_inline_header(ptr, data.len() as u32);
            copy_inline_data(ptr.add(4), data.as_ptr(), data.len());
        } else {
            write_data_seg(wqe_ptr.add(data_offset), sge.len, sge.lkey, sge.addr);
        }
    }

    ctx.advance_pi(wqebb_cnt);
    ctx.set_last_wqe(wqe_ptr, wqe_size);

    Ok(EmitResult {
        wqe_ptr,
        wqe_idx,
        wqe_size,
        wqebb_cnt,
    })
}

/// Type alias for TM-SRQ Command QP EmitContext (identical to EmitContext).
pub type TmCmdEmitContext<'a, Entry> = EmitContext<'a, Entry>;

// =============================================================================
// emit_tm_wqe! Macro (Direct Expansion Version)
// =============================================================================

/// Emit a TM WQE using the macro-based API.
///
/// This macro directly expands to WQE construction code without intermediate
/// function calls or parameter structs.
///
/// # Syntax
///
/// ```ignore
/// // TAG_ADD (unsignaled)
/// emit_tm_wqe!(ctx, tag_add {
///     index: tag_index,
///     tag: tag_value,
///     sge: { addr: buf_addr, len: buf_len, lkey: lkey },
/// })?;
///
/// // TAG_ADD (signaled)
/// emit_tm_wqe!(ctx, tag_add {
///     index: tag_index,
///     tag: tag_value,
///     sge: { addr: buf_addr, len: buf_len, lkey: lkey },
///     signaled: entry,
/// })?;
///
/// // TAG_DEL (unsignaled)
/// emit_tm_wqe!(ctx, tag_del {
///     index: tag_index,
/// })?;
///
/// // TAG_DEL (signaled)
/// emit_tm_wqe!(ctx, tag_del {
///     index: tag_index,
///     signaled: entry,
/// })?;
/// ```
#[macro_export]
macro_rules! emit_tm_wqe {
    // TAG_ADD (unsignaled)
    ($ctx:expr, tag_add {
        index: $index:expr,
        tag: $tag:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::TM_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe {
                ctx.sq_buf()
                    .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
            };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::TagMatching as u8,
                        wqe_idx,
                        qpn: ctx.sqn,
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $crate::wqe::WqeFlags::COMPLETION,
                        imm: 0,
                    },
                );
                $crate::wqe::write_tm_seg_add(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                    $index,
                    $index, // sw_cnt - same as index
                    $tag,
                    !0u64, // mask: all bits must match
                    false, // signaled
                );
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::TM_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // TAG_ADD (signaled)
    ($ctx:expr, tag_add {
        index: $index:expr,
        tag: $tag:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::TM_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe {
                ctx.sq_buf()
                    .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
            };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::TagMatching as u8,
                        wqe_idx,
                        qpn: ctx.sqn,
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $crate::wqe::WqeFlags::COMPLETION,
                        imm: 0,
                    },
                );
                $crate::wqe::write_tm_seg_add(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                    $index,
                    $index, // sw_cnt - same as index
                    $tag,
                    !0u64, // mask: all bits must match
                    true,  // signaled
                );
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::TM_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // TAG_DEL (unsignaled)
    ($ctx:expr, tag_del {
        index: $index:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::TM_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe {
                ctx.sq_buf()
                    .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
            };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::TagMatching as u8,
                        wqe_idx,
                        qpn: ctx.sqn,
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $crate::wqe::WqeFlags::COMPLETION,
                        imm: 0,
                    },
                );
                $crate::wqe::write_tm_seg_del(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                    $index,
                    false, // signaled
                );
            }

            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // TAG_DEL (signaled)
    ($ctx:expr, tag_del {
        index: $index:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::TM_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe {
                ctx.sq_buf()
                    .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
            };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::TagMatching as u8,
                        wqe_idx,
                        qpn: ctx.sqn,
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $crate::wqe::WqeFlags::COMPLETION,
                        imm: 0,
                    },
                );
                $crate::wqe::write_tm_seg_del(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                    $index,
                    true, // signaled
                );
            }

            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};
}

pub use emit_tm_wqe;

// =============================================================================
// emit_dci_wqe! Macro (Direct Expansion Version)
// =============================================================================

/// Emit a DCI WQE using the macro-based API.
///
/// This macro directly expands to WQE construction code without intermediate
/// function calls or parameter structs.
///
/// # Syntax
///
/// ```ignore
/// // WRITE with SGE (signaled)
/// emit_dci_wqe!(ctx, write {
///     av: av,
///     flags: WqeFlags::empty(),
///     remote_addr: dest,
///     rkey: rkey,
///     sge: { addr: local_addr, len: 64, lkey },
///     signaled: entry,
/// })?;
///
/// // READ with SGE (signaled)
/// emit_dci_wqe!(ctx, read {
///     av: av,
///     flags: WqeFlags::empty(),
///     remote_addr: src,
///     rkey: rkey,
///     sge: { addr: local_addr, len: 64, lkey },
///     signaled: entry,
/// })?;
///
/// // SEND with inline (signaled)
/// emit_dci_wqe!(ctx, send {
///     av: av,
///     flags: WqeFlags::empty(),
///     inline: &payload,
///     signaled: entry,
/// })?;
/// ```
#[macro_export]
macro_rules! emit_dci_wqe {
    // WRITE with SGE (signaled)
    ($ctx:expr, write {
        av: $av:expr,
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_dci_write_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    None,
                    0,
                    $crate::wqe::WqeOpcode::RdmaWrite,
                    WQEBB_CNT,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add(
                            $crate::wqe::CTRL_SEG_SIZE
                                + $crate::wqe::ADDRESS_VECTOR_SIZE
                                + $crate::wqe::RDMA_SEG_SIZE,
                        ),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // WRITE with inline (signaled)
    ($ctx:expr, write {
        av: $av:expr,
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_dci_write_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::RdmaWrite,
                    wqebb_cnt,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let inline_ptr = wqe_ptr.add(
                        $crate::wqe::CTRL_SEG_SIZE
                            + $crate::wqe::ADDRESS_VECTOR_SIZE
                            + $crate::wqe::RDMA_SEG_SIZE,
                    );
                    $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                    $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // WRITE with inline + immediate (signaled)
    ($ctx:expr, write_imm {
        av: $av:expr,
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        inline: $data:expr,
        imm: $imm:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_dci_write_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    Some(data),
                    $imm,
                    $crate::wqe::WqeOpcode::RdmaWriteImm,
                    wqebb_cnt,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: $imm,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $raddr,
                        $rkey,
                    );
                    let inline_ptr = wqe_ptr.add(
                        $crate::wqe::CTRL_SEG_SIZE
                            + $crate::wqe::ADDRESS_VECTOR_SIZE
                            + $crate::wqe::RDMA_SEG_SIZE,
                    );
                    $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                    $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // READ with SGE (signaled)
    ($ctx:expr, read {
        av: $av:expr,
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::RDMA_SEG_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_dci_read_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::RdmaParams {
                        remote_addr: $raddr,
                        rkey: $rkey,
                    },
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    WQEBB_CNT,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::RdmaRead as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    $crate::wqe::write_rdma_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $raddr,
                        $rkey,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add(
                            $crate::wqe::CTRL_SEG_SIZE
                                + $crate::wqe::ADDRESS_VECTOR_SIZE
                                + $crate::wqe::RDMA_SEG_SIZE,
                        ),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with SGE (signaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_dci_send_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    None,
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    WQEBB_CNT,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with inline (signaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_dci_send_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    wqebb_cnt,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    let inline_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE);
                    $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                    $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // SEND with SGE (unsignaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = ((WQE_SIZE + 63) / 64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_dci_send_wrap(
                    ctx,
                    av,
                    $flags,
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    None,
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    WQEBB_CNT,
                    slots_to_end,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with inline (unsignaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_dci_send_wrap(
                    ctx,
                    av,
                    $flags,
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    wqebb_cnt,
                    slots_to_end,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::write_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        av.dc_key,
                        av.dctn,
                        av.dlid,
                    );
                    let inline_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE);
                    $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                    $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};
}

pub use emit_dci_wqe;

// =============================================================================
// emit_ud_wqe! Macro (Direct Expansion Version)
// =============================================================================

/// Emit a UD WQE using the macro-based API.
///
/// This macro directly expands to WQE construction code without intermediate
/// function calls or parameter structs.
///
/// # Syntax
///
/// ```ignore
/// // SEND with SGE (signaled)
/// emit_ud_wqe!(ctx, send {
///     av: av,
///     flags: WqeFlags::empty(),
///     sge: { addr: buf_addr, len: buf_len, lkey },
///     signaled: entry,
/// })?;
///
/// // SEND with inline (signaled)
/// emit_ud_wqe!(ctx, send {
///     av: av,
///     flags: WqeFlags::empty(),
///     inline: &payload,
///     signaled: entry,
/// })?;
/// ```
#[macro_export]
macro_rules! emit_ud_wqe {
    // SEND with SGE (signaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_ud_send_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    None,
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    WQEBB_CNT,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::emit::write_ud_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        &av,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with inline (signaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE + inline_size;
        let wqebb_cnt = wqe_size.div_ceil(64) as u16;

        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                let result = $crate::wqe::emit::emit_ud_send_wrap(
                    ctx,
                    av,
                    $flags | $crate::wqe::WqeFlags::COMPLETION,
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    wqebb_cnt,
                    slots_to_end,
                );
                if let Ok(ref res) = result {
                    ctx.table().store(res.wqe_idx, $entry, ctx.pi.get());
                }
                result
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };
                let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::emit::write_ud_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        &av,
                    );
                    let inline_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE);
                    $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                    $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));
                ctx.table().store(wqe_idx, $entry, ctx.pi.get());

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};

    // SEND with SGE (unsignaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE
            + $crate::wqe::ADDRESS_VECTOR_SIZE
            + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < WQEBB_CNT) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(WQEBB_CNT > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_ud_send_wrap(
                    ctx,
                    av,
                    $flags,
                    $crate::wqe::emit::SgeParams {
                        addr: $addr,
                        len: $len,
                        lkey: $lkey,
                    },
                    None,
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    WQEBB_CNT,
                    slots_to_end,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (WQE_SIZE / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::emit::write_ud_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        &av,
                    );
                    $crate::wqe::write_data_seg(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE),
                        $len,
                        $lkey,
                        $addr,
                    );
                }

                ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
                ctx.last_wqe().set(Some((wqe_ptr, WQE_SIZE)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size: WQE_SIZE,
                    wqebb_cnt: WQEBB_CNT,
                })
            }
        }
    }};

    // SEND with inline (unsignaled)
    ($ctx:expr, send {
        av: $av:expr,
        flags: $flags:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE + inline_size;
        let wqebb_cnt = wqe_size.div_ceil(64) as u16;

        let available = ctx.wqe_cnt() - ctx.pi().get().wrapping_sub(ctx.ci().get());
        let av = $av;

        if $crate::wqe::unlikely(available < wqebb_cnt) {
            Err($crate::wqe::SubmissionError::SqFull)
        } else {
            let slots_to_end = ctx.wqe_cnt() - (ctx.pi().get() & (ctx.wqe_cnt() - 1));
            if $crate::wqe::unlikely(wqebb_cnt > slots_to_end && slots_to_end < ctx.wqe_cnt()) {
                $crate::wqe::emit::emit_ud_send_wrap(
                    ctx,
                    av,
                    $flags,
                    $crate::wqe::emit::SgeParams {
                        addr: 0,
                        len: 0,
                        lkey: 0,
                    },
                    Some(data),
                    0,
                    $crate::wqe::WqeOpcode::Send,
                    wqebb_cnt,
                    slots_to_end,
                )
            } else {
                let wqe_idx = ctx.pi().get();
                let wqe_ptr = unsafe {
                    ctx.sq_buf()
                        .add(((wqe_idx & (ctx.wqe_cnt() - 1)) as usize) * 64)
                };

                unsafe {
                    $crate::wqe::write_ctrl_seg(
                        wqe_ptr,
                        &$crate::wqe::CtrlSegParams {
                            opmod: 0,
                            opcode: $crate::wqe::WqeOpcode::Send as u8,
                            wqe_idx,
                            qpn: ctx.sqn(),
                            ds_cnt: (wqe_size / 16) as u8,
                            flags: $flags,
                            imm: 0,
                        },
                    );
                    $crate::wqe::emit::write_ud_address_vector_ib(
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                        &av,
                    );
                    let inline_ptr =
                        wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::ADDRESS_VECTOR_SIZE);
                    $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                    $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
                }

                ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
                ctx.last_wqe().set(Some((wqe_ptr, wqe_size)));

                Ok($crate::wqe::emit::EmitResult {
                    wqe_ptr,
                    wqe_idx,
                    wqe_size,
                    wqebb_cnt,
                })
            }
        }
    }};
}

pub use emit_ud_wqe;

// =============================================================================
// BlueFlame Emission Context
// =============================================================================

/// BlueFlame用バッファコンテキスト
///
/// BlueFlame doorbell経由でWQEを書き込むためのコンテキスト。
/// WQEはまずバッファに書き込まれ、その後BFレジスタにコピーされる。
pub struct BlueflameEmitContext<'a, Entry> {
    /// 内部バッファ（256バイト）
    pub buffer: &'a mut [u8; 256],
    /// 現在のオフセット
    pub offset: &'a mut usize,
    /// SQ状態への参照
    pub sqn: u32,
    pub wqe_cnt: u16,
    pub pi: &'a Cell<u16>,
    pub table: &'a OrderedWqeTable<Entry>,
}

impl<'a, Entry> BlueflameEmitContext<'a, Entry> {
    /// バッファの残り容量を取得
    #[inline]
    pub fn remaining(&self) -> usize {
        256 - *self.offset
    }

    /// 現在のWQEインデックスを取得
    #[inline]
    pub fn wqe_idx(&self) -> u16 {
        self.pi.get()
    }

    /// PIを進める
    #[inline]
    pub fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }
}

// =============================================================================
// emit_wqe_bf! Macro (Direct Expansion Version)
// =============================================================================

/// Emit a WQE to BlueFlame buffer using the macro-based API.
///
/// This macro directly expands to WQE construction code without intermediate
/// function calls or parameter structs.
///
/// # Syntax
///
/// ```ignore
/// // WRITE with SGE
/// emit_wqe_bf!(ctx, write {
///     flags: WqeFlags::empty(),
///     remote_addr: dest,
///     rkey: rkey,
///     sge: { addr: local_addr, len: 64, lkey },
///     signaled: entry,
/// })?;
///
/// // SEND with inline
/// emit_wqe_bf!(ctx, send {
///     flags: WqeFlags::empty(),
///     inline: &payload,
///     signaled: entry,
/// })?;
/// ```
#[macro_export]
macro_rules! emit_wqe_bf {
    // WRITE with SGE (unsignaled)
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < WQE_SIZE) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $flags,
                        imm: 0,
                    },
                );
                $crate::wqe::write_rdma_seg(wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE), $raddr, $rkey);
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            *ctx.offset += WQE_SIZE;
            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // WRITE with SGE (signaled)
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < WQE_SIZE) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };
            let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags,
                        imm: 0,
                    },
                );
                $crate::wqe::write_rdma_seg(wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE), $raddr, $rkey);
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            *ctx.offset += WQE_SIZE;
            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // WRITE with inline (unsignaled)
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < wqe_size) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (wqe_size / 16) as u8,
                        flags: $flags,
                        imm: 0,
                    },
                );
                $crate::wqe::write_rdma_seg(wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE), $raddr, $rkey);
                let inline_ptr =
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
            }

            *ctx.offset += wqe_size;
            ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size,
                wqebb_cnt,
            })
        }
    }};

    // WRITE with inline (signaled)
    ($ctx:expr, write {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < wqe_size) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };
            let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::RdmaWrite as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (wqe_size / 16) as u8,
                        flags,
                        imm: 0,
                    },
                );
                $crate::wqe::write_rdma_seg(wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE), $raddr, $rkey);
                let inline_ptr =
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE);
                $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
            }

            *ctx.offset += wqe_size;
            ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size,
                wqebb_cnt,
            })
        }
    }};

    // WRITE_IMM with SGE (unsignaled)
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < WQE_SIZE) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $flags,
                        imm: $imm,
                    },
                );
                $crate::wqe::write_rdma_seg(wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE), $raddr, $rkey);
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            *ctx.offset += WQE_SIZE;
            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // WRITE_IMM with SGE (signaled)
    ($ctx:expr, write_imm {
        flags: $flags:expr,
        remote_addr: $raddr:expr,
        rkey: $rkey:expr,
        imm: $imm:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize =
            $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < WQE_SIZE) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };
            let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::RdmaWriteImm as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags,
                        imm: $imm,
                    },
                );
                $crate::wqe::write_rdma_seg(wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE), $raddr, $rkey);
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE + $crate::wqe::RDMA_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            *ctx.offset += WQE_SIZE;
            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // SEND with SGE (unsignaled)
    ($ctx:expr, send {
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? } $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < WQE_SIZE) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::Send as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags: $flags,
                        imm: 0,
                    },
                );
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            *ctx.offset += WQE_SIZE;
            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // SEND with SGE (signaled)
    ($ctx:expr, send {
        flags: $flags:expr,
        sge: { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr $(,)? },
        signaled: $entry:expr $(,)?
    }) => {{
        const WQE_SIZE: usize = $crate::wqe::CTRL_SEG_SIZE + $crate::wqe::DATA_SEG_SIZE;
        const WQEBB_CNT: u16 = WQE_SIZE.div_ceil(64) as u16;

        let ctx = $ctx;
        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < WQE_SIZE) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };
            let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::Send as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (WQE_SIZE / 16) as u8,
                        flags,
                        imm: 0,
                    },
                );
                $crate::wqe::write_data_seg(
                    wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE),
                    $len,
                    $lkey,
                    $addr,
                );
            }

            *ctx.offset += WQE_SIZE;
            ctx.pi().set(wqe_idx.wrapping_add(WQEBB_CNT));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size: WQE_SIZE,
                wqebb_cnt: WQEBB_CNT,
            })
        }
    }};

    // SEND with inline (unsignaled)
    ($ctx:expr, send {
        flags: $flags:expr,
        inline: $data:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < wqe_size) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::Send as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (wqe_size / 16) as u8,
                        flags: $flags,
                        imm: 0,
                    },
                );
                let inline_ptr = wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE);
                $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
            }

            *ctx.offset += wqe_size;
            ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size,
                wqebb_cnt,
            })
        }
    }};

    // SEND with inline (signaled)
    ($ctx:expr, send {
        flags: $flags:expr,
        inline: $data:expr,
        signaled: $entry:expr $(,)?
    }) => {{
        let ctx = $ctx;
        let data: &[u8] = $data;
        let data_len = data.len();
        let inline_size = $crate::wqe::inline_padded_size(data_len);
        let wqe_size = $crate::wqe::CTRL_SEG_SIZE + inline_size;
        let wqebb_cnt = ((wqe_size + 63) / 64) as u16;

        let remaining = 256 - *ctx.offset;

        if $crate::wqe::unlikely(remaining < wqe_size) {
            Err($crate::wqe::SubmissionError::BlueflameOverflow)
        } else {
            let wqe_idx = ctx.pi().get();
            let wqe_ptr = unsafe { ctx.buffer.as_mut_ptr().add(*ctx.offset) };
            let flags = $flags | $crate::wqe::WqeFlags::COMPLETION;

            unsafe {
                $crate::wqe::write_ctrl_seg(
                    wqe_ptr,
                    &$crate::wqe::CtrlSegParams {
                        opmod: 0,
                        opcode: $crate::wqe::WqeOpcode::Send as u8,
                        wqe_idx,
                        qpn: ctx.sqn(),
                        ds_cnt: (wqe_size / 16) as u8,
                        flags,
                        imm: 0,
                    },
                );
                let inline_ptr = wqe_ptr.add($crate::wqe::CTRL_SEG_SIZE);
                $crate::wqe::write_inline_header(inline_ptr, data_len as u32);
                $crate::wqe::copy_inline_data(inline_ptr.add(4), data.as_ptr(), data_len);
            }

            *ctx.offset += wqe_size;
            ctx.pi().set(wqe_idx.wrapping_add(wqebb_cnt));
            ctx.table().store(wqe_idx, $entry, ctx.pi.get());

            Ok($crate::wqe::emit::EmitResult {
                wqe_ptr,
                wqe_idx,
                wqe_size,
                wqebb_cnt,
            })
        }
    }};
}

pub use emit_wqe_bf;
