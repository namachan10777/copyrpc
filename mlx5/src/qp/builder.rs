//! WQE builder types for RC QP.
//!
//! This module contains all WQE builder types that are used with RC QP.
//! These builders provide type-safe construction of Work Queue Elements.

use std::cell::Cell;
use std::io;
use std::marker::PhantomData;

use crate::types::GrhAttr;
use crate::wqe::{
    AddressVector, AtomicSeg, CtrlSeg, DataSeg, HasData, InlineHeader, Init, MaskedAtomicSeg32,
    MaskedAtomicSeg64, NeedsAtomic, NeedsData, NeedsRdma, NeedsRdmaThenAtomic, OrderedWqeTable,
    RdmaSeg, SubmissionError, WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
    // Address Vector trait and types
    Av, NoData,
    // Flags
    TxFlags,
};

use super::SendQueueState;

// =============================================================================
// WQE Builder
// =============================================================================

/// Zero-copy WQE builder for RC QP with type-state safety.
///
/// Writes segments directly to the SQ buffer without intermediate copies.
/// The `State` type parameter tracks the current builder state, ensuring
/// that only valid segment sequences can be constructed.
pub struct WqeBuilder<'a, Entry, TableType, State> {
    pub(super) sq: &'a SendQueueState<Entry, TableType>,
    pub(super) wqe_ptr: *mut u8,
    pub(super) wqe_idx: u16,
    pub(super) offset: usize,
    pub(super) ds_count: u8,
    /// Whether SIGNALED flag is set
    pub(super) signaled: bool,
    pub(super) _state: PhantomData<State>,
}

// =============================================================================
// WQE Builder Type-State Implementations
// =============================================================================

/// Helper to transition builder state.
impl<'a, Entry, TableType, State> WqeBuilder<'a, Entry, TableType, State> {
    #[inline]
    fn transition<NewState>(self) -> WqeBuilder<'a, Entry, TableType, NewState> {
        WqeBuilder {
            sq: self.sq,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            offset: self.offset,
            ds_count: self.ds_count,
            signaled: self.signaled,
            _state: PhantomData,
        }
    }

    /// Write control segment (internal helper).
    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                0, // opmod = 0 for normal operations
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0, // ds_count will be updated in finish
                flags.bits(),
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    /// Write control segment with custom opmod (internal helper).
    #[inline]
    fn write_ctrl_with_opmod(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32, opmod: u8) {
        let fm_ce_se = if self.signaled {
            (flags | WqeFlags::COMPLETION | WqeFlags::FENCE).bits()
        } else {
            (flags | WqeFlags::FENCE).bits()
        };

        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                opmod,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0, // ds_count will be updated in finish
                fm_ce_se,
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }
}

/// Init state: control segment methods.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, Init> {
    // -------------------------------------------------------------------------
    // Send operations -> NeedsData
    // -------------------------------------------------------------------------

    /// Write control segment for SEND operation.
    #[inline]
    pub fn ctrl_send(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        self.write_ctrl(WqeOpcode::Send, flags, 0);
        self.transition()
    }

    /// Write control segment for SEND with immediate data.
    #[inline]
    pub fn ctrl_send_imm(
        mut self,
        flags: WqeFlags,
        imm: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        self.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.transition()
    }

    /// Write control segment for SEND with invalidate.
    #[inline]
    pub fn ctrl_send_inval(
        mut self,
        flags: WqeFlags,
        inv_rkey: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        self.write_ctrl(WqeOpcode::SendInval, flags, inv_rkey);
        self.transition()
    }

    // -------------------------------------------------------------------------
    // RDMA operations -> NeedsRdma
    // -------------------------------------------------------------------------

    /// Write control segment for RDMA WRITE operation.
    #[inline]
    pub fn ctrl_rdma_write(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
        self.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.transition()
    }

    /// Write control segment for RDMA WRITE with immediate data.
    #[inline]
    pub fn ctrl_rdma_write_imm(
        mut self,
        flags: WqeFlags,
        imm: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
        self.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.transition()
    }

    /// Write control segment for RDMA READ operation.
    #[inline]
    pub fn ctrl_rdma_read(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
        self.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.transition()
    }

    // -------------------------------------------------------------------------
    // Atomic operations -> NeedsRdmaThenAtomic
    // -------------------------------------------------------------------------

    /// Write control segment for Atomic Compare-and-Swap operation.
    #[inline]
    pub fn ctrl_atomic_cas(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        self.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.transition()
    }

    /// Write control segment for Atomic Fetch-and-Add operation.
    #[inline]
    pub fn ctrl_atomic_fa(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        self.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.transition()
    }

    /// Write control segment for Masked Atomic Compare-and-Swap (32-bit).
    #[inline]
    pub fn ctrl_masked_atomic_cas32(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x08 for 32-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 0x08);
        self.transition()
    }

    /// Write control segment for Masked Atomic Fetch-and-Add (32-bit).
    #[inline]
    pub fn ctrl_masked_atomic_fa32(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x08 for 32-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 0x08);
        self.transition()
    }

    /// Write control segment for Masked Atomic Compare-and-Swap (64-bit).
    #[inline]
    pub fn ctrl_masked_atomic_cas64(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x09 for 64-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 0x09);
        self.transition()
    }

    /// Write control segment for Masked Atomic Fetch-and-Add (64-bit).
    #[inline]
    pub fn ctrl_masked_atomic_fa64(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x09 for 64-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 0x09);
        self.transition()
    }

    // -------------------------------------------------------------------------
    // NOP -> HasData (finish immediately available)
    // -------------------------------------------------------------------------

    /// Write control segment for NOP operation.
    #[inline]
    pub fn ctrl_nop(mut self, flags: WqeFlags) -> WqeBuilder<'a, Entry, TableType, HasData> {
        self.write_ctrl(WqeOpcode::Nop, flags, 0);
        self.transition()
    }
}

/// NeedsRdma state: RDMA segment required.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
    /// Add an RDMA segment specifying remote address and rkey.
    #[inline]
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }
}

/// NeedsRdmaThenAtomic state: RDMA segment required, then atomic segment.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
    /// Add an RDMA segment specifying remote address and rkey.
    #[inline]
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> WqeBuilder<'a, Entry, TableType, NeedsAtomic> {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }
}

/// NeedsAtomic state: atomic segment required.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsAtomic> {
    /// Add an atomic Compare-and-Swap segment.
    ///
    /// The CAS operation atomically compares the 8-byte value at `remote_addr`
    /// (specified in the preceding RDMA segment) with `compare`. If equal,
    /// replaces it with `swap`. The original value is written to the local
    /// buffer (specified in the following data segment).
    #[inline]
    pub fn atomic_cas(mut self, swap: u64, compare: u64) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            AtomicSeg::write_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Add an atomic Fetch-and-Add segment.
    ///
    /// The FA operation atomically adds `add_value` to the 8-byte value at
    /// `remote_addr` (specified in the preceding RDMA segment). The original
    /// value is written to the local buffer (specified in the following data
    /// segment).
    #[inline]
    pub fn atomic_fa(mut self, add_value: u64) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            AtomicSeg::write_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Masked Compare-and-Swap (64-bit, 2 segments).
    #[inline]
    pub fn masked_cas64(
        mut self,
        swap: u64,
        compare: u64,
        swap_mask: u64,
        compare_mask: u64,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            let ptr1 = self.wqe_ptr.add(self.offset);
            let ptr2 = self
                .wqe_ptr
                .add(self.offset + MaskedAtomicSeg64::SIZE_CAS / 2);

            MaskedAtomicSeg64::write_cas(ptr1, ptr2, swap, compare, swap_mask, compare_mask);
        }
        self.offset += MaskedAtomicSeg64::SIZE_CAS;
        self.ds_count += 2;
        self.transition()
    }

    /// Masked Fetch-and-Add (64-bit, 1 segment).
    #[inline]
    pub fn masked_fa64(mut self, add: u64, field_mask: u64) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            MaskedAtomicSeg64::write_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MaskedAtomicSeg64::SIZE_FA;
        self.ds_count += 1;
        self.transition()
    }

    /// Masked Compare-and-Swap (32-bit, 1 segment).
    #[inline]
    pub fn masked_cas32(
        mut self,
        swap: u32,
        compare: u32,
        swap_mask: u32,
        compare_mask: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            MaskedAtomicSeg32::write_cas(
                self.wqe_ptr.add(self.offset),
                swap,
                compare,
                swap_mask,
                compare_mask,
            );
        }
        self.offset += MaskedAtomicSeg32::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Masked Fetch-and-Add (32-bit, 1 segment).
    #[inline]
    pub fn masked_fa32(mut self, add: u32, field_mask: u32) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            MaskedAtomicSeg32::write_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MaskedAtomicSeg32::SIZE;
        self.ds_count += 1;
        self.transition()
    }
}

/// NeedsData state: data segment (SGE or inline) required.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsData> {
    /// Add a data segment (SGE).
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> WqeBuilder<'a, Entry, TableType, HasData> {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Add inline data.
    #[inline]
    pub fn inline_data(mut self, data: &[u8]) -> WqeBuilder<'a, Entry, TableType, HasData> {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        self.transition()
    }

    /// Get a mutable slice for inline data (zero-copy).
    ///
    /// Returns the builder and a slice that can be written to directly.
    #[inline]
    pub fn inline_slice(mut self, len: usize) -> (WqeBuilder<'a, Entry, TableType, HasData>, &'a mut [u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            InlineHeader::write(ptr, len as u32)
        };
        let data_ptr = unsafe { self.wqe_ptr.add(self.offset + 4) };
        let slice = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        (self.transition(), slice)
    }
}

/// HasData state: additional SGEs and finish methods available.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, HasData> {
    /// Add another data segment (SGE).
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self
    }
}

// =============================================================================
// Maximum WQEBB Calculation Functions
// =============================================================================

/// Calculate the padded inline data size (16-byte aligned).
///
/// inline_padded = ((4 + max_inline_data + 15) & !15)
#[inline]
fn calc_inline_padded(max_inline_data: u32) -> usize {
    ((4 + max_inline_data as usize) + 15) & !15
}

/// Calculate maximum WQEBB count for SEND operation.
///
/// Layout: ctrl(16) + AV + inline_padded
#[inline]
fn calc_max_wqebb_send(av_size: usize, max_inline_data: u32) -> u16 {
    let size = CtrlSeg::SIZE + av_size + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for WRITE operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + inline_padded
#[inline]
fn calc_max_wqebb_write(av_size: usize, max_inline_data: u32) -> u16 {
    let size = CtrlSeg::SIZE + av_size + RdmaSeg::SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for READ operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + sge(16)
#[inline]
fn calc_max_wqebb_read(av_size: usize) -> u16 {
    let size = CtrlSeg::SIZE + av_size + RdmaSeg::SIZE + DataSeg::SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Atomic operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + atomic(16) + sge(16)
#[inline]
fn calc_max_wqebb_atomic(av_size: usize) -> u16 {
    let size = CtrlSeg::SIZE + av_size + RdmaSeg::SIZE + AtomicSeg::SIZE + DataSeg::SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for NOP operation.
///
/// Layout: ctrl(16) only
#[inline]
fn calc_max_wqebb_nop() -> u16 {
    calc_wqebb_cnt(CtrlSeg::SIZE)
}

// =============================================================================
// WqeCore
// =============================================================================

/// Internal WQE builder core that handles direct buffer writes.
pub(super) struct WqeCore<'a, Entry> {
    sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    pub(super) signaled: bool,
    pub(super) entry: Option<Entry>,
}

impl<'a, Entry> WqeCore<'a, Entry> {
    #[inline]
    pub(super) fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>, entry: Option<Entry>) -> Self {
        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);
        Self {
            sq,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            signaled: entry.is_some(),
            entry,
        }
    }

    #[inline]
    pub(super) fn available(&self) -> u16 {
        self.sq.available()
    }

    #[inline]
    pub(super) fn max_inline_data(&self) -> u32 {
        self.sq.max_inline_data()
    }

    #[inline]
    pub(super) fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                0,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    /// Write Address Vector segment using the Av trait.
    #[inline]
    pub(super) fn write_av<A: Av>(&mut self, av: A) {
        unsafe {
            av.write_av(self.wqe_ptr.add(self.offset));
        }
        self.offset += A::SIZE;
        self.ds_count += A::DS_COUNT;
    }

    #[inline]
    pub(super) fn write_rdma(&mut self, addr: u64, rkey: u32) {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_inline(&mut self, data: &[u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
    }

    #[inline]
    pub(super) fn write_atomic_cas(&mut self, swap: u64, compare: u64) {
        unsafe {
            AtomicSeg::write_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_atomic_fa(&mut self, add_value: u64) {
        unsafe {
            AtomicSeg::write_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn finish_internal(self) -> io::Result<WqeHandle> {
        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let slots_to_end = self.sq.slots_to_end();

        if wqebb_cnt > slots_to_end && slots_to_end < self.sq.wqe_cnt {
            return self.finish_with_wrap_around(wqebb_cnt, slots_to_end);
        }

        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(self.wqe_ptr);
            }
        }

        let wqe_idx = self.wqe_idx;
        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx,
            size: self.offset,
        })
    }

    #[cold]
    fn finish_with_wrap_around(self, wqebb_cnt: u16, slots_to_end: u16) -> io::Result<WqeHandle> {
        let total_needed = slots_to_end + wqebb_cnt;
        if self.sq.available() < total_needed {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let mut temp_buf = [0u8; 256];
        unsafe {
            std::ptr::copy_nonoverlapping(self.wqe_ptr, temp_buf.as_mut_ptr(), self.offset);
            self.sq.post_nop(slots_to_end);
        }

        let new_wqe_idx = self.sq.pi.get();
        let new_wqe_ptr = self.sq.get_wqe_ptr(new_wqe_idx);

        unsafe {
            std::ptr::copy_nonoverlapping(temp_buf.as_ptr(), new_wqe_ptr, self.offset);
            CtrlSeg::update_wqe_idx(new_wqe_ptr, new_wqe_idx);
            CtrlSeg::update_ds_cnt(new_wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(new_wqe_ptr);
            }
        }

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(new_wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(new_wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx: new_wqe_idx,
            size: self.offset,
        })
    }
}

// =============================================================================
// Unified SQ WQE Entry Point
// =============================================================================

/// SQ WQE entry point for both IB and RoCE transport.
///
/// Provides verb methods that include all required parameters.
/// The `A` type parameter is the Address Vector type (NoAv for IB, &GrhAttr for RoCE).
#[must_use = "WQE builder must be finished"]
pub struct SqWqeEntryPoint<'a, Entry, A> {
    core: WqeCore<'a, Entry>,
    av: A,
}

impl<'a, Entry, A: Av> SqWqeEntryPoint<'a, Entry, A> {
    /// Create a new entry point with the given AV.
    #[inline]
    pub(crate) fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>, av: A) -> Self {
        Self {
            core: WqeCore::new(sq, None),
            av,
        }
    }

    // -------------------------------------------------------------------------
    // SEND operations
    // -------------------------------------------------------------------------

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: TxFlags) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_av(self.av);
        Ok(SendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(mut self, flags: TxFlags, imm: u32) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_av(self.av);
        Ok(SendWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // WRITE operations (remote_addr, rkey required)
    // -------------------------------------------------------------------------

    /// Start building an RDMA WRITE WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<WriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        Ok(WriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write_imm(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<WriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        Ok(WriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // READ operations (remote_addr, rkey required)
    // -------------------------------------------------------------------------

    /// Start building an RDMA READ WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn read(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<ReadWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_read(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        Ok(ReadWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Atomic operations (remote_addr, rkey, and atomic params required)
    // -------------------------------------------------------------------------

    /// Start building an Atomic Compare-and-Swap WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn cas(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_cas(swap, compare);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an Atomic Fetch-and-Add WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn fetch_add(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_fa(add_value);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: TxFlags) -> io::Result<NopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        Ok(NopWqeBuilder { core: self.core })
    }
}

// =============================================================================
// Send WQE Builder
// =============================================================================

/// SEND WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct SendWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> SendWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> SendWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        SendWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> SendWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        SendWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// sge/inline/finish: HasData state.
impl<'a, Entry> SendWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// Write WQE Builder
// =============================================================================

/// WRITE WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct WriteWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> WriteWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> WriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        WriteWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> WriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        WriteWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// sge/inline/finish: HasData state.
impl<'a, Entry> WriteWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// Read WQE Builder
// =============================================================================

/// READ WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct ReadWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge: NoData state, transitions to HasData.
impl<'a, Entry> ReadWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry for the read result.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> ReadWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        ReadWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// sge/finish: HasData state.
impl<'a, Entry> ReadWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// Atomic WQE Builder (CAS / Fetch-Add)
// =============================================================================

/// Atomic WQE builder (Compare-and-Swap or Fetch-and-Add).
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct AtomicWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge: NoData state, transitions to HasData.
impl<'a, Entry> AtomicWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry for the atomic result.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> AtomicWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        AtomicWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish: HasData state.
impl<'a, Entry> AtomicWqeBuilder<'a, Entry, HasData> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// NOP WQE Builder
// =============================================================================

/// NOP WQE builder.
///
/// NOP operations have no data segment, so `finish_*()` methods are
/// available immediately without needing to call `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct NopWqeBuilder<'a, Entry> {
    core: WqeCore<'a, Entry>,
}

impl<'a, Entry> NopWqeBuilder<'a, Entry> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// RQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame buffer size in bytes (256B doorbell window).
const BLUEFLAME_BUFFER_SIZE: usize = 256;

/// RQ WQE size in bytes (single DataSeg).
const RQ_WQE_SIZE: usize = DataSeg::SIZE;

/// BlueFlame WQE batch builder for RC RQ.
///
/// Allows posting multiple receive WQEs efficiently using BlueFlame MMIO.
/// WQEs are accumulated in an internal buffer and submitted together
/// via BlueFlame doorbell when `finish()` is called.
///
/// RQ WQE size is 16 bytes (single DataSeg), so up to 16 WQEs can fit in a batch.
///
/// # Example
/// ```ignore
/// let mut batch = qp.blueflame_rq_batch()?;
/// batch.post(entry1, addr1, len1, lkey1)?;
/// batch.post(entry2, addr2, len2, lkey2)?;
/// batch.finish();
/// ```
pub struct RqBlueflameWqeBatch<'a, Entry> {
    rq: &'a super::ReceiveQueueState<Entry>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
    wqe_count: u16,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &'a Cell<u32>,
}

impl<'a, Entry> RqBlueflameWqeBatch<'a, Entry> {
    /// Create a new RQ BlueFlame batch builder.
    pub(super) fn new(
        rq: &'a super::ReceiveQueueState<Entry>,
        bf_reg: *mut u8,
        bf_size: u32,
        bf_offset: &'a Cell<u32>,
    ) -> Self {
        Self {
            rq,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
            wqe_count: 0,
            bf_reg,
            bf_size,
            bf_offset,
        }
    }

    /// Post a receive WQE to the batch.
    ///
    /// # Arguments
    /// * `entry` - User entry to associate with this receive operation
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    ///
    /// # Errors
    /// Returns `RqFull` if the receive queue is full.
    /// Returns `BlueflameOverflow` if the batch buffer is full.
    #[inline]
    pub fn post(&mut self, entry: Entry, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.rq.available() == 0 {
            return Err(SubmissionError::RqFull);
        }
        if self.offset + RQ_WQE_SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }

        // Write data segment to buffer
        unsafe {
            DataSeg::write(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }

        // Store entry in RQ table
        let wqe_idx = self.rq.pi.get();
        let idx = (wqe_idx as usize) & (self.rq.table.len() - 1);
        self.rq.table[idx].set(Some(entry));

        // Advance state
        self.offset += RQ_WQE_SIZE;
        self.wqe_count += 1;
        self.rq.advance_pi(1);

        Ok(())
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    #[inline]
    pub fn finish(self) {
        if self.wqe_count == 0 {
            return; // No WQEs to submit
        }

        mmio_flush_writes!();

        // Update doorbell record
        unsafe {
            std::ptr::write_volatile(
                self.rq.dbrec,
                (self.rq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        // Copy buffer to BlueFlame register
        if self.bf_size > 0 {
            let bf_offset = self.bf_offset.get();
            let bf = unsafe { self.bf_reg.add(bf_offset as usize) };

            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.bf_offset.set(bf_offset ^ self.bf_size);
        }
    }
}

// =============================================================================
// SQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame WQE batch builder for RC SQ.
///
/// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
/// and submitted together via a single BlueFlame doorbell.
///
/// # Example
/// ```ignore
/// let mut bf = qp.blueflame_sq_wqe()?;
/// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
/// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
/// bf.finish();
/// ```
pub struct BlueflameWqeBatch<'a, Entry> {
    pub(super) sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> BlueflameWqeBatch<'a, Entry> {
    /// Create a new BlueFlame batch builder.
    pub(super) fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>) -> Self {
        Self {
            sq,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<BlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(BlueflameWqeEntryPoint {
            batch: self,
        })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    ///
    /// This method copies the accumulated WQEs to the BlueFlame register
    /// and rings the doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return; // No WQEs to submit
        }

        mmio_flush_writes!();

        unsafe {
            // Update doorbell record with new PI
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

            // Copy buffer to BlueFlame register in 64-byte chunks
            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.sq.bf_offset.set(bf_offset ^ self.sq.bf_size);
        }
    }
}

/// BlueFlame WQE entry point for a single WQE within a batch.
#[must_use = "WQE builder must be finished"]
pub struct BlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut BlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> BlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: TxFlags) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: TxFlags, imm: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: TxFlags, remote_addr: u64, rkey: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_rdma(remote_addr, rkey)?;
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        core.write_rdma(remote_addr, rkey)?;
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for BlueFlame WQE construction.
struct BlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut BlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> BlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut BlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
        // Ensure we have at least space for control segment
        if batch.offset + CtrlSeg::SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        Ok(Self {
            wqe_start: batch.offset,
            offset: batch.offset,
            batch,
            ds_count: 0,
            signaled: false,
        })
    }

    #[inline]
    fn remaining(&self) -> usize {
        BLUEFLAME_BUFFER_SIZE - self.offset
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: TxFlags, imm: u32) {
        let wqe_idx = self.batch.sq.pi.get();
        let flags = WqeFlags::from_bits_truncate(flags.bits());
        unsafe {
            CtrlSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset += CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < RdmaSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            RdmaSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DataSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            DataSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) -> Result<(), SubmissionError> {
        let padded_size = ((4 + data.len()) + 15) & !15;
        if self.remaining() < padded_size {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            let ptr = self.batch.buffer.as_mut_ptr().add(self.offset);
            InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
        }
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        Ok(())
    }

    #[inline]
    fn finish_internal(self, entry: Option<Entry>) -> Result<(), SubmissionError> {
        // Update ds_count in control segment
        unsafe {
            CtrlSeg::update_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                CtrlSeg::set_completion_flag(
                    self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                );
            }
        }

        let wqe_size = self.offset - self.wqe_start;
        let wqebb_cnt = calc_wqebb_cnt(wqe_size);

        // Store entry if signaled
        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        // Advance batch state
        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct BlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: BlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> BlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(BlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(BlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> BlueflameWqeBuilder<'b, 'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<Self, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(self)
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<Self, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(self)
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish(self) -> Result<(), SubmissionError> {
        self.core.finish_internal(None)
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(self, entry: Entry) -> Result<(), SubmissionError> {
        self.core.finish_internal(Some(entry))
    }
}

// =============================================================================
// RoCE SQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame WQE batch builder for RC SQ (RoCE transport).
///
/// Similar to `BlueflameWqeBatch` but includes GRH information for RoCE addressing.
/// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
/// and submitted together via a single BlueFlame doorbell.
///
/// # Example
/// ```ignore
/// let mut bf = qp.blueflame_sq_wqe(&grh)?;
/// bf.wqe()?.send(TxFlags::empty()).inline(&data)?.finish()?;
/// bf.wqe()?.send(TxFlags::empty()).inline(&data)?.finish()?;
/// bf.finish();
/// ```
pub struct RoceBlueflameWqeBatch<'a, Entry> {
    pub(super) sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>,
    grh: &'a GrhAttr,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> RoceBlueflameWqeBatch<'a, Entry> {
    /// Create a new RoCE BlueFlame batch builder.
    pub(super) fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>, grh: &'a GrhAttr) -> Self {
        Self {
            sq,
            grh,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<RoceBlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(RoceBlueflameWqeEntryPoint { batch: self })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    ///
    /// This method copies the accumulated WQEs to the BlueFlame register
    /// and rings the doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return; // No WQEs to submit
        }

        mmio_flush_writes!();

        unsafe {
            // Update doorbell record with new PI
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

            // Copy buffer to BlueFlame register in 64-byte chunks
            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.sq.bf_offset.set(bf_offset ^ self.sq.bf_size);
        }
    }
}

/// RoCE BlueFlame WQE entry point for a single WQE within a batch.
#[must_use = "WQE builder must be finished"]
pub struct RoceBlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut RoceBlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> RoceBlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: TxFlags) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_av_roce()?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: TxFlags, imm: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_av_roce()?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: TxFlags, remote_addr: u64, rkey: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_av_roce()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        core.write_av_roce()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for RoCE BlueFlame WQE construction.
struct RoceBlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut RoceBlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> RoceBlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut RoceBlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
        // Ensure we have at least space for control segment
        if batch.offset + CtrlSeg::SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        Ok(Self {
            wqe_start: batch.offset,
            offset: batch.offset,
            batch,
            ds_count: 0,
            signaled: false,
        })
    }

    #[inline]
    fn remaining(&self) -> usize {
        BLUEFLAME_BUFFER_SIZE - self.offset
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: TxFlags, imm: u32) {
        let wqe_idx = self.batch.sq.pi.get();
        let flags = WqeFlags::from_bits_truncate(flags.bits());
        unsafe {
            CtrlSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset += CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_av_roce(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < AddressVector::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            // For RC QP RoCE, dc_key and dctn are not used (set to 0)
            AddressVector::write_roce(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                0,
                self.batch.grh,
            );
        }
        self.offset += AddressVector::SIZE;
        self.ds_count += (AddressVector::SIZE / 16) as u8;
        Ok(())
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < RdmaSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            RdmaSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DataSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            DataSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) -> Result<(), SubmissionError> {
        let padded_size = ((4 + data.len()) + 15) & !15;
        if self.remaining() < padded_size {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            let ptr = self.batch.buffer.as_mut_ptr().add(self.offset);
            InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
        }
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        Ok(())
    }

    #[inline]
    fn finish_internal(self, entry: Option<Entry>) -> Result<(), SubmissionError> {
        // Update ds_count in control segment
        unsafe {
            CtrlSeg::update_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                CtrlSeg::set_completion_flag(
                    self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                );
            }
        }

        let wqe_size = self.offset - self.wqe_start;
        let wqebb_cnt = calc_wqebb_cnt(wqe_size);

        // Store entry if signaled
        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        // Advance batch state
        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// RoCE BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct RoceBlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: RoceBlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(RoceBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(RoceBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> RoceBlueflameWqeBuilder<'b, 'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<Self, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(self)
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<Self, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(self)
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish(self) -> Result<(), SubmissionError> {
        self.core.finish_internal(None)
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(self, entry: Entry) -> Result<(), SubmissionError> {
        self.core.finish_internal(Some(entry))
    }
}
