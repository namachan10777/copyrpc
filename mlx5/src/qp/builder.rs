//! WQE builder types for RC QP.
//!
//! This module contains all WQE builder types that are used with RC QP.
//! These builders provide type-safe construction of Work Queue Elements.

use std::cell::Cell;
use std::io;
use std::marker::PhantomData;

use crate::types::GrhAttr;
use crate::pd::{AccessFlags, MemoryWindow, MwBindInfo};
use crate::wqe::{
    ATOMIC_SEG_SIZE, CTRL_SEG_SIZE, DATA_SEG_SIZE, HasData, MaskedCasParams, OrderedWqeTable,
    MASKED_ATOMIC_SEG32_SIZE, MASKED_ATOMIC_SEG64_SIZE_CAS, MASKED_ATOMIC_SEG64_SIZE_FA,
    MASKED_ATOMIC_SEG128_SIZE_CAS, MASKED_ATOMIC_SEG128_SIZE_FA,
    RDMA_SEG_SIZE, SubmissionError, WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
    set_ctrl_seg_completion_flag, update_ctrl_seg_ds_cnt, update_ctrl_seg_wqe_idx,
    write_atomic_seg_cas, write_atomic_seg_fa, write_ctrl_seg, write_data_seg, write_inline_header,
    write_masked_atomic_seg32_cas, write_masked_atomic_seg32_fa,
    write_masked_atomic_seg64_cas, write_masked_atomic_seg64_fa,
    write_masked_atomic_seg128_cas, write_masked_atomic_seg128_fa,
    write_rdma_seg,
    ADDRESS_VECTOR_SIZE, write_address_vector_roce,
    // Address Vector trait and types
    Av, NoData,
    // UMR segments
    UMR_CTRL_SEG_SIZE, MKEY_CONTEXT_SEG_SIZE, UMR_KLM_SEG_PADDED_SIZE,
    write_umr_ctrl_seg_bind, write_umr_ctrl_seg_invalidate,
    write_mkey_context_seg_bind, write_mkey_context_seg_invalidate,
    write_umr_klm_seg_padded, mkey_access_flags,
};

use super::SendQueueState;

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
    let size = CTRL_SEG_SIZE + av_size + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for WRITE operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + inline_padded
#[inline]
fn calc_max_wqebb_write(av_size: usize, max_inline_data: u32) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for READ operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + sge(16)
#[inline]
fn calc_max_wqebb_read(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Atomic operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + atomic(16) + sge(16)
#[inline]
fn calc_max_wqebb_atomic(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + ATOMIC_SEG_SIZE + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for NOP operation.
///
/// Layout: ctrl(16) only
#[inline]
fn calc_max_wqebb_nop() -> u16 {
    calc_wqebb_cnt(CTRL_SEG_SIZE)
}

/// Calculate maximum WQEBB count for Masked Atomic 32-bit operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + masked_atomic_32(16) + sge(16)
#[inline]
fn calc_max_wqebb_masked_atomic_32(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG32_SIZE + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Masked Atomic 64-bit CAS operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + masked_atomic_64_cas(32) + sge(16)
/// Note: 64-bit CAS uses 2 segments (32 bytes total for atomic data)
#[inline]
fn calc_max_wqebb_masked_atomic_64_cas(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG64_SIZE_CAS + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Masked Atomic 64-bit FA operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + masked_atomic_64_fa(16) + sge(16)
#[inline]
fn calc_max_wqebb_masked_atomic_64_fa(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG64_SIZE_FA + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Masked Atomic 128-bit CAS operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + masked_atomic_128_cas(64) + sge(16)
/// Note: 128-bit CAS uses 4 segments (64 bytes total for atomic data)
#[inline]
fn calc_max_wqebb_masked_atomic_128_cas(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG128_SIZE_CAS + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Masked Atomic 128-bit FA operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + masked_atomic_128_fa(32) + sge(16)
/// Note: 128-bit FA uses 2 segments (32 bytes total for atomic data)
#[inline]
fn calc_max_wqebb_masked_atomic_128_fa(av_size: usize) -> u16 {
    let size = CTRL_SEG_SIZE + av_size + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG128_SIZE_FA + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for UMR (bind_mw) operation.
///
/// Layout: ctrl(16) + umr_ctrl(48) + mkey_context(64) + klm_padded(64) = 192 bytes
#[inline]
fn calc_max_wqebb_umr() -> u16 {
    let size = CTRL_SEG_SIZE + UMR_CTRL_SEG_SIZE + MKEY_CONTEXT_SEG_SIZE + UMR_KLM_SEG_PADDED_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Local Invalidate operation.
///
/// Layout: ctrl(16) + umr_ctrl(48) + mkey_context(64) = 128 bytes
#[inline]
fn calc_max_wqebb_local_inval() -> u16 {
    // Same as UMR
    calc_max_wqebb_umr()
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
        self.write_ctrl_with_opmod(opcode, flags, imm, 0)
    }

    #[inline]
    pub(super) fn write_ctrl_with_opmod(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32, opmod: u8) {
        // Apply fence from prior UMR operation (bind_mw/local_invalidate)
        let fence = self.sq.next_fence.get();
        self.sq.next_fence.set(0);

        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        // Combine passed flags with fence bits
        let flags_with_fence = WqeFlags::from_bits_truncate(flags.bits() | (fence as u16));
        unsafe {
            write_ctrl_seg(
                self.wqe_ptr,
                opmod,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags_with_fence,
                imm,
            );
        }
        self.offset = CTRL_SEG_SIZE;
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
            write_rdma_seg(self.wqe_ptr.add(self.offset), addr, rkey);
        }
        self.offset += RDMA_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) {
        unsafe {
            write_data_seg(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DATA_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_inline(&mut self, data: &[u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = write_inline_header(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
    }

    #[inline]
    pub(super) fn write_atomic_cas(&mut self, swap: u64, compare: u64) {
        unsafe {
            write_atomic_seg_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += ATOMIC_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_atomic_fa(&mut self, add_value: u64) {
        unsafe {
            write_atomic_seg_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += ATOMIC_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_masked_atomic_cas_32(
        &mut self,
        swap: u32,
        compare: u32,
        swap_mask: u32,
        compare_mask: u32,
    ) {
        unsafe {
            write_masked_atomic_seg32_cas(
                self.wqe_ptr.add(self.offset),
                swap,
                compare,
                swap_mask,
                compare_mask,
            );
        }
        self.offset += MASKED_ATOMIC_SEG32_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_masked_atomic_fa_32(&mut self, add: u32, field_mask: u32) {
        unsafe {
            write_masked_atomic_seg32_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MASKED_ATOMIC_SEG32_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_masked_atomic_cas_64(
        &mut self,
        swap: u64,
        compare: u64,
        swap_mask: u64,
        compare_mask: u64,
    ) {
        // 64-bit masked CAS uses 2 segments (32 bytes total)
        let ptr1 = unsafe { self.wqe_ptr.add(self.offset) };
        let ptr2 = unsafe { self.wqe_ptr.add(self.offset + 16) };
        unsafe {
            write_masked_atomic_seg64_cas(ptr1, ptr2, swap, compare, swap_mask, compare_mask);
        }
        self.offset += MASKED_ATOMIC_SEG64_SIZE_CAS;
        self.ds_count += 2; // 2 segments
    }

    #[inline]
    pub(super) fn write_masked_atomic_fa_64(&mut self, add: u64, field_mask: u64) {
        unsafe {
            write_masked_atomic_seg64_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MASKED_ATOMIC_SEG64_SIZE_FA;
        self.ds_count += 1;
    }

    #[inline]
    pub(super) fn write_masked_atomic_cas_128(
        &mut self,
        swap: u128,
        compare: u128,
        swap_mask: u128,
        compare_mask: u128,
    ) {
        // 128-bit masked CAS uses 4 segments (64 bytes total)
        unsafe {
            write_masked_atomic_seg128_cas(
                self.wqe_ptr.add(self.offset),
                swap,
                compare,
                swap_mask,
                compare_mask,
            );
        }
        self.offset += MASKED_ATOMIC_SEG128_SIZE_CAS;
        self.ds_count += 4; // 4 segments
    }

    #[inline]
    pub(super) fn write_masked_atomic_fa_128(&mut self, add: u128, field_boundary: u128) {
        // 128-bit masked FA uses 2 segments (32 bytes total)
        unsafe {
            write_masked_atomic_seg128_fa(self.wqe_ptr.add(self.offset), add, field_boundary);
        }
        self.offset += MASKED_ATOMIC_SEG128_SIZE_FA;
        self.ds_count += 2; // 2 segments
    }

    #[inline]
    pub(super) fn write_umr_ctrl_bind(&mut self, klm_octwords: u16) {
        unsafe {
            write_umr_ctrl_seg_bind(self.wqe_ptr.add(self.offset), klm_octwords);
        }
        self.offset += UMR_CTRL_SEG_SIZE;
        self.ds_count += (UMR_CTRL_SEG_SIZE / 16) as u8;
    }

    #[inline]
    pub(super) fn write_umr_ctrl_invalidate(&mut self) {
        unsafe {
            write_umr_ctrl_seg_invalidate(self.wqe_ptr.add(self.offset));
        }
        self.offset += UMR_CTRL_SEG_SIZE;
        self.ds_count += (UMR_CTRL_SEG_SIZE / 16) as u8;
    }

    #[inline]
    pub(super) fn write_mkey_context_bind(
        &mut self,
        mw_rkey: u32,
        pdn: u32,
        access_flags: u8,
        start_addr: u64,
        length: u64,
        klm_octwords: u32,
    ) {
        unsafe {
            write_mkey_context_seg_bind(
                self.wqe_ptr.add(self.offset),
                mw_rkey,
                self.sq.sqn,
                pdn,
                access_flags,
                start_addr,
                length,
                klm_octwords,
            );
        }
        self.offset += MKEY_CONTEXT_SEG_SIZE;
        self.ds_count += (MKEY_CONTEXT_SEG_SIZE / 16) as u8;
    }

    #[inline]
    pub(super) fn write_klm_seg_padded(&mut self, byte_count: u32, lkey: u32, address: u64) {
        unsafe {
            write_umr_klm_seg_padded(
                self.wqe_ptr.add(self.offset),
                byte_count,
                lkey,
                address,
            );
        }
        self.offset += UMR_KLM_SEG_PADDED_SIZE;
        self.ds_count += (UMR_KLM_SEG_PADDED_SIZE / 16) as u8;
    }

    #[inline]
    pub(super) fn write_mkey_context_invalidate(&mut self, mkey: u32) {
        unsafe {
            write_mkey_context_seg_invalidate(self.wqe_ptr.add(self.offset), mkey);
        }
        self.offset += MKEY_CONTEXT_SEG_SIZE;
        self.ds_count += (MKEY_CONTEXT_SEG_SIZE / 16) as u8;
    }

    #[inline]
    pub(super) fn finish_internal(self) -> io::Result<WqeHandle> {
        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let slots_to_end = self.sq.slots_to_end();

        if wqebb_cnt > slots_to_end && slots_to_end < self.sq.wqe_cnt {
            return self.finish_with_wrap_around(wqebb_cnt, slots_to_end);
        }

        unsafe {
            update_ctrl_seg_ds_cnt(self.wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                set_ctrl_seg_completion_flag(self.wqe_ptr);
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
            update_ctrl_seg_wqe_idx(new_wqe_ptr, new_wqe_idx);
            update_ctrl_seg_ds_cnt(new_wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                set_ctrl_seg_completion_flag(new_wqe_ptr);
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
    pub fn send(mut self, flags: WqeFlags) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
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
    pub fn send_imm(mut self, flags: WqeFlags, imm: u32) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_av(self.av);
        Ok(SendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with Invalidate WQE.
    ///
    /// This operation sends data to the remote side and also requests the remote
    /// side to invalidate the specified rkey (typically a Memory Window).
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `inv_rkey` - The rkey to invalidate at the remote side
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_with_invalidate(mut self, flags: WqeFlags, inv_rkey: u32) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // inv_rkey is placed in the imm field of the control segment
        self.core.write_ctrl(WqeOpcode::SendInval, flags, inv_rkey);
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
    pub fn write(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> io::Result<WriteWqeBuilder<'a, Entry, NoData>> {
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
    pub fn write_imm(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<WriteWqeBuilder<'a, Entry, NoData>> {
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
    pub fn read(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> io::Result<ReadWqeBuilder<'a, Entry, NoData>> {
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
    pub fn cas(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
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
    pub fn fetch_add(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
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
    // Masked Atomic operations (32-bit and 64-bit)
    // -------------------------------------------------------------------------

    /// Start building a Masked Compare-and-Swap (32-bit) WQE.
    ///
    /// The masked CAS operation compares (remote_value & compare_mask) with
    /// (compare & compare_mask). If equal, the remote value is updated to:
    /// (remote_value & ~swap_mask) | (swap & swap_mask)
    ///
    /// The original remote value is returned to the local buffer.
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `remote_addr` - Remote address (must be 4-byte aligned)
    /// * `rkey` - Remote memory key
    /// * `params` - Masked CAS parameters (swap, compare, swap_mask, compare_mask)
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **4 bytes** for the return buffer.
    /// Using 8 bytes will result in incorrect return values.
    ///
    /// **Return Value Byte Order**: The returned value (original remote value) is
    /// written in **big-endian** format. Use `u32::from_be()` to convert it to
    /// native byte order.
    ///
    /// **Remote Memory Byte Order**: The remote memory is accessed in **native**
    /// byte order. Write values to remote memory without byte swapping.
    ///
    /// # Example
    /// ```ignore
    /// // Read returned value
    /// let returned = u32::from_be(local_buf.read_u32(0));
    /// // Read remote value (native byte order)
    /// let remote = remote_buf.read_u32(0);
    /// ```
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_cas_32(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        params: MaskedCasParams<u32>,
    ) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_32(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 8 for 32-bit extended atomics (UCX: (8) | (log2(4) - 2) = 8)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 8);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_32(params.swap, params.compare, params.swap_mask, params.compare_mask);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Fetch-and-Add (32-bit) WQE.
    ///
    /// The masked FA operation adds `add` to the masked portion of the remote value:
    /// result = remote_value
    /// remote_value = (remote_value + add) & field_mask | (remote_value & ~field_mask)
    ///
    /// The original remote value is returned to the local buffer.
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `remote_addr` - Remote address (must be 4-byte aligned)
    /// * `rkey` - Remote memory key
    /// * `add` - Value to add (32-bit)
    /// * `field_mask` - Mask defining which bits are affected by the addition.
    ///   Use `0` to treat the entire value as a single field (normal addition).
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **4 bytes** for the return buffer.
    /// Using 8 bytes will result in incorrect return values.
    ///
    /// **Return Value Byte Order**: The returned value (original remote value) is
    /// written in **big-endian** format. Use `u32::from_be()` to convert it to
    /// native byte order.
    ///
    /// **Remote Memory Byte Order**: The remote memory is accessed in **native**
    /// byte order. Write values to remote memory without byte swapping.
    ///
    /// # Example
    /// ```ignore
    /// // Read returned value
    /// let returned = u32::from_be(local_buf.read_u32(0));
    /// // Read remote value (native byte order)
    /// let remote = remote_buf.read_u32(0);
    /// ```
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_fa_32(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u32,
        field_mask: u32,
    ) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_32(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 8 for 32-bit extended atomics (UCX: (8) | (log2(4) - 2) = 8)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 8);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_32(add, field_mask);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Compare-and-Swap (64-bit) WQE.
    ///
    /// The masked CAS operation compares (remote_value & compare_mask) with
    /// (compare & compare_mask). If equal, the remote value is updated to:
    /// (remote_value & ~swap_mask) | (swap & swap_mask)
    ///
    /// The original remote value is returned to the local buffer.
    ///
    /// Note: 64-bit masked CAS uses 2 WQEBB segments (32 bytes for atomic data).
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `remote_addr` - Remote address (must be 8-byte aligned)
    /// * `rkey` - Remote memory key
    /// * `params` - Masked CAS parameters (swap, compare, swap_mask, compare_mask)
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **8 bytes** for the return buffer.
    ///
    /// **Return Value Byte Order**: Unlike 32-bit masked atomics, the returned value
    /// is written in **native** byte order. Read it directly without byte swapping.
    ///
    /// **Remote Memory Byte Order**: The remote memory is accessed in **native**
    /// byte order. Write values to remote memory without byte swapping.
    ///
    /// # Example
    /// ```ignore
    /// // Read returned value (native byte order)
    /// let returned = local_buf.read_u64(0);
    /// // Read remote value (native byte order)
    /// let remote = remote_buf.read_u64(0);
    /// ```
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_cas_64(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        params: MaskedCasParams<u64>,
    ) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_64_cas(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 9 for 64-bit extended atomics (UCX: (8) | (log2(8) - 2) = 9)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 9);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_64(params.swap, params.compare, params.swap_mask, params.compare_mask);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Fetch-and-Add (64-bit) WQE.
    ///
    /// The masked FA operation adds `add` to the masked portion of the remote value:
    /// result = remote_value
    /// remote_value = (remote_value + add) & field_mask | (remote_value & ~field_mask)
    ///
    /// The original remote value is returned to the local buffer.
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `remote_addr` - Remote address (must be 8-byte aligned)
    /// * `rkey` - Remote memory key
    /// * `add` - Value to add (64-bit)
    /// * `field_mask` - Mask defining which bits are affected by the addition.
    ///   Use `0` to treat the entire value as a single field (normal addition).
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **8 bytes** for the return buffer.
    ///
    /// **Return Value Byte Order**: Unlike 32-bit masked atomics, the returned value
    /// is written in **native** byte order. Read it directly without byte swapping.
    ///
    /// **Remote Memory Byte Order**: The remote memory is accessed in **native**
    /// byte order. Write values to remote memory without byte swapping.
    ///
    /// # Example
    /// ```ignore
    /// // Read returned value (native byte order)
    /// let returned = local_buf.read_u64(0);
    /// // Read remote value (native byte order)
    /// let remote = remote_buf.read_u64(0);
    /// ```
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_fa_64(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u64,
        field_mask: u64,
    ) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_64_fa(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 9 for 64-bit extended atomics (UCX: (8) | (log2(8) - 2) = 9)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 9);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_64(add, field_mask);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Masked Atomic operations (128-bit)
    // -------------------------------------------------------------------------

    /// Start building a Masked Compare-and-Swap (128-bit) WQE.
    ///
    /// The masked CAS operation compares (remote_value & compare_mask) with
    /// (compare & compare_mask). If equal, the remote value is updated to:
    /// (remote_value & ~swap_mask) | (swap & swap_mask)
    ///
    /// The original remote value is returned to the local buffer.
    ///
    /// Note: 128-bit masked CAS uses 4 WQEBB segments (64 bytes for atomic data).
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `remote_addr` - Remote address (must be 16-byte aligned)
    /// * `rkey` - Remote memory key
    /// * `params` - Masked CAS parameters (swap, compare, swap_mask, compare_mask)
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **16 bytes** for the return buffer.
    ///
    /// **Return Value Byte Order**: The returned value is expected to be in **native**
    /// byte order. Read it directly without byte swapping.
    ///
    /// **Remote Memory Byte Order**: The remote memory is accessed in **native**
    /// byte order. Write values to remote memory without byte swapping.
    ///
    /// **Device Support**: 128-bit atomics require specific hardware support.
    /// Check `atomic_size_qp` capability bit 4 (0x10).
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_cas_128(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        params: MaskedCasParams<u128>,
    ) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_128_cas(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 10 for 128-bit extended atomics (8 | (log2(16) - 2) = 8 | 2 = 10)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 10);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_128(params.swap, params.compare, params.swap_mask, params.compare_mask);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Fetch-and-Add (128-bit) WQE.
    ///
    /// The masked FA operation adds `add` to the masked portion of the remote value:
    /// result = remote_value
    /// remote_value = (remote_value + add) & field_boundary | (remote_value & ~field_boundary)
    ///
    /// The original remote value is returned to the local buffer.
    ///
    /// Note: 128-bit masked FA uses 2 WQEBB segments (32 bytes for atomic data).
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `remote_addr` - Remote address (must be 16-byte aligned)
    /// * `rkey` - Remote memory key
    /// * `add` - Value to add (128-bit)
    /// * `field_boundary` - Mask defining which bits are affected by the addition.
    ///   Use `0` to treat the entire value as a single field (normal addition).
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **16 bytes** for the return buffer.
    ///
    /// **Return Value Byte Order**: The returned value is expected to be in **native**
    /// byte order. Read it directly without byte swapping.
    ///
    /// **Remote Memory Byte Order**: The remote memory is accessed in **native**
    /// byte order. Write values to remote memory without byte swapping.
    ///
    /// **Device Support**: 128-bit atomics require specific hardware support.
    /// Check `atomic_size_qp` capability bit 4 (0x10).
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_fa_128(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u128,
        field_boundary: u128,
    ) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_128_fa(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 10 for 128-bit extended atomics (8 | (log2(16) - 2) = 8 | 2 = 10)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 10);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_128(add, field_boundary);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: WqeFlags) -> io::Result<NopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        Ok(NopWqeBuilder { core: self.core })
    }

    // -------------------------------------------------------------------------
    // Memory Window operations
    // -------------------------------------------------------------------------

    /// Start building a Type 2 MW bind WQE using UMR.
    ///
    /// This operation binds a Memory Window to a Memory Region, allowing remote
    /// access through the MW's rkey.
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `mw` - The Memory Window to bind
    /// * `bind_info` - Binding information (MR, address, length, access)
    ///
    /// # Important
    /// After successful completion, the MW's rkey can be used for remote RDMA
    /// operations on the bound memory region.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn bind_mw(
        mut self,
        flags: WqeFlags,
        mw: &MemoryWindow,
        bind_info: MwBindInfo<'_>,
    ) -> io::Result<BindMwWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_umr();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        // Get current rkey and advance to a new rkey tag for this bind
        let orig_rkey = unsafe { mw.rkey() };
        let new_rkey = unsafe { mw.inc_rkey() };

        // Convert AccessFlags to MLX5 mkey access flags
        let access = convert_access_flags(bind_info.access);

        // KLM octowords in UMR ctrl seg: get_klm_octo(1) = ALIGN(1, 3) / 2 = 4
        let klm_octwords: u16 = 4;

        // translations_octword_size in mkey context: same as get_klm_octo(1)
        let translations_octwords: u32 = 4;

        // opmod = 0 for UMR bind
        let pdn = bind_info.mr.pdn();
        eprintln!("[DEBUG bind_mw] sqn={}, pdn={}, mw_rkey=0x{:x}, new_rkey=0x{:x}, mr_lkey=0x{:x}, addr=0x{:x}, len={}",
            self.core.sq.sqn, pdn, orig_rkey, new_rkey, bind_info.mr.lkey(), bind_info.addr, bind_info.length);
        self.core.write_ctrl_with_opmod(WqeOpcode::Umr, flags, orig_rkey, 0);
        self.core.write_umr_ctrl_bind(klm_octwords);
        self.core.write_mkey_context_bind(
            new_rkey,
            pdn,
            access,
            bind_info.addr,
            bind_info.length,
            translations_octwords,
        );
        // Write the KLM segment with the underlying MR's information
        // Padded to 64 bytes for hardware alignment requirements
        self.core.write_klm_seg_padded(
            bind_info.length as u32,
            bind_info.mr.lkey(),
            bind_info.addr,
        );

        Ok(BindMwWqeBuilder { core: self.core })
    }

    /// Start building a Local Invalidate WQE.
    ///
    /// This operation invalidates an mkey (Memory Window or Memory Region),
    /// making it unusable for further RDMA operations.
    ///
    /// # Arguments
    /// * `flags` - WQE flags
    /// * `mkey` - The mkey to invalidate (MW's rkey or MR's rkey)
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn local_invalidate(
        mut self,
        flags: WqeFlags,
        mkey: u32,
    ) -> io::Result<LocalInvalWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_local_inval();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        // opmod = 0 for local invalidate
        self.core.write_ctrl_with_opmod(WqeOpcode::Umr, flags, mkey, 0);
        self.core.write_umr_ctrl_invalidate();
        self.core.write_mkey_context_invalidate(mkey);

        Ok(LocalInvalWqeBuilder { core: self.core })
    }
}

/// Convert AccessFlags to MLX5 mkey access flags.
#[inline]
fn convert_access_flags(flags: AccessFlags) -> u8 {
    let mut result = mkey_access_flags::MLX5_MKEY_LOCAL_READ;
    if flags.contains(AccessFlags::LOCAL_WRITE) {
        result |= mkey_access_flags::MLX5_MKEY_LOCAL_WRITE;
    }
    if flags.contains(AccessFlags::REMOTE_READ) {
        result |= mkey_access_flags::MLX5_MKEY_REMOTE_READ;
    }
    if flags.contains(AccessFlags::REMOTE_WRITE) {
        result |= mkey_access_flags::MLX5_MKEY_REMOTE_WRITE;
    }
    if flags.contains(AccessFlags::REMOTE_ATOMIC) {
        result |= mkey_access_flags::MLX5_MKEY_REMOTE_ATOMIC;
    }
    result
}

// =============================================================================
// Bind MW WQE Builder
// =============================================================================

/// Bind MW WQE builder.
///
/// This builder is complete and ready to finish.
#[must_use = "WQE builder must be finished"]
pub struct BindMwWqeBuilder<'a, Entry> {
    core: WqeCore<'a, Entry>,
}

impl<'a, Entry> BindMwWqeBuilder<'a, Entry> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        // Save reference before finish_internal consumes self.core
        let sq = self.core.sq;
        let result = self.core.finish_internal();
        // Set fence for next WQE (required after UMR operations)
        sq.next_fence.set(WqeFlags::INITIATOR_SMALL_FENCE.bits() as u8);
        result
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        // Save reference before finish_internal consumes self.core
        let sq = self.core.sq;
        let result = self.core.finish_internal();
        // Set fence for next WQE (required after UMR operations)
        sq.next_fence.set(WqeFlags::INITIATOR_SMALL_FENCE.bits() as u8);
        result
    }
}

// =============================================================================
// Local Invalidate WQE Builder
// =============================================================================

/// Local Invalidate WQE builder.
///
/// This builder is complete and ready to finish.
#[must_use = "WQE builder must be finished"]
pub struct LocalInvalWqeBuilder<'a, Entry> {
    core: WqeCore<'a, Entry>,
}

impl<'a, Entry> LocalInvalWqeBuilder<'a, Entry> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        // Save reference before finish_internal consumes self.core
        let sq = self.core.sq;
        let result = self.core.finish_internal();
        // Set fence for next WQE (required after UMR operations)
        sq.next_fence.set(WqeFlags::INITIATOR_SMALL_FENCE.bits() as u8);
        result
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        // Save reference before finish_internal consumes self.core
        let sq = self.core.sq;
        let result = self.core.finish_internal();
        // Set fence for next WQE (required after UMR operations)
        sq.next_fence.set(WqeFlags::INITIATOR_SMALL_FENCE.bits() as u8);
        result
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
const RQ_WQE_SIZE: usize = DATA_SEG_SIZE;

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
            write_data_seg(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
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
/// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
/// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
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
    pub fn send(self, flags: WqeFlags) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: WqeFlags, imm: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_rdma(remote_addr, rkey)?;
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
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
        if batch.offset + CTRL_SEG_SIZE > BLUEFLAME_BUFFER_SIZE {
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
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        // Apply fence from prior UMR operation (bind_mw/local_invalidate)
        let fence = self.batch.sq.next_fence.get();
        self.batch.sq.next_fence.set(0);

        let wqe_idx = self.batch.sq.pi.get();
        // Combine passed flags with fence bits
        let flags_with_fence = WqeFlags::from_bits_truncate(flags.bits() | (fence as u16));
        unsafe {
            write_ctrl_seg(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags_with_fence,
                imm,
            );
        }
        self.offset += CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < RDMA_SEG_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            write_rdma_seg(self.batch.buffer.as_mut_ptr().add(self.offset), addr, rkey);
        }
        self.offset += RDMA_SEG_SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DATA_SEG_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            write_data_seg(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DATA_SEG_SIZE;
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
            write_inline_header(ptr, data.len() as u32);
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
            update_ctrl_seg_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                set_ctrl_seg_completion_flag(
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
/// bf.wqe()?.send(WqeFlags::empty()).inline(&data)?.finish()?;
/// bf.wqe()?.send(WqeFlags::empty()).inline(&data)?.finish()?;
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
    pub fn send(self, flags: WqeFlags) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_av_roce()?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: WqeFlags, imm: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_av_roce()?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = RoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_av_roce()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(RoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<RoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
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
        if batch.offset + CTRL_SEG_SIZE > BLUEFLAME_BUFFER_SIZE {
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
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        // Apply fence from prior UMR operation (bind_mw/local_invalidate)
        let fence = self.batch.sq.next_fence.get();
        self.batch.sq.next_fence.set(0);

        let wqe_idx = self.batch.sq.pi.get();
        // Combine passed flags with fence bits
        let flags_with_fence = WqeFlags::from_bits_truncate(flags.bits() | (fence as u16));
        unsafe {
            write_ctrl_seg(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags_with_fence,
                imm,
            );
        }
        self.offset += CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_av_roce(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < ADDRESS_VECTOR_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            // For RC QP RoCE, dc_key and dctn are not used (set to 0)
            write_address_vector_roce(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                0,
                self.batch.grh,
            );
        }
        self.offset += ADDRESS_VECTOR_SIZE;
        self.ds_count += (ADDRESS_VECTOR_SIZE / 16) as u8;
        Ok(())
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < RDMA_SEG_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            write_rdma_seg(self.batch.buffer.as_mut_ptr().add(self.offset), addr, rkey);
        }
        self.offset += RDMA_SEG_SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DATA_SEG_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            write_data_seg(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DATA_SEG_SIZE;
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
            write_inline_header(ptr, data.len() as u32);
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
            update_ctrl_seg_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                set_ctrl_seg_completion_flag(
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
