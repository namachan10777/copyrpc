//! DCI WQE Builder types.

use std::io;
use std::marker::PhantomData;

use crate::types::GrhAttr;
use crate::wqe::{
    ADDRESS_VECTOR_SIZE, ATOMIC_SEG_SIZE, CTRL_SEG_SIZE, DATA_SEG_SIZE, HasData, NoData,
    MASKED_ATOMIC_SEG32_SIZE, MASKED_ATOMIC_SEG64_SIZE_CAS, MASKED_ATOMIC_SEG64_SIZE_FA,
    MASKED_ATOMIC_SEG128_SIZE_CAS, MASKED_ATOMIC_SEG128_SIZE_FA,
    OrderedWqeTable, RDMA_SEG_SIZE, SubmissionError, WqeFlags, WQEBB_SIZE,
    WqeHandle, WqeOpcode, calc_wqebb_cnt,
    set_ctrl_seg_completion_flag, update_ctrl_seg_ds_cnt, update_ctrl_seg_wqe_idx,
    write_address_vector_ib, write_address_vector_roce, write_atomic_seg_cas, write_atomic_seg_fa,
    write_ctrl_seg, write_data_seg, write_inline_header,
    write_masked_atomic_seg32_cas, write_masked_atomic_seg32_fa,
    write_masked_atomic_seg64_cas, write_masked_atomic_seg64_fa,
    write_masked_atomic_seg128_cas, write_masked_atomic_seg128_fa,
    write_rdma_seg,
};

use super::{DciIb, DciRoCE, DciSendQueueState};

// =============================================================================
// Maximum WQEBB Calculation Functions for DCI
// =============================================================================

/// Calculate the padded inline data size (16-byte aligned).
///
/// inline_padded = ((4 + max_inline_data + 15) & !15)
#[inline]
fn calc_inline_padded(max_inline_data: u32) -> usize {
    ((4 + max_inline_data as usize) + 15) & !15
}

/// Calculate maximum WQEBB count for DCI SEND operation.
///
/// Layout: ctrl(16) + AV(48) + inline_padded
#[inline]
fn calc_max_wqebb_send(max_inline_data: u32) -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI WRITE operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + inline_padded
#[inline]
fn calc_max_wqebb_write(max_inline_data: u32) -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI READ operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + sge(16)
#[inline]
fn calc_max_wqebb_read() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Atomic operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + atomic(16) + sge(16)
#[inline]
fn calc_max_wqebb_atomic() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + ATOMIC_SEG_SIZE + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI NOP operation.
///
/// Layout: ctrl(16) + AV(48)
#[inline]
fn calc_max_wqebb_nop() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Masked Atomic 32-bit operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + masked_atomic_32(16) + sge(16)
#[inline]
fn calc_max_wqebb_masked_atomic_32() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG32_SIZE + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Masked Atomic 64-bit CAS operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + masked_atomic_64_cas(32) + sge(16)
#[inline]
fn calc_max_wqebb_masked_atomic_64_cas() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG64_SIZE_CAS + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Masked Atomic 64-bit FA operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + masked_atomic_64_fa(16) + sge(16)
#[inline]
fn calc_max_wqebb_masked_atomic_64_fa() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG64_SIZE_FA + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Masked Atomic 128-bit CAS operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + masked_atomic_128_cas(64) + sge(16)
/// Note: 128-bit CAS uses 4 segments (64 bytes total for atomic data)
#[inline]
fn calc_max_wqebb_masked_atomic_128_cas() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG128_SIZE_CAS + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Masked Atomic 128-bit FA operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + masked_atomic_128_fa(32) + sge(16)
/// Note: 128-bit FA uses 2 segments (32 bytes total for atomic data)
#[inline]
fn calc_max_wqebb_masked_atomic_128_fa() -> u16 {
    let size = CTRL_SEG_SIZE + ADDRESS_VECTOR_SIZE + RDMA_SEG_SIZE + MASKED_ATOMIC_SEG128_SIZE_FA + DATA_SEG_SIZE;
    calc_wqebb_cnt(size)
}

/// Pending AV info to be written after ctrl segment.
enum PendingAv<'a> {
    None,
    Ib { dc_key: u64, dctn: u32, dlid: u16 },
    RoCE { dc_key: u64, dctn: u32, grh: &'a GrhAttr },
}

/// Internal WQE builder core for DCI that handles direct buffer writes.
struct DciWqeCore<'a, Entry> {
    sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    signaled: bool,
    entry: Option<Entry>,
    pending_av: PendingAv<'a>,
}

impl<'a, Entry> DciWqeCore<'a, Entry> {
    #[inline]
    fn new(
        sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
        entry: Option<Entry>,
    ) -> Self {
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
            pending_av: PendingAv::None,
        }
    }

    #[inline]
    fn available(&self) -> u16 {
        self.sq.available()
    }

    #[inline]
    fn max_inline_data(&self) -> u32 {
        self.sq.max_inline_data()
    }

    /// Store AV parameters for IB transport to be written later.
    #[inline]
    fn set_av_ib(&mut self, dc_key: u64, dctn: u32, dlid: u16) {
        self.pending_av = PendingAv::Ib { dc_key, dctn, dlid };
    }

    /// Store AV parameters for RoCE transport to be written later.
    #[inline]
    fn set_av_roce(&mut self, dc_key: u64, dctn: u32, grh: &'a GrhAttr) {
        self.pending_av = PendingAv::RoCE { dc_key, dctn, grh };
    }

    /// Write the pending AV segment (must be called after write_ctrl).
    #[inline]
    fn write_pending_av(&mut self) {
        match self.pending_av {
            PendingAv::None => {}
            PendingAv::Ib { dc_key, dctn, dlid } => {
                unsafe {
                    write_address_vector_ib(self.wqe_ptr.add(self.offset), dc_key, dctn, dlid);
                }
                self.offset += ADDRESS_VECTOR_SIZE;
                self.ds_count += 3;
            }
            PendingAv::RoCE { dc_key, dctn, grh } => {
                unsafe {
                    write_address_vector_roce(self.wqe_ptr.add(self.offset), dc_key, dctn, grh);
                }
                self.offset += ADDRESS_VECTOR_SIZE;
                self.ds_count += 3;
            }
        }
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        self.write_ctrl_with_opmod(opcode, flags, imm, 0)
    }

    #[inline]
    fn write_ctrl_with_opmod(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32, opmod: u8) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            write_ctrl_seg(
                self.wqe_ptr,
                opmod,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags,
                imm,
            );
        }
        self.offset = CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) {
        unsafe {
            write_rdma_seg(self.wqe_ptr.add(self.offset), addr, rkey);
        }
        self.offset += RDMA_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) {
        unsafe {
            write_data_seg(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DATA_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) {
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
    fn write_atomic_cas(&mut self, swap: u64, compare: u64) {
        unsafe {
            write_atomic_seg_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += ATOMIC_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_atomic_fa(&mut self, add_value: u64) {
        unsafe {
            write_atomic_seg_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += ATOMIC_SEG_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_masked_atomic_cas_32(
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
    fn write_masked_atomic_fa_32(&mut self, add: u32, field_mask: u32) {
        unsafe {
            write_masked_atomic_seg32_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MASKED_ATOMIC_SEG32_SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_masked_atomic_cas_64(
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
    fn write_masked_atomic_fa_64(&mut self, add: u64, field_mask: u64) {
        unsafe {
            write_masked_atomic_seg64_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MASKED_ATOMIC_SEG64_SIZE_FA;
        self.ds_count += 1;
    }

    #[inline]
    fn write_masked_atomic_cas_128(
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
    fn write_masked_atomic_fa_128(&mut self, add: u128, field_boundary: u128) {
        // 128-bit masked FA uses 2 segments (32 bytes total)
        unsafe {
            write_masked_atomic_seg128_fa(self.wqe_ptr.add(self.offset), add, field_boundary);
        }
        self.offset += MASKED_ATOMIC_SEG128_SIZE_FA;
        self.ds_count += 2; // 2 segments
    }

    #[inline]
    fn finish_internal(self) -> io::Result<WqeHandle> {
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
// Simplified DCI SQ WQE Entry Point (IB)
// =============================================================================

/// DCI SQ WQE entry point for IB transport.
///
/// DC info (dc_key, dctn, dlid) is passed to entry point method.
#[must_use = "WQE builder must be finished"]
pub struct DciSqWqeEntryPoint<'a, Entry> {
    core: DciWqeCore<'a, Entry>,
}

impl<'a, Entry> DciSqWqeEntryPoint<'a, Entry> {
    /// Create a new DCI entry point with IB AV.
    #[inline]
    pub(super) fn new(
        sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
        dc_key: u64,
        dctn: u32,
        dlid: u16,
    ) -> Self {
        let mut core = DciWqeCore::new(sq, None);
        // Store AV parameters for later - AV is written AFTER ctrl segment
        core.set_av_ib(dc_key, dctn, dlid);
        Self { core }
    }

    // -------------------------------------------------------------------------
    // SEND operations
    // -------------------------------------------------------------------------

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: WqeFlags) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(mut self, flags: WqeFlags, imm: u32) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // WRITE operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA WRITE WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write_imm(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // READ operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA READ WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn read(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> io::Result<DciReadWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_read();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciReadWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Atomic operations
    // -------------------------------------------------------------------------

    /// Start building an Atomic Compare-and-Swap WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn cas(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_cas(swap, compare);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an Atomic Fetch-and-Add WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn fetch_add(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_fa(add_value);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
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
    /// * `swap` - Value to swap in (32-bit)
    /// * `compare` - Value to compare against (32-bit)
    /// * `swap_mask` - Mask for swap operation
    /// * `compare_mask` - Mask for compare operation
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
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_cas_32(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        swap: u32,
        compare: u32,
        swap_mask: u32,
        compare_mask: u32,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_32();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 8 for 32-bit extended atomics (UCX: (8) | (log2(4) - 2) = 8)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 8);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_32(swap, compare, swap_mask, compare_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
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
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_fa_32(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u32,
        field_mask: u32,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_32();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 8 for 32-bit extended atomics (UCX: (8) | (log2(4) - 2) = 8)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 8);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_32(add, field_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
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
    /// * `swap` - Value to swap in (64-bit)
    /// * `compare` - Value to compare against (64-bit)
    /// * `swap_mask` - Mask for swap operation
    /// * `compare_mask` - Mask for compare operation
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
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_cas_64(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        swap: u64,
        compare: u64,
        swap_mask: u64,
        compare_mask: u64,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_64_cas();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 9 for 64-bit extended atomics (UCX: (8) | (log2(8) - 2) = 9)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 9);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_64(swap, compare, swap_mask, compare_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
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
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_fa_64(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u64,
        field_mask: u64,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_64_fa();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 9 for 64-bit extended atomics (UCX: (8) | (log2(8) - 2) = 9)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 9);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_64(add, field_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
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
    /// * `swap` - Value to swap in (128-bit)
    /// * `compare` - Value to compare against (128-bit)
    /// * `swap_mask` - Mask for swap operation
    /// * `compare_mask` - Mask for compare operation
    ///
    /// # Important Notes
    ///
    /// **SGE Size**: The SGE must specify exactly **16 bytes** for the return buffer.
    ///
    /// **Return Value Byte Order**: The returned value is expected to be in **native**
    /// byte order. Read it directly without byte swapping.
    ///
    /// **Device Support**: 128-bit atomics require specific hardware support.
    /// Check `atomic_size_dc` capability bit 4 (0x10).
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn masked_cas_128(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        swap: u128,
        compare: u128,
        swap_mask: u128,
        compare_mask: u128,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_128_cas();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 10 for 128-bit extended atomics (8 | (log2(16) - 2) = 8 | 2 = 10)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 10);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_128(swap, compare, swap_mask, compare_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
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
    /// **Device Support**: 128-bit atomics require specific hardware support.
    /// Check `atomic_size_dc` capability bit 4 (0x10).
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
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_128_fa();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 10 for 128-bit extended atomics (8 | (log2(16) - 2) = 8 | 2 = 10)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 10);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_128(add, field_boundary);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: WqeFlags) -> io::Result<DciNopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        self.core.write_pending_av();
        Ok(DciNopWqeBuilder { core: self.core })
    }
}

// =============================================================================
// Simplified DCI SQ WQE Entry Point (RoCE)
// =============================================================================

/// DCI SQ WQE entry point for RoCE transport.
#[must_use = "WQE builder must be finished"]
pub struct DciRoceSqWqeEntryPoint<'a, Entry> {
    core: DciWqeCore<'a, Entry>,
}

impl<'a, Entry> DciRoceSqWqeEntryPoint<'a, Entry> {
    /// Create a new DCI entry point with RoCE AV.
    #[inline]
    pub(super) fn new(
        sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
        dc_key: u64,
        dctn: u32,
        grh: &'a GrhAttr,
    ) -> Self {
        let mut core = DciWqeCore::new(sq, None);
        // Store AV parameters for later - AV is written AFTER ctrl segment
        core.set_av_roce(dc_key, dctn, grh);
        Self { core }
    }

    // -------------------------------------------------------------------------
    // SEND operations
    // -------------------------------------------------------------------------

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: WqeFlags) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(mut self, flags: WqeFlags, imm: u32) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // WRITE operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA WRITE WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write_imm(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // READ operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA READ WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn read(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> io::Result<DciReadWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_read();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciReadWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Atomic operations
    // -------------------------------------------------------------------------

    /// Start building an Atomic Compare-and-Swap WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn cas(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_cas(swap, compare);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an Atomic Fetch-and-Add WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn fetch_add(mut self, flags: WqeFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_fa(add_value);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Masked Atomic operations (32-bit and 64-bit) - RoCE
    // -------------------------------------------------------------------------

    /// Start building a Masked Compare-and-Swap (32-bit) WQE.
    ///
    /// See the IB version for full documentation.
    ///
    /// **Important**: SGE must be 4 bytes. Return value is in big-endian.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn masked_cas_32(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        swap: u32,
        compare: u32,
        swap_mask: u32,
        compare_mask: u32,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_32();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 8 for 32-bit extended atomics (UCX: (8) | (log2(4) - 2) = 8)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 8);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_32(swap, compare, swap_mask, compare_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Fetch-and-Add (32-bit) WQE.
    ///
    /// See the IB version for full documentation.
    ///
    /// **Important**: SGE must be 4 bytes. Return value is in big-endian.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn masked_fa_32(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u32,
        field_mask: u32,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_32();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 8 for 32-bit extended atomics (UCX: (8) | (log2(4) - 2) = 8)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 8);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_32(add, field_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Compare-and-Swap (64-bit) WQE.
    ///
    /// See the IB version for full documentation.
    ///
    /// **Important**: SGE must be 8 bytes. Return value is in native byte order.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn masked_cas_64(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        swap: u64,
        compare: u64,
        swap_mask: u64,
        compare_mask: u64,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_64_cas();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 9 for 64-bit extended atomics (UCX: (8) | (log2(8) - 2) = 9)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 9);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_64(swap, compare, swap_mask, compare_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Fetch-and-Add (64-bit) WQE.
    ///
    /// See the IB version for full documentation.
    ///
    /// **Important**: SGE must be 8 bytes. Return value is in native byte order.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn masked_fa_64(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u64,
        field_mask: u64,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_64_fa();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 9 for 64-bit extended atomics (UCX: (8) | (log2(8) - 2) = 9)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 9);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_64(add, field_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Masked Atomic operations (128-bit)
    // -------------------------------------------------------------------------

    /// Start building a Masked Compare-and-Swap (128-bit) WQE.
    ///
    /// See the IB version for full documentation.
    ///
    /// **Important**: SGE must be 16 bytes. Return value is in native byte order.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn masked_cas_128(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        swap: u128,
        compare: u128,
        swap_mask: u128,
        compare_mask: u128,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_128_cas();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 10 for 128-bit extended atomics (8 | (log2(16) - 2) = 8 | 2 = 10)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 10);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_cas_128(swap, compare, swap_mask, compare_mask);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a Masked Fetch-and-Add (128-bit) WQE.
    ///
    /// See the IB version for full documentation.
    ///
    /// **Important**: SGE must be 16 bytes. Return value is in native byte order.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn masked_fa_128(
        mut self,
        flags: WqeFlags,
        remote_addr: u64,
        rkey: u32,
        add: u128,
        field_boundary: u128,
    ) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_masked_atomic_128_fa();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        // opmod = 10 for 128-bit extended atomics (8 | (log2(16) - 2) = 8 | 2 = 10)
        self.core.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 10);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_masked_atomic_fa_128(add, field_boundary);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: WqeFlags) -> io::Result<DciNopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        self.core.write_pending_av();
        Ok(DciNopWqeBuilder { core: self.core })
    }
}

// =============================================================================
// DCI Send WQE Builder
// =============================================================================

/// DCI SEND WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct DciSendWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> DciSendWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciSendWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> DciSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        DciSendWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciSendWqeBuilder<'a, Entry, HasData> {
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
// DCI Write WQE Builder
// =============================================================================

/// DCI RDMA WRITE WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct DciWriteWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> DciWriteWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciWriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciWriteWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> DciWriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        DciWriteWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciWriteWqeBuilder<'a, Entry, HasData> {
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
// DCI Read WQE Builder
// =============================================================================

/// DCI RDMA READ WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
/// Note: READ operations do not support inline data.
#[must_use = "WQE builder must be finished"]
pub struct DciReadWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge: NoData state, transitions to HasData.
impl<'a, Entry> DciReadWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciReadWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciReadWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciReadWqeBuilder<'a, Entry, HasData> {
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
// DCI Atomic WQE Builder
// =============================================================================

/// DCI Atomic operation WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
#[must_use = "WQE builder must be finished"]
pub struct DciAtomicWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge: NoData state, transitions to HasData.
impl<'a, Entry> DciAtomicWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry for the result buffer.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciAtomicWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciAtomicWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciAtomicWqeBuilder<'a, Entry, HasData> {
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
// DCI NOP WQE Builder
// =============================================================================

/// DCI NOP WQE builder.
///
/// NOP operations have no data segment, so `finish_*()` methods are
/// available immediately without needing to call `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct DciNopWqeBuilder<'a, Entry> {
    core: DciWqeCore<'a, Entry>,
}

impl<'a, Entry> DciNopWqeBuilder<'a, Entry> {
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
// DCI Entry Points
// =============================================================================

impl<Entry, OnComplete> DciIb<Entry, OnComplete> {
    /// Get a SQ WQE builder for InfiniBand transport.
    ///
    /// # Example
    /// ```ignore
    /// dci.sq_wqe(dc_key, dctn, dlid)?
    ///     .write(WqeFlags::empty(), remote_addr, rkey)
    ///     .sge(local_addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// dci.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe(&mut self, dc_key: u64, dctn: u32, dlid: u16) -> io::Result<DciSqWqeEntryPoint<'_, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(DciSqWqeEntryPoint::new(sq, dc_key, dctn, dlid))
    }
}

impl<Entry, OnComplete> DciRoCE<Entry, OnComplete> {
    /// Get a SQ WQE builder for RoCE transport.
    ///
    /// # Example
    /// ```ignore
    /// dci.sq_wqe(dc_key, dctn, &grh)?
    ///     .write(WqeFlags::empty(), remote_addr, rkey)
    ///     .sge(local_addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// dci.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe<'a>(&'a mut self, dc_key: u64, dctn: u32, grh: &'a GrhAttr) -> io::Result<DciRoceSqWqeEntryPoint<'a, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(DciRoceSqWqeEntryPoint::new(sq, dc_key, dctn, grh))
    }
}

// =============================================================================
// DCI BlueFlame Batch Builder
// =============================================================================

/// BlueFlame buffer size in bytes (256B doorbell window).
const BLUEFLAME_BUFFER_SIZE: usize = 256;

/// BlueFlame WQE batch builder for DCI (InfiniBand transport).
///
/// Allows building multiple WQEs that fit within the BlueFlame buffer (256 bytes).
/// Each WQE is copied to a contiguous buffer and submitted via BlueFlame doorbell
/// when `finish()` is called.
pub struct DciBlueflameWqeBatch<'a, Entry> {
    sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
    dc_key: u64,
    dctn: u32,
    dlid: u16,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> DciBlueflameWqeBatch<'a, Entry> {
    pub(super) fn new(sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>, dc_key: u64, dctn: u32, dlid: u16) -> Self {
        Self {
            sq,
            dc_key,
            dctn,
            dlid,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<DciBlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(DciBlueflameWqeEntryPoint { batch: self })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return;
        }

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

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

/// BlueFlame WQE entry point for DCI (InfiniBand).
#[must_use = "WQE builder must be finished"]
pub struct DciBlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut DciBlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> DciBlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: WqeFlags) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_dc_av_ib()?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: WqeFlags, imm: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_dc_av_ib()?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_dc_av_ib()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        core.write_dc_av_ib()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for DCI BlueFlame WQE construction.
struct DciBlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut DciBlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> DciBlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut DciBlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
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
        let wqe_idx = self.batch.sq.pi.get();
        unsafe {
            write_ctrl_seg(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags,
                imm,
            );
        }
        self.offset += CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_dc_av_ib(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < ADDRESS_VECTOR_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            write_address_vector_ib(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                self.batch.dc_key,
                self.batch.dctn,
                self.batch.dlid,
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

        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// DCI BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct DciBlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: DciBlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> DciBlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(DciBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(DciBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> DciBlueflameWqeBuilder<'b, 'a, Entry, HasData> {
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

impl<Entry, OnComplete> DciIb<Entry, OnComplete> {
    /// Get a BlueFlame batch builder for low-latency WQE submission.
    ///
    /// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// # Example
    /// ```ignore
    /// let mut bf = dci.blueflame_sq_wqe(dc_key, dctn, dlid)?;
    /// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
    /// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
    /// bf.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_sq_wqe(&self, dc_key: u64, dctn: u32, dlid: u16) -> Result<DciBlueflameWqeBatch<'_, Entry>, SubmissionError> {
        let sq = self.sq.as_ref().ok_or(SubmissionError::SqFull)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(DciBlueflameWqeBatch::new(sq, dc_key, dctn, dlid))
    }
}

// =============================================================================
// DCI BlueFlame Batch Builder (RoCE)
// =============================================================================

/// BlueFlame WQE batch builder for DCI (RoCE transport).
///
/// Allows building multiple WQEs that fit within the BlueFlame buffer (256 bytes).
/// Each WQE is copied to a contiguous buffer and submitted via BlueFlame doorbell
/// when `finish()` is called.
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
pub struct DciRoceBlueflameWqeBatch<'a, Entry> {
    sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
    dc_key: u64,
    dctn: u32,
    grh: &'a GrhAttr,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> DciRoceBlueflameWqeBatch<'a, Entry> {
    pub(super) fn new(sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>, dc_key: u64, dctn: u32, grh: &'a GrhAttr) -> Self {
        Self {
            sq,
            dc_key,
            dctn,
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
    pub fn wqe(&mut self) -> Result<DciRoceBlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(DciRoceBlueflameWqeEntryPoint { batch: self })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return;
        }

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

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

/// BlueFlame WQE entry point for DCI (RoCE).
#[must_use = "WQE builder must be finished"]
pub struct DciRoceBlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut DciRoceBlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> DciRoceBlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: WqeFlags) -> Result<DciRoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciRoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_dc_av_roce()?;
        Ok(DciRoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate WQE.
    #[inline]
    pub fn send_imm(self, flags: WqeFlags, imm: u32) -> Result<DciRoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciRoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_dc_av_roce()?;
        Ok(DciRoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: WqeFlags, remote_addr: u64, rkey: u32) -> Result<DciRoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciRoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_dc_av_roce()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(DciRoceBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate WQE.
    #[inline]
    pub fn write_imm(self, flags: WqeFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<DciRoceBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciRoceBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        core.write_dc_av_roce()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(DciRoceBlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for DCI BlueFlame WQE construction (RoCE).
struct DciRoceBlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut DciRoceBlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> DciRoceBlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut DciRoceBlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
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
        let wqe_idx = self.batch.sq.pi.get();
        unsafe {
            write_ctrl_seg(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags,
                imm,
            );
        }
        self.offset += CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_dc_av_roce(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < ADDRESS_VECTOR_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            write_address_vector_roce(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                self.batch.dc_key,
                self.batch.dctn,
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

        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// DCI BlueFlame WQE builder with type-state for data segments (RoCE).
#[must_use = "WQE builder must be finished"]
pub struct DciRoceBlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: DciRoceBlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> DciRoceBlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<DciRoceBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(DciRoceBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<DciRoceBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(DciRoceBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> DciRoceBlueflameWqeBuilder<'b, 'a, Entry, HasData> {
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

impl<Entry, OnComplete> DciRoCE<Entry, OnComplete> {
    /// Get a BlueFlame batch builder for low-latency WQE submission (RoCE).
    ///
    /// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// # Example
    /// ```ignore
    /// let mut bf = dci.blueflame_sq_wqe(dc_key, dctn, &grh)?;
    /// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
    /// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
    /// bf.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    #[inline]
    pub fn blueflame_sq_wqe<'a>(&'a self, dc_key: u64, dctn: u32, grh: &'a GrhAttr) -> Result<DciRoceBlueflameWqeBatch<'a, Entry>, SubmissionError> {
        let sq = self.sq.as_ref().ok_or(SubmissionError::SqFull)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(DciRoceBlueflameWqeBatch::new(sq, dc_key, dctn, grh))
    }
}
