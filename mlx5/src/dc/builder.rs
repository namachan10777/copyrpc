//! DCI WQE Builder types.

use std::io;
use std::marker::PhantomData;

use crate::types::GrhAttr;
use crate::wqe::{
    ADDRESS_VECTOR_SIZE, ATOMIC_SEG_SIZE, CTRL_SEG_SIZE, CtrlSegParams, DATA_SEG_SIZE, HasData,
    NoData, OrderedWqeTable, RDMA_SEG_SIZE, SubmissionError, WqeFlags, WQEBB_SIZE, WqeHandle,
    WqeOpcode, calc_wqebb_cnt, set_ctrl_seg_completion_flag, update_ctrl_seg_ds_cnt,
    update_ctrl_seg_wqe_idx, write_address_vector_ib, write_address_vector_roce,
    write_atomic_seg_cas, write_atomic_seg_fa, write_ctrl_seg, write_data_seg,
    write_inline_header, write_rdma_seg,
    emit::bf_finish_sq,
};

use super::{DciIb, DciRoCE, DciSendQueueState};

// =============================================================================
// Maximum WQEBB Calculation Functions for DCI
// =============================================================================

use crate::wqe::calc_inline_padded;

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
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            write_ctrl_seg(
                self.wqe_ptr,
                &CtrlSegParams {
                    opmod: 0,
                    opcode: opcode as u8,
                    wqe_idx: self.wqe_idx,
                    qpn: self.sq.sqn,
                    ds_cnt: 0,
                    flags,
                    imm,
                },
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

use crate::wqe::BLUEFLAME_BUFFER_SIZE;

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

        unsafe {
            bf_finish_sq(
                self.sq.dbrec,
                self.sq.pi.get(),
                self.sq.bf_reg,
                self.sq.bf_size,
                &self.sq.bf_offset,
                &self.buffer,
                self.offset,
            );
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
                &CtrlSegParams {
                    opmod: 0,
                    opcode: opcode as u8,
                    wqe_idx,
                    qpn: self.batch.sq.sqn,
                    ds_cnt: 0,
                    flags,
                    imm,
                },
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

        unsafe {
            bf_finish_sq(
                self.sq.dbrec,
                self.sq.pi.get(),
                self.sq.bf_reg,
                self.sq.bf_size,
                &self.sq.bf_offset,
                &self.buffer,
                self.offset,
            );
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
                &CtrlSegParams {
                    opmod: 0,
                    opcode: opcode as u8,
                    wqe_idx,
                    qpn: self.batch.sq.sqn,
                    ds_cnt: 0,
                    flags,
                    imm,
                },
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
