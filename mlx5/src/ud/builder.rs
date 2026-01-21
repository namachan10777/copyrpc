//! UD WQE Builder types.

use std::cell::Cell;
use std::io;
use std::marker::PhantomData;

use crate::pd::AddressHandle;
use crate::wqe::{
    CTRL_SEG_SIZE, DATA_SEG_SIZE, HasData, NoData, OrderedWqeTable,
    SubmissionError, WqeFlags, WQEBB_SIZE, WqeHandle, WqeOpcode, calc_wqebb_cnt,
    set_ctrl_seg_completion_flag, update_ctrl_seg_ds_cnt, update_ctrl_seg_wqe_idx,
    write_ctrl_seg, write_data_seg, write_inline_header,
};

use super::{UdAddressSeg, UdQpIb, UdRecvQueueState, UdSendQueueState};

// =============================================================================
// Maximum WQEBB Calculation Functions
// =============================================================================

/// Calculate maximum WQEBB count for UD SEND operation.
///
/// Layout: ctrl(16) + ud_addr(48) + inline_data(padded)
#[inline]
fn calc_ud_max_wqebb_send(max_inline_data: u32) -> u16 {
    let inline_padded = ((4 + max_inline_data as usize) + 15) & !15;
    let size = CTRL_SEG_SIZE + UdAddressSeg::SIZE + inline_padded;
    calc_wqebb_cnt(size)
}

/// Internal WQE builder core for UD operations.
struct UdWqeCore<'a, Entry> {
    sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    signaled: bool,
    entry: Option<Entry>,
}

impl<'a, Entry> UdWqeCore<'a, Entry> {
    #[inline]
    fn new(sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>, entry: Option<Entry>) -> Self {
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
    fn available(&self) -> u16 {
        self.sq.available()
    }

    #[inline]
    fn max_inline_data(&self) -> u32 {
        self.sq.max_inline_data()
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
                0,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset = CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_ud_av(&mut self, ah: &AddressHandle, qkey: u32) {
        unsafe {
            UdAddressSeg::write(self.wqe_ptr.add(self.offset), ah, qkey);
        }
        self.offset += UdAddressSeg::SIZE;
        self.ds_count += 3; // 48 bytes = 3 DS
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
// UD SQ WQE Entry Point
// =============================================================================

/// UD SQ WQE entry point.
///
/// Provides verb methods for UD operations (SEND only).
#[must_use = "WQE builder must be finished"]
pub struct UdSqWqeEntryPoint<'a, Entry> {
    core: UdWqeCore<'a, Entry>,
    ah: &'a AddressHandle,
    qkey: u32,
}

impl<'a, Entry> UdSqWqeEntryPoint<'a, Entry> {
    /// Create a new entry point.
    #[inline]
    pub(super) fn new(
        sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>,
        ah: &'a AddressHandle,
        qkey: u32,
    ) -> Self {
        Self {
            core: UdWqeCore::new(sq, None),
            ah,
            qkey,
        }
    }

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: WqeFlags) -> io::Result<UdSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_ud_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_ud_av(self.ah, self.qkey);
        Ok(UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(
        mut self,
        flags: WqeFlags,
        imm: u32,
    ) -> io::Result<UdSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_ud_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_ud_av(self.ah, self.qkey);
        Ok(UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        })
    }
}

// =============================================================================
// UD Send WQE Builder
// =============================================================================

/// UD SEND WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct UdSendWqeBuilder<'a, Entry, DataState> {
    core: UdWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> UdSendWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> UdSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> UdSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        UdSendWqeBuilder {
            core: self.core,
            _data: PhantomData,
        }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> UdSendWqeBuilder<'a, Entry, HasData> {
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
// sq_wqe / post_recv Methods
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> UdQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Get a SQ WQE builder for UD transport.
    ///
    /// # Example
    /// ```ignore
    /// qp.sq_wqe(&ah)?
    ///     .send(WqeFlags::empty())?
    ///     .sge(addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// qp.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe<'a>(&'a self, ah: &'a AddressHandle) -> io::Result<UdSqWqeEntryPoint<'a, SqEntry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(UdSqWqeEntryPoint::new(sq, ah, self.qkey))
    }

    /// Post a receive WQE with a single scatter/gather entry.
    ///
    /// # Arguments
    /// * `entry` - User entry to associate with this receive operation
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    ///
    /// # Example
    /// ```ignore
    /// qp.post_recv(entry, addr, len, lkey)?;
    /// qp.ring_rq_doorbell();
    /// ```
    #[inline]
    pub fn post_recv(&self, entry: RqEntry, addr: u64, len: u32, lkey: u32) -> io::Result<()> {
        let rq = self.rq()?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }
        let wqe_idx = rq.pi.get();
        unsafe {
            let wqe_ptr = rq.get_wqe_ptr(wqe_idx);
            write_data_seg(wqe_ptr, len, lkey, addr);
        }
        let idx = (wqe_idx as usize) & ((rq.wqe_cnt - 1) as usize);
        rq.table[idx].set(Some(entry));
        rq.pi.set(rq.pi.get().wrapping_add(1));
        Ok(())
    }

    /// Get a BlueFlame batch builder for low-latency RQ WQE submission.
    ///
    /// Multiple receive WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
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
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_rq_batch(&self) -> Result<UdRqBlueflameWqeBatch<'_, RqEntry>, SubmissionError> {
        let rq = self.rq.as_ref().ok_or(SubmissionError::RqFull)?;
        let sq = self.sq.as_ref().ok_or(SubmissionError::BlueflameNotAvailable)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(UdRqBlueflameWqeBatch::new(rq, sq.bf_reg, sq.bf_size, &sq.bf_offset))
    }

    /// Ring the send queue doorbell.
    ///
    /// Call this after posting one or more WQEs to notify the HCA.
    #[inline]
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            if let Some((wqe_ptr, _)) = sq.last_wqe.get() {
                sq.ring_db(wqe_ptr);
            }
        }
    }

    /// Get a BlueFlame batch builder for low-latency WQE submission.
    ///
    /// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// # Example
    /// ```ignore
    /// let mut bf = qp.blueflame_sq_wqe(&ah)?;
    /// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
    /// bf.wqe()?.send(WqeFlags::empty()).inline(&data).finish()?;
    /// bf.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_sq_wqe<'a>(&'a self, ah: &'a AddressHandle) -> Result<UdBlueflameWqeBatch<'a, SqEntry>, SubmissionError> {
        let sq = self.sq.as_ref().ok_or(SubmissionError::SqFull)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(UdBlueflameWqeBatch::new(sq, ah, self.qkey))
    }
}

// =============================================================================
// UD RQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame buffer size in bytes (256B doorbell window).
const BLUEFLAME_BUFFER_SIZE: usize = 256;

/// RQ WQE size in bytes (single DataSeg).
const RQ_WQE_SIZE: usize = DATA_SEG_SIZE;

/// BlueFlame WQE batch builder for UD RQ.
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
pub struct UdRqBlueflameWqeBatch<'a, Entry> {
    rq: &'a UdRecvQueueState<Entry>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
    wqe_count: u16,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &'a Cell<u32>,
}

impl<'a, Entry> UdRqBlueflameWqeBatch<'a, Entry> {
    /// Create a new UD RQ BlueFlame batch builder.
    pub(super) fn new(
        rq: &'a UdRecvQueueState<Entry>,
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
    /// Returns `RqFull` if the receive queue doesn't have enough space.
    /// Returns `BlueflameOverflow` if the batch buffer is full.
    #[inline]
    pub fn post(&mut self, entry: Entry, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.rq.available() <= self.wqe_count as u32 {
            return Err(SubmissionError::RqFull);
        }
        if self.offset + RQ_WQE_SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }

        // Write WQE to RQ buffer
        let wqe_idx = self.rq.pi.get().wrapping_add(self.wqe_count);
        unsafe {
            let wqe_ptr = self.rq.get_wqe_ptr(wqe_idx);
            write_data_seg(wqe_ptr, len, lkey, addr);

            // Also write to batch buffer for BlueFlame copy
            write_data_seg(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }

        // Store entry in table
        let idx = (wqe_idx as usize) & ((self.rq.wqe_cnt - 1) as usize);
        self.rq.table[idx].set(Some(entry));

        self.offset += RQ_WQE_SIZE;
        self.wqe_count += 1;
        Ok(())
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    ///
    /// This method updates the producer index, writes to the doorbell record,
    /// and copies the WQEs to the BlueFlame register.
    #[inline]
    pub fn finish(self) {
        if self.wqe_count == 0 {
            return; // No WQEs to submit
        }

        // Advance RQ producer index
        self.rq.pi.set(self.rq.pi.get().wrapping_add(self.wqe_count));

        mmio_flush_writes!();

        // Update doorbell record
        unsafe {
            std::ptr::write_volatile(self.rq.dbrec, (self.rq.pi.get() as u32).to_be());
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
// UD SQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame WQE batch builder for UD QP.
///
/// Allows building multiple WQEs that fit within the BlueFlame buffer (256 bytes).
/// Each WQE is copied to a contiguous buffer and submitted via BlueFlame doorbell
/// when `finish()` is called.
pub struct UdBlueflameWqeBatch<'a, Entry> {
    sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>,
    ah: &'a AddressHandle,
    qkey: u32,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> UdBlueflameWqeBatch<'a, Entry> {
    pub(super) fn new(sq: &'a UdSendQueueState<Entry, OrderedWqeTable<Entry>>, ah: &'a AddressHandle, qkey: u32) -> Self {
        Self {
            sq,
            ah,
            qkey,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<UdBlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(UdBlueflameWqeEntryPoint { batch: self })
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

/// BlueFlame WQE entry point for UD.
#[must_use = "WQE builder must be finished"]
pub struct UdBlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut UdBlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> UdBlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: WqeFlags) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = UdBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_ud_addr()?;
        Ok(UdBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: WqeFlags, imm: u32) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = UdBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_ud_addr()?;
        Ok(UdBlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for UD BlueFlame WQE construction.
struct UdBlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut UdBlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> UdBlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut UdBlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
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
        let flags = WqeFlags::from_bits_truncate(flags.bits());
        unsafe {
            write_ctrl_seg(
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
        self.offset += CTRL_SEG_SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_ud_addr(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < UdAddressSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            UdAddressSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                self.batch.ah,
                self.batch.qkey,
            );
        }
        self.offset += UdAddressSeg::SIZE;
        self.ds_count += 3;
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

/// UD BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct UdBlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: UdBlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> UdBlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(UdBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<UdBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(UdBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> UdBlueflameWqeBuilder<'b, 'a, Entry, HasData> {
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
