//! WQE (Work Queue Element) common definitions.
//!
//! Provides segment definitions, opcodes, WQE table, and traits shared across QP types.

use bitflags::bitflags;

/// WQEBB (Work Queue Element Basic Block) size in bytes.
pub(crate) const WQEBB_SIZE: usize = 64;

// =============================================================================
// WQE Segments
// =============================================================================

/// Control Segment (16 bytes).
///
/// First segment of every WQE.
pub struct CtrlSeg;

impl CtrlSeg {
    /// Size of the control segment in bytes.
    pub const SIZE: usize = 16;

    /// Write the control segment to the given pointer.
    ///
    /// # Safety
    /// The pointer must point to at least 16 bytes of writable memory.
    #[inline]
    pub unsafe fn write(
        ptr: *mut u8,
        opcode: u8,
        wqe_idx: u16,
        qpn: u32,
        ds_cnt: u8,
        fm_ce_se: u8,
        imm: u32,
    ) {
        let opmod_idx_opcode = ((wqe_idx as u32) << 8) | (opcode as u32);
        let qpn_ds = (qpn << 8) | (ds_cnt as u32);
        // Combine sig(0), dci_stream[15:8](0), dci_stream[7:0](0), fm_ce_se into single u32
        // Layout in big-endian: [sig][stream_hi][stream_lo][fm_ce_se]
        let sig_stream_fm = fm_ce_se as u32;

        let ptr32 = ptr as *mut u32;
        std::ptr::write_volatile(ptr32, opmod_idx_opcode.to_be());
        std::ptr::write_volatile(ptr32.add(1), qpn_ds.to_be());
        std::ptr::write_volatile(ptr32.add(2), sig_stream_fm.to_be());
        std::ptr::write_volatile(ptr32.add(3), imm.to_be());
    }

    /// Update the DS count after WQE is complete.
    ///
    /// # Safety
    /// The pointer must point to a valid control segment.
    #[inline]
    pub unsafe fn update_ds_cnt(ptr: *mut u8, ds_cnt: u8) {
        std::ptr::write_volatile(ptr.add(7), ds_cnt);
    }

    /// Update the WQE index in the control segment.
    ///
    /// Used when relocating a WQE due to ring wrap-around.
    ///
    /// # Safety
    /// The pointer must point to a valid control segment.
    #[inline]
    pub unsafe fn update_wqe_idx(ptr: *mut u8, wqe_idx: u16) {
        // wqe_idx is stored at bytes 1-2 in big-endian format
        // Layout: [0][wqe_idx_hi][wqe_idx_lo][opcode]
        std::ptr::write_volatile(ptr.add(1), (wqe_idx >> 8) as u8);
        std::ptr::write_volatile(ptr.add(2), wqe_idx as u8);
    }
}

/// RDMA Segment (16 bytes).
///
/// Used for RDMA WRITE, RDMA READ operations.
pub struct RdmaSeg;

impl RdmaSeg {
    /// Size of the RDMA segment in bytes.
    pub const SIZE: usize = 16;

    /// Write the RDMA segment to the given pointer.
    ///
    /// # Safety
    /// The pointer must point to at least 16 bytes of writable memory.
    #[inline]
    pub unsafe fn write(ptr: *mut u8, remote_addr: u64, rkey: u32) {
        let ptr64 = ptr as *mut u64;
        let ptr32 = ptr.add(8) as *mut u32;
        std::ptr::write_volatile(ptr64, remote_addr.to_be());
        std::ptr::write_volatile(ptr32, rkey.to_be());
        std::ptr::write_volatile(ptr32.add(1), 0);
    }
}

/// Data Segment / SGE (16 bytes).
///
/// Points to a memory region for data transfer.
pub struct DataSeg;

impl DataSeg {
    /// Size of the data segment in bytes.
    pub const SIZE: usize = 16;

    /// Write the data segment to the given pointer.
    ///
    /// # Safety
    /// The pointer must point to at least 16 bytes of writable memory.
    #[inline]
    pub unsafe fn write(ptr: *mut u8, byte_count: u32, lkey: u32, addr: u64) {
        let ptr32 = ptr as *mut u32;
        let ptr64 = ptr.add(8) as *mut u64;
        std::ptr::write_volatile(ptr32, byte_count.to_be());
        std::ptr::write_volatile(ptr32.add(1), lkey.to_be());
        std::ptr::write_volatile(ptr64, addr.to_be());
    }
}

/// Inline data header.
pub struct InlineHeader;

impl InlineHeader {
    /// Write inline header.
    ///
    /// Returns the padded size (16-byte aligned).
    ///
    /// # Safety
    /// The pointer must point to at least 4 bytes of writable memory.
    #[inline]
    pub unsafe fn write(ptr: *mut u8, byte_count: u32) -> usize {
        let ptr32 = ptr as *mut u32;
        let header = 0x8000_0000 | byte_count;
        std::ptr::write_volatile(ptr32, header.to_be());
        ((4 + byte_count as usize) + 15) & !15
    }
}

/// Address Vector (for DC QPs).
///
/// 48 bytes, specifies the destination for DC operations.
pub struct AddressVector;

impl AddressVector {
    /// Size of the address vector in bytes.
    pub const SIZE: usize = 48;

    /// Write the address vector to the given pointer.
    ///
    /// # Safety
    /// The pointer must point to at least 48 bytes of writable memory.
    #[inline]
    pub unsafe fn write(ptr: *mut u8, dc_key: u64, dctn: u32, dlid: u16) {
        let ptr64 = ptr as *mut u64;
        let ptr32 = ptr.add(8) as *mut u32;
        let ptr16 = ptr.add(14) as *mut u16;

        std::ptr::write_volatile(ptr64, dc_key.to_be());
        let dqp_dct = 0x8000_0000 | (dctn & 0x00FF_FFFF);
        std::ptr::write_volatile(ptr32, dqp_dct.to_be());
        std::ptr::write_volatile(ptr.add(12), 0);
        std::ptr::write_volatile(ptr.add(13), 0);
        std::ptr::write_volatile(ptr16, dlid.to_be());
        std::ptr::write_bytes(ptr.add(16), 0, 32);
    }
}

// =============================================================================
// WQE Opcodes and Flags
// =============================================================================

/// Atomic Segment (16 bytes).
///
/// Used for atomic Compare-and-Swap and Fetch-and-Add operations.
/// Follows the RDMA segment in an atomic WQE.
pub struct AtomicSeg;

impl AtomicSeg {
    /// Size of the atomic segment in bytes.
    pub const SIZE: usize = 16;

    /// Write the atomic segment for Compare-and-Swap operation.
    ///
    /// The CAS operation atomically compares the value at the remote address
    /// with `compare`. If equal, replaces it with `swap`. Returns the original
    /// value in the local buffer.
    ///
    /// # Safety
    /// The pointer must point to at least 16 bytes of writable memory.
    #[inline]
    pub unsafe fn write_cas(ptr: *mut u8, swap: u64, compare: u64) {
        let ptr64 = ptr as *mut u64;
        std::ptr::write_volatile(ptr64, swap.to_be());
        std::ptr::write_volatile(ptr64.add(1), compare.to_be());
    }

    /// Write the atomic segment for Fetch-and-Add operation.
    ///
    /// The FA operation atomically adds `add_value` to the value at the remote
    /// address. Returns the original value in the local buffer.
    ///
    /// # Safety
    /// The pointer must point to at least 16 bytes of writable memory.
    #[inline]
    pub unsafe fn write_fa(ptr: *mut u8, add_value: u64) {
        let ptr64 = ptr as *mut u64;
        std::ptr::write_volatile(ptr64, add_value.to_be());
        std::ptr::write_volatile(ptr64.add(1), 0u64.to_be());
    }
}

/// Tag Matching Segment (32 bytes).
///
/// Used for TM operations (TAG_ADD, TAG_DEL) via Command QP.
pub struct TmSeg;

impl TmSeg {
    /// Size of the TM segment in bytes.
    pub const SIZE: usize = 32;

    /// Write the TM segment for TAG_ADD operation.
    ///
    /// # Safety
    /// The pointer must point to at least 32 bytes of writable memory.
    ///
    /// # Arguments
    /// * `ptr` - Pointer to 32-byte TM segment buffer
    /// * `index` - TM tag index in the SRQ tag list
    /// * `sw_cnt` - Software count (typically used for tracking)
    /// * `tag` - Tag value to match against incoming messages
    /// * `mask` - Mask for tag matching (1 bits = must match)
    /// * `signaled` - Whether to generate a CQE on completion
    #[inline]
    pub unsafe fn write_add(
        ptr: *mut u8,
        index: u16,
        sw_cnt: u16,
        tag: u64,
        mask: u64,
        signaled: bool,
    ) {
        // mlx5_wqe_tm_seg layout (32 bytes):
        //   offset 0: opcode (1 byte) - APPEND=0x01 shifted left 4 bits = 0x10
        //   offset 1: flags (1 byte) - 0x80 for CQE request
        //   offset 2-3: index (2 bytes, big-endian)
        //   offset 4-5: rsvd0 (2 bytes)
        //   offset 6-7: sw_cnt (2 bytes, big-endian)
        //   offset 8-15: rsvd1 (8 bytes)
        //   offset 16-23: append_tag (8 bytes, big-endian)
        //   offset 24-31: append_mask (8 bytes, big-endian)

        // opcode = MLX5_TM_OPCODE_APPEND << 4 = 0x10
        std::ptr::write_volatile(ptr, 0x10);
        // flags: TM_CQE_REQ = 0x80
        let flags = if signaled { 0x80 } else { 0x00 };
        std::ptr::write_volatile(ptr.add(1), flags);
        // index (big-endian)
        std::ptr::write_volatile(ptr.add(2) as *mut u16, index.to_be());
        // rsvd0 (2 bytes at offset 4-5)
        std::ptr::write_volatile(ptr.add(4) as *mut u16, 0);
        // sw_cnt (2 bytes at offset 6-7, big-endian)
        std::ptr::write_volatile(ptr.add(6) as *mut u16, sw_cnt.to_be());
        // rsvd1 (8 bytes at offset 8-15)
        std::ptr::write_volatile(ptr.add(8) as *mut u64, 0);
        // append_tag (big-endian)
        std::ptr::write_volatile(ptr.add(16) as *mut u64, tag.to_be());
        // append_mask (big-endian)
        std::ptr::write_volatile(ptr.add(24) as *mut u64, mask.to_be());
    }

    /// Write the TM segment for TAG_DEL operation.
    ///
    /// # Safety
    /// The pointer must point to at least 32 bytes of writable memory.
    #[inline]
    pub unsafe fn write_del(ptr: *mut u8, index: u16, signaled: bool) {
        // opcode = MLX5_TM_OPCODE_REMOVE << 4 = 0x20
        std::ptr::write_volatile(ptr, 0x20);
        // flags
        let flags = if signaled { 0x80 } else { 0x00 };
        std::ptr::write_volatile(ptr.add(1), flags);
        // index (big-endian)
        std::ptr::write_volatile(ptr.add(2) as *mut u16, index.to_be());
        // Clear remaining bytes
        std::ptr::write_bytes(ptr.add(4), 0, 28);
    }
}

/// WQE opcodes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WqeOpcode {
    Nop = 0x00,
    SendInval = 0x01,
    RdmaWrite = 0x08,
    RdmaWriteImm = 0x09,
    Send = 0x0A,
    SendImm = 0x0B,
    RdmaRead = 0x10,
    AtomicCs = 0x11,
    AtomicFa = 0x12,
    TagMatching = 0x28,
}

bitflags! {
    /// WQE flags for fm_ce_se field.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct WqeFlags: u8 {
        /// Fence (wait for previous WQEs to complete).
        const FENCE = 0x40;
        /// Completion requested.
        const COMPLETION = 0x08;
        /// Solicited event.
        const SOLICITED = 0x02;
    }
}

/// Calculate the number of WQEBBs for a WQE size.
#[inline]
pub(crate) fn calc_wqebb_cnt(wqe_size: usize) -> u16 {
    wqe_size.div_ceil(WQEBB_SIZE) as u16
}

// =============================================================================
// WQE Entry
// =============================================================================

/// WQE table entry: user data + CI management data.
///
/// The `ci_delta` field stores the accumulated PI value at signaled WQE completion.
/// When processing completions, use `ci.set(ci_delta)` to update the consumer index.
#[derive(Debug, Clone, Copy)]
pub struct WqeEntry<T> {
    /// User-provided entry data.
    pub data: T,
    /// Accumulated PI value for correct CI management.
    pub ci_delta: u16,
}

// =============================================================================
// Handle
// =============================================================================

/// Handle to a posted WQE.
#[derive(Debug, Clone, Copy)]
pub struct WqeHandle {
    /// WQE index in the SQ.
    pub wqe_idx: u16,
    /// WQE size in bytes.
    pub size: usize,
}

// =============================================================================
// WQE Table (Ordered - for in-order signaled completion)
// =============================================================================

use std::cell::Cell;

/// Ordered WQE table for tracking signaled operations with in-order completion.
///
/// Only signaled WQEs have entries stored. When a CQE arrives, only the
/// single entry at that wqe_idx is retrieved (no draining).
///
/// This table is used for Send Queue completion tracking where completions
/// arrive in order. The `ci_delta` field stores the accumulated PI value
/// at the time of signaled WQE completion.
///
/// Uses `Cell<Option<WqeEntry<T>>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows.
///
/// The table size must be a power of 2 for fast modulo via bit masking.
pub struct OrderedWqeTable<T> {
    entries: Box<[Cell<Option<WqeEntry<T>>>]>,
    mask: u16,
}

impl<T> OrderedWqeTable<T> {
    /// Create a new ordered WQE table with the given capacity.
    ///
    /// # Arguments
    /// * `wqe_cnt` - Number of entries (must be power of 2)
    pub fn new(wqe_cnt: u16) -> Self {
        debug_assert!(wqe_cnt.is_power_of_two(), "wqe_cnt must be power of 2");
        let entries = (0..wqe_cnt)
            .map(|_| Cell::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            entries,
            mask: wqe_cnt - 1,
        }
    }

    /// Store an entry at the given index with CI delta.
    ///
    /// The `ci_delta` should be the accumulated PI value at the time of
    /// signaled WQE completion.
    #[inline]
    pub fn store(&self, idx: u16, entry: T, ci_delta: u16) {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].set(Some(WqeEntry {
            data: entry,
            ci_delta,
        }));
    }

    /// Take the entry at the given index, leaving None.
    ///
    /// Returns `WqeEntry<T>` containing both user data and CI delta.
    #[inline]
    pub fn take(&self, idx: u16) -> Option<WqeEntry<T>> {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].take()
    }

    /// Check if the slot at the given index is available (None).
    #[inline]
    pub fn is_available(&self, idx: u16) -> bool {
        let slot = (idx & self.mask) as usize;
        // Temporarily take and put back to check
        let val = self.entries[slot].take();
        let is_none = val.is_none();
        self.entries[slot].set(val);
        is_none
    }

    /// Count available slots by scanning the table.
    pub fn count_available(&self) -> u16 {
        self.entries
            .iter()
            .filter(|e| {
                let val = e.take();
                let is_none = val.is_none();
                e.set(val);
                is_none
            })
            .count() as u16
    }
}

// =============================================================================
// Unordered WQE Table (for unordered completion)
// =============================================================================

/// Entry for unordered WQE table.
#[derive(Debug, Clone, Copy)]
pub struct UnorderedWqeEntry<T> {
    /// User-provided entry data.
    pub data: T,
    /// Starting WQEBB index for this WQE.
    pub wqebb_start: u16,
    /// Number of WQEBBs consumed by this WQE.
    pub wqebb_count: u16,
}

/// Unordered WQE table for tracking operations with out-of-order completion.
///
/// Uses a bitmap for WQEBB-level free space management, allowing efficient
/// allocation and deallocation regardless of completion order.
///
/// This is used for TM-SRQ's RQ (Receive Queue) where completions arrive
/// in arbitrary order based on incoming message timing.
pub struct UnorderedWqeTable<T> {
    /// Entry storage indexed by wqe_idx.
    entries: Box<[Cell<Option<UnorderedWqeEntry<T>>>]>,
    /// Bitmap for WQEBB allocation (1 = in-use, 0 = free).
    /// Each u64 tracks 64 consecutive WQEBBs.
    bitmap: Box<[Cell<u64>]>,
    /// Total number of WQEBBs (power of 2).
    wqebb_cnt: u16,
    /// Mask for index wrapping (wqebb_cnt - 1).
    mask: u16,
    /// Hint for next allocation starting position.
    alloc_hint: Cell<u16>,
}

impl<T> UnorderedWqeTable<T> {
    /// Create a new unordered WQE table.
    ///
    /// # Arguments
    /// * `wqebb_cnt` - Number of WQEBBs (must be power of 2)
    pub fn new(wqebb_cnt: u16) -> Self {
        debug_assert!(wqebb_cnt.is_power_of_two(), "wqebb_cnt must be power of 2");
        let entries = (0..wqebb_cnt)
            .map(|_| Cell::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let bitmap_len = (wqebb_cnt as usize + 63) / 64;
        let bitmap = (0..bitmap_len)
            .map(|_| Cell::new(0u64))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            entries,
            bitmap,
            wqebb_cnt,
            mask: wqebb_cnt - 1,
            alloc_hint: Cell::new(0),
        }
    }

    /// Try to allocate the first WQEBB for a new WQE.
    ///
    /// Returns the starting WQEBB index on success.
    pub fn try_allocate_first(&self) -> Option<u16> {
        let hint = self.alloc_hint.get();
        // Search from hint position
        for offset in 0..self.wqebb_cnt {
            let idx = (hint + offset) & self.mask;
            let word = (idx / 64) as usize;
            let bit = idx % 64;

            let current = self.bitmap[word].get();
            if (current & (1u64 << bit)) == 0 {
                // Found a free WQEBB, mark it as used
                self.bitmap[word].set(current | (1u64 << bit));
                // Update hint to next position
                self.alloc_hint.set((idx + 1) & self.mask);
                return Some(idx);
            }
        }
        None
    }

    /// Try to extend the allocation by one more WQEBB.
    ///
    /// # Arguments
    /// * `start` - Starting WQEBB index of the current allocation
    /// * `current_count` - Current number of allocated WQEBBs
    ///
    /// Returns `true` if extension succeeded, `false` if the next WQEBB is in use.
    pub fn try_extend(&self, start: u16, current_count: u16) -> bool {
        let next_idx = (start + current_count) & self.mask;
        let word = (next_idx / 64) as usize;
        let bit = next_idx % 64;

        let current = self.bitmap[word].get();
        if (current & (1u64 << bit)) == 0 {
            // Free, mark as used
            self.bitmap[word].set(current | (1u64 << bit));
            true
        } else {
            false
        }
    }

    /// Release allocated WQEBBs (for rollback on failure).
    ///
    /// # Arguments
    /// * `start` - Starting WQEBB index
    /// * `count` - Number of WQEBBs to release
    pub fn release(&self, start: u16, count: u16) {
        for i in 0..count {
            let idx = (start + i) & self.mask;
            let word = (idx / 64) as usize;
            let bit = idx % 64;

            let current = self.bitmap[word].get();
            self.bitmap[word].set(current & !(1u64 << bit));
        }
    }

    /// Store an entry at the given WQE index.
    ///
    /// The bitmap should already be allocated via `try_allocate_first` and
    /// `try_extend` calls.
    ///
    /// # Arguments
    /// * `wqe_idx` - WQE index (typically same as wqebb_start for single-WQEBB WQEs)
    /// * `wqebb_start` - Starting WQEBB index
    /// * `wqebb_count` - Number of WQEBBs used
    /// * `data` - User-provided entry data
    #[inline]
    pub fn store(&self, wqe_idx: u16, wqebb_start: u16, wqebb_count: u16, data: T) {
        let slot = (wqe_idx & self.mask) as usize;
        self.entries[slot].set(Some(UnorderedWqeEntry {
            data,
            wqebb_start,
            wqebb_count,
        }));
    }

    /// Take an entry at the given WQE index, releasing its WQEBBs.
    ///
    /// Returns the user data on success.
    #[inline]
    pub fn take(&self, wqe_idx: u16) -> Option<T> {
        let slot = (wqe_idx & self.mask) as usize;
        let entry = self.entries[slot].take()?;
        // Release the WQEBBs
        self.release(entry.wqebb_start, entry.wqebb_count);
        Some(entry.data)
    }

    /// Get the number of available (free) WQEBBs.
    pub fn available_wqebbs(&self) -> u16 {
        let used: u32 = self.bitmap.iter().map(|b| b.get().count_ones()).sum();
        self.wqebb_cnt - used as u16
    }
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Segment Size Constants
    // -------------------------------------------------------------------------

    #[test]
    fn test_wqebb_size() {
        assert_eq!(WQEBB_SIZE, 64);
    }

    #[test]
    fn test_ctrl_seg_size() {
        assert_eq!(CtrlSeg::SIZE, 16);
    }

    #[test]
    fn test_rdma_seg_size() {
        assert_eq!(RdmaSeg::SIZE, 16);
    }

    #[test]
    fn test_data_seg_size() {
        assert_eq!(DataSeg::SIZE, 16);
    }

    #[test]
    fn test_atomic_seg_size() {
        assert_eq!(AtomicSeg::SIZE, 16);
    }

    #[test]
    fn test_address_vector_size() {
        assert_eq!(AddressVector::SIZE, 48);
    }

    #[test]
    fn test_tm_seg_size() {
        assert_eq!(TmSeg::SIZE, 32);
    }

    // -------------------------------------------------------------------------
    // calc_wqebb_cnt
    // -------------------------------------------------------------------------

    #[test]
    fn test_calc_wqebb_cnt_single() {
        // 64 bytes or less -> 1 WQEBB
        assert_eq!(calc_wqebb_cnt(1), 1);
        assert_eq!(calc_wqebb_cnt(16), 1);
        assert_eq!(calc_wqebb_cnt(64), 1);
    }

    #[test]
    fn test_calc_wqebb_cnt_multiple() {
        // 65 bytes -> 2 WQEBBs
        assert_eq!(calc_wqebb_cnt(65), 2);
        assert_eq!(calc_wqebb_cnt(128), 2);
        // 129 bytes -> 3 WQEBBs
        assert_eq!(calc_wqebb_cnt(129), 3);
        assert_eq!(calc_wqebb_cnt(192), 3);
        // 193 bytes -> 4 WQEBBs
        assert_eq!(calc_wqebb_cnt(193), 4);
        assert_eq!(calc_wqebb_cnt(256), 4);
    }

    #[test]
    fn test_calc_wqebb_cnt_boundary() {
        // Exact boundaries
        assert_eq!(calc_wqebb_cnt(63), 1);
        assert_eq!(calc_wqebb_cnt(64), 1);
        assert_eq!(calc_wqebb_cnt(127), 2);
        assert_eq!(calc_wqebb_cnt(128), 2);
    }

    // -------------------------------------------------------------------------
    // WQE Tables
    // -------------------------------------------------------------------------

    #[test]
    fn test_ordered_wqe_table_store_and_take() {
        let table = OrderedWqeTable::<u32>::new(8);
        table.store(0, 42, 1);
        let entry = table.take(0);
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.data, 42);
        assert_eq!(entry.ci_delta, 1);
        // Should be None after take
        assert!(table.take(0).is_none());
    }

    #[test]
    fn test_ordered_wqe_table_wrap_around() {
        let table = OrderedWqeTable::<u32>::new(4);
        // Store at indices that wrap around
        table.store(4, 100, 1); // 4 & 3 = 0
        table.store(5, 101, 2); // 5 & 3 = 1
        let entry0 = table.take(0);
        assert!(entry0.is_some());
        assert_eq!(entry0.unwrap().data, 100);
        let entry1 = table.take(1);
        assert!(entry1.is_some());
        assert_eq!(entry1.unwrap().data, 101);
    }

    #[test]
    fn test_ordered_wqe_table_is_available() {
        let table = OrderedWqeTable::<u32>::new(4);
        assert!(table.is_available(0));
        table.store(0, 42, 1);
        assert!(!table.is_available(0));
        table.take(0);
        assert!(table.is_available(0));
    }

    #[test]
    fn test_unordered_wqe_table_allocate_and_release() {
        let table = UnorderedWqeTable::<u32>::new(8);
        assert_eq!(table.available_wqebbs(), 8);

        // Allocate first WQEBB
        let start = table.try_allocate_first();
        assert!(start.is_some());
        assert_eq!(table.available_wqebbs(), 7);

        // Extend allocation
        let extended = table.try_extend(start.unwrap(), 1);
        assert!(extended);
        assert_eq!(table.available_wqebbs(), 6);

        // Store entry
        table.store(start.unwrap(), start.unwrap(), 2, 42);

        // Take entry (releases WQEBBs)
        let data = table.take(start.unwrap());
        assert_eq!(data, Some(42));
        assert_eq!(table.available_wqebbs(), 8);
    }

    #[test]
    fn test_unordered_wqe_table_full() {
        let table = UnorderedWqeTable::<u32>::new(4);

        // Allocate all WQEBBs
        for _ in 0..4 {
            let idx = table.try_allocate_first();
            assert!(idx.is_some());
        }

        // Should fail to allocate more
        assert!(table.try_allocate_first().is_none());
        assert_eq!(table.available_wqebbs(), 0);
    }
}


