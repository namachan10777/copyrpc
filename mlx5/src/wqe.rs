//! WQE (Work Queue Element) common definitions.
//!
//! Provides segment definitions, opcodes, WQE table, and traits shared across QP types.

use bitflags::bitflags;

/// WQEBB (Work Queue Element Basic Block) size in bytes.
pub const WQEBB_SIZE: usize = 64;

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

        let ptr32 = ptr as *mut u32;
        std::ptr::write_volatile(ptr32, opmod_idx_opcode.to_be());
        std::ptr::write_volatile(ptr32.add(1), qpn_ds.to_be());
        std::ptr::write_volatile(ptr.add(8), 0); // signature
        std::ptr::write_volatile(ptr.add(9), 0); // dci_stream[15:8]
        std::ptr::write_volatile(ptr.add(10), 0); // dci_stream[7:0]
        std::ptr::write_volatile(ptr.add(11), fm_ce_se);
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
pub fn calc_wqebb_cnt(wqe_size: usize) -> u16 {
    wqe_size.div_ceil(WQEBB_SIZE) as u16
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
// WQE Table (Sparse - for signaled-only completion)
// =============================================================================

use std::cell::Cell;

/// Sparse WQE table for tracking signaled operations only.
///
/// Only signaled WQEs have entries stored. When a CQE arrives, only the
/// single entry at that wqe_idx is retrieved (no draining).
///
/// Also used for unordered queues where entries track slot availability.
///
/// Uses `Cell<Option<T>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows.
///
/// The table size must be a power of 2 for fast modulo via bit masking.
pub struct SparseWqeTable<T> {
    entries: Box<[Cell<Option<T>>]>,
    mask: u16,
}

impl<T> SparseWqeTable<T> {
    /// Create a new sparse WQE table with the given capacity.
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

    /// Store an entry at the given index.
    #[inline]
    pub fn store(&self, idx: u16, entry: T) {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].set(Some(entry));
    }

    /// Take the entry at the given index, leaving None.
    #[inline]
    pub fn take(&self, idx: u16) -> Option<T> {
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
    ///
    /// For unordered queues where gaps may exist.
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
// WQE Table (Dense - for all-entry callback)
// =============================================================================

/// Dense WQE table for tracking all WQE operations.
///
/// Every WQE must have an entry. When a signaled CQE arrives, all entries
/// from old_ci to new_ci are drained and passed to the callback.
///
/// Uses `Cell<Option<T>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows. This is critical for
/// allowing callbacks to post new WQEs while completion processing is
/// in progress.
///
/// The table size must be a power of 2 for fast modulo via bit masking.
pub struct DenseWqeTable<T> {
    entries: Box<[Cell<Option<T>>]>,
    mask: u16,
}

impl<T> DenseWqeTable<T> {
    /// Create a new dense WQE table with the given capacity.
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

    /// Store an entry at the given index.
    #[inline]
    pub fn store(&self, idx: u16, entry: T) {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].set(Some(entry));
    }

    /// Take the entry at the given index.
    ///
    /// Returns the entry if present, None otherwise.
    #[inline]
    pub fn take(&self, idx: u16) -> Option<T> {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].take()
    }

    /// Take all entries from old_ci (exclusive) to new_ci (inclusive).
    ///
    /// Unlike the previous mutable iterator approach, this method takes `&self`
    /// and collects entries upfront, so no borrow is held during callback
    /// invocation.
    ///
    /// All slots in range are expected to have entries (dense table invariant).
    pub fn take_range(&self, old_ci: u16, new_ci: u16) -> impl Iterator<Item = (u16, T)> + '_ {
        DenseTakeRange {
            table: self,
            current: old_ci.wrapping_add(1),
            end: new_ci.wrapping_add(1),
        }
    }
}

/// Iterator that takes entries from a dense WQE table range.
///
/// This iterator only holds a shared reference to the table, allowing
/// callbacks to safely access the table through other references.
pub struct DenseTakeRange<'a, T> {
    table: &'a DenseWqeTable<T>,
    current: u16,
    end: u16,
}

impl<T> Iterator for DenseTakeRange<'_, T> {
    type Item = (u16, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current != self.end {
            let idx = self.current;
            self.current = self.current.wrapping_add(1);
            // Take the entry using Cell::take (interior mutability)
            // Dense table guarantees all entries in range are present
            self.table.take(idx).map(|entry| (idx, entry))
        } else {
            None
        }
    }
}

