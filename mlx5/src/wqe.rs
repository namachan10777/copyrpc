//! WQE (Work Queue Element) common definitions.
//!
//! Provides segment definitions, opcodes, WQE table, and traits shared across QP types.

use std::io;

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
    #[inline]
    pub unsafe fn write_add(ptr: *mut u8, index: u16, tag: u64, mask: u64, signaled: bool) {
        // opcode = MLX5_TM_OPCODE_APPEND << 4 = 0x10
        std::ptr::write_volatile(ptr, 0x10);
        // flags: TM_CQE_REQ = 0x80
        let flags = if signaled { 0x80 } else { 0x00 };
        std::ptr::write_volatile(ptr.add(1), flags);
        // index (big-endian)
        std::ptr::write_volatile(ptr.add(2) as *mut u16, index.to_be());
        // rsvd0
        std::ptr::write_volatile(ptr.add(4) as *mut u16, 0);
        // sw_cnt
        std::ptr::write_volatile(ptr.add(6) as *mut u16, 0);
        // rsvd1
        std::ptr::write_bytes(ptr.add(8), 0, 8);
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

/// WQE flags for fm_ce_se field.
#[allow(non_snake_case)]
pub mod WqeFlags {
    /// Fence (wait for previous WQEs to complete).
    pub const FENCE: u8 = 0x40;
    /// Completion requested.
    pub const COMPLETION: u8 = 0x08;
    /// Solicited event.
    pub const SOLICITED: u8 = 0x02;
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
// WQE Table
// =============================================================================

/// WQE table for tracking in-flight operations.
///
/// Maps WQE indices to user-defined entries. Used to associate completion
/// events (CQEs) with the original request metadata.
///
/// The table size must be a power of 2 for fast modulo via bit masking.
pub struct WqeTable<T> {
    entries: Box<[Option<T>]>,
    mask: u16,
}

impl<T> WqeTable<T> {
    /// Create a new WQE table with the given capacity.
    ///
    /// # Arguments
    /// * `wqe_cnt` - Number of entries (must be power of 2)
    pub fn new(wqe_cnt: u16) -> Self {
        debug_assert!(wqe_cnt.is_power_of_two(), "wqe_cnt must be power of 2");
        let entries = (0..wqe_cnt)
            .map(|_| None)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            entries,
            mask: wqe_cnt - 1,
        }
    }

    /// Store an entry at the given index.
    #[inline]
    pub fn store(&mut self, idx: u16, entry: T) {
        let slot = (idx & self.mask) as usize;
        self.entries[slot] = Some(entry);
    }

    /// Take the entry at the given index, leaving None.
    #[inline]
    pub fn take(&mut self, idx: u16) -> Option<T> {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].take()
    }

    /// Check if the slot at the given index is available (None).
    #[inline]
    pub fn is_available(&self, idx: u16) -> bool {
        let slot = (idx & self.mask) as usize;
        self.entries[slot].is_none()
    }

    /// Drain entries from old_ci (exclusive) to new_ci (inclusive).
    ///
    /// For ordered queues: iterates through all completed WQEs and yields
    /// entries that were stored (Some values).
    pub fn drain_range(&mut self, old_ci: u16, new_ci: u16) -> DrainRange<'_, T> {
        DrainRange {
            table: self,
            current: old_ci.wrapping_add(1),
            end: new_ci.wrapping_add(1),
        }
    }

    /// Count available slots by scanning the table.
    ///
    /// For unordered queues where gaps may exist.
    pub fn count_available(&self) -> u16 {
        self.entries.iter().filter(|e| e.is_none()).count() as u16
    }
}

/// Iterator that drains entries from a WQE table range.
pub struct DrainRange<'a, T> {
    table: &'a mut WqeTable<T>,
    current: u16,
    end: u16,
}

impl<T> Iterator for DrainRange<'_, T> {
    type Item = (u16, T);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current != self.end {
            let idx = self.current;
            self.current = self.current.wrapping_add(1);
            if let Some(entry) = self.table.take(idx) {
                return Some((idx, entry));
            }
        }
        None
    }
}

// =============================================================================
// Traits
// =============================================================================

/// Trait for ordered send queues with WQE table support.
///
/// Used for RC QP, DCI without streams where WQE completion order matches
/// submission order.
pub trait OrderedSendQueue {
    /// Builder type for this send queue.
    type Builder<'a>
    where
        Self: 'a;

    /// Entry type stored in the WQE table.
    type Entry;

    /// Get the number of available WQE slots.
    fn sq_available(&self) -> u16;

    /// Get a WQE builder for zero-copy WQE construction.
    ///
    /// # Arguments
    /// * `entry` - Optional entry to store in WQE table.
    ///   - `Some(entry)`: Store entry and set SIGNALED flag
    ///   - `None`: No entry stored, no completion requested
    fn wqe_builder(&mut self, entry: Option<Self::Entry>) -> io::Result<Self::Builder<'_>>;

    /// Ring the SQ doorbell to notify HCA of new WQEs.
    fn ring_sq_doorbell(&mut self);

    /// Process completions up to the given wqe_counter.
    ///
    /// Calls the callback for each entry that was stored (signaled WQEs).
    fn process_completions<F>(&mut self, new_ci: u16, callback: F)
    where
        F: FnMut(u16, Self::Entry);
}

/// Trait for unordered send queues with WQE table support.
///
/// Used for TM-SRQ Command QP, DCI with streams where completions may arrive
/// out of order.
pub trait UnorderedSendQueue {
    /// Builder type for this send queue.
    type Builder<'a>
    where
        Self: 'a;

    /// Entry type stored in the WQE table.
    type Entry;

    /// Get optimistic available count.
    ///
    /// Based on pi - ci, but actual availability may be less due to gaps.
    fn optimistic_available(&self) -> u16;

    /// Scan the table to get exact available count (slower).
    fn exact_available(&self) -> u16;

    /// Check if a specific slot is available.
    fn is_slot_available(&self, idx: u16) -> bool;

    /// Get a WQE builder for zero-copy WQE construction.
    ///
    /// Entry is required (always signaled for unordered queues).
    fn wqe_builder(&mut self, entry: Self::Entry) -> io::Result<Self::Builder<'_>>;

    /// Ring the SQ doorbell to notify HCA of new WQEs.
    fn ring_sq_doorbell(&mut self);

    /// Process a single completion.
    ///
    /// Returns the entry that was stored at the given wqe_idx.
    fn process_completion(&mut self, wqe_idx: u16) -> Option<Self::Entry>;
}

/// Trait for types that can post receive WQEs.
pub trait ReceiveQueue {
    /// Post a receive WQE.
    ///
    /// # Safety
    /// - The buffer must be registered and valid
    /// - There must be available slots in the RQ
    unsafe fn post_recv(&mut self, addr: u64, len: u32, lkey: u32);

    /// Ring the RQ doorbell to notify HCA of new WQEs.
    fn ring_rq_doorbell(&mut self);
}
