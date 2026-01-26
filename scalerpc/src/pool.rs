//! Message Pool and Slot management.
//!
//! This module implements the shared message pool used by ScaleRPC for
//! virtualized mapping. The pool provides pre-allocated, MR-registered
//! slots for request/response data.

use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

use mlx5::pd::{AccessFlags, MemoryRegion, Pd};

use crate::config::PoolConfig;
use crate::error::{Error, Result, SlotState};

/// Size of the slot header (state + owner_conn + padding).
pub const SLOT_HEADER_SIZE: usize = 16;

/// Default slot size including header (4KB page aligned).
pub const DEFAULT_SLOT_SIZE: usize = 4096;

/// Magic value indicating no owner.
pub const NO_OWNER: u32 = u32::MAX;

/// Header portion of a message slot.
#[repr(C)]
struct SlotHeader {
    /// Current state of the slot.
    state: AtomicU8,
    /// Padding byte.
    _pad1: u8,
    /// Reserved bytes.
    _reserved: [u8; 2],
    /// Owner connection ID (NO_OWNER if none).
    owner_conn: AtomicU32,
    /// Padding to align data to 16 bytes.
    _pad2: [u8; 8],
}

/// A single message slot in the pool.
///
/// Each slot is page-aligned (4KB) and contains:
/// - Header with state and owner information
/// - Payload data area
#[repr(C, align(4096))]
pub struct MessageSlot {
    header: SlotHeader,
    // Data follows immediately after header
    // Size is determined by PoolConfig::slot_data_size
}

impl MessageSlot {
    /// Get the current state of the slot.
    pub fn state(&self) -> SlotState {
        SlotState::from_u8(self.header.state.load(Ordering::Acquire))
            .unwrap_or(SlotState::Free)
    }

    /// Set the state of the slot.
    pub fn set_state(&self, state: SlotState) {
        self.header.state.store(state as u8, Ordering::Release);
    }

    /// Try to transition from expected state to new state atomically.
    pub fn try_transition(&self, expected: SlotState, new: SlotState) -> bool {
        self.header
            .state
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Get the owner connection ID.
    pub fn owner(&self) -> Option<u32> {
        let owner = self.header.owner_conn.load(Ordering::Acquire);
        if owner == NO_OWNER {
            None
        } else {
            Some(owner)
        }
    }

    /// Set the owner connection ID.
    pub fn set_owner(&self, conn_id: Option<u32>) {
        self.header
            .owner_conn
            .store(conn_id.unwrap_or(NO_OWNER), Ordering::Release);
    }

    /// Get pointer to the data area.
    pub fn data_ptr(&self) -> *mut u8 {
        unsafe {
            (self as *const Self as *mut u8).add(SLOT_HEADER_SIZE)
        }
    }

    /// Get the data area as a mutable slice.
    ///
    /// # Safety
    /// The caller must ensure the slot is properly owned and not
    /// being accessed concurrently.
    pub unsafe fn data_mut(&self, data_size: usize) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.data_ptr(), data_size)
    }

    /// Get the data area as a slice.
    ///
    /// # Safety
    /// The caller must ensure the slot is not being written to concurrently.
    pub unsafe fn data(&self, data_size: usize) -> &[u8] {
        std::slice::from_raw_parts(self.data_ptr(), data_size)
    }
}

/// Shared message pool for RPC operations.
///
/// The pool owns a contiguous block of page-aligned memory containing
/// multiple message slots. The memory is registered with the RDMA device
/// for direct NIC access.
pub struct MessagePool {
    /// Raw pointer to the allocated buffer.
    buffer: *mut u8,
    /// Total size of the buffer in bytes.
    #[allow(dead_code)]
    buffer_size: usize,
    /// Memory region registered with the HCA.
    mr: MemoryRegion,
    /// Number of slots in the pool.
    num_slots: usize,
    /// Size of data area in each slot.
    slot_data_size: usize,
    /// Total size of each slot (header + data).
    slot_size: usize,
    /// Free list of slot indices (protected by mutex for thread safety).
    free_list: std::sync::Mutex<Vec<usize>>,
}

impl MessagePool {
    /// Create a new message pool.
    ///
    /// # Arguments
    /// * `pd` - Protection domain for memory registration
    /// * `config` - Pool configuration
    ///
    /// # Returns
    /// A new message pool with all slots initially free.
    pub fn new(pd: &Pd, config: &PoolConfig) -> Result<Self> {
        let slot_size = DEFAULT_SLOT_SIZE;
        let buffer_size = slot_size * config.num_slots;

        // Allocate page-aligned memory
        let buffer = unsafe {
            let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, DEFAULT_SLOT_SIZE, buffer_size);
            if ret != 0 {
                return Err(Error::Io(std::io::Error::from_raw_os_error(ret)));
            }
            // Zero-initialize
            std::ptr::write_bytes(ptr as *mut u8, 0, buffer_size);
            ptr as *mut u8
        };

        // Initialize all slots as Free
        for i in 0..config.num_slots {
            let slot = unsafe { &*(buffer.add(i * slot_size) as *const MessageSlot) };
            slot.set_state(SlotState::Free);
            slot.set_owner(None);
        }

        // Register memory with HCA
        let access = AccessFlags::LOCAL_WRITE
            | AccessFlags::REMOTE_WRITE
            | AccessFlags::REMOTE_READ;
        let mr = unsafe { pd.register(buffer, buffer_size, access)? };

        // Initialize free list with all slot indices
        let free_list: Vec<usize> = (0..config.num_slots).collect();

        Ok(Self {
            buffer,
            buffer_size,
            mr,
            num_slots: config.num_slots,
            slot_data_size: config.slot_data_size,
            slot_size,
            free_list: std::sync::Mutex::new(free_list),
        })
    }

    /// Get the number of slots in the pool.
    pub fn num_slots(&self) -> usize {
        self.num_slots
    }

    /// Get the data size per slot.
    pub fn slot_data_size(&self) -> usize {
        self.slot_data_size
    }

    /// Get the local key for this memory region.
    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    /// Get the remote key for this memory region.
    pub fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    /// Get the base address of the pool.
    pub fn base_addr(&self) -> u64 {
        self.buffer as u64
    }

    /// Get a reference to a slot by index.
    ///
    /// # Returns
    /// `None` if the index is out of bounds.
    pub fn get_slot(&self, index: usize) -> Option<&MessageSlot> {
        if index >= self.num_slots {
            return None;
        }
        unsafe {
            Some(&*(self.buffer.add(index * self.slot_size) as *const MessageSlot))
        }
    }

    /// Get the address of a slot's data area.
    pub fn slot_data_addr(&self, index: usize) -> Option<u64> {
        if index >= self.num_slots {
            return None;
        }
        Some(self.buffer as u64 + (index * self.slot_size) as u64 + SLOT_HEADER_SIZE as u64)
    }

    /// Get the address of a slot (including header).
    pub fn slot_addr(&self, index: usize) -> Option<u64> {
        if index >= self.num_slots {
            return None;
        }
        Some(self.buffer as u64 + (index * self.slot_size) as u64)
    }

    /// Allocate a free slot.
    ///
    /// # Returns
    /// `Ok(SlotHandle)` if a slot was allocated, `Err(NoFreeSlots)` otherwise.
    pub fn alloc(&self) -> Result<SlotHandle<'_>> {
        let mut free_list = self.free_list.lock().unwrap();
        match free_list.pop() {
            Some(index) => {
                let slot = self.get_slot(index).unwrap();
                // Transition from Free to Reserved
                if !slot.try_transition(SlotState::Free, SlotState::Reserved) {
                    // Slot was not in Free state, put it back and try again
                    free_list.push(index);
                    return Err(Error::InvalidSlotState {
                        expected: SlotState::Free,
                        actual: slot.state(),
                    });
                }
                Ok(SlotHandle {
                    pool: self,
                    index,
                })
            }
            None => Err(Error::NoFreeSlots),
        }
    }

    /// Allocate a free slot from a specific range.
    ///
    /// This is used for virtualized mapping where each connection owns a
    /// specific range of slots.
    ///
    /// # Arguments
    /// * `slots` - The slot indices to search for a free slot (must be sorted/contiguous)
    ///
    /// # Returns
    /// `Ok(SlotHandle)` if a slot was allocated, `Err(NoFreeSlots)` otherwise.
    pub fn alloc_from_slots(&self, slots: &[usize]) -> Result<SlotHandle<'_>> {
        if slots.is_empty() {
            return Err(Error::NoFreeSlots);
        }

        // Optimization: slots are contiguous, so we can use range check
        // instead of O(n) contains() lookup
        let min_slot = slots[0];
        let max_slot = slots[slots.len() - 1];

        let mut free_list = self.free_list.lock().unwrap();

        // Find a slot from the given range that is in the free list
        for (pos, &free_idx) in free_list.iter().enumerate() {
            // O(1) range check instead of O(n) contains()
            if free_idx >= min_slot && free_idx <= max_slot {
                let slot = self.get_slot(free_idx).unwrap();
                // Transition from Free to Reserved
                if slot.try_transition(SlotState::Free, SlotState::Reserved) {
                    free_list.swap_remove(pos);
                    return Ok(SlotHandle {
                        pool: self,
                        index: free_idx,
                    });
                }
            }
        }

        Err(Error::NoFreeSlots)
    }

    /// Return a slot to the free list.
    ///
    /// This is used internally by SlotHandle::drop().
    fn free_slot(&self, index: usize) {
        self.free_slot_by_index(index);
    }

    /// Return a slot to the free list by index.
    ///
    /// This can be called when a slot has been released from a SlotHandle
    /// and needs to be returned to the pool.
    pub fn free_slot_by_index(&self, index: usize) {
        if let Some(slot) = self.get_slot(index) {
            slot.set_state(SlotState::Free);
            slot.set_owner(None);
            let mut free_list = self.free_list.lock().unwrap();
            free_list.push(index);
        }
    }

    /// Get the number of free slots.
    pub fn free_count(&self) -> usize {
        self.free_list.lock().unwrap().len()
    }
}

impl Drop for MessagePool {
    fn drop(&mut self) {
        // MR is deregistered automatically when dropped
        // Free the allocated buffer
        unsafe {
            libc::free(self.buffer as *mut std::ffi::c_void);
        }
    }
}

/// RAII handle for an allocated slot.
///
/// When dropped, the slot is automatically returned to the free list.
pub struct SlotHandle<'a> {
    pool: &'a MessagePool,
    index: usize,
}

impl<'a> SlotHandle<'a> {
    /// Get the slot index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get a reference to the underlying slot.
    pub fn slot(&self) -> &MessageSlot {
        self.pool.get_slot(self.index).unwrap()
    }

    /// Get the address of the slot's data area.
    pub fn data_addr(&self) -> u64 {
        self.pool.slot_data_addr(self.index).unwrap()
    }

    /// Get the address of the slot (including header).
    pub fn slot_addr(&self) -> u64 {
        self.pool.slot_addr(self.index).unwrap()
    }

    /// Get the data area as a mutable slice.
    ///
    /// # Safety
    /// The caller must ensure no concurrent access to the data.
    pub unsafe fn data_mut(&self) -> &mut [u8] {
        self.slot().data_mut(self.pool.slot_data_size)
    }

    /// Get the data area as a slice.
    ///
    /// # Safety
    /// The caller must ensure no concurrent writes to the data.
    pub unsafe fn data(&self) -> &[u8] {
        self.slot().data(self.pool.slot_data_size)
    }

    /// Write data to the slot.
    ///
    /// # Returns
    /// Number of bytes written.
    pub fn write(&self, data: &[u8]) -> usize {
        let len = data.len().min(self.pool.slot_data_size);
        unsafe {
            let dst = self.slot().data_mut(self.pool.slot_data_size);
            dst[..len].copy_from_slice(&data[..len]);
        }
        len
    }

    /// Read data from the slot.
    pub fn read(&self, len: usize) -> Vec<u8> {
        let len = len.min(self.pool.slot_data_size);
        unsafe {
            let src = self.slot().data(self.pool.slot_data_size);
            src[..len].to_vec()
        }
    }

    /// Set the slot state.
    pub fn set_state(&self, state: SlotState) {
        self.slot().set_state(state);
    }

    /// Get the slot state.
    pub fn state(&self) -> SlotState {
        self.slot().state()
    }

    /// Set the owner connection ID.
    pub fn set_owner(&self, conn_id: Option<u32>) {
        self.slot().set_owner(conn_id);
    }

    /// Get the local key.
    pub fn lkey(&self) -> u32 {
        self.pool.lkey()
    }

    /// Get the remote key.
    pub fn rkey(&self) -> u32 {
        self.pool.rkey()
    }

    /// Release ownership without freeing the slot.
    ///
    /// Returns the slot index. The caller is responsible for
    /// eventually freeing the slot.
    pub fn release(self) -> usize {
        let index = self.index;
        std::mem::forget(self);
        index
    }
}

impl Drop for SlotHandle<'_> {
    fn drop(&mut self) {
        self.pool.free_slot(self.index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_state_transitions() {
        // Create a minimal slot header for testing
        let header = SlotHeader {
            state: AtomicU8::new(SlotState::Free as u8),
            _pad1: 0,
            _reserved: [0; 2],
            owner_conn: AtomicU32::new(NO_OWNER),
            _pad2: [0; 8],
        };

        // Test initial state
        assert_eq!(
            SlotState::from_u8(header.state.load(Ordering::Acquire)),
            Some(SlotState::Free)
        );

        // Test transition
        let result = header.state.compare_exchange(
            SlotState::Free as u8,
            SlotState::Reserved as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        assert!(result.is_ok());
        assert_eq!(
            SlotState::from_u8(header.state.load(Ordering::Acquire)),
            Some(SlotState::Reserved)
        );

        // Test failed transition (wrong expected state)
        let result = header.state.compare_exchange(
            SlotState::Free as u8,
            SlotState::RequestPending as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_slot_owner() {
        let header = SlotHeader {
            state: AtomicU8::new(SlotState::Free as u8),
            _pad1: 0,
            _reserved: [0; 2],
            owner_conn: AtomicU32::new(NO_OWNER),
            _pad2: [0; 8],
        };

        // Initially no owner
        assert_eq!(header.owner_conn.load(Ordering::Acquire), NO_OWNER);

        // Set owner
        header.owner_conn.store(42, Ordering::Release);
        assert_eq!(header.owner_conn.load(Ordering::Acquire), 42);

        // Clear owner
        header.owner_conn.store(NO_OWNER, Ordering::Release);
        assert_eq!(header.owner_conn.load(Ordering::Acquire), NO_OWNER);
    }

    #[test]
    fn test_slot_size_alignment() {
        assert_eq!(std::mem::size_of::<SlotHeader>(), SLOT_HEADER_SIZE);
        assert_eq!(std::mem::align_of::<MessageSlot>(), DEFAULT_SLOT_SIZE);
    }
}
