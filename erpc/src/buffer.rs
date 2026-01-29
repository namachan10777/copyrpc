//! Message buffer management for eRPC.
//!
//! MsgBuffer provides a simple buffer abstraction that can be registered
//! with RDMA hardware for zero-copy operations.

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

use mlx5::pd::{AccessFlags, MemoryRegion, Pd};

use crate::error::{Error, Result};
use crate::packet::PKT_HDR_SIZE;

/// Alignment for message buffers (cache line aligned).
pub const MSG_BUFFER_ALIGN: usize = 64;

/// A message buffer that can be registered with RDMA hardware.
///
/// The buffer owns its memory and ensures proper alignment for DMA operations.
/// When registered with a Protection Domain, it can be used for zero-copy
/// RDMA send/receive operations.
pub struct MsgBuffer {
    /// Pointer to the allocated memory.
    ptr: NonNull<u8>,
    /// Total capacity of the buffer.
    capacity: usize,
    /// Current length of valid data.
    len: usize,
    /// Memory region registration (if registered).
    mr: Option<MemoryRegion>,
}

impl MsgBuffer {
    /// Create a new unregistered message buffer with the given capacity.
    ///
    /// The buffer is aligned to MSG_BUFFER_ALIGN bytes.
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::InvalidConfig("Buffer capacity cannot be 0".into()));
        }

        let layout = Layout::from_size_align(capacity, MSG_BUFFER_ALIGN)
            .map_err(|_| Error::InvalidConfig("Invalid buffer layout".into()))?;

        let ptr = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Failed to allocate buffer",
                )));
            }
            NonNull::new_unchecked(ptr)
        };

        Ok(Self {
            ptr,
            capacity,
            len: 0,
            mr: None,
        })
    }

    /// Create a new message buffer and register it with a Protection Domain.
    ///
    /// The buffer will be registered with LOCAL_WRITE access.
    pub fn new_registered(capacity: usize, pd: &Pd) -> Result<Self> {
        let mut buf = Self::new(capacity)?;
        buf.register(pd)?;
        Ok(buf)
    }

    /// Register this buffer with a Protection Domain.
    ///
    /// # Safety
    /// The buffer must remain valid for the lifetime of the registration.
    pub fn register(&mut self, pd: &Pd) -> Result<()> {
        if self.mr.is_some() {
            return Ok(());
        }

        let mr = unsafe {
            pd.register(
                self.ptr.as_ptr(),
                self.capacity,
                AccessFlags::LOCAL_WRITE,
            )?
        };
        self.mr = Some(mr);
        Ok(())
    }

    /// Check if the buffer is registered.
    #[inline]
    pub fn is_registered(&self) -> bool {
        self.mr.is_some()
    }

    /// Get the local key for RDMA operations.
    ///
    /// Returns None if the buffer is not registered.
    #[inline]
    pub fn lkey(&self) -> Option<u32> {
        self.mr.as_ref().map(|mr| mr.lkey())
    }

    /// Get a pointer to the buffer data.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Get a mutable pointer to the buffer data.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the current length of valid data.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set the length of valid data.
    ///
    /// # Panics
    /// Panics if len > capacity.
    #[inline]
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity);
        self.len = len;
    }

    /// Clear the buffer (set length to 0).
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Get the buffer contents as a byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get the buffer contents as a mutable byte slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Get the entire buffer capacity as a mutable byte slice.
    #[inline]
    pub fn as_capacity_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }

    /// Get the buffer address as u64 for SGE.
    #[inline]
    pub fn addr(&self) -> u64 {
        self.ptr.as_ptr() as u64
    }

    /// Get the maximum data size (excluding header space).
    #[inline]
    pub fn max_data_size(&self) -> usize {
        self.capacity.saturating_sub(PKT_HDR_SIZE)
    }

    /// Get a pointer to the data area (after header).
    #[inline]
    pub fn data_ptr(&self) -> *const u8 {
        unsafe { self.ptr.as_ptr().add(PKT_HDR_SIZE) }
    }

    /// Get a mutable pointer to the data area (after header).
    #[inline]
    pub fn data_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.ptr.as_ptr().add(PKT_HDR_SIZE) }
    }

    /// Get the data portion as a slice (excluding header).
    pub fn data(&self) -> &[u8] {
        let data_len = self.len.saturating_sub(PKT_HDR_SIZE);
        unsafe { std::slice::from_raw_parts(self.data_ptr(), data_len) }
    }

    /// Get the data portion as a mutable slice (excluding header).
    pub fn data_mut(&mut self) -> &mut [u8] {
        let data_len = self.len.saturating_sub(PKT_HDR_SIZE);
        unsafe { std::slice::from_raw_parts_mut(self.data_mut_ptr(), data_len) }
    }

    /// Copy data into the buffer.
    pub fn copy_from(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.capacity {
            return Err(Error::BufferTooSmall {
                required: data.len(),
                available: self.capacity,
            });
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr(), data.len());
        }
        self.len = data.len();
        Ok(())
    }

    /// Resize the buffer to a new capacity.
    ///
    /// If the buffer is registered, the registration is dropped and must be re-registered.
    pub fn resize(&mut self, new_capacity: usize) -> Result<()> {
        if new_capacity == 0 {
            return Err(Error::InvalidConfig("Buffer capacity cannot be 0".into()));
        }

        if new_capacity == self.capacity {
            return Ok(());
        }

        // Drop existing MR if registered
        self.mr = None;

        let old_layout = Layout::from_size_align(self.capacity, MSG_BUFFER_ALIGN).unwrap();
        let new_layout = Layout::from_size_align(new_capacity, MSG_BUFFER_ALIGN)
            .map_err(|_| Error::InvalidConfig("Invalid buffer layout".into()))?;

        let new_ptr = unsafe {
            // Allocate new buffer
            let ptr = alloc(new_layout);
            if ptr.is_null() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Failed to allocate buffer",
                )));
            }

            // Copy existing data
            let copy_len = self.len.min(new_capacity);
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), ptr, copy_len);

            // Deallocate old buffer
            dealloc(self.ptr.as_ptr(), old_layout);

            NonNull::new_unchecked(ptr)
        };

        self.ptr = new_ptr;
        self.capacity = new_capacity;
        self.len = self.len.min(new_capacity);

        Ok(())
    }
}

impl Drop for MsgBuffer {
    fn drop(&mut self) {
        // MR is dropped automatically when self.mr is dropped
        let layout = Layout::from_size_align(self.capacity, MSG_BUFFER_ALIGN).unwrap();
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

// Safety: MsgBuffer owns its memory and MR
unsafe impl Send for MsgBuffer {}

/// A pool of pre-allocated message buffers.
pub struct BufferPool {
    buffers: Vec<MsgBuffer>,
    free_list: Vec<usize>,
}

impl BufferPool {
    /// Create a new buffer pool with pre-allocated buffers.
    pub fn new(count: usize, buffer_size: usize, pd: &Pd) -> Result<Self> {
        let mut buffers = Vec::with_capacity(count);
        let mut free_list = Vec::with_capacity(count);

        for i in 0..count {
            let buf = MsgBuffer::new_registered(buffer_size, pd)?;
            buffers.push(buf);
            free_list.push(i);
        }

        Ok(Self { buffers, free_list })
    }

    /// Allocate a buffer from the pool.
    ///
    /// Returns the buffer index and a mutable reference to the buffer.
    pub fn alloc(&mut self) -> Option<(usize, &mut MsgBuffer)> {
        let idx = self.free_list.pop()?;
        self.buffers[idx].clear();
        Some((idx, &mut self.buffers[idx]))
    }

    /// Return a buffer to the pool.
    pub fn free(&mut self, idx: usize) {
        debug_assert!(idx < self.buffers.len());
        debug_assert!(!self.free_list.contains(&idx));
        self.free_list.push(idx);
    }

    /// Get a reference to a buffer by index.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&MsgBuffer> {
        self.buffers.get(idx)
    }

    /// Get a mutable reference to a buffer by index.
    #[inline]
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut MsgBuffer> {
        self.buffers.get_mut(idx)
    }

    /// Get the number of available buffers.
    #[inline]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Get the total number of buffers.
    #[inline]
    pub fn total(&self) -> usize {
        self.buffers.len()
    }
}

// =============================================================================
// Zero-Copy Buffer Pool
// =============================================================================

/// Page size alignment for zero-copy buffers.
pub const PAGE_SIZE: usize = 4096;

/// Fixed-size ring buffer for free list management.
///
/// This provides O(1) alloc/free with no heap allocation and excellent
/// cache locality. The ring buffer is stored inline with the pool.
struct FreeListRing {
    /// Ring buffer storage (boxed for large sizes).
    buffer: Box<[u32]>,
    /// Head index (next slot to pop).
    head: u32,
    /// Tail index (next slot to push).
    tail: u32,
    /// Capacity (power of 2 for fast modulo).
    capacity: u32,
}

impl FreeListRing {
    /// Create a new ring buffer with the given capacity.
    /// Capacity is rounded up to the next power of 2.
    fn new(capacity: usize) -> Self {
        // Round up to power of 2 for fast modulo
        let capacity = capacity.next_power_of_two().max(2) as u32;
        let buffer = vec![0u32; capacity as usize].into_boxed_slice();
        Self {
            buffer,
            head: 0,
            tail: 0,
            capacity,
        }
    }

    /// Initialize with all indices from 0 to count-1.
    fn init_full(&mut self, count: usize) {
        debug_assert!(count <= self.capacity as usize);
        for i in 0..count {
            self.buffer[i] = i as u32;
        }
        self.head = 0;
        self.tail = count as u32;
    }

    /// Pop an index from the ring buffer.
    #[inline]
    fn pop(&mut self) -> Option<usize> {
        if self.head == self.tail {
            return None;
        }
        let idx = self.buffer[(self.head & (self.capacity - 1)) as usize];
        self.head = self.head.wrapping_add(1);
        Some(idx as usize)
    }

    /// Push an index to the ring buffer.
    #[inline]
    fn push(&mut self, idx: usize) {
        debug_assert!(self.len() < self.capacity as usize);
        self.buffer[(self.tail & (self.capacity - 1)) as usize] = idx as u32;
        self.tail = self.tail.wrapping_add(1);
    }

    /// Get the number of elements in the ring buffer.
    #[inline]
    fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head) as usize
    }

    /// Check if the ring buffer is empty.
    #[inline]
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.head == self.tail
    }
}

/// A zero-copy buffer pool with single MR registration.
///
/// This pool allocates a single contiguous memory region and registers it
/// once with the RDMA hardware. Individual slots can be allocated and freed
/// without additional MR registration overhead.
///
/// Based on the ScaleRPC pattern for efficient buffer management.
pub struct ZeroCopyPool {
    /// Pointer to the contiguous buffer.
    buffer: *mut u8,
    /// Single memory region for all slots.
    mr: MemoryRegion,
    /// Size of each slot (page-aligned).
    slot_size: usize,
    /// Total number of slots.
    num_slots: usize,
    /// Free list ring buffer (fixed-size, cache-friendly).
    free_list: FreeListRing,
}

impl ZeroCopyPool {
    /// Create a new zero-copy buffer pool.
    ///
    /// # Arguments
    /// * `num_slots` - Number of buffer slots to allocate
    /// * `slot_size` - Size of each slot (will be rounded up to PAGE_SIZE alignment)
    /// * `pd` - Protection Domain for MR registration
    pub fn new(num_slots: usize, slot_size: usize, pd: &Pd) -> Result<Self> {
        if num_slots == 0 {
            return Err(Error::InvalidConfig("num_slots cannot be 0".into()));
        }
        if slot_size == 0 {
            return Err(Error::InvalidConfig("slot_size cannot be 0".into()));
        }

        // Round up slot_size to PAGE_SIZE alignment
        let slot_size = (slot_size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let total_size = num_slots * slot_size;

        // Allocate page-aligned memory using posix_memalign
        let buffer = unsafe {
            let mut ptr: *mut libc::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, PAGE_SIZE, total_size);
            if ret != 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("posix_memalign failed: {}", ret),
                )));
            }
            // Zero-initialize
            std::ptr::write_bytes(ptr as *mut u8, 0, total_size);
            ptr as *mut u8
        };

        // Register single MR for entire buffer
        let mr = unsafe {
            pd.register(buffer, total_size, AccessFlags::LOCAL_WRITE)?
        };

        // Initialize free list ring buffer with all slots
        let mut free_list = FreeListRing::new(num_slots);
        free_list.init_full(num_slots);

        Ok(Self {
            buffer,
            mr,
            slot_size,
            num_slots,
            free_list,
        })
    }

    /// Allocate a slot from the pool.
    ///
    /// Returns the slot index and a mutable slice to the slot's memory.
    #[inline]
    pub fn alloc(&mut self) -> Option<(usize, &mut [u8])> {
        let idx = self.free_list.pop()?;
        let slice = unsafe {
            let ptr = self.buffer.add(idx * self.slot_size);
            std::slice::from_raw_parts_mut(ptr, self.slot_size)
        };
        Some((idx, slice))
    }

    /// Free a slot back to the pool.
    #[inline]
    pub fn free(&mut self, idx: usize) {
        debug_assert!(idx < self.num_slots);
        self.free_list.push(idx);
    }

    /// Get the local key for RDMA operations.
    ///
    /// All slots share the same lkey.
    #[inline]
    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    /// Get the address of a slot.
    #[inline]
    pub fn slot_addr(&self, idx: usize) -> u64 {
        debug_assert!(idx < self.num_slots);
        (unsafe { self.buffer.add(idx * self.slot_size) }) as u64
    }

    /// Get a pointer to a slot.
    #[inline]
    pub fn slot_ptr(&self, idx: usize) -> *mut u8 {
        debug_assert!(idx < self.num_slots);
        unsafe { self.buffer.add(idx * self.slot_size) }
    }

    /// Get a slice to a slot's memory.
    #[inline]
    pub fn slot_slice(&self, idx: usize) -> &[u8] {
        debug_assert!(idx < self.num_slots);
        unsafe {
            let ptr = self.buffer.add(idx * self.slot_size);
            std::slice::from_raw_parts(ptr, self.slot_size)
        }
    }

    /// Get a mutable slice to a slot's memory.
    #[inline]
    pub fn slot_slice_mut(&mut self, idx: usize) -> &mut [u8] {
        debug_assert!(idx < self.num_slots);
        unsafe {
            let ptr = self.buffer.add(idx * self.slot_size);
            std::slice::from_raw_parts_mut(ptr, self.slot_size)
        }
    }

    /// Get the slot size.
    #[inline]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    /// Get the number of available slots.
    #[inline]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Get the total number of slots.
    #[inline]
    pub fn total(&self) -> usize {
        self.num_slots
    }
}

impl Drop for ZeroCopyPool {
    fn drop(&mut self) {
        // MR is dropped automatically
        unsafe {
            libc::free(self.buffer as *mut libc::c_void);
        }
    }
}

// Safety: ZeroCopyPool owns its memory and MR
unsafe impl Send for ZeroCopyPool {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msg_buffer_basic() {
        let mut buf = MsgBuffer::new(1024).unwrap();
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());

        buf.set_len(100);
        assert_eq!(buf.len(), 100);
        assert!(!buf.is_empty());

        buf.clear();
        assert!(buf.is_empty());
    }

    #[test]
    fn test_msg_buffer_copy() {
        let mut buf = MsgBuffer::new(1024).unwrap();
        let data = b"Hello, World!";

        buf.copy_from(data).unwrap();
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_slice(), data);
    }

    #[test]
    fn test_msg_buffer_alignment() {
        let buf = MsgBuffer::new(1024).unwrap();
        assert_eq!(buf.as_ptr() as usize % MSG_BUFFER_ALIGN, 0);
    }
}
