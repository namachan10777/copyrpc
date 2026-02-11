//! PmemStore: chunk-based persistent memory storage.
//!
//! Manages allocation and access to fixed-size chunks backed by
//! persistent memory (DevDax or FileRegion).

use std::collections::HashMap;

use pmem::PmemRegion;

/// Location of a chunk within the pmem region.
struct ChunkLocation {
    /// Byte offset from the start of the region.
    offset: usize,
    /// Number of valid bytes in this chunk.
    valid_len: usize,
}

/// Persistent memory storage organized as fixed-size chunks.
pub struct PmemStore {
    /// Backing pmem region.
    region: Box<dyn PmemRegion + Send>,
    /// Mapping from (path_hash, chunk_index) to location in the region.
    chunk_table: HashMap<(u64, u32), ChunkLocation>,
    /// Next free offset for allocation (bump allocator).
    next_offset: usize,
    /// Maximum bytes per chunk.
    chunk_capacity: usize,
}

impl PmemStore {
    /// Create a new PmemStore backed by the given pmem region.
    ///
    /// `chunk_capacity` is the maximum size of each chunk in bytes.
    pub fn new(region: Box<dyn PmemRegion + Send>, chunk_capacity: usize) -> Self {
        Self {
            region,
            chunk_table: HashMap::new(),
            next_offset: 0,
            chunk_capacity,
        }
    }

    /// Ensure a chunk exists, allocating if necessary.
    /// Returns a mutable pointer to the chunk data at the given offset.
    fn ensure_chunk(&mut self, path_hash: u64, chunk_idx: u32) -> &mut ChunkLocation {
        let capacity = self.chunk_capacity;
        let region_len = self.region.len();
        let next_offset = &mut self.next_offset;

        self.chunk_table
            .entry((path_hash, chunk_idx))
            .or_insert_with(|| {
                let offset = *next_offset;
                assert!(offset + capacity <= region_len, "PmemStore: out of space");
                *next_offset = offset + capacity;
                ChunkLocation {
                    offset,
                    valid_len: 0,
                }
            })
    }

    /// Write data to a chunk at the given offset within the chunk.
    pub fn write(&mut self, path_hash: u64, chunk_idx: u32, offset: usize, data: &[u8]) {
        let end = offset + data.len();
        assert!(end <= self.chunk_capacity, "write exceeds chunk capacity");

        let chunk = self.ensure_chunk(path_hash, chunk_idx);
        let chunk_offset = chunk.offset;

        if end > chunk.valid_len {
            chunk.valid_len = end;
        }

        let dst = unsafe { self.region.as_ptr().add(chunk_offset + offset) };
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
            pmem::persist(dst, data.len());
        }
    }

    /// Read data from a chunk at the given offset.
    /// Returns the number of bytes actually read.
    pub fn read(&self, path_hash: u64, chunk_idx: u32, offset: usize, buf: &mut [u8]) -> usize {
        let chunk = match self.chunk_table.get(&(path_hash, chunk_idx)) {
            Some(c) => c,
            None => return 0,
        };

        if offset >= chunk.valid_len {
            return 0;
        }

        let available = chunk.valid_len - offset;
        let to_read = buf.len().min(available);

        let src = unsafe { self.region.as_ptr().add(chunk.offset + offset) };
        unsafe {
            std::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), to_read);
        }

        to_read
    }

    /// Remove a chunk. Returns true if it existed.
    pub fn remove(&mut self, path_hash: u64, chunk_idx: u32) -> bool {
        self.chunk_table.remove(&(path_hash, chunk_idx)).is_some()
    }

    /// Get a raw pointer and valid length for a chunk region.
    /// Used for RDMA operations (register + DMA).
    pub fn ptr_for_chunk(
        &self,
        path_hash: u64,
        chunk_idx: u32,
        offset: usize,
    ) -> Option<(*const u8, usize)> {
        let chunk = self.chunk_table.get(&(path_hash, chunk_idx))?;
        if offset >= chunk.valid_len {
            return Some((std::ptr::null(), 0));
        }
        let available = chunk.valid_len - offset;
        let ptr = unsafe { self.region.as_ptr().add(chunk.offset + offset) as *const u8 };
        Some((ptr, available))
    }

    /// Get the base pointer of the entire pmem region (for MR registration).
    pub fn region_ptr(&self) -> *mut u8 {
        self.region.as_ptr()
    }

    /// Get the total size of the pmem region.
    pub fn region_len(&self) -> usize {
        self.region.len()
    }
}
