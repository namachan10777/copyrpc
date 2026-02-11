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
    /// Per-file chunk capacity (path_hash → chunk_size).
    file_chunk_size: HashMap<u64, usize>,
    /// Maximum (default) chunk capacity.
    max_chunk_capacity: usize,
}

impl PmemStore {
    /// Create a new PmemStore backed by the given pmem region.
    ///
    /// `max_chunk_capacity` is the default/maximum chunk size in bytes.
    pub fn new(region: Box<dyn PmemRegion + Send>, max_chunk_capacity: usize) -> Self {
        Self {
            region,
            chunk_table: HashMap::new(),
            next_offset: 0,
            file_chunk_size: HashMap::new(),
            max_chunk_capacity,
        }
    }

    /// Register a file's chunk size. Must be called before writing data chunks.
    pub fn register_file(&mut self, path_hash: u64, chunk_size: usize) {
        self.file_chunk_size.insert(path_hash, chunk_size);
    }

    /// Get the effective chunk capacity for a file.
    fn effective_chunk_size(&self, path_hash: u64) -> usize {
        self.file_chunk_size
            .get(&path_hash)
            .copied()
            .unwrap_or(self.max_chunk_capacity)
    }

    /// Ensure a chunk exists, allocating `capacity` bytes if necessary.
    fn ensure_chunk(
        &mut self,
        path_hash: u64,
        chunk_idx: u32,
        capacity: usize,
    ) -> &mut ChunkLocation {
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
        let capacity = self.effective_chunk_size(path_hash);
        let end = offset + data.len();
        assert!(end <= capacity, "write exceeds chunk capacity");

        let chunk = self.ensure_chunk(path_hash, chunk_idx, capacity);
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
        if chunk_idx == 0 {
            self.file_chunk_size.remove(&path_hash);
        }
        self.chunk_table.remove(&(path_hash, chunk_idx)).is_some()
    }

    /// Prepare a chunk region for zero-copy RDMA write.
    ///
    /// Ensures the chunk is allocated and returns a mutable pointer to the
    /// destination at `offset` within the chunk, plus the remaining capacity.
    /// After RDMA completes, call `commit_write()` to update valid_len and persist.
    pub fn prepare_write(
        &mut self,
        path_hash: u64,
        chunk_idx: u32,
        offset: usize,
    ) -> (*mut u8, usize) {
        let capacity = self.effective_chunk_size(path_hash);
        assert!(offset <= capacity);

        let chunk = self.ensure_chunk(path_hash, chunk_idx, capacity);
        let chunk_offset = chunk.offset;

        let ptr = unsafe { self.region.as_ptr().add(chunk_offset + offset) };
        (ptr, capacity - offset)
    }

    /// Commit a zero-copy write: update valid_len and persist the written region.
    pub fn commit_write(&mut self, path_hash: u64, chunk_idx: u32, offset: usize, len: usize) {
        let chunk = self
            .chunk_table
            .get_mut(&(path_hash, chunk_idx))
            .expect("commit_write: chunk must exist");
        let end = offset + len;
        if end > chunk.valid_len {
            chunk.valid_len = end;
        }
        let ptr = unsafe { self.region.as_ptr().add(chunk.offset + offset) };
        unsafe {
            pmem::persist(ptr, len);
        }
    }

    /// Get a mutable pointer and available length for reading from a chunk.
    /// Used for zero-copy RDMA WRITE from pmem to remote client.
    pub fn ptr_for_chunk_read(
        &self,
        path_hash: u64,
        chunk_idx: u32,
        offset: usize,
    ) -> Option<(*mut u8, usize)> {
        let chunk = self.chunk_table.get(&(path_hash, chunk_idx))?;
        if offset >= chunk.valid_len {
            return Some((std::ptr::null_mut(), 0));
        }
        let available = chunk.valid_len - offset;
        let ptr = unsafe { self.region.as_ptr().add(chunk.offset + offset) };
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

    /// Get the effective chunk size for a file (public accessor).
    pub fn chunk_size_for(&self, path_hash: u64) -> usize {
        self.effective_chunk_size(path_hash)
    }
}
