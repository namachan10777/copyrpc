//! Ring buffer implementation for SPSC (Single Producer, Single Consumer) communication.
//!
//! This module provides the ring buffer primitives used by copyrpc for
//! managing send and receive buffers with RDMA WRITE operations.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::encoding::{ALIGNMENT, padded_message_size};

/// Ring buffer state tracking producer and consumer positions.
///
/// Uses virtual (unwrapped) positions that monotonically increase.
/// The actual buffer offset is computed by taking modulo of the buffer size.
#[derive(Debug)]
pub struct RingBufferState {
    /// Producer position (virtual, monotonically increasing).
    /// Points to the next write position.
    producer: Cell<u64>,
    /// Consumer position (virtual, monotonically increasing).
    /// Points to the next read position.
    consumer: Cell<u64>,
    /// Buffer size (must be power of 2 for efficient modulo).
    size: u64,
}

impl RingBufferState {
    /// Create a new ring buffer state.
    ///
    /// # Arguments
    /// * `size` - Buffer size (should be power of 2)
    pub fn new(size: u64) -> Self {
        debug_assert!(size.is_power_of_two(), "size must be power of 2");
        Self {
            producer: Cell::new(0),
            consumer: Cell::new(0),
            size,
        }
    }

    /// Get the current producer position.
    #[inline]
    pub fn producer(&self) -> u64 {
        self.producer.get()
    }

    /// Get the current consumer position.
    #[inline]
    pub fn consumer(&self) -> u64 {
        self.consumer.get()
    }

    /// Get the buffer size.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Calculate available space for writing.
    ///
    /// Returns the number of bytes that can be written before hitting
    /// the consumer position.
    #[inline]
    pub fn available_write(&self) -> u64 {
        self.size - (self.producer.get() - self.consumer.get())
    }

    /// Calculate available data for reading.
    ///
    /// Returns the number of bytes available for reading.
    #[inline]
    pub fn available_read(&self) -> u64 {
        self.producer.get() - self.consumer.get()
    }

    /// Check if there's enough space to write a message of given size.
    #[inline]
    pub fn can_write(&self, size: u64) -> bool {
        self.available_write() >= size
    }

    /// Advance the producer position after writing.
    ///
    /// # Arguments
    /// * `delta` - Number of bytes written (must be aligned to ALIGNMENT)
    #[inline]
    pub fn advance_producer(&self, delta: u64) {
        debug_assert!(delta.is_multiple_of(ALIGNMENT));
        self.producer.set(self.producer.get() + delta);
    }

    /// Advance the consumer position after reading.
    ///
    /// # Arguments
    /// * `delta` - Number of bytes consumed (must be aligned to ALIGNMENT)
    #[inline]
    pub fn advance_consumer(&self, delta: u64) {
        debug_assert!(delta.is_multiple_of(ALIGNMENT));
        self.consumer.set(self.consumer.get() + delta);
    }

    /// Update consumer position from remote piggyback.
    ///
    /// This is called when we receive the remote's consumer position
    /// through a piggyback field in a message header.
    #[inline]
    pub fn update_consumer(&self, new_consumer: u64) {
        let current = self.consumer.get();
        if new_consumer > current {
            self.consumer.set(new_consumer);
        }
    }

    /// Convert a virtual position to a buffer offset.
    #[inline]
    pub fn to_offset(&self, pos: u64) -> usize {
        (pos & (self.size - 1)) as usize
    }

    /// Check if a write of given size would wrap around the buffer.
    ///
    /// Returns (start_offset, would_wrap)
    #[inline]
    pub fn check_wrap(&self, size: u64) -> (usize, bool) {
        let start = self.to_offset(self.producer.get());
        let end = start + size as usize;
        (start, end > self.size as usize)
    }
}

/// Ring buffer with backing memory.
///
/// The buffer memory is registered with the RDMA device for zero-copy transfers.
pub struct RingBuffer {
    /// Buffer memory (aligned allocation).
    buf: Box<[u8]>,
    /// Ring buffer state.
    state: RingBufferState,
}

impl RingBuffer {
    /// Create a new ring buffer with the given size.
    ///
    /// # Arguments
    /// * `size` - Buffer size (must be power of 2)
    pub fn new(size: usize) -> Self {
        let size = size.next_power_of_two();
        let buf = vec![0u8; size].into_boxed_slice();
        Self {
            buf,
            state: RingBufferState::new(size as u64),
        }
    }

    /// Get a pointer to the buffer for RDMA registration.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    /// Get a mutable pointer to the buffer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    /// Get the buffer length.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Check if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Get a reference to the ring buffer state.
    #[inline]
    pub fn state(&self) -> &RingBufferState {
        &self.state
    }

    /// Get the current producer position.
    #[inline]
    pub fn producer(&self) -> u64 {
        self.state.producer()
    }

    /// Get the current consumer position.
    #[inline]
    pub fn consumer(&self) -> u64 {
        self.state.consumer()
    }

    /// Check if we can write a message of given payload size.
    #[inline]
    pub fn can_write(&self, payload_len: u32) -> bool {
        self.state.can_write(padded_message_size(payload_len))
    }

    /// Get a slice at the given virtual position.
    ///
    /// Note: The slice may wrap around the buffer boundary. Caller must
    /// handle wrap-around cases.
    #[inline]
    pub fn slice_at(&self, pos: u64, len: usize) -> &[u8] {
        let offset = self.state.to_offset(pos);
        &self.buf[offset..offset + len]
    }

    /// Get a mutable slice at the given virtual position.
    #[inline]
    pub fn slice_at_mut(&mut self, pos: u64, len: usize) -> &mut [u8] {
        let offset = self.state.to_offset(pos);
        &mut self.buf[offset..offset + len]
    }

    /// Write data at the current producer position.
    ///
    /// Handles wrap-around by splitting the write if necessary.
    ///
    /// # Returns
    /// The virtual position where data was written.
    pub fn write(&mut self, data: &[u8]) -> u64 {
        let pos = self.state.producer();
        let offset = self.state.to_offset(pos);
        let end = offset + data.len();

        if end <= self.buf.len() {
            // No wrap-around
            self.buf[offset..end].copy_from_slice(data);
        } else {
            // Wrap-around: split the write
            let first_len = self.buf.len() - offset;
            self.buf[offset..].copy_from_slice(&data[..first_len]);
            self.buf[..data.len() - first_len].copy_from_slice(&data[first_len..]);
        }

        pos
    }

    /// Advance producer after writing.
    #[inline]
    pub fn advance_producer(&self, delta: u64) {
        self.state.advance_producer(delta);
    }

    /// Advance consumer after reading.
    #[inline]
    pub fn advance_consumer(&self, delta: u64) {
        self.state.advance_consumer(delta);
    }

    /// Update consumer from remote piggyback.
    #[inline]
    pub fn update_consumer(&self, new_consumer: u64) {
        self.state.update_consumer(new_consumer);
    }
}

/// Remote ring buffer information for RDMA WRITE operations.
///
/// Contains the remote endpoint's receive ring buffer address and rkey.
#[derive(Debug, Clone, Copy)]
pub struct RemoteRingInfo {
    /// Remote buffer virtual address.
    pub addr: u64,
    /// Remote memory region key.
    pub rkey: u32,
    /// Remote buffer size.
    pub size: u64,
}

impl RemoteRingInfo {
    /// Create new remote ring info.
    pub fn new(addr: u64, rkey: u32, size: u64) -> Self {
        Self { addr, rkey, size }
    }

    /// Calculate the remote address for a given virtual position.
    #[inline]
    pub fn remote_addr(&self, pos: u64) -> u64 {
        self.addr + (pos & (self.size - 1))
    }
}

/// Atomic consumer position for receiving piggyback updates.
///
/// This is used to track the remote endpoint's consumer position,
/// which is received through piggyback fields in message headers.
#[derive(Debug)]
pub struct RemoteConsumer {
    /// Last known remote consumer position.
    consumer: AtomicU64,
    /// Flag indicating if we've received at least one update.
    initialized: Cell<bool>,
}

impl RemoteConsumer {
    /// Create a new remote consumer tracker.
    pub fn new() -> Self {
        Self {
            consumer: AtomicU64::new(0),
            initialized: Cell::new(false),
        }
    }

    /// Get the last known remote consumer position.
    #[inline]
    pub fn get(&self) -> u64 {
        self.consumer.load(Ordering::Relaxed)
    }

    /// Check if we've received at least one consumer update.
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.initialized.get()
    }

    /// Update the remote consumer position.
    ///
    /// Only updates if the new value is greater than the current value.
    #[inline]
    pub fn update(&self, new_consumer: u64) {
        self.initialized.set(true);
        let current = self.consumer.load(Ordering::Relaxed);
        if new_consumer > current {
            self.consumer.store(new_consumer, Ordering::Relaxed);
        }
    }
}

impl Default for RemoteConsumer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_state() {
        let state = RingBufferState::new(1024);
        assert_eq!(state.producer(), 0);
        assert_eq!(state.consumer(), 0);
        assert_eq!(state.available_write(), 1024);
        assert_eq!(state.available_read(), 0);

        state.advance_producer(64);
        assert_eq!(state.producer(), 64);
        assert_eq!(state.available_write(), 960);
        assert_eq!(state.available_read(), 64);

        state.advance_consumer(32);
        assert_eq!(state.consumer(), 32);
        assert_eq!(state.available_write(), 992);
        assert_eq!(state.available_read(), 32);
    }

    #[test]
    fn test_ring_buffer_wrap() {
        let state = RingBufferState::new(256);

        // Fill most of the buffer
        state.advance_producer(224);
        state.advance_consumer(224);

        // Check wrap detection
        let (offset, would_wrap) = state.check_wrap(64);
        assert_eq!(offset, 224);
        assert!(would_wrap);

        let (offset, would_wrap) = state.check_wrap(32);
        assert_eq!(offset, 224);
        assert!(!would_wrap);
    }

    #[test]
    fn test_ring_buffer_write() {
        let mut ring = RingBuffer::new(256);
        let data = [1u8, 2, 3, 4, 5, 6, 7, 8];

        let pos = ring.write(&data);
        assert_eq!(pos, 0);

        let slice = ring.slice_at(0, 8);
        assert_eq!(slice, &data);
    }

    #[test]
    fn test_remote_consumer() {
        let rc = RemoteConsumer::new();
        assert!(!rc.is_initialized());
        assert_eq!(rc.get(), 0);

        rc.update(100);
        assert!(rc.is_initialized());
        assert_eq!(rc.get(), 100);

        // Should not decrease
        rc.update(50);
        assert_eq!(rc.get(), 100);

        rc.update(200);
        assert_eq!(rc.get(), 200);
    }
}
