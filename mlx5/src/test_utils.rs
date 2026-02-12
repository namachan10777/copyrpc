//! Test utilities for RDMA integration tests and benchmarks.
//!
//! This module provides common helpers for writing tests that interact with
//! RDMA hardware. Enable the `test-utils` feature to use these utilities.

use crate::cq::{Cq, Cqe};
use crate::device::{Context, DeviceList};
use crate::pd::{AccessFlags, Pd};
use crate::types::PortAttr;

/// Page size for aligned allocations.
pub const PAGE_SIZE: usize = 4096;

/// Find an available mlx5 device and open it with DevX support.
pub fn open_mlx5_device() -> Option<Context> {
    let device_list = DeviceList::list().ok()?;

    for device in device_list.iter() {
        if let Ok(ctx) = device.open_devx() {
            return Some(ctx);
        }
    }

    None
}

/// Aligned buffer with automatic cleanup.
pub struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
}

unsafe impl Send for AlignedBuffer {}

impl AlignedBuffer {
    /// Allocate page-aligned memory.
    pub fn new(size: usize) -> Self {
        let aligned_size = (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let ptr = unsafe {
            let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, PAGE_SIZE, aligned_size);
            if ret != 0 {
                panic!("posix_memalign failed: {}", ret);
            }
            std::ptr::write_bytes(ptr as *mut u8, 0, aligned_size);
            ptr as *mut u8
        };
        Self {
            ptr,
            size: aligned_size,
        }
    }

    /// Get the buffer pointer.
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Get the buffer address as u64.
    pub fn addr(&self) -> u64 {
        self.ptr as u64
    }

    /// Get the buffer size.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the buffer as an immutable slice.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// Get the buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }

    /// Fill the buffer with a pattern.
    pub fn fill(&mut self, pattern: u8) {
        unsafe {
            std::ptr::write_bytes(self.ptr, pattern, self.size);
        }
    }

    /// Fill the buffer with specific bytes.
    pub fn fill_bytes(&mut self, data: &[u8]) {
        let len = data.len().min(self.size);
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr, len);
        }
    }

    /// Read bytes from the buffer.
    pub fn read_bytes(&self, len: usize) -> Vec<u8> {
        let len = len.min(self.size);
        let mut buf = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), len);
        }
        buf
    }

    /// Read a u32 value at the given offset.
    pub fn read_u32(&self, offset: usize) -> u32 {
        assert!(offset + 4 <= self.size);
        unsafe { std::ptr::read_volatile((self.ptr.add(offset)) as *const u32) }
    }

    /// Write a u32 value at the given offset.
    pub fn write_u32(&mut self, offset: usize, value: u32) {
        assert!(offset + 4 <= self.size);
        unsafe {
            std::ptr::write_volatile((self.ptr.add(offset)) as *mut u32, value);
        }
    }

    /// Read a u64 value at the given offset.
    pub fn read_u64(&self, offset: usize) -> u64 {
        assert!(offset + 8 <= self.size);
        unsafe { std::ptr::read_volatile((self.ptr.add(offset)) as *const u64) }
    }

    /// Write a u64 value at the given offset.
    pub fn write_u64(&mut self, offset: usize, value: u64) {
        assert!(offset + 8 <= self.size);
        unsafe {
            std::ptr::write_volatile((self.ptr.add(offset)) as *mut u64, value);
        }
    }

    /// Read a u128 value at the given offset.
    pub fn read_u128(&self, offset: usize) -> u128 {
        assert!(offset + 16 <= self.size);
        unsafe { std::ptr::read_volatile((self.ptr.add(offset)) as *const u128) }
    }

    /// Write a u128 value at the given offset.
    pub fn write_u128(&mut self, offset: usize, value: u128) {
        assert!(offset + 16 <= self.size);
        unsafe {
            std::ptr::write_volatile((self.ptr.add(offset)) as *mut u128, value);
        }
    }

    /// Read a u128 value at the given offset as two native u64 values (high first, low second).
    pub fn read_u128_hilo(&self, offset: usize) -> u128 {
        assert!(offset + 16 <= self.size);
        unsafe {
            let ptr64 = self.ptr.add(offset) as *const u64;
            let high = std::ptr::read_volatile(ptr64);
            let low = std::ptr::read_volatile(ptr64.add(1));
            ((high as u128) << 64) | (low as u128)
        }
    }

    /// Write a u128 value at the given offset as two native u64 values (high first, low second).
    pub fn write_u128_hilo(&mut self, offset: usize, value: u128) {
        assert!(offset + 16 <= self.size);
        unsafe {
            let ptr64 = self.ptr.add(offset) as *mut u64;
            let high = (value >> 64) as u64;
            let low = value as u64;
            std::ptr::write_volatile(ptr64, high);
            std::ptr::write_volatile(ptr64.add(1), low);
        }
    }

    /// Read a u128 value at the given offset where each 64-bit part is in big-endian format.
    pub fn read_u128_be(&self, offset: usize) -> u128 {
        assert!(offset + 16 <= self.size);
        unsafe {
            let ptr64 = self.ptr.add(offset) as *const u64;
            let high_raw = std::ptr::read_volatile(ptr64);
            let low_raw = std::ptr::read_volatile(ptr64.add(1));
            let high = u64::from_be(high_raw);
            let low = u64::from_be(low_raw);
            ((high as u128) << 64) | (low as u128)
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut std::ffi::c_void);
        }
    }
}

/// Poll CQ until completion or timeout using send CQ's poll() method.
pub fn poll_cq_timeout(cq: &Cq, timeout_ms: u64) -> Option<Cqe> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    loop {
        let count = cq.poll();
        cq.flush();
        if count > 0 {
            return Some(Cqe::default());
        }
        if start.elapsed() > timeout {
            return None;
        }
        std::hint::spin_loop();
    }
}

/// Poll CQ for multiple completions using poll() method.
pub fn poll_cq_batch(cq: &Cq, count: usize, timeout_ms: u64) -> Result<Vec<Cqe>, String> {
    let mut completions_count = 0;
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    while completions_count < count {
        let n = cq.poll();
        cq.flush();
        completions_count += n;
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout: got {} of {} completions",
                completions_count, count
            ));
        }
        std::hint::spin_loop();
    }
    Ok((0..count).map(|_| Cqe::default()).collect())
}

/// Assert CQE is successful.
pub fn assert_cqe_success(cqe: &Cqe) {
    assert_eq!(
        cqe.syndrome, 0,
        "CQE error: opcode={:?}, syndrome={}",
        cqe.opcode, cqe.syndrome
    );
}

/// Test context with common RDMA resources.
pub struct TestContext {
    /// Device context
    pub ctx: Context,
    /// Protection Domain
    pub pd: Pd,
    /// Port attributes
    pub port_attr: PortAttr,
    /// Port number
    pub port: u8,
}

impl TestContext {
    /// Create a new test context.
    pub fn new() -> Option<Self> {
        let ctx = open_mlx5_device()?;

        let port = 1u8;
        let port_attr = ctx.query_port(port).ok()?;
        let pd = ctx.alloc_pd().ok()?;

        Some(Self {
            ctx,
            pd,
            port,
            port_attr,
        })
    }
}

/// Full access flags for RDMA operations.
pub fn full_access() -> AccessFlags {
    AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_ATOMIC
}
