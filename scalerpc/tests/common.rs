//! Common test utilities for scalerpc integration tests.
//!
//! Reuses mlx5 test utilities pattern.

// Test utilities may not all be used in every test file
#![allow(dead_code)]

use mlx5::device::{Context, DeviceList};
use mlx5::pd::{AccessFlags, Pd};
use mlx5::types::PortAttr;

/// Page size for aligned allocations.
pub const PAGE_SIZE: usize = 4096;

/// Find an available mlx5 device and open it.
pub fn open_mlx5_device() -> Option<Context> {
    let device_list = DeviceList::list().ok()?;

    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
            return Some(ctx);
        }
    }

    None
}

/// Skip test if no IB device is available.
#[macro_export]
macro_rules! require_device {
    () => {{
        match $crate::common::open_mlx5_device() {
            Some(ctx) => ctx,
            None => {
                eprintln!("Skipping test: no mlx5 device available");
                return;
            }
        }
    }};
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

/// Aligned buffer with automatic cleanup.
pub struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
}

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
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut std::ffi::c_void);
        }
    }
}
