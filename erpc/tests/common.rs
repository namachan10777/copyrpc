//! Common test utilities for eRPC integration tests.

#![allow(dead_code)]

use mlx5::device::{Context, DeviceList};
use mlx5::pd::Pd;
use mlx5::types::PortAttr;

/// Page size for aligned allocations.
pub const PAGE_SIZE: usize = 4096;

/// Test context with device, PD, and port info.
pub struct TestContext {
    pub ctx: Context,
    pub pd: Pd,
    pub port: u8,
    pub port_attr: PortAttr,
}

impl TestContext {
    /// Create a new test context.
    pub fn new() -> Option<Self> {
        let device_list = DeviceList::list().ok()?;

        for device in device_list.iter() {
            if let Ok(ctx) = device.open() {
                // Find an active port
                for port in 1..=2u8 {
                    if let Ok(port_attr) = ctx.query_port(port) {
                        if port_attr.state == mlx5::types::PortState::Active {
                            let pd = ctx.alloc_pd().ok()?;
                            return Some(Self {
                                ctx,
                                pd,
                                port,
                                port_attr,
                            });
                        }
                    }
                }
            }
        }

        None
    }
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

    /// Get as slice.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    /// Get as mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut std::ffi::c_void);
        }
    }
}

unsafe impl Send for AlignedBuffer {}

/// Poll completion queue with timeout.
pub fn poll_cq_timeout(cq: &mlx5::cq::Cq, timeout_ms: u64) -> usize {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    loop {
        let count = cq.poll();
        if count > 0 {
            cq.flush();
            return count;
        }

        if start.elapsed() > timeout {
            return 0;
        }

        std::hint::spin_loop();
    }
}
