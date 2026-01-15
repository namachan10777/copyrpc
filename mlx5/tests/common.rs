//! Common test utilities for mlx5 integration tests.
//!
//! This module provides helper functions and types for writing RDMA tests.

use std::cell::RefCell;
use std::rc::Rc;

use mlx5::cq::{CompletionQueue, Cqe};
use mlx5::device::{Context, DeviceList};
use mlx5::pd::{AccessFlags, Pd};
use mlx5::types::PortAttr;

/// Page size for aligned allocations.
pub const PAGE_SIZE: usize = 4096;

/// Find an available mlx5 device and open it.
pub fn open_mlx5_device() -> Option<Context> {
    let device_list = DeviceList::list().ok()?;

    for device in device_list.iter() {
        // Check if this is an mlx5 device by trying to open it
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
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut std::ffi::c_void);
        }
    }
}

/// Poll CQ until completion or timeout.
pub fn poll_cq_timeout(cq: &mut CompletionQueue, timeout_ms: u64) -> Option<Cqe> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    loop {
        if let Some(cqe) = cq.poll_one() {
            return Some(cqe);
        }
        if start.elapsed() > timeout {
            return None;
        }
        std::hint::spin_loop();
    }
}

/// Poll CQ for multiple completions.
pub fn poll_cq_batch(
    cq: &mut CompletionQueue,
    count: usize,
    timeout_ms: u64,
) -> Result<Vec<Cqe>, String> {
    let mut completions = Vec::with_capacity(count);
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    while completions.len() < count {
        if let Some(cqe) = cq.poll_one() {
            completions.push(cqe);
        }
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout: got {} of {} completions",
                completions.len(),
                count
            ));
        }
        std::hint::spin_loop();
    }
    Ok(completions)
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
///
/// IMPORTANT: Field order matters for drop order! In Rust, struct fields
/// are dropped in declaration order. Resources must be dropped before
/// the context they depend on:
/// - pd (and all QPs/CQs using it) must be dropped before ctx
pub struct TestContext {
    /// Protection Domain - dropped first (before ctx)
    pub pd: Rc<Pd>,
    /// Port attributes (plain data, no cleanup needed)
    pub port_attr: PortAttr,
    /// Port number (plain data, no cleanup needed)
    pub port: u8,
    /// Device context - dropped last (after pd and all resources)
    pub ctx: Context,
}

impl TestContext {
    /// Create a new test context.
    pub fn new() -> Option<Self> {
        let ctx = open_mlx5_device()?;

        let port = 1u8;
        let port_attr = ctx.query_port(port).ok()?;
        let pd = Rc::new(ctx.alloc_pd().ok()?);

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

/// Check if DCT (DC Target) is supported via verbs API.
///
/// DCT activation requires kernel driver support that may not be available
/// on all systems. Returns true if DCT can be activated.
pub fn is_dct_supported(ctx: &TestContext) -> bool {
    use mlx5::dc::DctConfig;
    use mlx5::srq::SrqConfig;

    let dct_cq = match ctx.ctx.create_cq(16) {
        Ok(cq) => cq,
        Err(e) => {
            eprintln!("  DCT check: CQ creation failed: {}", e);
            return false;
        }
    };

    let srq_config = SrqConfig {
        max_wr: 16,
        max_sge: 1,
    };
    let srq = match ctx.pd.create_srq(&srq_config) {
        Ok(srq) => srq,
        Err(e) => {
            eprintln!("  DCT check: SRQ creation failed: {}", e);
            return false;
        }
    };

    let dct_config = DctConfig { dc_key: 0x12345 };
    let mut dct = match ctx.ctx.create_dct(&ctx.pd, &srq, &dct_cq, &dct_config) {
        Ok(dct) => dct,
        Err(e) => {
            eprintln!("  DCT check: DCT creation failed: {}", e);
            return false;
        }
    };

    eprintln!("  DCT check: DCT created, DCTN=0x{:x}", dct.dctn());

    let access = full_access().bits();
    match dct.modify_to_init(ctx.port, access) {
        Ok(_) => eprintln!("  DCT check: INIT succeeded"),
        Err(e) => {
            eprintln!("  DCT check: INIT failed: {} (raw: {:?})", e, e.raw_os_error());
            return false;
        }
    }

    match dct.modify_to_rtr(ctx.port, 12) {
        Ok(_) => {
            eprintln!("  DCT check: RTR succeeded, DCTN=0x{:x}", dct.dctn());
            true
        }
        Err(e) => {
            eprintln!("  DCT check: RTR failed: {} (raw: {:?})", e, e.raw_os_error());
            false
        }
    }
}

/// Check if TM-SRQ (Tag Matching SRQ) is supported via verbs API.
///
/// TM-SRQ creation requires kernel driver support that may not be available
/// on all systems. Returns true if TM-SRQ can be created.
pub fn is_tm_srq_supported(ctx: &TestContext) -> bool {
    use std::cell::RefCell;
    use std::rc::Rc;
    use mlx5::tm_srq::TmSrqConfig;

    // First check device TM capabilities
    match ctx.ctx.query_tm_caps() {
        Some(caps) => {
            eprintln!("  TM-SRQ check: Device TM caps:");
            eprintln!("    max_num_tags: {}", caps.max_num_tags);
            eprintln!("    max_ops: {}", caps.max_ops);
            eprintln!("    max_sge: {}", caps.max_sge);
            eprintln!("    flags: 0x{:x}", caps.flags);
            if caps.max_num_tags == 0 {
                eprintln!("  TM-SRQ check: TM not supported (max_num_tags=0)");
                return false;
            }
        }
        None => {
            eprintln!("  TM-SRQ check: query_tm_caps failed");
            return false;
        }
    }

    let cq = match ctx.ctx.create_cq(16) {
        Ok(cq) => Rc::new(RefCell::new(cq)),
        Err(e) => {
            eprintln!("  TM-SRQ check: CQ creation failed: {}", e);
            return false;
        }
    };

    // Use power of 2 for max_wr and reasonable values for TM params
    let config = TmSrqConfig {
        max_wr: 256,  // Must be power of 2 for SRQ
        max_sge: 1,
        max_num_tags: 64,  // Within device max of 127
        max_ops: 16,
    };
    eprintln!("  TM-SRQ check: trying config: max_wr={}, max_num_tags={}, max_ops={}",
              config.max_wr, config.max_num_tags, config.max_ops);

    match ctx.ctx.create_tm_srq::<u64, _>(&ctx.pd, &cq, &config, |_, _| {}) {
        Ok(_) => {
            eprintln!("  TM-SRQ check: creation succeeded");
            true
        }
        Err(e) => {
            eprintln!("  TM-SRQ check: creation failed: {} (raw: {:?})", e, e.raw_os_error());
            false
        }
    }
}

/// Skip test if DCT is not supported.
#[macro_export]
macro_rules! require_dct {
    ($ctx:expr) => {{
        if !$crate::common::is_dct_supported($ctx) {
            eprintln!("Skipping test: DCT not supported via verbs API (kernel/driver limitation)");
            return;
        }
    }};
}

/// Skip test if TM-SRQ is not supported.
#[macro_export]
macro_rules! require_tm_srq {
    ($ctx:expr) => {{
        if !$crate::common::is_tm_srq_supported($ctx) {
            eprintln!("Skipping test: TM-SRQ not supported via verbs API (kernel/driver limitation)");
            return;
        }
    }};
}
