//! Common test utilities for mlx5 integration tests.
//!
//! When the `test-utils` feature is enabled, utilities are re-exported from
//! `mlx5::test_utils`. Otherwise, inline definitions are used as fallback.

#![allow(dead_code)]

// When test-utils feature is enabled, re-export from the crate module
#[cfg(feature = "test-utils")]
pub use mlx5::test_utils::*;

// When test-utils feature is NOT enabled, provide inline definitions
#[cfg(not(feature = "test-utils"))]
mod inline {
    use mlx5::cq::{Cq, Cqe};
    use mlx5::device::{Context, DeviceList};
    use mlx5::pd::{AccessFlags, Pd};
    use mlx5::types::PortAttr;

    pub const PAGE_SIZE: usize = 4096;

    pub fn open_mlx5_device() -> Option<Context> {
        let device_list = DeviceList::list().ok()?;
        for device in device_list.iter() {
            if let Ok(ctx) = device.open_devx() {
                return Some(ctx);
            }
        }
        None
    }

    pub struct AlignedBuffer {
        ptr: *mut u8,
        size: usize,
    }

    unsafe impl Send for AlignedBuffer {}

    impl AlignedBuffer {
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

        pub fn as_ptr(&self) -> *mut u8 {
            self.ptr
        }

        pub fn addr(&self) -> u64 {
            self.ptr as u64
        }

        pub fn size(&self) -> usize {
            self.size
        }

        pub fn as_slice(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
        }

        pub fn as_mut_slice(&mut self) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
        }

        pub fn fill(&mut self, pattern: u8) {
            unsafe {
                std::ptr::write_bytes(self.ptr, pattern, self.size);
            }
        }

        pub fn fill_bytes(&mut self, data: &[u8]) {
            let len = data.len().min(self.size);
            unsafe {
                std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr, len);
            }
        }

        pub fn read_bytes(&self, len: usize) -> Vec<u8> {
            let len = len.min(self.size);
            let mut buf = vec![0u8; len];
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), len);
            }
            buf
        }

        pub fn read_u32(&self, offset: usize) -> u32 {
            assert!(offset + 4 <= self.size);
            unsafe { std::ptr::read_volatile((self.ptr.add(offset)) as *const u32) }
        }

        pub fn write_u32(&mut self, offset: usize, value: u32) {
            assert!(offset + 4 <= self.size);
            unsafe {
                std::ptr::write_volatile((self.ptr.add(offset)) as *mut u32, value);
            }
        }

        pub fn read_u64(&self, offset: usize) -> u64 {
            assert!(offset + 8 <= self.size);
            unsafe { std::ptr::read_volatile((self.ptr.add(offset)) as *const u64) }
        }

        pub fn write_u64(&mut self, offset: usize, value: u64) {
            assert!(offset + 8 <= self.size);
            unsafe {
                std::ptr::write_volatile((self.ptr.add(offset)) as *mut u64, value);
            }
        }

        pub fn read_u128(&self, offset: usize) -> u128 {
            assert!(offset + 16 <= self.size);
            unsafe { std::ptr::read_volatile((self.ptr.add(offset)) as *const u128) }
        }

        pub fn write_u128(&mut self, offset: usize, value: u128) {
            assert!(offset + 16 <= self.size);
            unsafe {
                std::ptr::write_volatile((self.ptr.add(offset)) as *mut u128, value);
            }
        }

        pub fn read_u128_hilo(&self, offset: usize) -> u128 {
            assert!(offset + 16 <= self.size);
            unsafe {
                let ptr64 = self.ptr.add(offset) as *const u64;
                let high = std::ptr::read_volatile(ptr64);
                let low = std::ptr::read_volatile(ptr64.add(1));
                ((high as u128) << 64) | (low as u128)
            }
        }

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

    pub fn assert_cqe_success(cqe: &Cqe) {
        assert_eq!(
            cqe.syndrome, 0,
            "CQE error: opcode={:?}, syndrome={}",
            cqe.opcode, cqe.syndrome
        );
    }

    pub struct TestContext {
        pub ctx: Context,
        pub pd: Pd,
        pub port_attr: PortAttr,
        pub port: u8,
    }

    impl TestContext {
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

    pub fn full_access() -> AccessFlags {
        AccessFlags::LOCAL_WRITE
            | AccessFlags::REMOTE_WRITE
            | AccessFlags::REMOTE_READ
            | AccessFlags::REMOTE_ATOMIC
    }
}

#[cfg(not(feature = "test-utils"))]
pub use inline::*;

// =========================================================================
// mlx5-specific test helpers (always available, not shared via test-utils)
// =========================================================================

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

/// Check if DCT (DC Target) is supported via verbs API.
pub fn is_dct_supported(ctx: &TestContext) -> bool {
    use mlx5::cq::CqConfig;
    use mlx5::dc::DctConfig;
    use mlx5::srq::SrqConfig;

    let dct_cq = match ctx.ctx.create_cq(16, &CqConfig::default()) {
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
    let srq: mlx5::srq::Srq<()> = match ctx.pd.create_srq(&srq_config) {
        Ok(srq) => srq,
        Err(e) => {
            eprintln!("  DCT check: SRQ creation failed: {}", e);
            return false;
        }
    };

    let dct_config = DctConfig { dc_key: 0x12345 };
    let mut dct = match ctx
        .ctx
        .dct_builder(&ctx.pd, &srq, &dct_config)
        .recv_cq(&dct_cq)
        .build()
    {
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
            eprintln!(
                "  DCT check: INIT failed: {} (raw: {:?})",
                e,
                e.raw_os_error()
            );
            return false;
        }
    }

    match dct.modify_to_rtr(ctx.port, 12) {
        Ok(_) => {
            eprintln!("  DCT check: RTR succeeded, DCTN=0x{:x}", dct.dctn());
            true
        }
        Err(e) => {
            eprintln!(
                "  DCT check: RTR failed: {} (raw: {:?})",
                e,
                e.raw_os_error()
            );
            false
        }
    }
}

/// Check if TM-SRQ (Tag Matching SRQ) is supported via verbs API.
pub fn is_tm_srq_supported(ctx: &TestContext) -> bool {
    use mlx5::cq::CqConfig;
    use mlx5::tm_srq::TmSrqConfig;
    use std::rc::Rc;

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

    let cq = match ctx.ctx.create_cq(16, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(e) => {
            eprintln!("  TM-SRQ check: CQ creation failed: {}", e);
            return false;
        }
    };

    let config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 64,
        max_ops: 16,
    };
    eprintln!(
        "  TM-SRQ check: trying config: max_wr={}, max_num_tags={}, max_ops={}",
        config.max_wr, config.max_num_tags, config.max_ops
    );

    match ctx
        .ctx
        .create_tm_srq::<u64, u64, _>(&ctx.pd, &cq, &config, |_| {})
    {
        Ok(_) => {
            eprintln!("  TM-SRQ check: creation succeeded");
            true
        }
        Err(e) => {
            eprintln!(
                "  TM-SRQ check: creation failed: {} (raw: {:?})",
                e,
                e.raw_os_error()
            );
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
            eprintln!(
                "Skipping test: TM-SRQ not supported via verbs API (kernel/driver limitation)"
            );
            return;
        }
    }};
}
