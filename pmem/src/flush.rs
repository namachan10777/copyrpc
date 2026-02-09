use std::sync::OnceLock;

/// Cache flush methods available on x86_64
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMethod {
    /// CLWB - Cache Line Write Back (preferred, non-evicting)
    Clwb,
    /// CLFLUSHOPT - Optimized Cache Line Flush (evicting, weakly ordered)
    ClflushOpt,
    /// CLFLUSH - Cache Line Flush (evicting, strongly ordered)
    Clflush,
}

static FLUSH_METHOD: OnceLock<FlushMethod> = OnceLock::new();

/// Detect the best available cache flush method using CPUID
pub fn detect_flush_method() -> FlushMethod {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::__cpuid;

        // Check for CLWB (CPUID.07H:EBX.CLWB[bit 24])
        let cpuid_7 = __cpuid(7);
        if (cpuid_7.ebx & (1 << 24)) != 0 {
            return FlushMethod::Clwb;
        }

        // Check for CLFLUSHOPT (CPUID.07H:EBX.CLFLUSHOPT[bit 23])
        if (cpuid_7.ebx & (1 << 23)) != 0 {
            return FlushMethod::ClflushOpt;
        }

        // Check for CLFLUSH (CPUID.01H:EDX.CLFSH[bit 19])
        let cpuid_1 = __cpuid(1);
        if (cpuid_1.edx & (1 << 19)) != 0 {
            return FlushMethod::Clflush;
        }

        // Fallback to CLFLUSH (should always be available on x86_64)
        FlushMethod::Clflush
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        FlushMethod::Clflush
    }
}

/// Get the cached flush method, detecting it on first call
fn get_flush_method() -> FlushMethod {
    *FLUSH_METHOD.get_or_init(detect_flush_method)
}

/// Flush cache lines covering the specified address range
///
/// # Safety
///
/// The caller must ensure that `addr` points to a valid memory range of at least `len` bytes.
/// This function assumes the memory is mapped and accessible.
#[inline]
pub unsafe fn flush(addr: *const u8, len: usize) {
    if len == 0 {
        return;
    }

    let method = get_flush_method();

    // Align start address down to 64-byte boundary
    let start = (addr as usize) & !63;
    let end = (addr as usize) + len;

    // Iterate over cache lines
    let mut current = start;
    while current < end {
        let line_addr = current as *const u8;

        match method {
            FlushMethod::Clwb => unsafe {
                std::arch::asm!(
                    "clwb [{addr}]",
                    addr = in(reg) line_addr,
                    options(nostack)
                );
            },
            FlushMethod::ClflushOpt => unsafe {
                std::arch::asm!(
                    "clflushopt [{addr}]",
                    addr = in(reg) line_addr,
                    options(nostack)
                );
            },
            FlushMethod::Clflush => unsafe {
                std::arch::asm!(
                    "clflush [{addr}]",
                    addr = in(reg) line_addr,
                    options(nostack)
                );
            },
        }

        current += 64;
    }
}

/// Execute a store fence to ensure all previous stores are globally visible
///
/// This should be called after `flush()` to ensure persistence.
#[inline]
pub fn drain() {
    unsafe {
        std::arch::asm!("sfence", options(nostack));
    }
}

/// Flush cache lines and execute a store fence
///
/// This is a convenience function that combines `flush()` and `drain()`.
///
/// # Safety
///
/// The caller must ensure that `addr` points to a valid memory range of at least `len` bytes.
#[inline]
pub unsafe fn persist(addr: *const u8, len: usize) {
    unsafe {
        flush(addr, len);
    }
    drain();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_clwb_on_xeon_gold() {
        // Xeon Gold 6530 should support CLWB
        let method = detect_flush_method();
        assert_eq!(
            method,
            FlushMethod::Clwb,
            "Xeon Gold 6530 should support CLWB"
        );
    }

    #[test]
    fn test_flush_zero_length() {
        // Should not panic
        unsafe {
            flush(std::ptr::null(), 0);
        }
    }

    #[test]
    fn test_persist_basic() {
        // Allocate some test data
        let data = vec![0u8; 256];
        unsafe {
            std::ptr::write_volatile(data.as_ptr() as *mut u8, 42);
        }

        // Should not panic
        unsafe {
            persist(data.as_ptr(), data.len());
        }

        let value = unsafe { std::ptr::read_volatile(data.as_ptr()) };
        assert_eq!(value, 42);
    }

    #[test]
    fn test_flush_unaligned() {
        // Test with unaligned address
        let data = vec![0u8; 256];
        let unaligned_ptr = unsafe { data.as_ptr().add(7) };

        // Should not panic, should handle alignment internally
        unsafe {
            flush(unaligned_ptr, 100);
        }

        drain();
    }

    #[test]
    fn test_cached_method() {
        // Call multiple times, should use cached value
        let method1 = get_flush_method();
        let method2 = get_flush_method();
        assert_eq!(method1, method2);
    }
}
