//! Memory barrier macros for RDMA operations.
//!
//! These macros provide the necessary memory ordering guarantees for
//! interacting with mlx5 hardware via MMIO.

/// Flush Write Combining buffer.
///
/// On x86_64, issues `sfence` instruction to ensure all prior stores
/// are flushed to the memory subsystem.
/// On ARM64, uses `dsb st`.
/// Equivalent to rdma-core's `mmio_flush_writes()`.
macro_rules! mmio_flush_writes {
    () => {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        #[cfg(target_arch = "x86")]
        unsafe {
            std::arch::x86::_mm_sfence();
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("dsb st", options(nostack, preserves_flags));
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "x86", target_arch = "aarch64")))]
        {
            std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
        }
    };
}

/// Load barrier for device reads.
///
/// Used after reading CQE ownership bit to ensure subsequent reads
/// (like the receive buffer) see the data written by the hardware.
/// On x86/x86_64, this is a compiler barrier only - no instruction needed
/// due to TSO (Total Store Order) guaranteeing load-load ordering.
/// On ARM, explicit barrier is required (dmb ld).
/// Equivalent to rdma-core's `udma_from_device_barrier()`.
macro_rules! udma_from_device_barrier {
    () => {
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("dmb ld", options(nostack, preserves_flags));
        }
        // x86/x86_64: compiler barrier only (TSO guarantees load-load ordering)
        // The compiler_fence prevents the compiler from reordering loads across this point
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        {
            std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::Acquire);
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "x86", target_arch = "aarch64")))]
        {
            std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        }
    };
}

/// Store-store ordering barrier for device writes.
///
/// Used between doorbell record write and BlueFlame write.
/// On x86/x86_64, this is a no-op due to TSO (Total Store Order).
/// On ARM, explicit barrier is required.
/// Equivalent to rdma-core's `udma_to_device_barrier()`.
macro_rules! udma_to_device_barrier {
    () => {
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("dmb oshst", options(nostack, preserves_flags));
        }
        // x86/x86_64: no-op (TSO guarantees store-store ordering)
    };
}

/// Copy 64 bytes to BlueFlame register using non-temporal stores.
///
/// Non-temporal stores bypass the cache, which is appropriate for MMIO
/// regions where caching would be wasteful.
/// Equivalent to rdma-core's `mlx5_bf_copy()`.
///
/// # Safety
/// - `dst` must be a 64-byte aligned pointer to MMIO region
/// - `src` must be a pointer to at least 64 bytes of readable memory
#[cfg(target_arch = "x86_64")]
macro_rules! mlx5_bf_copy {
    ($dst:expr, $src:expr) => {
        unsafe {
            use std::arch::x86_64::{__m512i, _mm512_loadu_si512, _mm512_stream_si512};
            let src = $src as *const __m512i;
            let dst = $dst as *mut __m512i;
            let data = _mm512_loadu_si512(src);
            _mm512_stream_si512(dst, data);
        }
    };
}

#[cfg(target_arch = "aarch64")]
macro_rules! mlx5_bf_copy {
    ($dst:expr, $src:expr) => {
        unsafe {
            use std::arch::aarch64::*;
            let src = $src as *const u8;
            let dst = $dst as *mut u8;
            let v0: uint8x16_t = vld1q_u8(src.add(0));
            let v1: uint8x16_t = vld1q_u8(src.add(16));
            let v2: uint8x16_t = vld1q_u8(src.add(32));
            let v3: uint8x16_t = vld1q_u8(src.add(48));
            std::arch::asm!(
                "stnp {v0:q}, {v1:q}, [{dst}]",
                "stnp {v2:q}, {v3:q}, [{dst}, #32]",
                dst = in(reg) dst,
                v0 = in(vreg) v0,
                v1 = in(vreg) v1,
                v2 = in(vreg) v2,
                v3 = in(vreg) v3,
                options(nostack, preserves_flags),
            );
        }
    };
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
macro_rules! mlx5_bf_copy {
    ($dst:expr, $src:expr) => {
        unsafe {
            let src = $src as *const u64;
            let dst = $dst as *mut u64;
            for i in 0..8 {
                std::ptr::write_volatile(dst.add(i), *src.add(i));
            }
        }
    };
}
