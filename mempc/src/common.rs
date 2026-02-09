use std::sync::atomic::AtomicBool;

/// Cache-line padded wrapper for avoiding false sharing.
#[repr(C, align(64))]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// Shared state for disconnect detection.
#[repr(C, align(64))]
pub(crate) struct DisconnectState {
    pub tx_alive: AtomicBool,
    pub rx_alive: AtomicBool,
}

/// Demote a cache line from L1/L2 to shared LLC (L3).
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn cldemote(ptr: *const u8) {
    unsafe {
        std::arch::asm!("cldemote [{ptr}]", ptr = in(reg) ptr, options(nostack, preserves_flags));
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub fn cldemote(_ptr: *const u8) {}

/// Response wrapper that carries the token for matching responses to requests.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Response<T: Copy> {
    pub token: u64,
    pub data: T,
}

unsafe impl<T: crate::Serial> crate::Serial for Response<T> {}
