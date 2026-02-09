use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

// ============================================================================
// Response Ring (SPSC, per-caller)
// ============================================================================

#[repr(C, align(64))]
pub(crate) struct RespSlot<T> {
    valid: AtomicBool,
    data: UnsafeCell<MaybeUninit<(u64, T)>>,
}

pub(crate) struct ResponseRing<T> {
    buffer: Box<[RespSlot<T>]>,
    write_pos: CachePadded<AtomicUsize>,
    read_pos: CachePadded<AtomicUsize>,
    mask: usize,
    tx_alive: AtomicBool,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for ResponseRing<T> {}
unsafe impl<T: Send> Sync for ResponseRing<T> {}

impl<T> ResponseRing<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let buffer = (0..capacity)
            .map(|_| RespSlot {
                valid: AtomicBool::new(false),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            buffer,
            write_pos: CachePadded::new(AtomicUsize::new(0)),
            read_pos: CachePadded::new(AtomicUsize::new(0)),
            mask: capacity - 1,
            tx_alive: AtomicBool::new(true),
            rx_alive: AtomicBool::new(true),
        }
    }

    /// Try to push a response (server side).
    pub(crate) fn try_push(&self, token: u64, data: T) -> bool {
        if !self.rx_alive.load(Ordering::Acquire) {
            return false;
        }

        let pos = self.write_pos.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];

        if slot.valid.load(Ordering::Acquire) {
            // Slot still occupied
            return false;
        }

        // Write data
        unsafe {
            (*slot.data.get()).write((token, data));
        }

        // Mark as valid
        slot.valid.store(true, Ordering::Release);
        self.write_pos.store(pos + 1, Ordering::Release);
        true
    }

    /// Try to pop a response (caller side).
    pub(crate) fn try_pop(&self) -> Option<(u64, T)> {
        let pos = self.read_pos.load(Ordering::Relaxed);
        let slot = &self.buffer[pos & self.mask];

        if !slot.valid.load(Ordering::Acquire) {
            return None;
        }

        // Read data
        let data = unsafe { (*slot.data.get()).assume_init_read() };

        // Mark as free
        slot.valid.store(false, Ordering::Release);
        self.read_pos.store(pos + 1, Ordering::Release);

        Some(data)
    }

    pub(crate) fn disconnect_tx(&self) {
        self.tx_alive.store(false, Ordering::Release);
    }

    pub(crate) fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }

    pub(crate) fn is_rx_alive(&self) -> bool {
        self.rx_alive.load(Ordering::Relaxed)
    }
}
