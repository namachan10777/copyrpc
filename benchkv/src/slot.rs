//! 64-byte slot read/write primitives for benchkv shared-memory protocol.
//!
//! Layout per slot (64 bytes, cache-line aligned):
//!   [seq: u64 (8B)] [payload: 56B]

use std::sync::atomic::{AtomicU64, Ordering};

pub const SLOT_SIZE: usize = 64;
pub const PAYLOAD_OFFSET: usize = 8;
pub const PAYLOAD_SIZE: usize = SLOT_SIZE - PAYLOAD_OFFSET;

#[inline]
pub unsafe fn slot_write(slot: *mut u8, seq: u64, payload: &[u8]) {
    debug_assert!(payload.len() <= PAYLOAD_SIZE);
    unsafe {
        std::ptr::copy_nonoverlapping(payload.as_ptr(), slot.add(PAYLOAD_OFFSET), payload.len());
        (*slot.cast::<AtomicU64>()).store(seq, Ordering::Release);
    }
}

#[inline]
pub unsafe fn slot_read_seq(slot: *const u8) -> u64 {
    unsafe { (*slot.cast::<AtomicU64>()).load(Ordering::Acquire) }
}

#[inline]
pub unsafe fn slot_read_payload(slot: *const u8) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts(slot.add(PAYLOAD_OFFSET), PAYLOAD_SIZE) }
}
