//! Common types and utilities shared across transport implementations.

use std::sync::atomic::AtomicBool;

// ============================================================================
// Cache-padded wrapper
// ============================================================================

/// Cache-line padded wrapper for avoiding false sharing.
#[repr(C, align(64))]
pub(crate) struct CachePadded<T> {
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

// ============================================================================
// Slip maintenance constants (FastForward PPoPP 2008 Section 3.4.1)
// ============================================================================

pub(crate) const CACHE_LINE_SIZE: usize = 64;
pub(crate) const ENTRY_SIZE: usize = 8; // pointer size
pub(crate) const ENTRIES_PER_LINE: usize = CACHE_LINE_SIZE / ENTRY_SIZE; // = 8

/// DANGER: slip lost threshold (2 cache lines)
pub(crate) const SLIP_DANGER: usize = ENTRIES_PER_LINE * 2; // = 16

/// GOOD: target slip (6 cache lines)
pub(crate) const SLIP_GOOD: usize = ENTRIES_PER_LINE * 6; // = 48

/// Slip adjustment frequency
pub(crate) const SLIP_CHECK_INTERVAL: usize = 64;

// ============================================================================
// Disconnect detection state
// ============================================================================

/// Shared state for disconnect detection.
#[repr(C, align(64))]
pub(crate) struct DisconnectState {
    pub tx_alive: AtomicBool,
    pub rx_alive: AtomicBool,
}

// ============================================================================
// Slip maintenance logic
// ============================================================================

/// Establishes and maintains temporal slip.
///
/// Temporal slipping (FastForward PPoPP 2008 Section 3.4.1) delays the
/// consumer to ensure producer and consumer operate on different cache lines,
/// reducing cache line bouncing.
///
/// On first call: waits until producer is SLIP_GOOD entries ahead.
/// On subsequent calls (every SLIP_CHECK_INTERVAL): if slip drops below
/// SLIP_DANGER, waits until it recovers to SLIP_GOOD.
#[inline]
pub(crate) fn maintain_slip(
    recv_count: &mut usize,
    slip_initialized: &mut bool,
    distance_fn: impl Fn() -> usize,
) {
    *recv_count = recv_count.wrapping_add(1);

    // Check only every SLIP_CHECK_INTERVAL iterations (after initialization)
    if *slip_initialized && !recv_count.is_multiple_of(SLIP_CHECK_INTERVAL) {
        return;
    }

    let dist = distance_fn();

    if !*slip_initialized {
        // Initial slip establishment: wait until SLIP_GOOD
        while distance_fn() < SLIP_GOOD {
            std::hint::spin_loop();
        }
        *slip_initialized = true;
    } else if dist < SLIP_DANGER {
        // Slip recovery: wait until SLIP_GOOD (with progress check)
        let mut prev_dist = dist;
        while distance_fn() < SLIP_GOOD {
            let curr_dist = distance_fn();
            // Break if producer stopped making progress
            if curr_dist <= prev_dist && curr_dist > 0 {
                break;
            }
            prev_dist = curr_dist;
            std::hint::spin_loop();
        }
    }
}
