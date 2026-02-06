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
// Disconnect detection state
// ============================================================================

/// Shared state for disconnect detection.
#[repr(C, align(64))]
pub(crate) struct DisconnectState {
    pub tx_alive: AtomicBool,
    pub rx_alive: AtomicBool,
}

