//! Minimal hazard pointer implementation for CRQ node reclamation.
//!
//! This module provides a fixed-size slot-based hazard pointer domain
//! suitable for protecting pointers during concurrent queue operations.

use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Per-thread hazard pointer slot.
///
/// Cache-line padded to avoid false sharing between threads.
#[repr(C, align(64))]
pub(crate) struct HazardSlot {
    protected: AtomicPtr<u8>,
}

impl HazardSlot {
    /// Creates a new hazard slot initialized to null.
    fn new() -> Self {
        Self {
            protected: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Stores a pointer to this slot with Release ordering.
    fn store(&self, ptr: *mut u8) {
        self.protected.store(ptr, Ordering::Release);
    }

    /// Loads the protected pointer with Acquire ordering.
    fn load(&self) -> *mut u8 {
        self.protected.load(Ordering::Acquire)
    }

    /// Clears this slot (stores null) with Release ordering.
    fn clear(&self) {
        self.protected.store(ptr::null_mut(), Ordering::Release);
    }
}

/// Hazard pointer domain for a single LCRQ/LPRQ instance.
///
/// Provides memory reclamation through hazard pointers. Each slot
/// corresponds to one thread accessing the queue.
pub(crate) struct HazardDomain {
    slots: Box<[HazardSlot]>,
}

impl HazardDomain {
    /// Creates a new hazard pointer domain with the specified number of slots.
    ///
    /// # Arguments
    ///
    /// * `num_slots` - Number of hazard pointer slots (typically one per thread)
    pub(crate) fn new(num_slots: usize) -> Self {
        let slots = (0..num_slots)
            .map(|_| HazardSlot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    /// Protects a pointer by storing it in the specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot_id` - Slot index (must be < num_slots)
    /// * `ptr` - Pointer to protect
    ///
    /// # Panics
    ///
    /// Panics if `slot_id` is out of bounds.
    pub(crate) fn protect(&self, slot_id: usize, ptr: *mut u8) {
        self.slots[slot_id].store(ptr);
    }

    /// Clears the protection for the specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot_id` - Slot index (must be < num_slots)
    ///
    /// # Panics
    ///
    /// Panics if `slot_id` is out of bounds.
    pub(crate) fn clear(&self, slot_id: usize) {
        self.slots[slot_id].clear();
    }

    /// Checks if a pointer is currently protected by any slot.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to check
    ///
    /// # Returns
    ///
    /// `true` if the pointer is protected by at least one slot, `false` otherwise.
    pub(crate) fn is_protected(&self, ptr: *const u8) -> bool {
        self.slots.iter().any(|slot| std::ptr::eq(slot.load(), ptr))
    }

    /// Drains unprotected pointers from the retire list.
    ///
    /// Retains only protected pointers in `retire_list` and returns
    /// the unprotected ones that can be safely deallocated.
    ///
    /// # Arguments
    ///
    /// * `retire_list` - Mutable list of retired pointers
    ///
    /// # Returns
    ///
    /// A vector of unprotected pointers that can be safely deallocated.
    pub(crate) fn drain_unprotected(&self, retire_list: &mut Vec<*mut u8>) -> Vec<*mut u8> {
        let mut unprotected = Vec::new();
        retire_list.retain(|&ptr| {
            if self.is_protected(ptr) {
                true // Keep in retire list
            } else {
                unprotected.push(ptr);
                false // Remove from retire list
            }
        });
        unprotected
    }
}

// Safety: HazardDomain uses atomic operations for all shared state
unsafe impl Send for HazardDomain {}
unsafe impl Sync for HazardDomain {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protect_and_check() {
        let domain = HazardDomain::new(4);
        let ptr = Box::into_raw(Box::new(42u64)) as *mut u8;

        // Initially not protected
        assert!(!domain.is_protected(ptr));

        // Protect the pointer
        domain.protect(0, ptr);
        assert!(domain.is_protected(ptr));

        // Clear protection
        domain.clear(0);
        assert!(!domain.is_protected(ptr));

        // Cleanup
        unsafe {
            let _ = Box::from_raw(ptr as *mut u64);
        }
    }

    #[test]
    fn test_drain_unprotected() {
        let domain = HazardDomain::new(4);

        let ptr1 = Box::into_raw(Box::new(1u64)) as *mut u8;
        let ptr2 = Box::into_raw(Box::new(2u64)) as *mut u8;
        let ptr3 = Box::into_raw(Box::new(3u64)) as *mut u8;

        let mut retire_list = vec![ptr1, ptr2, ptr3];

        // Protect ptr2
        domain.protect(0, ptr2);

        // Drain unprotected
        let unprotected = domain.drain_unprotected(&mut retire_list);

        // Should return ptr1 and ptr3
        assert_eq!(unprotected.len(), 2);
        assert!(unprotected.contains(&ptr1));
        assert!(unprotected.contains(&ptr3));

        // Retire list should only contain ptr2
        assert_eq!(retire_list.len(), 1);
        assert_eq!(retire_list[0], ptr2);

        // Cleanup
        domain.clear(0);
        unsafe {
            let _ = Box::from_raw(ptr1 as *mut u64);
            let _ = Box::from_raw(ptr2 as *mut u64);
            let _ = Box::from_raw(ptr3 as *mut u64);
        }
    }

    #[test]
    fn test_multiple_slots() {
        let domain = HazardDomain::new(4);

        let ptr1 = Box::into_raw(Box::new(1u64)) as *mut u8;
        let ptr2 = Box::into_raw(Box::new(2u64)) as *mut u8;
        let ptr3 = Box::into_raw(Box::new(3u64)) as *mut u8;

        // Protect different pointers in different slots
        domain.protect(0, ptr1);
        domain.protect(1, ptr2);
        domain.protect(2, ptr3);

        // All should be protected
        assert!(domain.is_protected(ptr1));
        assert!(domain.is_protected(ptr2));
        assert!(domain.is_protected(ptr3));

        // Clear one slot
        domain.clear(1);
        assert!(domain.is_protected(ptr1));
        assert!(!domain.is_protected(ptr2));
        assert!(domain.is_protected(ptr3));

        // Protect same pointer in multiple slots
        domain.protect(1, ptr1);
        domain.protect(3, ptr1);
        assert!(domain.is_protected(ptr1));

        // Clear one slot, should still be protected
        domain.clear(0);
        assert!(domain.is_protected(ptr1)); // Still protected by slots 1 and 3

        // Clear all slots protecting ptr1
        domain.clear(1);
        domain.clear(3);
        assert!(!domain.is_protected(ptr1));

        // Cleanup
        domain.clear(2);
        unsafe {
            let _ = Box::from_raw(ptr1 as *mut u64);
            let _ = Box::from_raw(ptr2 as *mut u64);
            let _ = Box::from_raw(ptr3 as *mut u64);
        }
    }

    #[test]
    #[should_panic]
    fn test_protect_out_of_bounds() {
        let domain = HazardDomain::new(4);
        let ptr = Box::into_raw(Box::new(42u64)) as *mut u8;
        domain.protect(4, ptr); // Should panic
    }

    #[test]
    #[should_panic]
    fn test_clear_out_of_bounds() {
        let domain = HazardDomain::new(4);
        domain.clear(4); // Should panic
    }
}
