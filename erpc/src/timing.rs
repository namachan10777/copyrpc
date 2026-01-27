//! Timing wheel for efficient timeout management.
//!
//! The timing wheel provides O(1) insertion and expiration checking for
//! retransmission timeouts.

use std::collections::VecDeque;

/// Entry in the timing wheel.
#[derive(Debug, Clone, Copy)]
pub struct TimerEntry {
    /// Session number.
    pub session_num: u16,
    /// SSlot index.
    pub sslot_idx: usize,
    /// Request number.
    pub req_num: u64,
    /// Expiration timestamp (microseconds).
    pub expires_at: u64,
}

/// Timing wheel for managing retransmission timeouts.
///
/// The timing wheel divides time into slots, where each slot covers
/// a fixed duration. Timers are inserted into the appropriate slot
/// based on their expiration time.
pub struct TimingWheel {
    /// Wheel slots, each containing timers expiring in that slot.
    slots: Vec<VecDeque<TimerEntry>>,
    /// Number of slots in the wheel.
    num_slots: usize,
    /// Duration of each slot in microseconds.
    slot_duration_us: u64,
    /// Current slot index.
    current_slot: usize,
    /// Timestamp of the current slot.
    current_ts: u64,
    /// Total duration covered by the wheel.
    wheel_duration_us: u64,
}

impl TimingWheel {
    /// Create a new timing wheel.
    ///
    /// # Arguments
    /// * `num_slots` - Number of slots in the wheel (should be power of 2)
    /// * `slot_duration_us` - Duration of each slot in microseconds
    pub fn new(num_slots: usize, slot_duration_us: u64) -> Self {
        let slots = (0..num_slots).map(|_| VecDeque::new()).collect();
        Self {
            slots,
            num_slots,
            slot_duration_us,
            current_slot: 0,
            current_ts: 0,
            wheel_duration_us: num_slots as u64 * slot_duration_us,
        }
    }

    /// Create a timing wheel with default parameters suitable for RTO tracking.
    ///
    /// Default: 256 slots, 100us per slot = 25.6ms total coverage
    pub fn default_for_rpc() -> Self {
        Self::new(256, 100)
    }

    /// Initialize the wheel with a starting timestamp.
    pub fn init(&mut self, ts: u64) {
        self.current_ts = ts;
        self.current_slot = 0;
    }

    /// Insert a timer entry.
    ///
    /// Returns the slot index where the timer was inserted.
    pub fn insert(&mut self, entry: TimerEntry) -> Option<usize> {
        if entry.expires_at <= self.current_ts {
            // Already expired
            return None;
        }

        let delta = entry.expires_at - self.current_ts;
        if delta > self.wheel_duration_us {
            // Too far in the future; insert into the last slot
            let slot = (self.current_slot + self.num_slots - 1) % self.num_slots;
            self.slots[slot].push_back(entry);
            return Some(slot);
        }

        let slots_ahead = (delta / self.slot_duration_us) as usize;
        let slot = (self.current_slot + slots_ahead) % self.num_slots;
        self.slots[slot].push_back(entry);
        Some(slot)
    }

    /// Advance the wheel to the given timestamp.
    ///
    /// Returns a Vec of expired entries (allocates on each call).
    /// For hot paths, prefer `advance_into` which reuses a pre-allocated buffer.
    pub fn advance(&mut self, ts: u64) -> Vec<TimerEntry> {
        let mut expired = Vec::new();
        self.advance_into(ts, &mut expired);
        expired
    }

    /// Advance the wheel to the given timestamp, pushing expired entries into `out`.
    ///
    /// This avoids allocation by reusing the caller's buffer.
    /// The buffer is NOT cleared; caller should clear it before calling if needed.
    #[inline]
    pub fn advance_into(&mut self, ts: u64, out: &mut Vec<TimerEntry>) {
        if ts <= self.current_ts {
            return;
        }

        // Calculate how many slots to advance
        let elapsed = ts - self.current_ts;
        let slots_to_advance = ((elapsed / self.slot_duration_us) as usize).min(self.num_slots);

        for _ in 0..slots_to_advance {
            // Drain current slot
            while let Some(entry) = self.slots[self.current_slot].pop_front() {
                if entry.expires_at <= ts {
                    out.push(entry);
                } else {
                    // Re-insert entries that haven't expired yet
                    // (can happen if slot_duration is large)
                    let delta = entry.expires_at - ts;
                    let slots_ahead =
                        (delta / self.slot_duration_us).min(self.num_slots as u64 - 1) as usize;
                    let new_slot = (self.current_slot + slots_ahead) % self.num_slots;
                    self.slots[new_slot].push_back(entry);
                }
            }

            self.current_slot = (self.current_slot + 1) % self.num_slots;
        }

        self.current_ts = ts;
    }

    /// Cancel a timer entry (slow path, O(n) - searches all slots).
    ///
    /// Returns true if the entry was found and removed.
    /// Prefer `cancel_fast()` when the wheel_slot is known.
    pub fn cancel(&mut self, session_num: u16, sslot_idx: usize, req_num: u64) -> bool {
        for slot in &mut self.slots {
            if let Some(pos) = slot.iter().position(|e| {
                e.session_num == session_num && e.sslot_idx == sslot_idx && e.req_num == req_num
            }) {
                slot.remove(pos);
                return true;
            }
        }
        false
    }

    /// Cancel a timer entry with known wheel slot (fast path, O(k) where k = entries in slot).
    ///
    /// Returns true if the entry was found and removed.
    #[inline]
    pub fn cancel_fast(&mut self, wheel_slot: usize, req_num: u64) -> bool {
        if wheel_slot >= self.num_slots {
            return false;
        }
        if let Some(pos) = self.slots[wheel_slot].iter().position(|e| e.req_num == req_num) {
            self.slots[wheel_slot].remove(pos);
            return true;
        }
        false
    }

    /// Get the number of active timers.
    pub fn active_count(&self) -> usize {
        self.slots.iter().map(|s| s.len()).sum()
    }

    /// Check if the wheel is empty.
    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(|s| s.is_empty())
    }

    /// Clear all timers.
    pub fn clear(&mut self) {
        for slot in &mut self.slots {
            slot.clear();
        }
    }

    /// Get the current timestamp.
    #[inline]
    pub fn current_ts(&self) -> u64 {
        self.current_ts
    }

    /// Get the wheel duration in microseconds.
    #[inline]
    pub fn wheel_duration(&self) -> u64 {
        self.wheel_duration_us
    }
}

/// Get current timestamp in microseconds using RDTSC.
///
/// Note: This assumes a roughly constant CPU frequency. For more accurate
/// timing, use clock_gettime(CLOCK_MONOTONIC).
#[inline]
pub fn rdtsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe { std::arch::x86_64::_rdtsc() }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        use std::time::Instant;
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(Instant::now);
        start.elapsed().as_nanos() as u64
    }
}

/// Get current timestamp in microseconds.
#[inline]
pub fn current_time_us() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timing_wheel_basic() {
        let mut wheel = TimingWheel::new(8, 100);
        wheel.init(1000);

        // Insert a timer expiring at 1500
        let entry = TimerEntry {
            session_num: 1,
            sslot_idx: 0,
            req_num: 42,
            expires_at: 1500,
        };
        wheel.insert(entry);

        assert_eq!(wheel.active_count(), 1);

        // Advance to 1200 - not yet expired
        let expired = wheel.advance(1200);
        assert!(expired.is_empty());

        // Advance to 1600 - should expire
        let expired = wheel.advance(1600);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].req_num, 42);
    }

    #[test]
    fn test_timing_wheel_cancel() {
        let mut wheel = TimingWheel::new(8, 100);
        wheel.init(1000);

        let entry = TimerEntry {
            session_num: 1,
            sslot_idx: 0,
            req_num: 42,
            expires_at: 1500,
        };
        wheel.insert(entry);

        assert!(wheel.cancel(1, 0, 42));
        assert!(!wheel.cancel(1, 0, 42)); // Already cancelled
        assert!(wheel.is_empty());
    }

    #[test]
    fn test_timing_wheel_multiple() {
        let mut wheel = TimingWheel::new(8, 100);
        wheel.init(1000);

        for i in 0..5 {
            let entry = TimerEntry {
                session_num: 1,
                sslot_idx: i,
                req_num: i as u64,
                expires_at: 1100 + i as u64 * 200,
            };
            wheel.insert(entry);
        }

        assert_eq!(wheel.active_count(), 5);

        // Advance to 1400 to include both 1100 and 1300
        // (entries at 1500, 1700, 1900 remain)
        let expired = wheel.advance(1400);
        assert_eq!(expired.len(), 2); // 1100 and 1300
    }
}
