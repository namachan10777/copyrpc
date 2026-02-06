//! Flow control and congestion control for eRPC.
//!
//! Implements credit-based flow control and Timely congestion control algorithm.

use std::cell::Cell;

/// Timely congestion control state.
///
/// Timely is an RTT-based congestion control algorithm that adjusts the
/// sending rate based on observed RTT variations.
///
/// Reference: Timely: RTT-based Congestion Control for the Datacenter (SIGCOMM 2015)
#[derive(Debug, Clone)]
pub struct TimelyState {
    /// Current sending rate in packets per microsecond.
    rate: Cell<f64>,
    /// Previous RTT measurement in microseconds.
    prev_rtt: Cell<u64>,
    /// RTT gradient (rate of RTT change).
    rtt_grad: Cell<f64>,
    /// Minimum RTT observed (baseline).
    min_rtt: Cell<u64>,
    /// High-availability mode flag.
    ha_mode: bool,
    /// Number of packets sent since last rate update.
    pkts_since_update: Cell<u32>,
}

// Timely parameters (from the paper)
const TIMELY_T_LOW: u64 = 30;      // Low threshold in microseconds
const TIMELY_T_HIGH: u64 = 500;    // High threshold in microseconds
const TIMELY_ALPHA: f64 = 0.875;   // EWMA smoothing factor
const TIMELY_BETA: f64 = 0.8;      // Multiplicative decrease factor
const TIMELY_DELTA: f64 = 10.0;    // Additive increase (Mpps)
const TIMELY_MIN_RTT: u64 = 10;    // Minimum RTT floor in microseconds
const TIMELY_UPDATE_INTERVAL: u32 = 16; // Packets between updates

impl TimelyState {
    /// Create a new Timely state with default parameters.
    pub fn new() -> Self {
        Self {
            rate: Cell::new(100.0), // Initial rate: 100 Mpps
            prev_rtt: Cell::new(0),
            rtt_grad: Cell::new(0.0),
            min_rtt: Cell::new(u64::MAX),
            ha_mode: false,
            pkts_since_update: Cell::new(0),
        }
    }

    /// Create a new Timely state with high-availability mode.
    pub fn new_ha() -> Self {
        let mut state = Self::new();
        state.ha_mode = true;
        state
    }

    /// Get the current sending rate.
    #[inline]
    pub fn rate(&self) -> f64 {
        self.rate.get()
    }

    /// Get the minimum RTT observed.
    #[inline]
    pub fn min_rtt(&self) -> u64 {
        self.min_rtt.get()
    }

    /// Update the congestion control state with a new RTT measurement.
    ///
    /// Returns true if the rate was updated.
    pub fn update(&self, rtt: u64) -> bool {
        // Update minimum RTT
        let min_rtt = self.min_rtt.get();
        if rtt < min_rtt {
            self.min_rtt.set(rtt);
        }
        let min_rtt = self.min_rtt.get().max(TIMELY_MIN_RTT);

        // Check if we should update rate
        let pkts = self.pkts_since_update.get() + 1;
        self.pkts_since_update.set(pkts);
        if pkts < TIMELY_UPDATE_INTERVAL {
            return false;
        }
        self.pkts_since_update.set(0);

        let prev_rtt = self.prev_rtt.get();
        self.prev_rtt.set(rtt);

        if prev_rtt == 0 {
            return false;
        }

        let current_rate = self.rate.get();
        let new_rate;

        // Calculate RTT gradient
        let rtt_diff = rtt as f64 - prev_rtt as f64;
        let rtt_grad = TIMELY_ALPHA * self.rtt_grad.get() + (1.0 - TIMELY_ALPHA) * rtt_diff;
        self.rtt_grad.set(rtt_grad);

        // Timely algorithm
        if rtt < TIMELY_T_LOW {
            // Additive increase
            new_rate = current_rate + TIMELY_DELTA;
        } else if rtt > TIMELY_T_HIGH {
            // Multiplicative decrease
            new_rate = current_rate * TIMELY_BETA;
        } else {
            // Gradient-based adjustment
            let normalized_grad = rtt_grad / min_rtt as f64;
            if normalized_grad <= 0.0 {
                // RTT is decreasing, increase rate
                new_rate = current_rate + TIMELY_DELTA;
            } else {
                // RTT is increasing, decrease rate proportionally
                let decrease = current_rate * normalized_grad * TIMELY_BETA;
                new_rate = (current_rate - decrease).max(1.0);
            }
        }

        // Apply rate limits
        let final_rate = new_rate.clamp(1.0, 1000.0); // 1-1000 Mpps
        self.rate.set(final_rate);

        true
    }

    /// Reset the Timely state.
    pub fn reset(&self) {
        self.rate.set(100.0);
        self.prev_rtt.set(0);
        self.rtt_grad.set(0.0);
        self.min_rtt.set(u64::MAX);
        self.pkts_since_update.set(0);
    }
}

impl Default for TimelyState {
    fn default() -> Self {
        Self::new()
    }
}

/// Credit manager for session-level flow control.
pub struct CreditManager {
    /// Maximum credits per session.
    max_credits: usize,
    /// Credits to return before sending explicit credit return.
    credit_return_threshold: usize,
}

impl CreditManager {
    /// Create a new credit manager.
    pub fn new(max_credits: usize) -> Self {
        Self {
            max_credits,
            credit_return_threshold: max_credits / 4,
        }
    }

    /// Get the maximum credits per session.
    #[inline]
    pub fn max_credits(&self) -> usize {
        self.max_credits
    }

    /// Get the credit return threshold.
    #[inline]
    pub fn credit_return_threshold(&self) -> usize {
        self.credit_return_threshold
    }

    /// Check if explicit credit return is needed.
    #[inline]
    pub fn should_return_credits(&self, pending_credits: usize) -> bool {
        pending_credits >= self.credit_return_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timely_additive_increase() {
        let state = TimelyState::new();
        let initial_rate = state.rate();

        // First cycle: sets prev_rtt, doesn't update rate yet
        for _ in 0..TIMELY_UPDATE_INTERVAL {
            state.update(5); // 5us, well below T_LOW
        }
        state.update(5); // This sets prev_rtt but doesn't change rate

        // Second cycle: now prev_rtt is set, rate should increase
        for _ in 0..TIMELY_UPDATE_INTERVAL {
            state.update(5);
        }
        state.update(5);

        // After second cycle, rate should have increased
        assert!(state.rate() > initial_rate);
    }

    #[test]
    fn test_timely_multiplicative_decrease() {
        let state = TimelyState::new();
        let initial_rate = state.rate();

        // First, set prev_rtt
        for _ in 0..TIMELY_UPDATE_INTERVAL {
            state.update(10);
        }
        state.update(10);

        // High RTT should cause multiplicative decrease
        for _ in 0..TIMELY_UPDATE_INTERVAL {
            state.update(600); // Above T_HIGH
        }
        state.update(600);

        assert!(state.rate() < initial_rate);
    }

    #[test]
    fn test_credit_manager() {
        let manager = CreditManager::new(32);
        assert_eq!(manager.max_credits(), 32);
        assert_eq!(manager.credit_return_threshold(), 8);
        assert!(!manager.should_return_credits(5));
        assert!(manager.should_return_credits(10));
    }
}
