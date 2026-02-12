/// Adaptive poll budget controller for the delegation daemon.
///
/// Controls how many extra recv-CQ-only polls to perform at the top of
/// the daemon loop. When CQE batching collapses (daemon loop faster than
/// RDMA RTT), extra polls accumulate CQEs before proceeding, breaking the
/// positive feedback loop that causes bistability.

/// Configuration for the adaptive budget controller.
#[derive(Debug, Clone)]
pub struct AdaptiveBudgetConfig {
    /// Maximum extra polls (0 = disabled)
    pub u_max: u32,
    /// Step size for increasing u
    pub step_up: u32,
    /// Step size for decreasing u
    pub step_down: u32,
    /// rho = loop_ema / rtt_ema threshold (below → increase u)
    pub rho_low: f64,
    /// rho threshold (above → decrease u)
    pub rho_high: f64,
    /// Empty-loop ratio threshold (above → increase u)
    pub e_high: f64,
    /// Empty-loop ratio threshold (below → decrease u)
    pub e_low: f64,
    /// Consecutive loops needed before increasing u
    pub low_need: u32,
    /// Consecutive loops needed before decreasing u
    pub high_need: u32,
    /// Hold period after each adjustment (loops to skip)
    pub hold_min: u32,
    /// Fixed RTT estimate in microseconds
    pub rtt_estimate_us: f64,
    /// EWMA alpha for batch size
    pub alpha_batch: f64,
    /// EWMA alpha for empty ratio
    pub alpha_empty: f64,
    /// EWMA alpha for loop time
    pub alpha_loop: f64,
    /// Warmup loops per phase
    pub warmup_loops: u64,
    /// Probe u value during warmup phase 2
    pub warmup_probe_u: u32,
}

impl Default for AdaptiveBudgetConfig {
    fn default() -> Self {
        Self {
            u_max: 32,
            step_up: 2,
            step_down: 1,
            rho_low: 0.8,
            rho_high: 1.2,
            e_high: 0.75,
            e_low: 0.40,
            low_need: 64,
            high_need: 128,
            hold_min: 32,
            rtt_estimate_us: 6.0,
            alpha_batch: 0.05,
            alpha_empty: 0.05,
            alpha_loop: 0.05,
            warmup_loops: 5000,
            warmup_probe_u: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Warmup phase 1: u=0, measure baseline
    Warmup1,
    /// Warmup phase 2: u=probe, measure probed baseline
    Warmup2,
    /// Adaptive control active
    Active,
}

pub struct AdaptiveBudgetCtl {
    config: AdaptiveBudgetConfig,
    phase: Phase,

    // EWMA state
    b_ema: f64,
    e_ema: f64,
    loop_ema_us: f64,

    // Warmup accumulators
    warmup_total_batch: f64,
    warmup_total_empty: f64,
    warmup_count: u64,

    // Control state
    u: u32,
    low_cnt: u32,
    high_cnt: u32,
    hold_cnt: u32,
}

impl AdaptiveBudgetCtl {
    pub fn new(config: AdaptiveBudgetConfig) -> Self {
        Self {
            phase: Phase::Warmup1,
            b_ema: 0.0,
            e_ema: 0.5,
            loop_ema_us: 1.0,
            warmup_total_batch: 0.0,
            warmup_total_empty: 0.0,
            warmup_count: 0,
            u: 0,
            low_cnt: 0,
            high_cnt: 0,
            hold_cnt: 0,
            config,
        }
    }

    /// Returns the current poll budget (number of extra recv-only polls).
    #[inline]
    pub fn budget(&self) -> u32 {
        self.u
    }

    /// Feed metrics from the current loop iteration.
    ///
    /// `batch_size`: total CQEs returned this loop (extra polls + regular poll)
    /// `loop_period_ns`: elapsed time of this loop iteration in nanoseconds
    pub fn update(&mut self, batch_size: u32, loop_period_ns: u64) {
        match self.phase {
            Phase::Warmup1 => self.update_warmup1(batch_size),
            Phase::Warmup2 => self.update_warmup2(batch_size),
            Phase::Active => self.update_active(batch_size, loop_period_ns),
        }
    }

    fn update_warmup1(&mut self, batch_size: u32) {
        self.warmup_total_batch += batch_size as f64;
        self.warmup_total_empty += if batch_size == 0 { 1.0 } else { 0.0 };
        self.warmup_count += 1;
        if self.warmup_count >= self.config.warmup_loops {
            // Transition to phase 2
            self.warmup_total_batch = 0.0;
            self.warmup_total_empty = 0.0;
            self.warmup_count = 0;
            self.u = self.config.warmup_probe_u;
            self.phase = Phase::Warmup2;
        }
    }

    fn update_warmup2(&mut self, batch_size: u32) {
        self.warmup_total_batch += batch_size as f64;
        self.warmup_total_empty += if batch_size == 0 { 1.0 } else { 0.0 };
        self.warmup_count += 1;
        if self.warmup_count >= self.config.warmup_loops {
            // Initialize EWMAs from probed baseline
            self.b_ema = self.warmup_total_batch / self.warmup_count as f64;
            self.e_ema = self.warmup_total_empty / self.warmup_count as f64;
            self.loop_ema_us = 1.0;
            self.phase = Phase::Active;
        }
    }

    fn update_active(&mut self, batch_size: u32, loop_period_ns: u64) {
        let alpha_b = self.config.alpha_batch;
        let alpha_e = self.config.alpha_empty;
        let alpha_l = self.config.alpha_loop;

        // Update EWMAs
        self.b_ema = self.b_ema * (1.0 - alpha_b) + batch_size as f64 * alpha_b;
        let empty_val = if batch_size == 0 { 1.0 } else { 0.0 };
        self.e_ema = self.e_ema * (1.0 - alpha_e) + empty_val * alpha_e;
        let loop_us = loop_period_ns as f64 / 1000.0;
        self.loop_ema_us = self.loop_ema_us * (1.0 - alpha_l) + loop_us * alpha_l;

        // Compute rho = loop_ema / rtt_estimate
        let rho = self.loop_ema_us / self.config.rtt_estimate_us;

        // Hold-off after adjustment
        if self.hold_cnt > 0 {
            self.hold_cnt -= 1;
            return;
        }

        // Collapse recovery: emergency boost
        if self.b_ema < 1.0 && self.e_ema > 0.9 {
            self.u = self.config.u_max;
            self.hold_cnt = self.config.hold_min * 4;
            self.low_cnt = 0;
            self.high_cnt = 0;
            return;
        }

        // Hysteresis-based adjustment
        let should_increase = rho < self.config.rho_low || self.e_ema > self.config.e_high;
        let should_decrease = rho > self.config.rho_high && self.e_ema < self.config.e_low;

        if should_increase {
            self.high_cnt = 0;
            self.low_cnt += 1;
            if self.low_cnt >= self.config.low_need {
                self.u = (self.u + self.config.step_up).min(self.config.u_max);
                self.hold_cnt = self.config.hold_min;
                self.low_cnt = 0;
            }
        } else if should_decrease {
            self.low_cnt = 0;
            self.high_cnt += 1;
            if self.high_cnt >= self.config.high_need {
                self.u = self.u.saturating_sub(self.config.step_down);
                self.hold_cnt = self.config.hold_min;
                self.high_cnt = 0;
            }
        } else {
            self.low_cnt = 0;
            self.high_cnt = 0;
        }
    }

    /// Get current EWMA values for diagnostics.
    pub fn diagnostics(&self) -> (f64, f64, f64, u32) {
        (self.b_ema, self.e_ema, self.loop_ema_us, self.u)
    }
}
