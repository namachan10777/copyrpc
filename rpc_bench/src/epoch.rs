use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct EpochData {
    pub index: u32,
    pub completed: u64,
    pub duration_ns: u64,
}

const CHECK_INTERVAL: u64 = 1024;

pub struct EpochCollector {
    interval: Duration,
    epochs: Vec<EpochData>,
    epoch_start: Instant,
    epoch_completed: u64,
    since_last_check: u64,
    next_index: u32,
}

impl EpochCollector {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            epochs: Vec::new(),
            epoch_start: Instant::now(),
            epoch_completed: 0,
            since_last_check: 0,
            next_index: 0,
        }
    }

    /// Add completed operations. Time check runs only every CHECK_INTERVAL calls.
    #[inline]
    pub fn record(&mut self, delta: u64) -> bool {
        self.epoch_completed += delta;
        self.since_last_check += delta;
        if self.since_last_check < CHECK_INTERVAL {
            return false;
        }
        self.since_last_check = 0;
        self.check_epoch()
    }

    #[cold]
    fn check_epoch(&mut self) -> bool {
        let elapsed = self.epoch_start.elapsed();
        if elapsed >= self.interval {
            self.epochs.push(EpochData {
                index: self.next_index,
                completed: self.epoch_completed,
                duration_ns: elapsed.as_nanos() as u64,
            });
            self.next_index += 1;
            self.epoch_completed = 0;
            self.epoch_start = Instant::now();
            true
        } else {
            false
        }
    }

    /// Finalize: record the last partial epoch.
    pub fn finish(&mut self) {
        let elapsed = self.epoch_start.elapsed();
        if self.epoch_completed > 0 {
            self.epochs.push(EpochData {
                index: self.next_index,
                completed: self.epoch_completed,
                duration_ns: elapsed.as_nanos() as u64,
            });
        }
    }

    /// Return steady-state epochs after trimming `trim` epochs from each end.
    pub fn steady_state(&self, trim: usize) -> &[EpochData] {
        let len = self.epochs.len();
        if len <= trim * 2 {
            return &[];
        }
        &self.epochs[trim..len - trim]
    }

    #[allow(dead_code)]
    pub fn all_epochs(&self) -> &[EpochData] {
        &self.epochs
    }
}

/// RPS下位25%のepochを除外して返す。4個未満ならフィルタしない。
pub fn filter_bottom_quartile(epochs: &[EpochData]) -> Vec<EpochData> {
    if epochs.len() < 4 {
        return epochs.to_vec();
    }
    let mut rps_values: Vec<f64> = epochs
        .iter()
        .map(|e| e.completed as f64 / (e.duration_ns as f64 / 1e9))
        .collect();
    rps_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let threshold = rps_values[epochs.len() / 4];
    epochs
        .iter()
        .filter(|e| e.completed as f64 / (e.duration_ns as f64 / 1e9) >= threshold)
        .cloned()
        .collect()
}
