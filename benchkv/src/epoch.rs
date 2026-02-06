use std::time::{Duration, Instant};

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

    pub fn steady_state(&self, trim: usize) -> &[EpochData] {
        let len = self.epochs.len();
        if len <= trim * 2 {
            return &[];
        }
        &self.epochs[trim..len - trim]
    }
}
