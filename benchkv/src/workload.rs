use rand::Rng;
use rand::SeedableRng;
use rand_distr::Distribution;

#[derive(Debug, Clone, Copy)]
pub struct AccessEntry {
    pub rank: u32,
    pub key: u64,
    pub is_read: bool,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum KeyDistribution {
    Uniform,
    Zipfian,
}

pub fn generate_pattern(
    num_ranks: u32,
    key_range: u64,
    read_ratio: f64,
    distribution: KeyDistribution,
    pattern_len: usize,
    seed: u64,
) -> Vec<AccessEntry> {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut pattern = Vec::with_capacity(pattern_len);

    match distribution {
        KeyDistribution::Uniform => {
            for _ in 0..pattern_len {
                let rank = rng.random_range(0..num_ranks);
                let key = rng.random_range(0..key_range);
                let is_read = rng.random::<f64>() < read_ratio;
                pattern.push(AccessEntry { rank, key, is_read });
            }
        }
        KeyDistribution::Zipfian => {
            let total_keys = num_ranks as u64 * key_range;
            let zipf = rand_distr::Zipf::new(total_keys as f64, 1.0).unwrap();
            for _ in 0..pattern_len {
                let sample = zipf.sample(&mut rng) as u64;
                let global_key = sample.min(total_keys - 1);
                let rank = (global_key / key_range) as u32;
                let key = global_key % key_range;
                let is_read = rng.random::<f64>() < read_ratio;
                pattern.push(AccessEntry { rank, key, is_read });
            }
        }
    }

    pattern
}
