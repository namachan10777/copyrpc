use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use ipc::Client;

use crate::message::{Request, Response};
use crate::parquet_out::BatchRecord;
use crate::workload::AccessEntry;

fn make_request(entry: &AccessEntry) -> Request {
    if entry.is_read {
        Request::MetaGet {
            rank: entry.rank,
            key: entry.key,
        }
    } else {
        Request::MetaPut {
            rank: entry.rank,
            key: entry.key,
            value: entry.key,
        }
    }
}

/// Determine which local daemon should handle this request.
/// - LOCAL (target_rank == my_rank): route to key-owning daemon (key % num_daemons)
/// - REMOTE (target_rank != my_rank): route to daemon handling copyrpc for that rank
///   (target_rank % num_daemons)
#[inline]
fn target_daemon(entry: &AccessEntry, num_daemons: usize, my_rank: u32) -> usize {
    if entry.rank == my_rank {
        (entry.key % num_daemons as u64) as usize
    } else {
        (entry.rank as usize) % num_daemons
    }
}

pub fn run_client(
    shm_paths: &[String],
    num_daemons: usize,
    my_rank: u32,
    pattern: &[AccessEntry],
    queue_depth: u32,
    stop_flag: &AtomicBool,
    batch_size: u32,
    bench_start: Instant,
) -> Vec<BatchRecord> {
    // Connect to all daemons
    let mut clients: Vec<Client<Request, Response, (), _>> = shm_paths
        .iter()
        .map(|path| {
            unsafe { Client::<Request, Response, (), _>::connect(path, |(), _resp| {}) }
                .expect("Client failed to connect")
        })
        .collect();

    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;

    // Initial fill: pipeline queue_depth requests across daemons
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;
        let daemon = target_daemon(entry, num_daemons, my_rank);
        if clients[daemon].call(make_request(entry), ()).is_err() {
            return Vec::new();
        }
    }

    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;

    // Steady state: poll all daemons for completions, send replacements
    while !stop_flag.load(Ordering::Relaxed) {
        let mut total_n = 0u32;
        for client in &mut clients {
            match client.poll() {
                Ok(n) => total_n += n,
                Err(_) => return records,
            }
        }
        for _ in 0..total_n {
            let entry = &pattern[pattern_idx % pattern_len];
            pattern_idx += 1;
            let daemon = target_daemon(entry, num_daemons, my_rank);
            if clients[daemon].call(make_request(entry), ()).is_err() {
                return records;
            }
        }

        completed_in_batch += total_n;
        while completed_in_batch >= batch_size {
            completed_in_batch -= batch_size;
            records.push(BatchRecord {
                elapsed_ns: bench_start.elapsed().as_nanos() as u64,
                batch_size,
            });
        }

        if total_n == 0 {
            std::hint::spin_loop();
        }
    }
    records
}
