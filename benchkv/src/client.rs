use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::message::Request;
use crate::parquet_out::BatchRecord;
use crate::shm::ShmClient;
use crate::slot;
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

pub fn run_client(
    shm_clients: Vec<ShmClient>,
    num_daemons: usize,
    my_rank: u32,
    pattern: &[AccessEntry],
    stop_flag: &AtomicBool,
    batch_size: u32,
    bench_start: Instant,
) -> Vec<BatchRecord> {
    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;
    let mut seq: u64 = 1;

    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;

    while !stop_flag.load(Ordering::Relaxed) {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;

        // Route to correct daemon
        let daemon = if entry.rank == my_rank {
            (entry.key % num_daemons as u64) as usize
        } else {
            (entry.rank as usize) % num_daemons
        };
        let client = &shm_clients[daemon];

        let req = make_request(entry);

        // Submit request
        unsafe { slot::slot_write(client.req, seq, req.as_bytes()) };

        // Spin wait for response
        loop {
            if unsafe { slot::slot_read_seq(client.resp) } == seq {
                break;
            }
            if stop_flag.load(Ordering::Relaxed) {
                return records;
            }
            std::hint::spin_loop();
        }

        seq += 1;
        completed_in_batch += 1;

        if completed_in_batch >= batch_size {
            completed_in_batch -= batch_size;
            records.push(BatchRecord {
                elapsed_ns: bench_start.elapsed().as_nanos() as u64,
                batch_size,
            });
        }
    }

    records
}
