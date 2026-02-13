use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use ipc::Client;

use crate::message::{
    ClientSlot, Request, Response, slot_from_extra, slot_init, slot_mark_empty, slot_submit,
    slot_try_read_done,
};
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
    assert_eq!(queue_depth, 1, "meta path requires --queue-depth=1");

    // Connect to all daemons.
    let clients: Vec<Client<Request, Response, (), _>> = shm_paths
        .iter()
        .map(|path| {
            unsafe { Client::<Request, Response, (), _>::connect(path, |(), _resp| {}) }
                .expect("Client failed to connect")
        })
        .collect();

    let mut slots: Vec<*mut ClientSlot> = clients
        .iter()
        .map(|c| unsafe { slot_from_extra(c.extra_buffer()) })
        .collect();

    for slot in &mut slots {
        unsafe { slot_init(*slot) };
    }

    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;
    let mut seq = 1u32;

    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;

    while !stop_flag.load(Ordering::Relaxed) {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;
        let daemon = target_daemon(entry, num_daemons, my_rank);
        let slot = slots[daemon];

        let req = make_request(entry);
        while !unsafe { slot_submit(slot, req, seq) } {
            if stop_flag.load(Ordering::Relaxed) {
                return records;
            }
            std::hint::spin_loop();
        }

        loop {
            if stop_flag.load(Ordering::Relaxed) {
                return records;
            }
            if unsafe { slot_try_read_done(slot, seq) }.is_some() {
                unsafe { slot_mark_empty(slot) };
                break;
            }
            std::hint::spin_loop();
        }

        seq = seq.wrapping_add(1);
        completed_in_batch += 1;

        while completed_in_batch >= batch_size {
            completed_in_batch -= batch_size;
            records.push(BatchRecord {
                elapsed_ns: bench_start.elapsed().as_nanos() as u64,
                batch_size,
            });
        }
    }

    records
}
