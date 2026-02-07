use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::message::{Request, Response};
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
    shm_path: &str,
    pattern: &[AccessEntry],
    queue_depth: u32,
    stop_flag: &AtomicBool,
    completions: &AtomicU64,
) {
    let mut client =
        unsafe { shm_spsc::Client::<Request, Response>::connect(shm_path) }
            .expect("Client failed to connect");

    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;

    // Initial fill: pipeline queue_depth requests
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;
        if client.send(make_request(entry)).is_err() {
            return;
        }
    }

    // Steady state: recv any completed → count completion → send next
    while !stop_flag.load(Ordering::Relaxed) {
        match client.recv_any() {
            Ok(_resp) => {
                completions.fetch_add(1, Ordering::Relaxed);
                let entry = &pattern[pattern_idx % pattern_len];
                pattern_idx += 1;
                if client.send(make_request(entry)).is_err() {
                    break;
                }
            }
            Err(shm_spsc::CallError::ServerDisconnected) => break,
        }
    }
}
