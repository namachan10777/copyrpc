use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ipc::Client;

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
    let mut client = unsafe {
        Client::<Request, Response, (), _>::connect(shm_path, |(), _resp| {
            completions.fetch_add(1, Ordering::Relaxed);
        })
    }
    .expect("Client failed to connect");

    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;

    // Initial fill: pipeline queue_depth requests
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;
        if client.call(make_request(entry), ()).is_err() {
            return;
        }
    }

    // Steady state: poll for completions, send replacements
    while !stop_flag.load(Ordering::Relaxed) {
        match client.poll() {
            Ok(n) => {
                for _ in 0..n {
                    let entry = &pattern[pattern_idx % pattern_len];
                    pattern_idx += 1;
                    if client.call(make_request(entry), ()).is_err() {
                        return;
                    }
                }
                if n == 0 {
                    std::hint::spin_loop();
                }
            }
            Err(_) => break,
        }
    }
}
