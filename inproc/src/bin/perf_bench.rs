//! Simple benchmark for perf profiling.
//!
//! Run with: cargo build --release --bin perf_bench --features bench-bin
//! Then: perf record -g ./target/release/perf_bench
//!       perf report

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use inproc::Serial;
use inproc::{Flux, create_flux};

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct Payload {
    data: [u64; 4],
}

unsafe impl Serial for Payload {}

impl Default for Payload {
    fn default() -> Self {
        Self { data: [0u64; 4] }
    }
}

#[derive(Parser)]
#[command(name = "perf_bench")]
struct Args {
    #[arg(short, long, default_value = "5")]
    duration: u64,

    #[arg(short, long, default_value = "32")]
    inflight: usize,

    /// Starting core ID for thread pinning (threads count down from this core)
    #[arg(long, default_value = "31")]
    start_core: usize,
}

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

fn main() {
    let args = Args::parse();

    println!(
        "Running onesided transport for {}s with inflight={}",
        args.duration, args.inflight
    );

    let capacity = 1024;
    let completed = Arc::new(AtomicU64::new(0));

    let nodes: Vec<Flux<Payload, ()>> = create_flux(2, capacity, args.inflight);

    let mut nodes: Vec<_> = nodes.into_iter().collect();
    let mut node1 = nodes.pop().unwrap();
    let mut node0 = nodes.pop().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier);

    let payload = Payload::default();
    let start_core = args.start_core;

    // Responder thread
    let handle = thread::spawn(move || {
        pin_to_core(start_core - 1);
        barrier2.wait();
        while !stop2.load(Ordering::Relaxed) {
            node1.poll(|_, _| {});
            while let Some(h) = node1.try_recv() {
                let data = h.data();
                h.reply(data);
            }
        }
    });

    // Sender thread (main)
    pin_to_core(args.start_core);
    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    let inflight_max = args.inflight;

    let mut sent = 0u64;
    while start.elapsed() < run_duration {
        // Send up to inflight_max
        for _ in 0..inflight_max {
            if node0.call(1, payload, ()).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }
        node0.poll(|_: (), _: Payload| {
            completed.fetch_add(1, Ordering::Relaxed);
        });
    }

    // Wait for remaining responses
    while completed.load(Ordering::Relaxed) < sent {
        node0.poll(|_: (), _: Payload| {
            completed.fetch_add(1, Ordering::Relaxed);
        });
        std::hint::spin_loop();
    }

    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();

    let elapsed = start.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let mops = total as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "Completed: {}, Duration: {:?}, Throughput: {:.2} Mops/s",
        total, elapsed, mops
    );
}
