//! Raw Transport benchmark without Flux overhead.
//!
//! Tests Transport's call/reply pattern directly.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use inproc::Serial;
use inproc::{
    FastForwardTransport, LamportTransport, OnesidedTransport, Transport, TransportEndpoint,
};

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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportType {
    Onesided,
    FastForward,
    Lamport,
}

#[derive(Parser)]
#[command(name = "raw_spsc_bench")]
struct Args {
    #[arg(short, long, value_enum)]
    transport: TransportType,

    #[arg(short, long, default_value = "3")]
    duration: u64,

    /// Starting core ID for thread pinning (threads count down from this core)
    #[arg(long, default_value = "31")]
    start_core: usize,
}

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

fn run_bench<Tr: Transport>(duration_secs: u64, start_core: usize) {
    let capacity = 1024;
    let inflight_max = 256;
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(capacity, inflight_max);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier);

    let payload = Payload::default();

    // Responder thread
    let handle = thread::spawn(move || {
        pin_to_core(start_core - 1);
        barrier2.wait();
        let mut count = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            // sync() makes peer's calls visible, makes our replies visible
            endpoint_b.sync();
            // Receive requests and reply
            while let Some((token, data)) = endpoint_b.recv() {
                // Send response with same token
                loop {
                    if endpoint_b.reply(token, data).is_ok() {
                        count += 1;
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
        count
    });

    // Sender thread (main)
    pin_to_core(start_core);
    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(duration_secs);

    let mut sent = 0u64;
    let mut received = 0u64;

    while start.elapsed() < run_duration {
        // Send requests (Transport limits inflight via InflightExceeded error)
        while sent - received < inflight_max as u64 {
            if endpoint_a.call(payload).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }

        // Receive responses
        endpoint_a.sync();
        while let Some(_) = endpoint_a.try_recv_response() {
            received += 1;
        }
    }

    // Drain remaining
    while received < sent {
        endpoint_a.sync();
        while let Some(_) = endpoint_a.try_recv_response() {
            received += 1;
        }
        std::hint::spin_loop();
    }

    stop.store(true, Ordering::Relaxed);
    let responder_count = handle.join().unwrap();

    let elapsed = start.elapsed();
    let mops = received as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "Sent: {}, Received: {}, Responder: {}, Duration: {:?}, Throughput: {:.2} Mops/s",
        sent, received, responder_count, elapsed, mops
    );
}

fn main() {
    let args = Args::parse();

    println!("Running {:?} transport for {}s", args.transport, args.duration);

    match args.transport {
        TransportType::Onesided => run_bench::<OnesidedTransport>(args.duration, args.start_core),
        TransportType::FastForward => run_bench::<FastForwardTransport>(args.duration, args.start_core),
        TransportType::Lamport => run_bench::<LamportTransport>(args.duration, args.start_core),
    }
}
