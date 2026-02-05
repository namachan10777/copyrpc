//! Raw Transport benchmark without Flux overhead.
//!
//! Tests Transport's call/reply pattern directly.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use thread_channel::Serial;
use thread_channel::{
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
}

fn run_bench<Tr: Transport>(duration_secs: u64) {
    let capacity = 1024;
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(capacity);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier);

    let payload = Payload::default();

    // Responder thread
    let handle = thread::spawn(move || {
        barrier2.wait();
        let mut count = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            // Receive request
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
    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(duration_secs);

    let mut sent = 0u64;
    let mut received = 0u64;
    let inflight_max = 256usize;

    while start.elapsed() < run_duration {
        // Send requests
        while sent - received < inflight_max as u64 {
            if endpoint_a.call(payload).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }

        // Receive responses
        while let Some(_) = endpoint_a.poll() {
            received += 1;
        }
    }

    // Drain remaining
    while received < sent {
        while let Some(_) = endpoint_a.poll() {
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
        TransportType::Onesided => run_bench::<OnesidedTransport>(args.duration),
        TransportType::FastForward => run_bench::<FastForwardTransport>(args.duration),
        TransportType::Lamport => run_bench::<LamportTransport>(args.duration),
    }
}
