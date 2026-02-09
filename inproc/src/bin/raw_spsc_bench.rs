//! Raw SPSC benchmark without Flux overhead.
//!
//! Tests MpscChannel's call/reply pattern directly.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use inproc::Serial;
use mempc::{MpscCaller, MpscChannel, MpscRecvRef, MpscServer};

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

fn run_bench<M: MpscChannel>(duration_secs: u64, start_core: usize) {
    let capacity = 1024;
    let inflight_max = 256;
    let (mut callers, mut server) = M::create::<Payload, Payload>(1, capacity, inflight_max);
    let mut caller = callers.pop().unwrap();

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
            server.poll();
            loop {
                let (data, token) = match server.try_recv() {
                    Some(r) => (r.data(), r.into_token()),
                    None => break,
                };
                server.reply(token, data);
                count += 1;
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
        // Send requests
        while sent - received < inflight_max as u64 {
            if caller.call(payload).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }

        // Receive responses
        caller.sync();
        while caller.try_recv_response().is_some() {
            received += 1;
        }
    }

    // Drain remaining
    while received < sent {
        caller.sync();
        while caller.try_recv_response().is_some() {
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

    println!(
        "Running {:?} transport for {}s",
        args.transport, args.duration
    );

    match args.transport {
        TransportType::Onesided => {
            run_bench::<mempc::OnesidedMpsc>(args.duration, args.start_core)
        }
        TransportType::FastForward => {
            run_bench::<mempc::FastForwardMpsc>(args.duration, args.start_core)
        }
        TransportType::Lamport => {
            run_bench::<mempc::LamportMpsc>(args.duration, args.start_core)
        }
    }
}
