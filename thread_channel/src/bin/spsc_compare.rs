//! SPSC implementation comparison benchmark.
//!
//! Compares four SPSC implementations:
//! - Producer-Only Write SPSC (spsc_rpc): Consumer read-only, sentinel pattern
//! - FastForward SPSC (spsc): Valid flag based, bidirectional writes
//! - Lamport SPSC (spsc_lamport): Batched index synchronization
//! - std::sync::mpsc: Rust standard library baseline

use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use core_affinity::CoreId;

/// Pin current thread to the specified core
fn pin_to_core(core_id: usize) {
    let core = CoreId { id: core_id };
    core_affinity::set_for_current(core);
}

#[derive(Parser, Debug)]
#[command(name = "spsc_compare")]
#[command(about = "Compare SPSC implementations")]
struct Args {
    /// Number of iterations
    #[arg(short, long, default_value_t = 10_000_000)]
    iterations: u64,

    /// Queue capacity
    #[arg(short, long, default_value_t = 1024)]
    capacity: usize,

    /// Which implementations to benchmark (all, fastforward, lamport, mpsc, producer-only)
    #[arg(short = 't', long, default_value = "all")]
    target: String,

    /// Number of warmup iterations
    #[arg(short, long, default_value_t = 100_000)]
    warmup: u64,

    /// Max in-flight requests (for producer-only)
    #[arg(long, default_value_t = 128)]
    inflight: usize,

    /// Sender CPU core
    #[arg(long, default_value_t = 0)]
    sender_core: usize,

    /// Receiver CPU core
    #[arg(long, default_value_t = 1)]
    receiver_core: usize,
}

/// Throughput benchmark result
struct BenchResult {
    name: &'static str,
    iterations: u64,
    duration: Duration,
}

impl BenchResult {
    fn mops(&self) -> f64 {
        self.iterations as f64 / self.duration.as_secs_f64() / 1_000_000.0
    }
}

/// Benchmark FastForward SPSC (bidirectional valid flag writes)
fn bench_fastforward(iterations: u64, capacity: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx, mut rx) = thread_channel::spsc::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Sender: sends as fast as possible, spin-waits only when full
    let sender = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut i = 0u64;
        while i < iterations {
            while i < iterations {
                match tx.send(i) {
                    Ok(()) => i += 1,
                    Err(_) => break,
                }
            }
            if i < iterations {
                std::hint::spin_loop();
            }
        }
        start.elapsed()
    });

    // Receiver: receives as fast as possible
    let receiver = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut received = 0u64;
        while received < iterations {
            while let Some(v) = rx.recv() {
                debug_assert_eq!(v, received);
                received += 1;
            }
            if received < iterations {
                std::hint::spin_loop();
            }
        }
    });

    let duration = sender.join().unwrap();
    receiver.join().unwrap();

    BenchResult {
        name: "FastForward",
        iterations,
        duration,
    }
}

/// Benchmark Lamport SPSC (batched index synchronization)
fn bench_lamport(iterations: u64, capacity: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx, mut rx) = thread_channel::spsc_lamport::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Sender: sends as fast as possible, spin-waits only when full
    let sender = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut i = 0u64;
        while i < iterations {
            while i < iterations {
                match tx.send(i) {
                    Ok(()) => i += 1,
                    Err(_) => break,
                }
            }
            if i < iterations {
                std::hint::spin_loop();
            }
        }
        start.elapsed()
    });

    // Receiver: receives as fast as possible
    let receiver = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut received = 0u64;
        while received < iterations {
            while let Some(v) = rx.recv() {
                debug_assert_eq!(v, received);
                received += 1;
            }
            if received < iterations {
                std::hint::spin_loop();
            }
        }
    });

    let duration = sender.join().unwrap();
    receiver.join().unwrap();

    BenchResult {
        name: "Lamport",
        iterations,
        duration,
    }
}

/// Benchmark std::sync::mpsc
fn bench_std_mpsc(iterations: u64, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (tx, rx) = mpsc::channel::<u64>();

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    let sender = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        for i in 0..iterations {
            tx.send(i).unwrap();
        }
        start.elapsed()
    });

    let receiver = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut received = 0u64;
        while received < iterations {
            match rx.try_recv() {
                Ok(v) => {
                    debug_assert_eq!(v, received);
                    received += 1;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    std::hint::spin_loop();
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("sender disconnected");
                }
            }
        }
    });

    let duration = sender.join().unwrap();
    receiver.join().unwrap();

    BenchResult {
        name: "std::mpsc",
        iterations,
        duration,
    }
}

/// Benchmark Producer-Only Write SPSC with RPC pattern (request/response)
fn bench_producer_only_rpc(iterations: u64, capacity: usize, inflight_max: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut req_tx, mut req_rx) = thread_channel::spsc_rpc::channel::<u64>(capacity);
    let (mut resp_tx, mut resp_rx) = thread_channel::spsc::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Producer: sends requests, receives responses
    let producer = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut inflight = 0usize;
        let mut sent = 0u64;
        let mut received = 0u64;

        while received < iterations {
            // Send requests (up to inflight_max)
            while sent < iterations && inflight < inflight_max {
                req_tx.send(sent).unwrap();
                inflight += 1;
                sent += 1;
            }

            // Receive responses
            while let Some(resp_val) = resp_rx.recv() {
                debug_assert_eq!(resp_val, received);
                inflight -= 1;
                received += 1;
            }
        }

        start.elapsed()
    });

    // Consumer: receives requests, sends responses
    let consumer = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut processed = 0u64;

        while processed < iterations {
            while let Some((_, req_val)) = req_rx.recv() {
                debug_assert_eq!(req_val, processed);
                // Send response
                loop {
                    if resp_tx.send(req_val).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
                processed += 1;
            }
        }
    });

    let duration = producer.join().unwrap();
    consumer.join().unwrap();

    BenchResult {
        name: "Producer-Only (RPC)",
        iterations,
        duration,
    }
}

fn print_result(result: &BenchResult) {
    println!(
        "{:20} {:>10} iterations in {:>8.2?} ({:>8.2} Mops/s, {:>6.1} ns/op)",
        result.name,
        result.iterations,
        result.duration,
        result.mops(),
        result.duration.as_nanos() as f64 / result.iterations as f64
    );
}

fn main() {
    let args = Args::parse();

    println!("SPSC Implementation Comparison Benchmark");
    println!("========================================");
    println!("Iterations: {}", args.iterations);
    println!("Capacity: {}", args.capacity);
    println!("Warmup: {}", args.warmup);
    println!("Inflight (producer-only): {}", args.inflight);
    println!("Sender core: {}, Receiver core: {}", args.sender_core, args.receiver_core);
    println!();

    let targets: Vec<&str> = if args.target == "all" {
        vec!["fastforward", "lamport", "mpsc", "producer-only"]
    } else {
        args.target.split(',').collect()
    };

    // Throughput benchmarks
    println!("== Throughput Benchmark ==");

    for target in &targets {
        // Warmup
        match *target {
            "fastforward" => {
                let _ = bench_fastforward(args.warmup, args.capacity, args.sender_core, args.receiver_core);
                let result = bench_fastforward(args.iterations, args.capacity, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            "lamport" => {
                let _ = bench_lamport(args.warmup, args.capacity, args.sender_core, args.receiver_core);
                let result = bench_lamport(args.iterations, args.capacity, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            "mpsc" => {
                let _ = bench_std_mpsc(args.warmup, args.sender_core, args.receiver_core);
                let result = bench_std_mpsc(args.iterations, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            "producer-only" => {
                let _ = bench_producer_only_rpc(args.warmup, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                let result = bench_producer_only_rpc(args.iterations, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            _ => {
                eprintln!("Unknown target: {}", target);
            }
        }
    }

    println!();
    println!("Done.");
}
