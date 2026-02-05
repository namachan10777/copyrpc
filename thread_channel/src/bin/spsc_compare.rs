//! SPSC implementation comparison benchmark.
//!
//! Compares four SPSC implementations using call/response (pingpong) pattern:
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
    /// Number of iterations (call/response pairs)
    #[arg(short, long, default_value_t = 1_000_000)]
    iterations: u64,

    /// Queue capacity
    #[arg(short, long, default_value_t = 256)]
    capacity: usize,

    /// Which implementations to benchmark (all, fastforward, lamport, mpsc, producer-only)
    #[arg(short = 't', long, default_value = "all")]
    target: String,

    /// Number of warmup iterations
    #[arg(short, long, default_value_t = 10_000)]
    warmup: u64,

    /// Max in-flight requests
    #[arg(long, default_value_t = 128)]
    inflight: usize,

    /// Sender CPU core
    #[arg(long, default_value_t = 0)]
    sender_core: usize,

    /// Receiver CPU core
    #[arg(long, default_value_t = 1)]
    receiver_core: usize,

    /// Benchmark mode: "both", "oneway", "pingpong"
    #[arg(long, default_value = "both")]
    mode: String,
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

/// Benchmark FastForward SPSC with call/response pattern
fn bench_fastforward(iterations: u64, capacity: usize, inflight_max: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx_req, mut rx_req) = thread_channel::spsc::channel::<u64>(capacity);
    let (mut tx_resp, mut rx_resp) = thread_channel::spsc::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Client: sends requests, receives responses
    let client = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut inflight = 0usize;
        let mut sent = 0u64;
        let mut received = 0u64;

        while received < iterations {
            // Send requests (up to inflight_max)
            while sent < iterations && inflight < inflight_max {
                if tx_req.send(sent).is_ok() {
                    inflight += 1;
                    sent += 1;
                } else {
                    break;
                }
            }

            // Receive responses
            while let Some(resp_val) = rx_resp.recv() {
                debug_assert_eq!(resp_val, received);
                inflight -= 1;
                received += 1;
            }
        }

        start.elapsed()
    });

    // Server: receives requests, sends responses
    let server = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut processed = 0u64;

        while processed < iterations {
            while let Some(req_val) = rx_req.recv() {
                debug_assert_eq!(req_val, processed);
                loop {
                    if tx_resp.send(req_val).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
                processed += 1;
            }
        }
    });

    let duration = client.join().unwrap();
    server.join().unwrap();

    BenchResult {
        name: "FastForward",
        iterations,
        duration,
    }
}

/// Benchmark Lamport SPSC with call/response pattern
fn bench_lamport(iterations: u64, capacity: usize, inflight_max: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx_req, mut rx_req) = thread_channel::spsc_lamport::channel::<u64>(capacity);
    let (mut tx_resp, mut rx_resp) = thread_channel::spsc_lamport::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Client: sends requests, receives responses
    let client = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut inflight = 0usize;
        let mut sent = 0u64;
        let mut received = 0u64;

        while received < iterations {
            // Send requests (up to inflight_max)
            while sent < iterations && inflight < inflight_max {
                if tx_req.send(sent).is_ok() {
                    inflight += 1;
                    sent += 1;
                } else {
                    break;
                }
            }
            tx_req.poll();  // Publish batch

            // Receive responses
            while let Some(resp_val) = rx_resp.recv() {
                debug_assert_eq!(resp_val, received);
                inflight -= 1;
                received += 1;
            }
            rx_resp.poll();  // Free slots for sender
        }

        start.elapsed()
    });

    // Server: receives requests, sends responses
    let server = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut processed = 0u64;

        while processed < iterations {
            // Receive batch
            while let Some(req_val) = rx_req.recv() {
                debug_assert_eq!(req_val, processed);
                loop {
                    if tx_resp.send(req_val).is_ok() {
                        break;
                    }
                    tx_resp.poll();  // Publish what we have
                    rx_req.poll();   // Free slots
                    std::hint::spin_loop();
                }
                processed += 1;
            }
            tx_resp.poll();  // Publish responses
            rx_req.poll();   // Free slots
        }
    });

    let duration = client.join().unwrap();
    server.join().unwrap();

    BenchResult {
        name: "Lamport",
        iterations,
        duration,
    }
}

/// Benchmark std::sync::mpsc with call/response pattern
fn bench_std_mpsc(iterations: u64, inflight_max: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (tx_req, rx_req) = mpsc::channel::<u64>();
    let (tx_resp, rx_resp) = mpsc::channel::<u64>();

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Client: sends requests, receives responses
    let client = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut inflight = 0usize;
        let mut sent = 0u64;
        let mut received = 0u64;

        while received < iterations {
            // Send requests (up to inflight_max)
            while sent < iterations && inflight < inflight_max {
                tx_req.send(sent).unwrap();
                inflight += 1;
                sent += 1;
            }

            // Receive responses (must receive at least one if inflight > 0)
            if inflight > 0 {
                // Block until at least one response
                let resp_val = rx_resp.recv().unwrap();
                debug_assert_eq!(resp_val, received);
                inflight -= 1;
                received += 1;

                // Drain any additional responses
                while let Ok(resp_val) = rx_resp.try_recv() {
                    debug_assert_eq!(resp_val, received);
                    inflight -= 1;
                    received += 1;
                }
            }
        }

        start.elapsed()
    });

    // Server: receives requests, sends responses
    let server = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut processed = 0u64;

        while processed < iterations {
            match rx_req.recv() {
                Ok(req_val) => {
                    debug_assert_eq!(req_val, processed);
                    tx_resp.send(req_val).unwrap();
                    processed += 1;
                }
                Err(_) => break,
            }
        }
    });

    let duration = client.join().unwrap();
    server.join().unwrap();

    BenchResult {
        name: "std::mpsc",
        iterations,
        duration,
    }
}

/// Benchmark Producer-Only Write SPSC with call/response pattern
fn bench_producer_only(iterations: u64, capacity: usize, inflight_max: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx_req, mut rx_req) = thread_channel::spsc_rpc::channel::<u64>(capacity);
    let (mut tx_resp, mut rx_resp) = thread_channel::spsc::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    // Client: sends requests, receives responses
    let client = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut inflight = 0usize;
        let mut sent = 0u64;
        let mut received = 0u64;

        while received < iterations {
            // Send requests (up to inflight_max)
            while sent < iterations && inflight < inflight_max {
                tx_req.send(sent).unwrap();
                inflight += 1;
                sent += 1;
            }

            // Receive responses
            while let Some(resp_val) = rx_resp.recv() {
                debug_assert_eq!(resp_val, received);
                inflight -= 1;
                received += 1;
            }
        }

        start.elapsed()
    });

    // Server: receives requests, sends responses
    let server = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut processed = 0u64;

        while processed < iterations {
            while let Some((_, req_val)) = rx_req.recv() {
                debug_assert_eq!(req_val, processed);
                loop {
                    if tx_resp.send(req_val).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
                processed += 1;
            }
        }
    });

    let duration = client.join().unwrap();
    server.join().unwrap();

    BenchResult {
        name: "Producer-Only",
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

/// Benchmark Lamport SPSC one-way (no response)
fn bench_lamport_oneway(iterations: u64, capacity: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx, mut rx) = thread_channel::spsc_lamport::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    let sender = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut i = 0u64;
        while i < iterations {
            while i < iterations {
                if tx.send(i).is_ok() { i += 1; } else { break; }
            }
            tx.poll();  // Publish batch
            if i < iterations { std::hint::spin_loop(); }
        }
        start.elapsed()
    });

    let receiver = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut received = 0u64;
        while received < iterations {
            while let Some(_) = rx.recv() { received += 1; }
            rx.poll();  // Free slots
            if received < iterations { std::hint::spin_loop(); }
        }
    });

    let duration = sender.join().unwrap();
    receiver.join().unwrap();

    BenchResult {
        name: "Lamport (oneway)",
        iterations,
        duration,
    }
}

/// Benchmark FastForward SPSC one-way (no response)
fn bench_fastforward_oneway(iterations: u64, capacity: usize, sender_core: usize, receiver_core: usize) -> BenchResult {
    let (mut tx, mut rx) = thread_channel::spsc::channel::<u64>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    let sender = thread::spawn(move || {
        pin_to_core(sender_core);
        barrier_clone.wait();
        let start = Instant::now();

        let mut i = 0u64;
        while i < iterations {
            while i < iterations {
                if tx.send(i).is_ok() { i += 1; } else { break; }
            }
            if i < iterations { std::hint::spin_loop(); }
        }
        start.elapsed()
    });

    let receiver = thread::spawn(move || {
        pin_to_core(receiver_core);
        barrier.wait();

        let mut received = 0u64;
        while received < iterations {
            while let Some(_) = rx.recv() { received += 1; }
            if received < iterations { std::hint::spin_loop(); }
        }
    });

    let duration = sender.join().unwrap();
    receiver.join().unwrap();

    BenchResult {
        name: "FastForward (oneway)",
        iterations,
        duration,
    }
}

fn main() {
    let args = Args::parse();

    println!("SPSC Implementation Comparison Benchmark");
    println!("=========================================================");
    println!("Iterations: {}", args.iterations);
    println!("Capacity: {}", args.capacity);
    println!("Inflight max: {}", args.inflight);
    println!("Warmup: {}", args.warmup);
    println!("Sender core: {}, Receiver core: {}", args.sender_core, args.receiver_core);
    println!();

    let targets: Vec<&str> = if args.target == "all" {
        vec!["fastforward", "lamport", "mpsc", "producer-only"]
    } else {
        args.target.split(',').collect()
    };

    // One-way benchmarks
    if args.mode == "both" || args.mode == "oneway" {
        println!("== One-way Throughput (sender -> receiver only) ==");
        if targets.contains(&"fastforward") || args.target == "all" {
            let _ = bench_fastforward_oneway(args.warmup, args.capacity, args.sender_core, args.receiver_core);
            print_result(&bench_fastforward_oneway(args.iterations, args.capacity, args.sender_core, args.receiver_core));
        }
        if targets.contains(&"lamport") || args.target == "all" {
            let _ = bench_lamport_oneway(args.warmup, args.capacity, args.sender_core, args.receiver_core);
            print_result(&bench_lamport_oneway(args.iterations, args.capacity, args.sender_core, args.receiver_core));
        }
        println!();
    }

    if args.mode != "oneway" {
        // Call/response benchmarks
        println!("== Call/Response Throughput (pingpong) ==");

    for target in &targets {
        // Warmup
        match *target {
            "fastforward" => {
                let _ = bench_fastforward(args.warmup, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                let result = bench_fastforward(args.iterations, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            "lamport" => {
                let _ = bench_lamport(args.warmup, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                let result = bench_lamport(args.iterations, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            "mpsc" => {
                let _ = bench_std_mpsc(args.warmup, args.inflight, args.sender_core, args.receiver_core);
                let result = bench_std_mpsc(args.iterations, args.inflight, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            "producer-only" => {
                let _ = bench_producer_only(args.warmup, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                let result = bench_producer_only(args.iterations, args.capacity, args.inflight, args.sender_core, args.receiver_core);
                print_result(&result);
            }
            _ => {
                eprintln!("Unknown target: {}", target);
            }
        }
    }
    }

    println!();
    println!("Done.");
}
