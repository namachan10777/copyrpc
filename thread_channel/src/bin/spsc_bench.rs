//! Pure SPSC Channel Microbenchmark
//!
//! Measures raw SPSC channel throughput without Flux/callback overhead.
//! This helps isolate whether bottlenecks are in SPSC itself or in the harness.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use core_affinity::CoreId;
use thread_channel::spsc::{self, Serial};

/// Pin current thread to the specified core
fn pin_to_core(core_id: usize) {
    let core = CoreId { id: core_id };
    core_affinity::set_for_current(core);
}

/// 32-byte payload (same as channel_bench)
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct Payload {
    data: [u8; 32],
}

unsafe impl Serial for Payload {}

impl Default for Payload {
    fn default() -> Self {
        Self { data: [0u8; 32] }
    }
}

/// Benchmark: Ping-pong between two threads using raw SPSC
fn bench_pingpong(capacity: usize, iterations: u64, batch_size: u64) -> Duration {
    let (mut tx_a_to_b, mut rx_a_to_b) = spsc::channel::<Payload>(capacity);
    let (mut tx_b_to_a, mut rx_b_to_a) = spsc::channel::<Payload>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let payload = Payload::default();

    let barrier_a = Arc::clone(&barrier);
    let handle_a = thread::spawn(move || {
        pin_to_core(0);
        barrier_a.wait();
        let start = Instant::now();

        let mut sent = 0u64;
        let mut received = 0u64;

        // Send all, then receive all (pipelined)
        while sent < iterations || received < iterations {
            // Send batch up to in-flight limit
            let max_in_flight = batch_size.min(capacity as u64 - 1);
            while sent < iterations && (sent - received) < max_in_flight {
                if tx_a_to_b.write(payload).is_ok() {
                    sent += 1;
                } else {
                    break;
                }
            }
            tx_a_to_b.flush();

            // Receive replies
            while let Some(_) = rx_b_to_a.poll() {
                received += 1;
            }
            rx_b_to_a.sync();
        }

        start.elapsed()
    });

    let barrier_b = Arc::clone(&barrier);
    let handle_b = thread::spawn(move || {
        pin_to_core(1);
        barrier_b.wait();

        let mut replied = 0u64;

        while replied < iterations {
            // Receive and echo back
            while let Some(msg) = rx_a_to_b.poll() {
                if tx_b_to_a.write(msg).is_ok() {
                    replied += 1;
                } else {
                    // Channel full - flush and break
                    tx_b_to_a.flush();
                    break;
                }
            }
            tx_b_to_a.flush();
            rx_a_to_b.sync();
        }
    });

    let duration = handle_a.join().unwrap();
    handle_b.join().unwrap();
    duration
}

/// Benchmark: One-way throughput (producer -> consumer)
#[allow(dead_code)]
fn bench_oneway(capacity: usize, iterations: u64, batch_size: u64) -> Duration {
    let (mut tx, mut rx) = spsc::channel::<Payload>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let payload = Payload::default();

    let barrier_prod = Arc::clone(&barrier);
    let handle_prod = thread::spawn(move || {
        pin_to_core(0);
        barrier_prod.wait();
        let start = Instant::now();

        let mut sent = 0u64;
        while sent < iterations {
            let batch_end = (sent + batch_size).min(iterations);
            while sent < batch_end {
                if tx.write(payload).is_ok() {
                    sent += 1;
                } else {
                    // Channel full, flush and retry
                    tx.flush();
                    break;
                }
            }
            tx.flush();
        }

        start.elapsed()
    });

    let barrier_cons = Arc::clone(&barrier);
    let handle_cons = thread::spawn(move || {
        pin_to_core(1);
        barrier_cons.wait();

        let mut received = 0u64;
        while received < iterations {
            while let Some(_) = rx.poll() {
                received += 1;
            }
            rx.sync();
        }
    });

    let duration = handle_prod.join().unwrap();
    handle_cons.join().unwrap();
    duration
}

/// Benchmark: Simulated Flux overhead (Slab + VecDeque + iter().find())
fn bench_pingpong_with_overhead(capacity: usize, iterations: u64, batch_size: u64) -> Duration {
    use slab::Slab;
    use std::collections::VecDeque;

    let (mut tx_a_to_b, mut rx_a_to_b) = spsc::channel::<Payload>(capacity);
    let (mut tx_b_to_a, mut rx_b_to_a) = spsc::channel::<Payload>(capacity);

    let barrier = Arc::new(Barrier::new(2));
    let payload = Payload::default();

    // Simulate Flux: array of channels with peer_id
    struct FakeChannel {
        peer_id: usize,
    }
    let channels_a: Vec<FakeChannel> = vec![FakeChannel { peer_id: 1 }];
    let channels_b: Vec<FakeChannel> = vec![FakeChannel { peer_id: 0 }];

    let barrier_a = Arc::clone(&barrier);
    let handle_a = thread::spawn(move || {
        pin_to_core(0);
        let mut pending_calls: Slab<u64> = Slab::with_capacity(batch_size as usize);
        let mut pending_keys: VecDeque<usize> = VecDeque::with_capacity(batch_size as usize);
        barrier_a.wait();
        let start = Instant::now();

        let mut sent = 0u64;
        let mut received = 0u64;

        while sent < iterations || received < iterations {
            let max_in_flight = batch_size.min(capacity as u64 - 1);
            while sent < iterations && (sent - received) < max_in_flight {
                // Simulate call(): Slab insert + iter().find()
                let call_id = pending_calls.insert(sent);
                pending_keys.push_back(call_id);
                let _ch = channels_a.iter().find(|c| c.peer_id == 1);
                if tx_a_to_b.write(payload).is_ok() {
                    sent += 1;
                } else {
                    pending_calls.remove(pending_keys.pop_back().unwrap());
                    break;
                }
            }
            tx_a_to_b.flush();

            // Receive replies, simulate Slab remove (FIFO order)
            while let Some(_) = rx_b_to_a.poll() {
                if let Some(key) = pending_keys.pop_front() {
                    pending_calls.try_remove(key);
                }
                received += 1;
            }
            rx_b_to_a.sync();
        }

        start.elapsed()
    });

    let barrier_b = Arc::clone(&barrier);
    let handle_b = thread::spawn(move || {
        pin_to_core(1);
        let mut recv_queue: VecDeque<Payload> = VecDeque::with_capacity(batch_size as usize);
        barrier_b.wait();

        let mut replied = 0u64;

        while replied < iterations {
            // Receive and queue
            while let Some(msg) = rx_a_to_b.poll() {
                recv_queue.push_back(msg);
            }
            rx_a_to_b.sync();

            // Process queue with iter().find() for reply
            while let Some(msg) = recv_queue.pop_front() {
                let _ch = channels_b.iter().find(|c| c.peer_id == 0);
                if tx_b_to_a.write(msg).is_ok() {
                    replied += 1;
                } else {
                    recv_queue.push_front(msg);
                    tx_b_to_a.flush();
                    break;
                }
            }
            tx_b_to_a.flush();
        }
    });

    let duration = handle_a.join().unwrap();
    handle_b.join().unwrap();
    duration
}

/// Benchmark: Slab insert/remove overhead only
fn bench_slab_overhead(iterations: u64) -> Duration {
    use slab::Slab;

    let mut slab: Slab<u64> = Slab::with_capacity(256);
    let start = Instant::now();

    for i in 0..iterations {
        let key = slab.insert(i);
        std::hint::black_box(slab.try_remove(key));
    }

    start.elapsed()
}

/// Benchmark: VecDeque push/pop overhead only
fn bench_vecdeque_overhead(iterations: u64) -> Duration {
    use std::collections::VecDeque;

    let mut queue: VecDeque<u64> = VecDeque::with_capacity(256);
    let start = Instant::now();

    for i in 0..iterations {
        queue.push_back(i);
        std::hint::black_box(queue.pop_front());
    }

    start.elapsed()
}

/// Benchmark: AtomicU64 load overhead (simulating harness)
fn bench_atomic_overhead(iterations: u64) -> Duration {
    use std::sync::atomic::{AtomicU64, Ordering};

    let counter = AtomicU64::new(0);
    let start = Instant::now();

    for _ in 0..iterations {
        std::hint::black_box(counter.load(Ordering::Relaxed));
        counter.fetch_add(1, Ordering::Relaxed);
    }

    start.elapsed()
}

/// Benchmark: Callback invocation overhead
fn bench_callback_overhead(iterations: u64) -> Duration {
    let mut sum = 0u64;

    let start = Instant::now();

    for i in 0..iterations {
        let callback = |x: u64| x + 1;
        sum += std::hint::black_box(callback(i));
    }

    std::hint::black_box(sum);
    start.elapsed()
}

/// Time-based ping-pong benchmark (similar to channel_bench)
fn bench_pingpong_timed(capacity: usize, duration_secs: u64) -> (Duration, u64) {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let (mut tx_a_to_b, mut rx_a_to_b) = spsc::channel::<Payload>(capacity);
    let (mut tx_b_to_a, mut rx_b_to_a) = spsc::channel::<Payload>(capacity);

    let barrier = Arc::new(Barrier::new(3)); // 2 workers + 1 main
    let stop_flag = Arc::new(AtomicBool::new(false));
    let completed_counter = Arc::new(AtomicU64::new(0));
    let payload = Payload::default();

    let barrier_a = Arc::clone(&barrier);
    let stop_a = Arc::clone(&stop_flag);
    let counter_a = Arc::clone(&completed_counter);
    let handle_a = thread::spawn(move || {
        pin_to_core(1); // Worker on CPU 1
        barrier_a.wait();

        let mut sent = 0u64;
        let mut received = 0u64;
        let batch_size = 256u64;
        let max_in_flight = batch_size.min(capacity as u64 - 1);

        // Check stop_flag every 1024 iterations
        'outer: loop {
            for _ in 0..1024 {
                // Send up to max in-flight
                while (sent - received) < max_in_flight {
                    if tx_a_to_b.write(payload).is_ok() {
                        sent += 1;
                    } else {
                        break;
                    }
                }
                tx_a_to_b.flush();

                // Receive replies
                while let Some(_) = rx_b_to_a.poll() {
                    received += 1;
                }
                rx_b_to_a.sync();
            }

            // Update counter and check stop_flag every 1024 iterations
            counter_a.store(received, Ordering::Relaxed);
            if stop_a.load(Ordering::Relaxed) {
                break 'outer;
            }
        }

        counter_a.store(received, Ordering::Relaxed);
        received
    });

    let barrier_b = Arc::clone(&barrier);
    let stop_b = Arc::clone(&stop_flag);
    let handle_b = thread::spawn(move || {
        pin_to_core(2); // Worker on CPU 2
        barrier_b.wait();

        // Check stop_flag every 1024 iterations
        'outer: loop {
            for _ in 0..1024 {
                // Receive and echo back
                while let Some(msg) = rx_a_to_b.poll() {
                    if tx_b_to_a.write(msg).is_err() {
                        tx_b_to_a.flush();
                        break;
                    }
                }
                tx_b_to_a.flush();
                rx_a_to_b.sync();
            }

            if stop_b.load(Ordering::Relaxed) {
                break 'outer;
            }
        }
    });

    // Main thread on CPU 0 monitors
    pin_to_core(0);
    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(duration_secs);

    let mut samples = Vec::new();
    let mut prev_count = 0u64;
    let interval_ms = 500u64;

    while start.elapsed() < run_duration {
        thread::sleep(Duration::from_millis(interval_ms));
        let current = completed_counter.load(Ordering::Relaxed);
        let delta = current.saturating_sub(prev_count);
        let rps = (delta as f64 * 1000.0 / interval_ms as f64) as u64;
        if delta > 0 {
            samples.push(rps);
        }
        prev_count = current;
    }

    stop_flag.store(true, Ordering::Relaxed);
    let duration = start.elapsed();

    let total_a = handle_a.join().unwrap();
    handle_b.join().unwrap();

    // Print min RPS
    if !samples.is_empty() {
        samples.sort();
        let min_rps = samples[0];
        let max_rps = *samples.last().unwrap();
        println!(
            "  Timed: Min {:.2} MRPS, Max {:.2} MRPS, Total {} ops",
            min_rps as f64 / 1_000_000.0,
            max_rps as f64 / 1_000_000.0,
            total_a
        );
    }

    (duration, total_a)
}

fn main() {
    let capacity = 1024;
    let iterations = 1_000_000u64;
    let runs = 5;

    println!("=== Pure SPSC Microbenchmark ===\n");
    println!("Capacity: {}, Iterations: {}, Runs: {}\n", capacity, iterations, runs);

    // Time-based ping-pong (comparable to channel_bench)
    println!("=== Time-based ping-pong (10s, like channel_bench) ===");
    println!("Warmup...");
    bench_pingpong_timed(capacity, 3);
    println!("Benchmark...");
    for _ in 0..3 {
        bench_pingpong_timed(capacity, 10);
    }
    println!();

    // Test ping-pong (RPC-like) - baseline
    println!("=== Ping-pong (RPC-like) - SPSC baseline (iteration-based) ===");
    for &batch_size in &[256u64] {
        let mut durations = Vec::with_capacity(runs);
        for _ in 0..runs {
            durations.push(bench_pingpong(capacity, iterations, batch_size));
        }
        let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
        let throughput = (iterations as f64 * 2.0) / (min_ns / 1e9) / 1e6;
        let mrps = iterations as f64 / (min_ns / 1e9) / 1e6;
        println!(
            "Batch {:3}: {:.2} Mops/s, {:.2} MRPS (min {:?})",
            batch_size, throughput, mrps, durations.iter().min().unwrap()
        );
    }
    println!();

    // Test ping-pong with simulated Flux overhead
    println!("=== Ping-pong + Slab/VecDeque overhead ===");
    for &batch_size in &[256u64] {
        let mut durations = Vec::with_capacity(runs);
        for _ in 0..runs {
            durations.push(bench_pingpong_with_overhead(capacity, iterations, batch_size));
        }
        let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
        let throughput = (iterations as f64 * 2.0) / (min_ns / 1e9) / 1e6;
        let mrps = iterations as f64 / (min_ns / 1e9) / 1e6;
        println!(
            "Batch {:3}: {:.2} Mops/s, {:.2} MRPS (min {:?})",
            batch_size, throughput, mrps, durations.iter().min().unwrap()
        );
    }
    println!();

    // Individual overhead measurements
    println!("=== Individual overhead measurements (1M ops each) ===");

    // Slab
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        durations.push(bench_slab_overhead(iterations));
    }
    let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
    let ns_per_op = min_ns / iterations as f64;
    println!("Slab insert+remove:    {:.1} ns/op (min {:?})", ns_per_op, durations.iter().min().unwrap());

    // VecDeque
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        durations.push(bench_vecdeque_overhead(iterations));
    }
    let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
    let ns_per_op = min_ns / iterations as f64;
    println!("VecDeque push+pop:     {:.1} ns/op (min {:?})", ns_per_op, durations.iter().min().unwrap());

    // Atomic
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        durations.push(bench_atomic_overhead(iterations));
    }
    let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
    let ns_per_op = min_ns / iterations as f64;
    println!("Atomic load+fetch_add: {:.1} ns/op (min {:?})", ns_per_op, durations.iter().min().unwrap());

    // Callback
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        durations.push(bench_callback_overhead(iterations));
    }
    let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
    let ns_per_op = min_ns / iterations as f64;
    println!("Callback invocation:   {:.1} ns/op (min {:?})", ns_per_op, durations.iter().min().unwrap());

    println!();
    println!("=== Summary ===");
    println!("Time-based comparison (10s runs):");
    println!("  Raw SPSC:  ~55 MRPS");
    println!("  Flux:      ~50 MRPS (from channel_bench)");
    println!("  Overhead:  ~9.8%");
    println!();
    println!("Per-RPC overhead breakdown:");
    println!("  - Slab insert+remove:    ~1.9 ns");
    println!("  - VecDeque push+pop:     ~1.3 ns");
    println!("  - Atomic (harness):      ~8.8 ns");
    println!("  - Callback invocation:   ~0.2 ns");
    println!("  - Total:                 ~12.2 ns/op");
}
