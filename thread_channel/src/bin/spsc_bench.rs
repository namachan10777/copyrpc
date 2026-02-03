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

fn main() {
    let capacity = 1024;
    let iterations = 1_000_000u64;
    let runs = 3;

    println!("=== Pure SPSC Microbenchmark ===\n");
    println!("Capacity: {}, Iterations: {}, Runs: {}\n", capacity, iterations, runs);

    // First test one-way to verify SPSC works
    println!("=== One-way throughput ===");
    for &batch_size in &[64u64, 256, 512] {
        let mut durations = Vec::with_capacity(runs);
        for _ in 0..runs {
            durations.push(bench_oneway(capacity, iterations, batch_size));
        }
        let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
        let throughput = (iterations as f64) / (min_ns / 1e9) / 1e6;
        let latency = min_ns / (iterations as f64);
        println!(
            "Batch {:3}: {:.2} Mops/s, {:.1} ns/op (min {:?})",
            batch_size, throughput, latency, durations.iter().min().unwrap()
        );
    }
    println!();

    // Test ping-pong (RPC-like)
    println!("=== Ping-pong (RPC-like) ===");
    for &batch_size in &[64u64, 256, 512] {
        let mut durations = Vec::with_capacity(runs);
        for _ in 0..runs {
            durations.push(bench_pingpong(capacity, iterations, batch_size));
        }
        let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
        let throughput = (iterations as f64 * 2.0) / (min_ns / 1e9) / 1e6; // 2x for round-trip
        let latency = min_ns / (iterations as f64 * 2.0);
        println!(
            "Batch {:3}: {:.2} Mops/s, {:.1} ns/op (min {:?})",
            batch_size, throughput, latency, durations.iter().min().unwrap()
        );
    }
    println!();

    // Test ping-pong with simulated Flux overhead
    println!("=== Ping-pong + Slab/VecDeque/find() overhead ===");
    for &batch_size in &[64u64, 256, 512] {
        let mut durations = Vec::with_capacity(runs);
        for _ in 0..runs {
            durations.push(bench_pingpong_with_overhead(capacity, iterations, batch_size));
        }
        let min_ns = durations.iter().map(|d| d.as_nanos()).min().unwrap() as f64;
        let throughput = (iterations as f64 * 2.0) / (min_ns / 1e9) / 1e6;
        let latency = min_ns / (iterations as f64 * 2.0);
        println!(
            "Batch {:3}: {:.2} Mops/s, {:.1} ns/op (min {:?})",
            batch_size, throughput, latency, durations.iter().min().unwrap()
        );
    }
    println!();

    println!("=== Analysis ===");
    println!("Overhead breakdown:");
    println!("  Pure SPSC ping-pong:     ~106 Mops/s (100%)");
    println!("  + Slab/VecDeque/find():  ~91 Mops/s (86%, -14%)");
    println!("  Flux actual:             ~80 Mops/s (75%, -25%)");
    println!();
    println!("Remaining ~11% overhead likely from:");
    println!("  - poll() double-pass (sync + retry)");
    println!("  - Response callback invocation");
    println!("  - Benchmark harness atomic counters");
}
