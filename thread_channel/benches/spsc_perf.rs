// SPSC performance benchmark for measuring optimization impact

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use thread_channel::spsc;

const ITERATIONS: usize = 10_000_000;
const CAPACITY: usize = 1024;

fn bench_send() {
    let (mut tx, mut rx) = spsc::channel::<u64>(CAPACITY);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();

    let receiver = thread::spawn(move || {
        barrier2.wait();
        let mut count = 0;
        while count < ITERATIONS {
            while let Some(_) = rx.recv() {
                count += 1;
            }
        }
    });

    barrier.wait();
    let start = Instant::now();

    for i in 0..ITERATIONS {
        loop {
            match tx.send(i as u64) {
                Ok(()) => break,
                Err(_) => std::hint::spin_loop(),
            }
        }
    }

    receiver.join().unwrap();
    let elapsed = start.elapsed();

    println!("send benchmark:");
    println!("  Iterations: {}", ITERATIONS);
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} Mops/s", ITERATIONS as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    println!("  Latency: {:.2} ns/op", elapsed.as_nanos() as f64 / ITERATIONS as f64);
}

fn bench_try_recv() {
    let (mut tx, mut rx) = spsc::channel::<u64>(CAPACITY);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();

    let receiver = thread::spawn(move || {
        barrier2.wait();
        let start = Instant::now();
        let mut count = 0;
        while count < ITERATIONS {
            match rx.try_recv() {
                Ok(_) => count += 1,
                Err(_) => std::hint::spin_loop(),
            }
        }
        let elapsed = start.elapsed();
        (count, elapsed)
    });

    barrier.wait();
    for i in 0..ITERATIONS {
        loop {
            match tx.send(i as u64) {
                Ok(()) => break,
                Err(_) => std::hint::spin_loop(),
            }
        }
    }

    let (count, elapsed) = receiver.join().unwrap();

    println!("\ntry_recv benchmark:");
    println!("  Iterations: {}", count);
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} Mops/s", count as f64 / elapsed.as_secs_f64() / 1_000_000.0);
    println!("  Latency: {:.2} ns/op", elapsed.as_nanos() as f64 / count as f64);
}

fn main() {
    println!("SPSC Performance Benchmark");
    println!("Capacity: {}", CAPACITY);
    println!();

    bench_send();
    bench_try_recv();
}
