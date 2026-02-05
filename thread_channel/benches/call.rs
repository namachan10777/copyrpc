//! Benchmark comparing Transport implementations via Flux call/reply pattern.
//!
//! Measures:
//! - Ping-pong latency (single call/reply roundtrip)
//! - Throughput with 32 queue depth (pipelined calls)
//!
//! Compares:
//! - OnesidedTransport (default, producer-only write)
//! - FastForwardTransport (validity flags)
//! - LamportTransport (batched index sync)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use thread_channel::{
    create_flux_with_transport, FastForwardTransport, Flux, LamportTransport, OnesidedTransport,
    SendError, Transport,
};

const QUEUE_DEPTH: usize = 32;

/// Benchmark ping-pong latency (single call/reply).
fn bench_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("pingpong");
    group.throughput(Throughput::Elements(1));

    // OnesidedTransport
    group.bench_function("onesided", |b| {
        run_pingpong_bench::<OnesidedTransport>(b);
    });

    // FastForwardTransport
    group.bench_function("fastforward", |b| {
        run_pingpong_bench::<FastForwardTransport>(b);
    });

    // LamportTransport
    group.bench_function("lamport", |b| {
        run_pingpong_bench::<LamportTransport>(b);
    });

    group.finish();
}

fn run_pingpong_bench<Tr: Transport>(b: &mut criterion::Bencher) {
    let response_received = Arc::new(AtomicBool::new(false));

    let nodes: Vec<Flux<u64, Arc<AtomicBool>, _, Tr>> =
        create_flux_with_transport(2, 64, 32, move |flag: &mut Arc<AtomicBool>, _data: u64| {
            flag.store(true, Ordering::Release);
        });
    let mut nodes: Vec<_> = nodes.into_iter().collect();
    let mut node1 = nodes.pop().unwrap();
    let mut node0 = nodes.pop().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();

    let handle = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            node1.poll();
            while let Some(handle) = node1.try_recv() {
                let data = handle.data();
                handle.reply(data).ok();
            }
        }
        node1
    });

    b.iter(|| {
        response_received.store(false, Ordering::Release);
        node0
            .call(1, black_box(42), response_received.clone())
            .unwrap();
        loop {
            node0.poll();
            while node0.try_recv().is_some() {}
            if response_received.load(Ordering::Acquire) {
                break;
            }
            std::hint::spin_loop();
        }
    });

    stop.store(true, Ordering::Relaxed);
    node0
        .call(1, 0, Arc::new(AtomicBool::new(false)))
        .ok();
    node0.poll();
    handle.join().unwrap();
}

/// Benchmark throughput with 32 queue depth (pipelined calls).
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_32depth");
    group.throughput(Throughput::Elements(QUEUE_DEPTH as u64));

    // OnesidedTransport
    group.bench_function("onesided", |b| {
        run_throughput_bench::<OnesidedTransport>(b);
    });

    // FastForwardTransport
    group.bench_function("fastforward", |b| {
        run_throughput_bench::<FastForwardTransport>(b);
    });

    // LamportTransport
    group.bench_function("lamport", |b| {
        run_throughput_bench::<LamportTransport>(b);
    });

    group.finish();
}

fn run_throughput_bench<Tr: Transport>(b: &mut criterion::Bencher) {
    let completed_count = Arc::new(AtomicU64::new(0));

    let nodes: Vec<Flux<u64, Arc<AtomicU64>, _, Tr>> = {
        let completed = Arc::clone(&completed_count);
        create_flux_with_transport(2, 128, QUEUE_DEPTH, move |_: &mut Arc<AtomicU64>, _: u64| {
            completed.fetch_add(1, Ordering::Relaxed);
        })
    };
    let mut nodes: Vec<_> = nodes.into_iter().collect();
    let mut node1 = nodes.pop().unwrap();
    let mut node0 = nodes.pop().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();

    let handle = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            node1.poll();
            while let Some(handle) = node1.try_recv() {
                let data = handle.data();
                handle.reply(data).ok();
            }
        }
        node1
    });

    b.iter(|| {
        completed_count.store(0, Ordering::Relaxed);

        // Send QUEUE_DEPTH calls
        for i in 0..QUEUE_DEPTH {
            loop {
                match node0.call(1, black_box(i as u64), Arc::new(AtomicU64::new(0))) {
                    Ok(_) => break,
                    Err(SendError::Full(_)) => {
                        node0.poll();
                        std::hint::spin_loop();
                    }
                    Err(e) => panic!("send error: {:?}", e),
                }
            }
        }

        // Wait for all responses
        while completed_count.load(Ordering::Relaxed) < QUEUE_DEPTH as u64 {
            node0.poll();
            std::hint::spin_loop();
        }
    });

    stop.store(true, Ordering::Relaxed);
    node0.call(1, 0, Arc::new(AtomicU64::new(0))).ok();
    node0.poll();
    handle.join().unwrap();
}

/// Benchmark burst send (send N messages, then receive all responses).
fn bench_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("burst");

    for burst_size in [8, 32, 64, 128] {
        group.throughput(Throughput::Elements(burst_size as u64));

        group.bench_with_input(
            BenchmarkId::new("onesided", burst_size),
            &burst_size,
            |b, &burst_size| {
                run_burst_bench::<OnesidedTransport>(b, burst_size);
            },
        );

        group.bench_with_input(
            BenchmarkId::new("fastforward", burst_size),
            &burst_size,
            |b, &burst_size| {
                run_burst_bench::<FastForwardTransport>(b, burst_size);
            },
        );

        group.bench_with_input(
            BenchmarkId::new("lamport", burst_size),
            &burst_size,
            |b, &burst_size| {
                run_burst_bench::<LamportTransport>(b, burst_size);
            },
        );
    }

    group.finish();
}

fn run_burst_bench<Tr: Transport>(b: &mut criterion::Bencher, burst_size: usize) {
    let completed_count = Arc::new(AtomicU64::new(0));

    let capacity = (burst_size * 2).next_power_of_two();
    let inflight_max = burst_size;

    let nodes: Vec<Flux<u64, Arc<AtomicU64>, _, Tr>> = {
        let completed = Arc::clone(&completed_count);
        create_flux_with_transport(2, capacity, inflight_max, move |_: &mut Arc<AtomicU64>, _: u64| {
            completed.fetch_add(1, Ordering::Relaxed);
        })
    };
    let mut nodes: Vec<_> = nodes.into_iter().collect();
    let mut node1 = nodes.pop().unwrap();
    let mut node0 = nodes.pop().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();

    let handle = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            node1.poll();
            while let Some(handle) = node1.try_recv() {
                let data = handle.data();
                handle.reply(data).ok();
            }
        }
        node1
    });

    b.iter(|| {
        completed_count.store(0, Ordering::Relaxed);

        // Burst send
        for i in 0..burst_size {
            loop {
                match node0.call(1, black_box(i as u64), Arc::new(AtomicU64::new(0))) {
                    Ok(_) => break,
                    Err(SendError::Full(_)) => {
                        node0.poll();
                        std::hint::spin_loop();
                    }
                    Err(e) => panic!("send error: {:?}", e),
                }
            }
        }

        // Wait for all responses
        while completed_count.load(Ordering::Relaxed) < burst_size as u64 {
            node0.poll();
            std::hint::spin_loop();
        }
    });

    stop.store(true, Ordering::Relaxed);
    node0.call(1, 0, Arc::new(AtomicU64::new(0))).ok();
    node0.poll();
    handle.join().unwrap();
}

criterion_group!(benches, bench_pingpong, bench_throughput, bench_burst);
criterion_main!(benches);
