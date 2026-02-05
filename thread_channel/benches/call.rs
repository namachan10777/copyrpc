//! Benchmark comparing Transport implementations directly.
//!
//! Measures raw call/reply performance without Flux overhead.
//! Tests the bidirectional RPC pattern with proper flow control.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use thread_channel::spsc::Serial;
use thread_channel::{
    FastForwardTransport, LamportTransport, OnesidedTransport, Transport, TransportEndpoint,
};

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct Payload {
    data: [u64; 4],
}

unsafe impl Serial for Payload {}

/// Benchmark ping-pong latency (single call/reply roundtrip).
fn bench_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport_pingpong");
    group.throughput(Throughput::Elements(1));

    group.bench_function("onesided", |b| {
        run_pingpong_bench::<OnesidedTransport>(b);
    });

    group.bench_function("fastforward", |b| {
        run_pingpong_bench::<FastForwardTransport>(b);
    });

    group.bench_function("lamport", |b| {
        run_pingpong_bench::<LamportTransport>(b);
    });

    group.finish();
}

fn run_pingpong_bench<Tr: Transport>(b: &mut criterion::Bencher) {
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(64);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            // poll() does sync for Lamport: makes peer's calls visible, our replies visible
            // Single poll() handles both directions - this is the pipeline model
            if let Some((token, data)) = {
                endpoint_b.poll();
                endpoint_b.recv()
            } {
                loop {
                    if endpoint_b.reply(token, data).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    let payload = Payload { data: [42; 4] };

    b.iter(|| {
        let token = endpoint_a.call(black_box(payload)).unwrap();
        loop {
            if let Some((t, data)) = endpoint_a.poll() {
                debug_assert_eq!(t, token);
                black_box(data);
                break;
            }
            std::hint::spin_loop();
        }
    });

    stop.store(true, Ordering::Relaxed);
    endpoint_a.call(payload).ok();
    handle.join().unwrap();
}

/// Benchmark throughput with pipelined calls.
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport_throughput");

    for batch_size in [8, 32, 128] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("onesided", batch_size),
            &batch_size,
            |b, &batch_size| {
                run_throughput_bench::<OnesidedTransport>(b, batch_size);
            },
        );

        group.bench_with_input(
            BenchmarkId::new("fastforward", batch_size),
            &batch_size,
            |b, &batch_size| {
                run_throughput_bench::<FastForwardTransport>(b, batch_size);
            },
        );

        group.bench_with_input(
            BenchmarkId::new("lamport", batch_size),
            &batch_size,
            |b, &batch_size| {
                run_throughput_bench::<LamportTransport>(b, batch_size);
            },
        );
    }

    group.finish();
}

fn run_throughput_bench<Tr: Transport>(b: &mut criterion::Bencher, batch_size: usize) {
    let capacity = (batch_size * 4).next_power_of_two();
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(capacity);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            // poll() does sync for Lamport: makes peer's calls visible, our replies visible
            // Single poll() per batch - this enables proper batching
            endpoint_b.poll();
            let mut count = 0;
            while let Some((token, data)) = endpoint_b.recv() {
                loop {
                    if endpoint_b.reply(token, data).is_ok() {
                        count += 1;
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
            black_box(count);
        }
    });

    let payload = Payload { data: [42; 4] };

    b.iter(|| {
        // Send batch of calls
        for _ in 0..batch_size {
            loop {
                if endpoint_a.call(black_box(payload)).is_ok() {
                    break;
                }
                std::hint::spin_loop();
            }
        }

        // Receive batch of responses
        let mut received = 0;
        while received < batch_size {
            if let Some((_, data)) = endpoint_a.poll() {
                black_box(data);
                received += 1;
            }
        }
    });

    stop.store(true, Ordering::Relaxed);
    endpoint_a.call(payload).ok();
    handle.join().unwrap();
}

/// Benchmark one-way call throughput (caller sends, callee receives but doesn't reply).
/// This tests the request channel only, not the full RPC pattern.
fn bench_oneway(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport_oneway");

    for batch_size in [8, 32, 128] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("onesided", batch_size),
            &batch_size,
            |b, &batch_size| {
                run_oneway_bench::<OnesidedTransport>(b, batch_size);
            },
        );

        group.bench_with_input(
            BenchmarkId::new("fastforward", batch_size),
            &batch_size,
            |b, &batch_size| {
                run_oneway_bench::<FastForwardTransport>(b, batch_size);
            },
        );

        group.bench_with_input(
            BenchmarkId::new("lamport", batch_size),
            &batch_size,
            |b, &batch_size| {
                run_oneway_bench::<LamportTransport>(b, batch_size);
            },
        );
    }

    group.finish();
}

fn run_oneway_bench<Tr: Transport>(b: &mut criterion::Bencher, batch_size: usize) {
    let capacity = (batch_size * 4).next_power_of_two();
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(capacity);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            // poll() does sync for Lamport
            endpoint_b.poll();
            while endpoint_b.recv().is_some() {}
        }
    });

    let payload = Payload { data: [42; 4] };

    b.iter(|| {
        for _ in 0..batch_size {
            loop {
                if endpoint_a.call(black_box(payload)).is_ok() {
                    break;
                }
                std::hint::spin_loop();
            }
        }
    });

    stop.store(true, Ordering::Relaxed);
    endpoint_a.call(payload).ok();
    handle.join().unwrap();
}

criterion_group!(benches, bench_pingpong, bench_throughput, bench_oneway);
criterion_main!(benches);
