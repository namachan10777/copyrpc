//! Benchmark comparing Transport implementations directly.
//!
//! Measures raw call/reply performance without Flux overhead.
//! Tests the bidirectional RPC pattern with proper flow control.

use core_affinity::CoreId;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
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
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(64, 32);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);

    // Pin client (main) thread to core 0
    core_affinity::set_for_current(CoreId { id: 0 });

    let handle = thread::spawn(move || {
        // Pin responder thread to core 1
        core_affinity::set_for_current(CoreId { id: 1 });
        while !stop2.load(Ordering::Relaxed) {
            // sync() makes peer's calls visible, our replies visible
            endpoint_b.sync();
            if let Some((token, data)) = endpoint_b.recv() {
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
            endpoint_a.sync();
            if let Some((t, data)) = endpoint_a.try_recv_response() {
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

/// Benchmark sustained throughput with pipelined calls.
/// Uses QUEUE_DEPTH=32 and processes 1024 requests per iteration to measure steady-state.
fn bench_throughput(c: &mut Criterion) {
    const REQUESTS_PER_ITER: usize = 1024;

    let mut group = c.benchmark_group("transport_throughput");
    group.throughput(Throughput::Elements(REQUESTS_PER_ITER as u64));

    group.bench_function("onesided", |b| {
        run_throughput_bench::<OnesidedTransport>(b, REQUESTS_PER_ITER);
    });

    group.bench_function("fastforward", |b| {
        run_throughput_bench::<FastForwardTransport>(b, REQUESTS_PER_ITER);
    });

    group.bench_function("lamport", |b| {
        run_throughput_bench::<LamportTransport>(b, REQUESTS_PER_ITER);
    });

    group.finish();
}

fn run_throughput_bench<Tr: Transport>(b: &mut criterion::Bencher, requests: usize) {
    const QUEUE_DEPTH: usize = 32;
    let capacity = QUEUE_DEPTH * 4; // Ensure enough capacity for pipelining
    let (mut endpoint_a, mut endpoint_b) = Tr::channel::<Payload, Payload>(capacity, QUEUE_DEPTH);

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);

    // Pin client (main) thread to core 0
    core_affinity::set_for_current(CoreId { id: 0 });

    // Responder thread: sync() then process all requests
    let handle = thread::spawn(move || {
        // Pin responder thread to core 1
        core_affinity::set_for_current(CoreId { id: 1 });
        while !stop2.load(Ordering::Relaxed) {
            // sync() makes peer's calls visible, makes our replies visible
            endpoint_b.sync();
            // recv() as much as possible and reply() without syncing
            while let Some((token, data)) = endpoint_b.recv() {
                while endpoint_b.reply(token, data).is_err() {
                    std::hint::spin_loop();
                }
            }
        }
    });

    let payload = Payload { data: [42; 4] };

    b.iter(|| {
        let mut sent = 0usize;
        let mut received = 0usize;
        let mut inflight = 0usize;

        // Pipeline: maintain inflight = QUEUE_DEPTH while sending requests
        while sent < requests {
            // Fill up to QUEUE_DEPTH
            while inflight < QUEUE_DEPTH && sent < requests {
                if endpoint_a.call(black_box(payload)).is_ok() {
                    sent += 1;
                    inflight += 1;
                } else {
                    break;
                }
            }

            // sync() then receive responses to free up inflight slots
            endpoint_a.sync();
            while let Some((_, data)) = endpoint_a.try_recv_response() {
                black_box(data);
                received += 1;
                inflight -= 1;
            }
        }

        // Drain: receive remaining responses
        while received < requests {
            endpoint_a.sync();
            if let Some((_, data)) = endpoint_a.try_recv_response() {
                black_box(data);
                received += 1;
            }
        }
    });

    stop.store(true, Ordering::Relaxed);
    endpoint_a.call(payload).ok();
    handle.join().unwrap();
}

criterion_group!(benches, bench_pingpong, bench_throughput);
criterion_main!(benches);
