//! Benchmark for OnesidedMpsc call/reply performance.
//!
//! Measures raw call/reply performance without Flux overhead.
//! Tests the bidirectional RPC pattern with proper flow control.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use inproc::Serial;
use mempc::{MpscCaller, MpscChannel, MpscRecvRef, MpscServer, OnesidedMpsc};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

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
        let (mut callers, mut server) = OnesidedMpsc::create::<Payload, Payload>(1, 64, 32);
        let mut caller = callers.pop().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            pin_to_core(30);
            while !stop2.load(Ordering::Relaxed) {
                server.poll();
                loop {
                    let (data, token) = match server.try_recv() {
                        Some(r) => (r.data(), r.into_token()),
                        None => break,
                    };
                    server.reply(token, data);
                }
            }
        });

        let payload = Payload { data: [42; 4] };

        b.iter(|| {
            pin_to_core(31);
            caller.call(black_box(payload)).unwrap();
            caller.sync();
            loop {
                if let Some((_, data)) = caller.try_recv_response() {
                    black_box(data);
                    break;
                }
                std::hint::spin_loop();
            }
        });

        stop.store(true, Ordering::Relaxed);
        caller.call(payload).ok();
        caller.sync();
        handle.join().unwrap();
    });

    group.finish();
}

/// Benchmark sustained throughput with pipelined calls.
fn bench_throughput(c: &mut Criterion) {
    const REQUESTS_PER_ITER: usize = 1024;

    let mut group = c.benchmark_group("transport_throughput");
    group.throughput(Throughput::Elements(REQUESTS_PER_ITER as u64));

    group.bench_function("onesided", |b| {
        const QUEUE_DEPTH: usize = 32;
        let capacity = QUEUE_DEPTH * 4;
        let (mut callers, mut server) =
            OnesidedMpsc::create::<Payload, Payload>(1, capacity, QUEUE_DEPTH);
        let mut caller = callers.pop().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            pin_to_core(30);
            while !stop2.load(Ordering::Relaxed) {
                server.poll();
                loop {
                    let (data, token) = match server.try_recv() {
                        Some(r) => (r.data(), r.into_token()),
                        None => break,
                    };
                    server.reply(token, data);
                }
            }
        });

        let payload = Payload { data: [42; 4] };

        b.iter(|| {
            pin_to_core(31);
            let mut sent = 0usize;
            let mut received = 0usize;
            let mut inflight = 0usize;

            while sent < REQUESTS_PER_ITER {
                while inflight < QUEUE_DEPTH && sent < REQUESTS_PER_ITER {
                    if caller.call(black_box(payload)).is_ok() {
                        sent += 1;
                        inflight += 1;
                    } else {
                        break;
                    }
                }

                caller.sync();
                while let Some((_, data)) = caller.try_recv_response() {
                    black_box(data);
                    received += 1;
                    inflight -= 1;
                }
            }

            while received < REQUESTS_PER_ITER {
                caller.sync();
                if let Some((_, data)) = caller.try_recv_response() {
                    black_box(data);
                    received += 1;
                }
            }
        });

        stop.store(true, Ordering::Relaxed);
        caller.call(payload).ok();
        caller.sync();
        handle.join().unwrap();
    });

    group.finish();
}

criterion_group!(benches, bench_pingpong, bench_throughput);
criterion_main!(benches);
