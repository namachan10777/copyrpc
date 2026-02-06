//! Benchmark comparing Mesh MPSC and Flux SPSC implementations.
//!
//! Measures n-to-n communication performance with different backends.

use core_affinity::CoreId;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::thread;
use thread_channel::{
    create_flux_with_transport, create_mesh_with, Flux, Mesh, ReceivedMessage, StdMpsc,
};

#[cfg(feature = "crossbeam")]
use thread_channel::CrossbeamMpsc;

use thread_channel::mpsc::MpscChannel;
use thread_channel::Serial;
use thread_channel::{
    FastForwardTransport, LamportTransport, OnesidedTransport, Transport,
};
#[cfg(feature = "rtrb")]
use thread_channel::RtrbTransport;
#[cfg(feature = "omango")]
use thread_channel::OmangoTransport;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Payload {
    data: [u64; 4],
}

unsafe impl Serial for Payload {}

// ============================================================================
// Mesh benchmarks (MPSC)
// ============================================================================

/// Benchmark 4-node all-to-all notification throughput.
fn bench_mesh_notify(c: &mut Criterion) {
    const MSGS_PER_NODE: usize = 256;
    const NUM_NODES: usize = 4;
    const TOTAL_MSGS: usize = NUM_NODES * (NUM_NODES - 1) * MSGS_PER_NODE;

    let mut group = c.benchmark_group("mesh_notify");
    group.throughput(Throughput::Elements(TOTAL_MSGS as u64));

    group.bench_function("std_mpsc", |b| {
        run_mesh_notify_bench::<StdMpsc>(b, NUM_NODES, MSGS_PER_NODE);
    });

    #[cfg(feature = "crossbeam")]
    group.bench_function("crossbeam", |b| {
        run_mesh_notify_bench::<CrossbeamMpsc>(b, NUM_NODES, MSGS_PER_NODE);
    });

    group.finish();
}

fn run_mesh_notify_bench<M: MpscChannel>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
    msgs_per_node: usize,
) {
    b.iter(|| {
        let nodes: Vec<Mesh<Payload, M>> = create_mesh_with(num_nodes);
        let mut handles = Vec::new();

        for (idx, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            let msgs = msgs_per_node;
            handles.push(thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: idx });
                let id = node.id();
                let payload = Payload { data: [id as u64; 4] };

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for _ in 0..msgs {
                            node.notify(peer, black_box(payload)).unwrap();
                        }
                    }
                }

                // Receive from all peers
                let expected = (n - 1) * msgs;
                let mut count = 0;
                while count < expected {
                    match node.try_recv() {
                        Ok((_, ReceivedMessage::Notify(data))) => {
                            black_box(data);
                            count += 1;
                        }
                        _ => std::hint::spin_loop(),
                    }
                }
                count
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    });
}

/// Benchmark call/reply pattern in 4-node mesh.
fn bench_mesh_call_reply(c: &mut Criterion) {
    const CALLS_PER_NODE: usize = 64;
    const NUM_NODES: usize = 4;
    // Each node makes calls to all others and receives replies
    const TOTAL_CALLS: usize = NUM_NODES * (NUM_NODES - 1) * CALLS_PER_NODE;

    let mut group = c.benchmark_group("mesh_call_reply");
    group.throughput(Throughput::Elements(TOTAL_CALLS as u64));

    group.bench_function("std_mpsc", |b| {
        run_mesh_call_reply_bench::<StdMpsc>(b, NUM_NODES, CALLS_PER_NODE);
    });

    #[cfg(feature = "crossbeam")]
    group.bench_function("crossbeam", |b| {
        run_mesh_call_reply_bench::<CrossbeamMpsc>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.finish();
}

fn run_mesh_call_reply_bench<M: MpscChannel>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
    calls_per_node: usize,
) {
    b.iter(|| {
        let nodes: Vec<Mesh<Payload, M>> = create_mesh_with(num_nodes);
        let mut handles = Vec::new();

        for (idx, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            let calls = calls_per_node;
            handles.push(thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: idx });
                let id = node.id();
                let payload = Payload { data: [id as u64; 4] };

                // Send requests to all peers
                let mut calls_sent = 0;
                for peer in 0..n {
                    if peer != id {
                        for _ in 0..calls {
                            node.call(peer, black_box(payload)).unwrap();
                            calls_sent += 1;
                        }
                    }
                }

                // Handle incoming requests and responses
                let expected_requests = (n - 1) * calls;
                let expected_responses = calls_sent;
                let mut requests_handled = 0;
                let mut responses_received = 0;

                while requests_handled < expected_requests || responses_received < expected_responses
                {
                    match node.try_recv() {
                        Ok((from, ReceivedMessage::Request { req_num, data })) => {
                            black_box(data);
                            node.reply(from, req_num, data).unwrap();
                            requests_handled += 1;
                        }
                        Ok((_, ReceivedMessage::Response { data, .. })) => {
                            black_box(data);
                            responses_received += 1;
                        }
                        _ => std::hint::spin_loop(),
                    }
                }

                (requests_handled, responses_received)
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    });
}

// ============================================================================
// Flux benchmarks (SPSC Transport)
// ============================================================================

/// Benchmark 4-node all-to-all call/reply throughput via Flux.
fn bench_flux_call_reply(c: &mut Criterion) {
    const CALLS_PER_NODE: usize = 64;
    const NUM_NODES: usize = 4;
    const TOTAL_CALLS: usize = NUM_NODES * (NUM_NODES - 1) * CALLS_PER_NODE;

    let mut group = c.benchmark_group("flux_call_reply");
    group.throughput(Throughput::Elements(TOTAL_CALLS as u64));

    group.bench_function("onesided", |b| {
        run_flux_call_reply_bench::<OnesidedTransport>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.bench_function("fastforward", |b| {
        run_flux_call_reply_bench::<FastForwardTransport>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.bench_function("lamport", |b| {
        run_flux_call_reply_bench::<LamportTransport>(b, NUM_NODES, CALLS_PER_NODE);
    });

    #[cfg(feature = "rtrb")]
    group.bench_function("rtrb", |b| {
        run_flux_call_reply_bench::<RtrbTransport>(b, NUM_NODES, CALLS_PER_NODE);
    });

    #[cfg(feature = "omango")]
    group.bench_function("omango", |b| {
        run_flux_call_reply_bench::<OmangoTransport>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.finish();
}

fn run_flux_call_reply_bench<Tr: Transport>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
    calls_per_node: usize,
) {
    let capacity = 1024;
    let inflight_max = 256;

    b.iter(|| {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Barrier};

        let global_response_count = Arc::new(AtomicU64::new(0));
        let total_expected = (num_nodes * (num_nodes - 1) * calls_per_node) as u64;
        let barrier = Arc::new(Barrier::new(num_nodes));

        let nodes: Vec<Flux<Payload, (), _, Tr>> = {
            let counter = Arc::clone(&global_response_count);
            create_flux_with_transport(num_nodes, capacity, inflight_max, move |_: &mut (), _: Payload| {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        };
        let mut handles = Vec::new();

        for (idx, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            let calls = calls_per_node;
            let global_count = Arc::clone(&global_response_count);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                core_affinity::set_for_current(CoreId { id: idx });
                let id = node.id();
                let payload = Payload { data: [id as u64; 4] };
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                let total_to_send = peers.len() * calls;
                let expected_requests = peers.len() * calls;

                let mut sent_per_peer = vec![0usize; n];
                let mut total_sent = 0usize;
                let mut requests_handled = 0usize;

                // Interleave send/recv to avoid deadlock with batched transports
                while total_sent < total_to_send || requests_handled < expected_requests {
                    // Send calls (a few at a time)
                    for &peer in &peers {
                        if sent_per_peer[peer] < calls {
                            if node.call(peer, black_box(payload), ()).is_ok() {
                                sent_per_peer[peer] += 1;
                                total_sent += 1;
                            }
                        }
                    }

                    // Poll, process requests, and receive responses
                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        if !handle.reply_or_requeue(data) {
                            break;
                        }
                        requests_handled += 1;
                    }
                }

                // Wait for all responses globally
                while global_count.load(Ordering::Relaxed) < total_expected {
                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        handle.reply_or_requeue(data);
                    }
                    std::hint::spin_loop();
                }

                barrier.wait();
                total_sent
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    });
}

criterion_group!(benches, bench_mesh_notify, bench_mesh_call_reply, bench_flux_call_reply);
criterion_main!(benches);
