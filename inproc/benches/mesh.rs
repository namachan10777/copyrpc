//! Benchmark comparing Mesh MPSC and Flux SPSC implementations.
//!
//! Measures n-to-n communication performance with different backends.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use inproc::{FetchAddMpsc, Flux, ReceivedMessage, create_flux_with, create_mesh_with};
use std::thread;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

use inproc::Serial;
use inproc::mpsc::MpscChannel;
use mempc::MpscChannel as MempcMpscChannel;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Payload {
    data: [u64; 4],
}

unsafe impl Serial for Payload {}

// ============================================================================
// Mesh benchmarks (MPSC)
// ============================================================================

fn bench_mesh_notify(c: &mut Criterion) {
    const MSGS_PER_NODE: usize = 256;
    const NUM_NODES: usize = 4;
    const TOTAL_MSGS: usize = NUM_NODES * (NUM_NODES - 1) * MSGS_PER_NODE;

    let mut group = c.benchmark_group("mesh_notify");
    group.throughput(Throughput::Elements(TOTAL_MSGS as u64));

    group.bench_function("fetch_add", |b| {
        run_mesh_notify_bench::<FetchAddMpsc>(b, NUM_NODES, MSGS_PER_NODE);
    });

    group.finish();
}

fn run_mesh_notify_bench<M: MpscChannel>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
    msgs_per_node: usize,
) {
    b.iter(|| {
        let nodes = create_mesh_with::<Payload, (), fn((), Payload), M>(num_nodes, |(), _| {});
        let mut handles = Vec::new();

        for (node_index, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            let msgs = msgs_per_node;
            handles.push(thread::spawn(move || {
                pin_to_core(31 - node_index);
                let id = node.id();
                let payload = Payload {
                    data: [id as u64; 4],
                };

                for peer in 0..n {
                    if peer != id {
                        for _ in 0..msgs {
                            node.notify(peer, black_box(payload)).unwrap();
                        }
                    }
                }

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

fn bench_mesh_call_reply(c: &mut Criterion) {
    const CALLS_PER_NODE: usize = 64;
    const NUM_NODES: usize = 4;
    const TOTAL_CALLS: usize = NUM_NODES * (NUM_NODES - 1) * CALLS_PER_NODE;

    let mut group = c.benchmark_group("mesh_call_reply");
    group.throughput(Throughput::Elements(TOTAL_CALLS as u64));

    group.bench_function("fetch_add", |b| {
        run_mesh_call_reply_bench::<FetchAddMpsc>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.finish();
}

fn run_mesh_call_reply_bench<M: MpscChannel>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
    calls_per_node: usize,
) {
    b.iter(|| {
        let nodes = create_mesh_with::<Payload, (), fn((), Payload), M>(num_nodes, |(), _| {});
        let mut handles = Vec::new();

        for (node_index, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            let calls = calls_per_node;
            handles.push(thread::spawn(move || {
                pin_to_core(31 - node_index);
                let id = node.id();
                let payload = Payload {
                    data: [id as u64; 4],
                };

                for peer in 0..n {
                    if peer != id {
                        for _ in 0..calls {
                            node.call(peer, black_box(payload), ()).unwrap();
                        }
                    }
                }

                let expected_requests = (n - 1) * calls;
                let calls_sent = (n - 1) * calls;
                let mut requests_handled = 0;
                let mut responses_received = 0;

                while requests_handled < expected_requests || responses_received < calls_sent {
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
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    });
}

// ============================================================================
// Flux benchmarks (SPSC MpscChannel)
// ============================================================================

fn bench_flux_call_reply(c: &mut Criterion) {
    const CALLS_PER_NODE: usize = 64;
    const NUM_NODES: usize = 4;
    const TOTAL_CALLS: usize = NUM_NODES * (NUM_NODES - 1) * CALLS_PER_NODE;

    let mut group = c.benchmark_group("flux_call_reply");
    group.throughput(Throughput::Elements(TOTAL_CALLS as u64));

    group.bench_function("onesided", |b| {
        run_flux_call_reply_bench::<mempc::OnesidedMpsc>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.bench_function("fastforward", |b| {
        run_flux_call_reply_bench::<mempc::FastForwardMpsc>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.bench_function("lamport", |b| {
        run_flux_call_reply_bench::<mempc::LamportMpsc>(b, NUM_NODES, CALLS_PER_NODE);
    });

    group.finish();
}

fn run_flux_call_reply_bench<M: MempcMpscChannel>(
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

        let nodes: Vec<Flux<Payload, (), _, M>> = {
            let counter = Arc::clone(&global_response_count);
            create_flux_with(
                num_nodes,
                capacity,
                inflight_max,
                move |_: (), _: Payload| {
                    counter.fetch_add(1, Ordering::Relaxed);
                },
            )
        };
        let mut handles = Vec::new();

        for (node_index, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            let calls = calls_per_node;
            let global_count = Arc::clone(&global_response_count);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                pin_to_core(31 - node_index);
                let id = node.id();
                let payload = Payload {
                    data: [id as u64; 4],
                };
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                let total_to_send = peers.len() * calls;
                let expected_requests = peers.len() * calls;

                let mut sent_per_peer = vec![0usize; n];
                let mut total_sent = 0usize;
                let mut requests_handled = 0usize;

                while total_sent < total_to_send || requests_handled < expected_requests {
                    for &peer in &peers {
                        if sent_per_peer[peer] < calls
                            && node.call(peer, black_box(payload), ()).is_ok()
                        {
                            sent_per_peer[peer] += 1;
                            total_sent += 1;
                        }
                    }

                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        handle.reply(data);
                        requests_handled += 1;
                    }
                }

                while global_count.load(Ordering::Relaxed) < total_expected {
                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        handle.reply(data);
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

criterion_group!(
    benches,
    bench_mesh_notify,
    bench_mesh_call_reply,
    bench_flux_call_reply
);
criterion_main!(benches);
