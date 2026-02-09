//! QD=8 pipelined call/reply benchmark comparing Flux (SPSC) vs Mesh (MPSC).
//!
//! Measures sustained throughput with bounded in-flight requests.
//! Configurations: 2-thread and 4-thread.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use inproc::{
    create_flux_with_transport, create_mesh_with, FetchAddMpsc, Flux, ReceivedMessage, Serial,
    Transport,
};
use inproc::mpsc::MpscChannel;
use inproc::{FastForwardTransport, LamportTransport, OnesidedTransport};

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Payload {
    data: [u64; 4],
}

unsafe impl Serial for Payload {}

const QD: usize = 8;
const REQUESTS_PER_PEER: usize = 1024;

// ============================================================================
// Flux (SPSC) benchmarks
// ============================================================================

fn run_flux_qd_bench<Tr: Transport>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
) {
    let capacity = QD * 4;
    let inflight_max = QD;

    b.iter(|| {
        let global_response_count = Arc::new(AtomicU64::new(0));
        let total_expected =
            (num_nodes * (num_nodes - 1) * REQUESTS_PER_PEER) as u64;
        let barrier = Arc::new(Barrier::new(num_nodes));

        let nodes: Vec<Flux<Payload, (), _, Tr>> = {
            let counter = Arc::clone(&global_response_count);
            create_flux_with_transport(
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
            let global_count = Arc::clone(&global_response_count);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                pin_to_core(31 - node_index);
                let id = node.id();
                let payload = Payload {
                    data: [id as u64; 4],
                };
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                let total_to_send = peers.len() * REQUESTS_PER_PEER;
                let expected_requests = peers.len() * REQUESTS_PER_PEER;

                let mut sent_per_peer = vec![0usize; n];
                let mut total_sent = 0usize;
                let mut requests_handled = 0usize;

                while total_sent < total_to_send || requests_handled < expected_requests {
                    for &peer in &peers {
                        if sent_per_peer[peer] < REQUESTS_PER_PEER {
                            if node.call(peer, black_box(payload), ()).is_ok() {
                                sent_per_peer[peer] += 1;
                                total_sent += 1;
                            }
                        }
                    }

                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        if !handle.reply_or_requeue(data) {
                            break;
                        }
                        requests_handled += 1;
                    }
                }

                // Drain remaining responses
                while global_count.load(Ordering::Relaxed) < total_expected {
                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        handle.reply_or_requeue(data);
                    }
                    std::hint::spin_loop();
                }

                barrier.wait();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    });
}

// ============================================================================
// Mesh (MPSC) benchmarks
// ============================================================================

fn run_mesh_qd_bench<M: MpscChannel>(
    b: &mut criterion::Bencher,
    num_nodes: usize,
) {
    b.iter(|| {
        let nodes =
            create_mesh_with::<Payload, (), fn((), Payload), M>(num_nodes, |(), _| {});
        let mut handles = Vec::new();

        for (node_index, mut node) in nodes.into_iter().enumerate() {
            let n = num_nodes;
            handles.push(thread::spawn(move || {
                pin_to_core(31 - node_index);
                let id = node.id();
                let payload = Payload {
                    data: [id as u64; 4],
                };
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                let mut sent_per_peer = vec![0usize; n];
                let mut inflight_per_peer = vec![0usize; n];
                let total_to_send = peers.len() * REQUESTS_PER_PEER;
                let expected_requests = peers.len() * REQUESTS_PER_PEER;

                let mut total_sent = 0;
                let mut requests_handled = 0;
                let mut responses_received = 0;

                while total_sent < total_to_send
                    || requests_handled < expected_requests
                    || responses_received < total_to_send
                {
                    // Send with QD limit
                    for &peer in &peers {
                        if sent_per_peer[peer] < REQUESTS_PER_PEER
                            && inflight_per_peer[peer] < QD
                        {
                            node.call(peer, black_box(payload), ()).unwrap();
                            sent_per_peer[peer] += 1;
                            inflight_per_peer[peer] += 1;
                            total_sent += 1;
                        }
                    }

                    // Process messages
                    match node.try_recv() {
                        Ok((from, ReceivedMessage::Request { req_num, data })) => {
                            black_box(data);
                            node.reply(from, req_num, data).unwrap();
                            requests_handled += 1;
                        }
                        Ok((from, ReceivedMessage::Response { data, .. })) => {
                            black_box(data);
                            inflight_per_peer[from] -= 1;
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
// Benchmark groups
// ============================================================================

fn bench_qd8_2thread(c: &mut Criterion) {
    let total = 2 * 1 * REQUESTS_PER_PEER;

    let mut group = c.benchmark_group("qd8_2thread");
    group.throughput(Throughput::Elements(total as u64));

    // Flux (SPSC)
    group.bench_function("flux_onesided", |b| run_flux_qd_bench::<OnesidedTransport>(b, 2));
    group.bench_function("flux_fastforward", |b| run_flux_qd_bench::<FastForwardTransport>(b, 2));
    group.bench_function("flux_lamport", |b| run_flux_qd_bench::<LamportTransport>(b, 2));

    // Mesh (MPSC)
    group.bench_function("mesh_fetch_add", |b| run_mesh_qd_bench::<FetchAddMpsc>(b, 2));

    group.finish();
}

fn bench_qd8_4thread(c: &mut Criterion) {
    let total = 4 * 3 * REQUESTS_PER_PEER;

    let mut group = c.benchmark_group("qd8_4thread");
    group.throughput(Throughput::Elements(total as u64));

    // Flux (SPSC)
    group.bench_function("flux_onesided", |b| run_flux_qd_bench::<OnesidedTransport>(b, 4));
    group.bench_function("flux_fastforward", |b| run_flux_qd_bench::<FastForwardTransport>(b, 4));
    group.bench_function("flux_lamport", |b| run_flux_qd_bench::<LamportTransport>(b, 4));

    // Mesh (MPSC)
    group.bench_function("mesh_fetch_add", |b| run_mesh_qd_bench::<FetchAddMpsc>(b, 4));

    group.finish();
}

criterion_group!(benches, bench_qd8_2thread, bench_qd8_4thread);
criterion_main!(benches);
