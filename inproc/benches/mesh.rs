//! Benchmark for Flux (SPSC) all-to-all communication.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use inproc::{Flux, Serial, create_flux};
use std::thread;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Payload {
    data: [u64; 4],
}

unsafe impl Serial for Payload {}

fn bench_flux_call_reply(c: &mut Criterion) {
    const CALLS_PER_NODE: usize = 64;
    const NUM_NODES: usize = 4;
    const TOTAL_CALLS: usize = NUM_NODES * (NUM_NODES - 1) * CALLS_PER_NODE;

    let mut group = c.benchmark_group("flux_call_reply");
    group.throughput(Throughput::Elements(TOTAL_CALLS as u64));

    group.bench_function("onesided", |b| {
        let capacity = 1024;
        let inflight_max = 256;

        b.iter(|| {
            use std::sync::atomic::{AtomicU64, Ordering};
            use std::sync::{Arc, Barrier};

            let global_response_count = Arc::new(AtomicU64::new(0));
            let total_expected = TOTAL_CALLS as u64;
            let barrier = Arc::new(Barrier::new(NUM_NODES));

            let nodes: Vec<Flux<Payload, (), _>> = {
                let counter = Arc::clone(&global_response_count);
                create_flux(
                    NUM_NODES,
                    capacity,
                    inflight_max,
                    move |_: (), _: Payload| {
                        counter.fetch_add(1, Ordering::Relaxed);
                    },
                )
            };
            let mut handles = Vec::new();

            for (node_index, mut node) in nodes.into_iter().enumerate() {
                let global_count = Arc::clone(&global_response_count);
                let barrier = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    pin_to_core(31 - node_index);
                    let id = node.id();
                    let payload = Payload {
                        data: [id as u64; 4],
                    };
                    let peers: Vec<usize> = (0..NUM_NODES).filter(|&p| p != id).collect();

                    let total_to_send = peers.len() * CALLS_PER_NODE;
                    let expected_requests = peers.len() * CALLS_PER_NODE;

                    let mut sent_per_peer = vec![0usize; NUM_NODES];
                    let mut total_sent = 0usize;
                    let mut requests_handled = 0usize;

                    while total_sent < total_to_send || requests_handled < expected_requests {
                        for &peer in &peers {
                            if sent_per_peer[peer] < CALLS_PER_NODE
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
    });

    group.finish();
}

criterion_group!(benches, bench_flux_call_reply);
criterion_main!(benches);
