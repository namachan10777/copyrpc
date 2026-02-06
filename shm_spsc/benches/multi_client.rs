//! Benchmark for shm_spsc multi-client throughput.
//!
//! 16 client threads issue RPC calls concurrently to 1 server thread.
//! Threads are pre-spawned; iteration count is distributed via AtomicU64
//! and synchronized with two Barriers per batch.
//! All threads are pinned to high-numbered cores (15-31) to avoid
//! IRQ-handling cores (0,1) and system noise.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use shm_spsc::{Client, Server};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use uuid::Uuid;

const NUM_CLIENTS: usize = 16;
const SERVER_CORE: usize = 31;
const CLIENT_CORE_BASE: usize = 15;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

fn run_bench(b: &mut criterion::Bencher, depth: u32) {
    let name = format!("/shm_rpc_mc_{}", Uuid::now_v7());

    let iters_atomic = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start_barrier = Arc::new(Barrier::new(NUM_CLIENTS + 1));
    let end_barrier = Arc::new(Barrier::new(NUM_CLIENTS + 1));

    unsafe {
        let mut server = Server::<u64, u64>::create(&name, NUM_CLIENTS as u32, depth).unwrap();

        let stop_server = stop.clone();
        let server_thread = thread::spawn(move || {
            pin_to_core(SERVER_CORE);
            while !stop_server.load(Ordering::Relaxed) {
                server.process_batch(|_cid, req| req + 1);
            }
        });

        thread::sleep(std::time::Duration::from_millis(10));

        let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
        for client_idx in 0..NUM_CLIENTS {
            let name_clone = name.clone();
            let iters_clone = iters_atomic.clone();
            let stop_clone = stop.clone();
            let start_b = start_barrier.clone();
            let end_b = end_barrier.clone();
            let core_id = CLIENT_CORE_BASE + client_idx;

            client_handles.push(thread::spawn(move || {
                pin_to_core(core_id);
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

                // Warmup
                for _ in 0..100 {
                    client.call(0).unwrap();
                }

                loop {
                    start_b.wait();
                    if stop_clone.load(Ordering::Relaxed) {
                        return;
                    }
                    let iters = iters_clone.load(Ordering::Relaxed);

                    if depth <= 1 {
                        for _ in 0..iters {
                            client.call(42u64).unwrap();
                        }
                    } else {
                        let d = depth as u64;
                        let fill = d.min(iters);
                        // Fill pipeline
                        for _ in 0..fill {
                            client.send(42u64).unwrap();
                        }
                        // Steady state
                        for _ in fill..iters {
                            client.recv().unwrap();
                            client.send(42u64).unwrap();
                        }
                        // Drain
                        for _ in 0..fill {
                            client.recv().unwrap();
                        }
                    }

                    end_b.wait();
                }
            }));
        }

        b.iter_custom(|iters| {
            iters_atomic.store(iters, Ordering::Relaxed);
            start_barrier.wait();
            let start = Instant::now();
            end_barrier.wait();
            start.elapsed()
        });

        stop.store(true, Ordering::Relaxed);
        start_barrier.wait();
        for h in client_handles {
            h.join().unwrap();
        }
        server_thread.join().unwrap();
    }
}

fn bench_multi_client(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_rpc_multi_client");
    group.throughput(Throughput::Elements(NUM_CLIENTS as u64));

    group.bench_function("depth_1", |b| run_bench(b, 1));
    group.bench_function("depth_2", |b| run_bench(b, 2));
    group.bench_function("depth_4", |b| run_bench(b, 4));

    group.finish();
}

criterion_group!(benches, bench_multi_client);
criterion_main!(benches);
