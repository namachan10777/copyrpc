//! Benchmark for ipc multi-client throughput.
//!
//! 16 client threads issue RPC calls concurrently to 1 server thread.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use uuid::Uuid;

const NUM_CLIENTS: usize = 16;
const SERVER_CORE: usize = 31;
const RING_DEPTH: u32 = 64;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

fn run_bench(b: &mut criterion::Bencher, depth: u32) {
    let name = format!("/shm_mc_{}", Uuid::now_v7());

    let iters_atomic = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start_barrier = Arc::new(Barrier::new(NUM_CLIENTS + 1));
    let end_barrier = Arc::new(Barrier::new(NUM_CLIENTS + 1));

    unsafe {
        let server =
            ipc::Server::<u64, u64>::create(&name, NUM_CLIENTS as u32, RING_DEPTH, 0).unwrap();

        let stop_server = stop.clone();
        let server_thread = thread::spawn(move || {
            pin_to_core(SERVER_CORE);
            while !stop_server.load(Ordering::Relaxed) {
                server.poll();
                while let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req + 1);
                }
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
            let core_id = 31 - 1 - client_idx;

            client_handles.push(thread::spawn(move || {
                pin_to_core(core_id);

                if depth <= 1 {
                    let mut client =
                        ipc::SyncClient::<u64, u64>::connect_sync(&name_clone).unwrap();
                    for _ in 0..100 {
                        client.call_blocking(0).unwrap();
                    }
                    loop {
                        start_b.wait();
                        if stop_clone.load(Ordering::Relaxed) {
                            return;
                        }
                        let iters = iters_clone.load(Ordering::Relaxed);
                        for _ in 0..iters {
                            client.call_blocking(42u64).unwrap();
                        }
                        end_b.wait();
                    }
                } else {
                    let count = std::cell::Cell::new(0u64);
                    let mut client =
                        ipc::Client::<u64, u64, (), _>::connect(&name_clone, |(), _resp| {
                            count.set(count.get() + 1);
                        })
                        .unwrap();
                    for _ in 0..100 {
                        client.call(42u64, ()).unwrap();
                    }
                    while client.pending_count() > 0 {
                        client.poll().unwrap();
                        std::hint::spin_loop();
                    }
                    loop {
                        start_b.wait();
                        if stop_clone.load(Ordering::Relaxed) {
                            return;
                        }
                        let iters = iters_clone.load(Ordering::Relaxed);
                        count.set(0);
                        let fill = (depth as u64).min(iters);
                        for _ in 0..fill {
                            client.call(42u64, ()).unwrap();
                        }
                        let mut sent = fill;
                        while count.get() < iters {
                            let n = client.poll().unwrap();
                            let to_send = (n as u64).min(iters - sent);
                            for _ in 0..to_send {
                                client.call(42u64, ()).unwrap();
                            }
                            sent += to_send;
                            if n == 0 {
                                std::hint::spin_loop();
                            }
                        }
                        end_b.wait();
                    }
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
    group.bench_function("depth_4", |b| run_bench(b, 4));

    group.finish();
}

criterion_group!(benches, bench_multi_client);
criterion_main!(benches);
