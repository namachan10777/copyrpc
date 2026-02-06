//! Benchmark for shm_spsc multi-client throughput.
//!
//! 16 client threads issue RPC calls concurrently to 1 server thread.
//! Threads are pre-spawned; iteration count is distributed via AtomicU64
//! and synchronized with two Barriers per batch.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use shm_spsc::{Client, Server};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use uuid::Uuid;

const NUM_CLIENTS: usize = 16;

fn bench_multi_client(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_rpc_multi_client");
    group.throughput(Throughput::Elements(NUM_CLIENTS as u64));

    group.bench_function("u64_16clients", |b| {
        let name = format!("/shm_rpc_mc_{}", Uuid::now_v7());

        let iters_atomic = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let start_barrier = Arc::new(Barrier::new(NUM_CLIENTS + 1));
        let end_barrier = Arc::new(Barrier::new(NUM_CLIENTS + 1));

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, NUM_CLIENTS as u32).unwrap();

            let stop_server = stop.clone();
            let server_thread = thread::spawn(move || {
                while !stop_server.load(Ordering::Relaxed) {
                    if let Some((cid, req)) = server.try_poll() {
                        server.respond(cid, req + 1).unwrap();
                    }
                }
            });

            thread::sleep(std::time::Duration::from_millis(10));

            let mut client_handles = Vec::with_capacity(NUM_CLIENTS);
            for _ in 0..NUM_CLIENTS {
                let name_clone = name.clone();
                let iters_clone = iters_atomic.clone();
                let stop_clone = stop.clone();
                let start_b = start_barrier.clone();
                let end_b = end_barrier.clone();

                client_handles.push(thread::spawn(move || {
                    let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

                    for _ in 0..100 {
                        client.call(0).unwrap();
                    }

                    loop {
                        start_b.wait();
                        if stop_clone.load(Ordering::Relaxed) {
                            return;
                        }
                        let iters = iters_clone.load(Ordering::Relaxed);
                        for _ in 0..iters {
                            client.call(42u64).unwrap();
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
    });

    group.finish();
}

criterion_group!(benches, bench_multi_client);
criterion_main!(benches);
