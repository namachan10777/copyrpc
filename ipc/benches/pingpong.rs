//! Benchmark for ipc RPC call latency.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use uuid::Uuid;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

const RING_DEPTH: u32 = 8;

fn bench_call_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_rpc_call");
    group.throughput(Throughput::Elements(1));

    group.bench_function("u64", |b| {
        let name = format!("/shm_bench_{}", Uuid::now_v7());
        unsafe {
            let server = ipc::Server::<u64, u64>::create(&name, 4, RING_DEPTH, 0).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();
            let server_thread = thread::spawn(move || {
                pin_to_core(30);
                while !stop_clone.load(Ordering::Relaxed) {
                    server.poll();
                    while let Some(handle) = server.recv() {
                        let req = *handle.data();
                        handle.reply(req + 1);
                    }
                }
            });
            thread::sleep(std::time::Duration::from_millis(10));
            let mut client = ipc::SyncClient::<u64, u64>::connect_sync(&name).unwrap();
            for _ in 0..1000 {
                client.call_blocking(0).unwrap();
            }
            b.iter(|| {
                pin_to_core(31);
                black_box(client.call_blocking(black_box(42u64)).unwrap());
            });
            stop.store(true, Ordering::Relaxed);
            server_thread.join().unwrap();
        }
    });

    group.finish();
}

criterion_group!(benches, bench_call_latency);
criterion_main!(benches);
