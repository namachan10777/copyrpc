//! Benchmark for shm_spsc RPC call latency.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use shm_spsc::{Client, Server};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use uuid::Uuid;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

fn bench_call_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_rpc_call");
    group.throughput(Throughput::Elements(1));

    // u64 request/response
    group.bench_function("u64", |b| {
        let name = format!("/shm_rpc_bench_{}", Uuid::now_v7());

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, 4, 1).unwrap();

            let name_clone = name.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();

            let server_thread = thread::spawn(move || {
                pin_to_core(30);
                while !stop_clone.load(Ordering::Relaxed) {
                    server.process_batch(|_cid, req| req + 1);
                }
            });

            // Wait for server to be ready
            thread::sleep(std::time::Duration::from_millis(10));

            let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

            // Warmup
            for _ in 0..1000 {
                client.call(0).unwrap();
            }

            b.iter(|| {
                pin_to_core(31);
                black_box(client.call(black_box(42u64)).unwrap());
            });

            stop.store(true, Ordering::Relaxed);
            server_thread.join().unwrap();
        }
    });

    // [u8; 64] request/response
    group.bench_function("64_bytes", |b| {
        let name = format!("/shm_rpc_bench64_{}", Uuid::now_v7());

        unsafe {
            let mut server =
                Server::<[u8; 64], [u8; 64]>::create(&name, 4, 1).unwrap();

            let name_clone = name.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();

            let server_thread = thread::spawn(move || {
                pin_to_core(30);
                while !stop_clone.load(Ordering::Relaxed) {
                    server.process_batch(|_cid, req| req);
                }
            });

            thread::sleep(std::time::Duration::from_millis(10));

            let mut client =
                Client::<[u8; 64], [u8; 64]>::connect(&name_clone).unwrap();

            // Warmup
            for _ in 0..1000 {
                client.call([0u8; 64]).unwrap();
            }

            let data = [0xABu8; 64];
            b.iter(|| {
                pin_to_core(31);
                black_box(client.call(black_box(data)).unwrap());
            });

            stop.store(true, Ordering::Relaxed);
            server_thread.join().unwrap();
        }
    });

    // [u8; 256] request/response
    group.bench_function("256_bytes", |b| {
        let name = format!("/shm_rpc_bench256_{}", Uuid::now_v7());

        unsafe {
            let mut server =
                Server::<[u8; 256], [u8; 256]>::create(&name, 4, 1).unwrap();

            let name_clone = name.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();

            let server_thread = thread::spawn(move || {
                pin_to_core(30);
                while !stop_clone.load(Ordering::Relaxed) {
                    server.process_batch(|_cid, req| req);
                }
            });

            thread::sleep(std::time::Duration::from_millis(10));

            let mut client =
                Client::<[u8; 256], [u8; 256]>::connect(&name_clone).unwrap();

            // Warmup
            for _ in 0..1000 {
                client.call([0u8; 256]).unwrap();
            }

            let data = [0xABu8; 256];
            b.iter(|| {
                pin_to_core(31);
                black_box(client.call(black_box(data)).unwrap());
            });

            stop.store(true, Ordering::Relaxed);
            server_thread.join().unwrap();
        }
    });

    group.finish();
}

criterion_group!(benches, bench_call_latency);
criterion_main!(benches);
