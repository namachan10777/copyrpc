//! Benchmark for shm_spsc RPC call latency.
//!
//! Compares two implementations:
//! - slot: original toggle-bit slot protocol
//! - ffwd: per-client SPSC delegation (flat combining)

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use uuid::Uuid;

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

const RING_DEPTH: u32 = 8;

fn bench_call_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_rpc_call");
    group.throughput(Throughput::Elements(1));

    // --- Slot (original) ---
    group.bench_function("slot_u64", |b| {
        let name = format!("/shm_bench_slot_{}", Uuid::now_v7());
        unsafe {
            let mut server = shm_spsc::Server::<u64, u64>::create(&name, 4, 1).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();
            let server_thread = thread::spawn(move || {
                pin_to_core(30);
                while !stop_clone.load(Ordering::Relaxed) {
                    server.poll();
                    while let Some((token, req)) = server.recv() {
                        server.reply(token, req + 1);
                    }
                }
            });
            thread::sleep(std::time::Duration::from_millis(10));
            let mut client = shm_spsc::SyncClient::<u64, u64>::connect_sync(&name).unwrap();
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

    // --- ffwd delegation ---
    group.bench_function("ffwd_u64", |b| {
        let name = format!("/shm_bench_ffwd_{}", Uuid::now_v7());
        unsafe {
            let mut server =
                shm_spsc::mpsc_ffwd::Server::<u64, u64>::create(&name, 4, RING_DEPTH).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();
            let server_thread = thread::spawn(move || {
                pin_to_core(30);
                while !stop_clone.load(Ordering::Relaxed) {
                    server.poll();
                    while let Some((token, req)) = server.recv() {
                        server.reply(token, req + 1);
                    }
                }
            });
            thread::sleep(std::time::Duration::from_millis(10));
            let mut client =
                shm_spsc::mpsc_ffwd::SyncClient::<u64, u64>::connect_sync(&name).unwrap();
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
