//! Benchmark for shm_spsc ping-pong latency.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use shm_spsc::{Client, Server};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use uuid::Uuid;

fn bench_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_spsc_pingpong");
    group.throughput(Throughput::Elements(1));

    for capacity in [64, 256, 1024, 4096] {
        group.bench_with_input(
            BenchmarkId::new("capacity", capacity),
            &capacity,
            |b, &capacity| {
                let name = format!("/shm_bench_{}", Uuid::now_v7());

                unsafe {
                    let mut server = Server::<u64>::create(&name, capacity).unwrap();

                    let name_clone = name.clone();
                    let stop = Arc::new(AtomicBool::new(false));
                    let stop_clone = stop.clone();

                    let client_thread = thread::spawn(move || {
                        let mut client =
                            Client::<u64>::connect(&name_clone, capacity).unwrap();

                        while !stop_clone.load(Ordering::Relaxed) {
                            if let Ok(val) = client.try_recv() {
                                client.send(val).unwrap();
                            }
                        }
                    });

                    // Wait for connection
                    while !server.is_connected() {
                        std::hint::spin_loop();
                    }

                    // Warmup
                    for _ in 0..1000 {
                        server.send(0).unwrap();
                        server.recv().unwrap();
                    }

                    b.iter(|| {
                        server.send(black_box(42u64)).unwrap();
                        black_box(server.recv().unwrap());
                    });

                    stop.store(true, Ordering::Relaxed);
                    // Send one more to wake up the client
                    server.send(0).ok();
                    client_thread.join().unwrap();
                }
            },
        );
    }

    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_spsc_throughput");

    for capacity in [256, 1024, 4096] {
        group.throughput(Throughput::Elements(1000));

        group.bench_with_input(
            BenchmarkId::new("capacity", capacity),
            &capacity,
            |b, &capacity| {
                let name = format!("/shm_tput_{}", Uuid::now_v7());
                let batch_size = 1000u64;

                unsafe {
                    let mut server = Server::<u64>::create(&name, capacity).unwrap();

                    let name_clone = name.clone();
                    let stop = Arc::new(AtomicBool::new(false));
                    let stop_clone = stop.clone();

                    let client_thread = thread::spawn(move || {
                        let mut client =
                            Client::<u64>::connect(&name_clone, capacity).unwrap();
                        let mut count = 0u64;

                        while !stop_clone.load(Ordering::Relaxed) {
                            if client.try_recv().is_ok() {
                                count += 1;
                            }
                        }

                        // Drain remaining
                        while client.try_recv().is_ok() {
                            count += 1;
                        }

                        count
                    });

                    // Wait for connection
                    while !server.is_connected() {
                        std::hint::spin_loop();
                    }

                    // Warmup
                    for i in 0..1000 {
                        server.send(i).unwrap();
                    }
                    thread::sleep(std::time::Duration::from_millis(10));

                    b.iter(|| {
                        for i in 0..batch_size {
                            server.send(black_box(i)).unwrap();
                        }
                    });

                    stop.store(true, Ordering::Relaxed);
                    client_thread.join().unwrap();
                }
            },
        );
    }

    group.finish();
}

fn bench_message_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("shm_spsc_sizes");
    group.throughput(Throughput::Elements(1));

    // 8 bytes
    group.bench_function("8_bytes", |b| {
        let name = format!("/shm_size8_{}", Uuid::now_v7());
        let capacity = 1024;

        unsafe {
            let mut server = Server::<u64>::create(&name, capacity).unwrap();

            let name_clone = name.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();

            let client_thread = thread::spawn(move || {
                let mut client = Client::<u64>::connect(&name_clone, capacity).unwrap();
                while !stop_clone.load(Ordering::Relaxed) {
                    if let Ok(val) = client.try_recv() {
                        client.send(val).unwrap();
                    }
                }
            });

            while !server.is_connected() {
                std::hint::spin_loop();
            }

            b.iter(|| {
                server.send(black_box(42u64)).unwrap();
                black_box(server.recv().unwrap());
            });

            stop.store(true, Ordering::Relaxed);
            server.send(0).ok();
            client_thread.join().unwrap();
        }
    });

    // 64 bytes
    group.bench_function("64_bytes", |b| {
        let name = format!("/shm_size64_{}", Uuid::now_v7());
        let capacity = 1024;

        unsafe {
            let mut server = Server::<[u8; 64]>::create(&name, capacity).unwrap();

            let name_clone = name.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();

            let client_thread = thread::spawn(move || {
                let mut client = Client::<[u8; 64]>::connect(&name_clone, capacity).unwrap();
                while !stop_clone.load(Ordering::Relaxed) {
                    if let Ok(val) = client.try_recv() {
                        client.send(val).unwrap();
                    }
                }
            });

            while !server.is_connected() {
                std::hint::spin_loop();
            }

            let data = [0u8; 64];
            b.iter(|| {
                server.send(black_box(data)).unwrap();
                black_box(server.recv().unwrap());
            });

            stop.store(true, Ordering::Relaxed);
            server.send([0u8; 64]).ok();
            client_thread.join().unwrap();
        }
    });

    // 256 bytes
    group.bench_function("256_bytes", |b| {
        let name = format!("/shm_size256_{}", Uuid::now_v7());
        let capacity = 1024;

        unsafe {
            let mut server = Server::<[u8; 256]>::create(&name, capacity).unwrap();

            let name_clone = name.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stop_clone = stop.clone();

            let client_thread = thread::spawn(move || {
                let mut client = Client::<[u8; 256]>::connect(&name_clone, capacity).unwrap();
                while !stop_clone.load(Ordering::Relaxed) {
                    if let Ok(val) = client.try_recv() {
                        client.send(val).unwrap();
                    }
                }
            });

            while !server.is_connected() {
                std::hint::spin_loop();
            }

            let data = [0u8; 256];
            b.iter(|| {
                server.send(black_box(data)).unwrap();
                black_box(server.recv().unwrap());
            });

            stop.store(true, Ordering::Relaxed);
            server.send([0u8; 256]).ok();
            client_thread.join().unwrap();
        }
    });

    group.finish();
}

criterion_group!(benches, bench_pingpong, bench_throughput, bench_message_sizes);
criterion_main!(benches);
