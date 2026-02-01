//! Benchmark comparing Flux (SPSC-based) vs Mesh (MPSC-based) n-to-n communication.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use thread_channel::{create_flux, create_mesh, Flux, Mesh, RecvError, SendError};

/// Benchmark ping-pong latency between two nodes.
fn bench_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("pingpong");
    group.throughput(Throughput::Elements(1));

    // Flux
    group.bench_function("flux", |b| {
        let nodes: Vec<Flux<u64>> = create_flux(2, 64);
        let mut nodes: Vec<_> = nodes.into_iter().collect();
        let mut node1 = nodes.pop().unwrap();
        let mut node0 = nodes.pop().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();

        let handle = thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                if let Ok((_, v)) = node1.try_recv() {
                    node1.send(0, v).unwrap();
                }
            }
            node1
        });

        b.iter(|| {
            node0.send(1, black_box(42)).unwrap();
            loop {
                match node0.try_recv() {
                    Ok((_, v)) => {
                        black_box(v);
                        break;
                    }
                    Err(RecvError::Empty) => std::hint::spin_loop(),
                    Err(e) => panic!("recv error: {:?}", e),
                }
            }
        });

        stop.store(true, Ordering::Relaxed);
        node0.send(1, 0).ok();
        handle.join().unwrap();
    });

    // Mesh
    group.bench_function("mesh", |b| {
        let nodes: Vec<Mesh<u64>> = create_mesh(2);
        let mut nodes: Vec<_> = nodes.into_iter().collect();
        let mut node1 = nodes.pop().unwrap();
        let mut node0 = nodes.pop().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();

        let handle = thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                if let Ok((_, v)) = node1.try_recv() {
                    node1.send(0, v).unwrap();
                }
            }
            node1
        });

        b.iter(|| {
            node0.send(1, black_box(42)).unwrap();
            black_box(node0.recv().unwrap());
        });

        stop.store(true, Ordering::Relaxed);
        node0.send(1, 0).ok();
        handle.join().unwrap();
    });

    group.finish();
}

/// Benchmark fan-out: 1 node -> N nodes.
fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout");

    for n in [2, 4, 8] {
        group.throughput(Throughput::Elements((n - 1) as u64));

        group.bench_with_input(BenchmarkId::new("flux", n), &n, |b, &n| {
            let nodes: Vec<Flux<u64>> = create_flux(n, 1024);
            let mut nodes: Vec<_> = nodes.into_iter().collect();
            let mut sender = nodes.remove(0);

            let stop = Arc::new(AtomicBool::new(false));
            let handles: Vec<_> = nodes
                .into_iter()
                .map(|mut node| {
                    let stop = stop.clone();
                    thread::spawn(move || {
                        let mut count = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            if node.try_recv().is_ok() {
                                count += 1;
                            }
                        }
                        // Drain remaining
                        while node.try_recv().is_ok() {
                            count += 1;
                        }
                        count
                    })
                })
                .collect();

            b.iter(|| {
                for peer in 1..n {
                    sender.send(peer, black_box(42)).unwrap();
                }
            });

            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }
        });

        group.bench_with_input(BenchmarkId::new("mesh", n), &n, |b, &n| {
            let nodes: Vec<Mesh<u64>> = create_mesh(n);
            let mut nodes: Vec<_> = nodes.into_iter().collect();
            let sender = nodes.remove(0);

            let stop = Arc::new(AtomicBool::new(false));
            let handles: Vec<_> = nodes
                .into_iter()
                .map(|mut node| {
                    let stop = stop.clone();
                    thread::spawn(move || {
                        let mut count = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            if node.try_recv().is_ok() {
                                count += 1;
                            }
                        }
                        // Drain remaining
                        while node.try_recv().is_ok() {
                            count += 1;
                        }
                        count
                    })
                })
                .collect();

            b.iter(|| {
                for peer in 1..n {
                    sender.send(peer, black_box(42)).unwrap();
                }
            });

            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }
        });
    }

    group.finish();
}

/// Benchmark fan-in: N nodes -> 1 node.
fn bench_fanin(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanin");

    for n in [2, 4, 8] {
        group.throughput(Throughput::Elements((n - 1) as u64));

        group.bench_with_input(BenchmarkId::new("flux", n), &n, |b, &n| {
            let nodes: Vec<Flux<u64>> = create_flux(n, 1024);
            let mut nodes: Vec<_> = nodes.into_iter().collect();
            let mut receiver = nodes.remove(0);

            let stop = Arc::new(AtomicBool::new(false));
            let handles: Vec<_> = nodes
                .into_iter()
                .map(|mut node| {
                    let stop = stop.clone();
                    thread::spawn(move || {
                        let mut count = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            match node.try_send(0, count) {
                                Ok(()) => count += 1,
                                Err(SendError::Full(_)) => {
                                    std::hint::spin_loop();
                                }
                                Err(_) => break,
                            }
                        }
                        count
                    })
                })
                .collect();

            b.iter(|| {
                for _ in 1..n {
                    while receiver.try_recv().is_err() {
                        std::hint::spin_loop();
                    }
                }
            });

            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }
        });

        group.bench_with_input(BenchmarkId::new("mesh", n), &n, |b, &n| {
            let nodes: Vec<Mesh<u64>> = create_mesh(n);
            let mut nodes: Vec<_> = nodes.into_iter().collect();
            let mut receiver = nodes.remove(0);

            let stop = Arc::new(AtomicBool::new(false));
            let handles: Vec<_> = nodes
                .into_iter()
                .map(|node| {
                    let stop = stop.clone();
                    thread::spawn(move || {
                        let mut count = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            if node.send(0, count).is_ok() {
                                count += 1;
                            }
                        }
                        count
                    })
                })
                .collect();

            b.iter(|| {
                for _ in 1..n {
                    while receiver.try_recv().is_err() {
                        std::hint::spin_loop();
                    }
                }
            });

            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pingpong, bench_fanout, bench_fanin,);
criterion_main!(benches);
