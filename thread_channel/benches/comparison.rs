//! Benchmark comparing Flux (SPSC-based) vs Mesh (MPSC-based) n-to-n communication.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use thread_channel::{create_flux, create_mesh, Flux, Mesh, ReceivedMessage, RecvError, SendError};

/// Benchmark ping-pong latency between two nodes using call/reply.
fn bench_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("pingpong");
    group.throughput(Throughput::Elements(1));

    // Flux - using new callback-based API
    group.bench_function("flux", |b| {
        // Response counter shared between callback and main thread
        let response_received = Arc::new(AtomicBool::new(false));

        let nodes: Vec<Flux<u64, Arc<AtomicBool>, _>> =
            create_flux(2, 64, move |flag: &mut Arc<AtomicBool>, _data: u64| {
                flag.store(true, Ordering::Release);
            });
        let mut nodes: Vec<_> = nodes.into_iter().collect();
        let mut node1 = nodes.pop().unwrap();
        let mut node0 = nodes.pop().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();

        let handle = thread::spawn(move || {
            while !stop2.load(Ordering::Relaxed) {
                node1.poll();
                while let Some(handle) = node1.try_recv() {
                    let data = handle.data();
                    handle.reply(data).ok();
                }
            }
            node1
        });

        b.iter(|| {
            response_received.store(false, Ordering::Release);
            node0.call(1, black_box(42), response_received.clone()).unwrap();
            loop {
                node0.poll();
                // Drain any queued requests (shouldn't be any for node0)
                while node0.try_recv().is_some() {}
                if response_received.load(Ordering::Acquire) {
                    break;
                }
                std::hint::spin_loop();
            }
        });

        stop.store(true, Ordering::Relaxed);
        // Send one more message to wake up the responder thread
        node0.call(1, 0, Arc::new(AtomicBool::new(false))).ok();
        node0.flush();
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
                match node1.try_recv() {
                    Ok((from, ReceivedMessage::Request { req_num, data })) => {
                        node1.reply(from, req_num, data).unwrap();
                    }
                    Ok(_) => {}
                    Err(RecvError::Empty) => {}
                    Err(e) => panic!("recv error: {:?}", e),
                }
            }
            node1
        });

        b.iter(|| {
            let req_num = node0.call(1, black_box(42)).unwrap();
            loop {
                match node0.try_recv() {
                    Ok((_, ReceivedMessage::Response { req_num: r, data })) if r == req_num => {
                        black_box(data);
                        break;
                    }
                    Ok(_) => {}
                    Err(RecvError::Empty) => std::hint::spin_loop(),
                    Err(e) => panic!("recv error: {:?}", e),
                }
            }
        });

        stop.store(true, Ordering::Relaxed);
        node0.notify(1, 0).ok();
        handle.join().unwrap();
    });

    group.finish();
}

/// Benchmark fan-out: 1 node -> N nodes (using call with dummy callback).
fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout");

    for n in [2, 4, 8] {
        group.throughput(Throughput::Elements((n - 1) as u64));

        group.bench_with_input(BenchmarkId::new("flux", n), &n, |b, &n| {
            let nodes: Vec<Flux<u64, (), _>> = create_flux(n, 1024, |_, _| {});
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
                            node.poll();
                            while node.try_recv().is_some() {
                                count += 1;
                            }
                        }
                        // Drain remaining
                        node.poll();
                        while node.try_recv().is_some() {
                            count += 1;
                        }
                        count
                    })
                })
                .collect();

            b.iter(|| {
                for peer in 1..n {
                    // Use call with empty user_data as a notification
                    sender.call(peer, black_box(42), ()).unwrap();
                }
                sender.flush();
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
                    sender.notify(peer, black_box(42)).unwrap();
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
            let nodes: Vec<Flux<u64, (), _>> = create_flux(n, 1024, |_, _| {});
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
                            match node.call(0, count, ()) {
                                Ok(()) => {
                                    node.flush();
                                    count += 1;
                                }
                                Err(SendError::Full(_)) => {
                                    node.flush();
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
                    loop {
                        receiver.poll();
                        if receiver.try_recv().is_some() {
                            break;
                        }
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
                            if node.notify(0, count).is_ok() {
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

/// Benchmark SPSC batch send/receive using write/flush/poll/sync.
fn bench_spsc_batch(c: &mut Criterion) {
    use thread_channel::spsc;

    let mut group = c.benchmark_group("spsc_batch");

    for batch_size in [1, 8, 32, 128, 256] {
        group.throughput(Throughput::Elements(batch_size as u64));

        // Batch write/flush (main API)
        group.bench_with_input(
            BenchmarkId::new("write_flush", batch_size),
            &batch_size,
            |b, &batch_size| {
                let (mut tx, mut rx) = spsc::channel::<u64>(1024);

                let stop = Arc::new(AtomicBool::new(false));
                let stop2 = stop.clone();

                let handle = thread::spawn(move || {
                    let mut count = 0u64;
                    while !stop2.load(Ordering::Relaxed) {
                        rx.sync();
                        while let Some(_) = rx.poll() {
                            count += 1;
                        }
                    }
                    // Drain remaining
                    rx.sync();
                    while let Some(_) = rx.poll() {
                        count += 1;
                    }
                    count
                });

                b.iter(|| {
                    for i in 0..batch_size {
                        loop {
                            match tx.write(black_box(i as u64)) {
                                Ok(()) => break,
                                Err(_) => {
                                    tx.flush();
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    }
                    tx.flush();
                });

                stop.store(true, Ordering::Relaxed);
                handle.join().unwrap();
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_pingpong,
    bench_fanout,
    bench_fanin,
    bench_spsc_batch,
);
criterion_main!(benches);
