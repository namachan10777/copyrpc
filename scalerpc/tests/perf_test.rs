//! Simple performance test for ScaleRPC.
//!
//! Run with:
//! ```bash
//! cargo test --package scalerpc --test perf_test --release -- --nocapture
//! ```

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use common::TestContext;
use scalerpc::connection::RemoteEndpoint;
use scalerpc::{ClientConfig, GroupConfig, PoolConfig, RpcClient, RpcServer, ServerConfig};

#[derive(Clone)]
struct ConnectionInfo {
    qpn: u32,
    lid: u16,
    slot_addr: u64,
    slot_rkey: u32,
}

/// Latency test: measure RTT for individual RPC calls.
#[test]
fn test_latency() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: 64,
            slot_data_size: 4080,
        },
        timeout_ms: 5000,
    };

    let mut client = RpcClient::new(&ctx.pd, client_config).expect("create client");
    let conn_id = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("add connection");

    let client_endpoint = client.local_endpoint(conn_id).expect("client endpoint");

    // Setup server in thread
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => return,
        };

        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: 64,
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: GroupConfig::default(),
            enable_scheduler: false, // Disable for simple latency test
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload| (0, payload.to_vec()));

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();

        server_info_tx
            .send(ConnectionInfo {
                qpn: server_endpoint.qpn,
                lid: server_endpoint.lid,
                slot_addr: server_endpoint.slot_addr,
                slot_rkey: server_endpoint.slot_rkey,
            })
            .unwrap();

        let client_info = client_info_rx.recv().unwrap();

        let remote = RemoteEndpoint {
            qpn: client_info.qpn,
            psn: 0,
            lid: client_info.lid,
            slot_addr: client_info.slot_addr,
            slot_rkey: client_info.slot_rkey,
            ..Default::default()
        };

        server.connect(conn_id, remote).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.process();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx
        .send(ConnectionInfo {
            qpn: client_endpoint.qpn,
            lid: client_endpoint.lid,
            slot_addr: client_endpoint.slot_addr,
            slot_rkey: client_endpoint.slot_rkey,
        })
        .unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let server_endpoint = RemoteEndpoint {
        qpn: server_info.qpn,
        psn: 0,
        lid: server_info.lid,
        slot_addr: server_info.slot_addr,
        slot_rkey: server_info.slot_rkey,
        ..Default::default()
    };

    client.connect(conn_id, server_endpoint).expect("connect");

    // Warmup (using blocking call)
    let payload = vec![0xAAu8; 32];
    for _ in 0..10 {
        let _ = client.call(conn_id, 1, &payload);
    }

    // Measure latency (using blocking call)
    let iterations = 1000;
    let start = Instant::now();

    for _ in 0..iterations {
        let _response = client.call(conn_id, 1, &payload).expect("call");
    }

    let elapsed = start.elapsed();
    let avg_latency_us = elapsed.as_micros() as f64 / iterations as f64;

    println!("\n=== ScaleRPC Latency Test ===");
    println!("Iterations: {}", iterations);
    println!("Total time: {:.2?}", elapsed);
    println!("Average RTT: {:.2} µs", avg_latency_us);
    println!("Throughput: {:.2} MRPS", 1.0 / (avg_latency_us / 1_000_000.0) / 1_000_000.0);

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
}

/// Throughput test: measure sustained RPC throughput with pipelining.
#[test]
fn test_throughput() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: 256,
            slot_data_size: 4080,
        },
        timeout_ms: 5000,
    };

    let mut client = RpcClient::new(&ctx.pd, client_config).expect("create client");
    let conn_id = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("add connection");

    let client_endpoint = client.local_endpoint(conn_id).expect("client endpoint");

    // Setup server
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => return,
        };

        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: 256,
                slot_data_size: 4080,
            },
            num_recv_slots: 256,
            group: GroupConfig::default(),
            enable_scheduler: false, // Disable for throughput test
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload| (0, payload.to_vec()));

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();

        server_info_tx
            .send(ConnectionInfo {
                qpn: server_endpoint.qpn,
                lid: server_endpoint.lid,
                slot_addr: server_endpoint.slot_addr,
                slot_rkey: server_endpoint.slot_rkey,
            })
            .unwrap();

        let client_info = client_info_rx.recv().unwrap();

        let remote = RemoteEndpoint {
            qpn: client_info.qpn,
            psn: 0,
            lid: client_info.lid,
            slot_addr: client_info.slot_addr,
            slot_rkey: client_info.slot_rkey,
            ..Default::default()
        };

        server.connect(conn_id, remote).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.process();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx
        .send(ConnectionInfo {
            qpn: client_endpoint.qpn,
            lid: client_endpoint.lid,
            slot_addr: client_endpoint.slot_addr,
            slot_rkey: client_endpoint.slot_rkey,
        })
        .unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let server_endpoint = RemoteEndpoint {
        qpn: server_info.qpn,
        psn: 0,
        lid: server_info.lid,
        slot_addr: server_info.slot_addr,
        slot_rkey: server_info.slot_rkey,
        ..Default::default()
    };

    client.connect(conn_id, server_endpoint).expect("connect");

    let pipeline_depth = 16;
    let total_requests = 50;
    let payload = vec![0xAAu8; 32];

    println!("\n=== ScaleRPC Throughput Test ===");
    println!("Pipeline depth: {}", pipeline_depth);
    println!("Total requests: {}", total_requests);

    let mut pending_requests: Vec<scalerpc::PendingRpc<'_>> =
        Vec::with_capacity(pipeline_depth);
    let mut completed = 0usize;
    let mut sent = 0usize;

    let start = Instant::now();

    // Initial fill (using call_async for pipelining)
    while sent < pipeline_depth && sent < total_requests {
        if let Ok(p) = client.call_async(conn_id, 1, &payload) {
            eprintln!("[fill] sent={}, slot={}, req_id={}", sent, p.slot_index(), p.req_id());
            pending_requests.push(p);
            sent += 1;
        } else {
            eprintln!("[fill] call_async failed at sent={}", sent);
            break;
        }
    }
    eprintln!("[fill] done, sent={}", sent);

    // Main loop
    let mut iter = 0u64;
    while completed < total_requests {
        // Check for completed requests
        let mut i = 0;
        while i < pending_requests.len() {
            if pending_requests[i].poll().is_some() {
                if completed < 20 || completed % 100 == 0 {
                    eprintln!("[main] completed={}, req_id={}", completed, pending_requests[i].req_id());
                }
                pending_requests.swap_remove(i);
                completed += 1;

                // Send new request
                if sent < total_requests {
                    if let Ok(p) = client.call_async(conn_id, 1, &payload) {
                        if sent < 20 || sent % 100 == 0 {
                            eprintln!("[main] sent={}, slot={}", sent, p.slot_index());
                        }
                        pending_requests.push(p);
                        sent += 1;
                    }
                }
            } else {
                i += 1;
            }
        }
        iter += 1;
        if iter % 5_000_000 == 0 {
            let pending_info: Vec<_> = pending_requests.iter().map(|p| (p.slot_index(), p.req_id())).collect();
            eprintln!("[main] iter={}, completed={}, pending={:?}", iter, completed, pending_info);
        }

        std::hint::spin_loop();
    }

    let elapsed = start.elapsed();
    let throughput = completed as f64 / elapsed.as_secs_f64();

    println!("Total time: {:.2?}", elapsed);
    println!("Throughput: {:.2} KRPS", throughput / 1000.0);
    println!("Average latency: {:.2} µs", 1_000_000.0 / throughput);

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
}

/// Throughput test with 4KB payload to reproduce benchmark hang.
#[test]
fn test_throughput_4kb() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: 256,
            slot_data_size: 4080,
        },
        timeout_ms: 5000,
    };

    let mut client = RpcClient::new(&ctx.pd, client_config).expect("create client");
    let conn_id = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("add connection");

    let client_endpoint = client.local_endpoint(conn_id).expect("client endpoint");

    // Setup server
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => return,
        };

        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: 256,
                slot_data_size: 4080,
            },
            num_recv_slots: 256,
            group: GroupConfig::default(),
            enable_scheduler: false,
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload| (0, payload.to_vec()));

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();

        server_info_tx
            .send(ConnectionInfo {
                qpn: server_endpoint.qpn,
                lid: server_endpoint.lid,
                slot_addr: server_endpoint.slot_addr,
                slot_rkey: server_endpoint.slot_rkey,
            })
            .unwrap();

        let client_info = client_info_rx.recv().unwrap();

        let remote = RemoteEndpoint {
            qpn: client_info.qpn,
            psn: 0,
            lid: client_info.lid,
            slot_addr: client_info.slot_addr,
            slot_rkey: client_info.slot_rkey,
            ..Default::default()
        };

        server.connect(conn_id, remote).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.process();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx
        .send(ConnectionInfo {
            qpn: client_endpoint.qpn,
            lid: client_endpoint.lid,
            slot_addr: client_endpoint.slot_addr,
            slot_rkey: client_endpoint.slot_rkey,
        })
        .unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let server_endpoint = RemoteEndpoint {
        qpn: server_info.qpn,
        psn: 0,
        lid: server_info.lid,
        slot_addr: server_info.slot_addr,
        slot_rkey: server_info.slot_rkey,
        ..Default::default()
    };

    client.connect(conn_id, server_endpoint).expect("connect");

    let pipeline_depth = 16;
    let total_requests = 50;
    let payload = vec![0xAAu8; 4000]; // ~4KB payload (max is 4056)

    println!("\n=== ScaleRPC Throughput Test (4KB) ===");
    println!("Pipeline depth: {}", pipeline_depth);
    println!("Total requests: {}", total_requests);

    let mut pending_requests: Vec<scalerpc::PendingRpc<'_>> =
        Vec::with_capacity(pipeline_depth);
    let mut completed = 0usize;
    let mut sent = 0usize;

    let start = Instant::now();

    // Initial fill
    while sent < pipeline_depth && sent < total_requests {
        if let Ok(p) = client.call_async(conn_id, 1, &payload) {
            eprintln!("[fill] sent={}, slot={}, req_id={}", sent, p.slot_index(), p.req_id());
            pending_requests.push(p);
            sent += 1;
        } else {
            eprintln!("[fill] call_async failed at sent={}", sent);
            break;
        }
    }
    eprintln!("[fill] done, sent={}", sent);

    // Main loop with timeout
    let timeout = std::time::Duration::from_secs(10);
    let mut iter = 0u64;
    while completed < total_requests {
        if start.elapsed() > timeout {
            panic!("Test timed out after {:?}. completed={}, sent={}, pending={}",
                   timeout, completed, sent, pending_requests.len());
        }

        let mut i = 0;
        while i < pending_requests.len() {
            if pending_requests[i].poll().is_some() {
                if completed < 20 || completed % 100 == 0 {
                    eprintln!("[main] completed={}, req_id={}", completed, pending_requests[i].req_id());
                }
                pending_requests.swap_remove(i);
                completed += 1;

                if sent < total_requests {
                    if let Ok(p) = client.call_async(conn_id, 1, &payload) {
                        if sent < 20 || sent % 100 == 0 {
                            eprintln!("[main] sent={}, slot={}", sent, p.slot_index());
                        }
                        pending_requests.push(p);
                        sent += 1;
                    }
                }
            } else {
                i += 1;
            }
        }
        iter += 1;
        if iter % 5_000_000 == 0 {
            let pending_info: Vec<_> = pending_requests.iter().map(|p| (p.slot_index(), p.req_id())).collect();
            eprintln!("[main] iter={}, completed={}, pending={:?}", iter, completed, pending_info);
        }

        std::hint::spin_loop();
    }

    let elapsed = start.elapsed();
    let throughput = completed as f64 / elapsed.as_secs_f64();

    println!("Total time: {:.2?}", elapsed);
    println!("Throughput: {:.2} KRPS", throughput / 1000.0);

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
}
