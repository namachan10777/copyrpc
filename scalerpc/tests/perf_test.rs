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
    event_buffer_addr: u64,
    event_buffer_rkey: u32,
    warmup_buffer_addr: u64,
    warmup_buffer_rkey: u32,
    warmup_buffer_slots: u32,
    endpoint_entry_addr: u64,
    endpoint_entry_rkey: u32,
    server_conn_id: u32,
    pool_num_slots: u32,
}

impl From<RemoteEndpoint> for ConnectionInfo {
    fn from(e: RemoteEndpoint) -> Self {
        Self {
            qpn: e.qpn,
            lid: e.lid,
            slot_addr: e.slot_addr,
            slot_rkey: e.slot_rkey,
            event_buffer_addr: e.event_buffer_addr,
            event_buffer_rkey: e.event_buffer_rkey,
            warmup_buffer_addr: e.warmup_buffer_addr,
            warmup_buffer_rkey: e.warmup_buffer_rkey,
            warmup_buffer_slots: e.warmup_buffer_slots,
            endpoint_entry_addr: e.endpoint_entry_addr,
            endpoint_entry_rkey: e.endpoint_entry_rkey,
            server_conn_id: e.server_conn_id,
            pool_num_slots: e.pool_num_slots,
        }
    }
}

impl From<ConnectionInfo> for RemoteEndpoint {
    fn from(c: ConnectionInfo) -> Self {
        Self {
            qpn: c.qpn,
            psn: 0,
            lid: c.lid,
            slot_addr: c.slot_addr,
            slot_rkey: c.slot_rkey,
            event_buffer_addr: c.event_buffer_addr,
            event_buffer_rkey: c.event_buffer_rkey,
            warmup_buffer_addr: c.warmup_buffer_addr,
            warmup_buffer_rkey: c.warmup_buffer_rkey,
            warmup_buffer_slots: c.warmup_buffer_slots,
            endpoint_entry_addr: c.endpoint_entry_addr,
            endpoint_entry_rkey: c.endpoint_entry_rkey,
            server_conn_id: c.server_conn_id,
            pool_num_slots: c.pool_num_slots,
        }
    }
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
        max_connections: 4,
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
            group: GroupConfig {
                num_groups: 1,
                ..Default::default()
            },
            max_connections: 4,
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload, response_buf| {
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        });

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();

        server_info_tx.send(server_endpoint.into()).unwrap();

        let client_info = client_info_rx.recv().unwrap();
        server.connect(conn_id, client_info.into()).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx.send(client_endpoint.into()).unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_info.into()).expect("connect");

    // Warmup
    let payload = vec![0xAAu8; 32];
    for _ in 0..10 {
        let pending = client.call_async(conn_id, 1, &payload).expect("call_async");
        client.poll();
        loop {
            client.poll();
            if client.poll_pending(&pending).is_some() {
                break;
            }
            std::hint::spin_loop();
        }
    }

    // Measure latency
    let iterations = 1000;
    let start = Instant::now();

    for _ in 0..iterations {
        let pending = client.call_async(conn_id, 1, &payload).expect("call_async");
        client.poll();
        loop {
            client.poll();
            if client.poll_pending(&pending).is_some() {
                break;
            }
            std::hint::spin_loop();
        }
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
        max_connections: 8,
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
                num_slots: 64,  // Balanced for slot scanning
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: GroupConfig {
                num_groups: 1,
                ..Default::default()
            },
            max_connections: 1, // Single connection test
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload, response_buf| {
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        });

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();

        server_info_tx.send(server_endpoint.into()).unwrap();

        let client_info = client_info_rx.recv().unwrap();
        server.connect(conn_id, client_info.into()).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx.send(client_endpoint.into()).unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_info.into()).expect("connect");

    // Default to pipeline depth 8 for good throughput.
    // Use SCALERPC_PIPELINE_DEPTH env var to customize.
    let pipeline_depth = std::env::var("SCALERPC_PIPELINE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let total_requests = std::env::var("SCALERPC_TOTAL_REQUESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);
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
            pending_requests.push(p);
            sent += 1;
        } else {
            break;
        }
    }

    // Batch doorbell for initial fill
    client.poll();

    // Main loop
    let timeout = std::time::Duration::from_secs(30);
    while completed < total_requests {
        if start.elapsed() > timeout {
            panic!("Test timed out after {:?}. completed={}, sent={}, pending={}",
                   timeout, completed, sent, pending_requests.len());
        }

        // Check for completed requests
        let mut i = 0;
        while i < pending_requests.len() {
            if client.poll_pending(&pending_requests[i]).is_some() {
                pending_requests.swap_remove(i);
                completed += 1;

                // Send new request
                if sent < total_requests {
                    if let Ok(p) = client.call_async(conn_id, 1, &payload) {
                        pending_requests.push(p);
                        sent += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        // Batch doorbell + CQ drain
        client.poll();
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
        max_connections: 8,
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
                num_slots: 64,  // Balanced for slot scanning
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: GroupConfig {
                num_groups: 1,
                ..Default::default()
            },
            max_connections: 1, // Single connection test
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload, response_buf| {
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        });

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();

        server_info_tx.send(server_endpoint.into()).unwrap();

        let client_info = client_info_rx.recv().unwrap();
        server.connect(conn_id, client_info.into()).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx.send(client_endpoint.into()).unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_info.into()).expect("connect");

    // Default to pipeline depth 8 for good throughput.
    let pipeline_depth = std::env::var("SCALERPC_PIPELINE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let total_requests = std::env::var("SCALERPC_TOTAL_REQUESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
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
            pending_requests.push(p);
            sent += 1;
        } else {
            break;
        }
    }

    // Batch doorbell for initial fill
    client.poll();

    // Main loop with timeout
    let timeout = std::time::Duration::from_secs(30);
    while completed < total_requests {
        if start.elapsed() > timeout {
            panic!("Test timed out after {:?}. completed={}, sent={}, pending={}",
                   timeout, completed, sent, pending_requests.len());
        }

        let mut i = 0;
        while i < pending_requests.len() {
            if client.poll_pending(&pending_requests[i]).is_some() {
                pending_requests.swap_remove(i);
                completed += 1;

                if sent < total_requests {
                    if let Ok(p) = client.call_async(conn_id, 1, &payload) {
                        pending_requests.push(p);
                        sent += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        // Batch doorbell + CQ drain
        client.poll();
    }

    let elapsed = start.elapsed();
    let throughput = completed as f64 / elapsed.as_secs_f64();

    println!("Total time: {:.2?}", elapsed);
    println!("Throughput: {:.2} KRPS", throughput / 1000.0);

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
}

/// Pure Process mode throughput test.
///
/// Uses call_direct() to bypass the warmup state machine and measure
/// raw RDMA WRITE performance without context switch overhead.
#[test]
fn test_throughput_process_mode() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test - no RDMA device");
            return;
        }
    };

    // Client needs enough slots for:
    // - send_slots: pre-allocated for call_direct WRITEs
    // - response_slots: pre-allocated for responses (must be >= pipeline_depth)
    // - mapping slots: must match server's pool size for slot indexing
    // The mapping slots count determines server slot cycling in call_direct.
    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: 512,  // Large enough for pipeline depth + send slots
            slot_data_size: 4080,
        },
        max_connections: 1,
    };

    let mut client = RpcClient::new(&ctx.pd, client_config).expect("client");
    let conn_id = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("add_connection");
    let client_endpoint = client.local_endpoint(conn_id).expect("local_endpoint");

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

        let server_pool_size = std::env::var("SCALERPC_SERVER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256);
        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: server_pool_size,
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: GroupConfig {
                num_groups: 1,
                ..Default::default()
            },
            max_connections: 1,
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s,
            Err(_) => return,
        };

        server.set_handler(|_rpc_type, payload, response_buf| {
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        });

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(_) => return,
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();
        server_info_tx.send(server_endpoint.into()).unwrap();

        let client_info = client_info_rx.recv().unwrap();
        server.connect(conn_id, client_info.into()).unwrap();
        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }
            std::hint::spin_loop();
        }
        eprintln!("[server] exiting, total_processed={}", total_processed);
    });

    client_info_tx.send(client_endpoint.into()).unwrap();
    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_info.into()).expect("connect");

    let pipeline_depth = std::env::var("SCALERPC_PIPELINE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let total_requests = std::env::var("SCALERPC_TOTAL_REQUESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);
    let payload = vec![0xAAu8; 32];

    println!("\n=== ScaleRPC Process Mode Throughput Test ===");
    println!("Pipeline depth: {}", pipeline_depth);
    println!("Total requests: {}", total_requests);

    let mut pending_requests: Vec<scalerpc::PendingRpc<'_>> =
        Vec::with_capacity(pipeline_depth);
    let mut completed = 0usize;
    let mut sent = 0usize;

    let start = Instant::now();

    // Initial fill using call_direct (bypasses state machine)
    while sent < pipeline_depth && sent < total_requests {
        if let Ok(p) = client.call_direct(conn_id, 1, &payload) {
            pending_requests.push(p);
            sent += 1;
        } else {
            break;
        }
    }

    // Batch doorbell for initial fill
    client.poll();

    // Main loop
    let timeout = std::time::Duration::from_secs(30);
    while completed < total_requests {
        if start.elapsed() > timeout {
            panic!("Test timed out after {:?}. completed={}, sent={}, pending={}",
                   timeout, completed, sent, pending_requests.len());
        }

        let mut i = 0;
        while i < pending_requests.len() {
            if client.poll_pending(&pending_requests[i]).is_some() {
                pending_requests.swap_remove(i);
                completed += 1;

                // Send new request using call_direct
                if sent < total_requests {
                    if let Ok(p) = client.call_direct(conn_id, 1, &payload) {
                        pending_requests.push(p);
                        sent += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        client.poll();
    }

    let elapsed = start.elapsed();
    let throughput = completed as f64 / elapsed.as_secs_f64();

    println!("Total time: {:.2?}", elapsed);
    println!("Throughput: {:.2} KRPS", throughput / 1000.0);
    println!("Average latency: {:.2} µs", 1_000_000.0 / throughput);

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
}
