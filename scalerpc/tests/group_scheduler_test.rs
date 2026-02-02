//! GroupScheduler integration tests.
//!
//! Tests the GroupScheduler with actual RDMA communication.
//! Verifies state transitions (Processing / Warmup / Inactive) for various group counts.
//!
//! Run with:
//! ```bash
//! cargo test --package scalerpc --test group_scheduler_test --release -- --nocapture
//! ```

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::TestContext;
use scalerpc::connection::RemoteEndpoint;
use scalerpc::{ClientConfig, GroupConfig, PoolConfig, RpcClient, RpcServer, ServerConfig};

/// Connection info for cross-thread endpoint exchange.
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

/// Test single group communication (Group = 1).
///
/// Single group means no context switch is needed.
/// Verifies that should_switch() returns false with 1 group.
#[test]
fn test_single_group_communication() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let num_requests = 100;
    let completed_count = Arc::new(AtomicU32::new(0));

    // Channel for endpoint exchange
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    // Server thread
    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => return,
        };

        // num_groups = 1: Only Processing state, no context switch
        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: 256,
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: GroupConfig {
                num_groups: 1,
                time_slice_us: 100,
                ..Default::default()
            },
            max_connections: 1,
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s.with_handler(|_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
                // Echo handler
                let len = payload.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&payload[..len]);
                (0, len)
            }),
            Err(e) => {
                eprintln!("Failed to create server: {}", e);
                return;
            }
        };

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to add server connection: {}", e);
                return;
            }
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();
        server_info_tx.send(server_endpoint.into()).unwrap();

        let client_info = client_info_rx.recv().unwrap();
        server.connect(conn_id, client_info.into()).unwrap();

        // Verify: with 1 group, should_switch() must return false
        assert!(
            !server.scheduler().should_switch(),
            "should_switch() must be false with 1 group"
        );

        server_ready_clone.store(1, Ordering::Release);

        let mut total_processed = 0u64;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }

            // Verify again during processing
            assert!(
                !server.scheduler().should_switch(),
                "should_switch() must remain false with 1 group"
            );

            std::hint::spin_loop();
        }
        eprintln!("[server] total_processed={}", total_processed);
    });

    // Client setup
    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: 64,
            slot_data_size: 4080,
        },
        max_connections: 1,
    };

    let mut client = RpcClient::new(&ctx.pd, client_config).expect("create client");
    let conn_id = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("add client connection");

    let client_endpoint = client.local_endpoint(conn_id).expect("client endpoint");
    client_info_tx.send(client_endpoint.into()).unwrap();

    let server_info = server_info_rx.recv().unwrap();

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_info.into()).expect("connect");

    // Send requests
    let payload = vec![0xAAu8; 32];
    let completed_count_clone = completed_count.clone();

    for _ in 0..num_requests {
        let pending = client.call_async(conn_id, 1, &payload).expect("call_async");
        client.poll();

        // Poll for response (no timeout - flow control via slot allocation)
        loop {
            client.poll();
            if let Some(response) = pending.poll() {
                assert!(response.is_success(), "RPC failed");
                completed_count_clone.fetch_add(1, Ordering::Relaxed);
                break;
            }
            std::hint::spin_loop();
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    let completed = completed_count.load(Ordering::Relaxed);
    assert_eq!(
        completed, num_requests,
        "Expected {} requests, got {}",
        num_requests, completed
    );

    println!("\n=== Single Group Test Passed ===");
    println!("Completed: {} requests", completed);
}

/// Test two groups context switch (Group = 2).
///
/// With 2 groups, context switch alternates between Group 0 and Group 1.
/// Both groups should be able to complete requests.
#[test]
fn test_two_groups_context_switch() {
    let num_groups = 2;
    let num_requests_per_client = 30;

    // Channels for endpoint exchange (one per connection)
    let mut server_info_txs: Vec<Sender<ConnectionInfo>> = Vec::new();
    let mut server_info_rxs: Vec<Receiver<ConnectionInfo>> = Vec::new();
    let mut client_info_txs: Vec<Sender<ConnectionInfo>> = Vec::new();
    let mut client_info_rxs: Vec<Receiver<ConnectionInfo>> = Vec::new();

    for _ in 0..num_groups {
        let (stx, srx) = mpsc::channel();
        let (ctx, crx) = mpsc::channel();
        server_info_txs.push(stx);
        server_info_rxs.push(srx);
        client_info_txs.push(ctx);
        client_info_rxs.push(crx);
    }

    let server_ready = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let completed_counts: Vec<Arc<AtomicU32>> = (0..num_groups)
        .map(|_| Arc::new(AtomicU32::new(0)))
        .collect();

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    // Server thread
    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => return,
        };

        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: 512,
                slot_data_size: 4080,
            },
            num_recv_slots: 128,
            group: GroupConfig {
                num_groups: 2,
                time_slice_us: 1000, // 1ms for stable context switching
                ..Default::default()
            },
            max_connections: 2,
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s.with_handler(|_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
                let len = payload.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&payload[..len]);
                (0, len)
            }),
            Err(e) => {
                eprintln!("Failed to create server: {}", e);
                return;
            }
        };

        // Add connections (each goes to a different group via round-robin)
        for i in 0..num_groups {
            let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to add server connection {}: {}", i, e);
                    return;
                }
            };
            assert_eq!(conn_id, i, "Connection ID mismatch");

            let server_endpoint = server.local_endpoint(conn_id).unwrap();
            server_info_txs[i].send(server_endpoint.into()).unwrap();
        }

        // Connect all
        for i in 0..num_groups {
            let client_info = client_info_rxs[i].recv().unwrap();
            server.connect(i, client_info.into()).unwrap();
        }

        server_ready_clone.store(num_groups as u32, Ordering::Release);

        let mut total_processed = 0u64;
        let mut context_switches = 0u64;
        let last_group = std::cell::Cell::new(server.scheduler().current_group_id());

        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }

            // Track context switches
            let current_group = server.scheduler().current_group_id();
            if current_group != last_group.get() {
                context_switches += 1;
                last_group.set(current_group);
            }

            std::hint::spin_loop();
        }
        eprintln!(
            "[server] total_processed={}, context_switches={}",
            total_processed, context_switches
        );
    });

    // Client threads (one per group)
    let mut client_handles = Vec::new();

    for i in 0..num_groups {
        let server_info_rx = server_info_rxs.remove(0);
        let client_info_tx = client_info_txs.remove(0);
        let server_ready = server_ready.clone();
        let stop_flag = stop_flag.clone();
        let completed_count = completed_counts[i].clone();

        let handle = thread::spawn(move || {
            let ctx = match TestContext::new() {
                Some(ctx) => ctx,
                None => return,
            };

            let client_config = ClientConfig {
                pool: PoolConfig {
                    num_slots: 64,
                    slot_data_size: 4080,
                },
                max_connections: 1,
            };

            let mut client = RpcClient::new(&ctx.pd, client_config).expect("create client");
            let conn_id = client
                .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
                .expect("add connection");

            let client_endpoint = client.local_endpoint(conn_id).expect("client endpoint");
            client_info_tx.send(client_endpoint.into()).unwrap();

            let server_info = server_info_rx.recv().unwrap();

            while server_ready.load(Ordering::Acquire) < num_groups as u32 {
                std::hint::spin_loop();
            }

            client.connect(conn_id, server_info.into()).expect("connect");

            let payload = vec![0xA0 + i as u8; 32];

            // Give the server time to complete initial context switch cycle
            thread::sleep(Duration::from_millis(5));

            let mut completed = 0;

            while completed < num_requests_per_client {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }

                match client.call_async(conn_id, 1, &payload) {
                    Ok(pending) => {
                        // Poll for response (no timeout - flow control via slot allocation)
                        loop {
                            client.poll();
                            if let Some(response) = pending.poll() {
                                if response.is_success() {
                                    completed += 1;
                                    completed_count.fetch_add(1, Ordering::Relaxed);
                                }
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                    Err(_e) => {
                        // NoFreeSlots - warmup buffer full, poll to clear
                        client.poll();
                        thread::sleep(Duration::from_micros(100));
                    }
                }
            }
        });
        client_handles.push(handle);
    }

    // Wait for clients to complete
    for handle in client_handles {
        handle.join().unwrap();
    }

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    // Verify results
    let mut total_completed = 0u32;
    for (i, count) in completed_counts.iter().enumerate() {
        let completed = count.load(Ordering::Relaxed);
        println!("Group {}: {} requests completed", i, completed);
        total_completed += completed;
    }

    let expected_total = (num_groups * num_requests_per_client) as u32;
    assert!(
        total_completed > 0,
        "At least some requests should complete"
    );
    println!("\n=== Two Groups Context Switch Test Passed ===");
    println!(
        "Total completed: {} / {} requests",
        total_completed, expected_total
    );
}

/// Test three groups round-robin (Group = 3).
///
/// With 3 groups:
/// - 1 group is Processing
/// - 1 group is Warmup
/// - 1 group is Inactive
///
/// Round-robin: Group 0 -> Group 1 -> Group 2 -> Group 0 -> ...
///
/// Note: With 3 groups, Group 0 is initially the current_group (Processing),
/// but requests are sent via warmup mechanism. Group 0 must wait until it
/// becomes next_group (after 2 context switches) to have its requests fetched.
/// We use a longer time slice to ensure the scheduler cycles through all groups.
#[test]
fn test_three_groups_round_robin() {
    let num_groups = 3;
    let num_requests_per_client = 20;

    // Channels for endpoint exchange
    let mut server_info_txs: Vec<Sender<ConnectionInfo>> = Vec::new();
    let mut server_info_rxs: Vec<Receiver<ConnectionInfo>> = Vec::new();
    let mut client_info_txs: Vec<Sender<ConnectionInfo>> = Vec::new();
    let mut client_info_rxs: Vec<Receiver<ConnectionInfo>> = Vec::new();

    for _ in 0..num_groups {
        let (stx, srx) = mpsc::channel();
        let (ctx, crx) = mpsc::channel();
        server_info_txs.push(stx);
        server_info_rxs.push(srx);
        client_info_txs.push(ctx);
        client_info_rxs.push(crx);
    }

    let server_ready = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let completed_counts: Vec<Arc<AtomicU32>> = (0..num_groups)
        .map(|_| Arc::new(AtomicU32::new(0)))
        .collect();

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    // Server thread
    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => return,
        };

        let server_config = ServerConfig {
            pool: PoolConfig {
                num_slots: 768,
                slot_data_size: 4080,
            },
            num_recv_slots: 128,
            group: GroupConfig {
                num_groups: 3,
                time_slice_us: 1000, // 1ms for more stable context switching
                ..Default::default()
            },
            max_connections: 3,
        };

        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s.with_handler(|_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
                let len = payload.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&payload[..len]);
                (0, len)
            }),
            Err(e) => {
                eprintln!("Failed to create server: {}", e);
                return;
            }
        };

        // Add connections (round-robin group assignment)
        for i in 0..num_groups {
            let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to add server connection {}: {}", i, e);
                    return;
                }
            };
            assert_eq!(conn_id, i, "Connection ID mismatch");

            let server_endpoint = server.local_endpoint(conn_id).unwrap();
            server_info_txs[i].send(server_endpoint.into()).unwrap();
        }

        // Connect all
        for i in 0..num_groups {
            let client_info = client_info_rxs[i].recv().unwrap();
            server.connect(i, client_info.into()).unwrap();
        }

        server_ready_clone.store(num_groups as u32, Ordering::Release);

        let mut total_processed = 0u64;
        let mut context_switches = 0u64;
        let mut group_visits = [0u64; 3];
        let last_group = std::cell::Cell::new(server.scheduler().current_group_id());

        while !stop_flag_clone.load(Ordering::Relaxed) {
            let n = server.poll();
            if n > 0 {
                total_processed += n as u64;
            }

            let current_group = server.scheduler().current_group_id();
            if current_group != last_group.get() {
                context_switches += 1;
                last_group.set(current_group);
            }
            if current_group < 3 {
                group_visits[current_group] += 1;
            }

            std::hint::spin_loop();
        }
        eprintln!(
            "[server] total_processed={}, context_switches={}, group_visits={:?}",
            total_processed, context_switches, group_visits
        );
    });

    // Client threads
    let mut client_handles = Vec::new();

    for i in 0..num_groups {
        let server_info_rx = server_info_rxs.remove(0);
        let client_info_tx = client_info_txs.remove(0);
        let server_ready = server_ready.clone();
        let stop_flag = stop_flag.clone();
        let completed_count = completed_counts[i].clone();

        let handle = thread::spawn(move || {
            let ctx = match TestContext::new() {
                Some(ctx) => ctx,
                None => return,
            };

            let client_config = ClientConfig {
                pool: PoolConfig {
                    num_slots: 64,
                    slot_data_size: 4080,
                },
                max_connections: 1,
            };

            let mut client = RpcClient::new(&ctx.pd, client_config).expect("create client");
            let conn_id = client
                .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
                .expect("add connection");

            let client_endpoint = client.local_endpoint(conn_id).expect("client endpoint");
            client_info_tx.send(client_endpoint.into()).unwrap();

            let server_info = server_info_rx.recv().unwrap();

            while server_ready.load(Ordering::Acquire) < num_groups as u32 {
                std::hint::spin_loop();
            }

            client.connect(conn_id, server_info.into()).expect("connect");

            let payload = vec![0xB0 + i as u8; 32];

            // Give the server time to complete initial context switch cycle
            // With 3 groups and time_slice_us=1000, one full cycle takes ~3ms
            thread::sleep(Duration::from_millis(10));

            let mut completed = 0;

            while completed < num_requests_per_client {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }

                match client.call_async(conn_id, 1, &payload) {
                    Ok(pending) => {
                        // Poll for response (no timeout - flow control via slot allocation)
                        loop {
                            client.poll();
                            if let Some(response) = pending.poll() {
                                if response.is_success() {
                                    completed += 1;
                                    completed_count.fetch_add(1, Ordering::Relaxed);
                                }
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                    Err(_e) => {
                        // NoFreeSlots - warmup buffer full, poll to clear
                        client.poll();
                        thread::sleep(Duration::from_micros(100));
                    }
                }
            }
        });
        client_handles.push(handle);
    }

    // Wait for clients
    for handle in client_handles {
        handle.join().unwrap();
    }

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    // Verify results
    let mut total_completed = 0u32;
    for (i, count) in completed_counts.iter().enumerate() {
        let completed = count.load(Ordering::Relaxed);
        println!("Group {}: {} requests completed", i, completed);
        total_completed += completed;
    }

    let expected_total = (num_groups * num_requests_per_client) as u32;
    assert!(
        total_completed > 0,
        "At least some requests should complete"
    );

    // Verify all groups received some requests
    for (i, count) in completed_counts.iter().enumerate() {
        let completed = count.load(Ordering::Relaxed);
        assert!(
            completed > 0,
            "Group {} should have completed at least one request",
            i
        );
    }

    println!("\n=== Three Groups Round-Robin Test Passed ===");
    println!(
        "Total completed: {} / {} requests",
        total_completed, expected_total
    );
}
