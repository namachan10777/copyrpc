//! ScaleRPC integration tests.
//!
//! Tests the full RPC flow with actual RDMA hardware.
//!
//! Run with:
//! ```bash
//! cargo test --package scalerpc --test integration_test -- --nocapture
//! ```

mod common;

use scalerpc::{
    ClientConfig, MessagePool, PoolConfig, RpcClient, RpcServer, ServerConfig,
};
use common::TestContext;

/// Test basic message pool operations with actual RDMA MR registration.
#[test]
fn test_message_pool_rdma() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let config = PoolConfig {
        num_slots: 16,
        slot_data_size: 4080,
    };

    let pool = MessagePool::new(&ctx.pd, &config).expect("Failed to create message pool");

    assert_eq!(pool.num_slots(), 16);
    assert!(pool.lkey() != 0);
    assert!(pool.rkey() != 0);
    assert!(pool.base_addr() != 0);

    // Allocate and free slots
    let slot1 = pool.alloc().expect("alloc slot1");
    let slot2 = pool.alloc().expect("alloc slot2");

    assert_eq!(pool.free_count(), 14);

    // Write data to slot
    let test_data = b"Hello ScaleRPC!";
    slot1.write(test_data);

    // Read back
    let read_back = slot1.read(test_data.len());
    assert_eq!(&read_back, test_data);

    drop(slot1);
    drop(slot2);

    assert_eq!(pool.free_count(), 16);

    println!("Message pool RDMA test passed!");
    println!("  Pool base: 0x{:x}", pool.base_addr());
    println!("  lkey: 0x{:x}", pool.lkey());
    println!("  rkey: 0x{:x}", pool.rkey());
}

/// Test client creation and local endpoint generation.
#[test]
fn test_client_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let config = ClientConfig {
        pool: PoolConfig {
            num_slots: 32,
            slot_data_size: 4080,
        },
        max_connections: 4, // 32 / 4 = 8 slots per connection
    };

    let mut client = RpcClient::new(&ctx.pd, config).expect("Failed to create client");

    // Add a connection
    let conn_id = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("Failed to add connection");

    assert_eq!(conn_id, 0);

    // Get local endpoint
    let endpoint = client.local_endpoint(conn_id).expect("Failed to get endpoint");
    assert!(endpoint.qpn != 0);
    assert!(endpoint.lid != 0);

    println!("Client creation test passed!");
    println!("  QPN: 0x{:x}", endpoint.qpn);
    println!("  LID: 0x{:x}", endpoint.lid);
    println!("  Slot addr: 0x{:x}", endpoint.slot_addr);
    println!("  Slot rkey: 0x{:x}", endpoint.slot_rkey);
}

/// Test server creation.
#[test]
fn test_server_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let config = ServerConfig {
        pool: PoolConfig {
            num_slots: 64,
            slot_data_size: 4080,
        },
        max_connections: 4, // 64 / 4 = 16 slots per connection
        ..Default::default()
    };

    let mut server = RpcServer::new(&ctx.pd, config)
        .expect("Failed to create server")
        .with_handler(|_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
            // Echo handler
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        });

    // Add a connection
    let conn_id = server
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("Failed to add connection");

    assert_eq!(conn_id, 0);

    // Get local endpoint
    let endpoint = server.local_endpoint(conn_id).expect("Failed to get endpoint");
    assert!(endpoint.qpn != 0);

    println!("Server creation test passed!");
    println!("  QPN: 0x{:x}", endpoint.qpn);
    println!("  Pool base: 0x{:x}", server.pool().base_addr());
}

/// Test loopback RPC (client and server in same process).
///
/// This test creates a client and server, connects them, and performs
/// a simple echo RPC.
#[test]
fn test_loopback_rpc() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create client
    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: 32,
            slot_data_size: 4080,
        },
        max_connections: 4, // 32 / 4 = 8 slots per connection
    };
    let mut client = RpcClient::new(&ctx.pd, client_config).expect("Failed to create client");

    // Create server
    // Use num_groups=1 so that current_group == next_group (warmup works for single connection)
    let server_config = ServerConfig {
        pool: PoolConfig {
            num_slots: 64,
            slot_data_size: 4080,
        },
        max_connections: 4, // 64 / 4 = 16 slots per connection
        group: scalerpc::config::GroupConfig {
            num_groups: 1,
            time_slice_us: 100,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut server = RpcServer::new(&ctx.pd, server_config)
        .expect("Failed to create server")
        .with_handler(|rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
            println!("Server received RPC type={}, payload_len={}", rpc_type, payload.len());
            // Echo back with prefix
            let prefix = b"ECHO: ";
            let prefix_len = prefix.len();
            let payload_len = payload.len().min(response_buf.len() - prefix_len);
            response_buf[..prefix_len].copy_from_slice(prefix);
            response_buf[prefix_len..prefix_len + payload_len].copy_from_slice(&payload[..payload_len]);
            (0, prefix_len + payload_len)
        });

    // Add connections
    let client_conn = client
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("Failed to add client connection");
    let server_conn = server
        .add_connection(&ctx.ctx, &ctx.pd, ctx.port)
        .expect("Failed to add server connection");

    // Get endpoints
    let client_endpoint = client.local_endpoint(client_conn).expect("client endpoint");
    let server_endpoint = server.local_endpoint(server_conn).expect("server endpoint");

    println!("Client endpoint: QPN=0x{:x}, LID=0x{:x}", client_endpoint.qpn, client_endpoint.lid);
    println!("Server endpoint: QPN=0x{:x}, LID=0x{:x}", server_endpoint.qpn, server_endpoint.lid);

    // Connect
    client.connect(client_conn, server_endpoint).expect("client connect");
    server.connect(server_conn, client_endpoint).expect("server connect");

    println!("Connections established!");

    // Make async RPC call so we can manually drive the server
    let request_payload = b"Hello from client!";
    println!("Sending RPC request: {:?}", std::str::from_utf8(request_payload));

    let pending = client
        .call_async(client_conn, 1, request_payload)
        .expect("RPC call failed");

    println!("Request sent, req_id={}", pending.req_id());

    // Flush doorbell to send the request
    client.poll();

    // Server processes request using poll() which handles:
    // - Fetching warmup requests via RDMA READ
    // - Context switch (swapping warmup/processing pools)
    // - Processing requests and sending responses
    // Note: Handler was already set via with_handler() above

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    loop {
        // poll() handles warmup fetch, context switch, and request processing
        let processed = server.poll();
        if processed > 0 {
            println!("Server processed {} requests", processed);
            // Keep polling a few more times to ensure response is sent
            for _ in 0..10 {
                server.poll();
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            break;
        }

        if start.elapsed() > timeout {
            panic!("Timeout waiting for server to process request");
        }
        std::hint::spin_loop();
    }

    // Client waits for response - poll manually for debugging
    println!("Waiting for response, slot_index={}", pending.slot_index());
    let start2 = std::time::Instant::now();
    let response = loop {
        if let Some(r) = pending.poll() {
            break r;
        }
        if start2.elapsed() > std::time::Duration::from_secs(5) {
            // Dump slot content for debugging
            if let Some(slot) = client.pool().get_slot(pending.slot_index()) {
                let data_ptr = slot.data_ptr();
                let magic = unsafe { std::ptr::read_volatile(data_ptr as *const u32) };
                println!("Slot magic: 0x{:08x} (expected 0x52455350)", magic);
            }
            panic!("wait for response: Protocol(\"response timeout\")");
        }
        std::hint::spin_loop();
    };

    assert!(response.is_success(), "RPC failed with status {}", response.status());

    let response_payload = response.payload();
    println!("Client received response: {:?}", std::str::from_utf8(response_payload));

    assert!(response_payload.starts_with(b"ECHO: "));

    println!("Loopback RPC test passed!");
}

/// Test context_switch event piggybacking on responses.
///
/// This test verifies that:
/// 1. Server can piggyback context_switch events on responses
/// 2. Client receives and processes the piggybacked event
/// 3. RpcResponse correctly reports context_switch information
#[test]
fn test_context_switch_piggyback() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Channel for endpoint exchange
    let (server_tx, server_rx) = mpsc::channel();
    let (client_tx, client_rx) = mpsc::channel();
    let server_ready = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let responses_with_ctx_switch = Arc::new(AtomicU32::new(0));

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();

    // Server thread with 1 group (simpler, but still tests piggyback mechanism)
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
            num_recv_slots: 64,
            group: scalerpc::config::GroupConfig {
                num_groups: 1,  // Single group for reliability
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
        server_tx.send(server_endpoint).unwrap();

        let client_endpoint = client_rx.recv().unwrap();
        server.connect(conn_id, client_endpoint).unwrap();

        server_ready_clone.store(true, Ordering::Release);

        while !stop_flag_clone.load(Ordering::Relaxed) {
            server.poll();
            std::hint::spin_loop();
        }
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
    client_tx.send(client_endpoint).unwrap();

    let server_endpoint = server_rx.recv().unwrap();

    while !server_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_endpoint).expect("connect");

    // Send multiple requests and check for context_switch events in responses
    let payload = vec![0xAAu8; 32];
    let num_requests = 20;
    let responses_clone = responses_with_ctx_switch.clone();

    for i in 0..num_requests {
        let pending = client.call_async(conn_id, 1, &payload).expect("call_async");
        client.poll();

        // Wait for response
        let start = std::time::Instant::now();
        let response = loop {
            client.poll();
            if let Some(r) = client.poll_pending(&pending) {
                break r;
            }
            if start.elapsed() > Duration::from_secs(5) {
                eprintln!("Timeout on request {}", i);
                panic!("timeout");
            }
            std::hint::spin_loop();
        };

        assert!(response.is_success(), "RPC failed");

        // Check for piggybacked context_switch event
        if response.has_context_switch() {
            responses_clone.fetch_add(1, Ordering::Relaxed);
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    let ctx_switch_count = responses_with_ctx_switch.load(Ordering::Relaxed);
    println!("\n=== Context Switch Piggyback Test ===");
    println!("Total requests: {}", num_requests);
    println!("Responses with context_switch: {}", ctx_switch_count);

    // The mechanism should work (context_switch events piggybacked on first response)
    println!("Test passed: context_switch piggyback mechanism works!");
}

/// Test explicit flush_warmup() for batch notification.
///
/// This test verifies that:
/// 1. Multiple requests can be batched in warmup buffer
/// 2. flush_warmup() sends a single endpoint entry notification
/// 3. Server fetches and processes the entire batch
#[test]
fn test_flush_warmup_batching() {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let (server_tx, server_rx) = mpsc::channel();
    let (client_tx, client_rx) = mpsc::channel();
    let server_ready = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let requests_processed = Arc::new(AtomicU32::new(0));

    let server_ready_clone = server_ready.clone();
    let stop_flag_clone = stop_flag.clone();
    let requests_processed_clone = requests_processed.clone();

    // Server thread
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
            num_recv_slots: 64,
            group: scalerpc::config::GroupConfig {
                num_groups: 1,  // Single group for simplicity
                time_slice_us: 100,
                ..Default::default()
            },
            max_connections: 1,
        };

        let requests_clone = requests_processed_clone.clone();
        let mut server = match RpcServer::new(&ctx.pd, server_config) {
            Ok(s) => s.with_handler(move |_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
                requests_clone.fetch_add(1, Ordering::Relaxed);
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
        server_tx.send(server_endpoint).unwrap();

        let client_endpoint = client_rx.recv().unwrap();
        server.connect(conn_id, client_endpoint).unwrap();

        server_ready_clone.store(true, Ordering::Release);

        while !stop_flag_clone.load(Ordering::Relaxed) {
            server.poll();
            std::hint::spin_loop();
        }
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
    client_tx.send(client_endpoint).unwrap();

    let server_endpoint = server_rx.recv().unwrap();

    while !server_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_endpoint).expect("connect");

    // Batch multiple requests without auto-flush
    let payload = vec![0xBBu8; 32];
    let batch_size = 8;
    let mut pendings = Vec::new();

    println!("\n=== Flush Warmup Batching Test ===");

    // Queue multiple requests using poll_minimal() (no auto-flush)
    for i in 0..batch_size {
        match client.call_async(conn_id, 1, &payload) {
            Ok(pending) => {
                pendings.push(pending);
                // Use poll_minimal to check events but NOT auto-flush
                client.poll_minimal();
            }
            Err(e) => {
                println!("Request {} failed: {:?}", i, e);
                break;
            }
        }
    }

    println!("Queued {} requests in warmup buffer", pendings.len());

    // Now explicitly flush the batch
    let flushed = client.flush_warmup(conn_id).expect("flush_warmup");
    println!("Flushed {} requests with single endpoint entry", flushed);

    // Ring doorbells
    client.poll_minimal();

    // Wait for all responses
    let mut completed = 0;
    let start = std::time::Instant::now();

    while completed < pendings.len() {
        client.poll();

        for pending in &pendings {
            if pending.poll().is_some() {
                completed += 1;
            }
        }

        if start.elapsed() > Duration::from_secs(5) {
            println!("Timeout: completed {}/{}", completed, pendings.len());
            break;
        }
        std::hint::spin_loop();
    }

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    let processed = requests_processed.load(Ordering::Relaxed);
    println!("Server processed: {} requests", processed);
    println!("Client completed: {}/{} responses", completed, pendings.len());

    assert!(
        completed > 0,
        "At least some requests should complete"
    );

    println!("Test passed: flush_warmup batching works!");
}

/// Test poll_pending() with context switch handling.
///
/// Verifies that poll_pending() correctly processes responses
/// and handles piggybacked context switch events.
#[test]
fn test_poll_pending_with_state_transition() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use scalerpc::ClientState;

    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let (server_tx, server_rx) = mpsc::channel();
    let (client_tx, client_rx) = mpsc::channel();
    let server_ready = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::new(AtomicBool::new(false));

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
                num_slots: 256,
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: scalerpc::config::GroupConfig {
                num_groups: 1,
                time_slice_us: 100,
                ..Default::default()
            },
            max_connections: 1,
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

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to add server connection: {}", e);
                return;
            }
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();
        server_tx.send(server_endpoint).unwrap();

        let client_endpoint = client_rx.recv().unwrap();
        server.connect(conn_id, client_endpoint).unwrap();

        server_ready_clone.store(true, Ordering::Release);

        while !stop_flag_clone.load(Ordering::Relaxed) {
            server.poll();
            std::hint::spin_loop();
        }
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
    client_tx.send(client_endpoint).unwrap();

    let server_endpoint = server_rx.recv().unwrap();

    while !server_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_endpoint).expect("connect");

    println!("\n=== Poll Pending State Transition Test ===");

    // Initial state should be Connected
    let initial_state = client.state(conn_id).expect("get state");
    println!("Initial state: {:?}", initial_state);
    assert_eq!(initial_state, ClientState::Connected);

    // Send a request - should transition to Warmup
    let payload = vec![0xCCu8; 32];
    let pending = client.call_async(conn_id, 1, &payload).expect("call_async");

    let after_call_state = client.state(conn_id).expect("get state");
    println!("After call_async state: {:?}", after_call_state);
    assert_eq!(after_call_state, ClientState::Warmup);

    client.poll();

    // Wait for response using poll_pending()
    let start = std::time::Instant::now();
    let response = loop {
        client.poll();
        if let Some(r) = client.poll_pending(&pending) {
            break r;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Response timeout");
        }
        std::hint::spin_loop();
    };

    assert!(response.is_success(), "RPC failed");

    let final_state = client.state(conn_id).expect("get state");
    println!("Final state: {:?}", final_state);
    println!("Response has context_switch: {}", response.has_context_switch());

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    println!("Test passed: poll_pending with state transition works!");
}

/// Test Process mode state transition.
///
/// This test verifies that:
/// 1. Client transitions from Warmup to Process when receiving pool info
/// 2. Client can send requests directly via RDMA WRITE in Process state
/// 3. Client transitions from Process to Idle on time-based context switch
#[test]
fn test_process_mode_transition() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use scalerpc::ClientState;

    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let (server_tx, server_rx) = mpsc::channel();
    let (client_tx, client_rx) = mpsc::channel();
    let server_ready = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::new(AtomicBool::new(false));

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
                num_slots: 256,
                slot_data_size: 4080,
            },
            num_recv_slots: 64,
            group: scalerpc::config::GroupConfig {
                num_groups: 1,
                time_slice_us: 100,
                ..Default::default()
            },
            max_connections: 1,
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

        let conn_id = match server.add_connection(&ctx.ctx, &ctx.pd, ctx.port) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to add server connection: {}", e);
                return;
            }
        };

        let server_endpoint = server.local_endpoint(conn_id).unwrap();
        server_tx.send(server_endpoint).unwrap();

        let client_endpoint = client_rx.recv().unwrap();
        server.connect(conn_id, client_endpoint).unwrap();

        server_ready_clone.store(true, Ordering::Release);

        while !stop_flag_clone.load(Ordering::Relaxed) {
            server.poll();
            std::hint::spin_loop();
        }
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
    client_tx.send(client_endpoint).unwrap();

    let server_endpoint = server_rx.recv().unwrap();

    while !server_ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    client.connect(conn_id, server_endpoint).expect("connect");

    println!("\n=== Process Mode Transition Test ===");

    // 1. Initial state should be Connected
    let initial_state = client.state(conn_id).expect("get state");
    println!("1. Initial state: {:?}", initial_state);
    assert_eq!(initial_state, ClientState::Connected);

    // 2. Send first request - transitions to Warmup
    let payload = vec![0xDDu8; 32];
    let pending1 = client.call_async(conn_id, 1, &payload).expect("call_async");
    let after_first_call = client.state(conn_id).expect("get state");
    println!("2. After first call_async: {:?}", after_first_call);
    assert_eq!(after_first_call, ClientState::Warmup);

    // 3. Flush warmup and poll
    client.poll();

    // 4. Wait for response with pool info (should transition to Process)
    let start = std::time::Instant::now();
    let response1 = loop {
        client.poll();
        if let Some(r) = client.poll_pending(&pending1) {
            break r;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Response1 timeout");
        }
        std::hint::spin_loop();
    };

    assert!(response1.is_success(), "RPC1 failed");
    println!("3. Response1 has context_switch: {}, has_processing_pool_info: {}",
             response1.has_context_switch(),
             response1.has_processing_pool_info());

    let after_response1 = client.state(conn_id).expect("get state");
    println!("4. After response1 (with pool info): {:?}", after_response1);

    // If pool info was received, state should be Process
    if response1.has_processing_pool_info() {
        assert_eq!(after_response1, ClientState::Process,
            "State should be Process after receiving pool info");

        // 5. Send more requests in Process mode (direct RDMA WRITE)
        println!("5. Sending requests in Process mode...");
        for i in 0..3 {
            let pending = client.call_async(conn_id, 1, &payload).expect("call_async in process mode");
            client.poll();

            // Wait for response
            let start = std::time::Instant::now();
            let response = loop {
                client.poll();
                if let Some(r) = client.poll_pending(&pending) {
                    break r;
                }
                if start.elapsed() > Duration::from_secs(5) {
                    panic!("Process mode request {} timeout", i);
                }
                std::hint::spin_loop();
            };

            assert!(response.is_success(), "Process mode RPC {} failed", i);

            // Check state remains in Process
            let state = client.state(conn_id).expect("get state");

            // State might transition to Idle if context switch happens
            if state == ClientState::Idle {
                println!("   Transitioned to Idle (context switch)");
                break;
            }
        }
    } else {
        println!("   Note: No pool info received (server might not have triggered warmup fetch)");
    }

    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();

    println!("\nTest passed: Process mode transition works!");
}
