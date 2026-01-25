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
        ..Default::default()
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
        ..Default::default()
    };

    let mut server = RpcServer::new(&ctx.pd, config).expect("Failed to create server");

    // Set a simple handler
    server.set_handler(|_rpc_type, payload| {
        // Echo handler
        (0, payload.to_vec())
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
        timeout_ms: 5000,
        ..Default::default()
    };
    let mut client = RpcClient::new(&ctx.pd, client_config).expect("Failed to create client");

    // Create server
    let server_config = ServerConfig {
        pool: PoolConfig {
            num_slots: 64,
            slot_data_size: 4080,
        },
        ..Default::default()
    };
    let mut server = RpcServer::new(&ctx.pd, server_config).expect("Failed to create server");

    // Set echo handler
    server.set_handler(|rpc_type, payload| {
        println!("Server received RPC type={}, payload_len={}", rpc_type, payload.len());
        // Echo back with prefix
        let mut response = b"ECHO: ".to_vec();
        response.extend_from_slice(payload);
        (0, response)
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

    // Server processes request
    // In a real scenario, this would be in a separate thread/process
    // For loopback test, we manually drive both sides
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    loop {
        // Check for incoming request on server
        if let Some(request) = server.recv() {
            println!("Server received request: type={}, payload={:?}",
                     request.rpc_type,
                     std::str::from_utf8(&request.payload));

            // Process and send response
            let (status, response_payload) = {
                let handler = |_rpc_type: u16, payload: &[u8]| {
                    let mut response = b"ECHO: ".to_vec();
                    response.extend_from_slice(payload);
                    (0u32, response)
                };
                handler(request.rpc_type, &request.payload)
            };

            server
                .reply(&request, status, &response_payload)
                .expect("send response");

            // Flush doorbell to send the response
            server.flush_doorbells();
            server.clear_needs_flush();

            println!("Server sent response");
            break;
        }

        if start.elapsed() > timeout {
            panic!("Timeout waiting for server to receive request");
        }
        std::hint::spin_loop();
    }

    // Client waits for response
    let response = pending.wait().expect("wait for response");

    assert!(response.is_success(), "RPC failed with status {}", response.status());

    let response_payload = response.payload();
    println!("Client received response: {:?}", std::str::from_utf8(response_payload));

    assert!(response_payload.starts_with(b"ECHO: "));

    println!("Loopback RPC test passed!");
}
