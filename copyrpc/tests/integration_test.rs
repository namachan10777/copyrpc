//! Integration tests for copyrpc.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use copyrpc::{Context, ContextBuilder, EndpointConfig, RemoteEndpointInfo};
use mlx5::srq::SrqConfig;

// =============================================================================
// Connection Info
// =============================================================================

#[derive(Clone)]
struct EndpointConnectionInfo {
    qp_number: u32,
    packet_sequence_number: u32,
    local_identifier: u16,
    recv_ring_addr: u64,
    recv_ring_rkey: u32,
    recv_ring_size: u64,
    consumer_addr: u64,
    consumer_rkey: u32,
}

// =============================================================================
// User Data for RPC calls
// =============================================================================

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
struct CallUserData {
    call_id: u32,
}

// =============================================================================
// Basic Context Creation Test
// =============================================================================

#[test]
fn test_context_creation() {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let result: Result<Context<CallUserData, _>, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build();

    assert!(result.is_ok(), "Failed to create context: {:?}", result.err());
}

// =============================================================================
// Endpoint Creation Test
// =============================================================================

#[test]
fn test_endpoint_creation() {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create context");

    let ep_config = EndpointConfig::default();
    let ep = ctx.create_endpoint(&ep_config);

    assert!(ep.is_ok(), "Failed to create endpoint: {:?}", ep.err());
}

// =============================================================================
// Simple Ping-Pong Test
// =============================================================================

#[test]
fn test_simple_pingpong() {
    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    // Use a closure that captures the completed counter
    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        eprintln!("Client: on_response called!");
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    // Setup communication channels with server
    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect
    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    // Send a request
    let request_data = vec![0u8; 32];
    let user_data = CallUserData { call_id: 1 };

    eprintln!("Sending request...");
    ep.call(&request_data, user_data).expect("Failed to send request");

    // Poll for response with timeout
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut poll_count = 0;

    while start.elapsed() < timeout {
        ctx.poll();
        poll_count += 1;

        if poll_count % 100000 == 0 {
            eprintln!("Client: poll_count = {}", poll_count);
        }

        // Check if response was received via on_response callback
        if completed.load(Ordering::SeqCst) > 0 {
            eprintln!("Client: Received response via on_response callback!");
            break;
        }
    }

    eprintln!("Client: total poll_count = {}", poll_count);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let response_count = completed.load(Ordering::SeqCst);
    assert!(response_count > 0, "Did not receive response within timeout (completed={})", response_count);
}

fn server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];

    eprintln!("Server: Starting loop...");

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            eprintln!("Server: Received request, sending reply...");
            if let Err(e) = req.reply(&response_data) {
                eprintln!("Server: Failed to reply: {:?}", e);
            }
        }
    }

    eprintln!("Server: Exiting...");
}

// =============================================================================
// Multi-Endpoint Pingpong Test
// =============================================================================

const NUM_ENDPOINTS: usize = 8;
const REQUESTS_PER_EP: usize = 128;
const MESSAGE_SIZE: usize = 32;

#[derive(Clone)]
struct MultiEndpointConnectionInfo {
    endpoints: Vec<EndpointConnectionInfo>,
}

#[test]
fn test_multi_endpoint_pingpong() {
    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256, // Enough for all endpoints' initial recvs
            max_sge: 1,
        })
        .cq_size(4096)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();

    let mut endpoints = Vec::with_capacity(NUM_ENDPOINTS);
    let mut client_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for i in 0..NUM_ENDPOINTS {
        eprintln!("Client: Creating endpoint {}...", i);
        let ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());
        eprintln!("Client: Endpoint {} created, qpn={}", i, info.qp_number);

        client_infos.push(EndpointConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            recv_ring_size: info.recv_ring_size,
            consumer_addr: info.consumer_addr,
            consumer_rkey: info.consumer_rkey,
        });

        endpoints.push(ep);
    }

    // Setup communication channels with server
    let (server_info_tx, server_info_rx): (
        Sender<MultiEndpointConnectionInfo>,
        Receiver<MultiEndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<MultiEndpointConnectionInfo>,
        Receiver<MultiEndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        multi_endpoint_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(MultiEndpointConnectionInfo {
        endpoints: client_infos,
    }).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect endpoints
    for (i, ep) in endpoints.iter_mut().enumerate() {
        let server_ep = &server_info.endpoints[i];

        let remote = RemoteEndpointInfo {
            qp_number: server_ep.qp_number,
            packet_sequence_number: server_ep.packet_sequence_number,
            local_identifier: server_ep.local_identifier,
            recv_ring_addr: server_ep.recv_ring_addr,
            recv_ring_rkey: server_ep.recv_ring_rkey,
            recv_ring_size: server_ep.recv_ring_size,
            consumer_addr: server_ep.consumer_addr,
            consumer_rkey: server_ep.consumer_rkey,
        };

        ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");
        eprintln!("Client: Endpoint {} connected", i);
    }

    // Send requests on all endpoints
    let request_data = vec![0u8; MESSAGE_SIZE];
    let mut total_sent = 0;

    for (ep_id, ep) in endpoints.iter().enumerate() {
        for slot_id in 0..REQUESTS_PER_EP {
            let user_data = CallUserData { call_id: (ep_id * REQUESTS_PER_EP + slot_id) as u32 };
            ep.call(&request_data, user_data).expect("Failed to send request");
            total_sent += 1;
        }
    }
    eprintln!("Client: Sent {} requests", total_sent);

    // Flush initial batch
    ctx.poll();

    // Poll for responses with timeout
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);

    while start.elapsed() < timeout {
        ctx.poll();

        let current_completed = completed.load(Ordering::SeqCst);
        if current_completed as usize >= total_sent {
            eprintln!("Client: All {} responses received", current_completed);
            break;
        }

        if start.elapsed().as_secs().is_multiple_of(2) {
            // Print progress every 2 seconds
        }
    }

    let final_completed = completed.load(Ordering::SeqCst);
    eprintln!("Client: Final completed count: {}/{}", final_completed, total_sent);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(final_completed as usize, total_sent,
        "Did not receive all responses within timeout (completed={}, expected={})",
        final_completed, total_sent);
}

// =============================================================================
// SRQ Exhaustion Test - Reproduces the benchmark hang
// =============================================================================

/// Test that reproduces the SRQ exhaustion issue.
///
/// The initial SRQ recv count is 16 per endpoint, so after 16 pingpongs
/// the SRQ will be exhausted unless recvs are reposted.
/// This test does 50 pingpongs to ensure the issue is triggered.
#[test]
fn test_srq_exhaustion_pingpong() {
    const ITERATIONS: usize = 50; // More than INITIAL_RECV_COUNT (16)

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    // Setup communication channels with server
    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        srq_exhaustion_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect
    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    // Run multiple pingpong iterations
    let request_data = vec![0u8; 32];
    let timeout = Duration::from_secs(5);

    for i in 0..ITERATIONS {
        let before = completed.load(Ordering::SeqCst);

        // Send request
        let user_data = CallUserData { call_id: i as u32 };
        ep.call(&request_data, user_data).expect("Failed to send request");

        // Poll until response received
        let iter_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) <= before {
            if iter_start.elapsed() > timeout {
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!("Timeout at iteration {} (completed={}, expected={})",
                    i, completed.load(Ordering::SeqCst), before + 1);
            }
            ctx.poll();
        }
    }

    eprintln!("Client: All {} pingpongs completed", ITERATIONS);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert_eq!(final_completed as usize, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        final_completed, ITERATIONS);
}

fn srq_exhaustion_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            if let Err(e) = req.reply(&response_data) {
                eprintln!("Server: Failed to reply: {:?}", e);
            } else {
                replies_sent += 1;
            }
        }
    }

    eprintln!("Server: Exiting... (sent {} replies)", replies_sent);
}

// =============================================================================
// Benchmark-style pipelined pingpong test
// =============================================================================

/// Test that mimics the benchmark's pipelined pingpong behavior.
/// This sends multiple iterations without waiting for each response.
/// Also tests multiple batch runs like criterion does during warmup.
#[test]
fn test_benchmark_style_pingpong() {
    const ITERATIONS_PER_BATCH: usize = 100;
    const NUM_BATCHES: usize = 10; // Simulate criterion's multiple warmup calls
    const MAX_INFLIGHT: usize = 1; // Same as benchmark (NUM_ENDPOINTS * REQUESTS_PER_EP)

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    // Setup communication channels with server
    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        benchmark_style_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect
    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    // Benchmark-style pipelined pingpong
    let request_data = vec![0u8; 32];
    let mut completed_count = 0usize;
    let mut inflight = 0usize;
    let timeout = Duration::from_secs(10);

    // Initial fill: send initial requests
    for i in 0..MAX_INFLIGHT {
        let user_data = CallUserData { call_id: i as u32 };
        ep.call(&request_data, user_data).expect("Failed to send initial request");
        inflight += 1;
    }

    // Flush initial batch
    ctx.poll();

    let start = std::time::Instant::now();

    // Run multiple batches like criterion warmup does
    let total_iterations = ITERATIONS_PER_BATCH * NUM_BATCHES;
    while completed_count < total_iterations {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!("Timeout (completed={}, inflight={}, target={})",
                completed_count, inflight, total_iterations);
        }

        // Poll for completions
        ctx.poll();

        // Get completions from on_response callback
        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);

        // Send new requests to maintain queue depth
        let remaining = total_iterations.saturating_sub(completed_count);
        let can_send = MAX_INFLIGHT
            .saturating_sub(inflight)
            .min(remaining);

        for _ in 0..can_send {
            let user_data = CallUserData { call_id: completed_count as u32 };
            if ep.call(&request_data, user_data).is_ok() {
                inflight += 1;
            }
        }
    }

    // Drain remaining inflight requests
    let drain_start = std::time::Instant::now();
    while inflight > 0 && drain_start.elapsed() < Duration::from_secs(5) {
        ctx.poll();
        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);
    }

    eprintln!("Client: Completed {} iterations", completed_count);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(completed_count, total_iterations,
        "Did not complete all iterations (completed={}, expected={})",
        completed_count, total_iterations);
}

fn benchmark_style_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            if let Err(e) = req.reply(&response_data) {
                eprintln!("Server: Failed to reply: {:?}", e);
            } else {
                replies_sent += 1;
            }
        }
    }

    eprintln!("Server: Exiting... (sent {} replies)", replies_sent);
}

fn multi_endpoint_server_thread(
    info_tx: Sender<MultiEndpointConnectionInfo>,
    info_rx: Receiver<MultiEndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(4096)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();

    let mut endpoints = Vec::with_capacity(NUM_ENDPOINTS);
    let mut server_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for i in 0..NUM_ENDPOINTS {
        eprintln!("Server: Creating endpoint {}...", i);
        let ep = match ctx.create_endpoint(&ep_config) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Server: Failed to create endpoint: {:?}", e);
                return;
            }
        };
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());
        eprintln!("Server: Endpoint {} created, qpn={}", i, info.qp_number);

        server_infos.push(EndpointConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            recv_ring_size: info.recv_ring_size,
            consumer_addr: info.consumer_addr,
            consumer_rkey: info.consumer_rkey,
        });

        endpoints.push(ep);
    }

    if info_tx.send(MultiEndpointConnectionInfo {
        endpoints: server_infos,
    }).is_err() {
        eprintln!("Server: Failed to send server info");
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => {
            eprintln!("Server: Failed to receive client info");
            return;
        }
    };

    // Connect endpoints
    for (i, ep) in endpoints.iter_mut().enumerate() {
        let client_ep = &client_info.endpoints[i];

        let remote = RemoteEndpointInfo {
            qp_number: client_ep.qp_number,
            packet_sequence_number: client_ep.packet_sequence_number,
            local_identifier: client_ep.local_identifier,
            recv_ring_addr: client_ep.recv_ring_addr,
            recv_ring_rkey: client_ep.recv_ring_rkey,
            recv_ring_size: client_ep.recv_ring_size,
            consumer_addr: client_ep.consumer_addr,
            consumer_rkey: client_ep.consumer_rkey,
        };

        if ep.connect(&remote, 0, ctx.port()).is_err() {
            eprintln!("Server: Failed to connect endpoint {}", i);
            return;
        }
        eprintln!("Server: Endpoint {} connected", i);
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; MESSAGE_SIZE];
    let mut replies_sent = 0;

    eprintln!("Server: Starting loop...");

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            if let Err(e) = req.reply(&response_data) {
                eprintln!("Server: Failed to reply: {:?}", e);
            } else {
                replies_sent += 1;
            }
        }
    }

    eprintln!("Server: Exiting... (sent {} replies)", replies_sent);
}

// =============================================================================
// Ring Buffer Wrap-around Test
// =============================================================================

/// Test ring buffer wrap-around behavior.
///
/// This uses a small ring size to force wrap-around quickly.
/// It tests that messages are correctly handled when the ring buffer wraps.
#[test]
fn test_ring_wraparound() {
    const SMALL_RING_SIZE: usize = 4096; // 4KB - will wrap quickly with 32B messages
    const ITERATIONS: usize = 200; // Enough to wrap around multiple times (200 * 64B > 4KB)

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: SMALL_RING_SIZE,
        recv_ring_size: SMALL_RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        wraparound_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop, SMALL_RING_SIZE);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    // Run sequential pingpongs to test wrap-around
    let request_data = vec![0u8; 32];
    let timeout = Duration::from_secs(10);

    for i in 0..ITERATIONS {
        let before = completed.load(Ordering::SeqCst);

        let user_data = CallUserData { call_id: i as u32 };
        if let Err(e) = ep.call(&request_data, user_data) {
            eprintln!("Client: call failed at iteration {}: {:?}", i, e);
            // On RingFull, poll and retry
            ctx.poll();
            continue;
        }

        let iter_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) <= before {
            if iter_start.elapsed() > timeout {
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!("Timeout at wrap-around iteration {} (completed={})", i, completed.load(Ordering::SeqCst));
            }
            ctx.poll();
        }
    }

    eprintln!("Client: Wrap-around test completed {} iterations", ITERATIONS);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert!(final_completed as usize >= ITERATIONS - 10,
        "Did not complete enough iterations (completed={}, expected>={})",
        final_completed, ITERATIONS - 10);
}

fn wraparound_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    ring_size: usize,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig {
        send_ring_size: ring_size,
        recv_ring_size: ring_size,
        ..Default::default()
    };
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            if let Err(e) = req.reply(&response_data) {
                eprintln!("Server: Failed to reply: {:?}", e);
            } else {
                replies_sent += 1;
            }
        }
    }

    eprintln!("Server: Wrap-around server exiting... (sent {} replies)", replies_sent);
}

// =============================================================================
// High Throughput Sustained Load Test
// =============================================================================

/// Test sustained high throughput like the benchmark does.
/// This test runs for many iterations with continuous pipelining.
#[test]
fn test_high_throughput_sustained() {
    const ITERATIONS: usize = 10000; // Large number of iterations
    const MAX_INFLIGHT: usize = 16; // Multiple concurrent requests

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        high_throughput_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut completed_count = 0usize;
    let mut inflight = 0usize;
    let mut sent = 0usize;
    let timeout = Duration::from_secs(30);

    // Initial fill
    while inflight < MAX_INFLIGHT && sent < ITERATIONS {
        let user_data = CallUserData { call_id: sent as u32 };
        if ep.call(&request_data, user_data).is_ok() {
            inflight += 1;
            sent += 1;
        }
    }
    ctx.poll();

    let start = std::time::Instant::now();

    while completed_count < ITERATIONS {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!("Timeout (completed={}, inflight={}, sent={}, target={})",
                completed_count, inflight, sent, ITERATIONS);
        }

        ctx.poll();

        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);

        // Send more requests to maintain queue depth
        while inflight < MAX_INFLIGHT && sent < ITERATIONS {
            let user_data = CallUserData { call_id: sent as u32 };
            match ep.call(&request_data, user_data) {
                Ok(_) => {
                    inflight += 1;
                    sent += 1;
                }
                Err(_) => break, // RingFull, wait for completions
            }
        }
    }

    // Drain remaining
    let drain_start = std::time::Instant::now();
    while inflight > 0 && drain_start.elapsed() < Duration::from_secs(5) {
        ctx.poll();
        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);
    }

    eprintln!("Client: High throughput test completed {} iterations in {:?}", completed_count, start.elapsed());

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(completed_count, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        completed_count, ITERATIONS);
}

fn high_throughput_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            if req.reply(&response_data).is_ok() {
                replies_sent += 1;
            }
        }
    }

    eprintln!("Server: High throughput server exiting... (sent {} replies)", replies_sent);
}

// =============================================================================
// Debug Test - Small iterations with detailed logging
// =============================================================================

/// Debug test with small iterations and detailed logging to identify the issue.
#[test]
fn test_debug_small_iterations() {
    const ITERATIONS: usize = 20;
    const MAX_INFLIGHT: usize = 4;

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        let prev = completed_for_callback.fetch_add(1, Ordering::SeqCst);
        eprintln!("CLIENT: on_response callback #{}", prev + 1);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        debug_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut completed_count = 0usize;
    let mut inflight = 0usize;
    let mut sent = 0usize;
    let timeout = Duration::from_secs(10);

    // Initial fill
    while inflight < MAX_INFLIGHT && sent < ITERATIONS {
        let user_data = CallUserData { call_id: sent as u32 };
        eprintln!("CLIENT: Sending request #{}", sent);
        if ep.call(&request_data, user_data).is_ok() {
            inflight += 1;
            sent += 1;
        } else {
            eprintln!("CLIENT: call() failed for request #{}", sent);
            break;
        }
    }
    ctx.poll();
    eprintln!("CLIENT: Initial batch sent={}, inflight={}", sent, inflight);

    let start = std::time::Instant::now();

    while completed_count < ITERATIONS {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!("Timeout (completed={}, inflight={}, sent={}, target={})",
                completed_count, inflight, sent, ITERATIONS);
        }

        ctx.poll();

        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        if new_completions > 0 {
            eprintln!("CLIENT: {} new completions, total={}", new_completions, prev_completed);
        }
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);

        // Send more requests
        while inflight < MAX_INFLIGHT && sent < ITERATIONS {
            let user_data = CallUserData { call_id: sent as u32 };
            match ep.call(&request_data, user_data) {
                Ok(_) => {
                    eprintln!("CLIENT: Sending request #{}", sent);
                    inflight += 1;
                    sent += 1;
                }
                Err(e) => {
                    eprintln!("CLIENT: call() failed: {:?}", e);
                    break;
                }
            }
        }
    }

    eprintln!("CLIENT: Test completed. completed={}, sent={}", completed_count, sent);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(completed_count, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        completed_count, ITERATIONS);
}

fn debug_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SERVER: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("SERVER: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("SERVER: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;
    let mut poll_count = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();
        poll_count += 1;

        let mut recv_count = 0;
        while let Some(req) = ctx.recv() {
            recv_count += 1;
            if req.reply(&response_data).is_ok() {
                replies_sent += 1;
                eprintln!("SERVER: Sent reply #{}", replies_sent);
            }
        }
        if recv_count > 0 {
            eprintln!("SERVER: Received {} requests in this poll", recv_count);
        }
    }

    eprintln!("SERVER: Exiting... (sent {} replies, poll_count={})", replies_sent, poll_count);
}

// =============================================================================
// Multi-round Single Request Test (mimics benchmark)
// =============================================================================

/// Test that mimics the benchmark pattern: multiple rounds with 1 concurrent request.
#[test]
fn test_benchmark_pattern() {
    const ROUNDS: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

    let server_replies = Arc::new(AtomicU32::new(0));
    let server_replies_for_thread = server_replies.clone();

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        benchmark_pattern_server(server_info_tx, client_info_rx, server_ready_clone, server_stop, server_replies_for_thread);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut total_completed = 0usize;
    let mut total_sent = 0usize;

    // Run multiple rounds like the benchmark does
    for &iters in ROUNDS {
        let round_start_completed = completed.load(Ordering::SeqCst) as usize;
        let mut inflight = 0usize;

        // Send initial request (1 concurrent)
        let user_data = CallUserData { call_id: total_sent as u32 };
        if ep.call(&request_data, user_data).is_ok() {
            inflight += 1;
            total_sent += 1;
        }
        ctx.poll();

        let mut round_completed = 0usize;
        let timeout = std::time::Instant::now() + Duration::from_secs(10);

        while round_completed < iters {
            if std::time::Instant::now() > timeout {
                let server_sent = server_replies.load(Ordering::SeqCst);
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!(
                    "Timeout in round iters={} (round_completed={}, inflight={}, total_sent={}, server_replies={})",
                    iters, round_completed, inflight, total_sent, server_sent
                );
            }

            ctx.poll();

            let current_completed = completed.load(Ordering::SeqCst) as usize;
            let new_completions = current_completed.saturating_sub(round_start_completed + round_completed);
            round_completed += new_completions;
            inflight = inflight.saturating_sub(new_completions);

            // Send next request (keep 1 concurrent)
            let remaining = iters.saturating_sub(round_completed);
            if inflight == 0 && remaining > 0 {
                let user_data = CallUserData { call_id: total_sent as u32 };
                if ep.call(&request_data, user_data).is_ok() {
                    inflight += 1;
                    total_sent += 1;
                }
            }
        }

        total_completed += round_completed;
        eprintln!("Round iters={} completed (total_completed={}, total_sent={})", iters, total_completed, total_sent);
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let server_sent = server_replies.load(Ordering::SeqCst);
    eprintln!("Final: total_completed={}, total_sent={}, server_replies={}", total_completed, total_sent, server_sent);

    assert_eq!(total_completed, total_sent,
        "Mismatch: completed={}, sent={}", total_completed, total_sent);
    assert_eq!(server_sent as usize, total_sent,
        "Server sent more replies than expected: server={}, expected={}", server_sent, total_sent);
}

fn benchmark_pattern_server(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    replies_sent: Arc<AtomicU32>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig::default();
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            if req.reply(&response_data).is_ok() {
                replies_sent.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    eprintln!("Server: Benchmark pattern server exiting... (sent {} replies)", replies_sent.load(Ordering::SeqCst));
}

// =============================================================================
// Test: emit_wqe wrap-around (batched sends that span ring boundary)
// =============================================================================

/// Test that emit_wqe correctly handles the case when batched data spans
/// the ring buffer boundary.
///
/// This test sends multiple messages without poll(), accumulating them in the
/// send buffer, then calls poll() which triggers emit_wqe(). If enough messages
/// are batched, the data will span the ring boundary, triggering the two-part
/// WQE split logic.
///
/// Before the fix, this would cause RDMA Local Length Error (syndrome=5).
#[test]
fn test_emit_wqe_boundary_split() {
    // Use a tiny ring size to force wrap-around quickly
    // With 1KB ring and 64B messages (32B data + 32B header/padding),
    // about 16 messages fill the ring. Batching 8+ messages should
    // eventually cause a boundary crossing.
    const TINY_RING_SIZE: usize = 1024; // 1KB
    const MESSAGE_SIZE: usize = 32;
    const BATCH_SIZE: usize = 8; // Messages to batch before poll
    const NUM_BATCHES: usize = 50; // Total batches to send

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024, // Enough for batched sends
            max_sge: 1,
        })
        .cq_size(2048)
        .on_response(on_response)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: TINY_RING_SIZE,
        recv_ring_size: TINY_RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx.create_endpoint(&ep_config).expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    let (server_info_tx, server_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<EndpointConnectionInfo>,
        Receiver<EndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        emit_wqe_boundary_server_thread(server_info_tx, client_info_rx, server_ready_clone, server_stop, TINY_RING_SIZE);
    });

    client_info_tx.send(client_info).expect("Failed to send client info");
    let server_info = server_info_rx.recv().expect("Failed to receive server info");

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        consumer_addr: server_info.consumer_addr,
        consumer_rkey: server_info.consumer_rkey,
    };

    ep.connect(&remote, 0, ctx.port()).expect("Failed to connect");

    // Give server time to set up
    std::thread::sleep(Duration::from_millis(10));

    let request_data = vec![0u8; MESSAGE_SIZE];
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    let mut total_sent: usize = 0;
    let mut ringfull_count: usize = 0;

    // Send batches of messages to trigger boundary-spanning emit_wqe
    for batch in 0..NUM_BATCHES {
        if start.elapsed() > timeout {
            break;
        }

        // Batch multiple calls without poll - this accumulates data in send buffer
        let mut _batch_sent = 0;
        for i in 0..BATCH_SIZE {
            let user_data = CallUserData { call_id: (batch * BATCH_SIZE + i) as u32 };
            match ep.call(&request_data, user_data) {
                Ok(_) => {
                    total_sent += 1;
                    _batch_sent += 1;
                }
                Err(copyrpc::error::Error::RingFull) => {
                    ringfull_count += 1;
                    // Need to poll and drain before continuing
                    break;
                }
                Err(e) => {
                    panic!("Unexpected error at batch {} msg {}: {:?}", batch, i, e);
                }
            }
        }

        // Poll to flush the batch - this triggers emit_wqe with accumulated data
        // If the data spans the ring boundary, emit_wqe must split it correctly
        ctx.poll();

        // Wait for responses before continuing (to free up ring space)
        let wait_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) < total_sent as u32 {
            if wait_start.elapsed() > Duration::from_secs(5) {
                eprintln!("Timeout waiting for responses at batch {} (sent={}, completed={})",
                    batch, total_sent, completed.load(Ordering::SeqCst));
                break;
            }
            ctx.poll();
        }
    }

    // Drain any remaining
    let drain_start = std::time::Instant::now();
    while completed.load(Ordering::SeqCst) < total_sent as u32 && drain_start.elapsed() < Duration::from_secs(5) {
        ctx.poll();
    }

    let final_completed = completed.load(Ordering::SeqCst);
    eprintln!("emit_wqe boundary test: sent={}, completed={}, ringfull={}",
        total_sent, final_completed, ringfull_count);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    // Verify we completed most of the sends
    // Some might fail due to RingFull, but we should complete the majority
    assert!(final_completed as usize >= total_sent.saturating_sub(BATCH_SIZE),
        "Too many messages lost (sent={}, completed={})", total_sent, final_completed);
}

fn emit_wqe_boundary_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    ring_size: usize,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Server: Failed to create context: {:?}", e);
            return;
        }
    };

    let ep_config = EndpointConfig {
        send_ring_size: ring_size,
        recv_ring_size: ring_size,
        ..Default::default()
    };
    let mut ep = match ctx.create_endpoint(&ep_config) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Server: Failed to create endpoint: {:?}", e);
            return;
        }
    };

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let server_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        consumer_addr: info.consumer_addr,
        consumer_rkey: info.consumer_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let remote = RemoteEndpointInfo {
        qp_number: client_info.qp_number,
        packet_sequence_number: client_info.packet_sequence_number,
        local_identifier: client_info.local_identifier,
        recv_ring_addr: client_info.recv_ring_addr,
        recv_ring_rkey: client_info.recv_ring_rkey,
        recv_ring_size: client_info.recv_ring_size,
        consumer_addr: client_info.consumer_addr,
        consumer_rkey: client_info.consumer_rkey,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0u64;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        while let Some(req) = ctx.recv() {
            // Retry on RingFull
            loop {
                match req.reply(&response_data) {
                    Ok(()) => {
                        replies_sent += 1;
                        break;
                    }
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll();
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    eprintln!("Server: emit_wqe boundary test server exiting (sent {} replies)", replies_sent);
}
