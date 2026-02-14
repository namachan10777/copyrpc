//! Integration tests for copyrpc.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
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
    initial_credit: u64,
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

    let result: Result<Context<CallUserData>, _> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build();

    assert!(
        result.is_ok(),
        "Failed to create context: {:?}",
        result.err()
    );
}

// =============================================================================
// Endpoint Creation Test
// =============================================================================

#[test]
fn test_endpoint_creation() {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        eprintln!("Client: on_response called!");
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Send a request
    let request_data = vec![0u8; 32];
    let user_data = CallUserData { call_id: 1 };

    eprintln!("Sending request...");
    ep.call(&request_data, user_data, 64)
        .expect("Failed to send request");

    // Poll for response with timeout
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let mut poll_count = 0;

    while start.elapsed() < timeout {
        ctx.poll(&mut on_response);
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
    assert!(
        response_count > 0,
        "Did not receive response within timeout (completed={})",
        response_count
    );
}

fn server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];

    eprintln!("Server: Starting loop...");

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256, // Enough for all endpoints' initial recvs
            max_sge: 1,
        })
        .cq_size(4096)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();

    let mut endpoints = Vec::with_capacity(NUM_ENDPOINTS);
    let mut client_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for i in 0..NUM_ENDPOINTS {
        eprintln!("Client: Creating endpoint {}...", i);
        let ep = ctx
            .create_endpoint(&ep_config)
            .expect("Failed to create endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());
        eprintln!("Client: Endpoint {} created, qpn={}", i, info.qp_number);

        client_infos.push(EndpointConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            recv_ring_size: info.recv_ring_size,
            initial_credit: info.initial_credit,
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
        multi_endpoint_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx
        .send(MultiEndpointConnectionInfo {
            endpoints: client_infos,
        })
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
            initial_credit: server_ep.initial_credit,
        };

        ep.connect(&remote, 0, ctx.port())
            .expect("Failed to connect");
        eprintln!("Client: Endpoint {} connected", i);
    }

    // Send requests on all endpoints
    let request_data = vec![0u8; MESSAGE_SIZE];
    let mut total_sent = 0;

    for (ep_id, ep) in endpoints.iter().enumerate() {
        for slot_id in 0..REQUESTS_PER_EP {
            let user_data = CallUserData {
                call_id: (ep_id * REQUESTS_PER_EP + slot_id) as u32,
            };
            ep.call(&request_data, user_data, 64)
                .expect("Failed to send request");
            total_sent += 1;
        }
    }
    eprintln!("Client: Sent {} requests", total_sent);

    // Flush initial batch
    ctx.poll(&mut on_response);

    // Poll for responses with timeout
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);

    while start.elapsed() < timeout {
        ctx.poll(&mut on_response);

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
    eprintln!(
        "Client: Final completed count: {}/{}",
        final_completed, total_sent
    );

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(
        final_completed as usize, total_sent,
        "Did not receive all responses within timeout (completed={}, expected={})",
        final_completed, total_sent
    );
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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        srq_exhaustion_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Run multiple pingpong iterations
    let request_data = vec![0u8; 32];
    let timeout = Duration::from_secs(5);

    for i in 0..ITERATIONS {
        let before = completed.load(Ordering::SeqCst);

        // Send request
        let user_data = CallUserData { call_id: i as u32 };
        ep.call(&request_data, user_data, 64)
            .expect("Failed to send request");

        // Poll until response received
        let iter_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) <= before {
            if iter_start.elapsed() > timeout {
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!(
                    "Timeout at iteration {} (completed={}, expected={})",
                    i,
                    completed.load(Ordering::SeqCst),
                    before + 1
                );
            }
            ctx.poll(&mut on_response);
        }
    }

    eprintln!("Client: All {} pingpongs completed", ITERATIONS);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert_eq!(
        final_completed as usize, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        final_completed, ITERATIONS
    );
}

fn srq_exhaustion_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        benchmark_style_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Benchmark-style pipelined pingpong
    let request_data = vec![0u8; 32];
    let mut completed_count = 0usize;
    let mut inflight = 0usize;
    let timeout = Duration::from_secs(10);

    // Initial fill: send initial requests
    for i in 0..MAX_INFLIGHT {
        let user_data = CallUserData { call_id: i as u32 };
        ep.call(&request_data, user_data, 64)
            .expect("Failed to send initial request");
        inflight += 1;
    }

    // Flush initial batch
    ctx.poll(&mut on_response);

    let start = std::time::Instant::now();

    // Run multiple batches like criterion warmup does
    let total_iterations = ITERATIONS_PER_BATCH * NUM_BATCHES;
    while completed_count < total_iterations {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout (completed={}, inflight={}, target={})",
                completed_count, inflight, total_iterations
            );
        }

        // Poll for completions
        ctx.poll(&mut on_response);

        // Get completions from on_response callback
        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);

        // Send new requests to maintain queue depth
        let remaining = total_iterations.saturating_sub(completed_count);
        let can_send = MAX_INFLIGHT.saturating_sub(inflight).min(remaining);

        for _ in 0..can_send {
            let user_data = CallUserData {
                call_id: completed_count as u32,
            };
            if ep.call(&request_data, user_data, 64).is_ok() {
                inflight += 1;
            }
        }
    }

    // Drain remaining inflight requests
    let drain_start = std::time::Instant::now();
    while inflight > 0 && drain_start.elapsed() < Duration::from_secs(5) {
        ctx.poll(&mut on_response);
        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);
    }

    eprintln!("Client: Completed {} iterations", completed_count);

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(
        completed_count, total_iterations,
        "Did not complete all iterations (completed={}, expected={})",
        completed_count, total_iterations
    );
}

fn benchmark_style_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

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

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(4096)
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
            initial_credit: info.initial_credit,
        });

        endpoints.push(ep);
    }

    if info_tx
        .send(MultiEndpointConnectionInfo {
            endpoints: server_infos,
        })
        .is_err()
    {
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
            initial_credit: client_ep.initial_credit,
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
        ctx.poll(|_, _| {});

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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: SMALL_RING_SIZE,
        recv_ring_size: SMALL_RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        wraparound_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            SMALL_RING_SIZE,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Run sequential pingpongs to test wrap-around
    let request_data = vec![0u8; 32];
    let timeout = Duration::from_secs(10);

    for i in 0..ITERATIONS {
        let before = completed.load(Ordering::SeqCst);

        let user_data = CallUserData { call_id: i as u32 };
        if let Err(e) = ep.call(&request_data, user_data, 64) {
            eprintln!("Client: call failed at iteration {}: {:?}", i, e);
            // On RingFull, poll and retry
            ctx.poll(&mut on_response);
            continue;
        }

        let iter_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) <= before {
            if iter_start.elapsed() > timeout {
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!(
                    "Timeout at wrap-around iteration {} (completed={})",
                    i,
                    completed.load(Ordering::SeqCst)
                );
            }
            ctx.poll(&mut on_response);
        }
    }

    eprintln!(
        "Client: Wrap-around test completed {} iterations",
        ITERATIONS
    );

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert!(
        final_completed as usize >= ITERATIONS - 10,
        "Did not complete enough iterations (completed={}, expected>={})",
        final_completed,
        ITERATIONS - 10
    );
}

fn wraparound_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    ring_size: usize,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

        while let Some(req) = ctx.recv() {
            if let Err(e) = req.reply(&response_data) {
                eprintln!("Server: Failed to reply: {:?}", e);
            } else {
                replies_sent += 1;
            }
        }
    }

    eprintln!(
        "Server: Wrap-around server exiting... (sent {} replies)",
        replies_sent
    );
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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        high_throughput_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut completed_count = 0usize;
    let mut inflight = 0usize;
    let mut sent = 0usize;
    let timeout = Duration::from_secs(30);

    // Initial fill
    while inflight < MAX_INFLIGHT && sent < ITERATIONS {
        let user_data = CallUserData {
            call_id: sent as u32,
        };
        if ep.call(&request_data, user_data, 64).is_ok() {
            inflight += 1;
            sent += 1;
        }
    }
    ctx.poll(&mut on_response);

    let start = std::time::Instant::now();

    while completed_count < ITERATIONS {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout (completed={}, inflight={}, sent={}, target={})",
                completed_count, inflight, sent, ITERATIONS
            );
        }

        ctx.poll(&mut on_response);

        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);

        // Send more requests to maintain queue depth
        while inflight < MAX_INFLIGHT && sent < ITERATIONS {
            let user_data = CallUserData {
                call_id: sent as u32,
            };
            match ep.call(&request_data, user_data, 64) {
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
        ctx.poll(&mut on_response);
        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);
    }

    eprintln!(
        "Client: High throughput test completed {} iterations in {:?}",
        completed_count,
        start.elapsed()
    );

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(
        completed_count, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        completed_count, ITERATIONS
    );
}

fn high_throughput_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

        while let Some(req) = ctx.recv() {
            if req.reply(&response_data).is_ok() {
                replies_sent += 1;
            }
        }
    }

    eprintln!(
        "Server: High throughput server exiting... (sent {} replies)",
        replies_sent
    );
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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        let prev = completed_for_callback.fetch_add(1, Ordering::SeqCst);
        eprintln!("CLIENT: on_response callback #{}", prev + 1);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        debug_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut completed_count = 0usize;
    let mut inflight = 0usize;
    let mut sent = 0usize;
    let timeout = Duration::from_secs(10);

    // Initial fill
    while inflight < MAX_INFLIGHT && sent < ITERATIONS {
        let user_data = CallUserData {
            call_id: sent as u32,
        };
        eprintln!("CLIENT: Sending request #{}", sent);
        if ep.call(&request_data, user_data, 64).is_ok() {
            inflight += 1;
            sent += 1;
        } else {
            eprintln!("CLIENT: call() failed for request #{}", sent);
            break;
        }
    }
    ctx.poll(&mut on_response);
    eprintln!("CLIENT: Initial batch sent={}, inflight={}", sent, inflight);

    let start = std::time::Instant::now();

    while completed_count < ITERATIONS {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout (completed={}, inflight={}, sent={}, target={})",
                completed_count, inflight, sent, ITERATIONS
            );
        }

        ctx.poll(&mut on_response);

        let prev_completed = completed.load(Ordering::SeqCst) as usize;
        let new_completions = prev_completed.saturating_sub(completed_count);
        if new_completions > 0 {
            eprintln!(
                "CLIENT: {} new completions, total={}",
                new_completions, prev_completed
            );
        }
        completed_count = prev_completed;
        inflight = inflight.saturating_sub(new_completions);

        // Send more requests
        while inflight < MAX_INFLIGHT && sent < ITERATIONS {
            let user_data = CallUserData {
                call_id: sent as u32,
            };
            match ep.call(&request_data, user_data, 64) {
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

    eprintln!(
        "CLIENT: Test completed. completed={}, sent={}",
        completed_count, sent
    );

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(
        completed_count, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        completed_count, ITERATIONS
    );
}

fn debug_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
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
        ctx.poll(|_, _| {});
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

    eprintln!(
        "SERVER: Exiting... (sent {} replies, poll_count={})",
        replies_sent, poll_count
    );
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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig::default();
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        benchmark_pattern_server(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            server_replies_for_thread,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut total_completed = 0usize;
    let mut total_sent = 0usize;

    // Run multiple rounds like the benchmark does
    for &iters in ROUNDS {
        let round_start_completed = completed.load(Ordering::SeqCst) as usize;
        let mut inflight = 0usize;

        // Send initial request (1 concurrent)
        let user_data = CallUserData {
            call_id: total_sent as u32,
        };
        if ep.call(&request_data, user_data, 64).is_ok() {
            inflight += 1;
            total_sent += 1;
        }
        ctx.poll(&mut on_response);

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

            ctx.poll(&mut on_response);

            let current_completed = completed.load(Ordering::SeqCst) as usize;
            let new_completions =
                current_completed.saturating_sub(round_start_completed + round_completed);
            round_completed += new_completions;
            inflight = inflight.saturating_sub(new_completions);

            // Send next request (keep 1 concurrent)
            let remaining = iters.saturating_sub(round_completed);
            if inflight == 0 && remaining > 0 {
                let user_data = CallUserData {
                    call_id: total_sent as u32,
                };
                if ep.call(&request_data, user_data, 64).is_ok() {
                    inflight += 1;
                    total_sent += 1;
                }
            }
        }

        total_completed += round_completed;
        eprintln!(
            "Round iters={} completed (total_completed={}, total_sent={})",
            iters, total_completed, total_sent
        );
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let server_sent = server_replies.load(Ordering::SeqCst);
    eprintln!(
        "Final: total_completed={}, total_sent={}, server_replies={}",
        total_completed, total_sent, server_sent
    );

    assert_eq!(
        total_completed, total_sent,
        "Mismatch: completed={}, sent={}",
        total_completed, total_sent
    );
    assert_eq!(
        server_sent as usize, total_sent,
        "Server sent more replies than expected: server={}, expected={}",
        server_sent, total_sent
    );
}

fn benchmark_pattern_server(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    replies_sent: Arc<AtomicU32>,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

        while let Some(req) = ctx.recv() {
            if req.reply(&response_data).is_ok() {
                replies_sent.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    eprintln!(
        "Server: Benchmark pattern server exiting... (sent {} replies)",
        replies_sent.load(Ordering::SeqCst)
    );
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

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024, // Enough for batched sends
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: TINY_RING_SIZE,
        recv_ring_size: TINY_RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        emit_wqe_boundary_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            TINY_RING_SIZE,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

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
            let user_data = CallUserData {
                call_id: (batch * BATCH_SIZE + i) as u32,
            };
            match ep.call(&request_data, user_data, 64) {
                Ok(_) => {
                    total_sent += 1;
                    _batch_sent += 1;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => {
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
        ctx.poll(&mut on_response);

        // Wait for responses before continuing (to free up ring space)
        let wait_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) < total_sent as u32 {
            if wait_start.elapsed() > Duration::from_secs(5) {
                eprintln!(
                    "Timeout waiting for responses at batch {} (sent={}, completed={})",
                    batch,
                    total_sent,
                    completed.load(Ordering::SeqCst)
                );
                break;
            }
            ctx.poll(&mut on_response);
        }
    }

    // Drain any remaining
    let drain_start = std::time::Instant::now();
    while completed.load(Ordering::SeqCst) < total_sent as u32
        && drain_start.elapsed() < Duration::from_secs(5)
    {
        ctx.poll(&mut on_response);
    }

    let final_completed = completed.load(Ordering::SeqCst);
    eprintln!(
        "emit_wqe boundary test: sent={}, completed={}, ringfull={}",
        total_sent, final_completed, ringfull_count
    );

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    // Verify we completed most of the sends
    // Some might fail due to RingFull, but we should complete the majority
    assert!(
        final_completed as usize >= total_sent.saturating_sub(BATCH_SIZE),
        "Too many messages lost (sent={}, completed={})",
        total_sent,
        final_completed
    );
}

fn emit_wqe_boundary_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    ring_size: usize,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0u64;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

        while let Some(req) = ctx.recv() {
            // Retry on RingFull
            loop {
                match req.reply(&response_data) {
                    Ok(()) => {
                        replies_sent += 1;
                        break;
                    }
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll(|_, _| {});
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    eprintln!(
        "Server: emit_wqe boundary test server exiting (sent {} replies)",
        replies_sent
    );
}

// =============================================================================
// Credit-Based Flow Control Tests
// =============================================================================

/// Generic server thread helper for credit-based flow control tests.
/// Reduces boilerplate by accepting ring size as parameter.
fn generic_server_thread(
    info_tx: Sender<EndpointConnectionInfo>,
    info_rx: Receiver<EndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    ring_size: usize,
) {
    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
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
        initial_credit: info.initial_credit,
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
        initial_credit: client_info.initial_credit,
    };

    if ep.connect(&remote, 0, ctx.port()).is_err() {
        eprintln!("Server: Failed to connect");
        return;
    }

    ready_signal.store(1, Ordering::Release);

    let response_data = vec![0u8; 32];
    let mut replies_sent = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|_, _| {});

        while let Some(req) = ctx.recv() {
            // Retry on RingFull
            loop {
                match req.reply(&response_data) {
                    Ok(()) => {
                        replies_sent += 1;
                        break;
                    }
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll(|_, _| {});
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    eprintln!("Server: Exiting (sent {} replies)", replies_sent);
}

// =============================================================================
// Test 1: Small ring with large response allowance
// =============================================================================

/// Test credit-based flow control with a small ring and large response_allowance.
/// Validates that 200 sequential pingpongs complete successfully.
#[test]
fn test_credit_small_ring_large_response() {
    const RING_SIZE: usize = 4096;
    const RESPONSE_ALLOWANCE: u64 = 512;
    const ITERATIONS: usize = 200;

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        generic_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            RING_SIZE,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Sequential pingpong with large response allowance
    let request_data = vec![0u8; 32];
    let timeout = Duration::from_secs(10);

    for i in 0..ITERATIONS {
        let before = completed.load(Ordering::SeqCst);
        let user_data = CallUserData { call_id: i as u32 };

        ep.call(&request_data, user_data, RESPONSE_ALLOWANCE)
            .expect("Failed to send request");

        let iter_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) <= before {
            if iter_start.elapsed() > timeout {
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!(
                    "Timeout at iteration {} (completed={})",
                    i,
                    completed.load(Ordering::SeqCst)
                );
            }
            ctx.poll(&mut on_response);
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert_eq!(
        final_completed as usize, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        final_completed, ITERATIONS
    );
}

// =============================================================================
// Test 2: Mixed response allowance sizes
// =============================================================================

/// Test credit-based flow control with varying response_allowance values.
/// Cycles through different allowances to validate dynamic behavior.
#[test]
fn test_credit_mixed_sizes() {
    const RING_SIZE: usize = 8192;
    const ITERATIONS: usize = 200;
    const ALLOWANCES: [u64; 4] = [32, 256, 512, 1024];

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        generic_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            RING_SIZE,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Sequential pingpong with cycling response allowances
    let request_data = vec![0u8; 32];
    let timeout = Duration::from_secs(10);

    for i in 0..ITERATIONS {
        let before = completed.load(Ordering::SeqCst);
        let user_data = CallUserData { call_id: i as u32 };
        let allowance = ALLOWANCES[i % ALLOWANCES.len()];

        ep.call(&request_data, user_data, allowance)
            .expect("Failed to send request");

        let iter_start = std::time::Instant::now();
        while completed.load(Ordering::SeqCst) <= before {
            if iter_start.elapsed() > timeout {
                stop_flag.store(true, Ordering::SeqCst);
                let _ = handle.join();
                panic!(
                    "Timeout at iteration {} (completed={})",
                    i,
                    completed.load(Ordering::SeqCst)
                );
            }
            ctx.poll(&mut on_response);
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert_eq!(
        final_completed as usize, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        final_completed, ITERATIONS
    );
}

// =============================================================================
// Test 3: Bidirectional stress test
// =============================================================================

/// Test credit-based flow control with bidirectional traffic.
/// Both client and server send calls to each other and reply to incoming requests.
#[test]
fn test_credit_bidirectional_stress() {
    const RING_SIZE: usize = 16384;
    const CALLS_PER_SIDE: usize = 500;
    const QUEUE_DEPTH: usize = 4;
    const RESPONSE_ALLOWANCE: u64 = 128;

    let client_completed = Arc::new(AtomicU32::new(0));
    let client_completed_for_callback = client_completed.clone();

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        client_completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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

    let server_completed = Arc::new(AtomicU32::new(0));
    let server_completed_clone = server_completed.clone();

    // Server thread that also sends calls
    let handle: JoinHandle<()> = thread::spawn(move || {
        let server_completed_for_callback = server_completed_clone.clone();
        let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
            server_completed_for_callback.fetch_add(1, Ordering::SeqCst);
        };

        let ctx: Context<CallUserData> = match ContextBuilder::new()
            .device_index(0)
            .port(1)
            .srq_config(SrqConfig {
                max_wr: 1024,
                max_sge: 1,
            })
            .cq_size(2048)
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Server: Failed to create context: {:?}", e);
                return;
            }
        };

        let ep_config = EndpointConfig {
            send_ring_size: RING_SIZE,
            recv_ring_size: RING_SIZE,
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
            initial_credit: info.initial_credit,
        };

        if server_info_tx.send(server_info).is_err() {
            return;
        }

        let client_info = match client_info_rx.recv() {
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
            initial_credit: client_info.initial_credit,
        };

        if ep.connect(&remote, 0, ctx.port()).is_err() {
            eprintln!("Server: Failed to connect");
            return;
        }

        server_ready_clone.store(1, Ordering::Release);

        let response_data = vec![0u8; 32];
        let request_data = vec![0u8; 32];
        let mut sent: usize = 0;

        while !server_stop.load(Ordering::Relaxed) {
            ctx.poll(&mut on_response);

            // Receive and reply to incoming requests
            while let Some(req) = ctx.recv() {
                loop {
                    match req.reply(&response_data) {
                        Ok(()) => break,
                        Err(copyrpc::error::Error::RingFull) => {
                            ctx.poll(&mut on_response);
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            }

            // Send calls to client
            let completed = server_completed_clone.load(Ordering::SeqCst) as usize;
            let mut inflight = sent.saturating_sub(completed);

            while inflight < QUEUE_DEPTH && sent < CALLS_PER_SIDE {
                let user_data = CallUserData {
                    call_id: sent as u32,
                };
                match ep.call(&request_data, user_data, RESPONSE_ALLOWANCE) {
                    Ok(_) => {
                        sent += 1;
                        inflight += 1;
                    }
                    Err(
                        copyrpc::error::CallError::RingFull(_)
                        | copyrpc::error::CallError::InsufficientCredit(_),
                    ) => {
                        break;
                    }
                    Err(_) => break,
                }
            }
        }

        // Drain remaining responses after stop flag
        let drain_start = std::time::Instant::now();
        while (server_completed_clone.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
            && drain_start.elapsed() < Duration::from_secs(5)
        {
            ctx.poll(&mut on_response);
            while let Some(req) = ctx.recv() {
                let _ = req.reply(&response_data);
            }
        }

        eprintln!(
            "Server: Exiting (sent {} calls, completed {})",
            sent,
            server_completed_clone.load(Ordering::SeqCst)
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Client sends calls and replies to incoming requests
    let request_data = vec![0u8; 32];
    let response_data = vec![0u8; 32];
    let mut sent = 0;
    let mut inflight = 0;
    let timeout = Duration::from_secs(20);
    let start = std::time::Instant::now();

    while sent < CALLS_PER_SIDE || inflight > 0 {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout (sent={}, inflight={}, completed={})",
                sent,
                inflight,
                client_completed.load(Ordering::SeqCst)
            );
        }

        ctx.poll(&mut on_response);

        // Receive and reply to incoming requests
        while let Some(req) = ctx.recv() {
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll(&mut on_response);
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }

        // Update inflight count
        let completed = client_completed.load(Ordering::SeqCst) as usize;
        inflight = sent.saturating_sub(completed);

        // Send more calls
        while inflight < QUEUE_DEPTH && sent < CALLS_PER_SIDE {
            let user_data = CallUserData {
                call_id: sent as u32,
            };
            match ep.call(&request_data, user_data, RESPONSE_ALLOWANCE) {
                Ok(_) => {
                    sent += 1;
                    inflight += 1;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => {
                    break;
                }
                Err(_) => break,
            }
        }
    }

    eprintln!(
        "Client: Completed all calls (sent={}, completed={})",
        sent,
        client_completed.load(Ordering::SeqCst)
    );

    // Keep polling and replying to flush remaining server-bound responses
    let drain_start = std::time::Instant::now();
    while drain_start.elapsed() < Duration::from_secs(5)
        && (server_completed.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
    {
        ctx.poll(&mut on_response);
        while let Some(req) = ctx.recv() {
            let _ = req.reply(&response_data);
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    assert_eq!(
        client_completed.load(Ordering::SeqCst) as usize,
        CALLS_PER_SIDE,
        "Client did not receive all responses"
    );
    assert_eq!(
        server_completed.load(Ordering::SeqCst) as usize,
        CALLS_PER_SIDE,
        "Server did not receive all responses"
    );
}

// =============================================================================
// Test 4: Deep queue with tiny ring
// =============================================================================

/// Test credit-based flow control with a small ring and deep queue.
/// Stresses credit exhaustion and recovery with multiple inflight requests.
#[test]
fn test_credit_deep_queue_tiny_ring() {
    const RING_SIZE: usize = 2048;
    const RESPONSE_ALLOWANCE: u64 = 256;
    const QUEUE_DEPTH: usize = 3;
    const ITERATIONS: usize = 200;

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        generic_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            RING_SIZE,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    // Pipelined sends with retry on RingFull/InsufficientCredit
    let request_data = vec![0u8; 32];
    let mut sent = 0;
    let mut inflight = 0;
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while sent < ITERATIONS || inflight > 0 {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout (sent={}, inflight={}, completed={})",
                sent,
                inflight,
                completed.load(Ordering::SeqCst)
            );
        }

        ctx.poll(&mut on_response);

        let current_completed = completed.load(Ordering::SeqCst) as usize;
        inflight = sent.saturating_sub(current_completed);

        // Send more requests to maintain queue depth
        while inflight < QUEUE_DEPTH && sent < ITERATIONS {
            let user_data = CallUserData {
                call_id: sent as u32,
            };
            match ep.call(&request_data, user_data, RESPONSE_ALLOWANCE) {
                Ok(_) => {
                    sent += 1;
                    inflight += 1;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => {
                    // Poll and retry
                    break;
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst);
    assert_eq!(
        final_completed as usize, ITERATIONS,
        "Did not complete all iterations (completed={}, expected={})",
        final_completed, ITERATIONS
    );
}

// =============================================================================
// Test 5: Credit exhaustion and recovery
// =============================================================================

/// Test credit-based flow control exhaustion and recovery.
/// Sends calls until InsufficientCredit occurs, then polls to regain credit.
#[test]
fn test_credit_exhaustion_recovery() {
    const RING_SIZE: usize = 8192;
    const RESPONSE_ALLOWANCE: u64 = 512;
    const MIN_COMPLETED: usize = 20;

    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_callback = completed.clone();

    let mut on_response = move |_user_data: CallUserData, _data: &[u8]| {
        completed_for_callback.fetch_add(1, Ordering::SeqCst);
    };

    let ctx: Context<CallUserData> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Failed to create client context");

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Failed to create endpoint");

    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let client_info = EndpointConnectionInfo {
        qp_number: info.qp_number,
        packet_sequence_number: 0,
        local_identifier: lid,
        recv_ring_addr: info.recv_ring_addr,
        recv_ring_rkey: info.recv_ring_rkey,
        recv_ring_size: info.recv_ring_size,
        initial_credit: info.initial_credit,
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
        generic_server_thread(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
            RING_SIZE,
        );
    });

    client_info_tx
        .send(client_info)
        .expect("Failed to send client info");
    let server_info = server_info_rx
        .recv()
        .expect("Failed to receive server info");

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
        initial_credit: server_info.initial_credit,
    };

    ep.connect(&remote, 0, ctx.port())
        .expect("Failed to connect");

    let request_data = vec![0u8; 32];
    let mut sent = 0;
    let mut insufficient_credit_count = 0;

    // Phase 1: Send until InsufficientCredit
    eprintln!("Phase 1: Sending until credit exhaustion...");
    loop {
        let user_data = CallUserData {
            call_id: sent as u32,
        };
        match ep.call(&request_data, user_data, RESPONSE_ALLOWANCE) {
            Ok(_) => {
                sent += 1;
            }
            Err(copyrpc::error::CallError::InsufficientCredit(_)) => {
                insufficient_credit_count += 1;
                eprintln!("InsufficientCredit at sent={}", sent);
                break;
            }
            Err(copyrpc::error::CallError::RingFull(_)) => {
                eprintln!("RingFull at sent={}", sent);
                break;
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    assert!(
        insufficient_credit_count > 0,
        "Did not observe InsufficientCredit"
    );

    // Phase 2: Poll to regain credit
    eprintln!("Phase 2: Polling to regain credit...");
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while completed.load(Ordering::SeqCst) < sent as u32 {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout waiting for responses (sent={}, completed={})",
                sent,
                completed.load(Ordering::SeqCst)
            );
        }
        ctx.poll(&mut on_response);
    }

    eprintln!("Phase 3: Sending more calls after recovery...");
    let recovery_start = sent;

    // Phase 3: Send more calls after recovery
    for i in 0..MIN_COMPLETED {
        let user_data = CallUserData {
            call_id: (recovery_start + i) as u32,
        };
        loop {
            match ep.call(&request_data, user_data, RESPONSE_ALLOWANCE) {
                Ok(_) => {
                    sent += 1;
                    break;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => {
                    ctx.poll(&mut on_response);
                    continue;
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    // Wait for all responses
    let start = std::time::Instant::now();
    while completed.load(Ordering::SeqCst) < sent as u32 {
        if start.elapsed() > timeout {
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "Timeout in final drain (sent={}, completed={})",
                sent,
                completed.load(Ordering::SeqCst)
            );
        }
        ctx.poll(&mut on_response);
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let final_completed = completed.load(Ordering::SeqCst) as usize;
    eprintln!(
        "Test completed: sent={}, completed={}, insufficient_credit_count={}",
        sent, final_completed, insufficient_credit_count
    );

    assert!(
        final_completed >= MIN_COMPLETED,
        "Did not complete enough iterations after recovery (completed={}, expected>={})",
        final_completed,
        MIN_COMPLETED
    );
}

// =============================================================================
// DC Transport: Bidirectional Stress Test
// =============================================================================

/// Connection info for DC transport endpoints.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct DcConnectionInfo {
    dct_number: u32,
    local_identifier: u16,
    _padding: u16,
    dc_key: u64,
    recv_ring_addr: u64,
    recv_ring_rkey: u32,
    endpoint_id: u32,
    recv_ring_size: u64,
    initial_credit: u64,
}

/// Minimal DC bidirectional test: both sides send calls AND reply to incoming requests.
/// Reproduces the benchkv 2-rank hang where DC transport freezes after ~17K operations.
#[test]
fn test_dc_bidirectional_stress() {
    use copyrpc::dc;
    use mlx5::dc::DciConfig;

    const RING_SIZE: usize = 4 * 1024 * 1024; // 4 MB (same as benchkv default)
    const CALLS_PER_SIDE: usize = 50_000;
    const QUEUE_DEPTH: usize = 4;

    let client_completed = Arc::new(AtomicU32::new(0));
    let client_completed_cb = client_completed.clone();

    let (server_info_tx, server_info_rx) = mpsc::channel::<DcConnectionInfo>();
    let (client_info_tx, client_info_rx) = mpsc::channel::<DcConnectionInfo>();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();
    let server_completed = Arc::new(AtomicU32::new(0));
    let server_completed_clone = server_completed.clone();

    // Server thread
    let handle: JoinHandle<()> = thread::spawn(move || {
        let server_completed_for_cb = server_completed_clone.clone();
        let mut on_response = move |_user_data: u32, _data: &[u8]| {
            server_completed_for_cb.fetch_add(1, Ordering::SeqCst);
        };

        let ctx: dc::Context<u32> = dc::ContextBuilder::new()
            .device_index(0)
            .port(1)
            .dci_config(DciConfig {
                max_send_wr: 256,
                max_send_sge: 1,
                max_inline_data: 256,
            })
            .srq_config(SrqConfig {
                max_wr: 4096,
                max_sge: 1,
            })
            .cq_size(2048)
            .build()
            .expect("Server: Failed to create dc context");

        let ep_config = dc::EndpointConfig {
            send_ring_size: RING_SIZE,
            recv_ring_size: RING_SIZE,
        };
        let mut ep = ctx
            .create_endpoint(&ep_config)
            .expect("Server: Failed to create endpoint");

        let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
        server_info_tx
            .send(DcConnectionInfo {
                dct_number: info.dct_number,
                local_identifier: lid,
                dc_key: info.dc_key,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                endpoint_id: info.endpoint_id,
                recv_ring_size: info.recv_ring_size,
                initial_credit: info.initial_credit,
                ..Default::default()
            })
            .unwrap();

        let client_info = client_info_rx.recv().unwrap();
        ep.connect(
            &dc::RemoteEndpointInfo {
                dct_number: client_info.dct_number,
                dc_key: client_info.dc_key,
                local_identifier: client_info.local_identifier,
                recv_ring_addr: client_info.recv_ring_addr,
                recv_ring_rkey: client_info.recv_ring_rkey,
                recv_ring_size: client_info.recv_ring_size,
                initial_credit: client_info.initial_credit,
                endpoint_id: client_info.endpoint_id,
            },
            0,
            ctx.port(),
        )
        .expect("Server: Failed to connect");

        server_ready_clone.store(1, Ordering::Release);

        let request_data = vec![0u8; 24];
        let response_data = vec![0u8; 16];
        let mut sent: usize = 0;

        while !server_stop.load(Ordering::Relaxed) {
            ctx.poll(&mut on_response);

            while let Some(req) = ctx.recv() {
                loop {
                    match req.reply(&response_data) {
                        Ok(()) => break,
                        Err(copyrpc::error::Error::RingFull) => {
                            ctx.poll(&mut on_response);
                        }
                        Err(_) => break,
                    }
                }
            }

            let completed = server_completed_clone.load(Ordering::SeqCst) as usize;
            let mut inflight = sent.saturating_sub(completed);
            while inflight < QUEUE_DEPTH && sent < CALLS_PER_SIDE {
                match ep.call(&request_data, sent as u32, 0) {
                    Ok(_) => {
                        sent += 1;
                        inflight += 1;
                    }
                    Err(
                        copyrpc::error::CallError::RingFull(_)
                        | copyrpc::error::CallError::InsufficientCredit(_),
                    ) => break,
                    Err(_) => break,
                }
            }
        }

        // Drain
        let drain_start = std::time::Instant::now();
        while (server_completed_clone.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
            && drain_start.elapsed() < Duration::from_secs(5)
        {
            ctx.poll(&mut on_response);
            while let Some(req) = ctx.recv() {
                let _ = req.reply(&response_data);
            }
        }
        eprintln!(
            "Server: sent={}, completed={}",
            sent,
            server_completed_clone.load(Ordering::SeqCst)
        );
    });

    // Client side
    let ctx: dc::Context<u32> = dc::ContextBuilder::new()
        .device_index(0)
        .port(1)
        .dci_config(DciConfig {
            max_send_wr: 256,
            max_send_sge: 1,
            max_inline_data: 256,
        })
        .srq_config(SrqConfig {
            max_wr: 4096,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Client: Failed to create dc context");

    let ep_config = dc::EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
    };
    let mut ep = ctx
        .create_endpoint(&ep_config)
        .expect("Client: Failed to create endpoint");

    let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
    client_info_tx
        .send(DcConnectionInfo {
            dct_number: info.dct_number,
            local_identifier: lid,
            dc_key: info.dc_key,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            endpoint_id: info.endpoint_id,
            recv_ring_size: info.recv_ring_size,
            initial_credit: info.initial_credit,
            ..Default::default()
        })
        .unwrap();

    let server_info = server_info_rx.recv().unwrap();
    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    ep.connect(
        &dc::RemoteEndpointInfo {
            dct_number: server_info.dct_number,
            dc_key: server_info.dc_key,
            local_identifier: server_info.local_identifier,
            recv_ring_addr: server_info.recv_ring_addr,
            recv_ring_rkey: server_info.recv_ring_rkey,
            recv_ring_size: server_info.recv_ring_size,
            initial_credit: server_info.initial_credit,
            endpoint_id: server_info.endpoint_id,
        },
        0,
        ctx.port(),
    )
    .expect("Client: Failed to connect");

    let mut on_response = move |_user_data: u32, _data: &[u8]| {
        client_completed_cb.fetch_add(1, Ordering::SeqCst);
    };

    let request_data = vec![0u8; 24];
    let response_data = vec![0u8; 16];
    let mut sent: usize = 0;
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();

    while sent < CALLS_PER_SIDE
        || (client_completed.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
    {
        if start.elapsed() > timeout {
            let cc = client_completed.load(Ordering::SeqCst) as usize;
            let sc = server_completed.load(Ordering::SeqCst) as usize;
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "DC bidir TIMEOUT: client sent={} completed={}, server completed={}",
                sent, cc, sc
            );
        }

        ctx.poll(&mut on_response);

        while let Some(req) = ctx.recv() {
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll(&mut on_response);
                    }
                    Err(_) => break,
                }
            }
        }

        let completed = client_completed.load(Ordering::SeqCst) as usize;
        let mut inflight = sent.saturating_sub(completed);
        while inflight < QUEUE_DEPTH && sent < CALLS_PER_SIDE {
            match ep.call(&request_data, sent as u32, 0) {
                Ok(_) => {
                    sent += 1;
                    inflight += 1;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => break,
                Err(_) => break,
            }
        }
    }

    // Drain server side
    let drain_start = std::time::Instant::now();
    while (server_completed.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
        && drain_start.elapsed() < Duration::from_secs(5)
    {
        ctx.poll(&mut on_response);
        while let Some(req) = ctx.recv() {
            let _ = req.reply(&response_data);
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server thread panicked");

    let cc = client_completed.load(Ordering::SeqCst) as usize;
    let sc = server_completed.load(Ordering::SeqCst) as usize;
    eprintln!(
        "DC bidir: client sent={} completed={}, server completed={}",
        sent, cc, sc
    );

    assert_eq!(cc, CALLS_PER_SIDE, "Client did not receive all responses");
    assert_eq!(sc, CALLS_PER_SIDE, "Server did not receive all responses");
}

/// DC bidirectional with tiny ring — forces many wraps and credit pressure.
#[test]
fn test_dc_bidirectional_tiny_ring() {
    use copyrpc::dc;
    use mlx5::dc::DciConfig;

    const RING_SIZE: usize = 8192; // 8 KB — wraps every ~128 messages
    const CALLS_PER_SIDE: usize = 10_000;
    const QUEUE_DEPTH: usize = 2;

    let client_completed = Arc::new(AtomicU32::new(0));
    let client_completed_cb = client_completed.clone();

    let (server_info_tx, server_info_rx) = mpsc::channel::<DcConnectionInfo>();
    let (client_info_tx, client_info_rx) = mpsc::channel::<DcConnectionInfo>();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();
    let server_completed = Arc::new(AtomicU32::new(0));
    let server_completed_clone = server_completed.clone();

    let handle: JoinHandle<()> = thread::spawn(move || {
        let server_completed_for_cb = server_completed_clone.clone();
        let mut on_response = move |_user_data: u32, _data: &[u8]| {
            server_completed_for_cb.fetch_add(1, Ordering::SeqCst);
        };

        let ctx: dc::Context<u32> = dc::ContextBuilder::new()
            .device_index(0)
            .port(1)
            .dci_config(DciConfig {
                max_send_wr: 256,
                max_send_sge: 1,
                max_inline_data: 256,
            })
            .srq_config(SrqConfig {
                max_wr: 4096,
                max_sge: 1,
            })
            .cq_size(2048)
            .build()
            .expect("Server dc ctx");

        let ep_config = dc::EndpointConfig {
            send_ring_size: RING_SIZE,
            recv_ring_size: RING_SIZE,
        };
        let mut ep = ctx.create_endpoint(&ep_config).expect("Server ep");
        let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
        server_info_tx
            .send(DcConnectionInfo {
                dct_number: info.dct_number,
                local_identifier: lid,
                dc_key: info.dc_key,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                endpoint_id: info.endpoint_id,
                recv_ring_size: info.recv_ring_size,
                initial_credit: info.initial_credit,
                ..Default::default()
            })
            .unwrap();

        let ci = client_info_rx.recv().unwrap();
        ep.connect(
            &dc::RemoteEndpointInfo {
                dct_number: ci.dct_number,
                dc_key: ci.dc_key,
                local_identifier: ci.local_identifier,
                recv_ring_addr: ci.recv_ring_addr,
                recv_ring_rkey: ci.recv_ring_rkey,
                recv_ring_size: ci.recv_ring_size,
                initial_credit: ci.initial_credit,
                endpoint_id: ci.endpoint_id,
            },
            0,
            ctx.port(),
        )
        .expect("Server connect");
        server_ready_clone.store(1, Ordering::Release);

        let req_data = vec![0u8; 24];
        let resp_data = vec![0u8; 16];
        let mut sent: usize = 0;
        while !server_stop.load(Ordering::Relaxed) {
            ctx.poll(&mut on_response);
            while let Some(req) = ctx.recv() {
                loop {
                    match req.reply(&resp_data) {
                        Ok(()) => break,
                        Err(copyrpc::error::Error::RingFull) => ctx.poll(&mut on_response),
                        Err(_) => break,
                    }
                }
            }
            let completed = server_completed_clone.load(Ordering::SeqCst) as usize;
            let mut inflight = sent.saturating_sub(completed);
            while inflight < QUEUE_DEPTH && sent < CALLS_PER_SIDE {
                match ep.call(&req_data, sent as u32, 0) {
                    Ok(_) => {
                        sent += 1;
                        inflight += 1;
                    }
                    Err(
                        copyrpc::error::CallError::RingFull(_)
                        | copyrpc::error::CallError::InsufficientCredit(_),
                    ) => break,
                    Err(_) => break,
                }
            }
        }
        let drain_start = std::time::Instant::now();
        while (server_completed_clone.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
            && drain_start.elapsed() < Duration::from_secs(5)
        {
            ctx.poll(&mut on_response);
            while let Some(req) = ctx.recv() {
                let _ = req.reply(&resp_data);
            }
        }
        if (server_completed_clone.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE {
            ctx.dump_credit_state("SERVER-DRAIN-TIMEOUT");
        }
    });

    let ctx: dc::Context<u32> = dc::ContextBuilder::new()
        .device_index(0)
        .port(1)
        .dci_config(DciConfig {
            max_send_wr: 256,
            max_send_sge: 1,
            max_inline_data: 256,
        })
        .srq_config(SrqConfig {
            max_wr: 4096,
            max_sge: 1,
        })
        .cq_size(2048)
        .build()
        .expect("Client dc ctx");

    let ep_config = dc::EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
    };
    let mut ep = ctx.create_endpoint(&ep_config).expect("Client ep");
    let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
    client_info_tx
        .send(DcConnectionInfo {
            dct_number: info.dct_number,
            local_identifier: lid,
            dc_key: info.dc_key,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            endpoint_id: info.endpoint_id,
            recv_ring_size: info.recv_ring_size,
            initial_credit: info.initial_credit,
            ..Default::default()
        })
        .unwrap();

    let si = server_info_rx.recv().unwrap();
    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }
    ep.connect(
        &dc::RemoteEndpointInfo {
            dct_number: si.dct_number,
            dc_key: si.dc_key,
            local_identifier: si.local_identifier,
            recv_ring_addr: si.recv_ring_addr,
            recv_ring_rkey: si.recv_ring_rkey,
            recv_ring_size: si.recv_ring_size,
            initial_credit: si.initial_credit,
            endpoint_id: si.endpoint_id,
        },
        0,
        ctx.port(),
    )
    .expect("Client connect");

    let mut on_response = move |_: u32, _: &[u8]| {
        client_completed_cb.fetch_add(1, Ordering::SeqCst);
    };
    let req_data = vec![0u8; 24];
    let resp_data = vec![0u8; 16];
    let mut sent: usize = 0;
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();

    while sent < CALLS_PER_SIDE
        || (client_completed.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
    {
        if start.elapsed() > timeout {
            let cc = client_completed.load(Ordering::SeqCst) as usize;
            let sc = server_completed.load(Ordering::SeqCst) as usize;
            ctx.dump_credit_state("CLIENT-TIMEOUT");
            stop_flag.store(true, Ordering::SeqCst);
            let _ = handle.join();
            panic!(
                "DC tiny-ring TIMEOUT: client sent={sent} completed={cc}, server completed={sc}"
            );
        }
        ctx.poll(&mut on_response);
        while let Some(req) = ctx.recv() {
            loop {
                match req.reply(&resp_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => ctx.poll(&mut on_response),
                    Err(_) => break,
                }
            }
        }
        let completed = client_completed.load(Ordering::SeqCst) as usize;
        let mut inflight = sent.saturating_sub(completed);
        while inflight < QUEUE_DEPTH && sent < CALLS_PER_SIDE {
            match ep.call(&req_data, sent as u32, 0) {
                Ok(_) => {
                    sent += 1;
                    inflight += 1;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => break,
                Err(_) => break,
            }
        }
    }

    let drain_start = std::time::Instant::now();
    while (server_completed.load(Ordering::SeqCst) as usize) < CALLS_PER_SIDE
        && drain_start.elapsed() < Duration::from_secs(5)
    {
        ctx.poll(&mut on_response);
        while let Some(req) = ctx.recv() {
            let _ = req.reply(&resp_data);
        }
    }

    stop_flag.store(true, Ordering::SeqCst);
    handle.join().expect("Server panicked");
    let cc = client_completed.load(Ordering::SeqCst) as usize;
    let sc = server_completed.load(Ordering::SeqCst) as usize;
    assert_eq!(cc, CALLS_PER_SIDE, "Client incomplete");
    assert_eq!(sc, CALLS_PER_SIDE, "Server incomplete");
}
