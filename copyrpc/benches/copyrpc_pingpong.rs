//! Copyrpc ping-pong benchmark using the high-level copyrpc API.
//!
//! This benchmark tests the copyrpc framework's performance with RDMA WRITE+IMM
//! and SPSC ring buffers, comparing against raw mlx5 performance.
//!
//! Run with:
//! ```bash
//! cargo bench --package copyrpc --bench copyrpc_pingpong
//! ```

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use copyrpc::{Context, ContextBuilder, Endpoint, EndpointConfig, RemoteEndpointInfo};
use mlx5::srq::SrqConfig;

// =============================================================================
// CPU Affinity
// =============================================================================

fn set_cpu_affinity(core_id: usize) {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(core_id, &mut cpuset);
        let result = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
        if result != 0 {
            eprintln!("Warning: Failed to set CPU affinity to core {}", core_id);
        }
    }
}

const CLIENT_CORE: usize = 0;
const SERVER_CORE: usize = 1;

// =============================================================================
// Constants
// =============================================================================

const NUM_ENDPOINTS: usize = 8;
const REQUESTS_PER_EP: usize = 128;
const MESSAGE_SIZE: usize = 32;
const RING_SIZE: usize = 1 << 20; // 1 MB

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

#[derive(Clone)]
struct MultiEndpointConnectionInfo {
    endpoints: Vec<EndpointConnectionInfo>,
}

// =============================================================================
// User Data for RPC calls
// =============================================================================

#[derive(Clone, Copy)]
struct CallUserData {
    endpoint_id: u8,
    slot_id: u8,
}

// =============================================================================
// Response Counter
// =============================================================================

/// Global response counter using atomic.
static RESPONSE_COUNT: AtomicU32 = AtomicU32::new(0);

fn on_response_callback(_user_data: CallUserData, _data: &[u8]) {
    RESPONSE_COUNT.fetch_add(1, Ordering::Relaxed);
}

fn get_and_reset_response_count() -> usize {
    RESPONSE_COUNT.swap(0, Ordering::Relaxed) as usize
}

#[allow(dead_code)]
fn get_response_count() -> usize {
    RESPONSE_COUNT.load(Ordering::Relaxed) as usize
}

// =============================================================================
// Client State
// =============================================================================

type OnResponseFn = fn(CallUserData, &[u8]);

struct CopyrpcClient {
    ctx: Context<CallUserData, OnResponseFn>,
    endpoints: Vec<Endpoint<CallUserData>>,
}

// =============================================================================
// Server Handle
// =============================================================================

struct ServerHandle {
    stop_flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ServerHandle {
    fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

// =============================================================================
// Benchmark Setup
// =============================================================================

struct BenchmarkSetup {
    client: CopyrpcClient,
    _server_handle: ServerHandle,
}

fn setup_copyrpc_benchmark() -> Option<BenchmarkSetup> {
    set_cpu_affinity(CLIENT_CORE);

    // Reset response counter
    get_and_reset_response_count();

    let ctx: Context<CallUserData, OnResponseFn> = ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: (NUM_ENDPOINTS * REQUESTS_PER_EP * 2).max(256) as u32,
            max_sge: 1,
        })
        .cq_size(4096)
        .on_response(on_response_callback as OnResponseFn)
        .build()
        .ok()?;

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };

    let mut endpoints = Vec::with_capacity(NUM_ENDPOINTS);
    let mut client_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for _ in 0..NUM_ENDPOINTS {
        let ep = ctx.create_endpoint(&ep_config).ok()?;
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

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

    let handle = thread::spawn(move || {
        server_thread_main(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx
        .send(MultiEndpointConnectionInfo {
            endpoints: client_infos,
        })
        .ok()?;
    let server_info = server_info_rx.recv().ok()?;

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

        ep.connect(&remote, 0, ctx.port()).ok()?;
    }

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(BenchmarkSetup {
        client: CopyrpcClient {
            ctx,
            endpoints,
        },
        _server_handle: server_handle,
    })
}

// =============================================================================
// Server Thread
// =============================================================================

fn server_thread_main(
    info_tx: Sender<MultiEndpointConnectionInfo>,
    info_rx: Receiver<MultiEndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    set_cpu_affinity(SERVER_CORE);

    let on_response: fn(CallUserData, &[u8]) = |_user_data, _data| {};

    let ctx: Context<CallUserData, _> = match ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: (NUM_ENDPOINTS * REQUESTS_PER_EP * 2).max(256) as u32,
            max_sge: 1,
        })
        .cq_size(4096)
        .on_response(on_response)
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
        ..Default::default()
    };

    let mut endpoints = Vec::with_capacity(NUM_ENDPOINTS);
    let mut server_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for _ in 0..NUM_ENDPOINTS {
        let ep = match ctx.create_endpoint(&ep_config) {
            Ok(e) => e,
            Err(_) => return,
        };
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

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

    if info_tx
        .send(MultiEndpointConnectionInfo {
            endpoints: server_infos,
        })
        .is_err()
    {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
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
            return;
        }
    }

    ready_signal.store(1, Ordering::Release);

    // Server response data
    let response_data = vec![0u8; MESSAGE_SIZE];

    // Server loop: receive requests and send replies
    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll();

        // Process received requests
        while let Some(req) = ctx.recv() {
            // Retry reply on RingFull
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        // Poll to update consumer position and retry
                        ctx.poll();
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

fn run_copyrpc_bench(client: &mut CopyrpcClient, iters: u64) -> Duration {
    let request_data = vec![0u8; MESSAGE_SIZE];
    let mut completed = 0u64;
    let mut inflight = 0usize;

    // Reset response counter
    get_and_reset_response_count();

    // Initial fill: send initial requests on each endpoint
    for (ep_id, ep) in client.endpoints.iter().enumerate() {
        for slot_id in 0..REQUESTS_PER_EP {
            let user_data = CallUserData {
                endpoint_id: ep_id as u8,
                slot_id: slot_id as u8,
            };
            if ep.call(&request_data, user_data).is_ok() {
                inflight += 1;
            }
        }
    }

    // Flush initial batch
    client.ctx.poll();

    let start = std::time::Instant::now();

    while completed < iters {
        // Poll for completions - this triggers on_response callbacks
        client.ctx.poll();

        // Get completions from on_response callback via thread-local counter
        let new_completions = get_and_reset_response_count();
        completed += new_completions as u64;
        inflight = inflight.saturating_sub(new_completions);

        // Send new requests to maintain queue depth
        let remaining = iters.saturating_sub(completed);
        let can_send = (NUM_ENDPOINTS * REQUESTS_PER_EP)
            .saturating_sub(inflight)
            .min(remaining as usize);

        for i in 0..can_send {
            let ep_id = i % NUM_ENDPOINTS;
            let slot_id = i / NUM_ENDPOINTS;
            let ep = &client.endpoints[ep_id];

            let user_data = CallUserData {
                endpoint_id: ep_id as u8,
                slot_id: slot_id as u8,
            };

            if ep.call(&request_data, user_data).is_ok() {
                inflight += 1;
            }
        }
    }

    // Drain remaining inflight requests
    while inflight > 0 {
        client.ctx.poll();
        let new_completions = get_and_reset_response_count();
        inflight = inflight.saturating_sub(new_completions);
    }

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("copyrpc_api");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    if let Some(setup) = setup_copyrpc_benchmark() {
        let client = RefCell::new(setup.client);
        let mut server_handle = setup._server_handle;

        group.bench_function(
            BenchmarkId::new("copyrpc", format!("{}ep_{}c_{}B", NUM_ENDPOINTS, REQUESTS_PER_EP, MESSAGE_SIZE)),
            |b| {
                b.iter_custom(|iters| run_copyrpc_bench(&mut client.borrow_mut(), iters));
            },
        );

        server_handle.stop();
    }

    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
