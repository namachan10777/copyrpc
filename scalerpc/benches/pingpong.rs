//! ScaleRPC ping-pong benchmark.
//!
//! Measures:
//! 1. RPC latency (single request, wait for response)
//! 2. RPC throughput (pipelined requests)
//!
//! Run with:
//! ```bash
//! cargo bench --package scalerpc --bench pingpong
//! ```

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use mlx5::device::{Context, DeviceList};
use mlx5::pd::Pd;
use mlx5::types::PortAttr;

use scalerpc::{
    ClientConfig, GroupConfig, PoolConfig, RpcClient, RpcServer, ServerConfig,
};
use scalerpc::connection::RemoteEndpoint;

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

const NUM_SLOTS: usize = 1024;
const SMALL_MSG_SIZE: usize = 32;
const LARGE_MSG_SIZE: usize = 4000; // Max payload is 4056 (4096 - 8 trailer - 32 header)

// =============================================================================
// Connection Info
// =============================================================================

#[derive(Clone)]
struct ConnectionInfo {
    qpn: u32,
    lid: u16,
    slot_addr: u64,
    slot_rkey: u32,
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
// Device Setup
// =============================================================================

fn open_mlx5_device() -> Option<Context> {
    let device_list = DeviceList::list().ok()?;
    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
            return Some(ctx);
        }
    }
    None
}

struct TestContext {
    ctx: Context,
    pd: Pd,
    port_attr: PortAttr,
    port: u8,
}

impl TestContext {
    fn new() -> Option<Self> {
        let ctx = open_mlx5_device()?;
        let port = 1u8;
        let port_attr = ctx.query_port(port).ok()?;
        let pd = ctx.alloc_pd().ok()?;

        Some(Self {
            ctx,
            pd,
            port,
            port_attr,
        })
    }
}

// =============================================================================
// Benchmark Setup
// =============================================================================

struct BenchmarkSetup {
    client: RpcClient,
    conn_id: usize,
    _server_handle: ServerHandle,
    _test_ctx: TestContext,
}

fn setup_benchmark() -> Option<BenchmarkSetup> {
    set_cpu_affinity(CLIENT_CORE);

    let test_ctx = TestContext::new()?;

    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: NUM_SLOTS,
            slot_data_size: 4080,
        },
        timeout_ms: 10000,
    };

    let mut client = RpcClient::new(&test_ctx.pd, client_config).ok()?;
    let conn_id = client.add_connection(&test_ctx.ctx, &test_ctx.pd, test_ctx.port).ok()?;

    let client_endpoint = client.local_endpoint(conn_id).ok()?;
    let client_info = ConnectionInfo {
        qpn: client_endpoint.qpn,
        lid: client_endpoint.lid,
        slot_addr: client_endpoint.slot_addr,
        slot_rkey: client_endpoint.slot_rkey,
    };

    // Setup communication channels
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle = thread::spawn(move || {
        server_thread_main(server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).ok()?;
    let server_info = server_info_rx.recv().ok()?;

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect to server
    let server_endpoint = RemoteEndpoint {
        qpn: server_info.qpn,
        psn: 0,
        lid: server_info.lid,
        slot_addr: server_info.slot_addr,
        slot_rkey: server_info.slot_rkey,
        ..Default::default()
    };

    client.connect(conn_id, server_endpoint).ok()?;

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    // Small delay to ensure connection is established
    std::thread::sleep(std::time::Duration::from_millis(10));

    Some(BenchmarkSetup {
        client,
        conn_id,
        _server_handle: server_handle,
        _test_ctx: test_ctx,
    })
}

// =============================================================================
// Server Thread
// =============================================================================

fn server_thread_main(
    info_tx: Sender<ConnectionInfo>,
    info_rx: Receiver<ConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    set_cpu_affinity(SERVER_CORE);

    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    let port = 1u8;
    let port_attr = match ctx.query_port(port) {
        Ok(attr) => attr,
        Err(_) => return,
    };

    let pd = match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    };

    let server_config = ServerConfig {
        pool: PoolConfig {
            num_slots: NUM_SLOTS,
            slot_data_size: 4080,
        },
        num_recv_slots: 256,
        group: GroupConfig::default(),
        enable_scheduler: false, // Disable for benchmark (simpler path)
    };

    let mut server = match RpcServer::new(&pd, server_config) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Echo handler
    server.set_handler(|_rpc_type, payload| {
        // Echo back the payload
        (0, payload.to_vec())
    });

    let conn_id = match server.add_connection(&ctx, &pd, port) {
        Ok(c) => c,
        Err(_) => return,
    };

    let server_endpoint = match server.local_endpoint(conn_id) {
        Ok(e) => e,
        Err(_) => return,
    };

    let server_info = ConnectionInfo {
        qpn: server_endpoint.qpn,
        lid: server_endpoint.lid,
        slot_addr: server_endpoint.slot_addr,
        slot_rkey: server_endpoint.slot_rkey,
    };

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client_endpoint = RemoteEndpoint {
        qpn: client_info.qpn,
        psn: 0,
        lid: client_info.lid,
        slot_addr: client_info.slot_addr,
        slot_rkey: client_info.slot_rkey,
        ..Default::default()
    };

    if server.connect(conn_id, client_endpoint).is_err() {
        return;
    }

    ready_signal.store(1, Ordering::Release);

    // Server loop
    while !stop_flag.load(Ordering::Relaxed) {
        server.process();
        std::hint::spin_loop();
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Ping-pong latency benchmark (single request, wait for response).
fn run_latency_bench(client: &RpcClient, conn_id: usize, msg_size: usize, iters: u64) -> Duration {
    let request_data = vec![0xAAu8; msg_size];

    let start = std::time::Instant::now();

    for _ in 0..iters {
        // call() is now blocking and returns RpcResponse directly
        match client.call(conn_id, 1, &request_data) {
            Ok(_response) => {}
            Err(_) => continue,
        };
    }

    start.elapsed()
}

/// Throughput benchmark (pipelined requests).
fn run_throughput_bench(
    client: &RpcClient,
    conn_id: usize,
    msg_size: usize,
    iters: u64,
    pipeline_depth: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let mut pending_requests: Vec<scalerpc::PendingRpc<'_>> = Vec::with_capacity(pipeline_depth);
    let mut completed = 0u64;
    let mut sent = 0u64;

    let start = std::time::Instant::now();

    // Initial fill (using call_async for pipelining)
    for _ in 0..pipeline_depth.min(iters as usize) {
        match client.call_async(conn_id, 1, &request_data) {
            Ok(p) => {
                pending_requests.push(p);
                sent += 1;
            }
            Err(_) => {}
        }
    }

    // Main loop
    while completed < iters {
        // Check for completed requests
        let mut i = 0;
        while i < pending_requests.len() {
            if pending_requests[i].poll().is_some() {
                pending_requests.swap_remove(i);
                completed += 1;

                // Send new request
                if sent < iters {
                    match client.call_async(conn_id, 1, &request_data) {
                        Ok(p) => {
                            pending_requests.push(p);
                            sent += 1;
                        }
                        Err(_) => {}
                    }
                }
            } else {
                i += 1;
            }
        }

        std::hint::spin_loop();
    }

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn bench_latency(c: &mut Criterion) {
    let setup = match setup_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let client = setup.client;
    let conn_id = setup.conn_id;
    let _server = setup._server_handle;
    let _ctx = setup._test_ctx;

    let mut group = c.benchmark_group("scalerpc_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    // 32B message latency
    group.bench_function(BenchmarkId::new("pingpong", "32B"), |b| {
        b.iter_custom(|iters| run_latency_bench(&client, conn_id, SMALL_MSG_SIZE, iters));
    });

    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let setup = match setup_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let client = setup.client;
    let conn_id = setup.conn_id;
    let _server = setup._server_handle;
    let _ctx = setup._test_ctx;

    let mut group = c.benchmark_group("scalerpc_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    // Pipeline depth 16
    let pipeline_depth = 16;

    // 32B message throughput
    group.bench_function(
        BenchmarkId::new("pipelined", format!("32B_depth{}", pipeline_depth)),
        |b| {
            b.iter_custom(|iters| {
                run_throughput_bench(&client, conn_id, SMALL_MSG_SIZE, iters, pipeline_depth)
            });
        },
    );

    // 4KB message throughput
    group.bench_function(
        BenchmarkId::new("pipelined", format!("4KB_depth{}", pipeline_depth)),
        |b| {
            b.iter_custom(|iters| {
                run_throughput_bench(&client, conn_id, LARGE_MSG_SIZE, iters, pipeline_depth)
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_latency, bench_throughput);
criterion_main!(benches);
