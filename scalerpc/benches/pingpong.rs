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
//!
//! Environment variables for configuration:
//! - `SCALERPC_NUM_SLOTS`: Number of slots for single QP tests (default: 1024)
//! - `SCALERPC_PIPELINE_DEPTH`: Pipeline depth for throughput test (default: 16)
//! - `SCALERPC_NUM_QPS`: Number of QPs for multi-QP test (default: 64)
//! - `SCALERPC_MULTI_QP_PIPELINE_DEPTH`: Pipeline depth for multi-QP test (default: 64)

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use minstant::Instant;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use mlx5::device::{Context, DeviceList};
use mlx5::pd::Pd;
use mlx5::types::PortAttr;

use scalerpc::{
    ClientConfig, GroupConfig, PoolConfig, RpcClient, RpcServer, ServerConfig,
};
use scalerpc::connection::RemoteEndpoint;

// =============================================================================
// BenchConfig - Environment Variable Configuration
// =============================================================================

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone)]
struct BenchConfig {
    // Single QP
    num_slots: usize,
    pipeline_depth: usize,
    // Multi QP
    num_qps: usize,
    num_groups: usize,
    multi_qp_pipeline_depth: usize,
    slots_per_conn: usize,
}

impl BenchConfig {
    fn load() -> Self {
        Self {
            num_slots: env_or("SCALERPC_NUM_SLOTS", 256),
            pipeline_depth: env_or("SCALERPC_PIPELINE_DEPTH", 16),
            num_qps: env_or("SCALERPC_NUM_QPS", 64),
            num_groups: env_or("SCALERPC_NUM_GROUPS", 4),
            multi_qp_pipeline_depth: env_or("SCALERPC_MULTI_QP_PIPELINE_DEPTH", 64),
            slots_per_conn: env_or("SCALERPC_SLOTS_PER_CONN", 4),
        }
    }

    /// Number of connections per group.
    fn group_size(&self) -> usize {
        self.num_qps / self.num_groups
    }

    /// Total slots needed - based on group size (only one group active at a time).
    fn multi_qp_num_slots(&self) -> usize {
        self.group_size() * self.slots_per_conn
    }
}

// =============================================================================
// Constants
// =============================================================================

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
    _port_attr: PortAttr,
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
            _port_attr: port_attr,
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

fn setup_benchmark(config: &BenchConfig) -> Option<BenchmarkSetup> {
    let test_ctx = TestContext::new()?;

    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: config.num_slots,
            slot_data_size: 4080,
        },
        max_connections: 64, // 16 slots per connection
    };

    let mut client = RpcClient::new(&test_ctx.pd, client_config).ok()?;
    let conn_id = client.add_connection(&test_ctx.ctx, &test_ctx.pd, test_ctx.port).ok()?;

    let client_endpoint = client.local_endpoint(conn_id).ok()?;
    let client_info: ConnectionInfo = client_endpoint.into();

    // Setup communication channels
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let server_config = config.clone();
    let handle = thread::spawn(move || {
        server_thread_main(&server_config, server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_info).ok()?;
    let server_info = server_info_rx.recv().ok()?;

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect to server
    client.connect(conn_id, server_info.into()).ok()?;

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
    config: &BenchConfig,
    info_tx: Sender<ConnectionInfo>,
    info_rx: Receiver<ConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    let port = 1u8;
    let _port_attr = match ctx.query_port(port) {
        Ok(attr) => attr,
        Err(_) => return,
    };

    let pd = match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    };

    let server_config = ServerConfig {
        pool: PoolConfig {
            num_slots: config.num_slots,
            slot_data_size: 4080,
        },
        num_recv_slots: 256,
        group: GroupConfig {
            num_groups: 1,  // Single QP test uses 1 group
            ..GroupConfig::default()
        },
        max_connections: 1, // Single QP test uses 1 connection
    };

    let mut server = match RpcServer::new(&pd, server_config) {
        Ok(s) => s.with_handler(|_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
            // Echo back the payload (zero-copy)
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        }),
        Err(_) => return,
    };

    let conn_id = match server.add_connection(&ctx, &pd, port) {
        Ok(c) => c,
        Err(_) => return,
    };

    let server_endpoint = match server.local_endpoint(conn_id) {
        Ok(e) => e,
        Err(_) => return,
    };

    let server_info: ConnectionInfo = server_endpoint.into();

    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    if server.connect(conn_id, client_info.into()).is_err() {
        return;
    }

    ready_signal.store(1, Ordering::Release);

    // Server loop
    let mut total_processed = 0u64;
    while !stop_flag.load(Ordering::Relaxed) {
        let n = server.poll();
        if n > 0 {
            total_processed += n as u64;
        }
        std::hint::spin_loop();
    }
    eprintln!("[server] exiting, total_processed={}", total_processed);
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Ping-pong latency benchmark (single request, wait for response).
fn run_latency_bench(client: &RpcClient, conn_id: usize, msg_size: usize, iters: u64) -> Duration {
    let request_data = vec![0xAAu8; msg_size];

    let start = Instant::now();

    for _ in 0..iters {
        match client.call_async(conn_id, 1, &request_data) {
            Ok(pending) => {
                // Poll until response is received
                loop {
                    client.poll();
                    if pending.poll().is_some() {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
            Err(_) => continue,
        };
    }

    start.elapsed()
}

/// Throughput benchmark (pipelined requests with doorbell batching).
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

    let start = Instant::now();

    // Initial fill (using call_async for pipelining)
    for _ in 0..pipeline_depth.min(iters as usize) {
        if let Ok(p) = client.call_async(conn_id, 1, &request_data) {
            pending_requests.push(p);
            sent += 1;
        }
    }

    // Batch doorbell for initial fill
    client.poll();

    // Main loop with timeout
    let timeout = Duration::from_secs(10);
    while completed < iters {
        if start.elapsed() > timeout {
            eprintln!("[bench] TIMEOUT: completed={}/{}, sent={}, pending={}",
                     completed, iters, sent, pending_requests.len());
            break;
        }
        // Check for completed requests
        let mut i = 0;
        while i < pending_requests.len() {
            if pending_requests[i].poll().is_some() {
                pending_requests.swap_remove(i);
                completed += 1;

                // Send new request
                if sent < iters {
                    if let Ok(p) = client.call_async(conn_id, 1, &request_data) {
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

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn bench_latency(c: &mut Criterion) {
    let config = BenchConfig::load();

    let setup = match setup_benchmark(&config) {
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
    let config = BenchConfig::load();

    let setup = match setup_benchmark(&config) {
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

    let pipeline_depth = config.pipeline_depth;

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

// =============================================================================
// Multi-QP Benchmark (configurable QPs for time-sharing scalability test)
// =============================================================================

struct MultiQpBenchmarkSetup {
    client: RpcClient,
    conn_ids: Vec<usize>,
    _server_handle: ServerHandle,
    _test_ctx: TestContext,
}

fn setup_multi_qp_benchmark(config: &BenchConfig) -> Option<MultiQpBenchmarkSetup> {
    let test_ctx = TestContext::new()?;

    let num_qps = config.num_qps;
    let multi_qp_num_slots = config.multi_qp_num_slots();

    let client_config = ClientConfig {
        pool: PoolConfig {
            num_slots: multi_qp_num_slots,
            slot_data_size: 4080,
        },
        max_connections: num_qps, // 16 slots per connection
    };

    let mut client = RpcClient::new(&test_ctx.pd, client_config).ok()?;

    // Create num_qps connections
    let mut conn_ids = Vec::with_capacity(num_qps);
    let mut client_infos = Vec::with_capacity(num_qps);

    for _ in 0..num_qps {
        let conn_id = client.add_connection(&test_ctx.ctx, &test_ctx.pd, test_ctx.port).ok()?;
        let endpoint = client.local_endpoint(conn_id).ok()?;
        conn_ids.push(conn_id);
        client_infos.push(endpoint.into());
    }

    // Setup communication channels for multi-QP
    let (server_info_tx, server_info_rx): (Sender<Vec<ConnectionInfo>>, Receiver<Vec<ConnectionInfo>>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<Vec<ConnectionInfo>>, Receiver<Vec<ConnectionInfo>>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let server_config = config.clone();
    let handle = thread::spawn(move || {
        multi_qp_server_thread_main(&server_config, server_info_tx, client_info_rx, server_ready_clone, server_stop);
    });

    client_info_tx.send(client_infos).ok()?;
    let server_infos = server_info_rx.recv().ok()?;

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect all QPs
    for (i, conn_id) in conn_ids.iter().enumerate() {
        let server_info = server_infos[i].clone();
        client.connect(*conn_id, server_info.into()).ok()?;
    }

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    std::thread::sleep(std::time::Duration::from_millis(10));

    Some(MultiQpBenchmarkSetup {
        client,
        conn_ids,
        _server_handle: server_handle,
        _test_ctx: test_ctx,
    })
}

fn multi_qp_server_thread_main(
    config: &BenchConfig,
    info_tx: Sender<Vec<ConnectionInfo>>,
    info_rx: Receiver<Vec<ConnectionInfo>>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    let port = 1u8;
    let _port_attr = match ctx.query_port(port) {
        Ok(attr) => attr,
        Err(_) => return,
    };

    let pd = match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    };

    let num_qps = config.num_qps;
    let multi_qp_num_slots = config.multi_qp_num_slots();
    let group_size = config.group_size();

    let server_config = ServerConfig {
        pool: PoolConfig {
            num_slots: multi_qp_num_slots,
            slot_data_size: 4080,
        },
        num_recv_slots: 256,
        group: GroupConfig {
            num_groups: config.num_groups,
            max_active_per_group: group_size,
            time_slice_us: 100,
            max_endpoint_entries: 256,
        },
        max_connections: num_qps,
    };

    let mut server = match RpcServer::new(&pd, server_config) {
        Ok(s) => s.with_handler(|_rpc_type: u16, payload: &[u8], response_buf: &mut [u8]| {
            let len = payload.len().min(response_buf.len());
            response_buf[..len].copy_from_slice(&payload[..len]);
            (0, len)
        }),
        Err(_) => return,
    };

    // Create num_qps connections on server side
    let mut server_infos = Vec::with_capacity(num_qps);
    let mut server_conn_ids = Vec::with_capacity(num_qps);

    for _ in 0..num_qps {
        let conn_id = match server.add_connection(&ctx, &pd, port) {
            Ok(c) => c,
            Err(_) => return,
        };
        let endpoint = match server.local_endpoint(conn_id) {
            Ok(e) => e,
            Err(_) => return,
        };
        server_conn_ids.push(conn_id);
        server_infos.push(endpoint.into());
    }

    if info_tx.send(server_infos).is_err() {
        return;
    }

    let client_infos = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    // Connect all server QPs to client QPs
    for (i, conn_id) in server_conn_ids.iter().enumerate() {
        let client_info = client_infos[i].clone();
        if server.connect(*conn_id, client_info.into()).is_err() {
            return;
        }
    }

    ready_signal.store(1, Ordering::Release);

    while !stop_flag.load(Ordering::Relaxed) {
        server.poll();
        std::hint::spin_loop();
    }
}

/// Throughput benchmark with multiple QPs (8 QPs, 1024 concurrent requests with doorbell batching).
fn run_multi_qp_throughput_bench(
    client: &RpcClient,
    conn_ids: &[usize],
    msg_size: usize,
    iters: u64,
    pipeline_depth: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let mut pending_requests: Vec<(usize, scalerpc::PendingRpc<'_>)> = Vec::with_capacity(pipeline_depth);
    let mut completed = 0u64;
    let mut sent = 0u64;
    let num_qps = conn_ids.len();

    let start = Instant::now();

    // Initial fill - distribute across QPs round-robin
    for i in 0..pipeline_depth.min(iters as usize) {
        let conn_id = conn_ids[i % num_qps];
        if let Ok(p) = client.call_async(conn_id, 1, &request_data) {
            pending_requests.push((conn_id, p));
            sent += 1;
        }
    }

    // Batch doorbell for initial fill
    client.poll();

    // Main loop
    while completed < iters {
        let mut i = 0;
        while i < pending_requests.len() {
            if pending_requests[i].1.poll().is_some() {
                pending_requests.swap_remove(i);
                completed += 1;

                if sent < iters {
                    let conn_id = conn_ids[(sent as usize) % num_qps];
                    if let Ok(p) = client.call_async(conn_id, 1, &request_data) {
                        pending_requests.push((conn_id, p));
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

    start.elapsed()
}

fn bench_multi_qp_throughput(c: &mut Criterion) {
    let config = BenchConfig::load();

    let setup = match setup_multi_qp_benchmark(&config) {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let client = setup.client;
    let conn_ids = setup.conn_ids;
    let _server = setup._server_handle;
    let _ctx = setup._test_ctx;

    let mut group = c.benchmark_group("scalerpc_multi_qp");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    let num_qps = config.num_qps;
    let pipeline_depth = config.multi_qp_pipeline_depth;
    let num_slots = config.multi_qp_num_slots();

    // Multi-QP, 32B messages
    // Format: {qps}qp_{groups}g_{slots}s_d{depth}
    group.bench_function(
        BenchmarkId::new(format!("{}qp_{}g_{}s_d{}", num_qps, config.num_groups, num_slots, pipeline_depth), "32B"),
        |b| {
            b.iter_custom(|iters| {
                run_multi_qp_throughput_bench(&client, &conn_ids, SMALL_MSG_SIZE, iters, pipeline_depth)
            });
        },
    );

    // Multi-QP, 4KB messages
    group.bench_function(
        BenchmarkId::new(format!("{}qp_{}g_{}s_d{}", num_qps, config.num_groups, num_slots, pipeline_depth), "4KB"),
        |b| {
            b.iter_custom(|iters| {
                run_multi_qp_throughput_bench(&client, &conn_ids, LARGE_MSG_SIZE, iters, pipeline_depth)
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_latency, bench_throughput, bench_multi_qp_throughput);
criterion_main!(benches);
