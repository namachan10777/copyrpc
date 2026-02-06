//! eRPC ping-pong benchmark.
//!
//! Measures:
//! 1. RPC latency (single request, wait for response)
//! 2. RPC throughput (pipelined requests)
//!
//! Run with:
//! ```bash
//! cargo bench --package erpc --bench rpc_bench
//! ```

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use mlx5::device::DeviceList;
use mlx5::types::PortAttr;

use erpc::{RemoteInfo, Rpc, RpcConfig};

// =============================================================================
// Constants
// =============================================================================

const SMALL_MSG_SIZE: usize = 32;
// Note: LARGE_MSG_SIZE must be less than MTU - PKT_HDR_SIZE (typically 4096 - 16 = 4080)
// Multi-packet messages are not yet supported in the benchmark
const LARGE_MSG_SIZE: usize = 4000;

// Multi-QP benchmark parameters (matching eRPC paper conditions)
const NUM_QPS: usize = 8;
const PIPELINE_DEPTH_PER_QP: usize = 32;

// =============================================================================
// Connection Info
// =============================================================================

#[derive(Clone)]
struct ConnectionInfo {
    qpn: u32,
    qkey: u32,
    lid: u16,
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

fn open_mlx5_device() -> Option<(mlx5::device::Context, u8, PortAttr)> {
    let device_list = DeviceList::list().ok()?;
    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
            for port in 1..=2u8 {
                if let Ok(port_attr) = ctx.query_port(port) {
                    if port_attr.state == mlx5::types::PortState::Active {
                        return Some((ctx, port, port_attr));
                    }
                }
            }
        }
    }
    None
}

// =============================================================================
// Benchmark Setup
// =============================================================================

struct BenchmarkSetup {
    client: Rpc<()>,
    session: erpc::SessionHandle,
    _server_handle: ServerHandle,
}

struct MultiQpBenchmarkSetup {
    clients: Vec<Rpc<usize>>,
    sessions: Vec<erpc::SessionHandle>,
    _server_handles: Vec<ServerHandle>,
}

// Shared completion counter for client callbacks
thread_local! {
    static COMPLETED: RefCell<u64> = RefCell::new(0);
}

fn setup_benchmark() -> Option<BenchmarkSetup> {
    let (ctx, port, _port_attr) = open_mlx5_device()?;

    let pipeline_depth = 1024;
    let config = RpcConfig::default()
        .with_req_window(pipeline_depth)
        .with_session_credits(pipeline_depth + 64)
        .with_num_recv_buffers(pipeline_depth * 2)
        .with_max_send_wr((pipeline_depth * 2) as u32)
        .with_max_recv_wr((pipeline_depth * 2) as u32);

    // Create client with on_response callback that increments counter
    let client: Rpc<()> = Rpc::new(&ctx, port, config.clone(), |_, _| {
        COMPLETED.with(|c| *c.borrow_mut() += 1);
    }).ok()?;
    let client_info = client.local_info();
    let client_conn_info = ConnectionInfo {
        qpn: client_info.qpn,
        qkey: client_info.qkey,
        lid: client_info.lid,
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

    let server_config = config.clone();
    let handle = thread::spawn(move || {
        server_thread_main_with_config(server_info_tx, client_info_rx, server_ready_clone, server_stop, server_config);
    });

    client_info_tx.send(client_conn_info).ok()?;
    let server_info = server_info_rx.recv().ok()?;

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Create session to server
    let server_remote = RemoteInfo {
        qpn: server_info.qpn,
        qkey: server_info.qkey,
        lid: server_info.lid,
    };
    let session = client.create_session(&server_remote).ok()?;

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    // Wait for session handshake to complete
    let handshake_start = std::time::Instant::now();
    let handshake_timeout = Duration::from_secs(5);
    while handshake_start.elapsed() < handshake_timeout {
        client.run_event_loop_once();
        if client.is_session_connected(session) {
            break;
        }
        std::thread::sleep(Duration::from_micros(100));
    }

    if !client.is_session_connected(session) {
        eprintln!("Session handshake failed");
        return None;
    }

    Some(BenchmarkSetup {
        client,
        session,
        _server_handle: server_handle,
    })
}

// Per-QP completion counters for multi-QP benchmark
thread_local! {
    static MULTI_QP_COMPLETED: RefCell<Vec<u64>> = RefCell::new(Vec::new());
}

fn setup_multi_qp_benchmark(num_qps: usize, pipeline_depth: usize) -> Option<MultiQpBenchmarkSetup> {
    let (ctx, port, _port_attr) = open_mlx5_device()?;

    let config = RpcConfig::default()
        .with_req_window(pipeline_depth)
        .with_session_credits(pipeline_depth * 2)
        .with_num_recv_buffers(pipeline_depth * 4)
        .with_max_send_wr((pipeline_depth * 4) as u32)
        .with_max_recv_wr((pipeline_depth * 4) as u32);

    // Initialize per-QP completion counters
    MULTI_QP_COMPLETED.with(|c| {
        let mut v = c.borrow_mut();
        v.clear();
        v.resize(num_qps, 0);
    });

    // Create all clients first
    let mut clients: Vec<Rpc<usize>> = Vec::with_capacity(num_qps);
    let mut client_infos = Vec::with_capacity(num_qps);
    for qp_idx in 0..num_qps {
        // Each client gets a callback that increments its specific counter
        let client: Rpc<usize> = Rpc::new(&ctx, port, config.clone(), move |_, _| {
            MULTI_QP_COMPLETED.with(|c| {
                let mut v = c.borrow_mut();
                if qp_idx < v.len() {
                    v[qp_idx] += 1;
                }
            });
        }).ok()?;
        let info = client.local_info();
        client_infos.push(ConnectionInfo {
            qpn: info.qpn,
            qkey: info.qkey,
            lid: info.lid,
        });
        clients.push(client);
    }

    // Setup channels for server thread (single thread handles all QPs)
    let (client_infos_tx, client_infos_rx): (Sender<Vec<ConnectionInfo>>, Receiver<Vec<ConnectionInfo>>) =
        mpsc::channel();
    let (server_infos_tx, server_infos_rx): (Sender<Vec<ConnectionInfo>>, Receiver<Vec<ConnectionInfo>>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let server_config = config.clone();
    let handle = thread::spawn(move || {
        multi_qp_server_thread(
            server_infos_tx,
            client_infos_rx,
            server_ready_clone,
            server_stop,
            server_config,
            num_qps,
        );
    });

    // Send all client infos to server
    client_infos_tx.send(client_infos).ok()?;
    let server_infos = server_infos_rx.recv().ok()?;

    // Wait for server to be ready
    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Create sessions for all clients
    let mut sessions = Vec::with_capacity(num_qps);
    for (i, client) in clients.iter().enumerate() {
        let server_remote = RemoteInfo {
            qpn: server_infos[i].qpn,
            qkey: server_infos[i].qkey,
            lid: server_infos[i].lid,
        };
        let session = client.create_session(&server_remote).ok()?;
        sessions.push(session);
    }

    // Wait for all session handshakes
    let handshake_start = std::time::Instant::now();
    let handshake_timeout = Duration::from_secs(5);
    loop {
        if handshake_start.elapsed() >= handshake_timeout {
            eprintln!("Session handshake timeout");
            return None;
        }

        let mut all_connected = true;
        for (i, client) in clients.iter().enumerate() {
            client.run_event_loop_once();
            if !client.is_session_connected(sessions[i]) {
                all_connected = false;
            }
        }
        if all_connected {
            break;
        }
        std::hint::spin_loop();
    }

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(MultiQpBenchmarkSetup {
        clients,
        sessions,
        _server_handles: vec![server_handle],
    })
}

/// Single server thread handling multiple QPs (async-style event loop).
fn multi_qp_server_thread(
    infos_tx: Sender<Vec<ConnectionInfo>>,
    infos_rx: Receiver<Vec<ConnectionInfo>>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    config: RpcConfig,
    num_qps: usize,
) {
    let (ctx, port, _port_attr) = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    // Create all server Rpc instances (server doesn't need response callback)
    let mut servers: Vec<Rpc<()>> = Vec::with_capacity(num_qps);
    let mut server_infos: Vec<ConnectionInfo> = Vec::with_capacity(num_qps);

    for _ in 0..num_qps {
        let server: Rpc<()> = match Rpc::new(&ctx, port, config.clone(), |_, _| {}) {
            Ok(s) => s,
            Err(_) => return,
        };

        let info = server.local_info();
        server_infos.push(ConnectionInfo {
            qpn: info.qpn,
            qkey: info.qkey,
            lid: info.lid,
        });
        servers.push(server);
    }

    // Send server infos to client
    if infos_tx.send(server_infos).is_err() {
        return;
    }

    // Receive client infos
    let client_infos = match infos_rx.recv() {
        Ok(infos) => infos,
        Err(_) => return,
    };

    // Create sessions to clients
    for (i, server) in servers.iter().enumerate() {
        let client_remote = RemoteInfo {
            qpn: client_infos[i].qpn,
            qkey: client_infos[i].qkey,
            lid: client_infos[i].lid,
        };
        if server.create_session(&client_remote).is_err() {
            return;
        }
    }

    ready_signal.store(1, Ordering::Release);

    // Single event loop for all QPs - poll recv() and reply()
    // Process limited requests per server per iteration to avoid buffer exhaustion
    const MAX_BATCH: usize = 16;
    while !stop_flag.load(Ordering::Relaxed) {
        for server in &servers {
            server.run_event_loop_once();
            // Process incoming requests and echo back (limited batch)
            let mut processed = 0;
            while processed < MAX_BATCH {
                if let Some(req) = server.recv() {
                    // Zero-copy: get data reference from request
                    let data = req.data(server).to_vec(); // Copy for echo response
                    if let Err(e) = server.reply(&req, &data) {
                        eprintln!("Server failed to respond: {:?}", e);
                    }
                    processed += 1;
                } else {
                    break;
                }
            }
        }
        std::hint::spin_loop();
    }
}

// =============================================================================
// Server Thread
// =============================================================================

#[allow(dead_code)]
fn server_thread_main(
    info_tx: Sender<ConnectionInfo>,
    info_rx: Receiver<ConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let config = RpcConfig::default()
        .with_req_window(16)
        .with_session_credits(64);
    server_thread_main_with_config(info_tx, info_rx, ready_signal, stop_flag, config);
}

fn server_thread_main_with_config(
    info_tx: Sender<ConnectionInfo>,
    info_rx: Receiver<ConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    config: RpcConfig,
) {
    let (ctx, port, _port_attr) = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    // Server doesn't need response callback
    let server: Rpc<()> = match Rpc::new(&ctx, port, config, |_, _| {}) {
        Ok(s) => s,
        Err(_) => return,
    };

    let server_info = server.local_info();
    let conn_info = ConnectionInfo {
        qpn: server_info.qpn,
        qkey: server_info.qkey,
        lid: server_info.lid,
    };

    if info_tx.send(conn_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    // Create session to client (for sending responses)
    let client_remote = RemoteInfo {
        qpn: client_info.qpn,
        qkey: client_info.qkey,
        lid: client_info.lid,
    };
    if server.create_session(&client_remote).is_err() {
        return;
    }

    ready_signal.store(1, Ordering::Release);

    // Server loop - poll recv() and reply()
    // Process limited requests per event loop iteration to avoid buffer exhaustion
    const MAX_BATCH: usize = 16;
    // Pre-allocated response buffer to avoid allocation in hot path
    let mut response_buf = vec![0u8; 4096];
    while !stop_flag.load(Ordering::Relaxed) {
        server.run_event_loop_once();
        // Process incoming requests and echo back (limited batch)
        let mut processed = 0;
        while processed < MAX_BATCH {
            if let Some(req) = server.recv() {
                // Zero-copy echo: copy request data to pre-allocated buffer
                let data = req.data(&server);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                if let Err(e) = server.reply(&req, &response_buf[..len]) {
                    eprintln!("Server failed to respond: {:?}", e);
                }
                processed += 1;
            } else {
                break;
            }
        }
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Ping-pong latency benchmark (single request, wait for response).
fn run_latency_bench(
    client: &Rpc<()>,
    session: erpc::SessionHandle,
    msg_size: usize,
    iters: u64,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];

    // Reset completion counter
    COMPLETED.with(|c| *c.borrow_mut() = 0);

    let start = std::time::Instant::now();

    for i in 0..iters {
        if client.call(session, 1, &request_data, ()).is_err() {
            continue;
        }

        // Wait for response
        let target = i + 1;
        loop {
            client.run_event_loop_once();
            let completed = COMPLETED.with(|c| *c.borrow());
            if completed >= target {
                break;
            }
            std::hint::spin_loop();
        }
    }

    start.elapsed()
}

/// Throughput benchmark (pipelined requests).
fn run_throughput_bench(
    client: &Rpc<()>,
    session: erpc::SessionHandle,
    msg_size: usize,
    iters: u64,
    pipeline_depth: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let mut sent = 0u64;

    // Reset completion counter
    COMPLETED.with(|c| *c.borrow_mut() = 0);

    let start = std::time::Instant::now();

    // Initial fill
    for _ in 0..pipeline_depth.min(iters as usize) {
        if client.call(session, 1, &request_data, ()).is_ok() {
            sent += 1;
        }
    }

    // Main loop
    loop {
        client.run_event_loop_once();

        let completed = COMPLETED.with(|c| *c.borrow());
        if completed >= iters {
            break;
        }

        // Send more requests if we have capacity
        while sent < iters && sent - completed < pipeline_depth as u64 {
            if client.call(session, 1, &request_data, ()).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }

        std::hint::spin_loop();
    }

    start.elapsed()
}

/// Multi-QP throughput benchmark (8 QPs, 1024 pipeline depth each).
fn run_multi_qp_throughput_bench(
    clients: &[Rpc<usize>],
    sessions: &[erpc::SessionHandle],
    msg_size: usize,
    iters: u64,
    pipeline_depth: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let num_qps = clients.len();
    let iters_per_qp = iters / num_qps as u64;

    // Reset per-QP completion counters
    MULTI_QP_COMPLETED.with(|c| {
        let mut v = c.borrow_mut();
        for i in 0..num_qps {
            if i < v.len() {
                v[i] = 0;
            }
        }
    });

    let mut sents: Vec<u64> = vec![0; num_qps];

    let start = std::time::Instant::now();

    // Initial fill for all QPs
    for qp_idx in 0..num_qps {
        for _ in 0..pipeline_depth.min(iters_per_qp as usize) {
            if clients[qp_idx]
                .call(sessions[qp_idx], 1, &request_data, qp_idx)
                .is_ok()
            {
                sents[qp_idx] += 1;
            }
        }
    }

    // Main loop - round-robin across all QPs
    let total_target = iters_per_qp * num_qps as u64;

    loop {
        // Process all QPs
        for qp_idx in 0..num_qps {
            clients[qp_idx].run_event_loop_once();

            // Send more requests if we have capacity
            let completed = MULTI_QP_COMPLETED.with(|c| c.borrow()[qp_idx]);
            while sents[qp_idx] < iters_per_qp
                && sents[qp_idx] - completed < pipeline_depth as u64
            {
                if clients[qp_idx]
                    .call(sessions[qp_idx], 1, &request_data, qp_idx)
                    .is_ok()
                {
                    sents[qp_idx] += 1;
                } else {
                    break;
                }
            }
        }

        // Update total completed
        let total_completed: u64 = MULTI_QP_COMPLETED.with(|c| c.borrow().iter().sum());
        if total_completed >= total_target {
            break;
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
    let session = setup.session;
    let _server = setup._server_handle;

    let mut group = c.benchmark_group("erpc_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    // 32B message latency
    group.bench_function(BenchmarkId::new("pingpong", "32B"), |b| {
        b.iter_custom(|iters| run_latency_bench(&client, session, SMALL_MSG_SIZE, iters));
    });

    // 4KB message latency
    group.bench_function(BenchmarkId::new("pingpong", "4KB"), |b| {
        b.iter_custom(|iters| run_latency_bench(&client, session, LARGE_MSG_SIZE, iters));
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
    let session = setup.session;
    let _server = setup._server_handle;

    let mut group = c.benchmark_group("erpc_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    // 32B message throughput with varying depths
    // eRPC paper uses B=8 as default session credits
    for &pipeline_depth in &[8, 16, 64, 256] {
        group.bench_function(
            BenchmarkId::new("pipelined", format!("32B_depth{}", pipeline_depth)),
            |b| {
                b.iter_custom(|iters| {
                    run_throughput_bench(&client, session, SMALL_MSG_SIZE, iters, pipeline_depth)
                });
            },
        );
    }

    // 4KB message throughput
    let pipeline_depth = 16;
    group.bench_function(
        BenchmarkId::new("pipelined", format!("4KB_depth{}", pipeline_depth)),
        |b| {
            b.iter_custom(|iters| {
                run_throughput_bench(&client, session, LARGE_MSG_SIZE, iters, pipeline_depth)
            });
        },
    );

    group.finish();
}

fn bench_multi_qp_throughput(c: &mut Criterion) {
    let setup = match setup_multi_qp_benchmark(NUM_QPS, PIPELINE_DEPTH_PER_QP) {
        Some(s) => s,
        None => {
            eprintln!("Skipping multi-QP benchmark: setup failed");
            return;
        }
    };

    let clients = setup.clients;
    let sessions = setup.sessions;
    let _servers = setup._server_handles;

    let mut group = c.benchmark_group("erpc_multi_qp");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    // 32B message, 8 QPs, 1024 pipeline depth each
    group.bench_function(
        BenchmarkId::new(
            "pipelined",
            format!("32B_{}qp_depth{}", NUM_QPS, PIPELINE_DEPTH_PER_QP),
        ),
        |b| {
            b.iter_custom(|iters| {
                run_multi_qp_throughput_bench(
                    &clients,
                    &sessions,
                    SMALL_MSG_SIZE,
                    iters,
                    PIPELINE_DEPTH_PER_QP,
                )
            });
        },
    );

    group.finish();
}

// =============================================================================
// eRPC Paper Condition Benchmark (1 Rpc, multiple sessions)
// =============================================================================

struct MultiSessionBenchmarkSetup {
    client: Rpc<usize>,
    sessions: Vec<erpc::SessionHandle>,
    _server_handle: ServerHandle,
}

// Completion counter for multi-session benchmark
thread_local! {
    static MULTI_SESSION_COMPLETED: RefCell<u64> = RefCell::new(0);
}

fn setup_multi_session_benchmark(num_sessions: usize) -> Option<MultiSessionBenchmarkSetup> {
    let (ctx, port, _port_attr) = open_mlx5_device()?;

    // eRPC paper: B=8 session credits, large req_window for pipelining
    let pipeline_depth = 64;
    let config = RpcConfig::default()
        .with_req_window(pipeline_depth * num_sessions)
        .with_session_credits(8) // eRPC default
        .with_num_recv_buffers(pipeline_depth * num_sessions * 2)
        .with_max_send_wr((pipeline_depth * num_sessions * 2) as u32)
        .with_max_recv_wr((pipeline_depth * num_sessions * 2) as u32);

    // Create single client Rpc with callback
    let client: Rpc<usize> = Rpc::new(&ctx, port, config.clone(), |_session_idx, _payload| {
        MULTI_SESSION_COMPLETED.with(|c| *c.borrow_mut() += 1);
    }).ok()?;
    let client_info = client.local_info();

    // Setup communication channels
    let (server_infos_tx, server_infos_rx): (Sender<Vec<ConnectionInfo>>, Receiver<Vec<ConnectionInfo>>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (num_sessions_tx, num_sessions_rx): (Sender<usize>, Receiver<usize>) = mpsc::channel();

    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let server_config = config.clone();
    let handle = thread::spawn(move || {
        multi_session_server_thread(
            server_infos_tx,
            client_info_rx,
            num_sessions_rx,
            server_ready_clone,
            server_stop,
            server_config,
        );
    });

    // Send client info and num_sessions
    client_info_tx.send(ConnectionInfo {
        qpn: client_info.qpn,
        qkey: client_info.qkey,
        lid: client_info.lid,
    }).ok()?;
    num_sessions_tx.send(num_sessions).ok()?;

    let server_infos = server_infos_rx.recv().ok()?;

    // Wait for server to be ready
    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Create sessions to server
    let mut sessions = Vec::with_capacity(num_sessions);
    for i in 0..num_sessions {
        let server_remote = RemoteInfo {
            qpn: server_infos[i].qpn,
            qkey: server_infos[i].qkey,
            lid: server_infos[i].lid,
        };
        let session = client.create_session(&server_remote).ok()?;
        sessions.push(session);
    }

    // Wait for all session handshakes
    let handshake_start = std::time::Instant::now();
    let handshake_timeout = Duration::from_secs(5);
    loop {
        if handshake_start.elapsed() >= handshake_timeout {
            eprintln!("Session handshake timeout");
            return None;
        }
        client.run_event_loop_once();
        let all_connected = sessions.iter().all(|&s| client.is_session_connected(s));
        if all_connected {
            break;
        }
        std::hint::spin_loop();
    }

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(MultiSessionBenchmarkSetup {
        client,
        sessions,
        _server_handle: server_handle,
    })
}

/// Server thread for multi-session benchmark (single Rpc handling multiple sessions)
fn multi_session_server_thread(
    infos_tx: Sender<Vec<ConnectionInfo>>,
    _client_info_rx: Receiver<ConnectionInfo>,
    num_sessions_rx: Receiver<usize>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    config: RpcConfig,
) {
    let (ctx, port, _port_attr) = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    // Receive client info (unused but kept for protocol)
    let _ = _client_info_rx.recv();
    let num_sessions = match num_sessions_rx.recv() {
        Ok(n) => n,
        Err(_) => return,
    };

    // Create single server Rpc
    // Sessions will be created on-demand when client sends ConnectRequest
    let server: Rpc<()> = match Rpc::new(&ctx, port, config, |_, _| {}) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Send server info (same for all sessions since it's the same Rpc/QP)
    let info = server.local_info();
    let server_infos: Vec<ConnectionInfo> = (0..num_sessions)
        .map(|_| ConnectionInfo {
            qpn: info.qpn,
            qkey: info.qkey,
            lid: info.lid,
        })
        .collect();

    if infos_tx.send(server_infos).is_err() {
        return;
    }

    ready_signal.store(1, Ordering::Release);

    // Server loop - sessions are auto-accepted via handle_connect_request
    let mut response_buf = vec![0u8; 4096];
    const MAX_BATCH: usize = 32;
    while !stop_flag.load(Ordering::Relaxed) {
        server.run_event_loop_once();
        let mut processed = 0;
        while processed < MAX_BATCH {
            if let Some(req) = server.recv() {
                let data = req.data(&server);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = server.reply(&req, &response_buf[..len]);
                processed += 1;
            } else {
                break;
            }
        }
    }
}

/// Run multi-session throughput benchmark (eRPC paper conditions)
fn run_multi_session_throughput_bench(
    client: &Rpc<usize>,
    sessions: &[erpc::SessionHandle],
    msg_size: usize,
    iters: u64,
    pipeline_depth_per_session: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let num_sessions = sessions.len();
    let iters_per_session = iters / num_sessions as u64;

    // Drain any pending completions from previous iterations
    for _ in 0..100 {
        client.run_event_loop_once();
    }

    // Record baseline completion count and stall count
    let baseline_completed = MULTI_SESSION_COMPLETED.with(|c| *c.borrow());
    let baseline_stall = client.stall_count();

    // Per-session tracking: sent count per session
    // This ensures we don't exceed per-session credit limits
    let mut sent_per_session = vec![0u64; num_sessions];
    let mut _total_sent: u64 = 0;

    let start = std::time::Instant::now();

    // Initial fill for all sessions (up to pipeline_depth_per_session each)
    // Use try_call() to avoid stall queue allocation
    for (i, &session) in sessions.iter().enumerate() {
        let to_send = pipeline_depth_per_session.min(iters_per_session as usize);
        for _ in 0..to_send {
            if client.try_call(session, 1, &request_data, 0).is_ok() {
                sent_per_session[i] += 1;
                _total_sent += 1;
            } else {
                // Credit exhausted for this session, move to next
                break;
            }
        }
    }

    // Main loop: simple round-robin with credit-aware retry
    // Use try_call() to avoid stall queue allocation on credit exhaustion
    let total_target = iters_per_session * num_sessions as u64;
    let mut session_idx = 0;
    loop {
        client.run_event_loop_once();

        let raw_completed = MULTI_SESSION_COMPLETED.with(|c| *c.borrow());
        let total_completed = raw_completed.saturating_sub(baseline_completed);
        if total_completed >= total_target {
            break;
        }

        // Try each session once per event loop iteration
        // try_call() returns Error::NoCredits without allocating if credits exhausted
        for _ in 0..num_sessions {
            let i = session_idx;
            session_idx = (session_idx + 1) % num_sessions;

            // Skip if this session has sent enough
            if sent_per_session[i] >= iters_per_session {
                continue;
            }

            // Try to send - try_call() returns error without allocating
            let session = sessions[i];
            if client.try_call(session, 1, &request_data, 0).is_ok() {
                sent_per_session[i] += 1;
                _total_sent += 1;
            }
            // Don't break on failure - try other sessions in this round
        }
    }

    // Print stall count periodically for debugging
    static ITER_COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let iter_num = ITER_COUNT.fetch_add(1, Ordering::Relaxed);
    let stalls_this_run = client.stall_count() - baseline_stall;
    if iter_num % 10 == 0 {
        eprintln!("DEBUG: stalls_this_run={}", stalls_this_run);
    }

    start.elapsed()
}

fn bench_erpc_paper_conditions(c: &mut Criterion) {
    // Test different session counts to find optimal configuration
    // eRPC paper achieves peak throughput with many sessions
    for num_sessions in [2, 8] {
        const PIPELINE_DEPTH: usize = 8; // B=8 per session

        let setup = match setup_multi_session_benchmark(num_sessions) {
            Some(s) => s,
            None => {
                eprintln!("Skipping eRPC paper benchmark ({}sessions): setup failed", num_sessions);
                continue;
            }
        };

        let client = setup.client;
        let sessions = setup.sessions;
        let _server = setup._server_handle;

        let mut group = c.benchmark_group("erpc_paper");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(5));
        group.warm_up_time(Duration::from_secs(3));
        group.throughput(Throughput::Elements(1));

        // 32B message with varying session count
        group.bench_function(
            BenchmarkId::new("32B", format!("{}sessions_B{}", num_sessions, PIPELINE_DEPTH)),
            |b| {
                b.iter_custom(|iters| {
                    run_multi_session_throughput_bench(
                        &client,
                        &sessions,
                        SMALL_MSG_SIZE,
                        iters,
                        PIPELINE_DEPTH,
                    )
                });
            },
        );

        group.finish();
    }
}

criterion_group!(benches, bench_latency, bench_throughput, bench_multi_qp_throughput, bench_erpc_paper_conditions);
criterion_main!(benches);
