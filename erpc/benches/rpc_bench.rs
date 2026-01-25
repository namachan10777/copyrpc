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
use std::rc::Rc;
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

const SMALL_MSG_SIZE: usize = 32;
// Note: LARGE_MSG_SIZE must be less than MTU - PKT_HDR_SIZE (typically 4096 - 16 = 4080)
// Multi-packet messages are not yet supported in the benchmark
const LARGE_MSG_SIZE: usize = 4000;

// Multi-QP benchmark parameters
const NUM_QPS: usize = 1;
const PIPELINE_DEPTH_PER_QP: usize = 256;

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
    client: Rpc,
    session: erpc::SessionHandle,
    _server_handle: ServerHandle,
}

struct MultiQpBenchmarkSetup {
    clients: Vec<Rpc>,
    sessions: Vec<erpc::SessionHandle>,
    _server_handles: Vec<ServerHandle>,
}

fn setup_benchmark() -> Option<BenchmarkSetup> {
    set_cpu_affinity(CLIENT_CORE);

    let (ctx, port, _port_attr) = open_mlx5_device()?;

    let pipeline_depth = 1024;
    let config = RpcConfig::default()
        .with_req_window(pipeline_depth)
        .with_session_credits(pipeline_depth + 64)
        .with_num_recv_buffers(pipeline_depth * 2)
        .with_max_send_wr((pipeline_depth * 2) as u32)
        .with_max_recv_wr((pipeline_depth * 2) as u32);

    let client = Rpc::new(&ctx, port, config.clone()).ok()?;
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

    let handle = thread::spawn(move || {
        server_thread_main(server_info_tx, client_info_rx, server_ready_clone, server_stop);
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

fn setup_multi_qp_benchmark(num_qps: usize, pipeline_depth: usize) -> Option<MultiQpBenchmarkSetup> {
    set_cpu_affinity(CLIENT_CORE);

    let (ctx, port, _port_attr) = open_mlx5_device()?;

    let mut clients = Vec::with_capacity(num_qps);
    let mut sessions = Vec::with_capacity(num_qps);
    let mut server_handles = Vec::with_capacity(num_qps);

    for i in 0..num_qps {
        let config = RpcConfig::default()
            .with_req_window(pipeline_depth)
            .with_session_credits(pipeline_depth + 64)
            .with_num_recv_buffers(pipeline_depth * 2)
            .with_max_send_wr((pipeline_depth * 2) as u32)
            .with_max_recv_wr((pipeline_depth * 2) as u32);

        let client = Rpc::new(&ctx, port, config.clone()).ok()?;
        let client_info = client.local_info();
        let client_conn_info = ConnectionInfo {
            qpn: client_info.qpn,
            qkey: client_info.qkey,
            lid: client_info.lid,
        };

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
            server_thread_main_with_config(
                server_info_tx,
                client_info_rx,
                server_ready_clone,
                server_stop,
                server_config,
            );
        });

        client_info_tx.send(client_conn_info).ok()?;
        let server_info = server_info_rx.recv().ok()?;

        while server_ready.load(Ordering::Acquire) == 0 {
            std::hint::spin_loop();
        }

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

        // Wait for session handshake
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
            eprintln!("Session {} handshake failed", i);
            return None;
        }

        clients.push(client);
        sessions.push(session);
        server_handles.push(server_handle);
    }

    Some(MultiQpBenchmarkSetup {
        clients,
        sessions,
        _server_handles: server_handles,
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
    set_cpu_affinity(SERVER_CORE);

    let (ctx, port, _port_attr) = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    let server = match Rpc::new(&ctx, port, config) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Echo handler
    server.set_req_handler(|ctx, resp| {
        // Echo back the payload
        if let Err(e) = resp.respond(ctx.data) {
            eprintln!("Server failed to respond: {:?}", e);
        }
    });

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

    // Server loop
    while !stop_flag.load(Ordering::Relaxed) {
        server.run_event_loop_once();
        std::hint::spin_loop();
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Ping-pong latency benchmark (single request, wait for response).
fn run_latency_bench(
    client: &Rpc,
    session: erpc::SessionHandle,
    msg_size: usize,
    iters: u64,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let completed = Rc::new(RefCell::new(0u64));

    let start = std::time::Instant::now();

    for _ in 0..iters {
        let completed_clone = completed.clone();

        if client
            .enqueue_request(session, 1, &request_data, move |_, _| {
                *completed_clone.borrow_mut() += 1;
            })
            .is_err()
        {
            continue;
        }

        // Wait for response
        let target = *completed.borrow() + 1;
        while *completed.borrow() < target {
            client.run_event_loop_once();
            std::hint::spin_loop();
        }
    }

    start.elapsed()
}

/// Throughput benchmark (pipelined requests).
fn run_throughput_bench(
    client: &Rpc,
    session: erpc::SessionHandle,
    msg_size: usize,
    iters: u64,
    pipeline_depth: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let completed = Rc::new(RefCell::new(0u64));
    let mut sent = 0u64;

    let start = std::time::Instant::now();

    // Initial fill
    for _ in 0..pipeline_depth.min(iters as usize) {
        let completed_clone = completed.clone();

        if client
            .enqueue_request(session, 1, &request_data, move |_, _| {
                *completed_clone.borrow_mut() += 1;
            })
            .is_ok()
        {
            sent += 1;
        }
    }

    // Main loop
    while *completed.borrow() < iters {
        client.run_event_loop_once();

        // Send more requests if we have capacity
        while sent < iters && sent - *completed.borrow() < pipeline_depth as u64 {
            let completed_clone = completed.clone();

            if client
                .enqueue_request(session, 1, &request_data, move |_, _| {
                    *completed_clone.borrow_mut() += 1;
                })
                .is_ok()
            {
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
    clients: &[Rpc],
    sessions: &[erpc::SessionHandle],
    msg_size: usize,
    iters: u64,
    pipeline_depth: usize,
) -> Duration {
    let request_data = vec![0xAAu8; msg_size];
    let num_qps = clients.len();
    let iters_per_qp = iters / num_qps as u64;

    // Per-QP completion counters
    let completeds: Vec<Rc<RefCell<u64>>> = (0..num_qps)
        .map(|_| Rc::new(RefCell::new(0u64)))
        .collect();
    let mut sents: Vec<u64> = vec![0; num_qps];

    let start = std::time::Instant::now();

    // Initial fill for all QPs
    for qp_idx in 0..num_qps {
        for _ in 0..pipeline_depth.min(iters_per_qp as usize) {
            let completed_clone = completeds[qp_idx].clone();

            if clients[qp_idx]
                .enqueue_request(sessions[qp_idx], 1, &request_data, move |_, _| {
                    *completed_clone.borrow_mut() += 1;
                })
                .is_ok()
            {
                sents[qp_idx] += 1;
            }
        }
    }

    // Main loop - round-robin across all QPs
    let mut total_completed = 0u64;
    let total_target = iters_per_qp * num_qps as u64;

    while total_completed < total_target {
        // Process all QPs
        for qp_idx in 0..num_qps {
            clients[qp_idx].run_event_loop_once();

            // Send more requests if we have capacity
            let completed = *completeds[qp_idx].borrow();
            while sents[qp_idx] < iters_per_qp
                && sents[qp_idx] - completed < pipeline_depth as u64
            {
                let completed_clone = completeds[qp_idx].clone();

                if clients[qp_idx]
                    .enqueue_request(sessions[qp_idx], 1, &request_data, move |_, _| {
                        *completed_clone.borrow_mut() += 1;
                    })
                    .is_ok()
                {
                    sents[qp_idx] += 1;
                } else {
                    break;
                }
            }
        }

        // Update total completed
        total_completed = completeds.iter().map(|c| *c.borrow()).sum();

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

    // Pipeline depth 16
    let pipeline_depth = 16;

    // 32B message throughput
    group.bench_function(
        BenchmarkId::new("pipelined", format!("32B_depth{}", pipeline_depth)),
        |b| {
            b.iter_custom(|iters| {
                run_throughput_bench(&client, session, SMALL_MSG_SIZE, iters, pipeline_depth)
            });
        },
    );

    // 4KB message throughput
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

criterion_group!(benches, bench_latency, bench_throughput, bench_multi_qp_throughput);
criterion_main!(benches);
