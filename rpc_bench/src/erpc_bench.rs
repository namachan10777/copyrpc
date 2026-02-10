use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use erpc::{RemoteInfo, Rpc, RpcConfig};
use mlx5::device::DeviceList;

use crate::epoch::EpochCollector;
use crate::mpi_util;
use crate::parquet_out::{self, BenchRow};
use crate::{CommonConfig, ModeCmd};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct ErpcConnectionInfo {
    qpn: u32,
    qkey: u32,
    lid: u16,
    _pad: u16,
}

const ERPC_INFO_SIZE: usize = std::mem::size_of::<ErpcConnectionInfo>();

impl ErpcConnectionInfo {
    fn to_bytes(self) -> Vec<u8> {
        let ptr = &self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, ERPC_INFO_SIZE).to_vec() }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= ERPC_INFO_SIZE);
        let mut info = Self::default();
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                ERPC_INFO_SIZE,
            );
        }
        info
    }
}

thread_local! {
    static COMPLETED: RefCell<u64> = const { RefCell::new(0) };
}

fn open_mlx5_device(device_index: usize, port: u8) -> (mlx5::device::Context, u8) {
    let device_list = DeviceList::list().expect("Failed to list devices");
    let device = device_list.get(device_index).expect("Device not found");
    let ctx = device.open().expect("Failed to open device");
    (ctx, port)
}

pub fn run(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    session_credits: usize,
    req_window: usize,
    mode: &ModeCmd,
) -> Vec<BenchRow> {
    let rank = world.rank();

    match mode {
        ModeCmd::OneToOne {
            endpoints: _,
            inflight,
            threads,
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("erpc one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            let num_threads = *threads as usize;
            if num_threads > 1 {
                run_one_to_one_threaded(
                    common,
                    world,
                    session_credits,
                    req_window,
                    *inflight as usize,
                    num_threads,
                )
            } else {
                run_one_to_one(
                    common,
                    world,
                    session_credits,
                    req_window,
                    *inflight as usize,
                )
            }
        }
        ModeCmd::MultiClient { inflight } => {
            if world.size() < 2 {
                if rank == 0 {
                    eprintln!("erpc multi-client requires at least 2 ranks");
                }
                return Vec::new();
            }
            run_multi_client(
                common,
                world,
                session_credits,
                req_window,
                *inflight as usize,
            )
        }
    }
}

fn run_one_to_one(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    session_credits: usize,
    req_window: usize,
    inflight: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let is_client = rank == 0;
    crate::affinity::pin_thread_if_configured(
        common.affinity_mode,
        common.affinity_start,
        rank,
        1,
        0,
    );

    let (ctx, port) = open_mlx5_device(common.device_index, common.port);

    let config = RpcConfig::default()
        .with_req_window(req_window)
        .with_session_credits(session_credits)
        .with_num_recv_buffers(req_window * 4)
        .with_max_send_wr((req_window * 4) as u32)
        .with_max_recv_wr((req_window * 4) as u32);

    if is_client {
        let rpc: Rpc<()> = Rpc::new(&ctx, port, config, |_, _| {
            COMPLETED.with(|c| *c.borrow_mut() += 1);
        })
        .expect("Failed to create client Rpc");

        let local = rpc.local_info();
        let local_info = ErpcConnectionInfo {
            qpn: local.qpn,
            qkey: local.qkey,
            lid: local.lid,
            _pad: 0,
        };

        let remote_bytes = mpi_util::exchange_bytes(world, rank, 1, &local_info.to_bytes());
        let remote_info = ErpcConnectionInfo::from_bytes(&remote_bytes);

        let session = rpc
            .create_session(&RemoteInfo {
                qpn: remote_info.qpn,
                qkey: remote_info.qkey,
                lid: remote_info.lid,
            })
            .expect("Failed to create session");

        // Wait for handshake
        let handshake_start = Instant::now();
        while handshake_start.elapsed() < Duration::from_secs(5) {
            rpc.run_event_loop_once();
            if rpc.is_session_connected(session) {
                break;
            }
            std::hint::spin_loop();
        }
        assert!(
            rpc.is_session_connected(session),
            "eRPC session handshake failed"
        );

        world.barrier();

        let duration = Duration::from_secs(common.duration_secs);
        let interval = Duration::from_millis(common.interval_ms);
        let mut all_rows = Vec::new();

        for run in 0..common.runs {
            world.barrier();

            let mut collector = EpochCollector::new(interval);
            run_erpc_client_duration(
                &rpc,
                session,
                common.message_size,
                inflight,
                duration,
                &mut collector,
            );
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let filtered = crate::epoch::filter_bottom_quartile(steady);
            let timestamp = parquet_out::now_unix_secs();
            let rows = parquet_out::rows_from_epochs(
                "erpc",
                "1to1",
                &filtered,
                common.message_size as u64,
                1,
                inflight as u32,
                1,
                1,
                run,
                timestamp,
            );

            if !filtered.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({}/{} epochs)",
                    run + 1,
                    avg_rps,
                    filtered.len(),
                    steady.len()
                );
            }

            all_rows.extend(rows);
        }

        all_rows
    } else {
        // Server
        let rpc: Rpc<()> =
            Rpc::new(&ctx, port, config, |_, _| {}).expect("Failed to create server Rpc");

        let local = rpc.local_info();
        let local_info = ErpcConnectionInfo {
            qpn: local.qpn,
            qkey: local.qkey,
            lid: local.lid,
            _pad: 0,
        };

        let remote_bytes = mpi_util::exchange_bytes(world, rank, 0, &local_info.to_bytes());
        let remote_info = ErpcConnectionInfo::from_bytes(&remote_bytes);

        let session = rpc
            .create_session(&RemoteInfo {
                qpn: remote_info.qpn,
                qkey: remote_info.qkey,
                lid: remote_info.lid,
            })
            .expect("Failed to create server session");

        // Wait for handshake on server side too
        let handshake_start = Instant::now();
        while handshake_start.elapsed() < Duration::from_secs(5) {
            rpc.run_event_loop_once();
            if rpc.is_session_connected(session) {
                break;
            }
            std::hint::spin_loop();
        }
        assert!(
            rpc.is_session_connected(session),
            "eRPC server session handshake failed"
        );

        world.barrier();

        let duration = Duration::from_secs(common.duration_secs);

        for _run in 0..common.runs {
            world.barrier();
            run_erpc_server_duration(&rpc, common.message_size, duration);
        }

        Vec::new()
    }
}

fn run_one_to_one_threaded(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    session_credits: usize,
    req_window: usize,
    inflight: usize,
    num_threads: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let is_client = rank == 0;

    // Phase 1: spawn workers, each creates its own Rpc + session
    let (info_tx, info_rx) = std::sync::mpsc::channel::<(usize, ErpcConnectionInfo)>();
    let (remote_txs, remote_rxs): (Vec<_>, Vec<_>) = (0..num_threads)
        .map(|_| std::sync::mpsc::channel::<ErpcConnectionInfo>())
        .unzip();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(std::sync::Barrier::new(num_threads + 1));
    let completions: Vec<Arc<AtomicU64>> = (0..num_threads)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let mut handles = Vec::with_capacity(num_threads);

    let affinity_mode = common.affinity_mode;
    let affinity_start = common.affinity_start;

    for (tid, remote_rx) in remote_rxs.into_iter().enumerate() {
        let info_tx = info_tx.clone();
        let stop = stop_flag.clone();
        let bar = barrier.clone();
        let comp = completions[tid].clone();
        let device_index = common.device_index;
        let port = common.port;
        let message_size = common.message_size;

        handles.push(std::thread::spawn(move || {
            crate::affinity::pin_thread_if_configured(
                affinity_mode,
                affinity_start,
                rank,
                num_threads,
                tid,
            );

            let (ctx, mlx5_port) = open_mlx5_device(device_index, port);

            let config = RpcConfig::default()
                .with_req_window(req_window)
                .with_session_credits(session_credits)
                .with_num_recv_buffers(req_window * 4)
                .with_max_send_wr((req_window * 4) as u32)
                .with_max_recv_wr((req_window * 4) as u32);

            if is_client {
                let rpc: Rpc<()> = Rpc::new(&ctx, mlx5_port, config, |_, _| {
                    COMPLETED.with(|c| *c.borrow_mut() += 1);
                })
                .expect("Failed to create client Rpc");

                let local = rpc.local_info();
                info_tx
                    .send((
                        tid,
                        ErpcConnectionInfo {
                            qpn: local.qpn,
                            qkey: local.qkey,
                            lid: local.lid,
                            _pad: 0,
                        },
                    ))
                    .unwrap();

                let remote_info = remote_rx.recv().unwrap();
                let session = rpc
                    .create_session(&RemoteInfo {
                        qpn: remote_info.qpn,
                        qkey: remote_info.qkey,
                        lid: remote_info.lid,
                    })
                    .expect("Failed to create session");

                // Handshake
                let handshake_start = Instant::now();
                while handshake_start.elapsed() < Duration::from_secs(5) {
                    rpc.run_event_loop_once();
                    if rpc.is_session_connected(session) {
                        break;
                    }
                    std::hint::spin_loop();
                }
                assert!(
                    rpc.is_session_connected(session),
                    "eRPC session handshake failed on thread {}",
                    tid
                );

                bar.wait(); // post-connect barrier

                loop {
                    bar.wait(); // run start
                    if stop.load(Ordering::Acquire) {
                        break;
                    }

                    run_erpc_client_duration_atomic(
                        &rpc,
                        session,
                        message_size,
                        inflight,
                        &comp,
                        &stop,
                    );

                    bar.wait(); // run end
                }
            } else {
                // Server thread
                let rpc: Rpc<()> = Rpc::new(&ctx, mlx5_port, config, |_, _| {})
                    .expect("Failed to create server Rpc");

                let local = rpc.local_info();
                info_tx
                    .send((
                        tid,
                        ErpcConnectionInfo {
                            qpn: local.qpn,
                            qkey: local.qkey,
                            lid: local.lid,
                            _pad: 0,
                        },
                    ))
                    .unwrap();

                let remote_info = remote_rx.recv().unwrap();
                let session = rpc
                    .create_session(&RemoteInfo {
                        qpn: remote_info.qpn,
                        qkey: remote_info.qkey,
                        lid: remote_info.lid,
                    })
                    .expect("Failed to create server session");

                // Handshake
                let handshake_start = Instant::now();
                while handshake_start.elapsed() < Duration::from_secs(5) {
                    rpc.run_event_loop_once();
                    if rpc.is_session_connected(session) {
                        break;
                    }
                    std::hint::spin_loop();
                }
                assert!(
                    rpc.is_session_connected(session),
                    "eRPC server session handshake failed on thread {}",
                    tid
                );

                bar.wait(); // post-connect barrier

                loop {
                    bar.wait(); // run start
                    if stop.load(Ordering::Acquire) {
                        break;
                    }

                    run_erpc_server_duration_atomic(&rpc, message_size, &stop);

                    bar.wait(); // run end
                }
            }
        }));
    }
    drop(info_tx);

    // Main thread: collect local infos ordered by thread id
    let mut all_local_infos: Vec<Option<ErpcConnectionInfo>> =
        (0..num_threads).map(|_| None).collect();
    for _ in 0..num_threads {
        let (tid, info) = info_rx.recv().expect("Failed to receive info from worker");
        all_local_infos[tid] = Some(info);
    }
    let all_local_infos: Vec<ErpcConnectionInfo> =
        all_local_infos.into_iter().map(|o| o.unwrap()).collect();

    // Phase 2: MPI exchange (T infos per side)
    let mut local_bytes = Vec::with_capacity(num_threads * ERPC_INFO_SIZE);
    for info in &all_local_infos {
        local_bytes.extend_from_slice(&info.to_bytes());
    }

    let remote_bytes = mpi_util::exchange_bytes(world, rank, 1 - rank, &local_bytes);

    // Distribute remote infos
    for (tid, remote_tx) in remote_txs.iter().enumerate() {
        let offset = tid * ERPC_INFO_SIZE;
        let remote_info =
            ErpcConnectionInfo::from_bytes(&remote_bytes[offset..offset + ERPC_INFO_SIZE]);
        remote_tx.send(remote_info).unwrap();
    }

    // Wait for all handshakes
    std::thread::sleep(Duration::from_millis(100));
    world.barrier();
    barrier.wait(); // release workers after MPI barrier + handshakes

    // Phase 3: run benchmarks
    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);
    let mut all_rows = Vec::new();

    for run in 0..common.runs {
        world.barrier();

        for c in &completions {
            c.store(0, Ordering::Release);
        }
        stop_flag.store(false, Ordering::Release);

        barrier.wait(); // start run

        if is_client {
            let mut collector = EpochCollector::new(interval);
            let start = Instant::now();

            while start.elapsed() < duration {
                std::thread::sleep(Duration::from_millis(1));
                let mut total_delta = 0u64;
                for c in &completions {
                    total_delta += c.swap(0, Ordering::Relaxed);
                }
                if total_delta > 0 {
                    collector.record(total_delta);
                }
            }

            stop_flag.store(true, Ordering::Release);
            barrier.wait(); // wait for run end

            let mut remaining = 0u64;
            for c in &completions {
                remaining += c.swap(0, Ordering::Relaxed);
            }
            if remaining > 0 {
                collector.record(remaining);
            }
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let timestamp = parquet_out::now_unix_secs();
            let filtered = crate::epoch::filter_bottom_quartile(steady);
            let rows = parquet_out::rows_from_epochs(
                "erpc",
                "1to1",
                &filtered,
                common.message_size as u64,
                1,
                inflight as u32,
                1,
                num_threads as u32,
                run,
                timestamp,
            );

            if !filtered.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({}/{} epochs, {} threads)",
                    run + 1,
                    avg_rps,
                    filtered.len(),
                    steady.len(),
                    num_threads
                );
            }

            all_rows.extend(rows);
        } else {
            std::thread::sleep(duration);
            stop_flag.store(true, Ordering::Release);
            barrier.wait();
        }
    }

    stop_flag.store(true, Ordering::Release);
    barrier.wait();

    for h in handles {
        h.join().expect("Worker thread panicked");
    }

    all_rows
}

fn run_erpc_client_duration_atomic(
    rpc: &Rpc<()>,
    session: erpc::SessionHandle,
    message_size: usize,
    inflight: usize,
    completions: &AtomicU64,
    stop_flag: &AtomicBool,
) {
    let request_data = vec![0xAAu8; message_size];

    COMPLETED.with(|c| *c.borrow_mut() = 0);
    let mut sent = 0u64;
    let mut completed_base = 0u64;

    // Initial fill
    for _ in 0..inflight {
        if rpc.try_call(session, 1, &request_data, ()).is_ok() {
            sent += 1;
        } else {
            break;
        }
    }

    while !stop_flag.load(Ordering::Relaxed) {
        rpc.run_event_loop_once();

        let total_completed = COMPLETED.with(|c| *c.borrow());
        let new = total_completed - completed_base;
        if new > 0 {
            completed_base = total_completed;
            completions.fetch_add(new, Ordering::Relaxed);
        }

        // Refill
        while sent - total_completed < inflight as u64 {
            if rpc.try_call(session, 1, &request_data, ()).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }
    }

    // Drain
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        rpc.run_event_loop_once();
        let total_completed = COMPLETED.with(|c| *c.borrow());
        if total_completed >= sent {
            break;
        }
    }
}

fn run_erpc_server_duration_atomic(rpc: &Rpc<()>, message_size: usize, stop_flag: &AtomicBool) {
    let mut response_buf = vec![0u8; message_size];

    while !stop_flag.load(Ordering::Relaxed) {
        rpc.run_event_loop_once();
        for _ in 0..16 {
            if let Some(req) = rpc.recv() {
                let data = req.data(rpc);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = rpc.reply(&req, &response_buf[..len]);
            } else {
                break;
            }
        }
    }

    // Drain
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        rpc.run_event_loop_once();
        let mut got_any = false;
        for _ in 0..16 {
            if let Some(req) = rpc.recv() {
                got_any = true;
                let data = req.data(rpc);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = rpc.reply(&req, &response_buf[..len]);
            } else {
                break;
            }
        }
        if !got_any {
            break;
        }
    }
}

fn run_erpc_client_duration(
    rpc: &Rpc<()>,
    session: erpc::SessionHandle,
    message_size: usize,
    inflight: usize,
    duration: Duration,
    collector: &mut EpochCollector,
) {
    let request_data = vec![0xAAu8; message_size];

    COMPLETED.with(|c| *c.borrow_mut() = 0);
    let mut sent = 0u64;
    let mut completed_base = 0u64;

    // Initial fill
    for _ in 0..inflight {
        if rpc.try_call(session, 1, &request_data, ()).is_ok() {
            sent += 1;
        } else {
            break;
        }
    }

    let start = Instant::now();

    while start.elapsed() < duration {
        rpc.run_event_loop_once();

        let total_completed = COMPLETED.with(|c| *c.borrow());
        let new = total_completed - completed_base;
        if new > 0 {
            completed_base = total_completed;
            collector.record(new);
        }

        // Refill
        while sent - total_completed < inflight as u64 {
            if rpc.try_call(session, 1, &request_data, ()).is_ok() {
                sent += 1;
            } else {
                break;
            }
        }
    }

    // Drain
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        rpc.run_event_loop_once();
        let total_completed = COMPLETED.with(|c| *c.borrow());
        if total_completed >= sent {
            break;
        }
    }
}

fn run_erpc_server_duration(rpc: &Rpc<()>, message_size: usize, duration: Duration) {
    let mut response_buf = vec![0u8; message_size];
    let start = Instant::now();

    while start.elapsed() < duration {
        rpc.run_event_loop_once();
        for _ in 0..16 {
            if let Some(req) = rpc.recv() {
                let data = req.data(rpc);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = rpc.reply(&req, &response_buf[..len]);
            } else {
                break;
            }
        }
    }

    // Drain
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        rpc.run_event_loop_once();
        let mut got_any = false;
        for _ in 0..16 {
            if let Some(req) = rpc.recv() {
                got_any = true;
                let data = req.data(rpc);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = rpc.reply(&req, &response_buf[..len]);
            } else {
                break;
            }
        }
        if !got_any {
            break;
        }
    }
}

fn run_multi_client(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    session_credits: usize,
    req_window: usize,
    inflight: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let size = world.size();
    let is_server = rank == 0;
    let num_clients = (size - 1) as usize;
    crate::affinity::pin_thread_if_configured(
        common.affinity_mode,
        common.affinity_start,
        rank,
        1,
        0,
    );

    let (ctx, port) = open_mlx5_device(common.device_index, common.port);

    let config = RpcConfig::default()
        .with_req_window(req_window)
        .with_session_credits(session_credits)
        .with_num_recv_buffers(req_window * 4)
        .with_max_send_wr((req_window * 4) as u32)
        .with_max_recv_wr((req_window * 4) as u32);

    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);

    if is_server {
        // Server: one Rpc, sessions auto-accepted
        let rpc: Rpc<()> =
            Rpc::new(&ctx, port, config, |_, _| {}).expect("Failed to create server Rpc");

        let local = rpc.local_info();
        let local_info = ErpcConnectionInfo {
            qpn: local.qpn,
            qkey: local.qkey,
            lid: local.lid,
            _pad: 0,
        };

        // Exchange info with each client
        for client_rank in 1..size {
            let remote_bytes =
                mpi_util::exchange_bytes(world, rank, client_rank, &local_info.to_bytes());
            let _remote_info = ErpcConnectionInfo::from_bytes(&remote_bytes);
            // Sessions are auto-accepted; we just need to run event loop to process connect requests
        }

        // Wait for all sessions to be established
        let handshake_start = Instant::now();
        while handshake_start.elapsed() < Duration::from_secs(5) {
            rpc.run_event_loop_once();
            if rpc.active_sessions() >= num_clients {
                break;
            }
            std::hint::spin_loop();
        }

        world.barrier();

        let mut all_rows = Vec::new();

        for run in 0..common.runs {
            world.barrier();

            let mut collector = EpochCollector::new(interval);
            run_erpc_server_duration_with_epoch(
                &rpc,
                common.message_size,
                duration,
                &mut collector,
            );
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let timestamp = parquet_out::now_unix_secs();
            let filtered = crate::epoch::filter_bottom_quartile(steady);
            let rows = parquet_out::rows_from_epochs(
                "erpc",
                "multi_client",
                &filtered,
                common.message_size as u64,
                1,
                inflight as u32,
                num_clients as u32,
                1,
                run,
                timestamp,
            );

            if !filtered.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({}/{} epochs)",
                    run + 1,
                    avg_rps,
                    filtered.len(),
                    steady.len()
                );
            }

            all_rows.extend(rows);
        }

        all_rows
    } else {
        // Client: one Rpc, one session to server
        let rpc: Rpc<()> = Rpc::new(&ctx, port, config, |_, _| {
            COMPLETED.with(|c| *c.borrow_mut() += 1);
        })
        .expect("Failed to create client Rpc");

        let local = rpc.local_info();
        let local_info = ErpcConnectionInfo {
            qpn: local.qpn,
            qkey: local.qkey,
            lid: local.lid,
            _pad: 0,
        };

        let remote_bytes = mpi_util::exchange_bytes(world, rank, 0, &local_info.to_bytes());
        let remote_info = ErpcConnectionInfo::from_bytes(&remote_bytes);

        let session = rpc
            .create_session(&RemoteInfo {
                qpn: remote_info.qpn,
                qkey: remote_info.qkey,
                lid: remote_info.lid,
            })
            .expect("Failed to create session");

        // Wait for handshake
        let handshake_start = Instant::now();
        while handshake_start.elapsed() < Duration::from_secs(5) {
            rpc.run_event_loop_once();
            if rpc.is_session_connected(session) {
                break;
            }
            std::hint::spin_loop();
        }
        assert!(
            rpc.is_session_connected(session),
            "eRPC session handshake failed"
        );

        world.barrier();

        for _run in 0..common.runs {
            world.barrier();

            let mut collector = EpochCollector::new(interval);
            run_erpc_client_duration(
                &rpc,
                session,
                common.message_size,
                inflight,
                duration,
                &mut collector,
            );
        }

        Vec::new()
    }
}

fn run_erpc_server_duration_with_epoch(
    rpc: &Rpc<()>,
    message_size: usize,
    duration: Duration,
    collector: &mut EpochCollector,
) {
    let mut response_buf = vec![0u8; message_size];
    let start = Instant::now();

    while start.elapsed() < duration {
        rpc.run_event_loop_once();
        for _ in 0..16 {
            if let Some(req) = rpc.recv() {
                let data = req.data(rpc);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = rpc.reply(&req, &response_buf[..len]);
                collector.record(1);
            } else {
                break;
            }
        }
    }

    // Drain
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        rpc.run_event_loop_once();
        let mut got_any = false;
        for _ in 0..16 {
            if let Some(req) = rpc.recv() {
                got_any = true;
                let data = req.data(rpc);
                let len = data.len().min(response_buf.len());
                response_buf[..len].copy_from_slice(&data[..len]);
                let _ = rpc.reply(&req, &response_buf[..len]);
            } else {
                break;
            }
        }
        if !got_any {
            break;
        }
    }
}
