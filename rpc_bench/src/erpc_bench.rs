use std::cell::RefCell;
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
    fn to_bytes(&self) -> Vec<u8> {
        let ptr = self as *const Self as *const u8;
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
    let device = device_list.iter().nth(device_index).expect("Device not found");
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
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("erpc one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            run_one_to_one(common, world, session_credits, req_window, *inflight as usize)
        }
        ModeCmd::MultiClient { inflight } => {
            if world.size() < 2 {
                if rank == 0 {
                    eprintln!("erpc multi-client requires at least 2 ranks");
                }
                return Vec::new();
            }
            run_multi_client(common, world, session_credits, req_window, *inflight as usize)
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
            run_erpc_client_duration(&rpc, session, common.message_size, inflight, duration, &mut collector);
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "erpc",
                "1to1",
                steady,
                common.message_size as u64,
                1,
                inflight as u32,
                1,
                run,
            );

            if !steady.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!("  Run {}: avg {:.0} RPS ({} steady epochs)", run + 1, avg_rps, steady.len());
            }

            all_rows.extend(rows);
        }

        all_rows
    } else {
        // Server
        let rpc: Rpc<()> = Rpc::new(&ctx, port, config, |_, _| {})
            .expect("Failed to create server Rpc");

        let local = rpc.local_info();
        let local_info = ErpcConnectionInfo {
            qpn: local.qpn,
            qkey: local.qkey,
            lid: local.lid,
            _pad: 0,
        };

        let remote_bytes = mpi_util::exchange_bytes(world, rank, 0, &local_info.to_bytes());
        let remote_info = ErpcConnectionInfo::from_bytes(&remote_bytes);

        let session = rpc.create_session(&RemoteInfo {
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
        let rpc: Rpc<()> = Rpc::new(&ctx, port, config, |_, _| {})
            .expect("Failed to create server Rpc");

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
            run_erpc_server_duration_with_epoch(&rpc, common.message_size, duration, &mut collector);
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "erpc",
                "multi_client",
                steady,
                common.message_size as u64,
                1,
                inflight as u32,
                num_clients as u32,
                run,
            );

            if !steady.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({} steady epochs)",
                    run + 1,
                    avg_rps,
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
            run_erpc_client_duration(&rpc, session, common.message_size, inflight, duration, &mut collector);
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
