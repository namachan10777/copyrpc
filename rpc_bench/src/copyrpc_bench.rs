use std::cell::Cell;
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use copyrpc::{Context, ContextBuilder, Endpoint, EndpointConfig, RemoteEndpointInfo};
use mlx5::srq::SrqConfig;

use crate::epoch::EpochCollector;
use crate::mpi_util;
use crate::parquet_out::{self, BenchRow};
use crate::{CommonConfig, ModeCmd};

// Fixed-size struct for MPI exchange.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct EndpointConnectionInfo {
    qp_number: u32,
    packet_sequence_number: u32,
    local_identifier: u16,
    _padding: u16,
    recv_ring_addr: u64,
    recv_ring_rkey: u32,
    _padding2: u32,
    recv_ring_size: u64,
    consumer_addr: u64,
    consumer_rkey: u32,
    _padding3: u32,
}

const CONNECTION_INFO_SIZE: usize = std::mem::size_of::<EndpointConnectionInfo>();

impl EndpointConnectionInfo {
    fn to_bytes(&self) -> Vec<u8> {
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, CONNECTION_INFO_SIZE).to_vec() }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= CONNECTION_INFO_SIZE);
        let mut info = Self::default();
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                CONNECTION_INFO_SIZE,
            );
        }
        info
    }
}

thread_local! {
    static RESPONSE_COUNT: Cell<u32> = const { Cell::new(0) };
}

fn on_response_callback(_user_data: (), _data: &[u8]) {
    RESPONSE_COUNT.with(|c| c.set(c.get() + 1));
}

fn get_and_reset_response_count() -> u32 {
    RESPONSE_COUNT.with(|c| c.replace(0))
}

pub fn run(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    ring_size: usize,
    mode: &ModeCmd,
) -> Vec<BenchRow> {
    let rank = world.rank();

    match mode {
        ModeCmd::OneToOne {
            endpoints,
            inflight,
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("copyrpc one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            run_one_to_one(
                common,
                world,
                ring_size,
                *endpoints as usize,
                *inflight as usize,
            )
        }
        ModeCmd::MultiClient { inflight } => {
            if world.size() < 2 {
                if rank == 0 {
                    eprintln!("copyrpc multi-client requires at least 2 ranks");
                }
                return Vec::new();
            }
            run_multi_client(common, world, ring_size, *inflight as usize)
        }
    }
}

fn run_one_to_one(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    ring_size: usize,
    num_endpoints: usize,
    inflight_per_ep: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let is_client = rank == 0;

    type OnResponseFn = fn((), &[u8]);
    let ctx: Context<(), OnResponseFn> = ContextBuilder::new()
        .device_index(common.device_index)
        .port(common.port)
        .srq_config(SrqConfig {
            max_wr: 16384,
            max_sge: 1,
        })
        .cq_size(4096)
        .on_response(on_response_callback as OnResponseFn)
        .build()
        .expect("Failed to create copyrpc context");

    let ep_config = EndpointConfig {
        send_ring_size: ring_size,
        recv_ring_size: ring_size,
        ..Default::default()
    };

    let mut endpoints = Vec::with_capacity(num_endpoints);
    let mut local_infos = Vec::with_capacity(num_endpoints);

    for _ in 0..num_endpoints {
        let ep = ctx
            .create_endpoint(&ep_config)
            .expect("Failed to create endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

        local_infos.push(EndpointConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            _padding: 0,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            _padding2: 0,
            recv_ring_size: info.recv_ring_size,
            consumer_addr: info.consumer_addr,
            consumer_rkey: info.consumer_rkey,
            _padding3: 0,
        });

        endpoints.push(ep);
    }

    // Exchange connection info via MPI
    let mut local_bytes = Vec::with_capacity(num_endpoints * CONNECTION_INFO_SIZE);
    for info in &local_infos {
        local_bytes.extend_from_slice(&info.to_bytes());
    }

    let remote_bytes = mpi_util::exchange_bytes(world, rank, 1 - rank, &local_bytes);

    // Parse remote info and connect
    for (i, ep) in endpoints.iter_mut().enumerate() {
        let start = i * CONNECTION_INFO_SIZE;
        let end = start + CONNECTION_INFO_SIZE;
        let remote_ep = EndpointConnectionInfo::from_bytes(&remote_bytes[start..end]);
        let remote = RemoteEndpointInfo {
            qp_number: remote_ep.qp_number,
            packet_sequence_number: remote_ep.packet_sequence_number,
            local_identifier: remote_ep.local_identifier,
            recv_ring_addr: remote_ep.recv_ring_addr,
            recv_ring_rkey: remote_ep.recv_ring_rkey,
            recv_ring_size: remote_ep.recv_ring_size,
            consumer_addr: remote_ep.consumer_addr,
            consumer_rkey: remote_ep.consumer_rkey,
        };
        ep.connect(&remote, 0, ctx.port())
            .expect("Failed to connect endpoint");
    }

    std::thread::sleep(Duration::from_millis(10));
    world.barrier();

    let mut all_rows = Vec::new();
    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);

    for run in 0..common.runs {
        world.barrier();

        if is_client {
            let mut collector = EpochCollector::new(interval);
            run_client_duration(
                &ctx,
                &endpoints,
                common.message_size,
                inflight_per_ep,
                duration,
                &mut collector,
            );
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "copyrpc",
                "1to1",
                steady,
                common.message_size as u64,
                num_endpoints as u32,
                inflight_per_ep as u32,
                1,
                run,
            );

            if !steady.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!("  Run {}: avg {:.0} RPS ({} steady epochs)", run + 1, avg_rps, steady.len());
            }

            all_rows.extend(rows);
        } else {
            run_server_duration(&ctx, common.message_size, duration);
        }
    }

    all_rows
}

fn run_client_duration<F: Fn((), &[u8])>(
    ctx: &Context<(), F>,
    endpoints: &[Endpoint<()>],
    message_size: usize,
    inflight_per_ep: usize,
    duration: Duration,
    collector: &mut EpochCollector,
) {
    let request_data = vec![0u8; message_size];
    let max_inflight = endpoints.len() * inflight_per_ep;
    let mut inflight = 0usize;

    get_and_reset_response_count();

    // Initial fill
    for ep in endpoints.iter() {
        for _ in 0..inflight_per_ep {
            if ep.call(&request_data, ()).is_ok() {
                inflight += 1;
            }
        }
    }
    ctx.poll();

    let start = Instant::now();
    let mut ep_idx = 0;

    while start.elapsed() < duration {
        ctx.poll();

        let new_completions = get_and_reset_response_count();
        if new_completions > 0 {
            inflight = inflight.saturating_sub(new_completions as usize);
            collector.record(new_completions as u64);
        }

        // Refill
        while inflight < max_inflight {
            let ep = &endpoints[ep_idx % endpoints.len()];
            ep_idx += 1;
            if ep.call(&request_data, ()).is_ok() {
                inflight += 1;
            } else {
                ctx.poll();
                let new = get_and_reset_response_count();
                inflight = inflight.saturating_sub(new as usize);
                if new > 0 {
                    collector.record(new as u64);
                }
                break;
            }
        }
    }

    // Drain
    while inflight > 0 {
        ctx.poll();
        let new = get_and_reset_response_count();
        inflight = inflight.saturating_sub(new as usize);
    }
}

fn run_server_duration<F: Fn((), &[u8])>(
    ctx: &Context<(), F>,
    message_size: usize,
    duration: Duration,
) {
    let response_data = vec![0u8; message_size];
    let start = Instant::now();

    while start.elapsed() < duration {
        ctx.poll();
        while let Some(req) = ctx.recv() {
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll();
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    // Drain remaining requests after duration
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        ctx.poll();
        let mut got_any = false;
        while let Some(req) = ctx.recv() {
            got_any = true;
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll();
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
        if !got_any {
            break;
        }
    }
    ctx.poll();
}

fn run_multi_client(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    ring_size: usize,
    inflight_per_client: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let size = world.size();
    let is_server = rank == 0;
    let num_clients = (size - 1) as usize;

    type OnResponseFn = fn((), &[u8]);
    let ctx: Context<(), OnResponseFn> = ContextBuilder::new()
        .device_index(common.device_index)
        .port(common.port)
        .srq_config(SrqConfig {
            max_wr: 16384,
            max_sge: 1,
        })
        .cq_size(4096)
        .on_response(on_response_callback as OnResponseFn)
        .build()
        .expect("Failed to create copyrpc context");

    let ep_config = EndpointConfig {
        send_ring_size: ring_size,
        recv_ring_size: ring_size,
        ..Default::default()
    };

    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);

    if is_server {
        // Server: create one endpoint per client
        let mut endpoints = Vec::with_capacity(num_clients);
        let mut local_infos = Vec::with_capacity(num_clients);

        for _ in 0..num_clients {
            let ep = ctx
                .create_endpoint(&ep_config)
                .expect("Failed to create endpoint");
            let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());
            local_infos.push(EndpointConnectionInfo {
                qp_number: info.qp_number,
                packet_sequence_number: 0,
                local_identifier: lid,
                _padding: 0,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                _padding2: 0,
                recv_ring_size: info.recv_ring_size,
                consumer_addr: info.consumer_addr,
                consumer_rkey: info.consumer_rkey,
                _padding3: 0,
            });
            endpoints.push(ep);
        }

        // Exchange connection info with each client
        for client_rank in 1..size {
            let i = (client_rank - 1) as usize;
            let local_bytes = local_infos[i].to_bytes();
            let remote_bytes =
                mpi_util::exchange_bytes(world, rank, client_rank, &local_bytes);
            let remote_ep = EndpointConnectionInfo::from_bytes(&remote_bytes);
            let remote = RemoteEndpointInfo {
                qp_number: remote_ep.qp_number,
                packet_sequence_number: remote_ep.packet_sequence_number,
                local_identifier: remote_ep.local_identifier,
                recv_ring_addr: remote_ep.recv_ring_addr,
                recv_ring_rkey: remote_ep.recv_ring_rkey,
                recv_ring_size: remote_ep.recv_ring_size,
                consumer_addr: remote_ep.consumer_addr,
                consumer_rkey: remote_ep.consumer_rkey,
            };
            endpoints[i]
                .connect(&remote, 0, ctx.port())
                .expect("Failed to connect endpoint");
        }

        std::thread::sleep(Duration::from_millis(10));
        world.barrier();

        let mut all_rows = Vec::new();

        for run in 0..common.runs {
            world.barrier();

            // Server side measures epoch
            let mut collector = EpochCollector::new(interval);
            run_server_duration_with_epoch(&ctx, common.message_size, duration, &mut collector);
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "copyrpc",
                "multi_client",
                steady,
                common.message_size as u64,
                1,
                inflight_per_client as u32,
                num_clients as u32,
                run,
            );

            if !steady.is_empty() {
                let avg_rps: f64 =
                    rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
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
        // Client: create one endpoint
        let ep = ctx
            .create_endpoint(&ep_config)
            .expect("Failed to create endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());
        let local_info = EndpointConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            _padding: 0,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            _padding2: 0,
            recv_ring_size: info.recv_ring_size,
            consumer_addr: info.consumer_addr,
            consumer_rkey: info.consumer_rkey,
            _padding3: 0,
        };

        let local_bytes = local_info.to_bytes();
        let remote_bytes = mpi_util::exchange_bytes(world, rank, 0, &local_bytes);
        let remote_ep = EndpointConnectionInfo::from_bytes(&remote_bytes);
        let remote = RemoteEndpointInfo {
            qp_number: remote_ep.qp_number,
            packet_sequence_number: remote_ep.packet_sequence_number,
            local_identifier: remote_ep.local_identifier,
            recv_ring_addr: remote_ep.recv_ring_addr,
            recv_ring_rkey: remote_ep.recv_ring_rkey,
            recv_ring_size: remote_ep.recv_ring_size,
            consumer_addr: remote_ep.consumer_addr,
            consumer_rkey: remote_ep.consumer_rkey,
        };

        let mut ep = ep;
        ep.connect(&remote, 0, ctx.port())
            .expect("Failed to connect endpoint");

        std::thread::sleep(Duration::from_millis(10));
        world.barrier();

        for _run in 0..common.runs {
            world.barrier();
            // Client just pumps requests for duration
            let mut collector = EpochCollector::new(interval);
            run_client_duration(
                &ctx,
                std::slice::from_ref(&ep),
                common.message_size,
                inflight_per_client,
                duration,
                &mut collector,
            );
        }

        Vec::new()
    }
}

fn run_server_duration_with_epoch<F: Fn((), &[u8])>(
    ctx: &Context<(), F>,
    message_size: usize,
    duration: Duration,
    collector: &mut EpochCollector,
) {
    let response_data = vec![0u8; message_size];
    let start = Instant::now();

    while start.elapsed() < duration {
        ctx.poll();
        while let Some(req) = ctx.recv() {
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll();
                        continue;
                    }
                    Err(_) => break,
                }
            }
            collector.record(1);
        }
    }

    // Drain remaining
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        ctx.poll();
        let mut got_any = false;
        while let Some(req) = ctx.recv() {
            got_any = true;
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll();
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
        if !got_any {
            break;
        }
    }
    ctx.poll();
}
