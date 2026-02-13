mod affinity;
mod client;
mod copyrpc_direct_backend;
mod daemon;
mod delegation_backend;
mod erpc_backend;
mod message;
mod mpi_util;
mod parquet_out;
mod qd_sample;
mod storage;
mod ucx_am;
mod ucx_am_backend;
mod workload;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use clap::Parser;
use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use daemon::{CopyrpcSetup, DaemonFlux, EndpointConnectionInfo};
use message::{DelegatePayload, Request, Response};

#[derive(Parser, Debug)]
#[command(name = "benchkv")]
#[command(about = "Distributed KV benchmark over copyrpc + inproc + ipc")]
struct Cli {
    /// Benchmark duration in seconds
    #[arg(short = 'd', long, default_value = "30")]
    duration: u64,

    /// Batch size for measurement
    #[arg(long, default_value = "100")]
    batch_size: u32,

    /// Number of runs
    #[arg(short = 'r', long, default_value = "3")]
    runs: u32,

    /// Output parquet file
    #[arg(short = 'o', long, default_value = "benchkv.parquet")]
    output: String,

    /// RDMA device index
    #[arg(long, default_value = "0")]
    device_index: usize,

    /// RDMA port number
    #[arg(long, default_value = "1")]
    port: u8,

    /// Daemon threads per node
    #[arg(long, default_value = "1")]
    server_threads: usize,

    /// Client threads per node
    #[arg(long, default_value = "1")]
    client_threads: usize,

    /// Queue depth per client (power of 2)
    #[arg(long, default_value = "4")]
    queue_depth: u32,

    /// Key range per node
    #[arg(long, default_value = "1024")]
    key_range: u64,

    /// Read ratio (0.0 = all writes, 1.0 = all reads)
    #[arg(long, default_value = "0.5")]
    read_ratio: f64,

    /// Key distribution
    #[arg(long, default_value = "uniform")]
    distribution: workload::KeyDistribution,

    /// copyrpc ring buffer size
    #[arg(long, default_value = "4194304")]
    ring_size: usize,

    /// Output directory for queue-depth samples (CSV). Disabled if not set.
    #[arg(long)]
    qd_sample_dir: Option<String>,

    /// Queue-depth sampling interval (every N loop iterations)
    #[arg(long, default_value = "1024")]
    qd_sample_interval: u32,

    #[command(subcommand)]
    subcommand: SubCmd,
}

#[derive(clap::Subcommand, Debug)]
enum SubCmd {
    /// copyrpc meta_put/meta_get benchmark
    Meta,
    /// eRPC meta_put/meta_get benchmark
    Erpc {
        /// Session credits
        #[arg(long, default_value = "32")]
        session_credits: usize,
        /// Request window size
        #[arg(long, default_value = "8")]
        req_window: usize,
    },
    /// UCX Active Message meta_put/meta_get benchmark
    UcxAm,
    /// copyrpc direct (no ipc/Flux) meta_put/meta_get benchmark
    CopyrpcDirect,
    /// Delegation: clients submit remote requests via shared-memory MPSC to Daemon#0
    Delegation {
        /// Recv coalescing RTT estimate [us] (0 = disabled)
        #[arg(long, default_value = "0.0")]
        coalesce_rtt_us: f64,
        /// Reply batch hold time [us] (0 = disabled, initial value for adaptive)
        #[arg(long, default_value = "10.0")]
        batch_hold_us: f64,
        /// Enable arrival-rate feedback to dynamically adjust hold time
        #[arg(long, default_value = "true")]
        adaptive_hold: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    let (universe, _threading) =
        mpi::initialize_with_threading(mpi::Threading::Funneled).expect("Failed to initialize MPI");
    let world = universe.world();
    let rank = world.rank() as u32;
    let size = world.size() as u32;

    let num_daemons = cli.server_threads;
    let num_clients = cli.client_threads;

    eprintln!(
        "rank {}: {} daemons, {} clients, QD={}, key_range={}, ranks={}, mode={:?}",
        rank, num_daemons, num_clients, cli.queue_depth, cli.key_range, size, cli.subcommand
    );

    let all_rows = match &cli.subcommand {
        SubCmd::Meta => run_meta(&cli, &world, rank, size, num_daemons, num_clients),
        SubCmd::Erpc {
            session_credits,
            req_window,
        } => erpc_backend::run_erpc(
            &cli,
            &world,
            rank,
            size,
            num_daemons,
            num_clients,
            *session_credits,
            *req_window,
        ),
        SubCmd::UcxAm => {
            ucx_am_backend::run_ucx_am(&cli, &world, rank, size, num_daemons, num_clients)
        }
        SubCmd::CopyrpcDirect => copyrpc_direct_backend::run_copyrpc_direct(
            &cli,
            &world,
            rank,
            size,
            num_daemons,
            num_clients,
        ),
        SubCmd::Delegation {
            coalesce_rtt_us,
            batch_hold_us,
            adaptive_hold,
        } => delegation_backend::run_delegation(
            &cli,
            &world,
            rank,
            size,
            num_daemons,
            num_clients,
            *coalesce_rtt_us,
            *batch_hold_us,
            *adaptive_hold,
        ),
    };

    // Rank 0: write parquet
    if rank == 0 {
        if let Err(e) = parquet_out::write_parquet(&cli.output, &all_rows) {
            eprintln!("Error writing parquet: {}", e);
        } else if !all_rows.is_empty() {
            eprintln!("Results written to {}", cli.output);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_meta(
    cli: &Cli,
    world: &mpi::topology::SimpleCommunicator,
    rank: u32,
    size: u32,
    num_daemons: usize,
    num_clients: usize,
) -> Vec<parquet_out::BenchRow> {
    let queue_depth = cli.queue_depth;

    // CPU affinity
    let available_cores = affinity::get_available_cores(cli.device_index);
    let (ranks_on_node, rank_on_node) = mpi_util::node_local_rank(world);
    let (daemon_cores, client_cores) = affinity::assign_cores(
        &available_cores,
        num_daemons,
        num_clients,
        ranks_on_node,
        rank_on_node,
    );

    // Generate access patterns (one per client, before barrier)
    let pattern_len = 10000;
    let patterns: Vec<Vec<workload::AccessEntry>> = (0..num_clients)
        .map(|c| {
            workload::generate_pattern(
                size,
                cli.key_range,
                cli.read_ratio,
                cli.distribution,
                pattern_len,
                rank as u64 * 1000 + c as u64,
            )
        })
        .collect();

    // Create ipc servers
    let shm_paths: Vec<String> = (0..num_daemons)
        .map(|d| format!("/benchkv_{}_{}", rank, d))
        .collect();

    // All clients connect to all daemons
    let max_clients_per_daemon = num_clients as u32;
    let mut servers: Vec<Option<ipc::Server<Request, Response>>> = shm_paths
        .iter()
        .map(|path| {
            Some(
                unsafe {
                    ipc::Server::<Request, Response>::create(
                        path,
                        max_clients_per_daemon,
                        queue_depth,
                        0,
                    )
                }
                .expect("Failed to create ipc server"),
            )
        })
        .collect();

    // Create Flux network (used for copyrpc recv forwarding between daemons)
    let flux_capacity = 1024;
    let flux_inflight_max = 256;
    let mut flux_nodes: Vec<Option<DaemonFlux>> = inproc::create_flux::<DelegatePayload, usize>(
        num_daemons.max(1),
        flux_capacity,
        flux_inflight_max,
    )
    .into_iter()
    .map(Some)
    .collect();

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Barrier: all daemon threads + main thread
    let ready_barrier = Arc::new(Barrier::new(num_daemons + 1));

    // copyrpc setup channels (per-daemon for multi-node)
    // Each daemon handles remote ranks where rank % num_daemons == daemon_id
    let (copyrpc_local_rxs, copyrpc_remote_txs, copyrpc_setups) = if size > 1 {
        let mut local_rxs = Vec::with_capacity(num_daemons);
        let mut remote_txs = Vec::with_capacity(num_daemons);
        let mut setups = Vec::with_capacity(num_daemons);

        for d in 0..num_daemons {
            let my_remote_ranks: Vec<u32> = (0..size)
                .filter(|&r| r != rank && (r as usize) % num_daemons == d)
                .collect();

            let (local_tx, local_rx) = std::sync::mpsc::channel();
            let (remote_tx, remote_rx) = std::sync::mpsc::channel();
            local_rxs.push(local_rx);
            remote_txs.push(remote_tx);
            setups.push(Some(CopyrpcSetup {
                local_info_tx: local_tx,
                remote_info_rx: remote_rx,
                device_index: cli.device_index,
                port: cli.port,
                ring_size: cli.ring_size,
                my_remote_ranks,
            }));
        }

        (local_rxs, remote_txs, setups)
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };
    let mut copyrpc_setups = copyrpc_setups;

    // Spawn daemon threads
    let mut daemon_handles = Vec::with_capacity(num_daemons);

    for d in 0..num_daemons {
        let server = servers[d].take().unwrap();
        let flux = flux_nodes[d].take().unwrap();
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let key_range = cli.key_range;

        let copyrpc_setup = if size > 1 {
            copyrpc_setups[d].take()
        } else {
            None
        };

        let core = daemon_cores.get(d).copied();
        let qd_interval = cli.qd_sample_dir.as_ref().map(|_| cli.qd_sample_interval);

        daemon_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("daemon-{}", d));
            }
            daemon::run_daemon(
                d,
                rank,
                num_daemons,
                key_range,
                queue_depth,
                server,
                flux,
                copyrpc_setup,
                &stop,
                &barrier,
                qd_interval,
            )
        }));
    }

    // Main thread: MPI exchange for copyrpc (if multi-node)
    // New topology: 1:1 per peer_rank
    //   Pairing: (my_rank, daemon peer_rank%D) ↔ (peer_rank, daemon my_rank%D)
    if size > 1 {
        // 1. Collect local endpoint infos from all daemons
        //    Each daemon sends infos ordered by its my_remote_ranks list
        let daemon_local_infos: Vec<Vec<EndpointConnectionInfo>> = copyrpc_local_rxs
            .iter()
            .map(|rx| rx.recv().expect("Failed to receive daemon local info"))
            .collect();

        // Compute each daemon's remote ranks list (same as what was passed to CopyrpcSetup)
        let daemon_remote_ranks: Vec<Vec<u32>> = (0..num_daemons)
            .map(|d| {
                (0..size)
                    .filter(|&r| r != rank && (r as usize) % num_daemons == d)
                    .collect()
            })
            .collect();

        // 2. Per-daemon accumulator for remote infos
        let mut daemon_remote_infos: Vec<Vec<EndpointConnectionInfo>> = (0..num_daemons)
            .map(|d| Vec::with_capacity(daemon_remote_ranks[d].len()))
            .collect();

        // 3. For each peer rank, exchange 1 endpoint info
        for peer_rank in 0..size {
            if peer_rank == rank {
                continue;
            }
            // Local daemon handling this peer: peer_rank % num_daemons
            let local_daemon = (peer_rank as usize) % num_daemons;
            // Find the index of peer_rank in that daemon's my_remote_ranks
            let local_ep_idx = daemon_remote_ranks[local_daemon]
                .iter()
                .position(|&r| r == peer_rank)
                .expect("peer_rank must be in local_daemon's remote_ranks");

            // Send this daemon's endpoint info for this peer
            let send_bytes = daemon_local_infos[local_daemon][local_ep_idx].to_bytes();

            // Exchange with peer rank
            let recv_bytes =
                mpi_util::exchange_bytes(world, rank as i32, peer_rank as i32, &send_bytes);

            // The received info belongs to the remote daemon that handles our rank:
            // remote daemon = rank % num_daemons (on peer_rank's node)
            // But we need to give it to the local daemon that owns the connection: local_daemon
            let remote_info = EndpointConnectionInfo::from_bytes(&recv_bytes);
            daemon_remote_infos[local_daemon].push(remote_info);
        }

        // 4. Send remote infos to each daemon (in my_remote_ranks order)
        for (d, tx) in copyrpc_remote_txs.iter().enumerate() {
            tx.send(std::mem::take(&mut daemon_remote_infos[d]))
                .expect("Failed to send remote info to daemon");
        }
    }

    // Wait for all daemons to be ready
    ready_barrier.wait();

    // Small delay for connections to settle
    std::thread::sleep(Duration::from_millis(10));
    world.barrier();

    // Spawn client threads (each connects to ALL daemons)
    let bench_start = Instant::now();
    let mut client_handles = Vec::with_capacity(num_clients);
    for c in 0..num_clients {
        let paths = shm_paths.clone();
        let pattern = patterns[c].clone();
        let stop = stop_flag.clone();
        let core = client_cores.get(c).copied();
        let batch_size = cli.batch_size;
        let bs = bench_start;

        client_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("client-{}", c));
            }
            client::run_client(
                &paths,
                num_daemons,
                rank,
                &pattern,
                queue_depth,
                &stop,
                batch_size,
                bs,
            )
        }));
    }

    // Benchmark: run boundary loop
    let duration = Duration::from_secs(cli.duration);
    let mut run_boundaries = Vec::new();

    for run in 0..cli.runs {
        world.barrier();
        let run_start_ns = bench_start.elapsed().as_nanos() as u64;
        std::thread::sleep(duration);
        let run_end_ns = bench_start.elapsed().as_nanos() as u64;
        run_boundaries.push((run, run_start_ns, run_end_ns));
    }

    // Stop
    stop_flag.store(true, Ordering::Release);

    for (d, h) in daemon_handles.into_iter().enumerate() {
        let samples = h.join().expect("Daemon thread panicked");
        if let Some(ref dir) = cli.qd_sample_dir {
            std::fs::create_dir_all(dir).ok();
            let path = format!("{}/qd_rank{}_d{}.csv", dir, rank, d);
            if let Err(e) = qd_sample::write_csv(&path, &samples) {
                eprintln!(
                    "  rank {} daemon {}: failed to write QD samples: {}",
                    rank, d, e
                );
            } else {
                eprintln!(
                    "  rank {} daemon {}: {} QD samples -> {}",
                    rank,
                    d,
                    samples.len(),
                    path
                );
            }
        }
    }

    let client_batches: Vec<Vec<parquet_out::BatchRecord>> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    // Log RPS per run
    for &(run, start_ns, end_ns) in &run_boundaries {
        let rps = parquet_out::compute_run_rps(&client_batches, start_ns, end_ns);
        eprintln!("  rank {} run {}: {:.0} RPS", rank, run + 1, rps);
        let mut total_rps = 0.0f64;
        world.all_reduce_into(
            &rps,
            &mut total_rps,
            mpi::collective::SystemOperation::sum(),
        );
        if rank == 0 {
            eprintln!("  total run {}: {:.0} RPS", run + 1, total_rps);
        }
    }

    parquet_out::rows_from_batches(
        "meta",
        rank,
        &client_batches,
        &run_boundaries,
        num_daemons as u32,
        num_clients as u32,
        queue_depth,
        cli.key_range,
    )
}
