mod affinity;
mod client;
mod daemon;
mod epoch;
mod erpc_backend;
mod message;
mod mpi_util;
mod parquet_out;
mod storage;
mod ucx_am;
mod ucx_am_backend;
mod workload;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use clap::Parser;
use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use daemon::{
    on_delegate_response, CopyrpcSetup, DaemonFlux, EndpointConnectionInfo, CONNECTION_INFO_SIZE,
};
use epoch::EpochCollector;
use message::{DelegatePayload, Request, Response};
use shm_spsc::RequestToken;

#[derive(Parser, Debug)]
#[command(name = "benchkv")]
#[command(about = "Distributed KV benchmark over copyrpc + thread_channel + shm_spsc")]
struct Cli {
    /// Benchmark duration in seconds
    #[arg(short = 'd', long, default_value = "30")]
    duration: u64,

    /// Epoch interval in milliseconds
    #[arg(long, default_value = "1000")]
    interval_ms: u64,

    /// Epochs to trim from each end
    #[arg(long, default_value = "3")]
    trim: usize,

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
    #[arg(long, default_value = "1048576")]
    ring_size: usize,

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
}

fn main() {
    let cli = Cli::parse();

    let (universe, _threading) = mpi::initialize_with_threading(mpi::Threading::Funneled)
        .expect("Failed to initialize MPI");
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
    let (daemon_cores, client_cores) =
        affinity::assign_cores(&available_cores, num_daemons, num_clients, ranks_on_node, rank_on_node);

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

    // Create shm_spsc servers
    let shm_paths: Vec<String> = (0..num_daemons)
        .map(|d| format!("/benchkv_{}_{}", rank, d))
        .collect();

    let max_clients_per_daemon = num_clients.div_ceil(num_daemons).max(1) as u32;
    let mut servers: Vec<Option<shm_spsc::Server<Request, Response>>> = shm_paths
        .iter()
        .map(|path| {
            Some(
                unsafe {
                    shm_spsc::Server::<Request, Response>::create(
                        path,
                        max_clients_per_daemon,
                        queue_depth,
                    )
                }
                .expect("Failed to create shm_spsc server"),
            )
        })
        .collect();

    // Create Flux network
    let flux_capacity = 1024;
    let flux_inflight_max = 256;
    let mut flux_nodes: Vec<Option<DaemonFlux>> = if num_daemons > 1 {
        thread_channel::create_flux::<DelegatePayload, RequestToken, _>(
            num_daemons,
            flux_capacity,
            flux_inflight_max,
            on_delegate_response as fn(&mut RequestToken, DelegatePayload),
        )
        .into_iter()
        .map(Some)
        .collect()
    } else {
        thread_channel::create_flux::<DelegatePayload, RequestToken, _>(
            1,
            flux_capacity,
            flux_inflight_max,
            on_delegate_response as fn(&mut RequestToken, DelegatePayload),
        )
        .into_iter()
        .map(Some)
        .collect()
    };

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));
    let completions: Vec<Arc<AtomicU64>> = (0..num_clients)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // Barrier: all daemon threads + main thread
    let ready_barrier = Arc::new(Barrier::new(num_daemons + 1));

    // copyrpc setup channels (per-daemon for multi-node)
    let num_remote_ranks = (size - 1) as usize;
    let (copyrpc_local_rxs, copyrpc_remote_txs, copyrpc_setups) = if size > 1 {
        let mut local_rxs = Vec::with_capacity(num_daemons);
        let mut remote_txs = Vec::with_capacity(num_daemons);
        let mut setups = Vec::with_capacity(num_daemons);

        for _ in 0..num_daemons {
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
                num_remote_ranks,
                num_daemons,
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

        daemon_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("daemon-{}", d));
            }
            daemon::run_daemon(
                d,
                rank,
                num_daemons,
                key_range,
                server,
                flux,
                copyrpc_setup,
                &stop,
                &barrier,
            );
        }));
    }

    // Main thread: MPI exchange for copyrpc (if multi-node)
    if size > 1 {
        // 1. Collect local endpoint infos from all daemons
        //    Each daemon sends D * num_remote_ranks infos, ordered by
        //    [rank_offset * D + target_daemon]
        let daemon_local_infos: Vec<Vec<EndpointConnectionInfo>> = copyrpc_local_rxs
            .iter()
            .map(|rx| rx.recv().expect("Failed to receive daemon local info"))
            .collect();

        // 2. For each remote rank, build D×D matrix, exchange, distribute
        //    Per-daemon accumulator for remote infos
        let mut daemon_remote_infos: Vec<Vec<EndpointConnectionInfo>> =
            (0..num_daemons).map(|_| Vec::with_capacity(num_daemons * num_remote_ranks)).collect();

        for peer_rank in 0..size {
            if peer_rank == rank {
                continue;
            }
            let rank_offset = if peer_rank < rank {
                peer_rank as usize
            } else {
                (peer_rank - 1) as usize
            };

            // Build send matrix: D×D infos
            // Layout: [d_local * D + d_remote]
            let mut send_bytes =
                Vec::with_capacity(num_daemons * num_daemons * CONNECTION_INFO_SIZE);
            for local_infos in &daemon_local_infos {
                for d_remote in 0..num_daemons {
                    let idx = rank_offset * num_daemons + d_remote;
                    send_bytes.extend(local_infos[idx].to_bytes());
                }
            }

            // Exchange D×D infos with peer rank
            let recv_bytes =
                mpi_util::exchange_bytes(world, rank as i32, peer_rank as i32, &send_bytes);

            // Parse receive matrix: [d_remote * D + d_local]
            // For our daemon d, extract recv_matrix[j * D + d] for j in 0..D
            for (d, remote_infos) in daemon_remote_infos.iter_mut().enumerate() {
                for remote_d in 0..num_daemons {
                    let matrix_idx = remote_d * num_daemons + d;
                    let byte_offset = matrix_idx * CONNECTION_INFO_SIZE;
                    remote_infos.push(EndpointConnectionInfo::from_bytes(
                        &recv_bytes[byte_offset..],
                    ));
                }
            }
        }

        // 3. Send remote infos to each daemon
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

    // Spawn client threads
    let mut client_handles = Vec::with_capacity(num_clients);
    for c in 0..num_clients {
        let daemon_id = c % num_daemons;
        let shm_path = shm_paths[daemon_id].clone();
        let pattern = patterns[c].clone();
        let stop = stop_flag.clone();
        let comp = completions[c].clone();
        let core = client_cores.get(c).copied();

        client_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("client-{}", c));
            }
            client::run_client(&shm_path, &pattern, queue_depth, &stop, &comp);
        }));
    }

    // Benchmark: epoch sampling loop
    let mut all_rows = Vec::new();
    let duration = Duration::from_secs(cli.duration);
    let interval = Duration::from_millis(cli.interval_ms);

    for run in 0..cli.runs {
        for c in &completions {
            c.store(0, Ordering::Release);
        }

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

        collector.finish();
        let steady = collector.steady_state(cli.trim);
        let rows = parquet_out::rows_from_epochs(
            "meta",
            steady,
            rank,
            num_daemons as u32,
            num_clients as u32,
            queue_depth,
            cli.key_range,
            run,
        );

        let avg_rps = if !steady.is_empty() {
            let avg = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
            eprintln!(
                "  rank {} run {}: avg {:.0} RPS ({} steady epochs)",
                rank,
                run + 1,
                avg,
                steady.len()
            );
            avg
        } else {
            0.0
        };
        let mut total_rps = 0.0f64;
        world.all_reduce_into(
            &avg_rps,
            &mut total_rps,
            mpi::collective::SystemOperation::sum(),
        );
        if rank == 0 {
            eprintln!("  total run {}: {:.0} RPS", run + 1, total_rps);
        }

        all_rows.extend(rows);
    }

    // Stop
    stop_flag.store(true, Ordering::Release);

    for h in daemon_handles {
        h.join().expect("Daemon thread panicked");
    }
    for h in client_handles {
        h.join().expect("Client thread panicked");
    }

    all_rows
}
