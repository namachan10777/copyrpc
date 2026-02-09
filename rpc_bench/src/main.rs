mod affinity;
mod copyrpc_bench;
mod epoch;
mod erpc_bench;

mod mpi_util;
mod parquet_out;
mod rc_send_recv_bench;
mod ucx_am_bench;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "rpc_bench")]
#[command(about = "Unified RPC benchmark suite")]
struct Cli {
    /// Message payload size in bytes
    #[arg(short = 's', long, default_value = "32")]
    message_size: usize,

    /// Total benchmark duration in seconds
    #[arg(short = 'd', long, default_value = "30")]
    duration: u64,

    /// Epoch interval in milliseconds
    #[arg(long, default_value = "1000")]
    interval_ms: u64,

    /// Number of epochs to trim from each end
    #[arg(long, default_value = "3")]
    trim: usize,

    /// Number of runs
    #[arg(short = 'r', long, default_value = "3")]
    runs: u32,

    /// Output parquet file path
    #[arg(short = 'o', long, default_value = "rpc_bench.parquet")]
    output: String,

    /// RDMA device index
    #[arg(long, default_value = "0")]
    device_index: usize,

    /// RDMA port number
    #[arg(long, default_value = "1")]
    port: u8,

    /// CPU affinity mode for thread pinning (cores assigned downward from affinity-start)
    #[arg(long)]
    affinity_mode: Option<AffinityMode>,

    /// Starting core ID for affinity (default: num_online_cores - 1)
    #[arg(long)]
    affinity_start: Option<usize>,

    #[command(subcommand)]
    system: SystemCmd,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum AffinityMode {
    /// Each rank independently pins from the back (for multi-node)
    Multinode,
    /// Ranks share a node; cores partitioned by local rank (for single-node)
    Singlenode,
}

#[derive(Subcommand, Debug)]
enum SystemCmd {
    /// copyrpc benchmark
    Copyrpc {
        /// Ring buffer size
        #[arg(long, default_value = "1048576")]
        ring_size: usize,

        #[command(subcommand)]
        mode: ModeCmd,
    },
    /// eRPC benchmark
    Erpc {
        /// Session credits
        #[arg(long, default_value = "8")]
        session_credits: usize,

        /// Request window size
        #[arg(long, default_value = "64")]
        req_window: usize,

        #[command(subcommand)]
        mode: ModeCmd,
    },
    /// UCX Active Message benchmark
    UcxAm {
        #[command(subcommand)]
        mode: ModeCmd,
    },
    /// RC send/recv benchmark
    RcSend {
        /// Number of buffer slots
        #[arg(long, default_value = "4096")]
        num_slots: usize,

        #[command(subcommand)]
        mode: ModeCmd,
    },
}

#[derive(Subcommand, Debug, Clone)]
enum ModeCmd {
    /// 1-to-1 benchmark (rank 0 = client, rank 1 = server)
    OneToOne {
        /// Number of endpoints/QPs
        #[arg(short = 'e', long, default_value = "1")]
        endpoints: u32,

        /// In-flight requests per endpoint
        #[arg(short = 'i', long, default_value = "256")]
        inflight: u32,

        /// Number of worker threads per rank
        #[arg(short = 't', long, default_value = "1")]
        threads: u32,
    },
    /// Multi-client benchmark (rank 0 = server, rank 1..N = clients)
    MultiClient {
        /// In-flight requests per client
        #[arg(short = 'i', long, default_value = "256")]
        inflight: u32,
    },
}

pub struct CommonConfig {
    pub message_size: usize,
    pub duration_secs: u64,
    pub interval_ms: u64,
    pub trim: usize,
    pub runs: u32,
    pub device_index: usize,
    pub port: u8,
    pub affinity_mode: Option<AffinityMode>,
    pub affinity_start: Option<usize>,
}

fn main() {
    let cli = Cli::parse();

    let (universe, _threading) = mpi::initialize_with_threading(mpi::Threading::Funneled)
        .expect("Failed to initialize MPI with threading");
    let world = universe.world();

    let common = CommonConfig {
        message_size: cli.message_size,
        duration_secs: cli.duration,
        interval_ms: cli.interval_ms,
        trim: cli.trim,
        runs: cli.runs,
        device_index: cli.device_index,
        port: cli.port,
        affinity_mode: cli.affinity_mode,
        affinity_start: cli.affinity_start,
    };

    let rows = match cli.system {
        SystemCmd::Copyrpc { ring_size, mode } => {
            copyrpc_bench::run(&common, &world, ring_size, &mode)
        }
        SystemCmd::Erpc {
            session_credits,
            req_window,
            mode,
        } => erpc_bench::run(&common, &world, session_credits, req_window, &mode),
        SystemCmd::UcxAm { mode } => ucx_am_bench::run(&common, &world, &mode),
        SystemCmd::RcSend { num_slots, mode } => {
            rc_send_recv_bench::run(&common, &world, num_slots, &mode)
        }
    };

    // Only rank 0 writes output
    use mpi::topology::Communicator;
    if world.rank() == 0 {
        if let Err(e) = parquet_out::write_parquet(&cli.output, &rows) {
            eprintln!("Error writing parquet: {}", e);
        } else if !rows.is_empty() {
            eprintln!("Results written to {}", cli.output);
        }
    }
}
