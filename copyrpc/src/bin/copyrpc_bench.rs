//! Copyrpc inter-node benchmark using MPI for control plane.
//!
//! This benchmark runs RDMA pingpong between two nodes using MPI for connection setup.
//!
//! Run with:
//! ```bash
//! cargo build --release -p copyrpc --bin copyrpc_bench --features bench-bin
//! mpirun -np 2 --host node0,node1 ./target/release/copyrpc_bench \
//!     -i 100000 -s 32,64,128 -w 10000 -r 5 -o results.parquet
//! ```

use std::cell::Cell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use mpi::collective::CommunicatorCollectives;
use mpi::point_to_point::{Destination, Source};
use mpi::topology::Communicator;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use copyrpc::{Context, ContextBuilder, Endpoint, EndpointConfig, RemoteEndpointInfo};
use mlx5::srq::SrqConfig;

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "copyrpc_bench")]
#[command(about = "Inter-node RDMA benchmark using copyrpc")]
struct Args {
    /// Number of iterations per run
    #[arg(short, long, default_value = "10000")]
    iterations: u64,

    /// Message sizes (comma-separated)
    #[arg(short = 's', long, value_delimiter = ',', default_value = "32")]
    message_sizes: Vec<usize>,

    /// Number of endpoints
    #[arg(short, long, default_value = "1")]
    endpoints: usize,

    /// Number of warmup iterations
    #[arg(short, long, default_value = "1000")]
    warmup: u64,

    /// Number of runs per configuration
    #[arg(short, long, default_value = "5")]
    runs: usize,

    /// Ring buffer size
    #[arg(long, default_value = "1048576")]
    ring_size: usize,

    /// Device index
    #[arg(long, default_value = "0")]
    device_index: usize,

    /// Port number
    #[arg(long, default_value = "1")]
    port: u8,

    /// Output parquet file path
    #[arg(short, long, default_value = "copyrpc_bench.parquet")]
    output: String,
}

// =============================================================================
// Connection Info Serialization
// =============================================================================

/// Fixed-size connection info for MPI exchange
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

// =============================================================================
// Response Counter
// =============================================================================

thread_local! {
    static RESPONSE_COUNT: Cell<u32> = const { Cell::new(0) };
}

fn on_response_callback(_user_data: (), _data: &[u8]) {
    RESPONSE_COUNT.with(|c| c.set(c.get() + 1));
}

fn get_and_reset_response_count() -> usize {
    RESPONSE_COUNT.with(|c| c.replace(0)) as usize
}

// =============================================================================
// Benchmark Config
// =============================================================================

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct BenchmarkConfig {
    iterations: u64,
    message_size: u64,
    warmup: u64,
    endpoints: u64,
    ring_size: u64,
}

const BENCHMARK_CONFIG_SIZE: usize = std::mem::size_of::<BenchmarkConfig>();

impl BenchmarkConfig {
    fn to_bytes(&self) -> Vec<u8> {
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, BENCHMARK_CONFIG_SIZE).to_vec() }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= BENCHMARK_CONFIG_SIZE);
        let mut config = Self::default();
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut config as *mut Self as *mut u8,
                BENCHMARK_CONFIG_SIZE,
            );
        }
        config
    }
}

// =============================================================================
// Benchmark Results
// =============================================================================

struct BenchmarkResult {
    message_size: usize,
    endpoints: usize,
    iterations: u64,
    durations_ns: Vec<u64>,
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let args = Args::parse();

    // Initialize MPI
    let universe = mpi::initialize().expect("Failed to initialize MPI");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    if size != 2 {
        if rank == 0 {
            eprintln!("Error: This benchmark requires exactly 2 MPI ranks");
        }
        std::process::exit(1);
    }

    let is_client = rank == 0;

    if is_client {
        eprintln!(
            "copyrpc_bench: iterations={}, message_sizes={:?}, endpoints={}, warmup={}, runs={}",
            args.iterations, args.message_sizes, args.endpoints, args.warmup, args.runs
        );
    }

    let mut results = Vec::new();

    // Run benchmarks for each message size
    for &message_size in &args.message_sizes {
        if is_client {
            eprintln!("Running benchmark with message_size={}", message_size);
        }

        match run_benchmark_for_size(&args, &world, message_size, is_client) {
            Ok(result) => {
                if is_client {
                    results.push(result);
                }
            }
            Err(e) => {
                if is_client {
                    eprintln!("Error running benchmark for size {}: {}", message_size, e);
                }
            }
        }
    }

    // Client writes parquet output
    if is_client && !results.is_empty() {
        if let Err(e) = write_parquet(&args.output, &results) {
            eprintln!("Error writing parquet: {}", e);
        } else {
            eprintln!("Results written to {}", args.output);
        }
    }
}

fn run_benchmark_for_size(
    args: &Args,
    world: &mpi::topology::SimpleCommunicator,
    message_size: usize,
    is_client: bool,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    // Client sends config to server
    let config = BenchmarkConfig {
        iterations: args.iterations,
        message_size: message_size as u64,
        warmup: args.warmup,
        endpoints: args.endpoints as u64,
        ring_size: args.ring_size as u64,
    };

    if is_client {
        let config_bytes = config.to_bytes();
        world.process_at_rank(1).send(&config_bytes[..]);
    } else {
        let mut config_bytes = vec![0u8; BENCHMARK_CONFIG_SIZE];
        world.process_at_rank(0).receive_into(&mut config_bytes[..]);
        let _config = BenchmarkConfig::from_bytes(&config_bytes);
    }

    // Create context and endpoints
    let ctx: Context<()> = ContextBuilder::new()
        .device_index(args.device_index)
        .port(args.port)
        .srq_config(SrqConfig {
            max_wr: 16384,
            max_sge: 1,
        })
        .cq_size(4096)
        .build()?;

    let ep_config = EndpointConfig {
        send_ring_size: args.ring_size,
        recv_ring_size: args.ring_size,
        ..Default::default()
    };

    let mut endpoints = Vec::with_capacity(args.endpoints);
    let mut local_infos = Vec::with_capacity(args.endpoints);

    for _ in 0..args.endpoints {
        let ep = ctx.create_endpoint(&ep_config)?;
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
        });

        endpoints.push(ep);
    }

    // Exchange connection info via MPI
    let mut local_bytes = Vec::with_capacity(args.endpoints * CONNECTION_INFO_SIZE);
    for info in &local_infos {
        local_bytes.extend_from_slice(&info.to_bytes());
    }

    let mut remote_bytes = vec![0u8; args.endpoints * CONNECTION_INFO_SIZE];

    // Exchange: each rank sends to the other
    if is_client {
        world.process_at_rank(1).send(&local_bytes[..]);
        world.process_at_rank(1).receive_into(&mut remote_bytes[..]);
    } else {
        world.process_at_rank(0).receive_into(&mut remote_bytes[..]);
        world.process_at_rank(0).send(&local_bytes[..]);
    }

    // Parse remote info and connect
    let mut remote_infos = Vec::with_capacity(args.endpoints);
    for i in 0..args.endpoints {
        let start = i * CONNECTION_INFO_SIZE;
        let end = start + CONNECTION_INFO_SIZE;
        remote_infos.push(EndpointConnectionInfo::from_bytes(
            &remote_bytes[start..end],
        ));
    }

    for (i, ep) in endpoints.iter_mut().enumerate() {
        let remote_ep = &remote_infos[i];
        let remote = RemoteEndpointInfo {
            qp_number: remote_ep.qp_number,
            packet_sequence_number: remote_ep.packet_sequence_number,
            local_identifier: remote_ep.local_identifier,
            recv_ring_addr: remote_ep.recv_ring_addr,
            recv_ring_rkey: remote_ep.recv_ring_rkey,
            recv_ring_size: remote_ep.recv_ring_size,
        };
        ep.connect(&remote, 0, ctx.port())?;
    }

    // Small delay to ensure connections are ready
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Barrier before warmup
    world.barrier();

    // Warmup
    if is_client {
        run_client_pingpong(&ctx, &endpoints, message_size, args.warmup);
    } else {
        run_server_loop(&ctx, message_size, args.warmup);
    }

    // Barrier after warmup
    world.barrier();

    let mut durations_ns = Vec::with_capacity(args.runs);

    // Main benchmark runs
    for run in 0..args.runs {
        // Barrier before each run
        world.barrier();

        if is_client {
            let duration = run_client_pingpong(&ctx, &endpoints, message_size, args.iterations);
            durations_ns.push(duration.as_nanos() as u64);

            let throughput = args.iterations as f64 / duration.as_secs_f64() / 1_000_000.0;
            let latency_ns = duration.as_nanos() as f64 / args.iterations as f64;
            eprintln!(
                "  Run {}: {:.3} Mops, {:.1} ns/op",
                run + 1,
                throughput,
                latency_ns
            );
        } else {
            run_server_loop(&ctx, message_size, args.iterations);
        }
    }

    Ok(BenchmarkResult {
        message_size,
        endpoints: args.endpoints,
        iterations: args.iterations,
        durations_ns,
    })
}

// =============================================================================
// Client Pingpong
// =============================================================================

const REQUESTS_PER_EP: usize = 256;

fn run_client_pingpong(
    ctx: &Context<()>,
    endpoints: &[Endpoint<()>],
    message_size: usize,
    iterations: u64,
) -> Duration {
    let request_data = vec![0u8; message_size];
    let num_endpoints = endpoints.len();
    let max_inflight = num_endpoints * REQUESTS_PER_EP;

    let mut inflight = 0usize;
    let mut sent = 0u64;
    let mut completed = 0u64;

    // Reset response counter
    get_and_reset_response_count();

    // Initial fill: send REQUESTS_PER_EP requests on each endpoint
    for ep in endpoints.iter() {
        for _ in 0..REQUESTS_PER_EP {
            if sent < iterations {
                if ep.call(&request_data, ()).is_ok() {
                    sent += 1;
                    inflight += 1;
                }
            }
        }
    }

    // Flush initial batch
    ctx.poll(on_response_callback);

    let start = Instant::now();

    while completed < iterations {
        // Poll for completions
        ctx.poll(on_response_callback);

        // Get completions from callback
        let new_completions = get_and_reset_response_count();
        completed += new_completions as u64;
        inflight = inflight.saturating_sub(new_completions);

        // Refill
        while inflight < max_inflight && sent < iterations {
            let ep_idx = (sent as usize) % num_endpoints;
            let ep = &endpoints[ep_idx];

            if ep.call(&request_data, ()).is_ok() {
                sent += 1;
                inflight += 1;
            } else {
                // Ring full, poll and retry
                ctx.poll(on_response_callback);
                let new = get_and_reset_response_count();
                completed += new as u64;
                inflight = inflight.saturating_sub(new);
            }
        }
    }

    // Drain remaining inflight requests
    while inflight > 0 {
        ctx.poll(on_response_callback);
        let new_completions = get_and_reset_response_count();
        inflight = inflight.saturating_sub(new_completions);
    }

    start.elapsed()
}

// =============================================================================
// Server Loop
// =============================================================================

fn run_server_loop(ctx: &Context<()>, message_size: usize, iterations: u64) {
    let response_data = vec![0u8; message_size];
    let mut processed = 0u64;

    while processed < iterations {
        ctx.poll(|_, _| {});

        while let Some(req) = ctx.recv() {
            // Reply with retry on RingFull
            loop {
                match req.reply(&response_data) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll(|_, _| {});
                        continue;
                    }
                    Err(_) => break,
                }
            }
            processed += 1;
        }
    }

    // Final flush to ensure all replies are sent
    ctx.poll(|_, _| {});
}

// =============================================================================
// Parquet Output
// =============================================================================

fn write_parquet(
    path: &str,
    results: &[BenchmarkResult],
) -> Result<(), Box<dyn std::error::Error>> {
    // Build arrays
    let mut message_sizes = Vec::new();
    let mut endpoints_arr = Vec::new();
    let mut iterations_arr = Vec::new();
    let mut run_indices = Vec::new();
    let mut durations_ns = Vec::new();
    let mut throughput_mops = Vec::new();
    let mut latency_ns = Vec::new();

    for result in results {
        for (run_idx, &duration_ns) in result.durations_ns.iter().enumerate() {
            message_sizes.push(result.message_size as i64);
            endpoints_arr.push(result.endpoints as i64);
            iterations_arr.push(result.iterations as i64);
            run_indices.push(run_idx as i64);
            durations_ns.push(duration_ns);

            let duration_secs = duration_ns as f64 / 1_000_000_000.0;
            let ops = result.iterations as f64 / duration_secs / 1_000_000.0;
            let lat = duration_ns as f64 / result.iterations as f64;

            throughput_mops.push(ops);
            latency_ns.push(lat);
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("message_size", DataType::Int64, false),
        Field::new("endpoints", DataType::Int64, false),
        Field::new("iterations", DataType::Int64, false),
        Field::new("run", DataType::Int64, false),
        Field::new("duration_ns", DataType::UInt64, false),
        Field::new("throughput_mops", DataType::Float64, false),
        Field::new("latency_ns", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(message_sizes)) as ArrayRef,
            Arc::new(Int64Array::from(endpoints_arr)) as ArrayRef,
            Arc::new(Int64Array::from(iterations_arr)) as ArrayRef,
            Arc::new(Int64Array::from(run_indices)) as ArrayRef,
            Arc::new(UInt64Array::from(durations_ns)) as ArrayRef,
            Arc::new(Float64Array::from(throughput_mops)) as ArrayRef,
            Arc::new(Float64Array::from(latency_ns)) as ArrayRef,
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
