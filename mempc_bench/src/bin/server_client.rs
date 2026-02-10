//! Server-Client Benchmark Binary
//!
//! Benchmarks raw mempc::MpscChannel API with 1 server + N clients pattern.
//! Tests OnesidedMpsc, FastForwardMpsc, LamportMpsc, and FetchAddMpsc.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use mempc::{
    BbqMpsc, FastForwardMpsc, FetchAddMpsc, JiffyMpsc, LamportMpsc, LcrqMpsc, LprqMpsc,
    MpscCaller, MpscChannel, MpscRecvRef, MpscServer, OnesidedMpsc, Serial,
};
use parquet::arrow::ArrowWriter;

/// 32-byte payload for benchmarking
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct Payload {
    data: [u8; 32],
}

unsafe impl Serial for Payload {}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportType {
    Onesided,
    FastForward,
    Lamport,
    FetchAdd,
    Lcrq,
    Lprq,
    Bbq,
    Jiffy,
    All,
}

#[derive(Parser)]
#[command(name = "server_client")]
#[command(about = "Server-client benchmark: 1 server + N clients using raw MpscChannel API")]
struct Args {
    /// Number of total threads (1 server + N-1 clients)
    #[arg(short = 'n', long, default_value = "16")]
    threads: usize,

    /// Duration in seconds
    #[arg(short, long, default_value = "10")]
    duration: u64,

    /// Channel capacity
    #[arg(long, default_value = "1024")]
    capacity: usize,

    /// Number of benchmark runs per configuration
    #[arg(short, long, default_value = "7")]
    runs: usize,

    /// Number of warmup runs
    #[arg(short, long, default_value = "1")]
    warmup: usize,

    /// Output parquet file
    #[arg(short, long, default_value = "server_client.parquet")]
    output: String,

    /// Transport implementation to benchmark
    #[arg(short = 't', long, value_enum, default_value = "all")]
    transport: TransportType,

    /// Max inflight requests per client
    #[arg(short = 'i', long, default_value = "256")]
    inflight: usize,

    /// Starting core ID for thread pinning (threads count down from this core)
    #[arg(long, default_value = "31")]
    start_core: usize,
}

struct BenchResult {
    threads: u32,
    kind: String,
    transport: String,
    duration_secs: u64,
    total_calls: u64,
    runs: u32,
    duration_ns_mean: f64,
    duration_ns_min: u64,
    duration_ns_max: u64,
    duration_ns_stddev: f64,
    throughput_mops_median: f64,
    latency_ns_median: f64,
}

struct RunResult {
    duration: Duration,
    total_completed: u64,
}

fn pin_to_core(core_id: usize) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
}

// ============================================================================
// Server-Client benchmark implementation
// ============================================================================

fn run_server_client<M: MpscChannel>(
    n: usize,
    capacity: usize,
    duration_secs: u64,
    max_inflight: usize,
    start_core: usize,
) -> RunResult {
    let num_clients = n - 1;
    let stop_flag = Arc::new(AtomicBool::new(false));

    let (callers, server) = M::create::<Payload, Payload>(num_clients, capacity, max_inflight);

    // barrier: num_clients + 1 server + 1 main thread = n + 1
    let barrier = Arc::new(Barrier::new(n + 1));
    let payload = Payload::default();
    let run_duration = Duration::from_secs(duration_secs);

    // Server thread
    let server_handle = {
        let barrier = Arc::clone(&barrier);
        let stop_flag = Arc::clone(&stop_flag);
        let core = start_core;
        thread::spawn(move || {
            pin_to_core(core);
            let mut server = server;
            barrier.wait();

            loop {
                server.poll();
                loop {
                    let Some(recv_ref) = server.try_recv() else {
                        break;
                    };
                    let data = recv_ref.data();
                    let token = recv_ref.into_token();
                    server.reply(token, data);
                }

                if stop_flag.load(Relaxed) {
                    // Drain remaining
                    server.poll();
                    loop {
                        let Some(recv_ref) = server.try_recv() else {
                            break;
                        };
                        let data = recv_ref.data();
                        let token = recv_ref.into_token();
                        server.reply(token, data);
                    }
                    break;
                }
            }

            // Server returns 0 since we only count client-side completions
            0
        })
    };

    // Client threads
    let mut client_handles: Vec<thread::JoinHandle<u64>> = Vec::with_capacity(num_clients);
    for (client_idx, mut caller) in callers.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let core = start_core - 1 - client_idx;
        client_handles.push(thread::spawn(move || {
            pin_to_core(core);
            barrier.wait();

            let deadline = fastant::Instant::now() + run_duration;
            let mut total_completed = 0u64;
            let mut iter_count = 0u32;

            loop {
                // Send requests
                for _ in 0..32 {
                    if caller.pending_count() < max_inflight {
                        let _ = caller.call(payload);
                    }
                }

                // Sync (needed for Lamport, no-op for others)
                caller.sync();

                // Receive responses
                while let Some((_token, resp)) = caller.try_recv_response() {
                    std::hint::black_box(resp);
                    total_completed += 1;
                }

                iter_count = iter_count.wrapping_add(1);
                if iter_count & 0x3F == 0 && fastant::Instant::now() >= deadline {
                    break;
                }
            }

            total_completed
        }));
    }

    // Main thread: wait for all clients to complete, then stop server
    barrier.wait();
    let start = fastant::Instant::now();
    let client_results: Vec<u64> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    let total_completed: u64 = client_results.iter().sum();
    let duration = start.elapsed();
    stop_flag.store(true, Relaxed);
    server_handle.join().unwrap();

    RunResult {
        duration,
        total_completed,
    }
}

fn compute_stats(durations: &[Duration]) -> (f64, u64, u64, f64) {
    let ns: Vec<u64> = durations.iter().map(|d| d.as_nanos() as u64).collect();
    let mean = ns.iter().sum::<u64>() as f64 / ns.len() as f64;
    let min = *ns.iter().min().unwrap();
    let max = *ns.iter().max().unwrap();
    let variance = ns.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / ns.len() as f64;
    let stddev = variance.sqrt();
    (mean, min, max, stddev)
}

#[allow(clippy::too_many_arguments)]
fn run_transport_benchmark(
    kind: &str,
    transport_name: &str,
    n: usize,
    capacity: usize,
    duration_secs: u64,
    inflight: usize,
    warmup: usize,
    runs: usize,
    start_core: usize,
    run_fn: fn(usize, usize, u64, usize, usize) -> RunResult,
) -> BenchResult {
    println!(
        "Benchmarking {} ({}/{}): n={}, duration={}s",
        transport_name, kind, transport_name, n, duration_secs
    );

    // Warmup
    for w in 0..warmup {
        println!("  Warmup {}/{}", w + 1, warmup);
        run_fn(n, capacity, duration_secs.min(3), inflight, start_core);
    }

    // Benchmark runs
    let mut durations = Vec::new();
    let mut total_ops = Vec::new();

    for r in 0..runs {
        println!("  Run {}/{}", r + 1, runs);
        let result = run_fn(n, capacity, duration_secs, inflight, start_core);
        durations.push(result.duration);
        total_ops.push(result.total_completed);
        let run_mops = result.total_completed as f64 / result.duration.as_secs_f64() / 1_000_000.0;
        println!(
            "    Duration: {:?}, Completed: {}, Throughput: {:.2} Mops/s",
            result.duration, result.total_completed, run_mops,
        );
    }

    // Compute per-run throughput and take median
    let mut throughputs: Vec<f64> = durations
        .iter()
        .zip(total_ops.iter())
        .map(|(d, &ops)| ops as f64 / d.as_secs_f64())
        .collect();
    throughputs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_rps = throughputs[throughputs.len() / 2];
    let median_mops = median_rps / 1_000_000.0;
    let total_calls: u64 = total_ops.iter().sum();

    println!(
        "  Summary: Median={:.2} Mops/s (runs: {:?} Mops/s)",
        median_mops,
        throughputs
            .iter()
            .map(|t| format!("{:.2}", t / 1_000_000.0))
            .collect::<Vec<_>>()
    );

    let (mean_ns, min_ns, max_ns, stddev_ns) = compute_stats(&durations);
    BenchResult {
        threads: n as u32,
        kind: kind.to_string(),
        transport: transport_name.to_string(),
        duration_secs,
        total_calls,
        runs: runs as u32,
        duration_ns_mean: mean_ns,
        duration_ns_min: min_ns,
        duration_ns_max: max_ns,
        duration_ns_stddev: stddev_ns,
        throughput_mops_median: median_mops,
        latency_ns_median: 1_000_000_000.0 / (median_mops * 1_000_000.0),
    }
}

fn main() {
    let args = Args::parse();

    if args.threads < 2 {
        eprintln!("Need at least 2 threads (1 server + 1 client minimum)");
        std::process::exit(1);
    }

    let mut results = Vec::new();

    if matches!(args.transport, TransportType::Onesided | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "onesided",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<OnesidedMpsc>,
        ));
    }

    if matches!(
        args.transport,
        TransportType::FastForward | TransportType::All
    ) {
        results.push(run_transport_benchmark(
            "server_client",
            "fastforward",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<FastForwardMpsc>,
        ));
    }

    if matches!(args.transport, TransportType::Lamport | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "lamport",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<LamportMpsc>,
        ));
    }

    if matches!(args.transport, TransportType::FetchAdd | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "fetchadd",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<FetchAddMpsc>,
        ));
    }

    if matches!(args.transport, TransportType::Lcrq | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "lcrq",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<LcrqMpsc>,
        ));
    }

    if matches!(args.transport, TransportType::Lprq | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "lprq",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<LprqMpsc>,
        ));
    }

    if matches!(args.transport, TransportType::Bbq | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "bbq",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<BbqMpsc>,
        ));
    }

    if matches!(args.transport, TransportType::Jiffy | TransportType::All) {
        results.push(run_transport_benchmark(
            "server_client",
            "jiffy",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            args.start_core,
            run_server_client::<JiffyMpsc>,
        ));
    }

    write_parquet(&args.output, &results).expect("Failed to write parquet file");
    println!("Results written to {}", args.output);
}

fn write_parquet(path: &str, results: &[BenchResult]) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("threads", DataType::UInt32, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("transport", DataType::Utf8, false),
        Field::new("duration_secs", DataType::UInt64, false),
        Field::new("total_calls", DataType::UInt64, false),
        Field::new("runs", DataType::UInt32, false),
        Field::new("duration_ns_mean", DataType::Float64, false),
        Field::new("duration_ns_min", DataType::UInt64, false),
        Field::new("duration_ns_max", DataType::UInt64, false),
        Field::new("duration_ns_stddev", DataType::Float64, false),
        Field::new("throughput_mops_median", DataType::Float64, false),
        Field::new("latency_ns_median", DataType::Float64, false),
    ]);

    let threads: Vec<u32> = results.iter().map(|r| r.threads).collect();
    let kind: Vec<&str> = results.iter().map(|r| r.kind.as_str()).collect();
    let transport: Vec<&str> = results.iter().map(|r| r.transport.as_str()).collect();
    let duration_secs: Vec<u64> = results.iter().map(|r| r.duration_secs).collect();
    let total_calls: Vec<u64> = results.iter().map(|r| r.total_calls).collect();
    let runs: Vec<u32> = results.iter().map(|r| r.runs).collect();
    let duration_ns_mean: Vec<f64> = results.iter().map(|r| r.duration_ns_mean).collect();
    let duration_ns_min: Vec<u64> = results.iter().map(|r| r.duration_ns_min).collect();
    let duration_ns_max: Vec<u64> = results.iter().map(|r| r.duration_ns_max).collect();
    let duration_ns_stddev: Vec<f64> = results.iter().map(|r| r.duration_ns_stddev).collect();
    let throughput_mops_median: Vec<f64> =
        results.iter().map(|r| r.throughput_mops_median).collect();
    let latency_ns_median: Vec<f64> = results.iter().map(|r| r.latency_ns_median).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(threads)),
            Arc::new(StringArray::from(kind)),
            Arc::new(StringArray::from(transport)),
            Arc::new(UInt64Array::from(duration_secs)),
            Arc::new(UInt64Array::from(total_calls)),
            Arc::new(UInt32Array::from(runs)),
            Arc::new(Float64Array::from(duration_ns_mean)),
            Arc::new(UInt64Array::from(duration_ns_min)),
            Arc::new(UInt64Array::from(duration_ns_max)),
            Arc::new(Float64Array::from(duration_ns_stddev)),
            Arc::new(Float64Array::from(throughput_mops_median)),
            Arc::new(Float64Array::from(latency_ns_median)),
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
