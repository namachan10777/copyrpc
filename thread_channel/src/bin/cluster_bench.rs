//! Cluster All-to-All Benchmark Binary
//!
//! Benchmarks Transport implementations with 16-thread all-to-all communication pattern.
//! Uses a management thread for monitoring and statistics collection.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::{Parser, ValueEnum};
use core_affinity::CoreId;
use parquet::arrow::ArrowWriter;
use thread_channel::Serial;
use thread_channel::{
    create_flux_with_transport, FastForwardTransport, Flux, LamportTransport, OnesidedTransport,
    Transport,
};

/// Pin current thread to the specified core
fn pin_to_core(core_id: usize) {
    let core = CoreId { id: core_id };
    core_affinity::set_for_current(core);
}

/// 32-byte payload for benchmarking
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct Payload {
    data: [u8; 32],
}

unsafe impl Serial for Payload {}

impl Default for Payload {
    fn default() -> Self {
        Self { data: [0u8; 32] }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportType {
    Onesided,
    FastForward,
    Lamport,
    All,
}

#[derive(Parser)]
#[command(name = "cluster_bench")]
#[command(about = "16-thread all-to-all benchmark with Transport comparison")]
struct Args {
    /// Number of worker threads
    #[arg(short = 'n', long, default_value = "16")]
    threads: usize,

    /// Duration in seconds
    #[arg(short, long, default_value = "10")]
    duration: u64,

    /// Channel capacity
    #[arg(short, long, default_value = "1024")]
    capacity: usize,

    /// Number of benchmark runs per configuration
    #[arg(short, long, default_value = "3")]
    runs: usize,

    /// Number of warmup runs
    #[arg(short, long, default_value = "1")]
    warmup: usize,

    /// Output parquet file
    #[arg(short, long, default_value = "cluster_bench.parquet")]
    output: String,

    /// Transport implementation to benchmark
    #[arg(short = 't', long, value_enum, default_value = "all")]
    transport: TransportType,

    /// Max inflight requests per channel
    #[arg(short = 'i', long, default_value = "256")]
    inflight: usize,
}

struct BenchResult {
    threads: u32,
    transport: String,
    duration_secs: u64,
    total_calls: u64,
    runs: u32,
    duration_ns_mean: f64,
    duration_ns_min: u64,
    duration_ns_max: u64,
    duration_ns_stddev: f64,
    throughput_mops_mean: f64,
    latency_ns_mean: f64,
}

struct RunResult {
    duration: Duration,
    total_completed: u64,
    min_rps: u64,
    max_rps: u64,
}

/// Run benchmark for a specific transport implementation
fn run_benchmark<Tr: Transport>(
    n: usize,
    capacity: usize,
    duration_secs: u64,
    max_inflight: usize,
) -> RunResult {
    // Per-thread completed counters (no contention)
    let per_thread_completed: Vec<Arc<AtomicU64>> =
        (0..n).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stop_flag = Arc::new(AtomicBool::new(false));

    // No-op callback
    let nodes: Vec<Flux<Payload, (), _, Tr>> =
        create_flux_with_transport(n, capacity, max_inflight, |_: &mut (), _: Payload| {});

    let barrier = Arc::new(Barrier::new(n + 1)); // +1 for monitor thread
    let payload = Payload::default();

    let handles: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(thread_idx, mut node)| {
            let barrier = Arc::clone(&barrier);
            let stop_flag = Arc::clone(&stop_flag);
            let my_completed = Arc::clone(&per_thread_completed[thread_idx]);
            thread::spawn(move || {
                // Worker threads on CPU 1+ (monitor is on CPU 0)
                pin_to_core(thread_idx + 1);

                let id = node.id();
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                barrier.wait();

                let mut total_sent = 0u64;
                let mut total_completed = 0u64;
                let mut can_send = true;

                // Main loop: run until stop_flag is set
                'outer: loop {
                    for _ in 0..1024 {
                        // Aggressive pipelining: try to send to all peers
                        let mut any_sent = false;
                        if can_send {
                            for &peer in &peers {
                                if node.call(peer, payload, ()).is_ok() {
                                    total_sent += 1;
                                    any_sent = true;
                                }
                            }
                        }

                        // Poll and process replies
                        if !any_sent || (total_sent & 0x1F) == 0 {
                            node.poll();
                            while let Some(handle) = node.try_recv() {
                                let data = handle.data();
                                if !handle.reply_or_requeue(data) {
                                    break;
                                }
                            }
                            can_send = node.pending_count() < max_inflight;
                        }
                    }

                    // Update completed count and check stop_flag
                    let pending = node.pending_count() as u64;
                    let new_completed = total_sent.saturating_sub(pending);
                    if new_completed > total_completed {
                        total_completed = new_completed;
                        my_completed.store(total_completed, Relaxed);
                    }

                    if stop_flag.load(Relaxed) {
                        break 'outer;
                    }
                }

                // Final update
                let pending = node.pending_count() as u64;
                total_completed = total_sent.saturating_sub(pending);
                my_completed.store(total_completed, Relaxed);

                total_completed
            })
        })
        .collect();

    // Monitor thread on CPU 0
    pin_to_core(0);

    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(duration_secs);

    let mut samples = Vec::new();
    let mut prev_total = 0u64;
    let interval_ms = 500u64;

    while start.elapsed() < run_duration {
        thread::sleep(Duration::from_millis(interval_ms));

        let current_total: u64 = per_thread_completed.iter().map(|c| c.load(Relaxed)).sum();
        let delta = current_total.saturating_sub(prev_total);
        let rps = (delta as f64 * 1000.0 / interval_ms as f64) as u64;

        if delta > 0 {
            samples.push(rps);
        }
        prev_total = current_total;
    }

    stop_flag.store(true, Relaxed);
    let duration = start.elapsed();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_completed: u64 = results.iter().sum();

    let (min_rps, max_rps) = if samples.is_empty() {
        (0, 0)
    } else {
        samples.sort();
        (*samples.first().unwrap(), *samples.last().unwrap())
    };

    RunResult {
        duration,
        total_completed,
        min_rps,
        max_rps,
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

fn run_transport_benchmark(
    transport_name: &str,
    n: usize,
    capacity: usize,
    duration_secs: u64,
    inflight: usize,
    warmup: usize,
    runs: usize,
    run_fn: fn(usize, usize, u64, usize) -> RunResult,
) -> BenchResult {
    println!(
        "Benchmarking {}: n={}, duration={}s",
        transport_name, n, duration_secs
    );

    // Warmup
    for w in 0..warmup {
        println!("  Warmup {}/{}", w + 1, warmup);
        run_fn(n, capacity, duration_secs.min(3), inflight);
    }

    // Benchmark runs
    let mut all_mins = Vec::new();
    let mut all_maxes = Vec::new();
    let mut durations = Vec::new();
    let mut total_ops = Vec::new();

    for r in 0..runs {
        println!("  Run {}/{}", r + 1, runs);
        let result = run_fn(n, capacity, duration_secs, inflight);
        durations.push(result.duration);
        total_ops.push(result.total_completed);
        all_mins.push(result.min_rps);
        all_maxes.push(result.max_rps);
        println!(
            "    Duration: {:?}, Completed: {}, Min RPS: {:.2} Mops/s, Max RPS: {:.2} Mops/s",
            result.duration,
            result.total_completed,
            result.min_rps as f64 / 1_000_000.0,
            result.max_rps as f64 / 1_000_000.0
        );
    }

    let avg_min_mops = all_mins.iter().sum::<u64>() as f64 / all_mins.len() as f64 / 1_000_000.0;
    let max_mops = *all_maxes.iter().max().unwrap() as f64 / 1_000_000.0;
    let total_calls: u64 = total_ops.iter().sum();

    println!(
        "  Summary: Avg Min={:.2} Mops/s, Best Max={:.2} Mops/s",
        avg_min_mops, max_mops
    );

    let (mean_ns, min_ns, max_ns, stddev_ns) = compute_stats(&durations);
    BenchResult {
        threads: n as u32,
        transport: transport_name.to_string(),
        duration_secs,
        total_calls,
        runs: runs as u32,
        duration_ns_mean: mean_ns,
        duration_ns_min: min_ns,
        duration_ns_max: max_ns,
        duration_ns_stddev: stddev_ns,
        throughput_mops_mean: avg_min_mops,
        latency_ns_mean: 1_000_000_000.0 / (avg_min_mops * 1_000_000.0),
    }
}

fn main() {
    let args = Args::parse();

    if args.threads < 2 {
        eprintln!("Need at least 2 threads");
        std::process::exit(1);
    }

    let mut results = Vec::new();

    // Onesided
    if matches!(args.transport, TransportType::Onesided | TransportType::All) {
        results.push(run_transport_benchmark(
            "onesided",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            run_benchmark::<OnesidedTransport>,
        ));
    }

    // FastForward
    if matches!(args.transport, TransportType::FastForward | TransportType::All) {
        results.push(run_transport_benchmark(
            "fastforward",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            run_benchmark::<FastForwardTransport>,
        ));
    }

    // Lamport
    if matches!(args.transport, TransportType::Lamport | TransportType::All) {
        results.push(run_transport_benchmark(
            "lamport",
            args.threads,
            args.capacity,
            args.duration,
            args.inflight,
            args.warmup,
            args.runs,
            run_benchmark::<LamportTransport>,
        ));
    }

    // Write to parquet
    write_parquet(&args.output, &results).expect("Failed to write parquet file");
    println!("Results written to {}", args.output);
}

fn write_parquet(path: &str, results: &[BenchResult]) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("threads", DataType::UInt32, false),
        Field::new("transport", DataType::Utf8, false),
        Field::new("duration_secs", DataType::UInt64, false),
        Field::new("total_calls", DataType::UInt64, false),
        Field::new("runs", DataType::UInt32, false),
        Field::new("duration_ns_mean", DataType::Float64, false),
        Field::new("duration_ns_min", DataType::UInt64, false),
        Field::new("duration_ns_max", DataType::UInt64, false),
        Field::new("duration_ns_stddev", DataType::Float64, false),
        Field::new("throughput_mops_mean", DataType::Float64, false),
        Field::new("latency_ns_mean", DataType::Float64, false),
    ]);

    let threads: Vec<u32> = results.iter().map(|r| r.threads).collect();
    let transport: Vec<&str> = results.iter().map(|r| r.transport.as_str()).collect();
    let duration_secs: Vec<u64> = results.iter().map(|r| r.duration_secs).collect();
    let total_calls: Vec<u64> = results.iter().map(|r| r.total_calls).collect();
    let runs: Vec<u32> = results.iter().map(|r| r.runs).collect();
    let duration_ns_mean: Vec<f64> = results.iter().map(|r| r.duration_ns_mean).collect();
    let duration_ns_min: Vec<u64> = results.iter().map(|r| r.duration_ns_min).collect();
    let duration_ns_max: Vec<u64> = results.iter().map(|r| r.duration_ns_max).collect();
    let duration_ns_stddev: Vec<f64> = results.iter().map(|r| r.duration_ns_stddev).collect();
    let throughput_mops_mean: Vec<f64> = results.iter().map(|r| r.throughput_mops_mean).collect();
    let latency_ns_mean: Vec<f64> = results.iter().map(|r| r.latency_ns_mean).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt32Array::from(threads)),
            Arc::new(StringArray::from(transport)),
            Arc::new(UInt64Array::from(duration_secs)),
            Arc::new(UInt64Array::from(total_calls)),
            Arc::new(UInt32Array::from(runs)),
            Arc::new(Float64Array::from(duration_ns_mean)),
            Arc::new(UInt64Array::from(duration_ns_min)),
            Arc::new(UInt64Array::from(duration_ns_max)),
            Arc::new(Float64Array::from(duration_ns_stddev)),
            Arc::new(Float64Array::from(throughput_mops_mean)),
            Arc::new(Float64Array::from(latency_ns_mean)),
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
