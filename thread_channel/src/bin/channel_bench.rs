//! Channel All-to-All Benchmark Binary
//!
//! Benchmarks Flux (SPSC-based) and Mesh (MPSC-based) n-to-n communication
//! performance with all-to-all call pattern using time-based RPS monitoring.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use core_affinity::CoreId;
use parquet::arrow::ArrowWriter;
use thread_channel::spsc::Serial;
use thread_channel::{create_flux, create_mesh, Flux, Mesh, ReceivedMessage, RecvError};

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

#[derive(Parser)]
#[command(name = "channel_bench")]
#[command(about = "Flux vs Mesh all-to-all call benchmark with RPS monitoring")]
struct Args {
    /// Thread counts to benchmark (comma-separated)
    #[arg(short = 'n', long, value_delimiter = ',', default_value = "2,3,4")]
    threads: Vec<usize>,

    /// Duration in seconds
    #[arg(short, long, default_value = "10")]
    duration: u64,

    /// Channel capacity (for Flux only)
    #[arg(short, long, default_value = "1024")]
    capacity: usize,

    /// Number of benchmark runs per configuration
    #[arg(short, long, default_value = "5")]
    runs: usize,

    /// Number of warmup runs
    #[arg(short, long, default_value = "1")]
    warmup: usize,

    /// Output parquet file
    #[arg(short, long, default_value = "channel_bench.parquet")]
    output: String,
}

struct BenchResult {
    threads: u32,
    implementation: String,
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

/// Result from the monitor thread
struct MonitorResult {
    #[allow(dead_code)]
    samples: Vec<u64>,
    median_rps: u64, // Actually min_rps for conservative estimate
    max_rps: u64,
}

/// Flux benchmark result
struct FluxBenchResult {
    duration: Duration,
    total_completed: u64,
    monitor_result: MonitorResult,
}

/// Run Flux benchmark with RPS monitoring
/// Main thread acts as monitor and controls timing
fn run_flux_benchmark(n: usize, capacity: usize, duration_secs: u64) -> FluxBenchResult {
    // Per-thread completed counters (no contention)
    let per_thread_completed: Vec<Arc<AtomicU64>> =
        (0..n).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stop_flag = Arc::new(AtomicBool::new(false));

    // No-op callback
    let nodes: Vec<Flux<Payload, (), _>> =
        create_flux(n, capacity, |_: &mut (), _: Payload| {});

    let barrier = Arc::new(Barrier::new(n + 1)); // +1 for main thread
    let payload = Payload::default();

    let handles: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(thread_idx, mut node)| {
            let barrier = Arc::clone(&barrier);
            let stop_flag = Arc::clone(&stop_flag);
            let my_completed = Arc::clone(&per_thread_completed[thread_idx]);
            thread::spawn(move || {
                // Worker threads on CPU 1+ (main is on CPU 0)
                pin_to_core(thread_idx + 1);

                let id = node.id();
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                barrier.wait();

                let mut total_sent = 0u64;
                let mut total_completed = 0u64;

                // Main loop: run until stop_flag is set
                while !stop_flag.load(Relaxed) {
                    // Aggressive pipelining: try to send to all peers
                    let mut any_sent = false;
                    for &peer in &peers {
                        if node.call(peer, payload, ()).is_ok() {
                            total_sent += 1;
                            any_sent = true;
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
                    }

                    // Track completed accurately: completed = sent - pending
                    // Update every 1024 sends to reduce pending_count() calls
                    if (total_sent & 0x3FF) == 0 {
                        let pending = node.pending_count() as u64;
                        let new_completed = total_sent.saturating_sub(pending);
                        if new_completed > total_completed {
                            total_completed = new_completed;
                            // Publish to per-thread counter (main will aggregate)
                            my_completed.store(total_completed, Relaxed);
                        }
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

    // Main thread on CPU 0
    pin_to_core(0);

    // Main thread acts as monitor
    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(duration_secs);

    let mut samples = Vec::new();
    let mut prev_total = 0u64;
    let interval_ms = 500u64;

    while start.elapsed() < run_duration {
        thread::sleep(Duration::from_millis(interval_ms));

        // Sum all per-thread counters
        let current_total: u64 = per_thread_completed.iter().map(|c| c.load(Relaxed)).sum();
        let delta = current_total.saturating_sub(prev_total);
        let rps = (delta as f64 * 1000.0 / interval_ms as f64) as u64;

        if delta > 0 {
            samples.push(rps);
        }
        prev_total = current_total;
    }

    // Signal workers to stop
    stop_flag.store(true, Relaxed);

    let duration = start.elapsed();

    // Collect results
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_completed: u64 = results.iter().sum();

    // Calculate min and max (report min for conservative estimate)
    let monitor_result = if samples.is_empty() {
        MonitorResult {
            samples: vec![],
            median_rps: 0,
            max_rps: 0,
        }
    } else {
        samples.sort();
        MonitorResult {
            median_rps: *samples.first().unwrap(), // Use min for conservative RPS
            max_rps: *samples.last().unwrap(),
            samples,
        }
    };

    FluxBenchResult {
        duration,
        total_completed,
        monitor_result,
    }
}

/// Mesh benchmark result
struct MeshBenchResult {
    duration: Duration,
    total_completed: u64,
    monitor_result: MonitorResult,
}

/// Run Mesh benchmark with RPS monitoring
/// Main thread acts as monitor and controls timing
fn run_mesh_benchmark(n: usize, duration_secs: u64) -> MeshBenchResult {
    // Per-thread completed counters (no contention)
    let per_thread_completed: Vec<Arc<AtomicU64>> =
        (0..n).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let stop_flag = Arc::new(AtomicBool::new(false));

    let nodes: Vec<Mesh<Payload>> = create_mesh(n);
    let barrier = Arc::new(Barrier::new(n + 1)); // +1 for main thread
    let payload = Payload::default();

    // Mesh uses blocking send, so we need to limit in-flight requests
    let max_in_flight_per_peer = 256usize;

    let handles: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(thread_idx, mut node)| {
            let barrier = Arc::clone(&barrier);
            let stop_flag = Arc::clone(&stop_flag);
            let my_completed = Arc::clone(&per_thread_completed[thread_idx]);
            thread::spawn(move || {
                // Worker threads on CPU 1+
                pin_to_core(thread_idx + 1);

                let id = node.id();
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                barrier.wait();

                let mut sent_per_peer = vec![0u64; n];
                let mut completed_per_peer = vec![0u64; n];
                let mut total_completed = 0u64;

                // Main loop: run until stop_flag is set
                while !stop_flag.load(Relaxed) {
                    // Fill pipeline to max in-flight for all peers
                    for &peer in &peers {
                        while (sent_per_peer[peer] - completed_per_peer[peer])
                            < max_in_flight_per_peer as u64
                        {
                            if node.call(peer, payload).is_ok() {
                                sent_per_peer[peer] += 1;
                            } else {
                                break;
                            }
                        }
                    }

                    // Process received messages
                    loop {
                        match node.try_recv() {
                            Ok((from, ReceivedMessage::Request { req_num, data })) => {
                                let _ = node.reply(from, req_num, data);
                            }
                            Ok((from, ReceivedMessage::Response { .. })) => {
                                completed_per_peer[from] += 1;
                                total_completed += 1;
                            }
                            Ok((_, ReceivedMessage::Notify(_))) => {}
                            Err(RecvError::Empty) => break,
                            Err(RecvError::Disconnected) => break,
                        }
                    }

                    // Publish completed count periodically
                    if (total_completed & 0x3FF) == 0 {
                        my_completed.store(total_completed, Relaxed);
                    }
                }

                // Final update
                my_completed.store(total_completed, Relaxed);
                total_completed
            })
        })
        .collect();

    // Main thread on CPU 0
    pin_to_core(0);

    // Main thread acts as monitor
    barrier.wait();
    let start = Instant::now();
    let run_duration = Duration::from_secs(duration_secs);

    let mut samples = Vec::new();
    let mut prev_total = 0u64;
    let interval_ms = 500u64;

    while start.elapsed() < run_duration {
        thread::sleep(Duration::from_millis(interval_ms));

        // Sum all per-thread counters
        let current_total: u64 = per_thread_completed.iter().map(|c| c.load(Relaxed)).sum();
        let delta = current_total.saturating_sub(prev_total);
        let rps = (delta as f64 * 1000.0 / interval_ms as f64) as u64;

        if delta > 0 {
            samples.push(rps);
        }
        prev_total = current_total;
    }

    // Signal workers to stop
    stop_flag.store(true, Relaxed);

    let duration = start.elapsed();

    // Collect results
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let total_completed: u64 = results.iter().sum();

    // Calculate min and max (report min for conservative estimate)
    let monitor_result = if samples.is_empty() {
        MonitorResult {
            samples: vec![],
            median_rps: 0,
            max_rps: 0,
        }
    } else {
        samples.sort();
        MonitorResult {
            median_rps: *samples.first().unwrap(), // Use min
            max_rps: *samples.last().unwrap(),
            samples,
        }
    };

    MeshBenchResult {
        duration,
        total_completed,
        monitor_result,
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

fn main() {
    let args = Args::parse();

    let mut results = Vec::new();

    for &n in &args.threads {
        if n < 2 {
            eprintln!("Skipping n={} (need at least 2 threads)", n);
            continue;
        }

        // Benchmark Flux
        println!(
            "Benchmarking Flux: n={}, duration={}s",
            n, args.duration
        );

        // Warmup
        for w in 0..args.warmup {
            println!("  Warmup {}/{}", w + 1, args.warmup);
            run_flux_benchmark(n, args.capacity, args.duration.min(3));
        }

        // Benchmark runs
        let mut all_medians = Vec::new();
        let mut all_maxes = Vec::new();
        let mut durations = Vec::new();
        let mut total_ops = Vec::new();

        for r in 0..args.runs {
            println!("  Run {}/{}", r + 1, args.runs);
            let result = run_flux_benchmark(n, args.capacity, args.duration);
            durations.push(result.duration);
            total_ops.push(result.total_completed);
            all_medians.push(result.monitor_result.median_rps);
            all_maxes.push(result.monitor_result.max_rps);
            println!(
                "    Duration: {:?}, Completed: {}, Min RPS: {:.2} Mops/s, Max RPS: {:.2} Mops/s",
                result.duration,
                result.total_completed,
                result.monitor_result.median_rps as f64 / 1_000_000.0,
                result.monitor_result.max_rps as f64 / 1_000_000.0
            );
        }

        // Aggregate results
        let avg_median_mops =
            all_medians.iter().sum::<u64>() as f64 / all_medians.len() as f64 / 1_000_000.0;
        let max_mops = *all_maxes.iter().max().unwrap() as f64 / 1_000_000.0;
        let total_calls: u64 = total_ops.iter().sum();

        println!(
            "  Summary: Avg Min={:.2} Mops/s, Best Max={:.2} Mops/s",
            avg_median_mops, max_mops
        );

        let (mean_ns, min_ns, max_ns, stddev_ns) = compute_stats(&durations);
        results.push(BenchResult {
            threads: n as u32,
            implementation: "flux".to_string(),
            duration_secs: args.duration,
            total_calls,
            runs: args.runs as u32,
            duration_ns_mean: mean_ns,
            duration_ns_min: min_ns,
            duration_ns_max: max_ns,
            duration_ns_stddev: stddev_ns,
            throughput_mops_mean: avg_median_mops,
            latency_ns_mean: 1_000_000_000.0 / (avg_median_mops * 1_000_000.0),
        });

        // Benchmark Mesh
        println!(
            "Benchmarking Mesh: n={}, duration={}s",
            n, args.duration
        );

        // Warmup
        for w in 0..args.warmup {
            println!("  Warmup {}/{}", w + 1, args.warmup);
            run_mesh_benchmark(n, args.duration.min(3));
        }

        // Benchmark runs
        let mut all_medians = Vec::new();
        let mut all_maxes = Vec::new();
        let mut durations = Vec::new();
        let mut total_ops = Vec::new();

        for r in 0..args.runs {
            println!("  Run {}/{}", r + 1, args.runs);
            let result = run_mesh_benchmark(n, args.duration);
            durations.push(result.duration);
            total_ops.push(result.total_completed);
            all_medians.push(result.monitor_result.median_rps);
            all_maxes.push(result.monitor_result.max_rps);
            println!(
                "    Duration: {:?}, Completed: {}, Min RPS: {:.2} Mops/s, Max RPS: {:.2} Mops/s",
                result.duration,
                result.total_completed,
                result.monitor_result.median_rps as f64 / 1_000_000.0,
                result.monitor_result.max_rps as f64 / 1_000_000.0
            );
        }

        // Aggregate results
        let avg_median_mops =
            all_medians.iter().sum::<u64>() as f64 / all_medians.len() as f64 / 1_000_000.0;
        let max_mops = *all_maxes.iter().max().unwrap() as f64 / 1_000_000.0;
        let total_calls: u64 = total_ops.iter().sum();

        println!(
            "  Summary: Avg Min={:.2} Mops/s, Best Max={:.2} Mops/s",
            avg_median_mops, max_mops
        );

        let (mean_ns, min_ns, max_ns, stddev_ns) = compute_stats(&durations);
        results.push(BenchResult {
            threads: n as u32,
            implementation: "mesh".to_string(),
            duration_secs: args.duration,
            total_calls,
            runs: args.runs as u32,
            duration_ns_mean: mean_ns,
            duration_ns_min: min_ns,
            duration_ns_max: max_ns,
            duration_ns_stddev: stddev_ns,
            throughput_mops_mean: avg_median_mops,
            latency_ns_mean: 1_000_000_000.0 / (avg_median_mops * 1_000_000.0),
        });
    }

    // Write to parquet
    write_parquet(&args.output, &results).expect("Failed to write parquet file");
    println!("Results written to {}", args.output);
}

fn write_parquet(path: &str, results: &[BenchResult]) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Schema::new(vec![
        Field::new("threads", DataType::UInt32, false),
        Field::new("implementation", DataType::Utf8, false),
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
    let implementation: Vec<&str> = results.iter().map(|r| r.implementation.as_str()).collect();
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
            Arc::new(StringArray::from(implementation)),
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
