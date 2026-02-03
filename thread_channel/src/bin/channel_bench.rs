//! Channel All-to-All Benchmark Binary
//!
//! Benchmarks Flux (SPSC-based) and Mesh (MPSC-based) n-to-n communication
//! performance with all-to-all call pattern.

use std::sync::atomic::{AtomicU64, Ordering};
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
#[command(about = "Flux vs Mesh all-to-all call benchmark")]
struct Args {
    /// Thread counts to benchmark (comma-separated)
    #[arg(short = 'n', long, value_delimiter = ',', default_value = "2,3,4")]
    threads: Vec<usize>,

    /// Number of calls per peer from each thread
    #[arg(short, long, default_value = "10000")]
    iterations: u64,

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
    iterations: u64,
    total_calls: u64,
    runs: u32,
    duration_ns_mean: f64,
    duration_ns_min: u64,
    duration_ns_max: u64,
    duration_ns_stddev: f64,
    throughput_mops_mean: f64,
    latency_ns_mean: f64,
}

fn run_flux_benchmark(n: usize, capacity: usize, iterations: u64) -> Duration {
    // Per-node, per-peer completion counters: completed_counters[node][peer]
    let completed_counters: Vec<Vec<Arc<AtomicU64>>> = (0..n)
        .map(|_| (0..n).map(|_| Arc::new(AtomicU64::new(0))).collect())
        .collect();
    let counters_clone: Vec<Vec<Arc<AtomicU64>>> = completed_counters
        .iter()
        .map(|v| v.iter().map(Arc::clone).collect())
        .collect();

    // Create nodes with response callback
    // user_data is (node_idx, peer) tuple to track per-peer completion
    let nodes: Vec<Flux<Payload, (usize, usize), _>> =
        create_flux(n, capacity, move |&mut (node_idx, peer): &mut (usize, usize), _: Payload| {
            counters_clone[node_idx][peer].fetch_add(1, Ordering::Relaxed);
        });

    let barrier = Arc::new(Barrier::new(n));
    let payload = Payload::default();

    // Pipeline depth per peer - 256 allows good throughput while leaving room for replies
    let max_in_flight_per_peer = 256u64;

    let handles: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(thread_idx, mut node)| {
            let barrier = Arc::clone(&barrier);
            let my_counters: Vec<Arc<AtomicU64>> =
                completed_counters[thread_idx].iter().map(Arc::clone).collect();
            thread::spawn(move || {
                // Pin to core
                pin_to_core(thread_idx);

                let id = node.id();
                let num_peers = node.num_peers();
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                barrier.wait();
                let start = Instant::now();

                let mut sent_per_peer = vec![0u64; n];
                let expected_requests = iterations * (num_peers as u64);
                let mut sent_replies = 0u64;
                let mut total_completed = 0u64;
                let expected_responses = iterations * (num_peers as u64);

                // Loop until all responses received AND all incoming requests processed
                while total_completed < expected_responses || sent_replies < expected_requests {
                    // Batch send: fill pipeline to max in-flight for each peer
                    for &peer in &peers {
                        let completed_from_peer = my_counters[peer].load(Ordering::Relaxed);
                        let in_flight = sent_per_peer[peer].saturating_sub(completed_from_peer);
                        let can_send = max_in_flight_per_peer.saturating_sub(in_flight);

                        // Send up to can_send requests to this peer
                        for _ in 0..can_send {
                            if sent_per_peer[peer] >= iterations {
                                break;
                            }
                            // Pass (thread_idx, peer) as user_data for per-peer completion tracking
                            if node.call(peer, payload, (thread_idx, peer)).is_ok() {
                                sent_per_peer[peer] += 1;
                            } else {
                                break; // channel full
                            }
                        }
                    }

                    // Poll: flushes pending writes, receives messages, invokes response callbacks
                    node.poll();

                    // Update total completed count
                    total_completed = peers.iter().map(|&p| my_counters[p].load(Ordering::Relaxed)).sum();

                    // Process received requests and reply
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        if handle.reply_or_requeue(data) {
                            sent_replies += 1;
                        } else {
                            // Requeued - break to poll again
                            break;
                        }
                    }
                }

                // Final flush to ensure all pending replies are visible to peers
                node.poll();

                start.elapsed()
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap()
}

fn run_mesh_benchmark(n: usize, iterations: u64) -> Duration {
    let nodes: Vec<Mesh<Payload>> = create_mesh(n);
    let barrier = Arc::new(Barrier::new(n));
    let payload = Payload::default();

    // Mesh uses blocking send, so we need to limit in-flight requests
    // to avoid deadlock (all threads blocked sending while no one receives)
    let max_in_flight_per_peer = 256usize;

    let handles: Vec<_> = nodes
        .into_iter()
        .enumerate()
        .map(|(thread_idx, mut node)| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // Pin to core
                pin_to_core(thread_idx);

                let id = node.id();
                let num_peers = node.num_peers();
                let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                barrier.wait();
                let start = Instant::now();

                let mut sent_per_peer = vec![0u64; n];
                let mut completed_per_peer = vec![0u64; n];
                let mut completed = 0u64;
                let expected_responses = iterations * (num_peers as u64);
                let expected_requests = iterations * (num_peers as u64);
                let mut sent_replies = 0u64;

                // Loop until all responses received AND all incoming requests processed
                while completed < expected_responses || sent_replies < expected_requests {
                    // Fill pipeline to max in-flight for all peers
                    for &peer in &peers {
                        while sent_per_peer[peer] < iterations {
                            let in_flight = sent_per_peer[peer] - completed_per_peer[peer];
                            if in_flight >= max_in_flight_per_peer as u64 {
                                break;
                            }
                            if node.call(peer, payload).is_ok() {
                                sent_per_peer[peer] += 1;
                            } else {
                                break; // channel full
                            }
                        }
                    }

                    // Process received messages
                    loop {
                        match node.try_recv() {
                            Ok((from, ReceivedMessage::Request { req_num, data })) => {
                                node.reply(from, req_num, data).unwrap();
                                sent_replies += 1;
                            }
                            Ok((from, ReceivedMessage::Response { .. })) => {
                                completed_per_peer[from] += 1;
                                completed += 1;
                            }
                            Ok((_, ReceivedMessage::Notify(_))) => {}
                            Err(RecvError::Empty) => break,
                            Err(RecvError::Disconnected) => break,
                        }
                    }
                }

                start.elapsed()
            })
        })
        .collect();

    handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap()
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

        let total_calls = (n as u64) * ((n - 1) as u64) * args.iterations;

        // Benchmark Flux
        println!(
            "Benchmarking Flux: n={}, iterations={}, total_calls={}",
            n, args.iterations, total_calls
        );

        // Warmup
        for w in 0..args.warmup {
            println!("  Warmup {}/{}", w + 1, args.warmup);
            run_flux_benchmark(n, args.capacity, args.iterations);
        }

        // Benchmark runs
        let mut durations = Vec::with_capacity(args.runs);
        for r in 0..args.runs {
            println!("  Run {}/{}", r + 1, args.runs);
            let duration = run_flux_benchmark(n, args.capacity, args.iterations);
            durations.push(duration);
            println!("    Duration: {:?}", duration);
        }

        let (mean_ns, min_ns, max_ns, stddev_ns) = compute_stats(&durations);
        let throughput_mops = (total_calls as f64) / (mean_ns / 1_000_000_000.0) / 1_000_000.0;
        let latency_ns = mean_ns / (total_calls as f64);

        println!(
            "  Results: mean={:.2}ms, min={:.2}ms, max={:.2}ms, stddev={:.2}ms",
            mean_ns / 1_000_000.0,
            min_ns as f64 / 1_000_000.0,
            max_ns as f64 / 1_000_000.0,
            stddev_ns / 1_000_000.0
        );
        println!(
            "  Throughput: {:.2} Mops/s, Latency: {:.2} ns/call",
            throughput_mops, latency_ns
        );

        results.push(BenchResult {
            threads: n as u32,
            implementation: "flux".to_string(),
            iterations: args.iterations,
            total_calls,
            runs: args.runs as u32,
            duration_ns_mean: mean_ns,
            duration_ns_min: min_ns,
            duration_ns_max: max_ns,
            duration_ns_stddev: stddev_ns,
            throughput_mops_mean: throughput_mops,
            latency_ns_mean: latency_ns,
        });

        // Benchmark Mesh
        println!(
            "Benchmarking Mesh: n={}, iterations={}, total_calls={}",
            n, args.iterations, total_calls
        );

        // Warmup
        for w in 0..args.warmup {
            println!("  Warmup {}/{}", w + 1, args.warmup);
            run_mesh_benchmark(n, args.iterations);
        }

        // Benchmark runs
        let mut durations = Vec::with_capacity(args.runs);
        for r in 0..args.runs {
            println!("  Run {}/{}", r + 1, args.runs);
            let duration = run_mesh_benchmark(n, args.iterations);
            durations.push(duration);
            println!("    Duration: {:?}", duration);
        }

        let (mean_ns, min_ns, max_ns, stddev_ns) = compute_stats(&durations);
        let throughput_mops = (total_calls as f64) / (mean_ns / 1_000_000_000.0) / 1_000_000.0;
        let latency_ns = mean_ns / (total_calls as f64);

        println!(
            "  Results: mean={:.2}ms, min={:.2}ms, max={:.2}ms, stddev={:.2}ms",
            mean_ns / 1_000_000.0,
            min_ns as f64 / 1_000_000.0,
            max_ns as f64 / 1_000_000.0,
            stddev_ns / 1_000_000.0
        );
        println!(
            "  Throughput: {:.2} Mops/s, Latency: {:.2} ns/call",
            throughput_mops, latency_ns
        );

        results.push(BenchResult {
            threads: n as u32,
            implementation: "mesh".to_string(),
            iterations: args.iterations,
            total_calls,
            runs: args.runs as u32,
            duration_ns_mean: mean_ns,
            duration_ns_min: min_ns,
            duration_ns_max: max_ns,
            duration_ns_stddev: stddev_ns,
            throughput_mops_mean: throughput_mops,
            latency_ns_mean: latency_ns,
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
        Field::new("iterations", DataType::UInt64, false),
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
    let iterations: Vec<u64> = results.iter().map(|r| r.iterations).collect();
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
            Arc::new(UInt64Array::from(iterations)),
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
