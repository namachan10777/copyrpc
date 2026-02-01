//! Flux All-to-All Benchmark Binary
//!
//! Benchmarks Flux (SPSC-based n-to-n communication) performance with all-to-all call pattern.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use thread_channel::spsc::Serial;
use thread_channel::{create_flux, Flux, ReceivedMessage};

/// 32-byte payload for benchmarking
#[derive(Clone, Copy)]
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
#[command(name = "flux_bench")]
#[command(about = "Flux all-to-all call benchmark")]
struct Args {
    /// Thread counts to benchmark (comma-separated)
    #[arg(short = 'n', long, value_delimiter = ',', default_value = "2,3,4")]
    threads: Vec<usize>,

    /// Batch size (if specified, uses new batch API; otherwise uses legacy try_call API)
    #[arg(short, long)]
    batch: Option<usize>,

    /// Number of calls per peer from each thread
    #[arg(short, long, default_value = "10000")]
    iterations: u64,

    /// Channel capacity
    #[arg(short, long, default_value = "1024")]
    capacity: usize,

    /// Number of benchmark runs per configuration
    #[arg(short, long, default_value = "5")]
    runs: usize,

    /// Number of warmup runs
    #[arg(short, long, default_value = "1")]
    warmup: usize,

    /// Output parquet file
    #[arg(short, long, default_value = "flux_bench.parquet")]
    output: String,
}

struct BenchResult {
    threads: u32,
    api: String,
    batch_size: u32,
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

fn run_legacy_benchmark(
    n: usize,
    capacity: usize,
    iterations: u64,
) -> Duration {
    let nodes: Vec<Flux<Payload>> = create_flux(n, capacity);
    let barrier = Arc::new(Barrier::new(n));
    let payload = Payload::default();

    // Limit in-flight requests per peer to leave room for replies
    let max_in_flight_per_peer = (capacity / 4).max(1);

    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
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

                // Pending replies that couldn't be sent due to full channel
                let mut pending_replies: Vec<(usize, u64, Payload)> = Vec::new();
                let mut sent_replies = 0u64;

                // Loop until all responses received AND all incoming requests processed
                while completed < expected_responses || (sent_replies + pending_replies.len() as u64) < expected_requests {
                    // Try to flush pending replies first (highest priority)
                    let old_pending = pending_replies.len();
                    pending_replies.retain(|&(to, req_num, data)| {
                        node.try_reply(to, req_num, data).is_err()
                    });
                    sent_replies += (old_pending - pending_replies.len()) as u64;

                    // Try to send calls to all peers (with in-flight limit)
                    for &peer in &peers {
                        if sent_per_peer[peer] < iterations {
                            let in_flight = sent_per_peer[peer] - completed_per_peer[peer];
                            if in_flight < max_in_flight_per_peer as u64 {
                                if node.try_call(peer, payload).is_ok() {
                                    sent_per_peer[peer] += 1;
                                }
                            }
                        }
                    }

                    // Process received messages
                    loop {
                        match node.try_recv() {
                            Ok((from, ReceivedMessage::Request { req_num, data })) => {
                                if node.try_reply(from, req_num, data).is_ok() {
                                    sent_replies += 1;
                                } else {
                                    pending_replies.push((from, req_num, data));
                                }
                            }
                            Ok((from, ReceivedMessage::Response { .. })) => {
                                completed_per_peer[from] += 1;
                                completed += 1;
                            }
                            Ok((_, ReceivedMessage::Notify(_))) => {}
                            Err(_) => break,
                        }
                    }
                }

                start.elapsed()
            })
        })
        .collect();

    // Return the maximum duration (when all threads are done)
    handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .max()
        .unwrap()
}

fn run_batch_benchmark(
    n: usize,
    capacity: usize,
    iterations: u64,
    batch_size: usize,
) -> Duration {
    let nodes: Vec<Flux<Payload>> = create_flux(n, capacity);
    let barrier = Arc::new(Barrier::new(n));
    let payload = Payload::default();

    // Limit in-flight requests per peer to leave room for replies
    let max_in_flight_per_peer = (capacity / 4).max(1);

    let handles: Vec<_> = nodes
        .into_iter()
        .map(|mut node| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
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

                // Pending replies that couldn't be sent due to full channel
                let mut pending_replies: Vec<(usize, u64, Payload)> = Vec::new();
                let mut sent_replies = 0u64;

                // Loop until all responses received AND all incoming requests processed
                while completed < expected_responses || (sent_replies + pending_replies.len() as u64) < expected_requests {
                    // Try to flush pending replies first (highest priority)
                    let old_pending = pending_replies.len();
                    pending_replies.retain(|&(to, req_num, data)| {
                        node.write_reply(to, req_num, data).is_err()
                    });
                    sent_replies += (old_pending - pending_replies.len()) as u64;

                    // Batch send: write up to batch_size calls, then flush (with in-flight limit)
                    let mut written = 0;
                    for &peer in &peers {
                        if written >= batch_size {
                            break;
                        }
                        if sent_per_peer[peer] < iterations {
                            let in_flight = sent_per_peer[peer] - completed_per_peer[peer];
                            if in_flight < max_in_flight_per_peer as u64 {
                                if node.write_call(peer, payload).is_ok() {
                                    sent_per_peer[peer] += 1;
                                    written += 1;
                                }
                            }
                        }
                    }
                    node.flush();

                    // Batch receive: sync then poll all available
                    node.sync();
                    while let Some((from, msg)) = node.poll() {
                        match msg {
                            ReceivedMessage::Request { req_num, data } => {
                                if node.write_reply(from, req_num, data).is_ok() {
                                    sent_replies += 1;
                                } else {
                                    pending_replies.push((from, req_num, data));
                                }
                            }
                            ReceivedMessage::Response { .. } => {
                                completed_per_peer[from] += 1;
                                completed += 1;
                            }
                            ReceivedMessage::Notify(_) => {}
                        }
                    }
                    node.flush();
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
        let (api, batch_size) = match args.batch {
            Some(b) => ("batch".to_string(), b as u32),
            None => ("legacy".to_string(), 0),
        };

        println!(
            "Benchmarking n={}, api={}, batch_size={}, iterations={}, total_calls={}",
            n, api, batch_size, args.iterations, total_calls
        );

        // Warmup
        for w in 0..args.warmup {
            println!("  Warmup {}/{}", w + 1, args.warmup);
            match args.batch {
                Some(b) => run_batch_benchmark(n, args.capacity, args.iterations, b),
                None => run_legacy_benchmark(n, args.capacity, args.iterations),
            };
        }

        // Benchmark runs
        let mut durations = Vec::with_capacity(args.runs);
        for r in 0..args.runs {
            println!("  Run {}/{}", r + 1, args.runs);
            let duration = match args.batch {
                Some(b) => run_batch_benchmark(n, args.capacity, args.iterations, b),
                None => run_legacy_benchmark(n, args.capacity, args.iterations),
            };
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
            api,
            batch_size,
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
        Field::new("api", DataType::Utf8, false),
        Field::new("batch_size", DataType::UInt32, false),
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
    let api: Vec<&str> = results.iter().map(|r| r.api.as_str()).collect();
    let batch_size: Vec<u32> = results.iter().map(|r| r.batch_size).collect();
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
            Arc::new(StringArray::from(api)),
            Arc::new(UInt32Array::from(batch_size)),
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
