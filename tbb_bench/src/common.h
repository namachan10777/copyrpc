#pragma once

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <getopt.h>
#include <numeric>
#include <pthread.h>
#include <string>
#include <thread>
#include <vector>

#include <oneapi/tbb/concurrent_queue.h>

// 32-byte payload matching Rust Payload { data: [u8; 32] }
struct Payload {
    uint8_t data[32];
};

struct RunResult {
    std::chrono::nanoseconds duration;
    uint64_t total_completed;
};

struct BenchResult {
    uint32_t threads;
    std::string kind;
    std::string transport;
    uint64_t duration_secs;
    uint64_t total_calls;
    uint32_t runs;
    double duration_ns_mean;
    uint64_t duration_ns_min;
    uint64_t duration_ns_max;
    double duration_ns_stddev;
    double throughput_mops_median;
    double latency_ns_median;
};

struct CommonArgs {
    int threads = 16;
    int duration = 10;
    int capacity = 1024;
    int inflight = 256;
    std::string transport = "all";
    int runs = 7;
    int warmup = 1;
    std::string output;
    int start_core = 31;
};

inline void pin_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

inline CommonArgs parse_args(int argc, char* argv[], const char* default_output) {
    CommonArgs args;
    args.output = default_output;

    static struct option long_options[] = {
        {"threads",    required_argument, nullptr, 'n'},
        {"duration",   required_argument, nullptr, 'd'},
        {"capacity",   required_argument, nullptr, 'c'},
        {"inflight",   required_argument, nullptr, 'i'},
        {"transport",  required_argument, nullptr, 't'},
        {"runs",       required_argument, nullptr, 'r'},
        {"warmup",     required_argument, nullptr, 'w'},
        {"output",     required_argument, nullptr, 'o'},
        {"start_core", required_argument, nullptr, 's'},
        {nullptr, 0, nullptr, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "n:d:i:t:r:w:o:", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'n': args.threads = std::atoi(optarg); break;
            case 'd': args.duration = std::atoi(optarg); break;
            case 'c': args.capacity = std::atoi(optarg); break;
            case 'i': args.inflight = std::atoi(optarg); break;
            case 't': args.transport = optarg; break;
            case 'r': args.runs = std::atoi(optarg); break;
            case 'w': args.warmup = std::atoi(optarg); break;
            case 'o': args.output = optarg; break;
            case 's': args.start_core = std::atoi(optarg); break;
        }
    }
    return args;
}

struct DurationStats {
    double mean_ns;
    uint64_t min_ns;
    uint64_t max_ns;
    double stddev_ns;
};

inline DurationStats compute_stats(const std::vector<std::chrono::nanoseconds>& durations) {
    std::vector<uint64_t> ns;
    ns.reserve(durations.size());
    for (auto& d : durations) {
        ns.push_back(static_cast<uint64_t>(d.count()));
    }
    double mean = static_cast<double>(std::accumulate(ns.begin(), ns.end(), uint64_t{0}))
                  / static_cast<double>(ns.size());
    uint64_t min_val = *std::min_element(ns.begin(), ns.end());
    uint64_t max_val = *std::max_element(ns.begin(), ns.end());
    double variance = 0.0;
    for (auto x : ns) {
        double diff = static_cast<double>(x) - mean;
        variance += diff * diff;
    }
    variance /= static_cast<double>(ns.size());
    return {mean, min_val, max_val, std::sqrt(variance)};
}

using BenchRunFn = RunResult (*)(int n, int capacity, int duration_secs,
                                 int max_inflight, int start_core);

inline BenchResult run_transport_benchmark(
    const std::string& kind,
    const std::string& transport_name,
    int n, int capacity, int duration_secs, int inflight,
    int warmup, int runs, int start_core,
    BenchRunFn run_fn)
{
    std::printf("Benchmarking %s (%s/%s): n=%d, duration=%ds\n",
                transport_name.c_str(), kind.c_str(), transport_name.c_str(),
                n, duration_secs);

    // Warmup
    for (int w = 0; w < warmup; w++) {
        std::printf("  Warmup %d/%d\n", w + 1, warmup);
        run_fn(n, capacity, std::min(duration_secs, 3), inflight, start_core);
    }

    // Benchmark runs
    std::vector<std::chrono::nanoseconds> durations;
    std::vector<uint64_t> total_ops;

    for (int r = 0; r < runs; r++) {
        std::printf("  Run %d/%d\n", r + 1, runs);
        auto result = run_fn(n, capacity, duration_secs, inflight, start_core);
        durations.push_back(result.duration);
        total_ops.push_back(result.total_completed);
        double secs = static_cast<double>(result.duration.count()) / 1e9;
        double run_mops = static_cast<double>(result.total_completed) / secs / 1e6;
        std::printf("    Duration: %.3fs, Completed: %lu, Throughput: %.2f Mops/s\n",
                    secs, result.total_completed, run_mops);
    }

    // Compute per-run throughput and take median
    std::vector<double> throughputs;
    throughputs.reserve(runs);
    for (int i = 0; i < runs; i++) {
        double secs = static_cast<double>(durations[i].count()) / 1e9;
        throughputs.push_back(static_cast<double>(total_ops[i]) / secs);
    }
    std::sort(throughputs.begin(), throughputs.end());
    double median_rps = throughputs[throughputs.size() / 2];
    double median_mops = median_rps / 1e6;
    uint64_t total_calls = std::accumulate(total_ops.begin(), total_ops.end(), uint64_t{0});

    std::printf("  Summary: Median=%.2f Mops/s (runs:", median_mops);
    for (auto t : throughputs) {
        std::printf(" %.2f", t / 1e6);
    }
    std::printf(" Mops/s)\n");

    auto stats = compute_stats(durations);
    return BenchResult{
        .threads = static_cast<uint32_t>(n),
        .kind = kind,
        .transport = transport_name,
        .duration_secs = static_cast<uint64_t>(duration_secs),
        .total_calls = total_calls,
        .runs = static_cast<uint32_t>(runs),
        .duration_ns_mean = stats.mean_ns,
        .duration_ns_min = stats.min_ns,
        .duration_ns_max = stats.max_ns,
        .duration_ns_stddev = stats.stddev_ns,
        .throughput_mops_median = median_mops,
        .latency_ns_median = 1e9 / (median_mops * 1e6),
    };
}

inline void write_csv(const std::string& path, const std::vector<BenchResult>& results) {
    std::ofstream f(path);
    f << "threads,kind,transport,duration_secs,total_calls,runs,"
         "duration_ns_mean,duration_ns_min,duration_ns_max,duration_ns_stddev,"
         "throughput_mops_median,latency_ns_median\n";
    for (auto& r : results) {
        f << r.threads << ","
          << r.kind << ","
          << r.transport << ","
          << r.duration_secs << ","
          << r.total_calls << ","
          << r.runs << ","
          << r.duration_ns_mean << ","
          << r.duration_ns_min << ","
          << r.duration_ns_max << ","
          << r.duration_ns_stddev << ","
          << r.throughput_mops_median << ","
          << r.latency_ns_median << "\n";
    }
    f.close();
    std::printf("Results written to %s\n", path.c_str());
}
