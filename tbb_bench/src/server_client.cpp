// TBB Server-Client Benchmark
//
// 1 server + (N-1) clients using tbb::concurrent_bounded_queue.
// Two variants:
//   - tbb_mpsc: single shared request queue (true MPSC) + per-client response queues
//   - tbb_spsc: per-client request queues + per-client response queues (SPSC pattern)
// Matches the communication pattern of mempc_bench/src/bin/server_client.rs.

#include "common.h"

struct RequestMsg {
    uint32_t client_id;
    Payload data;
};

// ============================================================================
// tbb_mpsc: Single shared request queue (true MPSC)
// Corresponds to FetchAddMpsc in Rust
// ============================================================================

RunResult run_mpsc(int n, int capacity, int duration_secs,
                   int max_inflight, int start_core) {
    int num_clients = n - 1;

    tbb::concurrent_bounded_queue<RequestMsg> req_queue;
    req_queue.set_capacity(capacity);

    std::vector<tbb::concurrent_bounded_queue<Payload>> resp_queues(num_clients);
    for (int i = 0; i < num_clients; i++) {
        resp_queues[i].set_capacity(capacity);
    }

    std::vector<std::atomic<uint64_t>> per_client_completed(num_clients);
    for (int i = 0; i < num_clients; i++) {
        per_client_completed[i].store(0, std::memory_order_relaxed);
    }
    std::atomic<bool> stop_flag{false};
    // barrier: num_clients + 1 server + 1 monitor = n + 1
    std::barrier sync_barrier(n + 1);

    Payload payload{};
    std::vector<std::thread> threads;
    threads.reserve(n);

    // Server thread
    threads.emplace_back([&]() {
        pin_to_core(start_core);
        sync_barrier.arrive_and_wait();

        while (true) {
            RequestMsg msg;
            while (req_queue.try_pop(msg)) {
                resp_queues[msg.client_id].push(msg.data);
            }

            if (stop_flag.load(std::memory_order_relaxed)) {
                // Drain remaining
                RequestMsg msg2;
                while (req_queue.try_pop(msg2)) {
                    resp_queues[msg2.client_id].push(msg2.data);
                }
                break;
            }
        }
    });

    // Client threads
    for (int client_idx = 0; client_idx < num_clients; client_idx++) {
        threads.emplace_back([&, client_idx]() {
            pin_to_core(start_core - 1 - client_idx);
            sync_barrier.arrive_and_wait();

            uint64_t total_completed = 0;
            uint64_t total_sent = 0;

            while (true) {
                // Send requests
                for (int i = 0; i < 32; i++) {
                    if ((total_sent - total_completed) < static_cast<uint64_t>(max_inflight)) {
                        RequestMsg msg{static_cast<uint32_t>(client_idx), payload};
                        if (req_queue.try_push(msg)) {
                            total_sent++;
                        }
                    }
                }

                // Receive responses
                Payload p;
                while (resp_queues[client_idx].try_pop(p)) {
                    total_completed++;
                }

                per_client_completed[client_idx].store(total_completed, std::memory_order_relaxed);

                if (stop_flag.load(std::memory_order_relaxed)) {
                    break;
                }
            }
        });
    }

    // Monitor
    sync_barrier.arrive_and_wait();
    auto start = std::chrono::steady_clock::now();
    auto run_duration = std::chrono::seconds(duration_secs);

    while (std::chrono::steady_clock::now() - start < run_duration) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    stop_flag.store(true, std::memory_order_relaxed);
    auto elapsed = std::chrono::steady_clock::now() - start;

    for (auto& t : threads) {
        t.join();
    }

    uint64_t total = 0;
    for (int i = 0; i < num_clients; i++) {
        total += per_client_completed[i].load(std::memory_order_relaxed);
    }

    return RunResult{
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed),
        total,
    };
}

// ============================================================================
// tbb_spsc: Per-client request/response queues (SPSC pattern)
// Corresponds to Onesided/FastForward/Lamport in Rust
// ============================================================================

RunResult run_spsc(int n, int capacity, int duration_secs,
                   int max_inflight, int start_core) {
    int num_clients = n - 1;

    std::vector<tbb::concurrent_bounded_queue<Payload>> req_queues(num_clients);
    std::vector<tbb::concurrent_bounded_queue<Payload>> resp_queues(num_clients);
    for (int i = 0; i < num_clients; i++) {
        req_queues[i].set_capacity(capacity);
        resp_queues[i].set_capacity(capacity);
    }

    std::vector<std::atomic<uint64_t>> per_client_completed(num_clients);
    for (int i = 0; i < num_clients; i++) {
        per_client_completed[i].store(0, std::memory_order_relaxed);
    }
    std::atomic<bool> stop_flag{false};
    std::barrier sync_barrier(n + 1);

    Payload payload{};
    std::vector<std::thread> threads;
    threads.reserve(n);

    // Server thread: round-robin poll all client request queues
    threads.emplace_back([&]() {
        pin_to_core(start_core);
        sync_barrier.arrive_and_wait();

        while (true) {
            for (int i = 0; i < num_clients; i++) {
                Payload p;
                while (req_queues[i].try_pop(p)) {
                    resp_queues[i].push(p);
                }
            }

            if (stop_flag.load(std::memory_order_relaxed)) {
                // Drain remaining
                for (int i = 0; i < num_clients; i++) {
                    Payload p;
                    while (req_queues[i].try_pop(p)) {
                        resp_queues[i].push(p);
                    }
                }
                break;
            }
        }
    });

    // Client threads
    for (int client_idx = 0; client_idx < num_clients; client_idx++) {
        threads.emplace_back([&, client_idx]() {
            pin_to_core(start_core - 1 - client_idx);
            sync_barrier.arrive_and_wait();

            uint64_t total_completed = 0;
            uint64_t total_sent = 0;

            while (true) {
                // Send requests
                for (int i = 0; i < 32; i++) {
                    if ((total_sent - total_completed) < static_cast<uint64_t>(max_inflight)) {
                        if (req_queues[client_idx].try_push(payload)) {
                            total_sent++;
                        }
                    }
                }

                // Receive responses
                Payload p;
                while (resp_queues[client_idx].try_pop(p)) {
                    total_completed++;
                }

                per_client_completed[client_idx].store(total_completed, std::memory_order_relaxed);

                if (stop_flag.load(std::memory_order_relaxed)) {
                    break;
                }
            }
        });
    }

    // Monitor
    sync_barrier.arrive_and_wait();
    auto start = std::chrono::steady_clock::now();
    auto run_duration = std::chrono::seconds(duration_secs);

    while (std::chrono::steady_clock::now() - start < run_duration) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    stop_flag.store(true, std::memory_order_relaxed);
    auto elapsed = std::chrono::steady_clock::now() - start;

    for (auto& t : threads) {
        t.join();
    }

    uint64_t total = 0;
    for (int i = 0; i < num_clients; i++) {
        total += per_client_completed[i].load(std::memory_order_relaxed);
    }

    return RunResult{
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed),
        total,
    };
}

int main(int argc, char* argv[]) {
    auto args = parse_args(argc, argv, "server_client_tbb.csv");

    if (args.threads < 2) {
        std::fprintf(stderr, "Need at least 2 threads (1 server + 1 client minimum)\n");
        return 1;
    }

    std::vector<BenchResult> results;

    bool run_mpsc_bench = (args.transport == "mpsc" || args.transport == "all");
    bool run_spsc_bench = (args.transport == "spsc" || args.transport == "all");

    if (run_mpsc_bench) {
        results.push_back(run_transport_benchmark(
            "server_client", "tbb_mpsc",
            args.threads, args.capacity, args.duration, args.inflight,
            args.warmup, args.runs, args.start_core,
            run_mpsc));
    }

    if (run_spsc_bench) {
        results.push_back(run_transport_benchmark(
            "server_client", "tbb_spsc",
            args.threads, args.capacity, args.duration, args.inflight,
            args.warmup, args.runs, args.start_core,
            run_spsc));
    }

    write_csv(args.output, results);
    return 0;
}
