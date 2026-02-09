// TBB All-to-All Benchmark
//
// N-thread all-to-all communication using tbb::concurrent_bounded_queue (SPSC usage pattern).
// Matches the communication pattern of mempc_bench/src/bin/all_to_all.rs (Flux benchmark).

#include "common.h"

RunResult run_all_to_all(int n, int capacity, int duration_secs,
                         int max_inflight, int start_core) {
    // N*(N-1) request queues + N*(N-1) response queues
    // req_queues[i][j]: thread i -> thread j (request)
    // resp_queues[i][j]: thread i -> thread j (response)
    std::vector<std::vector<tbb::concurrent_bounded_queue<Payload>>> req_queues(n);
    std::vector<std::vector<tbb::concurrent_bounded_queue<Payload>>> resp_queues(n);
    for (int i = 0; i < n; i++) {
        req_queues[i].resize(n);
        resp_queues[i].resize(n);
        for (int j = 0; j < n; j++) {
            req_queues[i][j].set_capacity(capacity);
            resp_queues[i][j].set_capacity(capacity);
        }
    }

    std::vector<std::atomic<uint64_t>> per_thread_completed(n);
    for (int i = 0; i < n; i++) {
        per_thread_completed[i].store(0, std::memory_order_relaxed);
    }
    std::atomic<bool> stop_flag{false};
    std::barrier sync_barrier(n + 1);

    Payload payload{};
    std::vector<std::thread> threads;
    threads.reserve(n);

    for (int me = 0; me < n; me++) {
        threads.emplace_back([&, me]() {
            pin_to_core(start_core - me);

            std::vector<int> peers;
            for (int p = 0; p < n; p++) {
                if (p != me) peers.push_back(p);
            }

            sync_barrier.arrive_and_wait();

            uint64_t total_sent = 0;
            uint64_t total_completed = 0;
            bool can_send = true;

            while (true) {
                for (int batch = 0; batch < 1024; batch++) {
                    bool any_sent = false;
                    if (can_send) {
                        for (int j : peers) {
                            if (req_queues[me][j].try_push(payload)) {
                                total_sent++;
                                any_sent = true;
                            }
                        }
                    }

                    if (!any_sent || (total_sent & 0x1F) == 0) {
                        // Process incoming requests and reply
                        for (int j : peers) {
                            Payload p;
                            while (req_queues[j][me].try_pop(p)) {
                                resp_queues[me][j].push(p);
                            }
                        }
                        // Receive responses
                        for (int j : peers) {
                            Payload p;
                            while (resp_queues[j][me].try_pop(p)) {
                                total_completed++;
                            }
                        }
                        can_send = (total_sent - total_completed) < static_cast<uint64_t>(max_inflight);
                    }
                }

                per_thread_completed[me].store(total_completed, std::memory_order_relaxed);

                if (stop_flag.load(std::memory_order_relaxed)) {
                    break;
                }
            }

            // Final drain of responses
            for (int j : peers) {
                Payload p;
                while (resp_queues[j][me].try_pop(p)) {
                    total_completed++;
                }
            }
            per_thread_completed[me].store(total_completed, std::memory_order_relaxed);
        });
    }

    // Monitor thread
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
    for (int i = 0; i < n; i++) {
        total += per_thread_completed[i].load(std::memory_order_relaxed);
    }

    return RunResult{
        std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed),
        total,
    };
}

int main(int argc, char* argv[]) {
    auto args = parse_args(argc, argv, "all_to_all_tbb.csv");

    if (args.threads < 2) {
        std::fprintf(stderr, "Need at least 2 threads\n");
        return 1;
    }

    std::vector<BenchResult> results;
    results.push_back(run_transport_benchmark(
        "flux", "tbb_bounded",
        args.threads, args.capacity, args.duration, args.inflight,
        args.warmup, args.runs, args.start_core,
        run_all_to_all));

    write_csv(args.output, results);
    return 0;
}
