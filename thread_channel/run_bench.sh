#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

OUTPUT_DIR="$SCRIPT_DIR/bench_results"
mkdir -p "$OUTPUT_DIR"

DURATION=10
RUNS=3
WARMUP=1
CAPACITY=1024
INFLIGHT=256

echo "=== Cluster All-to-All Benchmark (Flux + Mesh) ==="
echo "Duration: ${DURATION}s"
echo "Runs: $RUNS"
echo "Warmup: $WARMUP"
echo "Capacity: $CAPACITY"
echo "Inflight: $INFLIGHT"
echo ""

# Build with all features
echo ">>> Building cluster_bench..."
cargo build --release --bin cluster_bench \
    --manifest-path "$SCRIPT_DIR/Cargo.toml" \
    --features "bench-bin,rtrb,omango,crossbeam"

CLUSTER_BENCH="$REPO_ROOT/target/release/cluster_bench"

# Run for various thread counts
for N in 2 4 8 16; do
    echo ""
    echo ">>> Running n=$N threads..."
    "$CLUSTER_BENCH" \
        -n "$N" \
        -d "$DURATION" \
        -c "$CAPACITY" \
        -r "$RUNS" \
        -w "$WARMUP" \
        -i "$INFLIGHT" \
        -o "$OUTPUT_DIR/results_n${N}.parquet"
done

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to $OUTPUT_DIR/"
ls -la "$OUTPUT_DIR/"
