#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
FLUX_BENCH="$REPO_ROOT/target/release/flux_bench"

OUTPUT_DIR="$SCRIPT_DIR/bench_results"
mkdir -p "$OUTPUT_DIR"

THREADS="2,3,4,6,8,12,16,20,24"
ITERATIONS=10000
RUNS=5
WARMUP=2
CAPACITY=4096

echo "=== Flux All-to-All Benchmark ==="
echo "Threads: $THREADS"
echo "Iterations: $ITERATIONS"
echo "Runs: $RUNS"
echo "Warmup: $WARMUP"
echo "Capacity: $CAPACITY"
echo ""

# Legacy API (no batch)
echo ">>> Running Legacy API benchmark..."
"$FLUX_BENCH" \
    -n "$THREADS" \
    -i "$ITERATIONS" \
    -r "$RUNS" \
    -w "$WARMUP" \
    -c "$CAPACITY" \
    -o "$OUTPUT_DIR/legacy.parquet"

# Batch size 8
echo ""
echo ">>> Running Batch API (batch_size=8) benchmark..."
"$FLUX_BENCH" \
    -n "$THREADS" \
    -b 8 \
    -i "$ITERATIONS" \
    -r "$RUNS" \
    -w "$WARMUP" \
    -c "$CAPACITY" \
    -o "$OUTPUT_DIR/batch_8.parquet"

# Batch size 256
echo ""
echo ">>> Running Batch API (batch_size=256) benchmark..."
"$FLUX_BENCH" \
    -n "$THREADS" \
    -b 256 \
    -i "$ITERATIONS" \
    -r "$RUNS" \
    -w "$WARMUP" \
    -c "$CAPACITY" \
    -o "$OUTPUT_DIR/batch_256.parquet"

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to $OUTPUT_DIR/"
ls -la "$OUTPUT_DIR/"
