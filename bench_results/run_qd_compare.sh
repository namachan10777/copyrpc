#!/bin/bash
set -e

RUST_BIN=./target/release/server_client
TBB_BIN=./tbb_bench/build/server_client
OUTDIR=bench_results/qd_compare
mkdir -p "$OUTDIR"

RUST_TRANSPORTS="onesided fast-forward fetch-add"
TBB_TRANSPORTS="mpsc spsc"
THREADS="2 3 5 9 17"
QDS="1 8"

# Rust benchmarks
for t in $RUST_TRANSPORTS; do
  for n in $THREADS; do
    for qd in $QDS; do
      clients=$((n - 1))
      outfile="${OUTDIR}/rust_${t}_n${n}_qd${qd}.parquet"
      echo "=== Rust: $t, n=$n (${clients}c), QD=$qd ==="
      timeout 60 taskset -c 0-31 $RUST_BIN \
        -t "$t" -n "$n" -d 5 -r 3 --capacity 1024 -i "$qd" \
        --start-core 31 -o "$outfile" 2>&1 || echo "TIMEOUT/ERROR: $t n=$n qd=$qd"
      echo ""
    done
  done
done

# TBB benchmarks
for t in $TBB_TRANSPORTS; do
  for n in $THREADS; do
    for qd in $QDS; do
      clients=$((n - 1))
      outfile="${OUTDIR}/tbb_${t}_n${n}_qd${qd}.csv"
      echo "=== TBB: $t, n=$n (${clients}c), QD=$qd ==="
      timeout 60 taskset -c 0-31 $TBB_BIN \
        -t "$t" -n "$n" -d 5 -r 3 -c 1024 -i "$qd" \
        -s 31 -o "$outfile" 2>&1 || echo "TIMEOUT/ERROR: tbb_$t n=$n qd=$qd"
      echo ""
    done
  done
done

echo "=== ALL QD COMPARE BENCHMARKS COMPLETE ==="
