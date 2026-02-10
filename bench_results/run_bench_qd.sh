#!/bin/bash
set -e

BINARY=./target/release/server_client
OUTDIR=bench_results/qd
mkdir -p "$OUTDIR"

TRANSPORTS="onesided fast-forward lamport scq wcq-cas2 wcq lcrq lprq bbq jiffy fetch-add"
THREADS="2 3 5 9 17"
QDS="1 8 32"

for t in $TRANSPORTS; do
  for n in $THREADS; do
    for qd in $QDS; do
      clients=$((n - 1))
      outfile="${OUTDIR}/results_${t}_n${n}_qd${qd}.parquet"
      echo "=== Transport: $t, threads: $n (${clients}c), QD=$qd ==="
      timeout 60 taskset -c 0-31 $BINARY \
        -t "$t" -n "$n" -d 5 -r 3 --capacity 1024 -i "$qd" \
        --start-core 31 -o "$outfile" 2>&1 || echo "TIMEOUT/ERROR: $t n=$n qd=$qd"
      echo ""
    done
  done
done

echo "=== ALL QD BENCHMARKS COMPLETE ==="
