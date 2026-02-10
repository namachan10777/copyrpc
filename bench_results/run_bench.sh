#!/bin/bash
set -e

BINARY=./target/release/server_client
OUTDIR=bench_results
TRANSPORTS="onesided fast-forward lamport scq wcq-cas2 wcq lcrq lprq bbq jiffy fetch-add"
THREADS="2 3 5 9 17 33"

for t in $TRANSPORTS; do
  for n in $THREADS; do
    clients=$((n - 1))
    outfile="${OUTDIR}/results_${t}_n${n}.parquet"
    echo "=== Transport: $t, threads: $n (${clients} clients) ==="
    timeout 120 taskset -c 0-31 $BINARY \
      -t "$t" -n "$n" -d 10 -r 7 --capacity 1024 -i 256 \
      --start-core 31 -o "$outfile" 2>&1 || echo "TIMEOUT or ERROR: $t n=$n"
    echo ""
  done
done

echo "=== ALL BENCHMARKS COMPLETE ==="
