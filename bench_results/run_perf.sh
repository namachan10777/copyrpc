#!/bin/bash
set -e

BINARY=./target/release/server_client
TRANSPORTS="onesided fast-forward lamport scq wcq-cas2 wcq lcrq lprq jiffy fetch-add"
THREADS="2 9"
EVENTS="ls_dmnd_fills_from_sys.local_l2,ls_dmnd_fills_from_sys.local_ccx,ls_dmnd_fills_from_sys.near_cache,ls_dmnd_fills_from_sys.far_cache,ls_dmnd_fills_from_sys.dram_io_near,ls_dmnd_fills_from_sys.dram_io_far"

for t in $TRANSPORTS; do
  for n in $THREADS; do
    clients=$((n - 1))
    echo "=== PERF: Transport=$t, threads=$n (${clients} clients) ==="
    sudo perf stat -e "$EVENTS" \
      timeout 10 taskset -c 0-31 $BINARY \
      -t "$t" -n "$n" -d 5 -r 1 --capacity 1024 -i 256 \
      --start-core 31 -o /dev/null 2>&1
    echo ""
  done
done

echo "=== ALL PERF RUNS COMPLETE ==="
