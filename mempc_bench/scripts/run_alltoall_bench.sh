#!/bin/bash
set -e

RUST_BIN=./target/release/all_to_all
TBB_BIN=./tbb_bench/build/all_to_all
OUTDIR=mempc_bench/result/alltoall
DISTDIR=mempc_bench/dist
mkdir -p "$OUTDIR" "$DISTDIR"

RUST_TRANSPORTS="onesided fast-forward lamport fetch-add scq wcq-cas2 wcq bbq jiffy"
TBB_TRANSPORTS="tbb_alltoall"

THREADS="2 4 8 16"
QDS="1 2 4 8 16 32 64 128 256"

DURATION=5
RUNS=3
CAPACITY=1024
START_CORE=31
TIMEOUT=60

run_bench() {
  local transport="$1"
  local n="$2"
  local qd="$3"
  local bin_type="$4"

  if [ "$bin_type" = "rust" ]; then
    local label="$transport"
    local bin="$RUST_BIN"
    local bench_args="-t $transport -n $n --capacity $CAPACITY --start-core $START_CORE"
  else
    local label="$transport"
    local bin="$TBB_BIN"
    local bench_args="-n $n -c $CAPACITY -s $START_CORE"
  fi

  local outjson="${OUTDIR}/${label}_n${n}_qd${qd}.json"
  local tmpfile
  tmpfile=$(mktemp)

  echo "=== ${label}, n=${n}, QD=${qd} ==="

  local median_mops="null"
  if timeout "$TIMEOUT" taskset -c 0-31 $bin \
      $bench_args -d "$DURATION" -r "$RUNS" -i "$qd" \
      -o /dev/null > "$tmpfile" 2>&1; then
    median_mops=$(grep -oP 'Median=\K[0-9.]+' "$tmpfile" | tail -1)
    if [ -z "$median_mops" ]; then median_mops="null"; fi
    echo "  Throughput: ${median_mops} Mops/s"
  else
    echo "  TIMEOUT/ERROR"
    tail -3 "$tmpfile" 2>/dev/null
  fi

  jq -n \
    --arg scenario "alltoall" \
    --arg transport "$label" \
    --argjson threads "$n" \
    --argjson qd "$qd" \
    --argjson capacity "$CAPACITY" \
    --argjson duration_secs "$DURATION" \
    --argjson runs "$RUNS" \
    --argjson median_mops "$median_mops" \
    '{
      scenario: $scenario,
      transport: $transport,
      threads: $threads,
      qd: $qd,
      capacity: $capacity,
      duration_secs: $duration_secs,
      runs: $runs,
      median_mops: $median_mops
    }' > "$outjson"

  echo "  -> $outjson"
  rm -f "$tmpfile"
  echo ""
}

# --- Run all benchmarks ---

for t in $RUST_TRANSPORTS; do
  for n in $THREADS; do
    for qd in $QDS; do
      run_bench "$t" "$n" "$qd" "rust"
    done
  done
done

for t in $TBB_TRANSPORTS; do
  for n in $THREADS; do
    for qd in $QDS; do
      run_bench "$t" "$n" "$qd" "tbb"
    done
  done
done

# --- Generate summary ---
echo "=== Generating summary ==="
jq -s '.' "$OUTDIR"/*.json > "$DISTDIR/alltoall_summary.json"
echo "Summary: $DISTDIR/alltoall_summary.json ($(jq length "$DISTDIR/alltoall_summary.json") entries)"
echo "=== ALL ALLTOALL BENCHMARKS COMPLETE ==="
