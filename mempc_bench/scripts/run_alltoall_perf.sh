#!/bin/bash
set -e

RUST_BIN=./target/release/all_to_all
TBB_BIN=./tbb_bench/build/all_to_all
OUTDIR=mempc_bench/result/alltoall
DISTDIR=mempc_bench/dist
mkdir -p "$OUTDIR" "$DISTDIR"

PERF_EVENTS="ls_dmnd_fills_from_sys.local_l2,ls_dmnd_fills_from_sys.local_ccx,ls_dmnd_fills_from_sys.near_cache,ls_dmnd_fills_from_sys.far_cache,ls_dmnd_fills_from_sys.dram_io_near,ls_dmnd_fills_from_sys.dram_io_far"

RUST_TRANSPORTS="onesided fast-forward lamport fetch-add scq wcq-cas2 wcq bbq jiffy"
TBB_TRANSPORTS="tbb_alltoall"

THREADS="2 4 8 16"
QDS="1 2 4 8 16 32 64 128 256"

DURATION=5
RUNS=3
PERF_DURATION=5
CAPACITY=1024
START_CORE=31
BENCH_TIMEOUT=60
PERF_TIMEOUT=15

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
  local tmpdir
  tmpdir=$(mktemp -d)

  echo "=== ${label}, n=${n}, QD=${qd} ==="

  # --- Phase 1: Throughput (3 runs) ---
  local median_mops="null"
  if timeout "$BENCH_TIMEOUT" taskset -c 0-31 $bin \
      $bench_args -d "$DURATION" -r "$RUNS" -i "$qd" \
      -o /dev/null > "$tmpdir/throughput.txt" 2>&1; then
    median_mops=$(grep -oP 'Median=\K[0-9.]+' "$tmpdir/throughput.txt" | tail -1)
    if [ -z "$median_mops" ]; then median_mops="null"; fi
    echo "  Throughput: ${median_mops} Mops/s"
  else
    echo "  Throughput: TIMEOUT/ERROR"
    tail -3 "$tmpdir/throughput.txt" 2>/dev/null
  fi

  # --- Phase 2: perf stat (1 run) ---
  local perf_ok=false
  if sudo perf stat -j -e "$PERF_EVENTS" -- \
      timeout "$PERF_TIMEOUT" taskset -c 0-31 $bin \
      $bench_args -d "$PERF_DURATION" -r 1 -i "$qd" \
      -o /dev/null > "$tmpdir/perf_stdout.txt" 2> "$tmpdir/perf_raw.json"; then
    perf_ok=true
  else
    echo "  perf: TIMEOUT/ERROR"
  fi

  local perf_mops="null"
  if [ -f "$tmpdir/perf_stdout.txt" ]; then
    perf_mops=$(grep -oP 'Median=\K[0-9.]+' "$tmpdir/perf_stdout.txt" | tail -1)
    if [ -z "$perf_mops" ]; then perf_mops="null"; fi
  fi

  # --- Build JSON ---
  local perf_array="[]"
  if [ "$perf_ok" = true ] && [ -s "$tmpdir/perf_raw.json" ]; then
    perf_array=$(jq -s '.' "$tmpdir/perf_raw.json" 2>/dev/null || echo "[]")
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
    --argjson perf_duration_secs "$PERF_DURATION" \
    --argjson perf_run_mops "$perf_mops" \
    --argjson perf_events "$perf_array" \
    '{
      scenario: $scenario,
      transport: $transport,
      threads: $threads,
      qd: $qd,
      capacity: $capacity,
      duration_secs: $duration_secs,
      runs: $runs,
      median_mops: $median_mops,
      perf_duration_secs: $perf_duration_secs,
      perf_run_mops: $perf_run_mops,
      perf_events: $perf_events
    }' > "$outjson"

  echo "  -> $outjson"
  rm -rf "$tmpdir"
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
echo "=== ALL ALLTOALL+PERF BENCHMARKS COMPLETE ==="
