#!/bin/bash
set -e

RUST_BIN=./target/release/server_client
TBB_BIN=./tbb_bench/build/server_client
OUTDIR=bench_results/qd_perf
mkdir -p "$OUTDIR"

PERF_EVENTS="ls_dmnd_fills_from_sys.local_l2,ls_dmnd_fills_from_sys.local_ccx,ls_dmnd_fills_from_sys.near_cache,ls_dmnd_fills_from_sys.far_cache,ls_dmnd_fills_from_sys.dram_io_near,ls_dmnd_fills_from_sys.dram_io_far"

# Baseline transports
RUST_TRANSPORTS="onesided fast-forward fetch-add"
# Paper reference implementations
PAPER_TRANSPORTS="scq wcq wcq-cas2 lcrq lprq bbq jiffy"
TBB_TRANSPORTS="mpsc spsc"

THREADS="2 3 5 9 17"
QDS="1 8"

THROUGHPUT_DURATION=5
THROUGHPUT_RUNS=3
PERF_DURATION=5

# run_bench transport n qd binary_type
# binary_type: "rust" or "tbb"
# Outputs: {outdir}/{label}_n{n}_qd{qd}.json
run_bench() {
  local transport="$1"
  local n="$2"
  local qd="$3"
  local bin_type="$4"
  local clients=$((n - 1))

  if [ "$bin_type" = "rust" ]; then
    local label="$transport"
    local bin="$RUST_BIN"
    local bench_args="-t $transport -n $n --capacity 1024 --start-core 31"
  else
    local label="tbb_${transport}"
    local bin="$TBB_BIN"
    local bench_args="-t $transport -n $n"
  fi

  local outjson="${OUTDIR}/${label}_n${n}_qd${qd}.json"
  local tmpdir
  tmpdir=$(mktemp -d)

  echo "=== ${label}, n=${n} (${clients}c), QD=${qd} ==="

  # --- Phase 1: Throughput (3 runs) ---
  local median_mops="null"
  if timeout 45 taskset -c 0-31 $bin \
      $bench_args -d "$THROUGHPUT_DURATION" -r "$THROUGHPUT_RUNS" -i "$qd" \
      -o /dev/null > "$tmpdir/throughput.txt" 2>&1; then
    median_mops=$(grep -oP 'Median=\K[0-9.]+' "$tmpdir/throughput.txt" | tail -1)
    if [ -z "$median_mops" ]; then median_mops="null"; fi
    echo "  Throughput: ${median_mops} Mops/s"
  else
    echo "  Throughput: TIMEOUT/ERROR"
    cat "$tmpdir/throughput.txt" 2>/dev/null | tail -3
  fi

  # --- Phase 2: perf stat (1 run) ---
  local perf_ok=false
  if sudo perf stat -j -e "$PERF_EVENTS" -- \
      timeout 15 taskset -c 0-31 $bin \
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
    --arg transport "$label" \
    --argjson threads "$n" \
    --argjson clients "$clients" \
    --argjson qd "$qd" \
    --argjson throughput_duration "$THROUGHPUT_DURATION" \
    --argjson throughput_runs "$THROUGHPUT_RUNS" \
    --argjson perf_duration "$PERF_DURATION" \
    --argjson median_mops "${median_mops}" \
    --argjson perf_run_mops "${perf_mops}" \
    --argjson perf_events "$perf_array" \
    '{
      transport: $transport,
      threads: $threads,
      clients: $clients,
      qd: $qd,
      throughput_duration_secs: $throughput_duration,
      throughput_runs: $throughput_runs,
      perf_duration_secs: $perf_duration,
      median_mops: $median_mops,
      perf_run_mops: $perf_run_mops,
      perf_events: $perf_events
    }' > "$outjson"

  echo "  -> $outjson"
  rm -rf "$tmpdir"
  echo ""
}

# --- Run all benchmarks ---

for t in $RUST_TRANSPORTS $PAPER_TRANSPORTS; do
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

# --- Combine all JSON files into summary ---
echo "=== Generating summary ==="
jq -s '.' "$OUTDIR"/*.json > "$OUTDIR/summary.json"
echo "Summary: $OUTDIR/summary.json ($(jq length "$OUTDIR/summary.json") entries)"

echo "=== ALL QD+PERF BENCHMARKS COMPLETE ==="
