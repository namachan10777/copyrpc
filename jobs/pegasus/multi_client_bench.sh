#!/bin/bash
#PBS -b 9
#PBS -q gen_S
#PBS -A NBBG
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=01:00:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/jobs/pegasus/logs/%r.log
#PBS -j o

set -ux

export LOGDIR="$WORKDIR/jobs/pegasus/logs/$(echo $PBS_JOBID | sed -E 's/^[^:]*:([0-9]+)\.nqsv$/\1/')"
mkdir -p "$LOGDIR"

cd "$WORKDIR"

# Mercury shared libs (spack)
export LD_LIBRARY_PATH="$WORKDIR/spack/.spack-env/view/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Build
cargo build --release --bin rpc_bench

BENCH="$WORKDIR/target/release/rpc_bench"
OUTDIR="$LOGDIR/results"
mkdir -p "$OUTDIR"

DURATION=10
RUNS=3
MSG_SIZE=32
QD=32

HOSTFILE="$PBS_NODEFILE"
SERVER=$(head -1 "$HOSTFILE")
mapfile -t ALL_HOSTS < "$HOSTFILE"
CLIENT_HOSTS=("${ALL_HOSTS[@]:1}")

for NC in 1 2 3 4 6 8 12 16 24 32 48 64 96 128; do
  NP=$((NC + 1))

  # Generate rankfile: rank 0 -> server node, ranks 1..NC -> round-robin on client nodes
  RANKFILE="$LOGDIR/rankfile_nc${NC}.txt"
  echo "rank 0=$SERVER slot=0" > "$RANKFILE"
  for i in $(seq 1 $NC); do
    NODE_IDX=$(( (i - 1) % ${#CLIENT_HOSTS[@]} ))
    SLOT=$(( (i - 1) / ${#CLIENT_HOSTS[@]} ))
    echo "rank $i=${CLIENT_HOSTS[$NODE_IDX]} slot=$SLOT" >> "$RANKFILE"
  done

  echo "=== copyrpc NC=$NC ==="
  timeout 120 mpirun -np $NP --rankfile "$RANKFILE" "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    --affinity-mode multinode --affinity-start 47 \
    copyrpc multi-client -i $QD \
    -o "$OUTDIR/copyrpc_mc_nc${NC}.parquet" \
    || echo "FAILED: copyrpc NC=$NC"

  echo "=== erpc NC=$NC ==="
  timeout 120 mpirun -np $NP --rankfile "$RANKFILE" "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    --affinity-mode multinode --affinity-start 47 \
    erpc multi-client -i $QD \
    -o "$OUTDIR/erpc_mc_nc${NC}.parquet" \
    || echo "FAILED: erpc NC=$NC"

  echo "=== ucx-am NC=$NC ==="
  timeout 120 mpirun -np $NP --rankfile "$RANKFILE" "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    --affinity-mode multinode --affinity-start 47 \
    ucx-am multi-client -i $QD \
    -o "$OUTDIR/ucx_am_mc_nc${NC}.parquet" \
    || echo "FAILED: ucx-am NC=$NC"
done

echo "=== All multi-client benchmarks completed ==="
