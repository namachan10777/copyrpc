#!/bin/bash
#PBS -b 17
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:30:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/rpc_bench/logs/%r.log
#PBS -j o

set -eux

export LOGDIR="$WORKDIR/rpc_bench/logs/$(echo $PBS_JOBID | sed -E 's/^[^:]*:([0-9]+)\.nqsv$/\1/')"
mkdir -p "$LOGDIR"

cd "$WORKDIR"

# Environment
module purge
module load openmpi/5.0.7/gcc11.4.0-cuda12.8.1

# Build (skip on compute nodes without libclang; pre-build on login node)
if [[ -f "$WORKDIR/target/release/rpc_bench" ]]; then
  echo "Binary already exists, skipping build"
else
  cargo build --release --bin rpc_bench
fi

BENCH="$WORKDIR/target/release/rpc_bench"
OUTDIR="$WORKDIR/rpc_bench/result/multi_client"
mkdir -p "$OUTDIR"

DURATION=10
RUNS=3
MSG_SIZE=32
QD=32

for NC in 6 8 12 16; do
  NP=$((NC + 1))

  echo "=== copyrpc NC=$NC ==="
  timeout 120 mpirun ${NQSV_MPIOPTS:-} -np $NP --bind-to none "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    -o "$OUTDIR/copyrpc_mc_nc${NC}.parquet" \
    --affinity-mode multinode --affinity-start 47 \
    copyrpc multi-client -i $QD \
    || echo "FAILED: copyrpc NC=$NC"

  echo "=== erpc NC=$NC ==="
  timeout 120 mpirun ${NQSV_MPIOPTS:-} -np $NP --bind-to none "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    -o "$OUTDIR/erpc_mc_nc${NC}.parquet" \
    --affinity-mode multinode --affinity-start 47 \
    erpc multi-client -i $QD \
    || echo "FAILED: erpc NC=$NC"

  echo "=== ucx-am NC=$NC ==="
  timeout 120 mpirun ${NQSV_MPIOPTS:-} -np $NP --bind-to none "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    -o "$OUTDIR/ucx_am_mc_nc${NC}.parquet" \
    --affinity-mode multinode --affinity-start 47 \
    ucx-am multi-client -i $QD \
    || echo "FAILED: ucx-am NC=$NC"
done

echo "=== All multi-client (17n) benchmarks completed ==="
