#!/bin/bash
#PBS -b 16
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:30:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/benchkv/logs/%r.log
#PBS -j o

set -eux

export LOGDIR="$WORKDIR/benchkv/logs/$(echo $PBS_JOBID | sed -E 's/^[^:]*:([0-9]+)\.nqsv$/\1/')"
mkdir -p "$LOGDIR"

cd "$WORKDIR"

module purge
module load openmpi/5.0.7/gcc11.4.0-cuda12.8.1

if [[ -f "$WORKDIR/target/release/benchkv" ]]; then
  echo "Binary already exists, skipping build"
else
  cargo build --release --package benchkv
fi

BENCH="$WORKDIR/target/release/benchkv"
OUTDIR="$WORKDIR/benchkv/result"
mkdir -p "$OUTDIR"

DURATION=10
RUNS=3
SERVER_THREADS=1
CLIENT_THREADS=46
QD=1

for NP in 12 16; do
  echo "=== meta NP=$NP ==="
  timeout 120 mpirun ${NQSV_MPIOPTS:-} -np $NP --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/meta_np${NP}.parquet" \
    meta \
    || echo "FAILED: meta NP=$NP"

  echo "=== copyrpc-direct NP=$NP ==="
  timeout 120 mpirun ${NQSV_MPIOPTS:-} -np $NP --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/copyrpc_direct_np${NP}.parquet" \
    copyrpc-direct \
    || echo "FAILED: copyrpc-direct NP=$NP"

  echo "=== ucx-am NP=$NP ==="
  timeout 120 mpirun ${NQSV_MPIOPTS:-} -np $NP --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/ucx_am_np${NP}.parquet" \
    ucx-am \
    || echo "FAILED: ucx-am NP=$NP"
done

echo "=== All benchkv (16n) benchmarks completed ==="
