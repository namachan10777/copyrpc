#!/bin/bash
#PBS -b 64
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:45:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/benchkv/logs/%r.log
#PBS -j o

set -eux

# Only run on job 0 (NQS/V runs the script on every allocated node)
JOB_IDX=${PBS_JOBID%%:*}
if [ "$JOB_IDX" != "0" ]; then
  echo "Skipping on job $JOB_IDX (mpirun dispatched from job 0)"
  exit 0
fi

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

for NP in 2 4 6 8 16 24 32 48 64; do
  echo "=== delegation NP=$NP ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/delegation_np${NP}.parquet" \
    delegation \
    || echo "FAILED: delegation NP=$NP"
done

echo "=== meta comparison ==="
for NP in 2 4 8 16 32 64; do
  echo "=== meta NP=$NP ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/deleg_meta_np${NP}.parquet" \
    meta \
    || echo "FAILED: meta NP=$NP"
done

echo "=== Delegation benchmarks completed ==="
