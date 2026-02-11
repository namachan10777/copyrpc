#!/bin/bash
#PBS -b 8
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:15:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/benchkv/logs/%r.log
#PBS -j o

set -eux

# Only run on job 0
JOB_IDX=${PBS_JOBID%%:*}
if [ "$JOB_IDX" != "0" ]; then
  echo "Skipping on job $JOB_IDX"
  exit 0
fi

cd "$WORKDIR"

module purge
module load openmpi/5.0.7/gcc11.4.0-cuda12.8.1

BENCH="$WORKDIR/target/release/benchkv"
if [[ ! -f "$BENCH" ]]; then
  echo "ERROR: Binary not found at $BENCH"
  exit 1
fi
OUTDIR="$WORKDIR/benchkv/result"
mkdir -p "$OUTDIR"

DURATION=5
RUNS=1
SERVER_THREADS=1
CLIENT_THREADS=46
QD=1

for NP in 2 4 6 8; do
  echo "=== meta NP=$NP ==="
  timeout 60 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/debug_meta_np${NP}.parquet" \
    meta \
    || echo "FAILED: meta NP=$NP"
done

echo "=== copyrpc-direct comparison ==="
for NP in 2 4 6 8; do
  echo "=== copyrpc-direct NP=$NP ==="
  timeout 60 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    --ring-size 4096 \
    -o "$OUTDIR/debug_direct_np${NP}.parquet" \
    copyrpc-direct \
    || echo "FAILED: copyrpc-direct NP=$NP"
done

echo "=== Debug benchmarks completed ==="
