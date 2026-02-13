#!/bin/bash
#PBS -b 8
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:10:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/benchkv/logs/%r.log
#PBS -j o

set -eux

JOB_IDX=${PBS_JOBID%%:*}
if [ "$JOB_IDX" != "0" ]; then
  echo "Skipping on job $JOB_IDX"
  exit 0
fi

cd "$WORKDIR"
module purge
module load openmpi/5.0.7/gcc11.4.0-cuda12.8.1

BENCH="$WORKDIR/target/release/benchkv"
OUTDIR="$WORKDIR/benchkv/result"
mkdir -p "$OUTDIR"

COMMON="-d 10 -r 1 --server-threads 1 --client-threads 46 --queue-depth 1"

echo "=== NP=8 adaptive hold ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/adaptive_hold_np8.parquet" \
  delegation --adaptive-hold \
  || echo "FAILED: adaptive NP=8"

echo "=== NP=8 fixed hold 10us (baseline) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/adaptive_hold_fixed_np8.parquet" \
  delegation \
  || echo "FAILED: fixed NP=8"

echo "=== NP=8 no hold ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/adaptive_hold_nohold_np8.parquet" \
  delegation --batch-hold-us 0.0 \
  || echo "FAILED: no-hold NP=8"

echo "=== adaptive hold test completed ==="
