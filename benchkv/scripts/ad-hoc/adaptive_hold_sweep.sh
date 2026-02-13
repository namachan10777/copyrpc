#!/bin/bash
#PBS -b 32
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:30:00
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

for NP in 2 4 8 16 32; do
  echo "=== NP=$NP adaptive hold ==="
  timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    $COMMON -o "$OUTDIR/adaptive_np${NP}.parquet" \
    delegation --adaptive-hold \
    || echo "FAILED: adaptive NP=$NP"

  echo "=== NP=$NP fixed hold 10us ==="
  timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    $COMMON -o "$OUTDIR/adaptive_fixed_np${NP}.parquet" \
    delegation \
    || echo "FAILED: fixed NP=$NP"
done

echo "=== adaptive hold sweep completed ==="
