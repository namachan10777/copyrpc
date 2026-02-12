#!/bin/bash
#PBS -b 8
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:15:00
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

NP=8
COMMON="-d 10 -r 3 --server-threads 1 --client-threads 46 --queue-depth 1"

echo "=== Fig2: delegation NP=$NP (batch-hold default) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/paper_deleg_np${NP}.parquet" \
  delegation \
  || echo "FAILED: delegation NP=$NP"

echo "=== Fig2: copyrpc-direct NP=$NP ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/paper_direct_np${NP}.parquet" \
  copyrpc-direct \
  || echo "FAILED: copyrpc-direct NP=$NP"

echo "=== Fig2: NP=$NP completed ==="
