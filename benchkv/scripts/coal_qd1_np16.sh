#!/bin/bash
#PBS -b 16
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

NP=16

echo "=== delegation NP=$NP QD=1 coalesce=6us ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
  -d 10 -r 1 --server-threads 1 --client-threads 46 --queue-depth 1 \
  -o "$OUTDIR/deleg_qd1_coal_np${NP}.parquet" \
  delegation --coalesce-rtt-us 6.0 \
  || echo "FAILED: delegation NP=$NP coalesce"

echo "=== delegation NP=$NP QD=1 no-coalesce ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
  -d 10 -r 1 --server-threads 1 --client-threads 46 --queue-depth 1 \
  -o "$OUTDIR/deleg_qd1_nocoal_np${NP}.parquet" \
  delegation \
  || echo "FAILED: delegation NP=$NP no-coalesce"

echo "=== ucx-am NP=$NP QD=1 ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
  -d 10 -r 1 --server-threads 1 --client-threads 46 --queue-depth 1 \
  -o "$OUTDIR/ucxam_qd1_coal_np${NP}.parquet" \
  ucx-am \
  || echo "FAILED: ucx-am NP=$NP"

echo "=== NP=$NP QD=1 coalesce test completed ==="
