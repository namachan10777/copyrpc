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

echo "=== delegation NP=8 dynamic hold (multiplier=1.0) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/dynhold_m1.0_np8.parquet" \
  delegation --hold-rtt-multiplier 1.0 \
  || echo "FAILED: dynhold m=1.0"

echo "=== delegation NP=8 dynamic hold (multiplier=1.5) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/dynhold_m1.5_np8.parquet" \
  delegation --hold-rtt-multiplier 1.5 \
  || echo "FAILED: dynhold m=1.5"

echo "=== delegation NP=8 dynamic hold (multiplier=2.0) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/dynhold_m2.0_np8.parquet" \
  delegation --hold-rtt-multiplier 2.0 \
  || echo "FAILED: dynhold m=2.0"

echo "=== delegation NP=8 fixed hold 10us (baseline) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  $COMMON -o "$OUTDIR/dynhold_fixed_np8.parquet" \
  delegation \
  || echo "FAILED: fixed hold"

echo "=== NP=8 dynamic hold test completed ==="
