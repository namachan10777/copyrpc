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

DURATION=10
RUNS=1
SERVER_THREADS=1
CLIENT_THREADS=46
QD=32

for NP in 2 4 8 16 32; do
  echo "=== delegation NP=$NP ==="
  timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/deleg_qd32_np${NP}.parquet" \
    delegation \
    || echo "FAILED: delegation NP=$NP"
done

for NP in 2 4 8 16 32; do
  echo "=== ucx-am NP=$NP ==="
  timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/ucxam_qd32_np${NP}.parquet" \
    ucx-am \
    || echo "FAILED: ucx-am NP=$NP"
done

echo "=== All benchmarks completed ==="
