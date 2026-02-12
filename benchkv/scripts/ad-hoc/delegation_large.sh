#!/bin/bash
#PBS -b 64
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

DURATION=10
RUNS=3
SERVER_THREADS=1
CLIENT_THREADS=46
QD=1

# NP>=32: use small ring_size to avoid RDMA memory exhaustion
for NP in 32 48 64; do
  echo "=== delegation NP=$NP (ring_size=4096) ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    --ring-size 4096 \
    -o "$OUTDIR/delegation_np${NP}.parquet" \
    delegation \
    || echo "FAILED: delegation NP=$NP"
done

echo "=== Delegation large-NP benchmarks completed ==="
