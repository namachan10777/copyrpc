#!/bin/bash
#PBS -b 64
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:45:00
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

# === Delegation (all NP values) ===
for NP in 2 4 6 8 16 24 32 48 64; do
  # NP>=32: use small ring to avoid RDMA memory exhaustion
  if [ $NP -ge 32 ]; then
    RING_OPT="--ring-size 4096"
  else
    RING_OPT=""
  fi
  echo "=== delegation NP=$NP ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    $RING_OPT \
    -o "$OUTDIR/delegation_v2_np${NP}.parquet" \
    delegation \
    || echo "FAILED: delegation NP=$NP"
done

# === UCX-AM (all NP values) ===
for NP in 2 4 6 8 16 24 32 48 64; do
  echo "=== ucx-am NP=$NP ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none -x UCX_TLS=rc,self "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    -o "$OUTDIR/ucx_am_v2_np${NP}.parquet" \
    ucx-am \
    || echo "FAILED: ucx-am NP=$NP"
done

# === copyrpc-direct (all NP values, ring-size=4096 for NP>=16) ===
for NP in 2 4 6 8 16 24 32 48 64; do
  if [ $NP -ge 16 ]; then
    RING_OPT="--ring-size 4096"
  else
    RING_OPT=""
  fi
  echo "=== copyrpc-direct NP=$NP ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np $NP --map-by node --bind-to none "$BENCH" \
    -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
    $RING_OPT \
    -o "$OUTDIR/copyrpc_direct_v2_np${NP}.parquet" \
    copyrpc-direct \
    || echo "FAILED: copyrpc-direct NP=$NP"
done

echo "=== All v2 benchmarks completed ==="
