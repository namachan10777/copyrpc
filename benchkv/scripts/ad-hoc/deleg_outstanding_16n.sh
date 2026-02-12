#!/bin/bash
#PBS -b 16
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:30:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/benchkv/logs/%r.log
#PBS -j o

set -eux

# Only run on job 0 (NQS/V runs the script on every allocated node)
JOB_IDX=${PBS_JOBID%%:*}
if [ "$JOB_IDX" != "0" ]; then
  echo "Skipping on job $JOB_IDX (mpirun dispatched from job 0)"
  exit 0
fi

cd "$WORKDIR"

module purge
module load openmpi/5.0.7/gcc11.4.0-cuda12.8.1

BENCH="$WORKDIR/target/release/benchkv"
OUTDIR="$WORKDIR/benchkv/result"
QDSAMPLEDIR="$WORKDIR/benchkv/qd_samples"
mkdir -p "$OUTDIR" "$QDSAMPLEDIR"

DURATION=10
RUNS=1
SERVER_THREADS=1
CLIENT_THREADS=46
QD=32

# NP=2: sanity check (should see outstanding ~46 quickly)
echo "=== delegation NP=2 (with QD sampling) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 2 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  --qd-sample-dir "$QDSAMPLEDIR/deleg_np2" --qd-sample-interval 1 \
  -o "$OUTDIR/deleg_outstanding_np2.parquet" \
  delegation \
  || echo "FAILED: delegation NP=2"

# NP=16: the target — does outstanding reach 1380?
echo "=== delegation NP=16 (with QD sampling) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 16 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  --qd-sample-dir "$QDSAMPLEDIR/deleg_np16" --qd-sample-interval 1 \
  -o "$OUTDIR/deleg_outstanding_np16.parquet" \
  delegation \
  || echo "FAILED: delegation NP=16"

# NP=8: control case (high throughput, should see high outstanding)
echo "=== delegation NP=8 (with QD sampling) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  --qd-sample-dir "$QDSAMPLEDIR/deleg_np8" --qd-sample-interval 1 \
  -o "$OUTDIR/deleg_outstanding_np8.parquet" \
  delegation \
  || echo "FAILED: delegation NP=8"

echo "=== All outstanding profiling benchmarks completed ==="
