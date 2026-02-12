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
BUDGET_MAX=32

# NP=8 control: no adaptive budget (should collapse)
echo "=== delegation NP=8 NO budget (control) ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  --qd-sample-dir "$QDSAMPLEDIR/adaptive_np8_nobudget" --qd-sample-interval 1 \
  -o "$OUTDIR/adaptive_np8_nobudget.parquet" \
  delegation --budget-max 0 \
  || echo "FAILED: delegation NP=8 no budget"

# NP=8 with adaptive budget (should recover)
echo "=== delegation NP=8 adaptive budget=$BUDGET_MAX ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 8 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  --qd-sample-dir "$QDSAMPLEDIR/adaptive_np8_budget${BUDGET_MAX}" --qd-sample-interval 1 \
  -o "$OUTDIR/adaptive_np8_budget${BUDGET_MAX}.parquet" \
  delegation --budget-max $BUDGET_MAX \
  || echo "FAILED: delegation NP=8 budget=$BUDGET_MAX"

# NP=16 with adaptive budget (should maintain high throughput, u→0)
echo "=== delegation NP=16 adaptive budget=$BUDGET_MAX ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 16 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  --qd-sample-dir "$QDSAMPLEDIR/adaptive_np16_budget${BUDGET_MAX}" --qd-sample-interval 1 \
  -o "$OUTDIR/adaptive_np16_budget${BUDGET_MAX}.parquet" \
  delegation --budget-max $BUDGET_MAX \
  || echo "FAILED: delegation NP=16 budget=$BUDGET_MAX"

# NP=2 sanity check with adaptive budget
echo "=== delegation NP=2 adaptive budget=$BUDGET_MAX ==="
timeout 180 mpirun --hostfile "$PBS_NODEFILE" -np 2 --map-by node --bind-to none "$BENCH" \
  -d $DURATION -r $RUNS --server-threads $SERVER_THREADS --client-threads $CLIENT_THREADS --queue-depth $QD \
  -o "$OUTDIR/adaptive_np2_budget${BUDGET_MAX}.parquet" \
  delegation --budget-max $BUDGET_MAX \
  || echo "FAILED: delegation NP=2 budget=$BUDGET_MAX"

echo "=== All adaptive budget benchmarks completed ==="
