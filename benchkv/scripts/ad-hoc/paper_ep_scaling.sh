#!/bin/bash
#PBS -b 2
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

BENCH="$WORKDIR/target/release/rpc_bench"
OUTDIR="$WORKDIR/rpc_bench/result/paper"
mkdir -p "$OUTDIR"

for E in 1 2 4 8 16 32 64; do
  echo "=== copyrpc 1-to-1 QD=1 E=$E ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np 2 --map-by node --bind-to none "$BENCH" \
    -d 10 -r 3 -s 32 \
    -o "$OUTDIR/copyrpc_1to1_qd1_e${E}.parquet" \
    --affinity-mode multinode --affinity-start 47 \
    copyrpc --ring-size 1048576 one-to-one -e $E -i 1 -t 1 \
    || echo "FAILED: copyrpc E=$E QD=1"

  echo "=== copyrpc 1-to-1 QD=256 E=$E ==="
  timeout 120 mpirun --hostfile "$PBS_NODEFILE" -np 2 --map-by node --bind-to none "$BENCH" \
    -d 10 -r 3 -s 32 \
    -o "$OUTDIR/copyrpc_1to1_qd256_e${E}.parquet" \
    --affinity-mode multinode --affinity-start 47 \
    copyrpc --ring-size 1048576 one-to-one -e $E -i 256 -t 1 \
    || echo "FAILED: copyrpc E=$E QD=256"
done

echo "=== EP scaling benchmarks completed ==="
