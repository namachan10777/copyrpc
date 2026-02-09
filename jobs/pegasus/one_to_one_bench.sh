#!/bin/bash
#PBS -b 2
#PBS -q gpu
#PBS -A NBB
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=04:00:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/jobs/pegasus/logs/%r.log
#PBS -j o

set -ux

export LOGDIR="$WORKDIR/jobs/pegasus/logs/$(echo $PBS_JOBID | sed -E 's/^[^:]*:([0-9]+)\.nqsv$/\1/')"
mkdir -p "$LOGDIR"

cd "$WORKDIR"

# Spack shared libs
export LD_LIBRARY_PATH="$WORKDIR/spack/.spack-env/view/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Build
cargo build --release --bin rpc_bench

BENCH="$WORKDIR/target/release/rpc_bench"
OUTDIR="$LOGDIR/results"
mkdir -p "$OUTDIR"

DURATION=10
RUNS=3
MSG_SIZE=32

# copyrpc: QD x Threads x Endpoints (E >= T, E % T == 0)
for QD in 1 32 256; do
  for T in 1 2 4 8 16; do
    for E in 1 2 3 4 6 8 12 16 24 32 48 64 96 128 192 256 384 512 768 1024; do
      [[ $E -lt $T || $((E % T)) -ne 0 ]] && continue
      echo "=== copyrpc QD=$QD T=$T E=$E ==="
      timeout 120 mpirun -np 2 "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
        -o "$OUTDIR/copyrpc_qd${QD}_t${T}_e${E}.parquet" \
        --affinity-mode multinode --affinity-start 47 \
        copyrpc one-to-one -e $E -i $QD -t $T \
        || echo "FAILED: copyrpc QD=$QD T=$T E=$E"
    done
  done
done

# erpc: QD x Threads
for QD in 1 32 256; do
  for T in 1 2 4 8 16; do
    echo "=== erpc QD=$QD T=$T ==="
    timeout 120 mpirun -np 2 "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
      -o "$OUTDIR/erpc_qd${QD}_t${T}.parquet" \
      --affinity-mode multinode --affinity-start 47 \
      erpc one-to-one -i $QD -t $T \
      || echo "FAILED: erpc QD=$QD T=$T"
  done
done

# rc-send: QD x Threads
for QD in 1 32 256; do
  for T in 1 2 4 8 16; do
    echo "=== rc-send QD=$QD T=$T ==="
    timeout 120 mpirun -np 2 "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
      -o "$OUTDIR/rc_send_qd${QD}_t${T}.parquet" \
      --affinity-mode multinode --affinity-start 47 \
      rc-send one-to-one -i $QD -t $T \
      || echo "FAILED: rc-send QD=$QD T=$T"
  done
done

# ucx-am: QD (single-thread only)
for QD in 1 32 256; do
  echo "=== ucx-am QD=$QD ==="
  timeout 120 mpirun -np 2 "$BENCH" -d $DURATION -r $RUNS -s $MSG_SIZE \
    -o "$OUTDIR/ucx_am_qd${QD}.parquet" \
    --affinity-mode multinode --affinity-start 47 \
    ucx-am one-to-one -i $QD \
    || echo "FAILED: ucx-am QD=$QD"
done

echo "=== All one-to-one benchmarks completed ==="
