#!/bin/bash
#PBS -b 1
#PBS -q gen_S
#PBS -A NBBG
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=02:00:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/jobs/pegasus/logs/%r.log
#PBS -j o

set -ux

export LOGDIR="$WORKDIR/jobs/pegasus/logs/$(echo $PBS_JOBID | sed -E 's/^[^:]*:([0-9]+)\.nqsv$/\1/')"
mkdir -p "$LOGDIR"

cd "$WORKDIR"

# Build
cargo build --release --bin cluster_bench --features "bench-bin,rtrb,omango,crossbeam"

BENCH_BIN="$WORKDIR/target/release/cluster_bench"
OUTDIR="$LOGDIR/results"
mkdir -p "$OUTDIR"

DURATION=10
RUNS=5
WARMUP=2

for QD in 1 32 256; do
    CAP=$((QD * 4))
    if [ $CAP -lt 4 ]; then CAP=4; fi

    for N in 2 4 8 16; do
        echo "=== QD=$QD N=$N (capacity=$CAP) ==="
        timeout 120 "$BENCH_BIN" -n $N -d $DURATION -r $RUNS -w $WARMUP \
            -i $QD -c $CAP \
            -o "$OUTDIR/qd${QD}_n${N}.parquet" \
            || echo "FAILED: QD=$QD N=$N"
    done
done

echo "=== All benchmarks completed ==="
