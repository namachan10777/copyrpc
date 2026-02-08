#!/bin/bash
#PBS -b 1
#PBS -q gen_S
#PBS -A NBBG
#PBS -v WORKDIR=/work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc
#PBS -l elapstim_req=00:10:00
#PBS -o /work/NBB/mnakano/ghq/github.com/namachan10777/copyrpc/jobs/pegasus/logs/%r.log
#PBS -j o

set -eux

cd "$WORKDIR"

echo "=== cargo test --package pmem ==="
cargo test --package pmem 2>&1

echo "=== cargo clippy --package pmem ==="
cargo clippy --package pmem 2>&1

echo "=== All pmem tests passed ==="
