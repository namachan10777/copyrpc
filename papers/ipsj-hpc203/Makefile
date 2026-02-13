DOCKER_IMAGE = ipsj-latex
DOCKER_RUN = docker run --rm -v $(PWD):/workdir $(DOCKER_IMAGE)

BENCHKV_DATA = result/benchkv/benchkv_unified.parquet
RPC_BENCH_DATA = result/rpc_bench/rpc_bench_unified.parquet
FIGURE_PDFS = figures/benchkv_throughput.pdf figures/benchkv_batch_hold.pdf figures/rpc_bench_throughput.pdf

.PHONY: all build docker-build clean watch fix-punctuation

all: build

# Replace Japanese punctuation (、→，  。→．)
fix-punctuation:
	./fix-punctuation.sh hpc203.tex

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE) .

# Generate figures from benchmark results (only when data or plot.py changes)
$(FIGURE_PDFS): plot.py $(wildcard $(BENCHKV_DATA)) $(wildcard $(RPC_BENCH_DATA))
	uv run plot.py

# Build PDF using Docker (pLaTeX + dvipdfmx)
build: fix-punctuation $(FIGURE_PDFS) docker-build
	$(DOCKER_RUN) latexmk hpc203.tex

# Watch for changes and rebuild
watch: docker-build
	$(DOCKER_RUN) latexmk -pvc hpc203.tex

# Clean auxiliary files
clean:
	$(DOCKER_RUN) latexmk -C
