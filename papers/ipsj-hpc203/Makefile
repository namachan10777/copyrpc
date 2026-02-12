DOCKER_IMAGE = ipsj-latex
DOCKER_RUN = docker run --rm -v $(PWD):/workdir $(DOCKER_IMAGE)

.PHONY: all build docker-build clean watch fix-punctuation

all: build

# Replace Japanese punctuation (、→，  。→．)
fix-punctuation:
	./fix-punctuation.sh hpc203.tex

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE) .

# Build PDF using Docker (pLaTeX + dvipdfmx)
build: fix-punctuation docker-build
	$(DOCKER_RUN) latexmk hpc203.tex

# Watch for changes and rebuild
watch: docker-build
	$(DOCKER_RUN) latexmk -pvc hpc203.tex

# Clean auxiliary files
clean:
	$(DOCKER_RUN) latexmk -C
