DOCKER_IMAGE = ipsj-latex
DOCKER_RUN = docker run --rm -v $(PWD):/workdir $(DOCKER_IMAGE)

.PHONY: all build docker-build clean watch

all: build

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE) .

# Build PDF using Docker
build: docker-build
	$(DOCKER_RUN) latexmk -pdf main.tex

# Watch for changes and rebuild
watch: docker-build
	$(DOCKER_RUN) latexmk -pdf -pvc main.tex

# Clean auxiliary files
clean:
	$(DOCKER_RUN) latexmk -C
