FROM texlive/texlive:latest

# Install additional packages if needed
RUN apt-get update && apt-get install -y \
    make \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workdir

# Default command
CMD ["make"]
