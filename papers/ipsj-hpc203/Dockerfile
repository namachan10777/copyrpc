FROM texlive/texlive:latest

# Install additional packages if needed
RUN apt-get update && apt-get install -y \
    make \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download and install IPSJ style files
RUN mkdir -p /usr/local/share/texmf/tex/latex/ipsj && \
    cd /tmp && \
    wget -q https://www.ipsj.or.jp/journal/submit/tech-style-files.zip && \
    unzip -q tech-style-files.zip && \
    find . -name "*.cls" -exec cp {} /usr/local/share/texmf/tex/latex/ipsj/ \; && \
    find . -name "*.sty" -exec cp {} /usr/local/share/texmf/tex/latex/ipsj/ \; && \
    find . -name "*.bst" -exec cp {} /usr/local/share/texmf/tex/latex/ipsj/ \; && \
    rm -rf /tmp/* && \
    mktexlsr

WORKDIR /workdir

# Default command
CMD ["make"]
