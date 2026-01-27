FROM texlive/texlive:latest

# Install additional packages if needed
RUN apt-get update && apt-get install -y \
    make \
    wget \
    unzip \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# Download and install IPSJ style files (UTF8 version)
RUN mkdir -p /usr/local/texlive/texmf-local/tex/latex/ipsj && \
    cd /tmp && \
    wget -q https://www.ipsj.or.jp/journal/submit/9faeag0000001nsz-att/ipsj_v4-1.zip && \
    unzip -q ipsj_v4-1.zip && \
    cp ipsj_v4-1/UTF8/*.cls /usr/local/texlive/texmf-local/tex/latex/ipsj/ && \
    cp ipsj_v4-1/UTF8/*.sty /usr/local/texlive/texmf-local/tex/latex/ipsj/ && \
    cp ipsj_v4-1/UTF8/*.bst /usr/local/texlive/texmf-local/tex/latex/ipsj/ && \
    rm -rf /tmp/* && \
    mktexlsr

# Configure Japanese fonts
RUN kanji-config-updmap-sys noto

WORKDIR /workdir

# Default command
CMD ["make"]
