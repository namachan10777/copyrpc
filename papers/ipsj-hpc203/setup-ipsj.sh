#!/bin/bash
set -e

# Download IPSJ style files for technical reports
# See: https://www.ipsj.or.jp/journal/submit/style.html

IPSJ_URL="https://www.ipsj.or.jp/journal/submit/tech-style-files.zip"
TEMP_DIR=$(mktemp -d)

echo "Downloading IPSJ style files..."
wget -q -O "$TEMP_DIR/ipsj-style.zip" "$IPSJ_URL"

echo "Extracting style files..."
unzip -q -o "$TEMP_DIR/ipsj-style.zip" -d "$TEMP_DIR"

# Copy necessary files
echo "Installing style files..."
find "$TEMP_DIR" -name "*.cls" -exec cp {} . \;
find "$TEMP_DIR" -name "*.sty" -exec cp {} . \;
find "$TEMP_DIR" -name "*.bst" -exec cp {} . \;

# Cleanup
rm -rf "$TEMP_DIR"

echo "Done! IPSJ style files have been installed."
echo "You can now run 'make build' to compile your document."
