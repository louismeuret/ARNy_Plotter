#!/bin/sh

# Ensure we're in the script's directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Move one level up from the script's directory
cd "$SCRIPT_DIR/.."

# Use python3 if available, fallback to python
PYTHON_BIN="$(command -v python3 || command -v python)"

if [ -z "$PYTHON_BIN" ]; then
    echo "Error: Python is not installed." >&2
    exit 1
fi

# Run the install script
"$PYTHON_BIN" install_script.py --repo https://github.com/louismeuret/Plot_RNA2 --install-path .
