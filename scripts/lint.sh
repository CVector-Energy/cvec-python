#!/bin/bash
# This script runs black and mypy on the specified file or directory.

# Exit immediately if a command exits with a non-zero status.
set -e

TARGET=${1:-.}

echo "Running black..."
poetry run black "$TARGET"

echo "Running mypy..."
poetry run mypy --strict "$TARGET"

echo "Linting complete."
