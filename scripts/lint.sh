#!/bin/bash
# This script runs ruff and mypy on the specified file or directory.

# Exit immediately if a command exits with a non-zero status.
set -e

TARGET=${1:-.}

poetry run ruff check --fix "$TARGET"
poetry run ruff format "$TARGET"
poetry run mypy --strict "$TARGET"
