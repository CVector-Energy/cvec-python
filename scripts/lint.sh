#!/bin/bash
# This script runs ruff and mypy on the specified file or directory.

# Exit immediately if a command exits with a non-zero status.
set -e

TARGET=${1:-.}

echo -e "poetry run ruff check --fix" "$@"
poetry run ruff check --fix "$@"

echo -e "\npoetry run ruff format" "$@"
poetry run ruff format "$@"

echo -e "\nrun mypy --strict" "$@"
poetry run mypy --strict "$@"

echo -e "\npoetry run pytest"
poetry run pytest
