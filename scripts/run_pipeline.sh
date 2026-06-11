#!/bin/bash
# Run ETL pipeline script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Starting ETL pipeline..."
echo "Time: $(date)"

# Load environment
if [ -f .env ]; then
    source .env
fi

# Run pipeline
python -m src.main

echo "Pipeline completed!"
echo "Time: $(date)"
