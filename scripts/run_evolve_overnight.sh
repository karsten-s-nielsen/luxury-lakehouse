#!/usr/bin/env bash
# Overnight evolution run — RTX 5070 Ti + DGX Spark in parallel
# Config: local_cuda,remote_ssh with parallel_evaluations=2
# Expected: ~22 candidate evaluations over 8 hours
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Evolve Engine: Dual-GPU Overnight Run ==="
echo "Started: $(date)"
echo "Backends: RTX 5070 Ti (local_cuda) + DGX Spark (remote_ssh)"
echo "Iterations: ${1:-20}"
echo ""

uv run evolve --target scoutgpt --iterations "${1:-20}"

echo ""
echo "Completed: $(date)"
