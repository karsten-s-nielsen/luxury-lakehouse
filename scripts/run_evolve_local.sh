#!/usr/bin/env bash
# Overnight evolution run on RTX 5070 Ti (local CUDA)
# Expected: ~2hr seed eval + ~6hr evolution = ~12 LLM iterations
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Evolve Engine: Local RTX 5070 Ti ==="
echo "Started: $(date)"
echo "Target: scoutgpt, Backend: local_cuda, Iterations: 15"
echo ""

uv run evolve --target scoutgpt --iterations 15

echo ""
echo "Completed: $(date)"
