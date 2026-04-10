# Evolve Error Feedback Artifacts — Design Spec

**Date:** 2026-04-09
**Branch:** `feat/evolve-error-feedback-artifacts`
**Status:** Approved

## Problem

When an L2 code mutation crashes or produces a runtime error, the LLM sees only `combined_score=0.0, error=1.0` with no explanation. It has no traceback, no shape mismatch details, no indication of what went wrong. This causes the LLM to repeat the same mistakes across iterations, wasting GPU time on avoidable failures.

## Solution

Wire error tracebacks through OpenEvolve's existing `EvaluationResult` artifact system so the LLM sees *why* a candidate failed in its next prompt.

OpenEvolve already has the plumbing:
- `EvaluationResult(metrics, artifacts)` — side-channel for text/binary data
- `include_artifacts: True` by default in config
- `PromptSampler._render_artifacts()` — injects artifact text into the LLM prompt (up to 20KB)
- `_process_evaluation_result()` — handles both `dict` and `EvaluationResult` returns

We need to feed error text into this pipeline from our evaluator and backends.

## Architecture

```
LLM generates candidate
  → OpenEvolve calls evaluate(program_path)
    → EvolveEvaluator loads program, validates, delegates to backend
      → Backend calls train_and_evaluate() (local or remote)
        → On error: capture traceback, return in metrics["_error_text"]
      ← Backend returns dict with optional _error_text
    ← EvolveEvaluator strips _error_text, wraps in EvaluationResult(metrics, artifacts={"error": text})
  ← OpenEvolve stores artifacts, renders into next LLM prompt
```

## Changes

### 1. `src/evolve/targets/scoutgpt/evaluator.py`

**Current:** The `except (torch.cuda.OutOfMemoryError, RuntimeError)` block (line 277) returns `{"combined_score": 0.0, "error": 1.0}` with no error detail.

**Change:** Capture `traceback.format_exc()` and include it as `"_error_text"` in the returned metrics dict. This key is a convention between the target evaluator and `EvolveEvaluator` — it never reaches OpenEvolve's metrics storage.

### 2. `src/evolve/remote_worker.py`

**Current:** If `train_and_evaluate` raises an unhandled exception, the process exits non-zero. The `RemoteSSHBackend` sees the exit code and returns `fail_metrics()` — no error detail propagated.

**Change:** Wrap `train_and_evaluate` in a try/except. On failure, print a JSON dict with `fail_metrics()` + `"_error_text": traceback.format_exc()` to stdout (normal JSON output channel) and exit 0 so the SSH backend parses it normally. The error text flows through the same path as successful metrics.

### 3. `src/evolve/evaluator.py`

**Current:** `EvolveEvaluator.evaluate()` returns `dict[str, Any]`. On failure (load error, search space rejection, validation rejection, backend error), returns `fail_metrics()` with a `reject_reason` string but no traceback.

**Change:**
- Return `EvaluationResult` instead of `dict`
- At each failure point, capture the relevant error text (traceback for exceptions, rejection reason for validation failures)
- Strip `_error_text` from backend metrics if present
- Attach error text as `artifacts={"error": error_text}` on the `EvaluationResult`
- On success, return `EvaluationResult.from_dict(metrics)` (no artifacts)

### What does NOT change

- **`src/evolve/backends/local_cuda.py`** — passes through whatever dict `train_and_evaluate` returns
- **`src/evolve/backends/remote_ssh.py`** — parses JSON from stdout, returns as dict. The `_error_text` key passes through transparently
- **`src/evolve/backends/base.py`** — `fail_metrics()` unchanged
- **`src/evolve/backends/pool.py`** — passes through backend return values
- **OpenEvolve** — no changes needed, uses existing artifact pipeline

## Error Text Convention

The `_error_text` key in metrics dicts is an internal convention:
- **Set by:** target evaluator (`scoutgpt/evaluator.py`) and remote worker on caught exceptions
- **Consumed by:** `EvolveEvaluator.evaluate()` which strips it from metrics and converts to an artifact
- **Never reaches:** OpenEvolve's metrics storage or MAP-Elites grid (stripped before return)
- **Prefix underscore** signals "internal, not a real metric"

## What the LLM sees

On the next iteration after a failure, the LLM prompt will include an artifacts section like:

```
## Evaluation Artifacts

### error
```
Traceback (most recent call last):
  File "evaluator.py", line 228, in train_and_evaluate
    _apply_program(model, program_path)
  File "evaluator.py", line 137, in _apply_program
    layers = layers_fn(hidden_dim)
TypeError: custom_layers() missing 1 required positional argument: 'hidden_dim'
```
```

This gives the LLM concrete information to fix the issue rather than guessing.

## Testing

- Unit test: `EvolveEvaluator.evaluate()` returns `EvaluationResult` with error artifact when backend returns `_error_text`
- Unit test: `EvolveEvaluator.evaluate()` returns `EvaluationResult` without artifacts on success
- Unit test: `_error_text` is stripped from metrics before `combined_score` computation
- Unit test: remote worker outputs valid JSON with `_error_text` on exception
