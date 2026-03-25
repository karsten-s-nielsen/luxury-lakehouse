# D24: Numba Evaluation for Pitch Control

**Date:** 2026-03-25
**Status:** Evaluated — Numba ≥2x faster, recommended as third dispatch tier

## Benchmark Results

| Kernel | Median | Notes |
|--------|--------|-------|
| NumPy full pipeline (`compute_pitch_control_at_points`, 22 targets) | 361 µs | Includes coord conversion + 2× TTI + 2× influence |
| Numba `tti_numba` warm (11 players × 22 targets) | 1.5 µs | Kernel-level only, post-JIT |

**Kernel-level speedup:** ~240× for TTI computation.

## Parity

Both `tti_numba` and `influence_numba` produce results identical to their NumPy counterparts at `atol=1e-10`.

## Decision

**Adopt Numba as third dispatch tier.** The 240× kernel speedup far exceeds the 2× threshold. Numba is particularly valuable for:

1. **Short-lived processes** where JAX compile overhead (~seconds) dominates. Numba's cached compilation is ~50ms cold, <1µs warm.
2. **Databricks serverless executors** (if pitch control is ever moved back) where JAX cannot be installed but Numba's LLVM backend is lightweight.
3. **CPU-only environments** where JAX's XLA backend provides no GPU advantage.

### Dispatch hierarchy

```
NumPy (always available) → Numba (if installed) → JAX (if installed, GPU preferred)
```

### Next steps

- Integrate `tti_numba` and `influence_numba` into `pitch_control.py` dispatch logic (future task)
- Move `numba` from `[dependency-groups] dev` to `[project.optional-dependencies] numba` extra
- Add `_USE_NUMBA` flag matching the existing `_USE_JAX` pattern

### Caveats

- Benchmark compares the TTI kernel in isolation, not the full pipeline. End-to-end speedup will be lower since coordinate conversion and influence summation are also significant.
- Cold-start measurement via `pytest-benchmark` is unreliable (only first iteration is truly cold). Use `timeit` for single-invocation cold-start measurement if needed.
