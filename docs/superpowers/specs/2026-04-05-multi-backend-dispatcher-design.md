# Multi-Backend Dispatcher — Design Spec

**Date:** 2026-04-05
**Branch:** `feature/multi-backend-dispatcher`
**Goal:** Run evolution evaluations on multiple GPUs simultaneously (RTX 5070 Ti + DGX Spark) to maximize overnight search throughput.

## Architecture

OpenEvolve already supports `parallel_evaluations > 1`, calling the evaluator concurrently from multiple threads. The dispatcher exploits this: `BackendPool` wraps N backends behind the `ComputeBackend` protocol and distributes concurrent `train()` calls across them using a thread-safe queue.

```
OpenEvolve (parallel_evaluations=2)
    |              |
    v              v
  pool.train()   pool.train()
    |              |
    v              v
 RTX 5070 Ti    DGX Spark
 (local_cuda)   (remote_ssh)
```

### Submit-next-available strategy

A `threading.Semaphore`-guarded pool of backends. Each `train()` call acquires a backend from the pool, runs training, then releases it. The faster machine (RTX 5070 Ti, ~29 min) naturally handles more candidates than the slower one (DGX Spark, ~67 min). No round-robin — pure availability-based dispatch.

## Components

### `src/evolve/backends/pool.py` (~60 lines)

```python
class BackendPool:
    """ComputeBackend that dispatches to multiple backends."""
    
    def __init__(self, backends: list[ComputeBackend]) -> None
    def train(self, candidate_config, target, epochs, seed) -> dict[str, float]
    def available(self) -> bool
```

Internals:
- `_backends: list[ComputeBackend]` — the wrapped backends
- `_available: queue.Queue[ComputeBackend]` — thread-safe FIFO of idle backends
- `train()` blocks on `_available.get()`, runs `backend.train()`, then `_available.put()` to release
- `available()` returns `True` if any backend is available
- No ThreadPoolExecutor needed — OpenEvolve's own threads provide the concurrency; we just need the queue for backend assignment

### `src/evolve/backends/__init__.py` changes

`create_backend()` detects comma-separated `config.type` (e.g., `"local_cuda,remote_ssh"`):
- Single type → existing behavior, returns one backend
- Multiple types → creates each backend, wraps in `BackendPool`

### `src/evolve/targets/scoutgpt/config.yaml` changes

```yaml
backend:
  type: local_cuda,remote_ssh
  device: "cuda:0"
  ssh_host: "192.168.68.73"
  ssh_user: "karsten"
  ssh_remote_dir: "/home/karsten/Development"
  ssh_python_path: "/home/karsten/Development/evolve-env/bin/python"

evolution:
  parallel_evaluations: 2
```

### `src/tests/test_backend_pool.py`

- Test 1: two mock backends, two concurrent `train()` calls — both backends used
- Test 2: one backend fails, pool returns fail metrics without crashing
- Test 3: `available()` returns True when any backend is available
- Test 4: single-backend pool behaves identically to raw backend

## What does NOT change

- `evaluator.py` — unchanged, still calls `backend.train()`
- `runner.py` — unchanged, `create_backend()` handles the pool transparently
- `remote_ssh.py` — unchanged
- `local_cuda.py` — unchanged
- OpenEvolve integration — unchanged, `parallel_evaluations: 2` is a config value

## Overnight run config

With `parallel_evaluations: 2`:
- Seed eval: 4 seeds evaluated one-at-a-time (pool picks fastest available backend) — ~2 hr
- Evolution: 2 candidates training simultaneously — ~3.0 evals/hr combined
- 8 hours total: ~4 seed evals + ~18 evolution iterations = **~22 candidates evaluated**

## Files changed

| File | Change |
|------|--------|
| `src/evolve/backends/pool.py` | **New** — BackendPool class |
| `src/evolve/backends/__init__.py` | Parse comma-separated types, create pool |
| `src/evolve/targets/scoutgpt/config.yaml` | Add SSH fields, parallel_evaluations: 2 |
| `src/tests/test_backend_pool.py` | **New** — pool tests |
| `scripts/run_evolve_overnight.sh` | **New** — launch script |
