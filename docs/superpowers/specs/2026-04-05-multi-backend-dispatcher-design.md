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

A `queue.Queue`-backed pool of backends. Each `train()` call blocks on `queue.get()` to acquire the next idle backend, runs training, then `queue.put()` to release it. The faster machine (RTX 5070 Ti, ~28 min/5 epochs) naturally handles more candidates than the slower one (DGX Spark GB10, ~77 min/5 epochs). No round-robin — pure availability-based dispatch.

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

## OpenEvolve integration

Unchanged — `parallel_evaluations: 2` is a config value OpenEvolve reads to spawn concurrent evaluator threads.

## Performance characteristics

E2E testing (2026-04-05) shows the DGX Spark GB10 is ~3x slower than the RTX 5070 Ti:

| Backend | 1 Epoch (additive seed) | 5 Epochs (estimated) |
|---------|------------------------|---------------------|
| RTX 5070 Ti (local_cuda) | 462s (~8 min) | ~28 min |
| DGX Spark GB10 (remote_ssh) | 923s (~15 min) | ~77 min |

The `timeout_seconds` is set to 6000 (100 min) to cover the slowest backend with margin.

With `parallel_evaluations: 2`:
- Seed eval: 4 seeds dispatched 2-at-a-time — ~77 min (bounded by GB10 speed)
- Evolution: 2 candidates training simultaneously — ~1.8 evals/hr combined
- 8 hours total: ~4 seed evals + ~14 evolution iterations = **~18 candidates evaluated**

## Files changed

| File | Change |
|------|--------|
| `src/evolve/backends/pool.py` | **New** — BackendPool class (queue-based dispatcher) |
| `src/evolve/backends/__init__.py` | Registry-based factory, comma-separated type parsing |
| `src/evolve/backends/base.py` | Added shared `fail_metrics()` sentinel |
| `src/evolve/backends/remote_ssh.py` | JSON config, HF cache warming, device passthrough, atexit process cleanup |
| `src/evolve/backends/local_cuda.py` | CUDA availability caching |
| `src/evolve/backends/docker.py` | LSP fix — returns fail_metrics instead of raising |
| `src/evolve/backends/hf_jobs.py` | LSP fix — returns fail_metrics instead of raising |
| `src/evolve/evaluator.py` | AST-based config loading (security fix), `CandidateConfig` Pydantic model |
| `src/evolve/remote_worker.py` | JSON candidate config (replaces exec_module) |
| `src/evolve/runner.py` | Parallel seed evaluation, parallel_evaluations validation |
| `src/evolve/targets/scoutgpt/evaluator.py` | Dataset caching with threading.Lock, GPU memory cleanup |
| `src/evolve/targets/scoutgpt/config.yaml` | Multi-backend config, timeout_seconds: 6000 |
| `src/tests/test_backend_pool.py` | **New** — pool concurrency tests |
| `scripts/run_evolve_overnight.sh` | **New** — dual-GPU launch script |
