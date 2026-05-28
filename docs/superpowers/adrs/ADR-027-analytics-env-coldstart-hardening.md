# ADR-027: Analytics-env cold-start hardening (xgboost-cpu, mlflow-skinny, preflight on default env)

| Field | Value |
|---|---|
| **Date** | 2026-05-27 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The AC-1 pipeline (PRs #310–#312) shipped `preflight_action_context` on the
shared `analytics` Databricks serverless environment (wheel + 11 pip deps:
silly-kicks, accessible-space, numpy, xgboost, rapidfuzz, unidecode,
sparse-dot-topn, mlflow, mplsoccer, matplotlib, scipy). Every individual run of
the task failed with `setup_duration ≈ 305 s, execution_duration = 0 s` — the
300 s task timeout was consumed entirely by serverless **environment build**
(pip resolve + download + install) before any user code ran.

Investigation (`system.lakeflow.job_task_run_timeline` + driver logs)
established that the analytics env genuinely takes ~305 s to build cold — 5 s
over the 300 s timeout. A control run of `preflight_spadl_vaep` (same env)
reproduced the identical 308 s timeout; a `default`-env task (`preflight_idsse`,
wheel only) built in 3 s. So the bottleneck is the 11-dep install, not compute
provisioning. Two dominant costs in the build log:

1. `xgboost==3.2.0` pulls `nvidia-nccl-cu12; platform_system == "Linux"` as a
   **core** dep — a 300 MB NVIDIA GPU collective-communication library,
   useless on CPU-only serverless drivers.
2. `mlflow` (full) bundles the server-side stack (flask, gunicorn, sqlalchemy,
   alembic, docker, graphene, aiohttp, cryptography, …) — ~11 transitives the
   analytics path never uses (it only loads models + calls `MlflowClient`).

The forcing function: a brand-new task that has never once succeeded
individually, blocking AC-1 operation.

## Decision

Three changes harden the analytics-env cold start:

1. Move `preflight_action_context` from `environment_key = "analytics"` to
   `"default"` (wheel only) — its `main_preflight` imports only numpy + the
   wheel + pyspark, none of the analytics extras.
2. Replace `xgboost==3.2.0` with `xgboost-cpu==3.2.0` (same version, same
   `import xgboost` API, no `nvidia-nccl-cu12`) in the analytics env spec and
   the `[analytics]` extra.
3. Replace `mlflow>=2.19.0` with `mlflow-skinny>=2.19.0` (client-only) in the
   analytics env spec.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Raise `preflight_action_context` timeout 300 → 900 s | One-line, unblocks immediately | Masks the root cause; every cold build still wastes ~5 min; doesn't help the compute_action_context iterations or other analytics-env tasks | Treats the symptom; cold build stays bloated |
| B. Pin scipy/pandas/sklearn to cp310-compatible versions to avoid resolver backtracking | Could shave resolve time | Driver log shows pip already resolved directly to cp310 wheels (scipy 1.15.3, pandas 2.3.3, sklearn 1.7.2) — **no backtracking was happening** | No measurable cost to remove |
| C. Pre-warm the env once (long-timeout run) and rely on the cache | Restores 3 s warm setup immediately | Cache invalidates on every wheel/dep bump → recurring manual re-warm; doesn't reduce the build itself | Operational toil; not a fix |
| D. (chosen) Trim the env: CPU xgboost + skinny mlflow + preflight on default env | Cuts ~300 MB nccl + ~11 server-side transitives; preflight needs no analytics deps at all; benefits every analytics-env task and the for_each iterations | First build after deploy still pays one (smaller) cold build; CI must test the same package set prod ships | — |

## Consequences

### Positive

- Cold-start env build drops by an estimated ~90–150 s (300 MB nccl + server-side
  mlflow transitives removed), targeting ~150–220 s — back under the 300 s task
  timeout so individual analytics-env tasks run without a pre-warm.
- `preflight_action_context` runs in the `default` env (3 s setup observed for
  that env), fully decoupling the preflight from the heavy analytics build.
- Smaller env also shortens cold setup for the `compute_action_context`
  for_each iterations.
- `[analytics]` extra now installs `xgboost-cpu`, so CI exercises the exact
  package prod ships (shift-left parity).

### Negative

- `preflight_action_context` (default env) and `compute_action_context`
  (analytics env) now run on **different** env specs — a maintainer must keep in
  mind the preflight cannot import analytics-only libs. Enforced implicitly by
  the preflight code importing only numpy + wheel; if that ever changes the task
  must move back to `analytics`.
- `xgboost-cpu` and `xgboost` both provide `import xgboost`; they are mutually
  exclusive in one env. Anything that pulls full `xgboost` transitively would
  conflict (none does today; silly-kicks `[xgboost]` extra is unused).
- Deploying the env-spec change invalidates the current warm cache (spec hash
  changes), so the first run after deploy pays one cold build — a faster one.

### Neutral

- `mlflow-skinny` retains `mlflow.pyfunc` (`log_model`, `PythonModel`) and
  `mlflow.tracking` (`MlflowClient`); verified no framework-flavor modules
  (`mlflow.xgboost`, `mlflow.sklearn`) are used in analytics-env code.
- pyproject `[mlflow]` extra (training scripts) is left on full `mlflow` — those
  run on HF Jobs with their own deps and may need the server-side bits.

## Related

- **Issues / PRs:** AC-1 (#310, #311, #312) introduced the regression this fixes.
- **ADRs:** complements ADR-012 (training-to-production delivery) — the analytics
  env is the inference/scoring consumer side.
- **External references:** XGBoost project `xgboost-cpu` PyPI distribution;
  MLflow `mlflow-skinny` PyPI distribution; `xgboost==3.2.0` requires_dist
  (`nvidia-nccl-cu12; platform_system == "Linux"`).

## Notes

Evidence (individual-run `system.lakeflow.job_task_run_timeline`):

| Task | Env | setup_duration | Result |
|---|---|---|---|
| preflight_action_context | analytics (cold) | 301–306 s | TIMED_OUT |
| preflight_spadl_vaep (control) | analytics (cold) | 308 s | TIMED_OUT @ 300 s, then SUCCESS @ 305 s with 1800 s timeout |
| preflight_spadl_vaep (re-run) | analytics (warm) | 3 s | SUCCESS |
| preflight_idsse | default (cold) | 3 s | SUCCESS |

xgboost wheel footprint: `xgboost` 3.2.0 = 128 MB wheel + 300 MB nvidia-nccl-cu12;
`xgboost-cpu` 3.2.0 = 5.5 MB wheel, no nccl. mlflow wheel: full 10.1 MB vs skinny
3.1 MB, the latter dropping ~11 server-side transitives.
