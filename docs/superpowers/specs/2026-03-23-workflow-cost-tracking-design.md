# Workflow Cost Tracking — Design Spec

**Date:** 2026-03-23
**Status:** Approved
**Branch:** `feature/workflow-cost-tracking`
**Parent Spec:** `D:/Development/workflow-cards/2026-03-21-workflow-card-refactoring-design.md` (Section 11)
**Author:** Karsten + Claude

---

## 1. Problem Statement

The workflow framework (Tasks 1-12, merged in PR #54) provides lifecycle hooks and workflow cards with static cost estimates, but no actual cost data flows through the system. Users cannot see what a workflow run actually cost, whether a job is currently running, or how estimates compare to actuals.

### Goals

1. **Cold tier** — dbt model aggregating actual Databricks billing from system tables
2. **Warm/hot tier** — immediate cost visibility for running and completed jobs on both Databricks and HF Jobs
3. **Wheel deployment automation** — close the CI → Databricks deployment gap
4. **E2E verification** — run real jobs on both runtimes, verify cost data flows, record baselines

### Non-Goals

- Taipy Workflows UI page (Tasks 13-14, separate cycle)
- Cost UI with visual indicators and 2-min timer (Task 15D, depends on 13-14)
- Synced table for `workflow_cost_live` (created when Taipy page is built)

---

## 2. Architecture

Three-tier cost model from the parent spec, implemented across two storage backends:

| Tier | Name | Storage | Writer | Freshness |
|------|------|---------|--------|-----------|
| Cold | Actual | `fct_workflow_costs` (Delta → Lakebase) | dbt daily build | ~24h lag |
| Warm | Estimated (complete) | `workflow_cost_live` (Delta) / `_workflow_cost.json` (HF Hub) | `CostEstimateHook` / `HFJobsCostRecorder` | Immediate |
| Hot | Live (running) | Same as warm | Same writers | Real-time (poll) |
| — | Projected | Workflow card YAML | Static | Pre-computed |

**Key constraint:** `src/workflows/` has zero Spark imports. Cost hooks with Spark dependencies live in `src/ingestion/`. Cost recorders without Spark dependencies live in `src/analytics/` (in the wheel).

---

## 3. Component Design

### 3A. dbt `fct_workflow_costs` (Cold Tier)

**File:** `dbt_project/models/marts/fct_workflow_costs.sql`

Reads `system.billing.usage` joined with `system.billing.list_prices` on `sku_name` with price validity window. Attributes per-task cost proportionally using `execution_duration_seconds` from `system.lakeflow.job_task_run_timeline`. 90-day rolling window.

Output columns:
- `task_key` — maps to workflow card `execution.inference.entry_point`
- `usage_date`
- `job_run_id`
- `attributed_dbu` — proportional DBU for this task within the job run
- `attributed_cost_usd` — proportional dollar cost

**Contract:** Enforced in `_marts__models.yml` with explicit `data_type` on every column.

**Post-hook cleanup** (runs after each dbt build of this model):

```sql
-- Primary: delete non-running warm rows covered by cold tier.
-- COALESCE sentinel: if fct_workflow_costs is empty (first build), threshold
-- becomes 1970-01-02 — no legitimate workflow has ended_at in 1970, so the
-- DELETE matches zero rows and warm tier is preserved until cold catches up.
DELETE FROM {catalog}.{schema}.workflow_cost_live
WHERE state != 'RUNNING'
  AND ended_at IS NOT NULL
  AND ended_at < (
      SELECT COALESCE(MAX(usage_date), DATE '1970-01-01') + INTERVAL 1 DAY
      FROM {catalog}.{schema}.fct_workflow_costs
  );

-- Secondary: orphaned RUNNING rows older than 24 hours.
-- If a job has been "running" for > 24h, the hook clearly failed to update it.
DELETE FROM {catalog}.{schema}.workflow_cost_live
WHERE state = 'RUNNING'
  AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS;
```

**Access requirement:** `SELECT` on `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.job_task_run_timeline` for the ingestion service principal. See Section 9 (Prerequisites) for GRANT statements.

### 3B. `CostEstimateHook` (Databricks Warm/Hot)

**File:** `src/ingestion/cost_hook.py`

```python
DATABRICKS_SERVERLESS_RATE = float(
    os.environ.get("DATABRICKS_SERVERLESS_RATE_USD", "0.07")
)
```

Rate is configurable via environment variable, defaults to current Databricks serverless rate. Overridable in Terraform job env spec or cluster env vars.

**Class:** `CostEstimateHook` implementing the `LifecycleHook` protocol from `src/workflows/hooks.py`.

| Event | Row State | `estimated_cost_usd` |
|-------|-----------|---------------------|
| `on_start` | RUNNING | 0.00 |
| `on_complete` | COMPLETED | `duration_seconds × (rate / 3600)` |
| `on_skip` | SKIPPED | 0.00 |
| `on_error` | FAILED | `partial_duration × (rate / 3600)` |

**Duration computation:** `on_complete` and `on_error` compute elapsed time as `int((datetime.now(timezone.utc) - ctx.started_at).total_seconds())`. This uses `WorkflowContext.started_at` (set at context creation before the pipeline function runs) and the current UTC time at hook dispatch. Wall-clock time is the appropriate measure since Databricks billing is also wall-clock based.

**MERGE implementation:** `CostEstimateHook` writes its own MERGE statement (not reusing `merge_delta_table()` from `src/ingestion/utils.py`). The cost MERGE needs partial-column updates (updating `state`, `ended_at`, `duration_seconds`, `estimated_cost_usd`, `updated_at` while preserving `started_at`, `workflow_id`, etc. from the initial insert), which differs from the full-row upsert that `merge_delta_table()` implements.

**MERGE key:** `run_id` (UUID from `WorkflowContext`). Insert on start, update on completion. Single row per run — no separate start/end rows.

**Databricks metadata from Spark conf:**
- `spark.databricks.job.runId` → `job_run_id`
- `spark.databricks.task.key` → `task_key`

Both are `None` in local/notebook contexts (handled gracefully — hook writes the row without these fields).

**Constructor:**
```python
def __init__(
    self,
    spark: SparkSession,
    catalog: str,
    schema: str,
    rate_usd_per_hour: float = DATABRICKS_SERVERLESS_RATE,
    runtime: str = "databricks",
) -> None:
```

### 3C. `HFJobsCostRecorder` (HF Jobs Warm/Hot)

**File:** `src/analytics/cost.py` (in wheel, no Spark dependency)

**Design note:** `HFJobsCostRecorder` is intentionally a standalone class, **not** a `LifecycleHook` implementor. HF Jobs scripts are standalone PEP 723 runners that execute outside the workflow runner and lack a `WorkflowContext`. The recorder mirrors the `CostEstimateHook` lifecycle semantically (start/complete/fail/skip) but uses a direct API (`start()`, `complete()`) instead of the hook protocol's `on_start(ctx)`, `on_complete(ctx, row_count)`. If HF scripts are ever integrated into the workflow runner, an adapter from `LifecycleHook` to `HFJobsCostRecorder` would be trivial.

**Rate constants** are centralized in this module:

```python
HF_RATE_CPU_BASIC: float = 0.01    # $/hr
HF_RATE_A10G_SMALL: float = 1.00   # $/hr
HF_RATE_A10G_LARGE: float = 1.50   # $/hr
```

Scripts import the appropriate rate constant rather than hardcoding.

**Constructor:**
```python
def __init__(
    self,
    workflow_id: str,
    phase: str,
    rate_usd_per_hour: float,
    repo_id: str,
    repo_type: str = "dataset",
) -> None:
```

**Lifecycle:**

| Method | Action | `_workflow_cost.json` state |
|--------|--------|---------------------------|
| `start()` | Upload JSON with RUNNING state, `started_at`, `rate` | `{"state": "RUNNING", "started_at": "...", "rate_usd_per_hour": 0.01, ...}` |
| `complete(metadata, row_count)` | Update JSON to COMPLETED, return **new** enriched metadata dict | `{"state": "COMPLETED", "estimated_cost_usd": 0.04, ...}` |
| `fail(error)` | Update JSON to FAILED with partial cost | `{"state": "FAILED", ...}` |
| `skip(reason)` | Update JSON to SKIPPED, zero cost | `{"state": "SKIPPED", ...}` |

**Immutability:** `complete()` returns a new dict (`{**metadata, **cost_fields}`) rather than mutating the caller's metadata dict. This follows the project convention of immutable data (cf. `WorkflowContext` is frozen, `ExpectedThreatParams` is frozen).

**Error handling:** All HF Hub upload methods (`start()`, `complete()`, `fail()`, `skip()`) catch upload failures gracefully — log a warning and continue. Cost tracking is observability, not business logic; a failed cost upload must never prevent compute from running. Retry with exponential backoff (max 3 retries) on transient errors (429, 5xx) per CLAUDE.md, but ultimate failure is swallowed.

**`hf_job_id` capture:** The recorder reads `os.environ.get("HF_JOB_ID")` if available (set by HF Jobs runtime) and includes it in `_workflow_cost.json`. This allows future Taipy aggregation across the `hf_job_id` column in `workflow_cost_live` when HF cost data is consolidated.

**Hot tier:** Taipy reads `_workflow_cost.json` from HF Hub repos. For RUNNING entries, it computes `(now - started_at) × rate` for live cost display.

**Warm tier:** On completion, the JSON transitions to COMPLETED with final `estimated_cost_usd`. Taipy reads this as the cost until cold tier data (if applicable) supersedes it.

**`complete()` returns the enriched metadata dict** with injected fields:
- `elapsed_seconds`
- `rate_usd_per_hour`
- `estimated_cost_usd`
- `workflow_id`
- `workflow_phase`

This metadata is then written to the script's existing `metadata.json` artifact alongside domain-specific fields.

### 3D. `workflow_cost_live` Delta Table (DDL)

**File:** `scripts/create_cost_table.sql`

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.workflow_cost_live (
    workflow_id        STRING        NOT NULL,
    phase              STRING        NOT NULL,
    run_id             STRING        NOT NULL,
    runtime            STRING        NOT NULL,
    job_run_id         BIGINT,
    task_key           STRING,
    hf_job_id          STRING,
    state              STRING        NOT NULL,
    started_at         TIMESTAMP     NOT NULL,
    ended_at           TIMESTAMP,
    duration_seconds   INT,
    row_count          INT,
    rate_usd_per_hour  DECIMAL(10,6),
    estimated_cost_usd DECIMAL(10,4),
    cost_source        STRING        NOT NULL,
    updated_at         TIMESTAMP     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
-- No liquid clustering: table is bounded at <100 rows at any time (active runs +
-- recent completions before daily cleanup sweep). Sequential scan is faster than
-- index maintenance at this scale.
```

Run once via Databricks SQL or notebook. Not managed by dbt (written by Spark, not dbt).

**Column notes:**
- `rate_usd_per_hour` uses `DECIMAL(10,6)` for sub-cent rate granularity (HF cpu-basic is $0.01/hr; future fractional rates won't truncate).
- `estimated_cost_usd` uses `DECIMAL(10,4)` — sub-cent cost precision is sufficient for display.
- `duration_seconds` uses `INT` — sufficient for 24,855 days. Orphaned RUNNING rows are swept at 24h, so overflow is impossible.

### 3E. Runner `on_skip` Dispatch

**File:** `src/workflows/runner.py`

The current runner catches all exceptions via `on_error`. `WorkflowSkippedError` (raised when a pipeline's skip guard determines all items are already processed) should dispatch `on_skip` instead, so the `CostEstimateHook` writes a SKIPPED row rather than a FAILED row.

**Change:** In `run_workflow()`, catch `WorkflowSkippedError` before the generic `Exception` handler:

```python
try:
    result = func(*args, **kwargs)
    _dispatch(hooks, "on_complete", ctx, row_count=result)
except WorkflowSkippedError as exc:
    _dispatch(hooks, "on_skip", ctx, reason=str(exc))
    # Skip is not a failure — do not re-raise. Pipeline exits 0.
except Exception as exc:
    _dispatch(hooks, "on_error", ctx, error=exc)
    raise
```

**Behavior change:** After `on_skip`, the runner does **not** re-raise. A skip means "nothing to do" — the Databricks task should exit 0 (success). This matches existing pipeline behavior where skip guards return early before any work is done.

**Note:** Pipelines must explicitly `raise WorkflowSkippedError("reason")` for this to fire. Pipelines that return early without raising (current pattern in some pipelines) will trigger `on_complete` with `row_count=None` — which is correct behavior (the pipeline completed successfully, it just had nothing to write).

---

## 4. Hook Registration (12 Databricks Pipelines)

Each pipeline's `main()` registers the hook after `get_spark_session()`:

```python
from ingestion.cost_hook import CostEstimateHook
from workflows import register_hook

hook = CostEstimateHook(spark, catalog, schema)
register_hook(hook)
```

**12 files to modify:**
- `spadl_vaep.py`, `expected_threat.py`, `xg_model.py`, `defcon_lite.py`
- `pitch_control_batch.py`, `off_ball_xt.py`, `line_breaking.py`
- `elastic_sync.py`, `entity_resolution.py`, `pausa.py`
- `player_embeddings.py`, `model_validation.py`

The default rate (`DATABRICKS_SERVERLESS_RATE`) applies to all — no per-pipeline rate overrides needed since all run on serverless at the same rate.

---

## 5. HF Jobs Script Standardization (7 Compute Scripts)

Each compute script gets `HFJobsCostRecorder` lifecycle:

```python
from analytics.cost import HFJobsCostRecorder, HF_RATE_CPU_BASIC

recorder = HFJobsCostRecorder(
    workflow_id="wf-xt-grids",
    phase="grid_computation",
    rate_usd_per_hour=HF_RATE_CPU_BASIC,
    repo_id="luxury-lakehouse/xt-grid-values",
)
recorder.start()

# ... existing compute work ...

metadata = recorder.complete(metadata, row_count=len(df))
api.upload_file(json.dumps(metadata), path_in_repo="metadata.json", ...)
```

**7 scripts to modify:**
- `compute_xt_grid_hf.py` (`HF_RATE_CPU_BASIC`)
- `compute_epv_transition_hf.py` (`HF_RATE_CPU_BASIC`)
- `compute_obso_hf.py` (`HF_RATE_A10G_SMALL`)
- `compute_space_creation_hf.py` (`HF_RATE_A10G_SMALL`)
- `train_vaep_model_hf.py` (`HF_RATE_CPU_BASIC`)
- `train_xg_model_hf.py` (`HF_RATE_CPU_BASIC`)
- `train_xg_v2_hf.py` (`HF_RATE_A10G_SMALL`)

**Elapsed time standardization:** 5 of 7 scripts currently lack elapsed time tracking. The `HFJobsCostRecorder` handles this internally (records `started_at` on `start()`, computes duration on `complete()`), so no manual `time.time()` tracking is needed.

**Not modified:** `publish_xg_shots_hf.py`, `publish_freeze_frame_hf.py` (data publishing, not compute workflows).

---

## 6. Wheel Deploy Script

**File:** `scripts/deploy_wheel.py`

Downloads the latest `.whl` from `luxury-lakehouse/build-artifacts` on HF Hub and uploads to the UC Volume at `/Volumes/{catalog}/bronze/libs/`. This path matches the Terraform-managed `wheel_path` variable (`${module.catalog.libs_volume_path}/luxury_lakehouse-0.1.0-py3-none-any.whl`), where the `libs` volume is defined in `terraform/modules/catalog/main.tf` under the `bronze` schema.

**Note:** This is a standalone script under `scripts/`, not a registered entry point — consistent with `scripts/deploy_taipy.py`.

**Auth:** HF token from env/cache, Databricks from `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.

**Usage:**
```bash
uv run python scripts/deploy_wheel.py                    # defaults: soccer_analytics catalog
uv run python scripts/deploy_wheel.py --catalog prod     # override catalog
uv run python scripts/deploy_wheel.py --dry-run          # show what would happen
```

**Dependencies:** `huggingface_hub` + `databricks-sdk` (both already in dev extras).

---

## 7. Testing

### Unit Tests

**`src/tests/test_cost_hook.py`** — `CostEstimateHook`:
- Test all 4 state transitions (start → complete, start → skip, start → error, start → complete with row_count)
- Mock Spark session and DeltaTable MERGE
- Verify Spark conf reads for `job_run_id` and `task_key`
- Verify graceful handling when Spark conf values are missing (local/notebook)
- Verify `DATABRICKS_SERVERLESS_RATE_USD` env var override

**`src/tests/test_cost_recorder.py`** — `HFJobsCostRecorder`:
- Test all 4 state transitions
- Mock `HfApi` uploads
- Verify `_workflow_cost.json` schema at each state
- Verify `complete()` returns new dict (does not mutate input)
- Verify `start()` uploads RUNNING state before compute begins
- Verify graceful handling when HF Hub upload fails (log warning, continue)
- Verify `hf_job_id` capture from environment variable

**`src/tests/test_runner.py`** — additional tests:
- Test `WorkflowSkippedError` dispatches `on_skip` (not `on_error`)
- Test `WorkflowSkippedError` does not re-raise (exit 0)

### E2E Verification (Manual)

1. `deploy_wheel.py` → UC Volume
2. Trigger `compute_spadl_vaep` on Databricks → verify `workflow_cost_live` row
3. Run `compute_xt_grid_hf.py` on HF Jobs → verify `_workflow_cost.json`
4. `dbt build --select fct_workflow_costs` → verify model builds
5. Update workflow card baselines with actual runtimes

---

## 8. Files Changed

| Action | File | Purpose |
|--------|------|---------|
| **New** | `src/ingestion/cost_hook.py` | `CostEstimateHook` class |
| **New** | `src/analytics/cost.py` | `HFJobsCostRecorder` class + rate constants |
| **New** | `scripts/create_cost_table.sql` | DDL for `workflow_cost_live` |
| **New** | `scripts/deploy_wheel.py` | HF Hub → UC Volume deploy |
| **New** | `dbt_project/models/marts/fct_workflow_costs.sql` | Cold tier dbt model |
| **New** | `src/tests/test_cost_hook.py` | CostEstimateHook unit tests |
| **New** | `src/tests/test_cost_recorder.py` | HFJobsCostRecorder unit tests |
| **Edit** | `src/workflows/runner.py` | Add `WorkflowSkippedError` → `on_skip` dispatch |
| **Edit** | `src/tests/test_runner.py` | Tests for skip dispatch |
| **Edit** | `dbt_project/models/marts/_marts__models.yml` | Contract for fct_workflow_costs |
| **Edit** | 12 × `src/ingestion/*.py` | Register CostEstimateHook |
| **Edit** | 7 × `scripts/*_hf.py` | Add HFJobsCostRecorder lifecycle |
| **Edit** | `pyproject.toml` | Add `databricks-sdk` to dev deps (if not present) |
| **Edit** | Workflow card YAMLs | Update baselines after E2E runs |

---

## 9. Prerequisites

Before E2E verification, the following access must be configured:

```sql
-- Grant system table access to the ingestion service principal
GRANT SELECT ON system.billing.usage TO `ingestion-sp`;
GRANT SELECT ON system.billing.list_prices TO `ingestion-sp`;
GRANT SELECT ON system.lakeflow.job_task_run_timeline TO `ingestion-sp`;
```

**Note:** If system table access is not available (permissions denied), the dbt model gracefully degrades — it builds with zero rows. The warm/hot tiers operate independently and are not affected.

**Verification:** After granting, run:
```sql
SELECT COUNT(*) FROM system.billing.usage WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS;
```
If this returns >0, system table access is confirmed.
