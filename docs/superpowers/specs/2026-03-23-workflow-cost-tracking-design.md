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
-- Primary: delete non-running warm rows covered by cold tier
DELETE FROM {catalog}.{schema}.workflow_cost_live
WHERE state != 'RUNNING'
  AND ended_at IS NOT NULL
  AND ended_at < (
      SELECT COALESCE(MAX(usage_date), DATE '1970-01-01') + INTERVAL 1 DAY
      FROM {catalog}.{schema}.fct_workflow_costs
  );

-- Secondary: orphaned RUNNING rows older than 24 hours
DELETE FROM {catalog}.{schema}.workflow_cost_live
WHERE state = 'RUNNING'
  AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS;
```

**Access requirement:** `SELECT` on `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.job_task_run_timeline` for the ingestion service principal.

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

**MERGE key:** `run_id` (UUID from `WorkflowContext`). Insert on start, update on completion. Single row per run — no separate start/end rows.

**Databricks metadata from Spark conf:**
- `spark.databricks.job.runId` → `job_run_id`
- `spark.databricks.task.key` → `task_key`

Both are `None` in local/notebook contexts (handled gracefully).

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

Mirrors the `CostEstimateHook` lifecycle but writes `_workflow_cost.json` to the script's HF Hub repo instead of Delta.

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
| `complete(metadata, row_count)` | Update JSON to COMPLETED, inject cost fields into metadata dict, return enriched metadata | `{"state": "COMPLETED", "estimated_cost_usd": 0.04, ...}` |
| `fail(error)` | Update JSON to FAILED with partial cost | `{"state": "FAILED", ...}` |
| `skip(reason)` | Update JSON to SKIPPED, zero cost | `{"state": "SKIPPED", ...}` |

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
    rate_usd_per_hour  DECIMAL(10,4),
    estimated_cost_usd DECIMAL(10,4),
    cost_source        STRING        NOT NULL,
    updated_at         TIMESTAMP     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

Run once via Databricks SQL or notebook. Not managed by dbt (written by Spark, not dbt).

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
from analytics.cost import HFJobsCostRecorder

recorder = HFJobsCostRecorder(
    workflow_id="wf-xt-grids",
    phase="grid_computation",
    rate_usd_per_hour=0.01,
    repo_id="luxury-lakehouse/xt-grid-values",
)
recorder.start()

# ... existing compute work ...

metadata = recorder.complete(metadata, row_count=len(df))
api.upload_file(json.dumps(metadata), path_in_repo="metadata.json", ...)
```

**7 scripts to modify:**
- `compute_xt_grid_hf.py` (cpu-basic, $0.01/hr)
- `compute_epv_transition_hf.py` (cpu-basic, $0.01/hr)
- `compute_obso_hf.py` (a10g-small, $1.00/hr)
- `compute_space_creation_hf.py` (a10g-small, $1.00/hr)
- `train_vaep_model_hf.py` (cpu-basic, $0.01/hr)
- `train_xg_model_hf.py` (cpu-basic, $0.01/hr)
- `train_xg_v2_hf.py` (a10g-small, $1.00/hr)

**Elapsed time standardization:** 5 of 7 scripts currently lack elapsed time tracking. The `HFJobsCostRecorder` handles this internally (records `started_at` on `start()`, computes duration on `complete()`), so no manual `time.time()` tracking is needed.

**Not modified:** `publish_xg_shots_hf.py`, `publish_freeze_frame_hf.py` (data publishing, not compute workflows).

---

## 6. Wheel Deploy Script

**File:** `scripts/deploy_wheel.py`

Downloads the latest `.whl` from `luxury-lakehouse/build-artifacts` on HF Hub and uploads to the UC Volume at `/Volumes/{catalog}/bronze/libs/`.

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
- Verify `complete()` injects cost fields into metadata dict
- Verify `start()` uploads RUNNING state before compute begins

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
| **New** | `src/analytics/cost.py` | `HFJobsCostRecorder` class |
| **New** | `scripts/create_cost_table.sql` | DDL for `workflow_cost_live` |
| **New** | `scripts/deploy_wheel.py` | HF Hub → UC Volume deploy |
| **New** | `dbt_project/models/marts/fct_workflow_costs.sql` | Cold tier dbt model |
| **New** | `src/tests/test_cost_hook.py` | CostEstimateHook unit tests |
| **New** | `src/tests/test_cost_recorder.py` | HFJobsCostRecorder unit tests |
| **Edit** | `dbt_project/models/marts/_marts__models.yml` | Contract for fct_workflow_costs |
| **Edit** | 12 × `src/ingestion/*.py` | Register CostEstimateHook |
| **Edit** | 7 × `scripts/*_hf.py` | Add HFJobsCostRecorder lifecycle |
| **Edit** | `pyproject.toml` | Add `databricks-sdk` to dev deps (if not present) |
| **Edit** | Workflow card YAMLs | Update baselines after E2E runs |
