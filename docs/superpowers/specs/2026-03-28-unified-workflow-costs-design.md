# Design Spec: Unified Workflow Cost Tracking

**Date:** 2026-03-28
**Status:** Draft
**Branch:** `feature/hf-buckets-auto-refresh`

## Problem

The Workflows page table has six gaps that prevent cost and runtime data from displaying for HF Jobs workflows:

| # | Gap | Affected Workflows |
|---|---|---|
| G1 | `_build_table_data()` looks up by `execution.inference.entry_point` — HF-only workflows have no inference block | wf-epv-reachability, wf-space-creation |
| G2 | `_fetch_live_hf_status()` filters to `state == "RUNNING"` only — completed HF Jobs data discarded | All HF Jobs workflows |
| G3 | `sync_hf_costs.py` not scheduled — HF Hub → `workflow_cost_live` bridge never runs | All HF Jobs workflows |
| G4 | Cost column uses only cold-tier `fct_workflow_costs_synced` (Databricks billing only) | All HF Jobs workflows |
| G5 | DB+HF workflows need combined cost across platforms — no aggregation | wf-vaep, wf-xg-v1, wf-xg-v2, wf-xt-grids, wf-obso-pausa |
| G6 | Last Run / Duration from Databricks Jobs API only — HF Jobs have no path | All HF Jobs workflows |

### Workflow runtime classification

| Runtime Type | Workflows | Cost Sources |
|---|---|---|
| DB-only | wf-defcon, wf-football2vec, wf-line-breaking, wf-model-validation, wf-off-ball-xt, wf-pitch-control, wf-elastic-sync, wf-entity-resolution, wf-formations | Databricks billing (`system.billing.usage`) |
| HF-only | wf-epv-reachability, wf-space-creation | HF Jobs estimated cost (`_workflow_cost.json`) |
| DB+HF | wf-vaep, wf-xg-v1, wf-xg-v2, wf-xt-grids, wf-obso-pausa | Both — must be summed |

## Design

### 1. Unified key: `workflow_id` everywhere

All cost data keyed on `workflow_id` (the card `id`, e.g., `wf-vaep`).

**`CostEstimateHook`** — already writes `workflow_id` to `workflow_cost_live`. No change.

**`fct_workflow_costs` (dbt model)** — currently keyed by `task_key` only, built from `system.billing.usage` + `job_task_run_timeline`. Add a join to `workflow_cost_live` on `task_key` to resolve `workflow_id`. New column in the SELECT and enforced contract. `workflow_id` is NULL for any `task_key` without a `CostEstimateHook` run (graceful degradation).

**`fct_workflow_costs_synced`** — gains `workflow_id` column via the dbt model change. Synced table contract updated in Terraform.

**`_build_table_data()`** — all lookups switch from `entry_point` to `workflow_id`.

**`_fetch_job_runs()`** — currently returns `dict[str, dict]` keyed by Databricks `task_key`. Build a `task_key → workflow_id` reverse lookup from the loaded workflow cards (`card.execution.inference.entry_point → card.id`). Re-key the dict before returning.

### 2. HF Jobs cost history: per-run files on HF Hub

Each run persisted as a separate file in the output HF repo. Naturally idempotent — same filename overwrites with identical data.

**`HFJobsCostRecorder.complete()`:**

1. Write `_workflow_cost.json` (unchanged — still used for live RUNNING detection)
2. Upload `_cost_history/{hf_job_id}.json` with the same COMPLETED payload
3. List `_cost_history/` and delete files where `started_at > 90 days` (prune in same commit)
4. If `hf_job_id` is None (local dev), use UTC timestamp slug as filename

**`HFJobsCostRecorder.fail()` and `skip()`:** Same pattern — write to `_cost_history/{hf_job_id}.json`.

**`HFJobsCostRecorder.start()`:** Unchanged — writes `_workflow_cost.json` only (RUNNING state). No history file until completion.

**File format:** Identical to current `_workflow_cost.json` schema:

```json
{
  "workflow_id": "wf-epv-reachability",
  "phase": "grid_computation",
  "hf_job_id": "69c84c73bf20ec90acee3fce",
  "state": "COMPLETED",
  "started_at": "2026-03-28T21:47:30+00:00",
  "ended_at": "2026-03-28T21:48:11+00:00",
  "duration_seconds": 41,
  "estimated_cost_usd": 0.0001,
  "rate_usd_per_hour": 0.01,
  "row_count": 12800,
  "updated_at": "2026-03-28T21:48:11+00:00"
}
```

### 3. App-side data flow: reading and combining costs

**`_fetch_hf_cost_history()` (new, TTL 60s)** — replaces `_fetch_live_hf_status()`. For each HF Jobs repo discovered from workflow cards:

1. Read `_workflow_cost.json` for live RUNNING detection (unchanged behavior)
2. List `_cost_history/` via `list_repo_tree()`
3. Download files with `started_at` in last 30 days (huggingface_hub caches downloads)
4. Return `dict[str, HFCostData]` keyed by `workflow_id`:

```python
@dataclass
class HFCostData:
    runs: list[dict]        # All run records from _cost_history/
    is_running: bool        # True if _workflow_cost.json shows RUNNING
    latest_run: dict | None # Most recent completed/failed run
```

**`_build_table_data()` column population:**

| Column | DB-only | HF-only | DB+HF |
|---|---|---|---|
| Cost (30d) | Cold tier by `workflow_id` | Sum of `_cost_history/` runs | Cold tier + HF history summed |
| Avg/Run | Cold tier cost / cold tier runs | HF total / HF run count | Total cost / total runs |
| Last Run | Jobs API (re-keyed by `workflow_id`) | Most recent `_cost_history/` record | Whichever is more recent |
| Last Duration | Jobs API | Most recent `_cost_history/` record | From whichever ran most recently |
| Status | Jobs API state | `_workflow_cost.json` RUNNING or latest history state | RUNNING if either source shows RUNNING; otherwise most recent |
| Freshness | Last Run vs SLA | Last Run vs SLA | Last Run vs SLA (using most recent across both) |

### 4. `sync_hf_costs.py` — catch-all backup

Demoted from primary display path to historical backup for Delta-side queries.

**Purpose:** Populate `workflow_cost_live` with HF Jobs run data for (a) the dbt cold-tier model join and (b) ad-hoc Delta SQL.

**Changes:**

1. Read from `_cost_history/*.json` (not just `_workflow_cost.json`) to capture all runs
2. Include `workflow_id` in the MERGE payload (already present in the JSON)
3. Dedup via existing MERGE key: `run_id = "hf-{hf_job_id}"`

**Scheduling:**

- **Daily cron:** New Terraform Databricks job, `0 6 * * *` UTC. Single task, serverless, runs `sync_hf_costs.py`. Provides a floor guarantee of daily freshness.
- **Pre-task in dbt workflow:** Also runs before `fct_workflow_costs` during dbt builds, providing opportunistic freshness when the warehouse is already awake.

### 5. Schema and infrastructure changes

**`workflow_cost_live` table** — already has `workflow_id` column. No change.

**`fct_workflow_costs` dbt model:**

- Add CTE joining `job_task_run_timeline` with `workflow_cost_live` on `task_key` to resolve `workflow_id`
- Add `workflow_id` (STRING, nullable) to SELECT
- Update enforced contract in `_marts__models.yml`

**`fct_workflow_costs_synced` (Terraform):**

- Synced table gains `workflow_id` via the dbt model change
- PK stays unchanged (`["task_key", "usage_date", "job_run_id"]`) — `workflow_id` is a non-PK column addition, which syncs transparently without recreation

**New Terraform resource:**

- Databricks job: `sync_hf_costs_daily`
- Schedule: `0 6 * * *` UTC
- Single task: `sync_hf_costs` entry point
- Serverless compute

### 6. What's NOT changing

- **`_workflow_cost.json` write on `start()`** — unchanged, still used for RUNNING detection
- **`CostEstimateHook`** — no changes, already writes `workflow_id`
- **HF Jobs scripts** — no changes; they use `HFJobsCostRecorder` which inherits the new behavior
- **Stats panel / detail page** — uses `_fetch_warm_costs()`, not changing in this work
- **Auto-refresh timer** — already implemented in D34, continues to work with new TTL-cached function
- **`wf-xg-v1` / `wf-xg-v2` entry_point collision** — naturally fixed by switching to `workflow_id` as primary key; they'll show separate cost data
- **`wf-formations`** — no Terraform Databricks task defined; out of scope

## Files Modified

| File | Change |
|---|---|
| `src/analytics/cost.py` | `HFJobsCostRecorder`: add `_cost_history/` write + pruning on complete/fail/skip |
| `hf_taipy_app/src/state/workflows.py` | Replace `_fetch_live_hf_status()` with `_fetch_hf_cost_history()`. Rewrite `_build_table_data()` lookups to use `workflow_id`. Re-key `_fetch_job_runs()` output. |
| `dbt_project/models/marts/fct_workflow_costs.sql` | Join `workflow_cost_live` to resolve `workflow_id` |
| `dbt_project/models/marts/_marts__models.yml` | Add `workflow_id` to contract |
| `scripts/sync_hf_costs.py` | Read `_cost_history/*.json` instead of single file |
| `terraform/modules/workflows/main.tf` | Add `sync_hf_costs` as pre-task in dbt workflow |
| `terraform/environments/dev/main.tf` | Add `sync_hf_costs_daily` cron job resource |
| `src/tests/test_workflows_auto_refresh.py` | Update tests for new data flow |
| `src/tests/test_sync_hf_costs.py` | Update tests for `_cost_history/` reading |

## Testing

- Unit tests: `_fetch_hf_cost_history()` with mocked HF Hub responses (empty, single run, multiple runs, RUNNING + history)
- Unit tests: `_build_table_data()` with all three runtime types, verifying correct cost summation
- Unit tests: `HFJobsCostRecorder` writes to `_cost_history/` and prunes old files
- Integration: run EPV job, verify `_cost_history/{job_id}.json` appears in HF repo
- Integration: verify Workflows table shows Cost, Last Run, Duration for the completed EPV run
