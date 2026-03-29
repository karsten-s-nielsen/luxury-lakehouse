# D27 + D34 — HF Storage Buckets & Workflow Auto-Refresh

**Date**: 2026-03-26
**Branch**: `feature/hf-buckets-auto-refresh`
**Tasks**: D27 (HF Storage Buckets), D34 (Auto-Refresh & HF Jobs Cost Bridge)

**Scope revision (implementation):** HF Buckets do not expose HTTPS download URLs
for pip. The wheel stays in the `luxury-lakehouse/build-artifacts` model repo.
Only demo data moves to the `luxury-lakehouse/demo-data` bucket. CI workflow,
`deploy_wheel.py`, and PEP 723 wheel URLs are unchanged. PEP 723 scripts get
a `huggingface-hub` version bump only. `huggingface-hub` was removed from the
Taipy app `requirements.txt` — the live HF status function handles its absence
gracefully via deferred import, and the long-term cost bridge uses Delta/Lakebase
SQL instead.

---

## D27 — HF Storage Buckets

### Goal

Migrate binary artifacts from git-backed HF Hub repos to HF Storage Buckets. Eliminates git history bloat for wheels and parquet files, enables `hf://` fsspec paths, and prepares infrastructure for future large-file workflows (own-footage pipeline).

### Bucket Topology

| Bucket | Contents | Replaces |
|--------|----------|----------|
| `luxury-lakehouse/build-artifacts` | `luxury_lakehouse-*.whl` | `luxury-lakehouse/build-artifacts` model-type git repo |
| `luxury-lakehouse/demo-data` | `sample_tracking.parquet`, `defcon_pressure.parquet`, `career_embeddings.parquet`, `sample_shots.parquet`, `sample_passes.parquet`, `sample_pausa.parquet` | `demo_space/data/` git-committed files |

### Changes by File Area

#### `scripts/setup_hf_buckets.py` (new)

Idempotent provisioning script. Creates both buckets if they don't exist, uploads initial contents (current wheel + demo parquet files). Registered as entry point in `pyproject.toml`. Safe to re-run — checks for existing buckets before creating.

Uses `HfApi().create_bucket()` (or equivalent SDK method). Requires `HF_TOKEN` with write access to the `luxury-lakehouse` org.

#### `pyproject.toml`

Bump `huggingface_hub>=0.25.0` to `huggingface_hub>=1.5.0` in the `embeddings` optional extra. The Bucket API (`create_bucket`, `sync_bucket`, `batch_bucket_files`) requires `>= 1.5.0`.

#### `.github/workflows/python-ci.yml`

Replace the wheel upload at line 68:

```python
# Before: git repo upload
api.upload_file(path_or_fileobj=whl, path_in_repo=whl.split('/')[-1],
    repo_id='luxury-lakehouse/build-artifacts', repo_type='model')

# After: bucket upload
api.upload_file(path_or_fileobj=whl, path_in_repo=whl.split('/')[-1],
    repo_id='luxury-lakehouse/build-artifacts', repo_type='bucket')
```

Exact SDK method TBD — depends on whether `upload_file` supports `repo_type='bucket'` or requires a dedicated bucket upload API. Verify against `huggingface_hub >= 1.5.0` docs at implementation time.

#### `scripts/deploy_wheel.py`

Update from `repo_type="model"` to bucket reads:

- `list_repo_files(repo_id, repo_type="model")` -> bucket-compatible file listing
- `hf_hub_download(repo_id, filename, repo_type="model")` -> bucket download (or `hf://buckets/` fsspec path)

#### 8 PEP 723 Script Headers

All scripts in `scripts/` that reference the wheel URL:

- `compute_xt_grid_hf.py`
- `train_xg_model_hf.py`
- `train_xg_v2_hf.py`
- `train_vaep_model_hf.py`
- `compute_obso_hf.py`
- `compute_space_creation_hf.py`
- `compute_epv_transition_hf.py`
- `publish_xg_shots_hf.py`

Changes per script:
1. Update wheel URL from `https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl` to the bucket-equivalent URL.
2. Bump `huggingface-hub>=0.25.0` to `huggingface-hub>=1.5.0` in PEP 723 dependency block.

Note: `publish_freeze_frame_hf.py` does not reference the wheel — no changes needed.

#### `demo_space/app.py`

Replace local parquet reads with `hf://` fsspec paths:

```python
# Before
df = pd.read_parquet("data/sample_tracking.parquet")

# After
df = pd.read_parquet("hf://buckets/luxury-lakehouse/demo-data/sample_tracking.parquet")
```

All 6 parquet files in `demo_space/data/` migrate to the `demo-data` bucket.

#### `demo_space/data/`

Remove git-committed parquet files after they're uploaded to the bucket via `setup_hf_buckets.py`. The directory can be deleted or kept empty with a `.gitkeep` and a comment pointing to the bucket.

### `hf-mount` Evaluation

`hf-mount` is a FUSE-based local mount for HF Hub repos and buckets. Evaluated for local dev applicability:

**Not applicable today.** All current HF Hub I/O is handled by SDK calls (`upload_file`, `hf_hub_download`) or fsspec paths (`hf://`). No workflow requires filesystem-level access to HF-hosted files.

**Future applicability:** The own-footage pipeline (Veo3 -> Respo.Vision, ~17 GB/match) would benefit from mounting a bucket locally rather than downloading full match files. Revisit when that pipeline lands.

---

## D34 — Workflow Auto-Refresh & HF Jobs Cost Bridge

### Goal

Surface HF Jobs cost data in the Taipy Workflows page and add 2-minute auto-refresh with visual state indicators (running/completed/failed badges). Two sub-components: a bridge script for historical costs and a live status read for running jobs.

### Component 1: Cost Bridge (HF Hub -> Delta)

#### `scripts/sync_hf_costs.py` (new)

Reads `_workflow_cost.json` from each HF Jobs dataset repo and MERGEs into `workflow_cost_live` Delta table. Registered as entry point in `pyproject.toml`.

**Repo discovery:** Parses `workflow-cards/wf-*.yaml`, filters to `runtime: hf_jobs`, extracts `repo_id` from each card's config. Single source of truth — no hardcoded repo list.

**Schema mapping** (HF JSON -> Delta):

| HF JSON field | Delta column | Mapping |
|---------------|-------------|---------|
| `workflow_id` | `workflow_id` | Direct |
| `phase` | `phase` | Direct |
| `state` | `state` | Direct |
| `started_at` | `started_at` | Direct |
| `ended_at` | `ended_at` | Direct |
| `duration_seconds` | `duration_seconds` | Direct |
| `estimated_cost_usd` | `estimated_cost_usd` | Direct |
| `rate_usd_per_hour` | `rate_usd_per_hour` | Direct |
| `hf_job_id` (from env) | `hf_job_id` | Direct |
| — | `run_id` | Derived from `hf_job_id` |
| — | `runtime` | Constant `"hf_jobs"` |
| — | `job_run_id` | `None` (Databricks-specific) |
| — | `task_key` | From workflow card entry point |
| — | `cost_source` | Constant `"hf_hub_sync"` |
| — | `updated_at` | UTC now |

**MERGE key:** `run_id` (same as `CostEstimateHook`).

**Idempotent:** Safe to re-run. MERGE upserts — no duplicates on retry.

#### Databricks Task

New task in the existing Databricks job, scheduled every 15 minutes. Runs `sync_hf_costs.py`.

New workflow card: `workflow-cards/wf-sync-hf-costs.yaml` for self-tracking.

#### Data Flow

```
HF Jobs script -> _workflow_cost.json (HF Hub repo)
                          |
        sync_hf_costs.py (Databricks, every 15 min)
                          |
        workflow_cost_live Delta (MERGE on run_id)
                          |
        workflow_cost_live_synced (Lakebase, continuous sync)
                          |
        Taipy warm tier SQL (_fetch_warm_costs, 2 min TTL)
```

### Component 2: Live Status (HF Hub Direct Read)

For "is my job running right now?" — the bridge's 15-min latency is too slow for live monitoring.

#### `_fetch_live_hf_status()` in `hf_taipy_app/src/state/workflows.py`

- Uses `HfApi().hf_hub_download()` to read `_workflow_cost.json` from each HF Jobs repo (workflow-card-derived repo list).
- 60-second TTL cache — fast enough for live monitoring, light on API calls.
- Only returns records where `state == "RUNNING"`.
- Results merged into `_compute_stats()`: live running jobs get a "Running" badge, cost shows as "estimated (live)".
- **Fallback:** If the HF Hub read fails (network error, rate limit, repo not found), silently skipped. Page renders from warm tier only. No error surfaced to user for background poll failure.

#### Combined Running Jobs Count

Both runtimes contribute to the running jobs count:
- **Databricks**: from existing `_fetch_job_runs()` via `ws.jobs.list_runs()` SDK call
- **HF Jobs**: from `_fetch_live_hf_status()` via direct HF Hub read

The Run Volume stat card detail line shows combined count.

#### Taipy App Dependency

`huggingface_hub` must be in the Taipy app's `requirements.txt`. Verify current pin and bump to `>=1.5.0` if needed.

### Component 3: Auto-Refresh

#### Polling Mechanism

Taipy has no built-in polling primitive. Implementation:

- `threading.Timer` in the state module.
- On navigate to `"AI-ML-Workflows"`: start a 2-minute recurring timer.
- Timer calls `state.invoke_callback()` to trigger refresh on the GUI thread (Taipy requirement — state mutations must happen on the GUI thread).
- On navigate away: cancel the timer.
- Timer only runs while the Workflows page is active — zero cost for other pages.

#### Cache TTL Adjustment

Warm tier TTL drops from 1800s (30 min) to 120s (2 min) to match the auto-refresh interval. No point polling if the cache won't expire.

### Component 4: Visual State Indicators

#### Status Column

New "Status" column in the workflows dashboard table.

| State | Badge text | Color | Notes |
|-------|-----------|-------|-------|
| Running | `RUNNING` | Blue | Animated pulse CSS |
| Completed | `COMPLETED` | Green | |
| Failed | `FAILED` | Red | |
| Skipped | `SKIPPED` | Grey | |
| No recent run | `STALE` | Amber | No run in last 30 days |

Badges rendered via `table_cell_class_name` callback pattern (same as existing Type/Runtime/Freshness columns).

#### Stat Card Updates

- **Run Volume** detail shows currently running jobs count (both Databricks + HF Jobs).
- **Freshness** badges reflect live status — a workflow with a running job shows as fresh regardless of last-completed timestamp.

---

## File Change Summary

### New Files

| File | Purpose |
|------|---------|
| `scripts/setup_hf_buckets.py` | Idempotent bucket provisioning + initial upload |
| `scripts/sync_hf_costs.py` | HF Hub -> Delta cost bridge |
| `workflow-cards/wf-sync-hf-costs.yaml` | Workflow card for the bridge task |

### Modified Files

| File | Change |
|------|--------|
| `pyproject.toml` | Bump `huggingface_hub>=1.5.0`, add entry points |
| `.github/workflows/python-ci.yml` | Bucket upload instead of model repo |
| `scripts/deploy_wheel.py` | Read from bucket |
| 8x `scripts/*_hf.py` | Update wheel URL + bump `huggingface-hub` pin |
| `demo_space/app.py` | `hf://` fsspec paths for parquet reads |
| `hf_taipy_app/src/state/workflows.py` | Live HF status, auto-refresh timer, status badges, TTL adjustment |
| `hf_taipy_app/src/pages/workflows.py` | Status column in table ContentBlock |
| `hf_taipy_app/requirements.txt` | Bump `huggingface-hub>=1.5.0` |

### Removed Files

| File | Reason |
|------|--------|
| `demo_space/data/*.parquet` (6 files) | Migrated to `luxury-lakehouse/demo-data` bucket |

---

## Out of Scope

- `hf-mount` integration (document-only evaluation, no code)
- D35 (detail drilldown panel — separate task)
- Workflow card validation beyond the new `wf-sync-hf-costs.yaml`
- Changes to `CostEstimateHook` or `HFJobsCostRecorder` (these work as-is)
