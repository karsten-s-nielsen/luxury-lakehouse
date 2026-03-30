# Workflows Page Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 4 bugs on the AI/ML Workflows Taipy page: DB cost display, run volume, freshness stat/table discrepancy, and 3 empty rows.

**Architecture:** Split `compute_xg_model` Databricks task into v1-only and v2-only tasks with separate output tables. Fix the workflows page state module to unify freshness resolution across stat cards and table, handle 1:N task_key-to-workflow mappings, and improve cost display for zero-cost workflows.

**Tech Stack:** Python (Taipy state module), Terraform (Databricks workflow), dbt (Delta models), pyproject.toml (entry points)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/ingestion/xg_model.py` | Modify | Split into v1-only pipeline; remove v2 scoring from UDF and output schema |
| `src/ingestion/xg_model_v2.py` | Create | v2-only pipeline: load set encoder weights, score with freeze frames, write to `xg_predictions_v2` |
| `src/tests/test_xg_model.py` | Modify | Remove v2-related assertions from v1 tests |
| `src/tests/test_xg_model_v2.py` | Create | Tests for v2 pipeline (UDF, skip guard, output schema) |
| `terraform/modules/workflows/main.tf` | Modify | Add `compute_xg_model_v2` task alongside existing `compute_xg_model` |
| `pyproject.toml` | Modify | Add `compute_xg_model_v2` entry point |
| `workflow-cards/wf-xg-v2.yaml` | Modify | Change `entry_point` from `compute_xg_model` to `compute_xg_model_v2` |
| `hf_taipy_app/src/state/workflows.py` | Modify | Fix 1:N task_key mapping, unify freshness, improve cost display |
| `dbt_project/models/marts/_marts__models.yml` | Modify | Remove v2 columns from `fct_xg_predictions` contract if present |

---

### Task 1: Split xg_model.py -- Remove v2 from v1 pipeline

**Files:**
- Modify: `src/ingestion/xg_model.py`

- [ ] **Step 1: Remove v2 from `_make_scoring_udf`**

Remove the `v2_weights_bytes` parameter and all v2 scoring logic (lines 32, 39-41, 68-74, 86-118, 126-128). The UDF should only produce `shot_id`, `match_id`, `competition_id`, `xg_logistic`, `xg_gradient_boosted`.

- [ ] **Step 2: Remove `_try_load_champion_xg_v2` function**

Delete lines 181-213. This function moves to `xg_model_v2.py`.

- [ ] **Step 3: Remove `_load_shots_with_context` function**

Delete lines 216-244. The freeze-frame join is only needed for v2. The v1 pipeline reads from `fct_shots` directly (no freeze frame context needed).

- [ ] **Step 4: Simplify `run_pipeline`**

Remove step 4 (v2 weight loading, lines 306-316). Remove `v2_weights_bytes` from `_make_scoring_udf` call (line 319). Update `output_schema` to remove v2 columns (line 321-325). Remove `shot_freeze_frame` from the shots query.

- [ ] **Step 5: Update module docstring**

Remove the "V2 extension" paragraph. This module is now v1-only.

- [ ] **Step 6: Run existing tests**

Run: `uv run pytest src/tests/test_xg_model.py -v --tb=short`
Expected: Some tests will fail (they assert v2 columns). Note which ones.

---

### Task 2: Create xg_model_v2.py -- v2-only pipeline

**Files:**
- Create: `src/ingestion/xg_model_v2.py`

- [ ] **Step 1: Create the v2 pipeline module**

New file with:
- `_TABLE_NAME = "xg_predictions_v2"`
- `_make_v2_scoring_udf(v2_weights_bytes)` -- UDF that loads set encoder, parses freeze frames, produces `shot_id`, `match_id`, `competition_id`, `xg_set_encoder`, `xg_ci_lower`, `xg_ci_upper`
- `_try_load_champion_xg_v2(log, catalog, schema)` -- moved from xg_model.py
- `_load_shots_with_context(spark, catalog, schema)` -- moved from xg_model.py
- `@workflow("wf-xg-v2", phase="inference") def run_pipeline(...)` -- skip guard on competition_id against `xg_predictions_v2`, load v2 weights, score, write with `replaceWhere`
- `def main()` -- CLI entry point with `CostEstimateHook`

The v2 pipeline is structurally identical to v1 but:
- Only loads v2 weights (no logistic/XGBoost)
- Uses `_load_shots_with_context` for freeze frames
- Writes to `xg_predictions_v2` (separate table)
- Decorated with `@workflow("wf-xg-v2", phase="inference")`

- [ ] **Step 2: Run linting**

Run: `uv run ruff check src/ingestion/xg_model_v2.py`
Expected: All checks passed

---

### Task 3: Update entry points and Terraform

**Files:**
- Modify: `pyproject.toml`
- Modify: `terraform/modules/workflows/main.tf`
- Modify: `workflow-cards/wf-xg-v2.yaml`

- [ ] **Step 1: Add entry point to pyproject.toml**

Add after the existing `compute_xg_model` line:
```
compute_xg_model_v2 = "ingestion.xg_model_v2:main"
```

- [ ] **Step 2: Add Terraform task**

Add a new `task` block after `compute_xg_model` in `main.tf`:
```hcl
task {
    task_key        = "compute_xg_model_v2"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_xg_model_v2"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }
```

- [ ] **Step 3: Update wf-xg-v2.yaml**

Change `execution.inference.entry_point` from `compute_xg_model` to `compute_xg_model_v2`.
Change `execution.inference.module` from `ingestion.xg_model` to `ingestion.xg_model_v2`.

- [ ] **Step 4: Validate workflow cards**

Run: `uv run validate_workflow_cards --validate workflow-cards/`
Expected: 16/16 OK

---

### Task 4: Update tests

**Files:**
- Modify: `src/tests/test_xg_model.py`
- Create: `src/tests/test_xg_model_v2.py`

- [ ] **Step 1: Remove v2 assertions from v1 tests**

In `test_xg_model.py`, find all assertions on `xg_set_encoder`, `xg_ci_lower`, `xg_ci_upper` and remove them. Update any `output_schema` references. Remove tests for `_try_load_champion_xg_v2` and `_load_shots_with_context`.

- [ ] **Step 2: Create v2 test file**

Tests for:
- `_make_v2_scoring_udf` produces correct output columns
- Skip guard on `xg_predictions_v2`
- `_load_shots_with_context` returns freeze frame column
- Output schema has `shot_id`, `match_id`, `competition_id`, `xg_set_encoder`, `xg_ci_lower`, `xg_ci_upper`

- [ ] **Step 3: Run all tests**

Run: `uv run pytest src/tests/test_xg_model.py src/tests/test_xg_model_v2.py -v --tb=short`
Expected: All pass

---

### Task 5: Fix workflows page -- task_key 1:N mapping

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py`

Changes already started in working tree. Verify and complete:

- [ ] **Step 1: Confirm `_build_task_key_to_wf_ids` returns `dict[str, list[str]]`**

The global variable, builder function, and re-keying in `_fetch_job_runs` should all use the 1:N pattern. Jobs API run data (last_run, duration, state) is shared across all cards with the same entry_point. Cost is NOT shared (comes from `_fetch_cold_costs` keyed on workflow_id).

- [ ] **Step 2: Confirm freshness stat card uses Jobs + HF sources**

The freshness loop in `_compute_stats` should resolve `last_run` from both Jobs API and HF history (matching the table's logic at lines 1064-1098).

---

### Task 6: Fix workflows page -- cost and volume display

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py`

- [ ] **Step 1: Show `$0.00` instead of `--` when workflow has run**

In `_build_table_data`, change the cost display logic: if `total_cost == 0` but `last_run_ts is not None`, show `$0.00` instead of `--`. The `--` should only appear when the workflow has never run.

- [ ] **Step 2: Add debug logging to `_fetch_cold_costs`**

Replace the bare `except Exception` at line 771 with explicit error logging:
```python
except Exception:
    logger.warning("Cold cost query failed", exc_info=True)
    return _empty
```
This is already in place (line 771) but verify the message includes `exc_info=True` so the actual error is visible in Space logs.

---

### Task 7: Deploy and verify

- [ ] **Step 1: Run full local CI**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ --tb=short
```

- [ ] **Step 2: Deploy to staging**

```bash
python scripts/manage_space.py deploy staging
```

- [ ] **Step 3: Verify on staging**

Check all 4 stat cards on the AI/ML Workflows page:
- Total Cost (30 Days): should show DB + HF breakdown
- Run Volume (30 Days): should show non-zero count
- Freshness: should show 16/16 monitored, all sources resolved
- Table: all 16 rows should have data (some may show $0.00 for cost, which is correct)

- [ ] **Step 4: Deploy to production**

```bash
python scripts/manage_space.py deploy production
```

- [ ] **Step 5: Apply Terraform**

```bash
cd terraform/environments/dev && terraform plan && terraform apply
```

This adds the `compute_xg_model_v2` task to the Databricks workflow.

- [ ] **Step 6: Commit**

Single commit with all changes.
