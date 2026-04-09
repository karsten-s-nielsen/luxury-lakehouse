# Guard & Pipeline Hardening — Design Spec

**Date:** 2026-04-08
**Scope:** D48, D49, D50, D46, D47, D40e + 3 cross-cutting TDD requirements
**E2E testing:** Full end-to-end via AWS, Databricks, and HF access

---

## Motivation

The Cycle 5 Phase 1 E2E verification (session 33) exposed five production issues and one performance bottleneck:

- **D48:** `backfill_extra_json` silently fails on every run (12.1M row protobuf overflow on MERGE), burning 19 minutes per pipeline run with zero output. Guard/writer semantic mismatch causes infinite re-runs.
- **D49:** `defcon_lite_360`/`defcon_lite_tracking` guards fail at import time due to transitive `xgboost` dependency through `defcon_lite_common`, silently disabling two compute pipelines.
- **D50:** 3 guards fail with Spark `CheckAnalysis` errors — 2 schema column mismatches (`spadl_vaep`, `line_breaking`), 1 is the D49 import failure.
- **D46:** `formations_shape_graph` guard uses `wf-formations-sg` but decorator/card/seed all use `wf-shape-graphs`. 4 seed rows missing. 1 truly orphaned workflow decorator.
- **D47:** 4 guards return binary 0/1 counts without entity IDs, forcing pipelines to redo discovery queries.
- **D40e:** 33 guards run sequentially in the freshness gate, adding unnecessary latency.

Three cross-cutting TDD requirements ensure these classes of bugs cannot recur:

1. **Time/cost capture:** Every guard+body execution produces observability data.
2. **Exception surfacing:** No `run_pipeline` silently swallows exceptions from its body.
3. **Count/ID consistency:** Guard `count` equals `len(distinct entity IDs)` for non-exempt guards.

---

## Execution Order

TDD-first, vertical slices. Tests written first (red), each subsequent fix turns tests green.

| Step | Item | Dependency |
|------|------|------------|
| 1 | TDD conformance tests | None — foundation layer |
| 2 | D50 — schema fixes | None (2 one-line changes) |
| 3 | D49 — import isolation | None (unblocks defcon_lite guards) |
| 4 | D48 — backfill extra JSON | D49 semantic mismatch fix feeds D47 |
| 5 | D47 — promote 4 guards | D48 (backfill-extra guard promotion depends on semantic fix) |
| 6 | D46 — orphan + ID cleanup | Independent |
| 7 | D40e — guard parallelization | All guards correct before parallelizing |

---

## Step 1: TDD Conformance Tests

All tests written before any implementation changes. Tests start red, each subsequent step turns them green.

### Test Class 1: `TestCostTimeCapture` (in `test_guard_conformance.py`)

**Guarantee:** Every pipeline running through `run_workflow` with `CostEstimateHook` produces a MERGE row with valid duration and cost.

- Parametrized over all 33 workflow IDs.
- Mock `SparkSession` + mock `DeltaTable.forName` (same pattern as `test_cost_hook.py`).
- For each workflow: register `CostEstimateHook`, call `run_workflow` with a trivial `@workflow(wf_id)`-decorated function, assert MERGE row contains:
  - `state="COMPLETED"`
  - `duration_seconds >= 0`
  - `estimated_cost_usd` is `Decimal`
  - `entity_count` matches `FilterResult.count`
- Separate test method for error path: function raises, assert `state="FAILED"`, cost still computed.
- **Scope:** Tests pipeline body cost capture. Guard timing is captured separately by `run_gate`'s `timings` dict.

### Test Class 2: `TestExceptionPropagation` (in `test_guard_conformance.py`)

**Guarantee:** No `run_pipeline` silently swallows exceptions from its body.

Two layers — AST for the simple case, behavioral for cross-module helpers:

**Layer 1 — AST scan (same pattern as `TestNoInlineGuardInPipeline`):**
- Walks every `run_pipeline` function body across all guard modules.
- Flags any `try/except` block inside `run_pipeline` that catches `Exception` (or bare `except:`) without a `raise` statement in the handler.
- Exempt: `WorkflowSkippedError` catches (normal skip path).
- Catches the pattern when it occurs directly in `run_pipeline`.

**Layer 2 — Behavioral test (parametrized over all 33 workflow IDs):**
- For each workflow: register a `_RecordingHook`, call `run_workflow` with the real `run_pipeline` function, injecting a mock dependency that raises `RuntimeError`.
- Assert `on_error` was called (exception reached the lifecycle runner).
- Assert `on_complete` was NOT called.
- Assert the `RuntimeError` is re-raised (not swallowed).
- This catches cross-module swallows like D48's `backfill_extra_json` (in `statsbomb.py`) which is called by `statsbomb_backfill_extra.run_pipeline` — the AST scan on `run_pipeline` alone wouldn't see the `try/except` in the helper module.

**Starts RED** because `backfill_extra_json` swallows exceptions at `statsbomb.py:603-604`. D48 fix (add `raise`) turns it green.

**Note:** `TestCostTimeCapture`'s error-path test also verifies `state="FAILED"` rows are written, providing a complementary observability guarantee.

### Test Class 3: `TestGuardCountMatchesIds` (in `test_guard_conformance.py`)

**Guarantee:** For non-exempt guards, `result.count == len(id_list)` and IDs are distinct.

- Parametrized over all workflow IDs NOT in `_METADATA_EXEMPT`.
- For each guard: call `guard.check()` with a mock Spark session returning known test data (2-3 mock IDs from `find_new_ids`).
- Assert `result.count == len(first_list_in_metadata)`.
- Assert `len(set(id_list)) == len(id_list)` — no duplicate IDs.
- Guards currently in `_METADATA_EXEMPT` that are being promoted (D47) start RED until promoted.

### Test Class 4: `TestFreshnessGateTaskValuePropagation` (in `test_freshness_gate.py`)

**Guarantee:** Every `FilterResult` returned by a guard is faithfully written as Databricks task values, including all metadata (entity IDs, chunks), and guard exceptions are caught without crashing the gate.

Tests:

- **`test_write_task_values_two_keys_per_workflow`** — Mock `dbutils.jobs.taskValues.set()`, call `_write_task_values` with a FilterResult containing `count=5`, `metadata={"new_match_ids": ["m1","m2","m3","m4","m5"]}`, `chunks`. Assert `set()` called exactly twice per workflow: once with key=`{wf_id}` + JSON string, once with key=`{wf_id}-count` + integer 5.
- **`test_json_payload_preserves_metadata`** — Write a FilterResult, deserialize the JSON arg from the mock call, assert `metadata["new_match_ids"]` round-trips intact (list order, string types, exact values).
- **`test_json_payload_preserves_chunks`** — Same for `chunks` field.
- **`test_count_value_is_integer`** — Assert the `-count` key value is `int`, not string or Decimal.
- **`test_guard_exception_yields_count_zero_still_written`** — Guard raises, `run_gate` catches and produces `count=0`, verify `_write_task_values` still writes the zero-count result (downstream tasks need the skip signal).
- **`test_read_gate_result_round_trip`** — Full chain: create FilterResult with entity IDs -> `to_json()` -> mock `dbutils.jobs.taskValues.get()` returns that JSON -> `read_gate_result()` -> assert `result.count`, `result.metadata`, `result.chunks` all match original.
- **`test_standalone_mode_no_crash`** — No SparkSession active, `_write_task_values` catches gracefully, returns without raising.

---

## Step 2: D50 — Guard Schema Fixes

Two one-line fixes for confirmed schema mismatches.

### Fix 1: `spadl_vaep.py:71`

`find_new_ids` on `wyscout_events` uses default `id_column="match_id"` but the table column is `matchId` (camelCase).

**Evidence:** `wyscout.py:255` validates `["eventId", "matchId", ...]`. `_wyscout__sources.yml` confirms. `spadl_conversion.py:361` already handles this: `match_id_col = "matchId" if "matchId" in events_columns else "match_id"`.

**Change:** Add `id_column="matchId"` to the `ws_new = find_new_ids(...)` call.

### Fix 2: `line_breaking.py:65`

`source_filter="event_type = 'PASS'"` on `metrica_events` but the table column is `type`.

**Evidence:** `metrica_events.py:55-58` renames to `"type"`. `_metrica__sources.yml:57` confirms.

**Change:** `source_filter="type = 'PASS'"`.

### The 3rd D50 failure

The 3rd production `guard_check_failed` log entry is `defcon_lite_360`/`defcon_lite_tracking` failing at import time with `ModuleNotFoundError: xgboost`. Same symptom (guard returns count=0), different root cause. Fixed by D49.

---

## Step 3: D49 — Import Isolation Fix

### Root Cause

```
defcon_lite_360.py (module-level)
  -> from ingestion.defcon_lite_common import _TABLE_NAME     [line 15]
      -> from analytics.defcon_lite import DefconLiteParams   [line 13]
          -> from xgboost import XGBRegressor                 [line 21]
```

`TestGuardImportIsolation` only does AST analysis of each guard module's direct source. `"ingestion"` is not in `_ANALYTICS_PACKAGES`, so the transitive chain through `defcon_lite_common` passes.

### Part A: Break the import chain

**`defcon_lite_common.py`:**
- Remove `from analytics.defcon_lite import DefconLiteParams` from module level.
- Move it to `TYPE_CHECKING` block for type annotations.
- Move actual import inside UDF closure bodies (`_make_values_udf` functions) where `DefconLiteParams` is used at runtime.
- Remove `DefconLiteParams` from `__all__` re-export.

**`defcon_lite_360.py` and `defcon_lite_tracking.py`:**
- Inline `_TABLE_NAME = "defcon_lite_credits"` directly (it's a single string constant, no abstraction needed).
- Remove `from ingestion.defcon_lite_common import _TABLE_NAME`.

**After fix:** `defcon_lite_common.py` contains only stdlib + pandas imports, column lists, and pure-Python helpers. No `analytics.*` at module level. Both guard modules load cleanly in the `default` environment.

### Part B: Strengthen `TestGuardImportIsolation`

Add a **runtime import test** alongside the existing AST test:
- For each module in `_GUARD_MODULES`, use `unittest.mock.patch` to replace each analytics package in `sys.modules` with a sentinel that raises `ImportError`.
- Attempt `importlib.import_module(guard_module)`.
- If the import succeeds, the guard is clean. If it raises, the guard has a transitive dependency.
- Catches the exact pattern that AST-only analysis misses.
- Existing AST test stays as a fast first pass; runtime test is the definitive check.

---

## Step 4: D48 — Backfill Extra JSON Fix

Three bugs fixed together.

### Bug 1: Silent exception swallow

**File:** `statsbomb.py:603-604`

```python
except Exception:
    logger.exception("Failed batch MERGE for _raw_extra_json backfill")
    # No re-raise — function returns None, task exits 0
```

**Fix:** Add `raise` after `logger.exception(...)`. This allows `run_workflow` to dispatch `on_error` and record `state="FAILED"` in the cost table. Turns `TestExceptionPropagation` green.

### Bug 2: Protobuf size limit

**File:** `statsbomb.py:591-592`

Single-shot `spark.createDataFrame(12.1M rows)` exceeds Spark Connect protobuf limit.

**Fix:** Chunk by `(competition_id, season_id)` — the exact pattern used by `backfill_360` at `statsbomb.py:477-523`. The `needs_backfill_rows` query already returns `competition_id` and `season_id` (line 546).

Loop structure:
```
group needs_backfill_rows by (competition_id, season_id)
  -> concurrent HTTP fetch for that group's matches (ThreadPoolExecutor, existing _HTTP_MAX_WORKERS=4)
  -> build mapping_rows for that group (~3-5K rows)
  -> createDataFrame + MERGE for that group
  -> log progress: "Backfilled group {comp_id}/{season_id}: {n} events in {m} matches"
```

Peak driver memory drops from 12.1M tuples to ~3-5K per group.

### Bug 3: Guard/writer semantic mismatch

**Guard condition** (`statsbomb_backfill_extra.py:36-38`): `_raw_extra_json IS NULL OR _raw_extra_json = '{}'`
**Writer output** (`statsbomb.py:101`): `json.dumps({})` = `"{}"` for events with no type-specific data.

"Successfully processed" events match the guard condition, causing infinite re-runs.

**Fix:** Change guard condition to `_raw_extra_json IS NULL` only. Events with `_raw_extra_json = '{}'` are legitimately backfilled (they have no extra data). The writer distinguishes "never processed" (`NULL`) from "processed, no extra data" (`'{}'`).

### Pipeline consumption

After D48, the chunked `backfill_extra_json` receives `match_ids` from `filter_result.metadata["new_match_ids"]` (set up by D47's guard promotion) instead of doing its own full-table discovery scan. The `needs_backfill_rows` query inside `backfill_extra_json` is replaced by the pre-computed ID list.

---

## Step 5: D47 — Promote 4 Guards to Full Guards

### Guard 1: `wf-backfill-extra` (`statsbomb_backfill_extra.py:25-45`)

- **Current:** `.filter("_raw_extra_json IS NULL OR ...").limit(1).count()` -> 0 or 1.
- **New:** `SELECT DISTINCT match_id FROM {table} WHERE _raw_extra_json IS NULL` -> collect as `list[str]` -> `FilterResult(count=len(ids), metadata={"new_match_ids": ids})`.
- Guard condition updated per D48 Bug 3 (IS NULL only).
- Pipeline uses `metadata["new_match_ids"]` instead of its own discovery query.

### Guard 2: `wf-backfill-360` (`statsbomb_backfill_360.py:24-56`)

- **Current:** Set difference `event_ids - three60_ids` -> IDs computed but discarded, returns `count=len(missing)`.
- **New:** Same logic, store result: `FilterResult(count=len(missing), metadata={"new_match_ids": list(missing)})`.
- Minimal change — IDs are already computed, just not passed through.

### Guard 3: `wf-entity-resolution` (`entity_resolution.py:39-55`)

- **Current:** Two `limit(1).count()` checks -> binary run/skip.
- **New:** Use `find_new_ids` pattern — compare player IDs across source tables against the existing cross-reference table. Return unresolved player IDs in metadata.
- Most substantial rewrite of the 4 — guard needs to identify which players lack cross-references.

### Guard 4: `wf-tracking-metadata` (`tracking_metadata.py:40-51`)

- **Current:** `limit(1).count()` on tracking metadata table -> binary.
- **New:** `find_new_ids(source_table=tracking_data, results_table=tracking_metadata_table)` -> returns match IDs missing metadata.
- Standard `find_new_ids` pattern, straightforward.

### Conformance changes

- Remove `wf-backfill-extra`, `wf-backfill-360`, `wf-entity-resolution`, `wf-tracking-metadata` from `_METADATA_EXEMPT` in `test_guard_conformance.py`.
- `TestGuardCountMatchesIds` (Step 1) now covers them.
- `TestGuardMetadataContract.test_real_guards_include_metadata` also kicks in.

### Pipeline consumption

Each promoted guard's `run_pipeline` already receives `filter_result` as a mandatory parameter (D40b). Where pipelines currently do their own discovery query, replace with `filter_result.metadata["new_match_ids"]`.

---

## Step 6: D46 — Orphaned Workflows + ID Mismatch Cleanup

### 6A: `wf-formations-sg` vs `wf-shape-graphs` ID mismatch

**File:** `formations_shape_graph.py`
- Guard class line 63: `workflow_id = "wf-formations-sg"` (outlier)
- `@workflow` decorator line 415: `"wf-shape-graphs"` (canonical)
- Workflow card, seed mapping, Terraform all use `"wf-shape-graphs"`.

**Fix:** Change guard's `workflow_id` to `"wf-shape-graphs"`. Freshness gate then writes task values under `wf-shape-graphs`, matching what `read_gate_result("wf-shape-graphs")` expects.

### 6B: Missing `task_workflow_mapping` seed entries

4 Terraform tasks missing from `dbt_project/seeds/task_workflow_mapping.csv`:

```csv
backfill_statsbomb_extra,wf-backfill-extra
backfill_statsbomb_360,wf-backfill-360
extract_tracking_metadata,wf-tracking-metadata
compute_xg_model_v2,wf-xg-v2
```

Ensures `fct_workflow_costs.sql` can join cost data for these tasks.

### 6C: Orphaned `export_scoutgpt_training_data`

- Has `@workflow("wf-scoutgpt-export")` and a workflow card.
- Not in `_GUARD_MODULES`, not in `hf_sync`, no Terraform task. Truly orphaned.
- ScoutGPT activation is gated on evolve framework maturity.

**Fix:** Remove the `@workflow` decorator (dead code decoration). Function works standalone via entry point. Workflow card stays as documentation. Decorator + guard added when evolve proves out and the pipeline joins the Databricks job.

### 6D: `TestWorkflowIdConsistency` (in `test_guard_conformance.py`)

New conformance test preventing future ID mismatches:
- For each module in `_GUARD_MODULES`, assert `guard.workflow_id == decorator_workflow_id`.
- Extract the decorator's workflow_id via AST inspection of the `@workflow(...)` call on the `run_pipeline` function.

---

## Step 7: D40e — Guard Parallelization

### Design

Replace sequential `for` loop in `freshness_gate.py:53` with `ThreadPoolExecutor(max_workers=4)` + `as_completed()`.

```python
_GATE_MAX_WORKERS = 4

def _check_one_guard(
    wf_id: str, guard: SkipGuard, spark: SparkSession, catalog: str, schema: str
) -> tuple[FilterResult, float]:
    """Run a single guard with timing and exception handling."""
    t0 = time.monotonic()
    try:
        result = guard.check(spark, catalog, schema)
        elapsed = time.monotonic() - t0
        return result, elapsed
    except Exception:
        logger.exception("guard_check_failed workflow_id=%s", wf_id)
        elapsed = time.monotonic() - t0
        return FilterResult(workflow_id=wf_id, count=0), elapsed
```

Main loop:
```python
with ThreadPoolExecutor(max_workers=_GATE_MAX_WORKERS) as pool:
    futures = {
        pool.submit(_check_one_guard, wf_id, guard, spark, catalog, schema): wf_id
        for wf_id, guard in guards.items()
    }
    for future in as_completed(futures):
        wf_id = futures[future]
        result, elapsed = future.result()
        results[wf_id] = result
        timings[wf_id] = elapsed
```

**Thread safety:** Dict writes happen only in the main thread (inside `for future in as_completed`). No locks needed.

**Why `max_workers=4`:** Conservative start. Serverless driver has 16 GB RAM. Guards submit Spark jobs to the cluster (cluster handles parallelism). 4 concurrent submissions cuts gate time by ~75% without overwhelming the driver. `_GATE_MAX_WORKERS` is a module-level constant, easy to tune.

### Tests

- Existing `TestRunGate` tests pass unchanged (mock guards, call `run_gate()`).
- **`test_parallel_execution_faster_than_sequential`** — 4 mock guards each sleeping 0.1s, assert total `run_gate` time < 0.25s.
- **`test_guard_exception_in_thread_does_not_crash_others`** — 1 guard raises, 3 succeed, all 4 results present.

---

## E2E Verification Plan

Full end-to-end testing using AWS, Databricks, and HuggingFace access.

### Pre-flight

1. `uv run ruff check src/ scripts/` — lint clean
2. `uv run ruff format --check src/ scripts/` — format clean
3. `uv run pyright src/` — type check clean
4. `uv run pytest src/tests/ -v` — all unit tests pass (including new TDD tests, now green)

### Databricks E2E

5. Build and upload wheel 0.3.0 to UC Volume.
6. `python scripts/ensure_warehouse.py -- echo "Warehouse ready"` — confirm warehouse RUNNING.
7. Run freshness gate standalone — verify:
   - All 33 guards execute without `guard_check_failed` (D49, D50 fixed)
   - `wf-formations-sg` ID mismatch gone (D46 — now `wf-shape-graphs` throughout)
   - Gate completes in < 2 minutes (D40e parallelization)
   - Task values written for all workflows with correct JSON + count
8. Run `backfill_statsbomb_extra` standalone — verify:
   - Guard returns actual match count (not 0/1) with entity IDs (D47)
   - Chunked MERGE succeeds per (comp_id, season_id) group (D48)
   - No silent failure — if a chunk fails, pipeline exits non-zero
   - `workflow_cost_live` shows `state="COMPLETED"` with `duration_seconds > 0`
9. Run `backfill_statsbomb_360` standalone — verify guard returns match IDs in metadata (D47).
10. Run `entity_resolution` standalone — verify guard returns player IDs in metadata (D47).
11. Run `tracking_metadata` standalone — verify guard returns match IDs in metadata (D47).

### dbt E2E

12. `dbt seed --select task_workflow_mapping` — verify 4 new rows loaded (D46).
13. `dbt build --select fct_workflow_costs` — verify join produces rows for the 4 newly-mapped tasks.

### Observability verification

14. Query `workflow_cost_live` — verify all pipeline runs from steps 8-11 have rows with `entity_count > 0`, `duration_seconds > 0`, `estimated_cost_usd > 0`.
15. Verify no `state="COMPLETED"` rows where the pipeline actually failed (D48 exception surfacing).

---

## Files Changed

| File | Changes |
|------|---------|
| `src/tests/test_guard_conformance.py` | +4 test classes: `TestCostTimeCapture`, `TestExceptionPropagation`, `TestGuardCountMatchesIds`, `TestWorkflowIdConsistency` |
| `src/tests/test_freshness_gate.py` | +1 test class: `TestFreshnessGateTaskValuePropagation`, +2 parallelization tests |
| `src/ingestion/spadl_vaep.py` | 1-line fix: `id_column="matchId"` |
| `src/ingestion/line_breaking.py` | 1-line fix: `source_filter="type = 'PASS'"` |
| `src/ingestion/defcon_lite_common.py` | Move `DefconLiteParams` import to `TYPE_CHECKING` + UDF closures |
| `src/ingestion/defcon_lite_360.py` | Inline `_TABLE_NAME`, remove `defcon_lite_common` import |
| `src/ingestion/defcon_lite_tracking.py` | Inline `_TABLE_NAME`, remove `defcon_lite_common` import |
| `src/ingestion/statsbomb.py` | Chunk MERGE by (comp_id, season_id), re-raise exceptions |
| `src/ingestion/statsbomb_backfill_extra.py` | Full guard (entity IDs), fix guard condition to IS NULL only |
| `src/ingestion/statsbomb_backfill_360.py` | Pass entity IDs through metadata |
| `src/ingestion/entity_resolution.py` | Full guard with `find_new_ids` pattern |
| `src/ingestion/tracking_metadata.py` | Full guard with `find_new_ids` pattern |
| `src/ingestion/formations_shape_graph.py` | Fix `workflow_id` to `"wf-shape-graphs"` |
| `src/ingestion/export_scoutgpt_training_data.py` | Remove orphaned `@workflow` decorator |
| `src/ingestion/freshness_gate.py` | `ThreadPoolExecutor(max_workers=4)`, extract `_check_one_guard` |
| `dbt_project/seeds/task_workflow_mapping.csv` | +4 rows |
