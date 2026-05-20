# OPT-3: Spark UDF + Driver-OOM Hardening

**Origin**: Optimization audit Cycle E (`memory/project_optimization_audit_cycles_b_to_f.md`).
**TODO ref**: OPT-3 row in `TODO.md`.

## 1. Problem Statement

Three `applyInPandas` pipelines carry latent memory/serialization risks flagged by the late-April optimization audit:

| ID | Pipeline | File | Risk | Empirical evidence |
|----|----------|------|------|-------------------|
| (c) | xG v2 scoring | `src/ingestion/xg_model_v2.py` | **High** | `groupBy("competition_id")` sends up to 21,186 shots (competition 11) into one executor group. Growth is unbounded (linear with seasons ingested). Each shot carries a `shot_freeze_frame` JSON blob. Groups can reach 50-200 MB today, approaching the 800 MB serverless cap. |
| (a) | DEFCON-lite | `src/ingestion/defcon_lite_360.py`, `defcon_lite_tracking.py`, `defcon_lite_common.py` | **Low** | Pre-join `.select()` projections are already in place. No performance issue today, but no compile-time or test-time guard prevents column-list drift between the join output and what the UDF actually reads. Same LL1 latent-bug class as PR #230. |
| (b) | SPADL/VAEP scoring | `src/ingestion/spadl_vaep.py` | **None** | `groupBy("match_id", "data_source")` produces max 3,236 rows/group (~1 MB). Verified safe. No production code change needed — add a `tracemalloc` regression guard. |

### Empirical data (queried 2026-05-19 from `soccer_analytics` production tables)

**xg_model_v2 — shots per competition_id (top 5)**:

| competition_id | shot_count | match_count |
|---|---|---|
| 11 | 21,186 | 867 |
| 2 | 10,837 | 418 |
| 7 | 10,346 | 435 |
| 12 | 10,033 | 381 |
| 524 | 8,805 | 380 |

**SPADL — rows per (match_id, data_source) group**:

| data_source | matches | max | mean | p95 | p99 |
|---|---|---|---|---|---|
| statsbomb | 3,463 | 3,236 | 2,065 | 2,496 | 2,711 |
| wyscout | 1,941 | 1,815 | 1,271 | 1,454 | 1,567 |
| idsse | 7 | 1,364 | 1,204 | 1,338 | 1,359 |
| skillcorner | 10 | 1,283 | 1,178 | 1,274 | 1,281 |
| metrica | 3 | 2,217 | 2,053 | 2,200 | 2,214 |

**DEFCON — defcon_results rows per match**: 323 matches, max 5,501, mean 2,565, p95 3,938, p99 4,693. statsbomb_360 rows per match: max 80,650, mean 48,248.

**Table column counts**: fct_action_values 53, fct_shots 29, statsbomb_360 10, fct_tracking_frames 29.

## 2. Design

### 2.1 Sub-item (c): xg_model_v2 regrouping (HIGH priority)

**Root cause**: `groupBy("competition_id")` was a convenience choice, not a semantic one. The v2 UDF scores each shot independently (freeze frame -> encode -> predict). No cross-shot or cross-match dependency exists.

**Fix**: Regroup to `match_key` (BIGINT surrogate from Kimball migration). Each match has 25-50 shots -- permanently bounded regardless of how many seasons get ingested.

**Changes to `src/ingestion/xg_model_v2.py`**:

1. **`_load_shots_with_context()`**: Add `s.match_key` to the SELECT list.

2. **`run_pipeline()`**: Replace `groupBy("competition_id")` with `groupBy("match_key")`.

3. **Remove temp table materialization**: The `_xg_v2_scored_temp` intermediate table (lines 439-441) exists because the per-competition `.filter()` loop (lines 443-453) re-triggers the UDF DAG. With match-level grouping, replace the per-competition write loop with a single bulk write:
   ```python
   new_comp_list = ", ".join(str(c) for c in new_comps)
   write_delta_table(
       scored_df, catalog, schema, _TABLE_NAME,
       replace_where=f"competition_id IN ({new_comp_list})",
       logger=logger,
   )
   ```

4. **Remove temp table cleanup** (lines 455-462).

5. **Defense-in-depth NULL match_key guard**: Spark `groupBy` puts NULLs into one shared group (doesn't drop them), so a broken dbt surrogate key invariant would silently recreate the exact OOM this spec eliminates. Add after `shots_filtered`:
   ```python
   null_count = shots_filtered.where("match_key IS NULL").count()
   if null_count > 0:
       logger.error("match_key IS NULL for %d shots -- invariant broken", null_count)
       raise RuntimeError(f"{null_count} shots have NULL match_key")
   ```
   Cost: 3 lines. The dbt macro guarantees non-NULL today, but defense-in-depth prevents silent reintroduction of the OOM class.

**UDF unchanged**: `_make_v2_scoring_udf` receives `competition_id` in the input pdf (from the SELECT) and emits it in the output (line 324). No UDF code changes. `applyInPandas` does NOT strip non-groupBy columns from the input DataFrame -- the UDF receives all columns from the SELECT, regardless of which column is the groupBy key. A new unit test (see Verification Strategy) must confirm `competition_id` is preserved in per-match group output.

**Output schema unchanged**: `shot_id STRING, competition_id INT, xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE`. `match_key` is a grouping key only, not in the output.

**Idempotency unchanged**: `replaceWhere` still keys on `competition_id`. The single bulk write is MORE atomic than the per-competition loop -- either the whole write succeeds or fails. On failure, the guard re-discovers the same competition set on retry.

**Module docstring update**: Line 6 says "grouped by `competition_id`" -- update to "grouped by `match_key`" to prevent stale-docstring drift.

**What this eliminates**:
- `_xg_v2_scored_temp` intermediate table (workaround for DAG re-execution)
- Per-competition write loop (~10 lines)
- Temp table cleanup try/except (~7 lines)

**Net**: ~5 lines added, ~25 lines removed.

### 2.2 Sub-item (a): DEFCON input projection hardening (LOW priority)

**Root cause**: No module-level column contract between the join output and what each UDF closure actually reads. Pre-join `.select()` calls are already correct, but nothing prevents drift.

**Changes across 3 files**:

1. **`defcon_lite_common.py`**: Add module-level constant `_VALUE_UDF_INPUT_COLS` listing the 18 columns Pass 2 receives from Pass 1 output.

2. **`defcon_lite_360.py`**:
   - Add module-level constant `_CREDITS_UDF_INPUT_COLS_360` listing the columns the Pass 1 UDF reads from the joined DF.
   - Add `.select(*_VALUE_UDF_INPUT_COLS)` before the Pass 2 `groupBy("match_id").applyInPandas(...)`.
   - The Pass 1 input is already projected via the pre-join `.select()` calls. Add `.select(*_CREDITS_UDF_INPUT_COLS_360)` before the Pass 1 `groupBy` for explicitness.

3. **`defcon_lite_tracking.py`**: Mirror the same pattern with `_CREDITS_UDF_INPUT_COLS_TRACKING`.

4. **New parity test**: Assert each module-level column constant matches the corresponding `StructType` field names. Same pattern as `test_spadl_vaep_writer_parity.py`.

**Net**: ~40 lines added, 0 removed.

### 2.3 Sub-item (b): spadl_vaep tracemalloc smoke test (regression guard)

**No production code changes.** One new test file.

**`src/tests/test_spadl_vaep_memory.py`**:
- Build a synthetic SPADL DataFrame at p99 size (2,711 rows) with realistic column count (~40 cols matching `_SPADL_SCHEMA`).
- Instantiate the scoring UDF with real XGBoost model bytes from a small pre-trained fixture.
- Run the UDF body under `tracemalloc`, capture peak allocation.
- **Empirical threshold**: Measure actual peak memory FIRST, then set the assertion at `measured_peak * 2` (100% headroom). Document the measured baseline as a comment in the test. This prevents both false positives (threshold too tight) and silent regressions (threshold too loose). The 800 MB cap is shared with Spark overhead, Python runtime, and model cache across concurrent groups on one executor -- the per-UDF inner budget must leave room for all of these.

**Net**: ~40 lines added.

## 3. Verification Strategy

### Pre-change baselines

Capture `pytest-benchmark` baselines before any code changes:
- `_make_v2_scoring_udf` with a 50-shot synthetic group (one match)
- `_make_credits_udf_360` with a synthetic match group
- `_make_values_udf` with a synthetic credits group

### Post-change verification

1. All existing tests pass (`uv run pytest src/tests/ -v`)
2. Benchmarks at-or-below baseline (no regression)
3. `tracemalloc` smoke test passes
4. Column parity tests pass
5. **Regrouping correctness test** (new, replaces manual row-count verification):
   - Build synthetic `fct_shots` DataFrame: 3 competitions, 2 matches each, ~5 shots per match (30 shots total)
   - Run through `groupBy("match_key").applyInPandas(scoring_udf, schema=output_schema)`
   - Assert: output row count == input row count (no shots dropped or duplicated)
   - Assert: every `competition_id` from input appears in output (non-groupBy column preserved)
   - Assert: no two output rows share the same `shot_id` (uniqueness preserved)
   - Assert: `competition_id` values in output match input per shot (not shuffled)
   - This is a permanent regression guard, not a one-shot manual check
6. Manual daily-job trigger pre-merge (supplementary, not primary): run `compute_xg_v2` task via `w.jobs.run_now(job_id=302697362345215, only=["compute_xg_v2"])`, verify `xg_predictions_v2` row counts match pre-change counts per competition

## 4. Scope Boundaries

**In scope**:
- `src/ingestion/xg_model_v2.py` (regrouping + temp table removal)
- `src/ingestion/defcon_lite_common.py` (column constants)
- `src/ingestion/defcon_lite_360.py` (projection + constants)
- `src/ingestion/defcon_lite_tracking.py` (projection + constants)
- New test files (benchmarks, parity, tracemalloc)

**Not in scope**:
- No Terraform changes (no task topology changes)
- No dbt changes
- No workflow card changes (same `wf-xg-v2`, `wf-defcon` workflow IDs)
- No entry point changes (no wheel version bump required by the code changes themselves -- user decides)
- No `spadl_vaep.py` production code changes

## 5. Risk Assessment

| Risk | Mitigation |
|------|-----------|
| xg_model_v2 row count mismatch after regrouping | Deterministic regrouping correctness test (row count, competition_id preservation, shot_id uniqueness) + supplementary manual job trigger |
| competition_id stripped by applyInPandas on non-groupBy column | Unit test calling scoring UDF with per-match group, asserting competition_id in output. Note: applyInPandas passes ALL input columns to the UDF regardless of groupBy key -- but the test proves this empirically. |
| DEFCON column constant drifts from StructType | Parity test (same pattern as `test_spadl_vaep_writer_parity.py`) |
| Benchmark regression | `pytest-benchmark` baseline captured before changes |
| match_key not populated for non-StatsBomb shots | `_load_shots_with_context` LEFT JOINs to stg_statsbomb__events; match_key comes from fct_shots (always populated via dbt surrogate key macro). A NULL match_key would silently drop shots from the UDF -- load-bearing invariant. |
| tracemalloc threshold too loose or too tight | Measure actual peak before setting threshold; assert at 2x measured baseline with documented derivation |
| Stale module docstring | Update xg_model_v2.py line 6 docstring from "grouped by competition_id" to "grouped by match_key" |

## 6. File Change Summary

| File | Change | Lines +/- |
|------|--------|-----------|
| `src/ingestion/xg_model_v2.py` | Regroup to `match_key`, remove temp table, update docstring | +5 / -25 |
| `src/ingestion/defcon_lite_common.py` | `_VALUE_UDF_INPUT_COLS` constant | +10 / 0 |
| `src/ingestion/defcon_lite_360.py` | `_CREDITS_UDF_INPUT_COLS_360` + `.select()` | +15 / 0 |
| `src/ingestion/defcon_lite_tracking.py` | `_CREDITS_UDF_INPUT_COLS_TRACKING` + `.select()` | +15 / 0 |
| `src/tests/test_defcon_projection_parity.py` | Column constant vs StructType parity | +30 / 0 |
| `src/tests/test_spadl_vaep_memory.py` | tracemalloc smoke test (empirically-derived threshold) | +40 / 0 |
| `src/tests/test_xg_v2_regrouping.py` | Regrouping correctness: row count, competition_id preservation, shot_id uniqueness | +50 / 0 |
| `src/tests/test_xg_v2_benchmark.py` (or extend existing) | pytest-benchmark baselines | +30 / 0 |
| `src/tests/test_defcon_benchmark.py` (or extend existing) | pytest-benchmark baselines | +30 / 0 |
| **Total** | | **~225 / -25** |
