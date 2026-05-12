# TC-1b: applyInPandas Tracking Context — Design Spec

## Problem

TC-1a (PR #275) added column projection, staged memory release, and `for_each_task` fan-out for `compute_tracking_context`. IDSSE matches still OOM on Databricks serverless (16 GB driver, fixed) because `.toPandas()` pulls ~3.1M rows x 16 projected columns (~350 MB raw, ~1 GB in-driver after Spark overhead) to the driver for each match.

The driver never needs to see tracking data. All computation (convert, link, enrich) is per-match-per-period and embarrassingly parallel — verified by enumerating every enrichment function in the pipeline (see Appendix A).

## Solution

Replace driver-bound `.toPandas()` with `groupBy("match_id", "period").applyInPandas(udf_fn, schema=_RESULT_SCHEMA)`. Tracking data stays on executors. The driver only sees the enriched result DataFrame (orders of magnitude smaller than raw tracking).

## Architecture

Three layers, each solving a distinct problem:

1. **Preflight** (`main_preflight`): discovers matches, writes chunk task values, fits xT grid once, serializes xT as a second task value. Runs once per daily job.

2. **`for_each_task` fan-out** (Terraform, unchanged from TC-1a): Databricks runtime spawns one task per chunk string (`provider:id1,id2,...`). Each task calls `main()`.

3. **`applyInPandas` executor pipeline** (new in TC-1b): inside each `main()` invocation, Spark reads tracking + actions, groups by `(match_id, period)`, and dispatches the full enrichment pipeline (convert -> link -> enrich) to executors via `applyInPandas`. Driver never calls `.toPandas()`.

### Memory Budget

Each UDF group = one match-period. IDSSE: ~1.5M rows per period (half of ~3.1M per match) x 16 projected columns = ~350 MB input.

Peak memory inside UDF with staged `del`:

| Phase | Allocation | Cumulative peak |
|-------|-----------|----------------|
| Input tracking DataFrame | ~350 MB | 350 MB |
| Convert → frames DataFrame | ~350 MB | 700 MB |
| `del tracking_pdf` | -350 MB | 350 MB |
| Link actions to frames | ~350 MB | 700 MB |
| `del frames` | -350 MB | 350 MB |
| Enrichment result | ~50 MB | 400 MB |
| `del linked` | -350 MB | 50 MB |

**Peak: ~700 MB** after staged `del` — within the 800 MB UDF group budget with ~100 MB headroom for Python overhead and numpy temporaries.

**Respo.Vision scaling note:** at 2x IDSSE density, input alone would be ~700 MB and peak would exceed the 1 GB UDF limit. At that scale, `for_each_task` chunk sizes would need to be paired with further period subdivision (e.g., frame-range windowing). This is a future concern documented here for awareness, not a TC-1b deliverable.

**Row-count guardrail:** the UDF will log a WARNING if the input group exceeds 2M rows, providing early signal before an OOM. This is observability, not a hard gate — the enrichment proceeds regardless.

## Components

### 1. Preflight xT Serialization

`main_preflight` currently discovers matches and writes `tracking_context_chunks` task values. TC-1b adds:

- Fit `ExpectedThreat` once on the full actions table (same as today, but moved to preflight).
- Serialize the grid as a JSON-safe scalar primitive: `xt_grid.xT.tolist()` → `list[list[float]]`.
- Write a second task value `tracking_context_xt` containing `{"xt_grid": [[...]], "l": 16, "w": 12}`.

**Why grid + dimensions is sufficient:** `ExpectedThreat.rate()` and `interpolator()` read only `self.xT`, `self.l`, and `self.w`. The probability matrices (`scoring_prob_matrix`, `shot_prob_matrix`, `move_prob_matrix`) and `transition_matrix` are intermediate fit artifacts never accessed after `fit()` completes. Verified by reading `silly_kicks/xthreat.py` lines 403-468 (rate) and 343-401 (interpolator).

This follows the existing codebase pattern from `off_ball_xt.py:121`:
```python
grid_data: list[list[float]] = xt_grid.values.tolist()  # serialize as scalar primitive
```

This guarantees every iteration uses the identical xT grid — no non-determinism from per-iteration fitting.

### 2. UDF Closure Capture (Scalar Primitives)

Following the established codebase pattern (see `off_ball_xt.py:102-143`, `pausa.py:86-158`), the UDF closure captures only Python scalar primitives — no pickle, no ndarray, no DataFrame objects:

```python
def _make_tracking_context_udf(
    provider: str,
    home_team_id: str,
    home_start_left: bool,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    actions_records: list[dict[str, object]],
    native_match_id: str,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
```

- `actions_records`: `actions_pdf.to_dict("records")` — actions are small (~hundreds of rows), scalar-primitive serialization is negligible overhead.
- `xt_grid_data`: `xt.xT.tolist()` — 12x16 grid as nested list of floats (~1.5 KB).
- `home_team_id`: match-level scalar derived on the driver (see Section 5).
- `home_start_left`: direction-of-play bool derived on the driver (see Section 5). Only meaningful for IDSSE (ADR-022); Metrica/SkillCorner converters do not use it.
- All other fields are strings, ints, or bools.

The closure captures these as Python locals. Spark pickles the closure — but the closure contains only primitives, not arbitrary objects. This avoids the `pickle.loads()` security concern (CLAUDE.md "No dangerous builtins") while matching the project's established `applyInPandas` serialization convention.

**Actions duplication note:** the same actions records are captured by each period group's UDF invocation (typically N=2 periods). Since actions are ~hundreds of rows (~50 KB serialized), this duplication is negligible.

### 3. UDF Body

Inside the UDF function body, all computation modules are **lazy-imported** following the established pattern (`off_ball_xt.py:129-134`):

```python
def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
    import numpy as _np
    import pandas as _pd
    from silly_kicks.xthreat import ExpectedThreat as _ExpectedThreat
    # ... other silly-kicks imports

    # Reconstruct xT from scalar primitives
    xt = _ExpectedThreat(l=xt_l, w=xt_w)
    xt.xT = _np.array(xt_grid_data, dtype=_np.float64)

    # Reconstruct actions from records
    actions = _pd.DataFrame(actions_records)

    # ... convert, link, enrich pipeline
```

Executors have the wheel pre-installed. Imports are fast (~0 cost). No internet access needed (serverless constraint).

The full pipeline inside the UDF:

1. Reconstruct xT and actions from scalar primitives.
2. Route to provider-specific converter (`_convert_idsse`, `_convert_metrica`, `_convert_skillcorner`).
3. **`del pdf`** — free input tracking DataFrame after conversion.
4. Link actions to frames via `link_actions_to_frames`.
5. **`del frames`** — free converted frames after linking.
6. Run enrichment pipeline (all silly-kicks action-coupled features + xT).
7. **`del linked`** — free linked DataFrame after enrichment.
8. Return enriched DataFrame matching `_RESULT_SCHEMA`.

Hard-fail-first per ADR-002 SS5: `raise RuntimeError(f"tracking_context UDF failed for match_id={match_id}, period={period}") from exc`.

### 4. Provider Routing Inside UDF

The UDF receives a `provider` string via closure capture. Inside the UDF body:

```python
if provider == "idsse":
    frames = _convert_idsse(actions, pdf, home_team_id, home_start_left)
elif provider == "metrica":
    frames = _convert_metrica(actions, pdf)
elif provider == "skillcorner":
    frames = _convert_skillcorner(actions, pdf, home_team_id)
else:
    raise ValueError(f"Unknown provider: {provider}")
```

**Provider-specific data dependencies** (all resolved on the driver as match-level scalars before UDF dispatch):

- **IDSSE**: `home_team_id` and `home_start_left` are derived from `bronze.idsse_events` (per ADR-022). The driver reads the events table (small — ~hundreds of rows per match), calls `adapt_idsse_events_for_silly_kicks` + `derive_idsse_home_team_start_left`, and passes both as scalar primitives. The IDSSE converter also calls `_bronze_idsse_to_sportec_input(trk_pdf)` — a row-level rename + synthetic-ball-row operation that works correctly on per-period data (no cross-row aggregation).
- **Metrica**: `home_team_id = "Home"` (hardcoded convention — Metrica tracking uses anonymous team labels). No external data dependency.
- **SkillCorner**: `home_team_id` derived from tracking data. Since the driver no longer calls `.toPandas()` on tracking, this is resolved via a one-row Spark query: `spark.table(...).filter(...).select("home_team_id").limit(1).collect()[0][0]`. Trivial, no memory concern.

Existing `_convert_*` helpers are already pure-pandas functions. They move into the UDF unchanged. The converter functions themselves must be **re-imported inside the UDF** (or defined at module level and captured via closure — either works since they're pure functions with no mutable state).

### 5. Spark Orchestration (per-iteration `main()`)

For each match in the chunk:

1. Read tracking DataFrame with column projection (unchanged from TC-1a). Keep as Spark DataFrame — do NOT call `.toPandas()`.
2. Read actions DataFrame, filter to match, collect to driver via `.toPandas()` (small — hundreds of rows, well within driver memory). Convert to `list[dict]` via `.to_dict("records")`.
3. **Resolve match-level metadata on the driver** (all small queries):
   - **IDSSE**: read `bronze.idsse_events` for this match (`.toPandas()`, ~hundreds of rows). Derive `home_team_id` from `home_team_id_native` and `home_start_left` via `derive_idsse_home_team_start_left`. Delete events DataFrame.
   - **Metrica**: `home_team_id = "Home"` (hardcoded). `home_start_left` derived from period-1 shot positions via `derive_metrica_home_team_start_left` (already computed in the existing pipeline).
   - **SkillCorner**: `home_team_id` via one-row Spark query: `spark.table(...).filter(...).select("home_team_id").limit(1).collect()[0][0]`. `home_start_left` not used by SkillCorner converter.
4. Build UDF via `_make_tracking_context_udf(provider, home_team_id, home_start_left, xt_grid_data, xt_l, xt_w, actions_records, native_match_id)`.
5. Call `tracking_df.groupBy("match_id", "period").applyInPandas(udf_fn, schema=_RESULT_SCHEMA)`.
6. Write result to Delta via `write_delta_table()`.

The driver handles only actions (small), match-level metadata (scalars), and orchestration. Tracking data stays on executors throughout.

### 6. Result Schema

`_RESULT_SCHEMA` is a module-level `StructType` constant derived from the existing Delta table schema for `tracking_context_results`. It contains all enrichment output columns (the full set produced by the current pipeline — no additions or removals in TC-1b).

The implementation will derive the exact column list from the existing `_enrich_match` return signature and the live table schema.

### 7. Legacy Mode Removal and Observability

`run_pipeline()` (the `@workflow`-decorated function) and the `else` branch in `main()` are dead code post-fan-out. TC-1b removes them.

**Observability disposition:** The `@workflow` decorator provides structured event logging (`workflow_start`, `workflow_complete`), `run_id` correlation, and lifecycle hook dispatch (cost estimation, guard timing). With the `for_each_task` architecture:

- **Per-iteration observability** is handled by Databricks' native `for_each_task` monitoring: each iteration gets its own task run with start/end timestamps, duration, exit status, and logs — all visible in the Jobs UI and accessible via the Runs API.
- **Guard timing** is already captured by `timed_check()` in `main_preflight`, which runs independently of the `@workflow` decorator.
- **Structured logging** in `main()` already provides match counts, provider, and row counts via the configured JSON logger.

What is lost: `run_id` UUID correlation across log lines within a single iteration, and cost hook dispatch. The `run_id` loss is acceptable because each `for_each_task` iteration already has a unique Databricks task run ID. Cost hooks operate at the job level, not the per-iteration level, so they are unaffected by removing the per-iteration `@workflow` wrapper.

If cost-hook integration at the iteration level becomes needed in the future, `main()` can add explicit `WorkflowContext` creation without re-introducing `run_pipeline()`.

## Error Handling

- **UDF failures**: hard-fail-first per ADR-002 SS5. `raise RuntimeError(f"tracking_context UDF failed for match_id={match_id}, period={period}") from exc`.
- **DAS (accessible-space) exception**: continues to be caught and logged at WARNING level inside the enrichment pipeline (pre-existing behavior, not changed by TC-1b). DAS is optional enrichment — its failure does not invalidate core tracking context.
- **Empty tracking data**: if a match-period group has zero rows after projection, the UDF returns an empty DataFrame matching `_RESULT_SCHEMA`. This is not an error — some periods may have no tracking data.
- **xT deserialization**: if the task value JSON is malformed, `json.loads` / `np.array` raises immediately — no silent fallback.

## Testing

### New Tests (`test_tracking_context_udf.py`)

1. **xT round-trip**: fit `ExpectedThreat`, serialize grid via `.tolist()`, reconstruct, assert `np.allclose` with original grid.
2. **Actions round-trip**: create synthetic actions DataFrame, serialize via `.to_dict("records")`, reconstruct, assert equality.
3. **UDF closure pickling**: build closure via `_make_tracking_context_udf` with synthetic data, pickle/unpickle, assert callable.
4. **UDF output schema**: call UDF factory with minimal synthetic data, verify output DataFrame columns match `_RESULT_SCHEMA` field names.

### Updated Tests

- `test_tracking_context_preflight.py`: add test for xT task value presence and structure (nested list + l/w integers).

### Unchanged Tests

- `test_tracking_context_column_projection.py`: projection constants unchanged.
- `test_workflows_tf_ordering.py`: Terraform unchanged, anchor count stays 33.
- `test_card_parity_with_terraform.py`: no new TF tasks.

## File Scope

| File | Change |
|------|--------|
| `src/ingestion/tracking_context.py` | Major refactor: add UDF factory with scalar-primitive closure capture, result schema; refactor `_process_*` to use `applyInPandas` with staged `del`; extend `main_preflight` with xT serialization; simplify `main()`; remove `run_pipeline()` |
| `src/tests/test_tracking_context_udf.py` | New: xT round-trip, actions round-trip, closure pickling, UDF schema tests |
| `src/tests/test_tracking_context_preflight.py` | Update: add xT task value test |

No changes to Terraform, pyproject.toml, seeds, or workflow cards.

## Non-Goals

- Changing the `for_each_task` fan-out structure (TC-1a, working).
- Changing column projection constants (TC-1a, working).
- Changing chunk sizes per provider.
- Adding new enrichment features (separate PRs via silly-kicks TF-* cycle).
- Modifying the Delta table schema (output columns unchanged).
- Sub-period windowing for Respo.Vision-scale data (future concern, documented in Memory Budget).

---

## Appendix A: Per-Period Decomposability Verification

Every enrichment function called inside the tracking context pipeline was verified to operate correctly on single-period data. The key invariant: all functions group by `(game_id, period_id, frame_id)` explicitly, and none compute cross-period aggregates.

| Function | Per-period safe? | Evidence |
|----------|-----------------|----------|
| `link_actions_to_frames` | YES | Filters by `period_id` before `merge_asof` (utils.py:150) |
| `add_pre_shot_gk_context` | YES | Frame-local GK position lookup |
| `add_action_context` | YES | Per-action-at-frame spatial metrics |
| `add_actor_pre_window` | YES | `slice_around_event` temporal window, period-bounded |
| `add_pressure_on_actor` | YES | Per-frame geometry (Andrienko oval, Bekkers pi) |
| `pitch_control_at_action` | YES | `groupby(['period_id', 'frame_id'])` (features.py:1680) |
| `add_defensive_line` | YES | `groupby(['game_id', 'period_id', 'frame_id', 'team_id'])` (_defensive_line.py:177) |
| `add_off_ball_context` | YES | `slice_around_event` temporal window, period-bounded |
| `add_line_break` (ward) | YES | Per-frame opponent x-coordinate clustering |
| `add_team_shape` | YES | `groupby(['game_id', 'period_id', 'frame_id'])` (features.py:1379) |
| `add_das` | YES | Per-frame `accessible_space` simulation |
| `add_gk_influence` | YES | Per-frame pitch control share/reachable area |
| `add_cover_shadows` | YES | Per-action-frame blocking score |
| `add_sync_score` | YES | Per-period linkage via `link_actions_to_frames` |

No function computes match-level baselines, rolling aggregates, or cross-period statistics. Single-period execution produces identical output to extracting that period's rows from a full-match run.
