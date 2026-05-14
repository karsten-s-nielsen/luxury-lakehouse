# TC-1c: Tracking Context Enrichment Reliability

## Goal

Fix three production bugs (DAS 0% fill rate, Metrica UDF crash, silent error swallowing) and eliminate 30-70s/match of redundant computation in `_enrich_match`, delivering correct and performant tracking context enrichment for IDSSE and Metrica.

## Context

PR #278 fixed identity resolution and match_id projection bugs. PR #279 + #280 fixed the skip guard SPADL intersection. After deploying, the first real enrichment run revealed:

1. **DAS columns are 100% NULL** across all completed matches. `add_das` requires a `team_in_possession` column on frames that `_enrich_match` never provides. silly-kicks 3.13.0's `add_das` catches the resulting `ValueError` internally, emits a `UserWarning` (invisible on Databricks), and returns NaN. This violates ADR-002 (warning-level logs are invisible in error-log queries).

2. **Metrica matches crash** with `RuntimeError: tracking_context UDF failed for match_id=Sample_Game_1, period=1, frame_batch_id=15`. The actual exception is hidden behind the UDF wrapper's `raise RuntimeError(...) from exc` chain, which Databricks truncates in the UI.

3. **IDSSE matches timeout** at 30 minutes. Each of the 13 enrichment steps that accept `links` internally calls `link_actions_to_frames` (~2-5s each on 3000 actions x 150k frames = 25-65s/match of pure re-linking overhead). silly-kicks 3.13.0 adds an optional `links` kwarg to all 13 tracking enrichment functions, enabling pre-link-once.

## Prerequisites

- silly-kicks 3.13.0 (already released): `links` kwarg on all 13 tracking enrichment functions
- silly-kicks pin already bumped from `>=3.11.3,<4` to `>=3.13.0,<4` (applied on this branch before spec was written)

## Deployment Strategy

Changes are deployed in **two stages** to preserve Metrica diagnostic fidelity:

**Stage 1 (diagnostic):** Deploy change #2 (UDF ERROR logging) alone. Trigger one Metrica run. Capture the baseline error log with the actual exception type and message. This guarantees the original root cause is recorded before any other changes alter the failure mode.

**Stage 2 (fixes + optimization):** Deploy changes #1 and #3 (DAS prerequisite + pre-link optimization). The DAS fix adds a `team_in_possession` column to frames, which could alter the Metrica failure mode. By capturing the baseline error in Stage 1, the original root cause is on record regardless.

This costs one extra partial run and one extra wheel version but eliminates the diagnostic blind spot where deploying all changes together could mutate the Metrica error before it's ever captured.

**Operational sequence:**

1. Create branch. Apply change #2 (ERROR logging) + wheel bump to 0.3.51. Commit, push, wait for CI.
2. Trigger `compute_tracking_context` (Metrica only if possible, otherwise full). Capture ERROR log.
3. Apply changes #1 + #3 (DAS prerequisite + pre-link) + wheel bump to 0.3.52. Commit, push, wait for CI.
4. Trigger full `preflight_tracking_context` + `compute_tracking_context`. Validate DAS fill rate + IDSSE timing.
5. Merge PR after Stage 2 validation passes.

Two wheel bumps are intentional -- Stage 1 must be a deployable artifact with only the logging change, so the Metrica baseline error is captured against code that hasn't modified the frames DataFrame.

## Changes

### 1. DAS prerequisite: derive team_in_possession

**File:** `src/ingestion/tracking_context.py` (`_enrich_match`)

Before calling `add_das` (step 12), derive `team_in_possession` on the frames DataFrame:

```python
from silly_kicks.tracking import infer_ball_carrier, derive_team_in_possession

carrier = infer_ball_carrier(frames)
frames = derive_team_in_possession(frames, carrier)
del carrier
```

`infer_ball_carrier` performs a nearest-player scan with hysteresis (~1-2s on 150k frames). `derive_team_in_possession` is a lightweight left-merge (one row per frame). The `carrier` DataFrame is discarded after the merge to conserve executor memory.

**Fault tolerance:** `infer_ball_carrier` is designed to never raise on edge cases. Empty frames return an empty DataFrame; NaN ball positions, no players within tolerance, and dead-ball frames all produce NaN carrier rows. `derive_team_in_possession` fills unmatched frames with NaN `team_in_possession`. When `team_in_possession` is NaN for a frame, `add_das` degrades that frame's DAS values to NaN -- the same end result as today's broken behavior, but via explicit data flow rather than swallowed exceptions.

**Defense-in-depth:** The entire ball-carrier + DAS chain is wrapped in a single `try/except` block that degrades all three DAS columns to NaN on any unexpected failure. This replaces the existing narrow `except IndexError` on `add_das` alone:

```python
try:
    carrier = infer_ball_carrier(frames)
    frames = derive_team_in_possession(frames, carrier)
    del carrier
    actions = add_das(actions, frames, links=links)
except (IndexError, ValueError, RuntimeError) as exc:
    logger.error(
        "DAS degraded to NaN for match_id=%s: %s: %s",
        match_id_native, type(exc).__name__, exc,
    )
    actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

The catch list is explicit: `IndexError` (DAS geometry edge cases in `accessible-space`), `ValueError` (missing prerequisite columns -- should not happen after the fix, but defense-in-depth), `RuntimeError` (unexpected `accessible-space` failures). All other exceptions propagate to the UDF wrapper per ADR-002 SS5. The ERROR log ensures every degradation is visible in Databricks structured log queries -- no silent swallows.

**Logger access:** `_enrich_match` is a module-level function called from the UDF closure on Spark executors. The module already imports `logging` (line 13) but lacks a module-level logger. Add `logger = logging.getLogger(__name__)` at module level. stdlib `logging.getLogger` is process-local and works identically on driver and executor -- no serialization concern.

**Placement:** After step 11 (team shape), before step 12 (DAS). The `team_in_possession` column is only consumed by `add_das` -- no other enrichment step reads it, so placement after all other steps that modify frames is safe.

**Permanence:** The `infer_ball_carrier` + `derive_team_in_possession` call is **permanent**, not a workaround. DAS requires ball-possession data by design -- the lakehouse must derive it from tracking frames regardless of how silly-kicks handles error reporting. When silly-kicks PR-S37 ships (splitting `add_das` error handling), the lakehouse derivation remains as the data-supply path; PR-S37 only changes what happens when the data is absent.

### 2. UDF error logging

**File:** `src/ingestion/tracking_context.py` (UDF wrapper `_udf` closure)

In the `except Exception as exc` block (line 470), log the actual exception at ERROR level before re-raising. The `_logger` is already defined at line 368 of the closure (`_logger = _logging.getLogger("tracking_context_udf")`), so no new import is needed:

```python
except Exception as exc:
    _logger.error(
        "UDF failed for match_id=%s, period=%s, batch=%s: %s: %s",
        match_id_val, period_val, batch_id_val,
        type(exc).__name__, exc,
    )
    raise RuntimeError(
        f"tracking_context UDF failed for match_id={match_id_val}, "
        f"period={period_val}, frame_batch_id={batch_id_val}"
    ) from exc
```

This ensures the actual exception type and message appear in Databricks structured JSON logs, which are queryable via the observability schema. The `from exc` chain is preserved for local debugging, but the ERROR log is the primary surfacing mechanism on Databricks.

### 3. Pre-link once, pass links to all enrichment steps

**File:** `src/ingestion/tracking_context.py` (`_enrich_match`)

Currently, `_enrich_match` calls `link_actions_to_frames` at step 0 for sync_score, but every subsequent enrichment step re-links internally. With silly-kicks 3.13.0's `links` kwarg, pre-link once and pass to all 13 tracking enrichment functions:

```python
# Step 0: Link actions to frames (single call, reused by all steps)
links, _report = link_actions_to_frames(actions, frames)

# Step 1: GK resolution (events + tracking) -- no links kwarg (spadl.utils, not tracking)
actions = add_pre_shot_gk_context(actions, frames=frames)

# Step 2: Action context
actions = add_action_context(actions, frames, links=links)

# Step 3: Actor pre-window
actions = add_actor_pre_window(actions, frames, links=links)

# Step 4: Pressure (all 3 methods)
actions = add_pressure_on_actor(actions, frames, links=links,
    methods=("andrienko_oval", "link_zones", "bekkers_pi"))

# Steps 5-7: Pitch control (3 methods)
for method in ("spearman", "fernandez_bornn", "voronoi"):
    s = pitch_control_at_action(actions, frames, links=links, method=method)
    actions[s.name] = s.values

# Step 8: Defensive line
actions = add_defensive_line(actions, frames, links=links, home_team_id=home_team_id)

# Step 9: Off-ball context
actions = add_off_ball_context(actions, frames, links=links, home_team_id=home_team_id)

# Step 10: Ward line-breaking
actions = add_line_break(actions, frames, links=links, method="ward", home_team_id=home_team_id)

# Step 11: Team shape
actions = add_team_shape(actions, frames, links=links, home_team_id=home_team_id)

# Step 12: DAS (with ball-carrier prerequisite, defense-in-depth wrapper)
try:
    carrier = infer_ball_carrier(frames)
    frames = derive_team_in_possession(frames, carrier)
    del carrier
    actions = add_das(actions, frames, links=links)
except (IndexError, ValueError, RuntimeError) as exc:
    logger.error(
        "DAS degraded to NaN for match_id=%s: %s: %s",
        match_id_native, type(exc).__name__, exc,
    )
    actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan

# Step 13: GK influence
actions = add_gk_influence(actions, frames, xt, links=links, home_team_id=home_team_id)

# Step 14: Cover shadows
actions = add_cover_shadows(actions, frames, xt, links=links, home_team_id=home_team_id)

# Step 15: Sync score (links is positional, already pre-linked)
actions = add_sync_score(actions, links)
```

**Functions receiving `links=` kwarg (13 total):**

| Step | Function | Returns | `links` type |
|------|----------|---------|-------------|
| 2 | `add_action_context` | DataFrame | optional kwarg |
| 3 | `add_actor_pre_window` | DataFrame | optional kwarg |
| 4 | `add_pressure_on_actor` | DataFrame | optional kwarg |
| 5-7 | `pitch_control_at_action` (x3) | Series | optional kwarg |
| 8 | `add_defensive_line` | DataFrame | optional kwarg |
| 9 | `add_off_ball_context` | DataFrame | optional kwarg |
| 10 | `add_line_break` | DataFrame | optional kwarg |
| 11 | `add_team_shape` | DataFrame | optional kwarg |
| 12 | `add_das` | DataFrame | optional kwarg |
| 13 | `add_gk_influence` | DataFrame | optional kwarg |
| 14 | `add_cover_shadows` | DataFrame | optional kwarg |

**Not receiving `links=`:**
- Step 1: `add_pre_shot_gk_context` -- imported from `silly_kicks.spadl.utils`, not a tracking enrichment function. Does not call `link_actions_to_frames` internally.
- Step 15: `add_sync_score` -- `links` is a required positional argument (already pre-linked since v1).

**Expected speedup:** 13 x 2-5s re-link calls eliminated. ~25-65s saved per match. At 20 matches across a daily run, ~8-20 minutes saved. This should bring IDSSE matches well under the 30-minute timeout.

### 4. Metrica failure investigation

The Metrica crash at `frame_batch_id=15` has an unknown root cause -- the chained exception is truncated in the Databricks UI. With change #2 (ERROR logging) deployed in Stage 1, the next run will surface the actual exception type and message in structured logs.

**Strategy:** Deploy Stage 1 (change #2 only), trigger a Metrica run, read the ERROR log. If the root cause is in silly-kicks, file PR-S37. If it's in the lakehouse converter or enrichment chain, fix in a hotfix commit on this same PR branch.

**No speculative fix.** The diagnostic-first approach avoids wasting cycles on fixes for the wrong root cause.

### 5. Wheel bump

**Files:** `pyproject.toml`, `src/shared/wheel.py`, 26 consumer files via `bump_wheel.py`

- silly-kicks pin: already applied (`>=3.13.0,<4`)
- Wheel: 0.3.50 -> 0.3.51 (or next available)
- Propagate via `uv run python scripts/bump_wheel.py`

## Testing

### Unit tests

1. **DAS with team_in_possession:** Test that `_enrich_match` with synthetic data produces non-NaN `das_team`/`das_opponent`/`das_diff` columns. Extend `test_tracking_context_enrichment.py::TestEnrichmentChain` -- the existing `test_output_columns_match_spec` uses live silly-kicks and will now exercise the ball-carrier inference path.

2. **Pre-link call count:** Monkey-patch `link_actions_to_frames` to count invocations, call `_enrich_match`, assert it is called exactly once. This verifies the purpose of pre-linking (single link pass) without coupling the test to the exact list of enrichment functions -- adding, removing, or reordering enrichment steps won't break it.

3. **UDF error logging:** Test that the UDF wrapper logs at ERROR level when an enrichment step raises. Mock `_enrich_match` to raise `ValueError("test")`, call the UDF, assert the ERROR log contains the exception type and message.

### Integration validation (post-deploy)

**Stage 1 (after change #2 only):**
1. Trigger `preflight_tracking_context` + `compute_tracking_context`
2. Read Metrica ERROR log for actual exception type and message
3. Record the root cause before proceeding to Stage 2

**Stage 2 (after changes #1 + #3):**
1. Trigger `preflight_tracking_context` + `compute_tracking_context`
2. Query `bronze.spadl_tracking_context` for non-NULL `das_team` rate (expect >0%)
3. Check Metrica matches complete or surface a clear ERROR log
4. Verify IDSSE matches complete within 15 minutes (was timing out at 30)

## Future work (out of scope)

- **silly-kicks PR-S37:** Split `add_das` error handling -- raise `ValueError` (missing prerequisite), log `RuntimeError`/`ImportError` at ERROR level instead of `warnings.warn`. This is the long-term fix for the silent swallowing pattern. Once PR-S37 ships, the lakehouse `infer_ball_carrier` + `derive_team_in_possession` call remains as the data-supply path (DAS needs possession data by design), but the lakehouse's defense-in-depth `except (IndexError, ValueError, RuntimeError)` block can be narrowed to `except IndexError` only.
- **TC-2 further optimization:** If performance is still insufficient after pre-linking, investigate frame sub-selection (only pass frames within the action time window to enrichment steps, not the full batch).

## Risks

- `infer_ball_carrier` adds ~1-2s per batch. Offset by ~25-65s savings from pre-linking. Net improvement.
- `derive_team_in_possession` adds a column to frames. pandas object-dtype string column costs ~8 bytes/row (pointer) plus string heap. For 150k frames with a small number of distinct team_id values, total ~1.5 MB. Negligible vs 800 MB budget.
- Metrica root cause remains unknown until ERROR logging is deployed (Stage 1). Accepted -- the staged deployment ensures the baseline error is captured before DAS/pre-link changes could alter the failure mode.
