# TC-1d: Tracking Context OOM + Bekkers Pressure Fix

## Goal

Fix two production bugs that cause 100% failure rate on all tracking context enrichment iterations:
1. IDSSE OOM caused by accessible-space DAS computing on all 5000 frames per batch
2. Metrica crash from `bekkers_pi` requiring ball rows that may be absent

## Context

PR #282 (TC-1c, wheel 0.3.53) deployed three changes: `traceback.format_exc()` in UDF error handler, `infer_ball_carrier` + `derive_team_in_possession` before `add_das`, and `links=` kwarg pre-linking. After deployment, **all 9 iterations failed** (7 IDSSE + 2 Metrica). Root cause investigation in this session established:

**Bug 1 — IDSSE OOM:** The old code called `add_das(actions, frames)` without `team_in_possession`. The library's `_validate_das_inputs` raised `ValueError`, caught internally by `add_das`'s own `except (ValueError, RuntimeError, ImportError)` — DAS was always NaN, zero memory used. PR #282 fixed the prerequisite, so `accessible_space.get_dangerous_accessible_space()` now actually runs. That library allocates 5D numpy arrays of shape `F x P x V0 x PHI x T` (150 x 23 x 15 x 30 x 46 = 71.4M elements per chunk). Per-array: 545 MB. Peak concurrent: ~2.2 GB per chunk. Total for 5000 frames: ~2.8 GB — nearly 3x the 1 GB UDF limit.

**Bug 2 — Metrica `bekkers_pi`:** `pressure_on_actor(method="bekkers_pi")` validates `frames["is_ball"].any()`. Metrica sample data has batches where all `ball_x`/`ball_y` are NaN, so the converter produces zero ball rows. The validation raises `ValueError`, crashing the entire UDF. The other two pressure methods (`andrienko_oval`, `link_zones`) don't need ball data and would compute fine.

## Prerequisites

- silly-kicks `>=3.13.0,<4` (already pinned)
- `get_das(frames, **kwargs)` passes `chunk_size` through to `get_dangerous_accessible_space` — verified: parameter name is `chunk_size`, kind `POSITIONAL_OR_KEYWORD`, default `150`
- `add_das` / `_precompute_das_lookup` do NOT expose `chunk_size` — confirmed; must bypass and call `get_das` directly
- `infer_ball_carrier(frames, gamma=1.0)` — hysteresis parameter is exposed; no large numpy allocations (pure Python + pandas groupby)

## Memory budget

- **Hard limit:** 1 GB (Databricks serverless UDF group memory cap)
- **Practical budget:** 800 MB (1 GB minus JVM overhead, Python interpreter, pandas/numpy base allocations)
- **Step isolation:** Steps in `_enrich_match` are sequential. DAS (step 12) runs after steps 1-11 complete, and steps 13-15 run after DAS finishes. Python GC can reclaim earlier temporaries between steps.

## Changes

### 1. DAS: action-linked frames + chunk_size=10

**File:** `src/ingestion/tracking_context.py` (`_enrich_match`, step 12)

Replace:

```python
try:
    carrier = infer_ball_carrier(frames)
    frames = derive_team_in_possession(frames, carrier)
    del carrier
    actions = add_das(actions, frames, links=links)
except (IndexError, ValueError, RuntimeError) as exc:
    ...
```

With a direct DAS computation that applies two memory levers:

```python
try:
    # ── Ball-carrier on ALL frames (contiguous → correct hysteresis) ──
    carrier = infer_ball_carrier(frames)
    frames_with_tip = derive_team_in_possession(frames, carrier)
    del carrier

    # ── Filter to action-linked frame_ids only (~50-107 vs 5000) ──
    # links has (action_id, frame_id) but no period_id — join via actions
    linked = links[["action_id", "frame_id"]].dropna(subset=["frame_id"])
    linked = linked.merge(actions[["action_id", "period_id"]], on="action_id", how="left")
    linked_frame_ids = linked[["period_id", "frame_id"]].drop_duplicates()
    das_frames = frames_with_tip.merge(linked_frame_ids, on=["period_id", "frame_id"], how="inner")
    del linked, frames_with_tip

    # ── Direct get_das with chunk_size=10 (bypasses add_das) ──
    from silly_kicks.tracking._das import get_das

    das_result = get_das(das_frames, use_progress_bar=False, chunk_size=10)
    del das_frames

    # ── Build (period_id, frame_id) -> {team_id: DAS} lookup ──
    # Same logic as silly_kicks.tracking.features._precompute_das_lookup
    player_rows = das_result[das_result["is_ball"] != True]  # noqa: E712
    valid_rows = player_rows.dropna(subset=["DAS"])
    das_lookup: dict[tuple, dict] = {}
    for (pid, fid, tid), grp in valid_rows.groupby(["period_id", "frame_id", "team_id"]):
        das_lookup.setdefault((pid, fid), {})[tid] = float(grp["DAS"].iloc[0])
    del das_result, player_rows, valid_rows

    # ── Map DAS to actions ──
    # Same logic as silly_kicks.tracking.features._map_das_to_actions (numpy pattern)
    pointer_lookup = links.set_index("action_id")
    team_vals = np.full(len(actions), np.nan)
    opp_vals = np.full(len(actions), np.nan)

    for i, (_idx, row) in enumerate(actions.iterrows()):
        aid = row["action_id"]
        if aid not in pointer_lookup.index:
            continue
        fid_raw = pointer_lookup.at[aid, "frame_id"]
        if pd.isna(fid_raw):
            continue
        key = (row["period_id"], int(float(fid_raw)))
        if key not in das_lookup:
            continue
        team_id = row["team_id"]
        team_vals[i] = das_lookup[key].get(team_id, np.nan)
        opp = [v for k, v in das_lookup[key].items() if k != team_id]
        if opp:
            opp_vals[i] = opp[0]

    actions["das_team"] = team_vals
    actions["das_opponent"] = opp_vals
    actions["das_diff"] = team_vals - opp_vals

except (IndexError, ValueError, RuntimeError) as exc:
    logger.error(
        "DAS degraded to NaN for match_id=%s: %s: %s",
        match_id_native, type(exc).__name__, exc,
    )
    actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

**Memory math (corrected, verified against silly-kicks review C1):**

5D array dimensions: `chunk_size x P x V0 x PHI x T` where P=23, V0=15, PHI=30, T=46.

| chunk_size | Elements per array | MB per array | Peak (4 concurrent) |
|------------|-------------------|-------------|---------------------|
| 150 (default) | 71,415,000 | 545 MB | 2,179 MB |
| 20 | 9,522,000 | 72.6 MB | 291 MB |
| **10** | **4,761,000** | **36.3 MB** | **145 MB** |

Full budget with `chunk_size=10`:

| Scenario | Peak chunk (MB) | Accumulated (MB) | Total (MB) | Verdict |
|----------|----------------|-------------------|------------|---------|
| Current: 5000 frames, chunk_size=150 | 2,179 | 526 | 2,706 | OOM |
| Fix: 100 frames, chunk_size=10 | 145 | 11 | 156 | OK |
| Fix: 50 frames, chunk_size=10 | 145 | 5 | 150 | OK |

Per-batch action count (5000 frames = 200 seconds at 25 fps): ~50-107 actions, each linking to 1 unique frame. So DAS processes ~50-107 frames, not 200-500 (the earlier estimate was per-match, not per-batch). Total DAS peak: ~150-156 MB — well within the 800 MB budget with >600 MB headroom.

`chunk_size=10` chosen over `chunk_size=20` for conservative headroom. The wall-clock penalty is negligible: ~50-107 frames / 10 = 5-11 chunks, each taking milliseconds.

**Why `infer_ball_carrier` runs on ALL frames, not filtered (silly-kicks review C2):**

`infer_ball_carrier` uses sequential hysteresis (`gamma=1.0`): the incumbent ball carrier gets a 1-meter distance bonus that carries across frames. On non-contiguous action-linked frames (50-125 frame gaps), this hysteresis would produce incorrect carrier assignments at possession transitions.

Solution: run `infer_ball_carrier` + `derive_team_in_possession` on the full 5000 frames (cheap — pure Python groupby + numpy distance, no 5D allocations, ~13 MB for the player_groups dict), then filter `das_frames` from the result. Only the expensive `get_das` call runs on the filtered subset.

**Why bypass `add_das` instead of patching it:**
- `add_das` calls `_precompute_das_lookup(frames)` with no kwargs passthrough — `chunk_size` cannot reach `get_das`
- `_precompute_das_lookup` always processes ALL frames — no filtering capability
- Both are internal to silly-kicks; patching them requires silly-kicks PR-S40 (tracked, out of scope)
- The inline DAS logic mirrors `_precompute_das_lookup` + `_map_das_to_actions` exactly — same groupby lookup construction, same numpy-array action mapping pattern

**The full `frames` remains unmodified** for steps 13-15 (GK influence, cover shadows, sync score). `frames_with_tip` is a separate merge result that's deleted after DAS filtering. This is a change from PR #282, which mutated `frames` via `frames = derive_team_in_possession(frames, carrier)`.

### 2. Bekkers pressure: graceful degradation on missing ball rows

**File:** `src/ingestion/tracking_context.py` (`_enrich_match`, step 4)

Replace the single `add_pressure_on_actor` call with a split that computes `andrienko_oval` + `link_zones` unconditionally, then attempts `bekkers_pi` with graceful degradation:

```python
# Step 4a: Pressure — andrienko_oval + link_zones (no ball rows needed)
actions = add_pressure_on_actor(
    actions, frames, links=links,
    methods=("andrienko_oval", "link_zones"),
)

# Step 4b: Pressure — bekkers_pi (needs ball rows; degrade if absent)
try:
    actions = add_pressure_on_actor(
        actions, frames, links=links,
        methods=("bekkers_pi",),
    )
except ValueError as exc:
    if "is_ball=True" in str(exc):
        logger.error(
            "bekkers_pi degraded to NaN for match_id=%s: %s",
            match_id_native, exc,
        )
        actions["pressure_on_actor__bekkers_pi"] = np.nan
    else:
        raise
```

**Why this is the correct long-term fix:**
- `use_ball_carrier_max=True` is the methodologically correct default per Bekkers 2024 section 2.4 — it models the fact that the ball carrier is under maximum pressing intensity. Setting it to `False` computes a weaker metric.
- The underlying issue is data quality (Metrica sample matches with NaN ball positions), not methodology. We should not degrade the model for all providers because one provider's data is incomplete.
- The `ValueError` check matches `"is_ball=True"` — a stable anchor from the error message `"frames missing is_ball=True rows in linked frames"` (silly-kicks `features.py:882`). More stable than matching on `"is_ball"` alone (silly-kicks review M1).
- The two ball-independent methods (`andrienko_oval`, `link_zones`) always compute — maximizing data yield.
- **Long-term (silly-kicks side):** A dedicated exception type (e.g. `MissingBallDataError`) would replace the string match. Tracked as silly-kicks TODO.

### 3. Wheel bump

**Files:** `pyproject.toml`, `src/shared/wheel.py`, ~27 consumer files via `bump_wheel.py`

Wheel: 0.3.53 -> 0.3.54 (or next available). Propagate via `uv run python scripts/bump_wheel.py`.

## Testing

### Unit tests

**File:** `src/tests/test_tracking_context_udf.py`

1. **DAS on action-linked frames:** Mock `silly_kicks.tracking._das.get_das` inside `_enrich_match`. Assert it receives a DataFrame with only the frame_ids present in the `links` pointer table, not all frames. Assert `chunk_size=10` is in the kwargs.

2. **Bekkers degradation with no ball rows:** Create synthetic frames with `is_ball=False` for all rows (simulating the Metrica NaN-ball-position case). Call `_enrich_match`. Assert:
   - `pressure_on_actor__andrienko_oval` and `pressure_on_actor__link_zones` are non-NaN
   - `pressure_on_actor__bekkers_pi` is NaN
   - No exception raised

3. **Bekkers success with ball rows:** Create synthetic frames with ball rows present. Call `_enrich_match`. Assert all three pressure columns are non-NaN.

### Integration validation (post-deploy)

1. Bump wheel, merge, wait for CI + wheel deploy
2. DELETE existing data from `bronze.spadl_tracking_context`
3. Trigger `compute_tracking_context_iteration` for both IDSSE and Metrica
4. Validate:
   - IDSSE iterations complete without OOM (was 0/7, expect 7/7)
   - Metrica iterations complete without crash (was 0/2, expect 2/2)
   - `das_team` fill rate > 0% for completed matches
   - `pressure_on_actor__bekkers_pi` is NaN for Metrica (expected — sample data has NaN ball positions) and non-NaN for IDSSE
   - IDSSE timing < 15 minutes per iteration (was timing out at 30)

## Risks

- **DAS accuracy on action-linked subset:** `accessible_space` simulates passes per frame independently (no cross-frame dependencies), so computing on a subset is semantically identical to computing on all frames and discarding unused results.
- **`infer_ball_carrier` on all 5000 frames:** Cheap — pure Python + pandas groupby + numpy distance, ~13 MB peak for the player_groups dict. No 5D allocations. Takes ~1-2s.
- **`chunk_size=10` vs default `150`:** More loop iterations in `simulate_passes_chunked`, but on ~50-107 frames (not 5000), total DAS time is ~50-100x faster than the current OOM path.
- **`silly_kicks.tracking._das.get_das` is a private import:** Importing from `_das` couples us to silly-kicks internals. Acceptable because: (a) `get_das` is the documented entry point in `_das.py`'s docstring, (b) the alternative (`add_das`) doesn't support `chunk_size`, (c) silly-kicks PR-S40 will expose `chunk_size` on `add_das`, at which point we switch back. Tracked as TODO.

## Out of scope

- **silly-kicks PR-S40:** Add `das_kwargs: dict | None = None` passthrough to `_precompute_das_lookup` and `add_das`. Once shipped, lakehouse switches back to `add_das(actions, frames, links=links, das_kwargs={"chunk_size": 10})` and deletes the inline DAS code. Added to both lakehouse and silly-kicks TODO.
- **silly-kicks `MissingBallDataError`:** Dedicated exception type for bekkers_pi ball-data validation, replacing the string-match guard. Added to silly-kicks TODO.
- **Metrica ball data quality investigation:** Why do the sample matches have NaN ball positions? Not blocking — the graceful degradation handles it.
- **TC-2 (Taipy page) / TC-3 (deprecate legacy tables):** Remain in TODO.
