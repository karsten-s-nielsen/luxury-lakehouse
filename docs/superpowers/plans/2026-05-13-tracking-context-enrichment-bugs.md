# Tracking Context Enrichment Bugs — Fix Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three bugs in the tracking context enrichment pipeline that cause silently wrong directional output for IDSSE and prevent Metrica/SkillCorner from running at all, plus align Metrica player_id format at the converter layer to enable actor-level enrichments.

**Architecture:** Bug 1 is a Spark column projection omission. Bug 2 is a Kimball identity resolution gap — BIGINT surrogates are NULL for tracking providers, causing silly-kicks enrichment functions to produce **systematically wrong directional output** (not just NaN) because `NaN == home_team_id` evaluates to `False`, treating all actions as the away team. Bug 3 is a broad `except Exception` on DAS — narrowed to `except IndexError` to preserve graceful degradation while satisfying ADR-002. Metrica player_id mismatch is fixed at the converter layer (not bronze) per Hyrum's Law — bronze stores raw provider data, the converter normalizes.

**Tech Stack:** PySpark, pandas, silly-kicks, Databricks serverless

**Branch:** `fix/tracking-context-enrichment-bugs`

---

## Verified Facts (from live Databricks queries + code reading)

- `bronze.spadl_tracking_context` has 7 IDSSE matches, all with `team_id = 'nan'` and `player_id = 'nan'`
- `bronze.spadl_actions` for IDSSE: `team_id` = NULL, `player_id` = NULL, `team_id_native` = DFL CLU strings (e.g. `DFL-CLU-000005`), `player_id_native` = DFL OBJ strings (e.g. `DFL-OBJ-0001LJ`)
- `bronze.spadl_actions` for Metrica: `team_id` = NULL, `player_id` = NULL, `team_id_native` = `metrica_Sample_Game_1_home`/`_away`, `player_id_native` = `Player1`–`Player28`
- **SkillCorner has no SPADL converter** — no `data_source = 'skillcorner'` rows exist in `bronze.spadl_actions`; `tracking_context.py:1383` hits `actions_pdf.empty` and `continue`
- IDSSE tracking frames: `team_id` = DFL CLU strings, `player_id` = DFL OBJ strings (from `_IDSSE_TRACKING_SELECT_COLS`)
- Metrica tracking frames (from `_bronze_metrica_to_frames`): `team_id` = `"Home"`/`"Away"`, `player_id` = `f"{team_label}_{jersey}"` (e.g. `Home_11`)
- SkillCorner tracking frames: `team_id` = `"home"`/`"away"` (lowercase), but `home_team_id` = kloppy numeric ID (e.g. `"31"`) — **format mismatch** (latent, blocked by missing SPADL actions)
- `home_team_id` for IDSSE = DFL CLU string (line 1397), for Metrica = `"Home"` (line 1402)
- **Bug 2 produces wrong data, not NaN**: `_defensive_line.py:199` does `defends_x0 = team_id == home_team_id` — NaN team_id always evaluates False, treating ALL teams as defending x=105. `_kernels.py:843` does `merged["team_id_dl"] != merged["team_id_action"]` — NaN != anything is True in pandas, matching BOTH teams' defensive lines arbitrarily. This is systematically wrong directional output, worse than NaN.
- `_METRICA_TRACKING_SELECT_COLS` and `_SKILLCORNER_TRACKING_SELECT_COLS` both omit `match_id` — causes `UNRESOLVED_COLUMN` at `groupBy("match_id", ...)`
- `_IDSSE_TRACKING_SELECT_COLS` includes `match_id` (converter renames it to `game_id`)
- Comment at lines 48–49 ("Catalyst pushes predicates below projections") is misleading — predicate pushdown works but `.select()` still restricts output columns
- `accessible-space` 2.0.15 is a transitive dependency via `silly-kicks[das]` — already in wheel
- Metrica bronze tracking JSON keys are bare jersey numbers (`"11"`, `"2"`, `"25"`) — raw provider format, stays in bronze per Hyrum's Law
- Converter `_bronze_metrica_to_frames` normalizes to silly-kicks frame format — the `PlayerN` prefix belongs here
- `line_breaking_tracking.py` is another consumer of `home_players`/`away_players` JSON (key-format agnostic — no impact)
- Staging model `stg_spadl__tracking_context.sql` renames `team_id` → `team_id_native` for dim joins
- `fct_tracking_context.sql` joins dim_teams on `(provider, native_team_id = team_id_native)` — output `team_id` MUST contain native IDs
- `identifiers.py:metrica_native_team_id(match_id, side)` is the canonical format generator — reverse mapping must use it

---

### Task 1: Create feature branch

**Files:** None (git only)

- [ ] **Step 1: Create and switch to feature branch**

```bash
git checkout -b fix/tracking-context-enrichment-bugs main
```

---

### Task 2: Bug 1 — Add `match_id` to Metrica + SkillCorner column projections

**Files:**
- Modify: `src/ingestion/tracking_context.py:48-49,70-80,82-95`
- Modify: `src/tests/test_tracking_context_column_projection.py:73-97`

- [ ] **Step 1: Write the failing test**

Add a new test to `src/tests/test_tracking_context_column_projection.py` that verifies `match_id` is present in all provider projection constants (since `groupBy("match_id", ...)` at line 1434 is provider-agnostic):

```python
def test_all_projections_include_match_id() -> None:
    """groupBy('match_id', ...) requires match_id in every provider's projection."""
    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    for name, cols in [
        ("IDSSE", _IDSSE_TRACKING_SELECT_COLS),
        ("Metrica", _METRICA_TRACKING_SELECT_COLS),
        ("SkillCorner", _SKILLCORNER_TRACKING_SELECT_COLS),
    ]:
        assert "match_id" in cols, (
            f"{name} projection missing 'match_id' — "
            f"groupBy('match_id', 'period', 'frame_batch_id') will fail with UNRESOLVED_COLUMN"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_column_projection.py::test_all_projections_include_match_id -v`
Expected: FAIL — Metrica and SkillCorner missing `match_id`

- [ ] **Step 3: Fix the projection constants**

In `src/ingestion/tracking_context.py`:

**Line 48–49** — Fix the misleading comment:
```python
# NOTE: match_id MUST be in every provider's select tuple — the
# provider-agnostic groupBy("match_id", "period", "frame_batch_id")
# at dispatch time requires it in the DataFrame.
```

**Lines 70–80** — Add `match_id` to `_METRICA_TRACKING_SELECT_COLS`:
```python
_METRICA_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "period",
    "frame",
    "timestamp",
    "frame_rate",
    "gk_jersey_numbers",
    "home_players",
    "away_players",
    "ball_x",
    "ball_y",
)
```

**Lines 82–95** — Add `match_id` to `_SKILLCORNER_TRACKING_SELECT_COLS`:
```python
_SKILLCORNER_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "frame",
    "period",
    "timestamp",
    "player_id",
    "team",
    "x",
    "y",
    "is_goalkeeper",
    "frame_rate",
    "home_team_id",
    "ball_x",
    "ball_y",
)
```

- [ ] **Step 4: Update wasteful-projection test**

In `src/tests/test_tracking_context_column_projection.py`, update `test_projection_is_not_wasteful` to declare `match_id` as a `process_extra` column for Metrica and SkillCorner (the converters don't consume it, but `groupBy` needs it):

```python
def test_projection_is_not_wasteful() -> None:
    """Projection should not include columns not consumed by converter or _process_*."""
    from ingestion.tracking_context import (
        _IDSSE_CONSUMED_COLS,
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_CONSUMED_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_CONSUMED_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )

    # match_id is needed by groupBy(), not by the converters.
    # home_team_id is consumed by _process_skillcorner, not the converter.
    groupby_extra = {"match_id"}
    sc_process_extra = {"home_team_id"}

    for name, proj, consumed, process_extra in [
        ("IDSSE", _IDSSE_TRACKING_SELECT_COLS, _IDSSE_CONSUMED_COLS, set()),
        ("Metrica", _METRICA_TRACKING_SELECT_COLS, _METRICA_CONSUMED_COLS, groupby_extra),
        ("SkillCorner", _SKILLCORNER_TRACKING_SELECT_COLS, _SKILLCORNER_CONSUMED_COLS, groupby_extra | sc_process_extra),
    ]:
        extra = set(proj) - consumed - process_extra
        assert not extra, (
            f"{name} projection has unexplained columns: {sorted(extra)}. "
            f"Remove from projection, add to consumed, or add to process_extra."
        )
```

- [ ] **Step 5: Run all projection tests**

Run: `uv run pytest src/tests/test_tracking_context_column_projection.py -v`
Expected: ALL PASS

---

### Task 3: Bug 3 — Narrow DAS exception catch to `IndexError`

The current `except Exception` is too broad (ADR-002), but removing it entirely is too aggressive — one `IndexError` from `accessible-space` on an edge-case frame would lose ALL ~70 enrichment columns for that (match, period, batch), not just the 3 DAS columns. Narrowing to `except IndexError` satisfies ADR-002 (no broad `except Exception`) while preserving graceful degradation.

**Files:**
- Modify: `src/ingestion/tracking_context.py:561-565`
- Modify: `src/tests/test_tracking_context_udf.py`

- [ ] **Step 1: Write the behavioral test**

Add tests to `src/tests/test_tracking_context_udf.py` that verify the DAS call behavior — `IndexError` is caught (graceful degradation), other exceptions propagate (ADR-002 §5):

```python
def _make_dummy_xt():
    """12×16 xT grid of zeros for tests that don't exercise xT values."""
    import numpy as np

    return np.zeros((12, 16))


def _make_minimal_actions():
    """Single-row SPADL actions DataFrame with all required columns."""
    import pandas as pd

    return pd.DataFrame(
        {
            "game_id": [1],
            "action_id": [0],
            "period_id": [1],
            "time_seconds": [10.0],
            "team_id": ["DFL-CLU-000005"],
            "player_id": ["DFL-OBJ-0001LJ"],
            "team_id_native": ["DFL-CLU-000005"],
            "player_id_native": ["DFL-OBJ-0001LJ"],
            "type_id": [0],
            "result_id": [1],
            "bodypart_id": [0],
            "start_x": [50.0],
            "start_y": [34.0],
            "end_x": [60.0],
            "end_y": [34.0],
        }
    )


def _make_minimal_frames():
    """Single-row tracking frames DataFrame with all required columns."""
    import pandas as pd

    return pd.DataFrame(
        {
            "game_id": [1],
            "frame_id": [1],
            "period_id": [1],
            "time_seconds": [10.0],
            "player_id": ["DFL-OBJ-0001LJ"],
            "team_id": ["DFL-CLU-000005"],
            "x": [50.0],
            "y": [34.0],
            "vx": [0.0],
            "vy": [0.0],
            "speed": [0.0],
            "ax": [0.0],
            "ay": [0.0],
            "is_goalkeeper": [False],
            "is_ball": [False],
        }
    )


def test_das_index_error_degrades_gracefully() -> None:
    """DAS IndexError fills 3 columns with NaN, preserving all other enrichments.

    _enrich_match runs 15 enrichment steps before DAS (step 12). To isolate
    the DAS exception handling, we mock ALL enrichment steps to pass through
    their input unchanged, leaving only add_das mocked to raise IndexError.
    """
    from unittest.mock import patch

    import numpy as np

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    def mock_add_das(actions, frames):
        raise IndexError("edge-case frame geometry")

    # Mock all silly-kicks enrichment functions to return their first arg
    # unchanged, so the chain runs without requiring real tracking data.
    # Only add_das is mocked to throw.
    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731
    patches = [
        patch("silly_kicks.tracking.link_actions_to_frames", return_value=(actions[["action_id"]], None)),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        patch("silly_kicks.tracking.add_pressure_on_actor", passthrough),
        patch("silly_kicks.tracking.pitch_control_at_action", passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.add_das", mock_add_das),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]

    for p in patches:
        p.start()
    try:
        result = _enrich_match(
            actions=actions,
            frames=frames,
            xt=_make_dummy_xt(),
            home_team_id="DFL-CLU-000005",
            match_id_native="test",
            data_source="idsse",
        )
    finally:
        for p in patches:
            p.stop()

    assert np.isnan(result["das_team"].iloc[0])
    assert np.isnan(result["das_opponent"].iloc[0])
    assert np.isnan(result["das_diff"].iloc[0])


def test_das_non_index_error_propagates() -> None:
    """Non-IndexError exceptions from DAS must propagate (ADR-002 §5)."""
    from unittest.mock import patch

    import pytest

    from ingestion.tracking_context import _enrich_match

    def mock_add_das(actions, frames):
        raise ValueError("unexpected DAS failure")

    # Same passthrough mocking as above, but add_das raises ValueError
    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731
    actions = _make_minimal_actions()
    patches = [
        patch("silly_kicks.tracking.link_actions_to_frames", return_value=(actions[["action_id"]], None)),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        patch("silly_kicks.tracking.add_pressure_on_actor", passthrough),
        patch("silly_kicks.tracking.pitch_control_at_action", passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.add_das", mock_add_das),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]

    for p in patches:
        p.start()
    try:
        with pytest.raises(ValueError, match="unexpected DAS failure"):
            _enrich_match(
                actions=_make_minimal_actions(),
                frames=_make_minimal_frames(),
                xt=_make_dummy_xt(),
                home_team_id="DFL-CLU-000005",
                match_id_native="test",
                data_source="idsse",
            )
    finally:
        for p in patches:
            p.stop()
```

NOTE: The helpers `_make_dummy_xt`, `_make_minimal_actions`, `_make_minimal_frames` are defined above the tests. The `passthrough` lambda + bulk patching isolates DAS exception handling from the 14 other enrichment steps that precede it in `_enrich_match`. If bulk patching proves too brittle against future enrichment-chain changes, fall back to an AST test that checks for `except IndexError` specifically (not `except Exception`).

- [ ] **Step 2: Narrow the exception catch**

In `src/ingestion/tracking_context.py`, replace lines 561–565:

**Before:**
```python
    # Step 12: DAS (defensive wrapper — accessible-space can IndexError)
    try:
        actions = add_das(actions, frames)
    except Exception:  # noqa: BLE001 — accessible-space IndexError on edge-case frames
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

**After:**
```python
    # Step 12: DAS (accessible-space)
    # Narrow catch: IndexError on edge-case frame geometry degrades 3 DAS columns
    # to NaN while preserving all other enrichments. Non-IndexError exceptions
    # propagate to the UDF wrapper (ADR-002 §5 — group key in error message).
    try:
        actions = add_das(actions, frames)
    except IndexError:
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

- [ ] **Step 3: Run the test**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v -k das`
Expected: PASS

---

### Task 4: Metrica player_id — normalize at converter layer (not bronze)

Bronze stores raw provider data (bare jersey numbers `"11"`, `"25"` in JSON keys). The converter `_bronze_metrica_to_frames` normalizes to silly-kicks frame format — the `PlayerN` prefix belongs here, not in bronze ingestion. This avoids a bronze schema format change (Hyrum's Law), mixed-format data during partial re-ingestion, and cascading changes to `gk_jersey_numbers` in two ingestion paths.

**Files:**
- Modify: `src/ingestion/tracking_context.py:908,912` (converter only)
- Create: `src/tests/test_metrica_tracking_player_id.py`

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_metrica_tracking_player_id.py`:

```python
"""Tests for Metrica tracking player_id format normalization.

Verifies that _bronze_metrica_to_frames normalizes bare jersey JSON keys
to kloppy 'PlayerN' format at the converter layer (bronze stores raw format).
"""

from __future__ import annotations

import json


def test_converter_normalizes_jersey_to_player_prefix() -> None:
    """_bronze_metrica_to_frames produces 'PlayerN' player_id from bare jersey JSON keys."""
    import pandas as pd

    from ingestion.tracking_context import _bronze_metrica_to_frames

    # Bronze format: bare jersey numbers as JSON keys
    trk_pdf = pd.DataFrame(
        {
            "period": [1],
            "frame": [1],
            "timestamp": [0.04],
            "frame_rate": [25],
            "gk_jersey_numbers": [json.dumps(["1"])],
            "home_players": [json.dumps({"11": {"x": 0.5, "y": 0.3}})],
            "away_players": [json.dumps({"25": {"x": 0.6, "y": 0.7}})],
            "ball_x": [0.5],
            "ball_y": [0.5],
        }
    )

    frames = _bronze_metrica_to_frames(trk_pdf, game_id=1)
    player_rows = frames[~frames["is_ball"]]

    player_ids = set(player_rows["player_id"].tolist())
    # Should be "Player11" and "Player25", NOT "Home_11" and "Away_25"
    assert "Player11" in player_ids, f"Expected 'Player11', got {player_ids}"
    assert "Player25" in player_ids, f"Expected 'Player25', got {player_ids}"
    assert not any(pid.startswith("Home_") or pid.startswith("Away_") for pid in player_ids), (
        f"player_id should not use Home_/Away_ prefix: {player_ids}"
    )


def test_converter_gk_detection_with_player_prefix() -> None:
    """GK detection works with Player-prefixed jersey matching bare gk_jersey_numbers."""
    import pandas as pd

    from ingestion.tracking_context import _bronze_metrica_to_frames

    trk_pdf = pd.DataFrame(
        {
            "period": [1],
            "frame": [1],
            "timestamp": [0.04],
            "frame_rate": [25],
            "gk_jersey_numbers": [json.dumps(["1"])],
            "home_players": [json.dumps({"1": {"x": 5.0, "y": 34.0}, "11": {"x": 50.0, "y": 34.0}})],
            "away_players": [json.dumps({})],
            "ball_x": [0.5],
            "ball_y": [0.5],
        }
    )

    frames = _bronze_metrica_to_frames(trk_pdf, game_id=1)
    player_rows = frames[~frames["is_ball"]]

    gk_row = player_rows[player_rows["player_id"] == "Player1"]
    non_gk_row = player_rows[player_rows["player_id"] == "Player11"]

    assert len(gk_row) == 1
    assert gk_row.iloc[0]["is_goalkeeper"] is True
    assert len(non_gk_row) == 1
    assert non_gk_row.iloc[0]["is_goalkeeper"] is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_metrica_tracking_player_id.py -v`
Expected: FAIL — converter produces `Home_11` format

- [ ] **Step 3: Fix converter — `_bronze_metrica_to_frames`**

In `src/ingestion/tracking_context.py`, two changes in `_bronze_metrica_to_frames`:

**Line 908** — Normalize bare jersey key to `PlayerN` format:

Before:
```python
                            "player_id": f"{team_label}_{jersey}",
```

After:
```python
                            "player_id": f"Player{jersey}",
```

**Line 912** — Update GK check to match the new format (bronze `gk_jersey_numbers` stores bare `["1"]`, `jersey` is now used raw from JSON key):

Before:
```python
                            "is_goalkeeper": str(jersey) in gk_jerseys,
```

After:
```python
                            "is_goalkeeper": jersey in gk_jerseys,
```

(Minor simplification — `str(jersey)` was redundant since JSON keys are already strings. `jersey` is the bare JSON key `"1"`, and `gk_jerseys` is `{"1"}` from bronze.)

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_metrica_tracking_player_id.py -v`
Expected: ALL PASS

---

### Task 5: Bug 2 — Resolve team_id/player_id for enrichment functions

This is the core fix. The enrichment chain needs silly-kicks-compatible `team_id`/`player_id` values (matching `frames["team_id"]` and `home_team_id`), while the output needs native IDs (for dim table joins via `stg_spadl__tracking_context.sql`).

**IMPORTANT — mutate-then-restore contract:** silly-kicks enrichment functions read `actions["team_id"]` directly — we cannot change which column they read. The pattern is: overwrite `team_id`/`player_id` before enrichment with silly-kicks-compatible values, then overwrite again after enrichment with native IDs for output. A prominent comment block documents this temporal contract.

**Files:**
- Modify: `src/ingestion/tracking_context.py` — `_enrich_match()` (lines 477–600)
- Create: `src/tests/test_tracking_context_identity_resolution.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_tracking_context_identity_resolution.py`:

```python
"""Tests for tracking context identity resolution (Bug 2 fix).

Verifies that _resolve_enrichment_identity produces non-null team_id/player_id
matching the tracking frame format, and that _restore_native_identity restores
native IDs for dim table joins.
"""

from __future__ import annotations


def test_idsse_team_id_uses_native() -> None:
    """For IDSSE, team_id passed to enrichments must be team_id_native (DFL CLU string)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "game_id": [1, 1],
            "action_id": [0, 1],
            "period_id": [1, 1],
            "time_seconds": [10.0, 25.0],
            "team_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "team_id_native": ["DFL-CLU-000005", "DFL-CLU-000008"],
            "player_id_native": ["DFL-OBJ-0001LJ", "DFL-OBJ-0002HE"],
            "type_id": [0, 1],
            "result_id": [1, 0],
            "bodypart_id": [0, 0],
            "start_x": [50.0, 30.0],
            "start_y": [34.0, 20.0],
            "end_x": [60.0, 40.0],
            "end_y": [34.0, 25.0],
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    resolved = _resolve_enrichment_identity(actions.copy(), provider="idsse", match_id_native="test")
    assert resolved["team_id"].iloc[0] == "DFL-CLU-000005"
    assert resolved["team_id"].iloc[1] == "DFL-CLU-000008"
    assert resolved["player_id"].iloc[0] == "DFL-OBJ-0001LJ"
    assert resolved["player_id"].iloc[1] == "DFL-OBJ-0002HE"


def test_metrica_team_id_maps_to_home_away() -> None:
    """For Metrica, team_id must be 'Home'/'Away' (matching frames and home_team_id)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "game_id": [1, 1],
            "action_id": [0, 1],
            "period_id": [1, 1],
            "time_seconds": [10.0, 25.0],
            "team_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA, pd.NA], dtype="Int64"),
            "team_id_native": [
                "metrica_Sample_Game_1_home",
                "metrica_Sample_Game_1_away",
            ],
            "player_id_native": ["Player11", "Player25"],
            "type_id": [0, 1],
            "result_id": [1, 0],
            "bodypart_id": [0, 0],
            "start_x": [50.0, 30.0],
            "start_y": [34.0, 20.0],
            "end_x": [60.0, 40.0],
            "end_y": [34.0, 25.0],
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    resolved = _resolve_enrichment_identity(
        actions.copy(), provider="metrica", match_id_native="Sample_Game_1"
    )
    assert resolved["team_id"].iloc[0] == "Home"
    assert resolved["team_id"].iloc[1] == "Away"
    # player_id_native is "PlayerN" (kloppy format) — matches frames after Task 4
    assert resolved["player_id"].iloc[0] == "Player11"
    assert resolved["player_id"].iloc[1] == "Player25"


def test_skillcorner_raises_not_implemented() -> None:
    """SkillCorner identity resolution raises NotImplementedError (no SPADL actions exist)."""
    import pandas as pd
    import pytest

    actions = pd.DataFrame(
        {
            "team_id": pd.array([pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA], dtype="Int64"),
            "team_id_native": ["sc_team_31"],
            "player_id_native": ["sc_player_123"],
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    with pytest.raises(NotImplementedError, match="SkillCorner"):
        _resolve_enrichment_identity(actions, provider="skillcorner", match_id_native="test")


def test_output_uses_native_ids() -> None:
    """Output team_id/player_id must be native IDs (for dim table joins via staging)."""
    import pandas as pd

    actions = pd.DataFrame(
        {
            "game_id": [1],
            "action_id": [0],
            "period_id": [1],
            "time_seconds": [10.0],
            "team_id": ["DFL-CLU-000005"],  # enrichment-resolved value
            "player_id": ["DFL-OBJ-0001LJ"],  # enrichment-resolved value
            "team_id_native": ["DFL-CLU-000005"],
            "player_id_native": ["DFL-OBJ-0001LJ"],
            "type_id": [0],
            "result_id": [1],
            "bodypart_id": [0],
            "start_x": [50.0],
            "start_y": [34.0],
            "end_x": [60.0],
            "end_y": [34.0],
        }
    )

    from ingestion.tracking_context import _restore_native_identity

    restored = _restore_native_identity(actions.copy())
    assert restored["team_id"].iloc[0] == "DFL-CLU-000005"
    assert restored["player_id"].iloc[0] == "DFL-OBJ-0001LJ"


def test_resolve_rejects_all_null_native() -> None:
    """If team_id_native is ALL null, resolution must raise (data quality gate)."""
    import pandas as pd
    import pytest

    actions = pd.DataFrame(
        {
            "team_id": pd.array([pd.NA], dtype="Int64"),
            "player_id": pd.array([pd.NA], dtype="Int64"),
            "team_id_native": pd.array([pd.NA], dtype="string"),
            "player_id_native": pd.array([pd.NA], dtype="string"),
        }
    )

    from ingestion.tracking_context import _resolve_enrichment_identity

    with pytest.raises(ValueError, match="team_id_native"):
        _resolve_enrichment_identity(actions, provider="idsse", match_id_native="test")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_tracking_context_identity_resolution.py -v`
Expected: FAIL — `_resolve_enrichment_identity` and `_restore_native_identity` do not exist

- [ ] **Step 3: Implement identity resolution**

In `src/ingestion/tracking_context.py`, add two new functions before `_enrich_match`:

```python
def _resolve_enrichment_identity(
    actions: pd.DataFrame,
    *,
    provider: str,
    match_id_native: str,
) -> pd.DataFrame:
    """Replace null team_id/player_id with silly-kicks-compatible values.

    Enrichment functions need team_id/player_id matching the tracking frame
    format. IDSSE uses DFL CLU/OBJ strings natively. Metrica needs reverse-
    mapping from lakehouse native IDs to "Home"/"Away" labels.

    IMPORTANT — mutate-then-restore contract:
    silly-kicks reads actions["team_id"] directly. This function overwrites
    team_id/player_id with silly-kicks-compatible values BEFORE enrichment.
    After enrichment, _restore_native_identity() overwrites them again with
    native IDs for output. Do not add enrichment steps after the restore call.

    Args:
        actions: SPADL actions with team_id_native and player_id_native columns.
        provider: "idsse", "metrica", or "skillcorner".
        match_id_native: Native match ID for Metrica reverse mapping.

    Returns:
        actions with team_id and player_id overwritten to match frame format.

    Raises:
        ValueError: If team_id_native is entirely null (data quality gate).
        NotImplementedError: If provider is "skillcorner" (no SPADL actions exist;
            frames use "home"/"away" but home_team_id is a kloppy numeric ID).
    """
    if actions["team_id_native"].dropna().empty:
        msg = (
            f"team_id_native is entirely null for provider={provider} — "
            f"cannot resolve enrichment identity"
        )
        raise ValueError(msg)

    if provider == "idsse":
        # DFL CLU/OBJ strings match both frames and home_team_id directly
        actions["team_id"] = actions["team_id_native"]
        actions["player_id"] = actions["player_id_native"]

    elif provider == "metrica":
        # Use canonical format generator for reverse mapping (identifiers.py
        # is the single source of truth for metrica native team ID format).
        from shared.identifiers import metrica_native_team_id

        fwd = {
            metrica_native_team_id(match_id_native, "home"): "Home",
            metrica_native_team_id(match_id_native, "away"): "Away",
        }
        actions["team_id"] = actions["team_id_native"].map(fwd)
        # player_id_native is "PlayerN" (kloppy convention) — matches
        # frames player_id after Task 4 converter normalization.
        actions["player_id"] = actions["player_id_native"]

    elif provider == "skillcorner":
        # SkillCorner has no SPADL converter — no rows exist in
        # bronze.spadl_actions. If SkillCorner SPADL is added, this must
        # address the home_team_id format mismatch: frames use "home"/"away"
        # (lowercase) but home_team_id is a kloppy numeric ID (e.g. "31").
        raise NotImplementedError(
            "SkillCorner identity resolution not implemented — "
            "no SPADL actions exist for this provider. When adding SkillCorner "
            "SPADL support, resolve the home_team_id vs frames team_id format "
            "mismatch (frames='home'/'away', home_team_id=kloppy numeric ID)."
        )

    return actions


def _restore_native_identity(actions: pd.DataFrame) -> pd.DataFrame:
    """Restore native IDs for output (dim table joins via staging layer).

    The staging model renames team_id -> team_id_native for dim_teams join.
    Output must contain native IDs, not the silly-kicks-compatible values
    used during enrichment.

    IMPORTANT: This must be called AFTER all enrichment steps and BEFORE
    building the output DataFrame. Do not add enrichment steps after this call.
    """
    actions["team_id"] = actions["team_id_native"]
    actions["player_id"] = actions["player_id_native"]
    return actions
```

- [ ] **Step 4: Wire into _enrich_match**

Modify `_enrich_match()` to call the new functions. Insert identity resolution BEFORE the enrichment chain (before Step 0) and native restoration in the output section.

**Before the enrichment chain (insert after line 498, before line 500):**
```python
    # ── Resolve enrichment-compatible identity ─────────────────────
    # MUTATE-THEN-RESTORE: team_id/player_id are overwritten here with
    # silly-kicks-compatible values (matching frames format), then restored
    # to native IDs by _restore_native_identity() in the output section.
    # Do not reorder these calls or add enrichment steps after the restore.
    actions = _resolve_enrichment_identity(
        actions, provider=data_source, match_id_native=match_id_native,
    )
```

**In the output section (replace lines 591–593):**

Before:
```python
    out["team_id"] = out["team_id"].astype(str)
    out["player_id"] = out["player_id"].astype(str)
```

After:
```python
    out = _restore_native_identity(out)
```

- [ ] **Step 5: Run identity resolution tests**

Run: `uv run pytest src/tests/test_tracking_context_identity_resolution.py -v`
Expected: ALL PASS

---

### Task 6: Run full test suite + lint

- [ ] **Step 1: Run all tracking context tests**

Run: `uv run pytest src/tests/test_tracking_context_udf.py src/tests/test_tracking_context_column_projection.py src/tests/test_tracking_context_identity_resolution.py src/tests/test_metrica_tracking_player_id.py -v`
Expected: ALL PASS

- [ ] **Step 2: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_identity_resolution.py src/tests/test_tracking_context_udf.py src/tests/test_tracking_context_column_projection.py src/tests/test_metrica_tracking_player_id.py`
Run: `uv run ruff format --check src/ingestion/tracking_context.py src/tests/test_tracking_context_identity_resolution.py src/tests/test_metrica_tracking_player_id.py`
Run: `uv run pyright src/ingestion/tracking_context.py`
Expected: Zero violations

- [ ] **Step 3: Run broader test suite**

Run: `uv run pytest src/tests/ -v --ignore=src/tests/benchmarks`
Expected: ALL PASS (no regressions)

---

### Task 7: Bump wheel version

**Files:**
- Modify: `pyproject.toml` (version field)
- Modify: `src/shared/wheel.py` (version constant)
- Modify: all consumer files (via `bump_wheel.py`)

- [ ] **Step 1: Bump wheel**

Run: `uv run python scripts/bump_wheel.py`

This updates the version in `pyproject.toml`, `src/shared/wheel.py`, and all downstream consumers (PEP 723 scripts, workflow cards, CI workflows).

- [ ] **Step 2: Verify bump**

Run: `uv run python -c "from shared.wheel import WHEEL_VERSION; print(WHEEL_VERSION)"`
Expected: version incremented from current

---

### Task 8: USER APPROVAL — Commit

Present the diff summary and request commit approval.

- [ ] **Step 1: Show changes**

Run: `git diff --stat`

- [ ] **Step 2: Commit (after user approval)**

```bash
git add -A
git commit -m "fix(tracking-context): resolve enrichment identity + match_id projection + Metrica player_id + DAS narrowing

Bug 1: Add match_id to Metrica + SkillCorner column projections — groupBy
requires it in the DataFrame, not just in the filter predicate.

Bug 2: Resolve team_id/player_id from native columns before enrichment —
silly-kicks functions need non-null values matching the tracking frame format.
Without this fix, NaN team_id causes systematically wrong directional output
(all actions treated as away team), not just missing data.
IDSSE uses DFL CLU/OBJ strings directly; Metrica reverse-maps to Home/Away
via identifiers.py canonical generator. SkillCorner raises NotImplementedError
(no SPADL actions exist; latent home_team_id format mismatch documented).
Output restores native IDs for dim table joins via staging layer.

Bug 3: Narrow DAS catch from except Exception to except IndexError (ADR-002).
Preserves graceful degradation (3 DAS columns fill NaN) while ensuring
non-IndexError exceptions propagate with group key context.

Metrica player_id: Normalize bare jersey JSON keys to 'PlayerN' format at
the converter layer (_bronze_metrica_to_frames), not in bronze ingestion.
Bronze stores raw provider data per Hyrum's Law. Enables actor-level
enrichments (player_id now matches between SPADL and frames).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Post-merge verification

After merge + wheel deploy + Databricks job retrigger:

1. **IDSSE matches:** Verify `team_id` is no longer `'nan'` — should be DFL CLU strings
2. **IDSSE enrichment columns:** Verify team_shape_*, pitch_control_*, defensive_line_*, gk_*, etc. are non-null. Critically verify defensive_line_x is directionally correct (home team back-line near x=0, away near x=105)
3. **Metrica matches:** Verify they run without `UNRESOLVED_COLUMN` error
4. **Metrica enrichment columns:** ALL enrichment columns (team-level AND actor-specific) should be non-null — player_id now matches between SPADL and frames (`PlayerN` format)
5. **DAS columns:** If `accessible-space` throws `IndexError`, the 3 DAS columns fill NaN while all other enrichments survive. Non-`IndexError` exceptions fail the UDF group with a clear error message (per ADR-002 §5)

## Review feedback incorporated

| ID | Concern | Resolution |
|----|---------|------------|
| C1 | SkillCorner team_id format mismatch | `NotImplementedError` in resolver — latent (no SPADL actions exist), documented for future implementer |
| C2 | Metrica bronze format change (Hyrum's Law) | Fix at converter layer, not bronze — no re-ingestion needed |
| C3 | DAS exception removal too aggressive | Narrowed to `except IndexError` — preserves graceful degradation |
| C4 | Mutate-then-restore fragile | Documented with prominent comment blocks at both call sites |
| C5 | Metrica reverse mapping via suffix match | Uses `identifiers.py:metrica_native_team_id()` canonical generator |
| C6 | AST test brittle | Replaced with behavioral test (mock + propagation check) |
| C7 | Missing bronze migration | Moot — no bronze changes |
| C8 | NaN team_id wrong directional output | Confirmed critical — documented in verified facts + architecture summary |
| R2-1 | `.astype(str)` converts NaN → `"nan"` string | Dropped `.astype(str)` — assign `team_id_native` directly, preserving actual nulls |
| R2-2 | Undefined test helpers for DAS behavioral tests | Defined `_make_dummy_xt`, `_make_minimal_actions`, `_make_minimal_frames` + bulk-patch all 14 enrichment steps |
| R2-3 | `all(expr for _ in [1])` no-op wrapper | Simplified to bare `assert np.isnan(...)` |
| R2-4 | "No change needed" on a line that changes | Reworded to "Minor simplification — `str()` was redundant" |
