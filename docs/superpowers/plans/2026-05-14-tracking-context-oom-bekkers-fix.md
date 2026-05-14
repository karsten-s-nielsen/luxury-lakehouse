# TC-1d: Tracking Context OOM + Bekkers Pressure Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two production bugs causing 100% failure rate on all tracking context enrichment iterations: IDSSE OOM from accessible-space DAS on all frames, and Metrica crash from `bekkers_pi` requiring ball rows.

**Architecture:** Split pressure computation (step 4) into ball-independent methods + bekkers_pi with graceful degradation. Replace `add_das` (step 12) with direct `get_das` on action-linked frames only + `chunk_size=10`, bypassing the library's `_precompute_das_lookup` which processes all frames and doesn't expose `chunk_size`. Ball-carrier inference runs on all frames for correct hysteresis, then DAS runs on the filtered subset.

**Tech Stack:** Python 3.10, pandas, numpy, silly-kicks `>=3.13.0,<4`, accessible-space (via silly-kicks)

**Spec:** `docs/superpowers/specs/2026-05-14-tracking-context-oom-bekkers-fix-design.md`

---

### Task 1: Split pressure computation with bekkers_pi graceful degradation

**Files:**
- Modify: `src/ingestion/tracking_context.py:647-653` (step 4 in `_enrich_match`)
- Test: `src/tests/test_tracking_context_udf.py`

- [ ] **Step 1: Write failing tests for bekkers_pi degradation**

Add these tests to `src/tests/test_tracking_context_udf.py`:

```python
def test_bekkers_pi_degrades_on_missing_ball_rows(caplog) -> None:
    """bekkers_pi degrades to NaN when frames lack ball rows; other methods compute."""
    import logging
    from unittest.mock import patch

    import numpy as np
    import pandas as pd

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()  # all is_ball=False, no ball rows

    # Mock all enrichment steps EXCEPT pressure — let pressure run with real logic
    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    def pc_passthrough(actions, frames, method="spearman", **kwargs):
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    def mock_link(actions, frames, **kwargs):
        links = pd.DataFrame({
            "action_id": actions["action_id"].values,
            "frame_id": pd.array([frames["frame_id"].iloc[0]] * len(actions), dtype="Int64"),
            "time_offset_seconds": [0.0] * len(actions),
            "n_candidate_frames": [1] * len(actions),
            "link_quality_score": [1.0] * len(actions),
        })
        return links, None

    def mock_infer_ball_carrier(frames, **kwargs):
        return pd.DataFrame(columns=["game_id", "frame_id", "period_id", "carrier_player_id", "carrier_team_id"])

    def mock_derive_tip(frames, carrier, **kwargs):
        f = frames.copy()
        f["team_in_possession"] = pd.NA
        return f

    patches = [
        patch("silly_kicks.tracking.link_actions_to_frames", mock_link),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        # add_pressure_on_actor is NOT mocked — it runs for real
        patch("silly_kicks.tracking.pitch_control_at_action", pc_passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.infer_ball_carrier", mock_infer_ball_carrier),
        patch("silly_kicks.tracking.derive_team_in_possession", mock_derive_tip),
        patch("silly_kicks.tracking._das.get_das", side_effect=ValueError("no TIP")),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]
    for p in patches:
        p.start()
    try:
        with caplog.at_level(logging.ERROR, logger="ingestion.tracking_context"):
            result = _enrich_match(
                actions=actions,
                frames=frames,
                xt=_make_dummy_xt(),  # type: ignore[arg-type]
                home_team_id="DFL-CLU-000005",
                match_id_native="test",
                data_source="idsse",
            )
    finally:
        for p in patches:
            p.stop()

    # andrienko_oval and link_zones should have computed (even if NaN due to minimal data)
    assert "pressure_on_actor__andrienko_oval" in result.columns
    assert "pressure_on_actor__link_zones" in result.columns
    # bekkers_pi should be NaN (degraded)
    assert "pressure_on_actor__bekkers_pi" in result.columns
    assert np.isnan(result["pressure_on_actor__bekkers_pi"].iloc[0])
    # Should log the degradation
    assert "bekkers_pi degraded" in caplog.text


def test_bekkers_pi_unrelated_valueerror_propagates() -> None:
    """ValueError NOT about is_ball=True must propagate (not silently caught)."""
    from unittest.mock import patch

    import pytest

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    def pc_passthrough(actions, frames, method="spearman", **kwargs):
        import pandas as pd
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    def mock_link(actions, frames, **kwargs):
        import pandas as pd
        links = pd.DataFrame({
            "action_id": actions["action_id"].values,
            "frame_id": pd.array([1] * len(actions), dtype="Int64"),
            "time_offset_seconds": [0.0] * len(actions),
            "n_candidate_frames": [1] * len(actions),
            "link_quality_score": [1.0] * len(actions),
        })
        return links, None

    # Step 4a passes, step 4b raises unrelated ValueError
    call_count = {"n": 0}

    def mock_pressure(actions, frames, *, links=None, methods=("andrienko_oval",), **kwargs):
        call_count["n"] += 1
        if "bekkers_pi" in methods:
            raise ValueError("completely unrelated error")
        return actions

    def mock_infer(frames, **kwargs):
        import pandas as pd
        return pd.DataFrame(columns=["game_id", "frame_id", "period_id", "carrier_player_id", "carrier_team_id"])

    def mock_tip(frames, carrier, **kwargs):
        import pandas as pd
        f = frames.copy()
        f["team_in_possession"] = pd.NA
        return f

    patches = [
        patch("silly_kicks.tracking.link_actions_to_frames", mock_link),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        patch("silly_kicks.tracking.add_pressure_on_actor", mock_pressure),
        patch("silly_kicks.tracking.pitch_control_at_action", pc_passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.infer_ball_carrier", mock_infer),
        patch("silly_kicks.tracking.derive_team_in_possession", mock_tip),
        patch("silly_kicks.tracking._das.get_das", side_effect=ValueError("no TIP")),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]
    for p in patches:
        p.start()
    try:
        with pytest.raises(ValueError, match="completely unrelated error"):
            _enrich_match(
                actions=actions,
                frames=frames,
                xt=_make_dummy_xt(),  # type: ignore[arg-type]
                home_team_id="DFL-CLU-000005",
                match_id_native="test",
                data_source="idsse",
            )
    finally:
        for p in patches:
            p.stop()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_tracking_context_udf.py::test_bekkers_pi_degrades_on_missing_ball_rows src/tests/test_tracking_context_udf.py::test_bekkers_pi_unrelated_valueerror_propagates -v`

Expected: Both FAIL — current code calls `add_pressure_on_actor` with all 3 methods in a single call (no split, no try/except).

- [ ] **Step 3: Implement the pressure split in `_enrich_match`**

In `src/ingestion/tracking_context.py`, replace step 4 (lines 647-653):

```python
    # Step 4: Pressure (all 3 methods)
    actions = add_pressure_on_actor(
        actions,
        frames,
        links=links,
        methods=("andrienko_oval", "link_zones", "bekkers_pi"),
    )
```

With:

```python
    # Step 4a: Pressure — andrienko_oval + link_zones (no ball rows needed)
    actions = add_pressure_on_actor(
        actions,
        frames,
        links=links,
        methods=("andrienko_oval", "link_zones"),
    )

    # Step 4b: Pressure — bekkers_pi (needs ball rows; degrade if absent)
    try:
        actions = add_pressure_on_actor(
            actions,
            frames,
            links=links,
            methods=("bekkers_pi",),
        )
    except ValueError as exc:
        if "is_ball=True" in str(exc):
            logger.error(
                "bekkers_pi degraded to NaN for match_id=%s: %s",
                match_id_native,
                exc,
            )
            actions["pressure_on_actor__bekkers_pi"] = np.nan
        else:
            raise
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_tracking_context_udf.py::test_bekkers_pi_degrades_on_missing_ball_rows src/tests/test_tracking_context_udf.py::test_bekkers_pi_unrelated_valueerror_propagates -v`

Expected: Both PASS.

- [ ] **Step 5: Run full test file to check for regressions**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v`

Expected: All tests PASS. The existing DAS tests still mock `add_das` directly — they are unaffected because Task 2 hasn't changed the DAS path yet.

---

### Task 2: Replace DAS with action-linked frames + chunk_size=10

**Files:**
- Modify: `src/ingestion/tracking_context.py:675-692` (step 12 in `_enrich_match`)
- Modify: `src/ingestion/tracking_context.py:610` (add `get_das` import)
- Test: `src/tests/test_tracking_context_udf.py`

- [ ] **Step 1: Write failing test for DAS on action-linked frames**

Add this test to `src/tests/test_tracking_context_udf.py`:

```python
def test_das_uses_action_linked_frames_and_chunk_size(caplog) -> None:
    """DAS calls get_das with only action-linked frame_ids and chunk_size=10."""
    import logging
    from unittest.mock import MagicMock, patch

    import numpy as np
    import pandas as pd

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()

    # Create frames with multiple frame_ids — only frame 250 is action-linked
    rows = []
    for fid in [100, 200, 250, 300, 400]:
        rows.append({
            "game_id": 1,
            "frame_id": fid,
            "period_id": 1,
            "time_seconds": fid / 25.0,
            "player_id": "DFL-OBJ-0001LJ",
            "team_id": "DFL-CLU-000005",
            "x": 50.0,
            "y": 34.0,
            "vx": 0.0,
            "vy": 0.0,
            "speed": 0.0,
            "ax": 0.0,
            "ay": 0.0,
            "is_goalkeeper": False,
            "is_ball": False,
            "source_provider": "idsse",
        })
    frames = pd.DataFrame(rows)

    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    def pc_passthrough(actions, frames, method="spearman", **kwargs):
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    # link_actions_to_frames: link action 0 to frame 250
    def mock_link(actions, frames, **kwargs):
        links = pd.DataFrame({
            "action_id": actions["action_id"].values,
            "frame_id": pd.array([250] * len(actions), dtype="Int64"),
            "time_offset_seconds": [0.0] * len(actions),
            "n_candidate_frames": [1] * len(actions),
            "link_quality_score": [1.0] * len(actions),
        })
        return links, None

    def mock_infer(frames, **kwargs):
        return pd.DataFrame(columns=["game_id", "frame_id", "period_id", "carrier_player_id", "carrier_team_id"])

    def mock_tip(frames, carrier, **kwargs):
        f = frames.copy()
        f["team_in_possession"] = pd.NA
        return f

    # Capture get_das call — return a plausible DAS result
    mock_get_das = MagicMock()
    mock_get_das.return_value = pd.DataFrame({
        "game_id": [1, 1],
        "frame_id": [250, 250],
        "period_id": [1, 1],
        "player_id": ["DFL-OBJ-0001LJ", "ball"],
        "team_id": ["DFL-CLU-000005", pd.NA],
        "is_ball": [False, True],
        "DAS": [0.42, np.nan],
    })

    patches = [
        patch("silly_kicks.tracking.link_actions_to_frames", mock_link),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        patch("silly_kicks.tracking.add_pressure_on_actor", passthrough),
        patch("silly_kicks.tracking.pitch_control_at_action", pc_passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.infer_ball_carrier", mock_infer),
        patch("silly_kicks.tracking.derive_team_in_possession", mock_tip),
        patch("silly_kicks.tracking._das.get_das", mock_get_das),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]
    for p in patches:
        p.start()
    try:
        with caplog.at_level(logging.ERROR, logger="ingestion.tracking_context"):
            result = _enrich_match(
                actions=actions,
                frames=frames,
                xt=_make_dummy_xt(),  # type: ignore[arg-type]
                home_team_id="DFL-CLU-000005",
                match_id_native="test",
                data_source="idsse",
            )
    finally:
        for p in patches:
            p.stop()

    # Verify get_das was called
    mock_get_das.assert_called_once()

    # Verify chunk_size=10 was passed
    _, kwargs = mock_get_das.call_args
    assert kwargs.get("chunk_size") == 10, f"Expected chunk_size=10, got {kwargs}"

    # Verify get_das received only action-linked frame_ids (250), not all frames
    das_frames_arg = mock_get_das.call_args[0][0]  # first positional arg
    actual_frame_ids = sorted(das_frames_arg["frame_id"].unique().tolist())
    assert actual_frame_ids == [250], f"Expected [250], got {actual_frame_ids}"

    # Verify das columns exist in output
    assert "das_team" in result.columns
    assert "das_opponent" in result.columns
    assert "das_diff" in result.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_udf.py::test_das_uses_action_linked_frames_and_chunk_size -v`

Expected: FAIL — current code calls `add_das(actions, frames, links=links)` which doesn't call `get_das` directly, so the mock on `silly_kicks.tracking._das.get_das` is not hit in the right way.

- [ ] **Step 3: Implement the DAS replacement in `_enrich_match`**

In `src/ingestion/tracking_context.py`, make two changes:

**3a.** Add `get_das` import at the top of `_enrich_match` (line ~606, inside the existing import block). Add `import pandas as pd` to the top-of-function import section if not already present for `pd.isna`:

In the import block starting at line 606, add `get_das` import. The function already has `import pandas as pd` at module level via `TYPE_CHECKING`. Inside the function body, `pd` is available because `_enrich_match` takes `pd.DataFrame` args. However, `pd.isna` is used in the DAS mapping loop, which is fine since pandas is imported at module level for type hints and available at runtime from the `frames` parameter.

Replace the step 12 block (lines 675-692):

```python
    # Step 12: DAS (ball-carrier prerequisite + defense-in-depth wrapper)
    # infer_ball_carrier derives team_in_possession from tracking frames — a
    # mandatory DAS input. Defense-in-depth catches (IndexError, ValueError,
    # RuntimeError) and degrades DAS columns to NaN with ERROR logging.
    # This wrapper is permanent — DAS needs possession data by design.
    try:
        carrier = infer_ball_carrier(frames)
        frames = derive_team_in_possession(frames, carrier)
        del carrier
        actions = add_das(actions, frames, links=links)
    except (IndexError, ValueError, RuntimeError) as exc:
        logger.error(
            "DAS degraded to NaN for match_id=%s: %s: %s",
            match_id_native,
            type(exc).__name__,
            exc,
        )
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

With:

```python
    # Step 12: DAS (action-linked frames + chunk_size=10)
    # Bypasses add_das because _precompute_das_lookup does not expose chunk_size.
    # TODO: Switch back to add_das once silly-kicks PR-S40 ships das_kwargs passthrough.
    import pandas as pd

    from silly_kicks.tracking._das import get_das

    try:
        # ── Ball-carrier on ALL frames (contiguous → correct hysteresis) ──
        carrier = infer_ball_carrier(frames)
        frames_with_tip = derive_team_in_possession(frames, carrier)
        del carrier

        # ── Filter to action-linked frame_ids only ──
        # links has (action_id, frame_id) but no period_id — join via actions
        linked = links[["action_id", "frame_id"]].dropna(subset=["frame_id"])
        linked = linked.merge(actions[["action_id", "period_id"]], on="action_id", how="left")
        linked_frame_ids = linked[["period_id", "frame_id"]].drop_duplicates()
        das_frames = frames_with_tip.merge(linked_frame_ids, on=["period_id", "frame_id"], how="inner")
        del linked, frames_with_tip

        # ── Direct get_das with chunk_size=10 (bypasses add_das) ──
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
            match_id_native,
            type(exc).__name__,
            exc,
        )
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan
```

Also remove `add_das` from the import block at line 610 (it's no longer used):

Replace:
```python
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_das,
        add_defensive_line,
```

With:
```python
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_defensive_line,
```

- [ ] **Step 4: Update `_make_enrichment_patches` to mock the new DAS path**

The existing `_make_enrichment_patches` helper mocks `silly_kicks.tracking.add_das`. Since step 12 now calls `silly_kicks.tracking._das.get_das` directly instead, update the helper.

In `src/tests/test_tracking_context_udf.py`, replace the `_make_enrichment_patches` function:

```python
def _make_enrichment_patches(actions, mock_get_das_side_effect=None):
    """Build patch list for all silly-kicks enrichment functions in _enrich_match.

    Mocks all enrichment steps to pass through their first arg unchanged,
    except get_das which uses the provided side_effect. Isolates DAS exception
    handling from the 14 other enrichment steps.

    Args:
        actions: Actions DataFrame (used to build mock links).
        mock_get_das_side_effect: Side effect for the get_das mock. If a
            callable, it's called with (frames, **kwargs). If an exception
            class/instance, it's raised. If None, returns an empty DataFrame.
    """
    from unittest.mock import patch

    import pandas as pd

    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    # pitch_control_at_action returns a Series (not DataFrame), so needs a
    # special mock that returns a named NaN series matching actions length.
    def pc_passthrough(actions, frames, method="spearman", **kwargs):
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    # infer_ball_carrier returns an empty carrier DataFrame; derive_team_in_possession
    # adds a NaN team_in_possession column. Both are mocked to isolate DAS tests
    # from ball-carrier inference (which needs ball_state, is_ball, etc.).
    def mock_infer_ball_carrier(frames, **kwargs):
        return pd.DataFrame(columns=["game_id", "frame_id", "period_id", "carrier_player_id", "carrier_team_id"])

    def mock_derive_tip(frames, carrier, **kwargs):
        frames = frames.copy()
        frames["team_in_possession"] = pd.NA
        return frames

    # Default get_das mock returns empty result
    if mock_get_das_side_effect is None:
        mock_get_das_side_effect = lambda frames, **kwargs: pd.DataFrame(  # noqa: E731
            columns=["game_id", "frame_id", "period_id", "player_id", "team_id", "is_ball", "DAS"]
        )

    # links must include frame_id — the new DAS code accesses links[["action_id", "frame_id"]]
    # before calling get_das. frame_id=0 won't match any real frame rows, so das_frames will
    # be empty after the inner merge. For error tests, get_das raises before processing the
    # empty DataFrame. For the default case, DAS lookup is empty → all NaN.
    mock_links = pd.DataFrame({
        "action_id": actions["action_id"].values,
        "frame_id": pd.array([0] * len(actions), dtype="Int64"),
        "time_offset_seconds": [0.0] * len(actions),
        "n_candidate_frames": [1] * len(actions),
        "link_quality_score": [1.0] * len(actions),
    })

    return [
        patch("silly_kicks.tracking.link_actions_to_frames", return_value=(mock_links, None)),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        patch("silly_kicks.tracking.add_pressure_on_actor", passthrough),
        patch("silly_kicks.tracking.pitch_control_at_action", pc_passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.infer_ball_carrier", mock_infer_ball_carrier),
        patch("silly_kicks.tracking.derive_team_in_possession", mock_derive_tip),
        patch("silly_kicks.tracking._das.get_das", side_effect=mock_get_das_side_effect),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]
```

- [ ] **Step 5: Update existing DAS tests to use new helper signature**

In `src/tests/test_tracking_context_udf.py`, update the three existing DAS tests.

Replace `test_das_index_error_degrades_gracefully`:

```python
def test_das_index_error_degrades_gracefully(caplog) -> None:
    """DAS IndexError fills 3 columns with NaN + logs ERROR (defense-in-depth)."""
    import logging

    import numpy as np

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    patches = _make_enrichment_patches(
        actions,
        mock_get_das_side_effect=IndexError("edge-case frame geometry"),
    )
    for p in patches:
        p.start()
    try:
        with caplog.at_level(logging.ERROR, logger="ingestion.tracking_context"):
            result = _enrich_match(
                actions=actions,
                frames=frames,
                xt=_make_dummy_xt(),  # type: ignore[arg-type]
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
    assert "DAS degraded" in caplog.text
    assert "IndexError" in caplog.text
```

Replace `test_das_value_error_degrades_gracefully`:

```python
def test_das_value_error_degrades_gracefully(caplog) -> None:
    """ValueError in DAS chain degrades to NaN + logs ERROR (defense-in-depth)."""
    import logging

    import numpy as np

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    patches = _make_enrichment_patches(
        actions,
        mock_get_das_side_effect=ValueError("DAS prerequisite missing"),
    )
    for p in patches:
        p.start()
    try:
        with caplog.at_level(logging.ERROR, logger="ingestion.tracking_context"):
            result = _enrich_match(
                actions=actions,
                frames=frames,
                xt=_make_dummy_xt(),  # type: ignore[arg-type]
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
    assert "DAS degraded" in caplog.text
    assert "ValueError" in caplog.text
```

Replace `test_das_uncaught_error_propagates`:

```python
def test_das_uncaught_error_propagates() -> None:
    """Exceptions NOT in the DAS catch list must propagate (ADR-002 section 5)."""
    import pytest

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    patches = _make_enrichment_patches(
        actions,
        mock_get_das_side_effect=TypeError("unexpected type error in DAS"),
    )
    for p in patches:
        p.start()
    try:
        with pytest.raises(TypeError, match="unexpected type error in DAS"):
            _enrich_match(
                actions=actions,
                frames=frames,
                xt=_make_dummy_xt(),  # type: ignore[arg-type]
                home_team_id="DFL-CLU-000005",
                match_id_native="test",
                data_source="idsse",
            )
    finally:
        for p in patches:
            p.stop()
```

- [ ] **Step 6: Run all tests to verify**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v`

Expected: All tests PASS, including both new tests (bekkers + DAS linked frames) and updated existing tests.

- [ ] **Step 7: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py && uv run ruff format --check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py && uv run pyright src/ingestion/tracking_context.py`

Expected: Zero violations. Fix any issues before proceeding.

---

### Task 3: Wheel bump + commit

**Files:**
- Modify: `pyproject.toml` (version field)
- Modify: `src/shared/wheel.py` (WHEEL_VERSION constant)
- Modify: ~27 consumer files (wheel URL references)

- [ ] **Step 1: Bump version in pyproject.toml**

In `pyproject.toml`, change the `version` field from `"0.3.53"` to `"0.3.54"`.

- [ ] **Step 2: Run bump_wheel.py to propagate**

Run: `uv run python scripts/bump_wheel.py`

This updates `src/shared/wheel.py` and all consumer files that reference the wheel filename.

- [ ] **Step 3: Verify wheel constant test passes**

Run: `uv run pytest src/tests/test_wheel_constants.py -v`

Expected: PASS — version in `pyproject.toml` matches `WHEEL_VERSION` in `wheel.py`.

- [ ] **Step 4: Run full tracking context test suite**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v`

Expected: All tests PASS.

- [ ] **Step 5: Run ruff on changed files**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/`

Expected: Zero violations.

- [ ] **Step 6: Commit**

Stage all changed files and commit:

```
fix(tracking-context): DAS OOM + bekkers_pi graceful degradation

- DAS: compute on action-linked frames only (vs all frames per batch)
  with chunk_size=10, bypassing add_das which doesn't expose chunk_size.
  Reduces peak memory to well within the 1 GB UDF limit.
  Ball-carrier inference runs on all frames for correct hysteresis.
- Bekkers: split pressure into ball-independent methods (always compute)
  + bekkers_pi with ValueError catch on missing ball rows (Metrica).
- Bump wheel 0.3.53 → 0.3.54
```
