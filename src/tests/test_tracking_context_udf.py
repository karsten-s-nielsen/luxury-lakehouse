"""Tests for tracking context applyInPandas UDF components."""

from __future__ import annotations

import pytest


def test_result_schema_matches_result_columns() -> None:
    """_get_result_schema() field names match _RESULT_COLUMNS (minus _ingested_at)."""
    pytest.importorskip("pyspark")
    from ingestion.tracking_context import _RESULT_COLUMNS, _get_result_schema

    schema = _get_result_schema()
    expected = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    actual = [f.name for f in schema.fields]  # type: ignore[attr-defined]
    assert actual == expected, f"Schema mismatch:\n  expected={expected}\n  actual={actual}"


def test_result_schema_field_count() -> None:
    """_get_result_schema() has 82 fields (83 columns minus _ingested_at)."""
    pytest.importorskip("pyspark")
    from ingestion.tracking_context import _get_result_schema

    schema = _get_result_schema()
    assert len(schema.fields) == 82, f"Expected 82 fields, got {len(schema.fields)}"  # type: ignore[attr-defined]


def test_xt_grid_round_trip() -> None:
    """Serialize xT grid via tolist(), reconstruct, assert array equality."""
    import numpy as np

    from ingestion.tracking_context import _deserialize_xt_grid, _serialize_xt_grid

    original = np.random.default_rng(42).random((12, 16))
    serialized = _serialize_xt_grid(original, grid_l=16, grid_w=12)

    assert isinstance(serialized, dict)
    assert "xt_grid" in serialized
    assert "l" in serialized
    assert "w" in serialized
    assert serialized["l"] == 16
    assert serialized["w"] == 12
    assert isinstance(serialized["xt_grid"], list)
    assert len(serialized["xt_grid"]) == 12
    assert len(serialized["xt_grid"][0]) == 16

    reconstructed = _deserialize_xt_grid(serialized)
    np.testing.assert_array_equal(reconstructed, original)


def test_xt_grid_json_serializable() -> None:
    """_serialize_xt_grid output is JSON-serializable (Databricks task values are JSON)."""
    import json

    import numpy as np

    from ingestion.tracking_context import _serialize_xt_grid

    grid = np.ones((12, 16), dtype=np.float64)
    data = _serialize_xt_grid(grid, grid_l=16, grid_w=12)

    json_str = json.dumps(data)
    restored = json.loads(json_str)

    assert restored["l"] == 16
    assert restored["w"] == 12
    assert len(restored["xt_grid"]) == 12
    assert len(restored["xt_grid"][0]) == 16


def test_actions_records_round_trip() -> None:
    """Actions DataFrame survives to_dict('records') -> pd.DataFrame round-trip."""
    import pandas as pd

    original = pd.DataFrame(
        {
            "game_id": [1, 1, 1],
            "action_id": [0, 1, 2],
            "period_id": [1, 1, 2],
            "time_seconds": [10.5, 25.3, 0.1],
            "team_id": ["T1", "T2", "T1"],
            "player_id": ["P1", "P2", "P3"],
            "type_id": [0, 1, 0],
            "result_id": [1, 0, 1],
            "bodypart_id": [0, 0, 1],
            "start_x": [50.0, 30.0, 52.5],
            "start_y": [34.0, 20.0, 34.0],
            "end_x": [60.0, 40.0, 55.0],
            "end_y": [34.0, 25.0, 30.0],
        }
    )
    records = original.to_dict("records")
    reconstructed = pd.DataFrame(records)
    pd.testing.assert_frame_equal(reconstructed, original)


def test_udf_factory_returns_callable() -> None:
    """_make_tracking_context_udf returns a callable closure."""
    from ingestion.tracking_context import _make_tracking_context_udf

    udf_fn = _make_tracking_context_udf(
        provider="metrica",
        home_team_id="Home",
        home_start_left=True,
        xt_grid_data=[[0.0] * 16 for _ in range(12)],
        xt_l=16,
        xt_w=12,
        actions_records=[
            {
                "game_id": 1,
                "action_id": 0,
                "period_id": 1,
                "time_seconds": 10.0,
                "team_id": "Home",
                "player_id": "P1",
                "type_id": 0,
                "result_id": 1,
                "bodypart_id": 0,
                "start_x": 50.0,
                "start_y": 34.0,
                "end_x": 60.0,
                "end_y": 34.0,
            }
        ],
        native_match_id="test_match",
    )
    assert callable(udf_fn)


def test_frame_batch_constants() -> None:
    """Frame batch constants are consistent and sensible."""
    from ingestion.tracking_context import _ACTION_TIME_BUFFER_SECONDS, _FRAME_BATCH_SIZE

    assert _FRAME_BATCH_SIZE == 250
    assert _ACTION_TIME_BUFFER_SECONDS == 0.5


def test_udf_filters_actions_by_period() -> None:
    """UDF filters actions to the batch's period — period 2 actions excluded from period 1 batch."""
    import pandas as pd

    from ingestion.tracking_context import _RESULT_COLUMNS, _make_tracking_context_udf

    # Two actions: period 1 and period 2
    actions_records = [
        {
            "game_id": 1,
            "action_id": 0,
            "period_id": 1,
            "time_seconds": 10.0,
            "team_id": "H",
            "player_id": "P1",
            "type_id": 0,
            "result_id": 1,
            "bodypart_id": 0,
            "start_x": 50.0,
            "start_y": 34.0,
            "end_x": 60.0,
            "end_y": 34.0,
        },
        {
            "game_id": 1,
            "action_id": 1,
            "period_id": 2,
            "time_seconds": 5.0,
            "team_id": "H",
            "player_id": "P2",
            "type_id": 0,
            "result_id": 1,
            "bodypart_id": 0,
            "start_x": 30.0,
            "start_y": 20.0,
            "end_x": 40.0,
            "end_y": 25.0,
        },
    ]

    udf_fn = _make_tracking_context_udf(
        provider="metrica",
        home_team_id="Home",
        home_start_left=True,
        xt_grid_data=[[0.0] * 16 for _ in range(12)],
        xt_l=16,
        xt_w=12,
        actions_records=actions_records,
        native_match_id="test",
    )

    # Empty tracking batch for period 1 — should return empty (no tracking data to convert)
    empty_pdf = pd.DataFrame(columns=pd.Index(["match_id", "period", "frame_batch_id", "timestamp"]))
    result = udf_fn(empty_pdf)
    output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    assert list(result.columns) == output_cols
    assert len(result) == 0


def _make_dummy_xt():
    """12x16 xT grid of zeros for tests that don't exercise xT values."""
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


def _make_enrichment_patches(actions, mock_get_das_side_effect=None):
    """Build patch list for all silly-kicks enrichment functions in _enrich_match.

    Mocks all enrichment steps to pass through their first arg unchanged,
    except get_individual_das which uses the provided side_effect. Isolates DAS
    exception handling from the 14 other enrichment steps.

    Args:
        actions: Actions DataFrame (used to build mock links).
        mock_get_das_side_effect: Side effect for the get_individual_das mock.
            If a callable, it's called with (frames, **kwargs). If an exception
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
    mock_links = pd.DataFrame(
        {
            "action_id": actions["action_id"].values,
            "frame_id": pd.array([0] * len(actions), dtype="Int64"),
            "time_offset_seconds": [0.0] * len(actions),
            "n_candidate_frames": [1] * len(actions),
            "link_quality_score": [1.0] * len(actions),
        }
    )

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
        patch("silly_kicks.tracking._das.get_individual_das", side_effect=mock_get_das_side_effect),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]


def test_das_uses_action_linked_frames_and_chunk_size(caplog) -> None:
    """DAS calls get_individual_das with only action-linked frame_ids and chunk_size=10."""
    import logging
    from unittest.mock import MagicMock, patch

    import numpy as np
    import pandas as pd

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()

    # Create frames with multiple frame_ids — only frame 250 is action-linked
    rows = []
    for fid in [100, 200, 250, 300, 400]:
        rows.append(
            {
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
            }
        )
    frames = pd.DataFrame(rows)

    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    def pc_passthrough(actions, frames, method="spearman", **kwargs):
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    # link_actions_to_frames: link action 0 to frame 250
    def mock_link(actions, frames, **kwargs):
        links = pd.DataFrame(
            {
                "action_id": actions["action_id"].values,
                "frame_id": pd.array([250] * len(actions), dtype="Int64"),
                "time_offset_seconds": [0.0] * len(actions),
                "n_candidate_frames": [1] * len(actions),
                "link_quality_score": [1.0] * len(actions),
            }
        )
        return links, None

    def mock_infer(frames, **kwargs):
        return pd.DataFrame(columns=["game_id", "frame_id", "period_id", "carrier_player_id", "carrier_team_id"])

    def mock_tip(frames, carrier, **kwargs):
        f = frames.copy()
        f["team_in_possession"] = pd.NA
        return f

    # Capture get_individual_das call — return a plausible DAS result
    mock_get_das = MagicMock()
    mock_get_das.return_value = pd.DataFrame(
        {
            "game_id": [1, 1],
            "frame_id": [250, 250],
            "period_id": [1, 1],
            "player_id": ["DFL-OBJ-0001LJ", "ball"],
            "team_id": ["DFL-CLU-000005", pd.NA],
            "is_ball": [False, True],
            "DAS": [0.42, np.nan],
        }
    )

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
        patch("silly_kicks.tracking._das.get_individual_das", mock_get_das),
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

    # Verify get_individual_das was called
    mock_get_das.assert_called_once()

    # Verify chunk_size=10 was passed
    _, kwargs = mock_get_das.call_args
    assert kwargs.get("chunk_size") == 10, f"Expected chunk_size=10, got {kwargs}"

    # Verify get_individual_das received only action-linked frame_ids (250), not all frames
    das_frames_arg = mock_get_das.call_args[0][0]  # first positional arg
    actual_frame_ids = sorted(das_frames_arg["frame_id"].unique().tolist())
    assert actual_frame_ids == [250], f"Expected [250], got {actual_frame_ids}"

    # Verify das columns exist in output
    assert "das_team" in result.columns
    assert "das_opponent" in result.columns
    assert "das_diff" in result.columns


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


def test_das_value_error_degrades_gracefully(caplog) -> None:
    """ValueError in DAS chain degrades to NaN + logs ERROR (defense-in-depth).

    Before TC-1c, ValueError propagated. Now it is caught because the
    ball-carrier -> DAS chain can raise ValueError on missing prerequisites.
    """
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


def test_das_uncaught_error_propagates() -> None:
    """Exceptions NOT in the DAS catch list must propagate (ADR-002 section 5).

    The defense-in-depth wrapper catches (IndexError, ValueError, RuntimeError, TypeError).
    KeyError is outside this list and must crash the UDF group loudly.
    """
    import pytest

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    patches = _make_enrichment_patches(
        actions,
        mock_get_das_side_effect=KeyError("unexpected key error in DAS"),
    )
    for p in patches:
        p.start()
    try:
        with pytest.raises(KeyError, match="unexpected key error in DAS"):
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


def test_udf_logs_error_on_exception(caplog) -> None:
    """UDF wrapper logs ERROR with actual exception before re-raising (ADR-002)."""
    import logging
    from unittest.mock import patch

    import pandas as pd
    import pytest

    from ingestion.tracking_context import _make_tracking_context_udf

    udf_fn = _make_tracking_context_udf(
        provider="idsse",
        home_team_id="T1",
        home_start_left=True,
        xt_grid_data=[[0.0] * 16] * 12,
        xt_l=16,
        xt_w=12,
        actions_records=[
            {
                "game_id": 1,
                "action_id": 0,
                "period_id": 1,
                "time_seconds": 10.0,
                "team_id": "T1",
                "player_id": "P1",
                "type_id": 0,
                "result_id": 1,
                "bodypart_id": 0,
                "start_x": 50.0,
                "start_y": 34.0,
                "end_x": 60.0,
                "end_y": 34.0,
            }
        ],
        native_match_id="test_match",
    )

    # Non-empty DataFrame to get past empty-check, trigger conversion path
    pdf = pd.DataFrame(
        {
            "match_id": ["test_match"],
            "period": [1],
            "frame_batch_id": [0],
            "timestamp": [10.0],
        }
    )

    mock_frames = pd.DataFrame({"game_id": [1], "frame_id": [0]})
    with (
        patch(
            "ingestion.tracking_context._bronze_idsse_to_sportec_input",
            return_value=pd.DataFrame({"col": [1]}),
        ),
        patch(
            "silly_kicks.tracking.sportec.convert_to_frames",
            return_value=(mock_frames, None),
        ),
        patch(
            "ingestion.tracking_context._enrich_match",
            side_effect=ValueError("test enrichment error"),
        ),
        caplog.at_level(logging.ERROR, logger="tracking_context_udf"),
    ):
        with pytest.raises(RuntimeError, match=r"(?s)tracking_context UDF failed.*ValueError.*test enrichment error"):
            udf_fn(pdf)

    assert "ValueError" in caplog.text, f"Expected 'ValueError' in log, got: {caplog.text}"
    assert "test enrichment error" in caplog.text, f"Expected 'test enrichment error' in log, got: {caplog.text}"


def test_udf_empty_batch_returns_empty() -> None:
    """UDF returns empty DataFrame when the tracking batch has no rows."""
    import pandas as pd

    from ingestion.tracking_context import _RESULT_COLUMNS, _make_tracking_context_udf

    udf_fn = _make_tracking_context_udf(
        provider="idsse",
        home_team_id="T1",
        home_start_left=True,
        xt_grid_data=[[0.0] * 16 for _ in range(12)],
        xt_l=16,
        xt_w=12,
        actions_records=[],
        native_match_id="test",
    )
    empty_pdf = pd.DataFrame()
    result = udf_fn(empty_pdf)
    output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    assert list(result.columns) == output_cols
    assert len(result) == 0


def test_bekkers_pi_valueerror_propagates_unconditionally() -> None:
    """silly-kicks 4.0+ falls back per-action on missing ball rows (no raise), so
    the legacy ``is_ball=True``-message try/except was deleted. Any ValueError
    from add_pressure_on_actor now propagates unconditionally — this guard
    catches a future regression that re-introduces silent swallowing.

    Replaces the pre-4.0 ``test_bekkers_pi_degrades_on_missing_ball_rows`` +
    ``test_bekkers_pi_unrelated_valueerror_propagates`` pair, both of which
    asserted the now-deleted wrapper's behavior.
    """
    from unittest.mock import patch

    import pandas as pd
    import pytest

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    def pc_passthrough(actions, frames, method="spearman", **kwargs):
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    def mock_link(actions, frames, **kwargs):
        links = pd.DataFrame(
            {
                "action_id": actions["action_id"].values,
                "frame_id": pd.array([1] * len(actions), dtype="Int64"),
                "time_offset_seconds": [0.0] * len(actions),
                "n_candidate_frames": [1] * len(actions),
                "link_quality_score": [1.0] * len(actions),
            }
        )
        return links, None

    def mock_pressure(actions, frames, *, links=None, methods=("andrienko_oval",), **kwargs):
        if "bekkers_pi" in methods:
            # 4.0 doesn't normally raise on missing ball rows (per-action fallback),
            # but if it does (genuine data shape error), we want propagation.
            raise ValueError("simulated bekkers ValueError")
        return actions

    def mock_infer(frames, **kwargs):
        return pd.DataFrame(columns=["game_id", "frame_id", "period_id", "carrier_player_id", "carrier_team_id"])

    def mock_tip(frames, carrier, **kwargs):
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
        patch("silly_kicks.tracking._das.get_individual_das", side_effect=ValueError("no TIP")),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]
    for p in patches:
        p.start()
    try:
        with pytest.raises(ValueError, match="simulated bekkers ValueError"):
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
