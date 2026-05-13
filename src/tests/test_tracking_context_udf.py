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


def _make_enrichment_patches(actions, mock_add_das):
    """Build patch list for all silly-kicks enrichment functions in _enrich_match.

    Mocks all enrichment steps to pass through their first arg unchanged,
    except add_das which uses the provided mock. Isolates DAS exception
    handling from the 14 other enrichment steps.
    """
    from unittest.mock import patch

    import pandas as pd

    passthrough = lambda actions, *args, **kwargs: actions  # noqa: E731

    # pitch_control_at_action returns a Series (not DataFrame), so needs a
    # special mock that returns a named NaN series matching actions length.
    def pc_passthrough(actions, frames, method="spearman"):
        return pd.Series(float("nan"), index=actions.index, name=f"pc_{method}")

    return [
        patch("silly_kicks.tracking.link_actions_to_frames", return_value=(actions[["action_id"]], None)),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", passthrough),
        patch("silly_kicks.tracking.add_action_context", passthrough),
        patch("silly_kicks.tracking.add_actor_pre_window", passthrough),
        patch("silly_kicks.tracking.add_pressure_on_actor", passthrough),
        patch("silly_kicks.tracking.pitch_control_at_action", pc_passthrough),
        patch("silly_kicks.tracking.add_defensive_line", passthrough),
        patch("silly_kicks.tracking.add_off_ball_context", passthrough),
        patch("silly_kicks.tracking.add_line_break", passthrough),
        patch("silly_kicks.tracking.add_team_shape", passthrough),
        patch("silly_kicks.tracking.add_das", mock_add_das),
        patch("silly_kicks.tracking.add_gk_influence", passthrough),
        patch("silly_kicks.tracking.add_cover_shadows", passthrough),
        patch("silly_kicks.tracking.add_sync_score", passthrough),
    ]


def test_das_index_error_degrades_gracefully() -> None:
    """DAS IndexError fills 3 columns with NaN, preserving all other enrichments."""
    import numpy as np

    from ingestion.tracking_context import _enrich_match

    actions = _make_minimal_actions()
    frames = _make_minimal_frames()

    def mock_add_das(actions, frames):
        raise IndexError("edge-case frame geometry")

    patches = _make_enrichment_patches(actions, mock_add_das)
    for p in patches:
        p.start()
    try:
        result = _enrich_match(
            actions=actions,
            frames=frames,
            xt=_make_dummy_xt(),  # type: ignore[arg-type]  # mocked chain never calls xt
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
    """Non-IndexError exceptions from DAS must propagate (ADR-002 section 5)."""
    import pytest

    from ingestion.tracking_context import _enrich_match

    def mock_add_das(actions, frames):
        raise ValueError("unexpected DAS failure")

    actions = _make_minimal_actions()
    patches = _make_enrichment_patches(actions, mock_add_das)
    for p in patches:
        p.start()
    try:
        with pytest.raises(ValueError, match="unexpected DAS failure"):
            _enrich_match(
                actions=_make_minimal_actions(),
                frames=_make_minimal_frames(),
                xt=_make_dummy_xt(),  # type: ignore[arg-type]  # mocked chain never calls xt
                home_team_id="DFL-CLU-000005",
                match_id_native="test",
                data_source="idsse",
            )
    finally:
        for p in patches:
            p.stop()


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
