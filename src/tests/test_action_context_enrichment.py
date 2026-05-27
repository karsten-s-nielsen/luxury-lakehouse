# src/tests/test_action_context_enrichment.py
"""Unit tests for action_context enrichment chains.

Mock-patches all silly-kicks add_* calls to verify:
- call ordering and links propagation (tracking chain)
- event-only chain produces game_state + GK resolution only
- output column selection matches _RESULT_COLUMNS
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from ingestion.action_context import (
    _RESULT_COLUMNS,
    _build_output,
    _enrich_event_only_match,
    _enrich_sb360_match,
    _enrich_tracking_match,
    _is_event_only_provider,
    _is_tracking_provider,
)


def _make_actions(n: int = 5) -> pd.DataFrame:
    """Minimal SPADL actions DataFrame for testing."""
    return pd.DataFrame(
        {
            "game_id": ["m1"] * n,
            "action_id": list(range(n)),
            "period_id": [1] * n,
            "time_seconds": [float(i * 10) for i in range(n)],
            "team_id": ["t1"] * n,
            "player_id": ["p1"] * n,
            "type_id": [0] * n,
            "start_x": [50.0] * n,
            "start_y": [34.0] * n,
            "end_x": [60.0] * n,
            "end_y": [34.0] * n,
            "result_id": [1] * n,
            "bodypart_id": [0] * n,
        }
    )


def _make_tracking(n_frames: int = 50) -> pd.DataFrame:
    """Minimal tracking DataFrame."""
    return pd.DataFrame(
        {
            "frame_id": list(range(n_frames)),
            "timestamp": [float(i * 0.04) for i in range(n_frames)],
            "player_id": ["p1"] * n_frames,
            "team_id": ["t1"] * n_frames,
            "x": [50.0] * n_frames,
            "y": [34.0] * n_frames,
        }
    )


def _make_mock_links(actions: pd.DataFrame) -> pd.DataFrame:
    """Mock link report matching action rows."""
    return pd.DataFrame(
        {
            "action_id": actions["action_id"].values,
            "frame_id": pd.array([0] * len(actions), dtype="Int64"),
            "time_offset_seconds": [0.0] * len(actions),
            "n_candidate_frames": [1] * len(actions),
            "link_quality_score": [1.0] * len(actions),
        }
    )


_PASSTHROUGH = lambda actions, *args, **kwargs: actions  # noqa: E731


def test_enrich_event_only_produces_game_state_and_gk() -> None:
    """Event-only chain must add game_state + 4 GK resolution columns."""
    actions = _make_actions()
    with (
        patch(
            "silly_kicks.spadl.add_game_state",
            side_effect=lambda df: df.assign(game_state="drawing"),
        ) as mock_gs,
        patch(
            "silly_kicks.spadl.utils.add_pre_shot_gk_context",
            side_effect=lambda df, **kw: df.assign(
                defending_gk_player_id=np.nan,
                gk_was_distributing=False,
                gk_was_engaged=False,
                gk_actions_in_possession=0,
            ),
        ) as mock_gk,
    ):
        result = _enrich_event_only_match(actions)
    mock_gs.assert_called_once()
    mock_gk.assert_called_once()
    assert "game_state" in result.columns
    assert "defending_gk_player_id" in result.columns
    assert result["game_state"].iloc[0] == "drawing"


def test_enrich_event_only_game_state_values() -> None:
    """game_state values must be winning, losing, or drawing."""
    actions = _make_actions(3)
    actions_with_gs = actions.assign(game_state=pd.array(["winning", "losing", "drawing"]))
    with (
        patch("silly_kicks.spadl.add_game_state", return_value=actions_with_gs),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", side_effect=lambda df, **kw: df),
    ):
        result = _enrich_event_only_match(actions)
    assert set(result["game_state"].unique()) == {"winning", "losing", "drawing"}


def test_enrich_tracking_calls_all_steps_with_links() -> None:
    """Tracking chain must call all 20 add_* steps and propagate links."""
    actions = _make_actions()
    tracking = _make_tracking()
    mock_links = _make_mock_links(actions)
    mock_xt = MagicMock()

    mock_link_fn = MagicMock(return_value=(mock_links, MagicMock()))
    mock_pc = MagicMock(return_value=pd.Series([0.5] * len(actions), name="pitch_control_at_ball__spearman"))
    mock_def_line = MagicMock(side_effect=_PASSTHROUGH)
    mock_action_ctx = MagicMock(side_effect=_PASSTHROUGH)
    mock_das = MagicMock(side_effect=_PASSTHROUGH)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.tracking.link_actions_to_frames", mock_link_fn),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_action_context", mock_action_ctx),
        patch("silly_kicks.tracking.add_actor_pre_window", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pressure_on_actor", _PASSTHROUGH),
        patch("silly_kicks.tracking.pitch_control_at_action", mock_pc),
        patch("silly_kicks.tracking.add_defensive_line", mock_def_line),
        patch("silly_kicks.tracking.add_off_ball_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_line_break", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_team_shape", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_das", mock_das),
        patch("silly_kicks.tracking.add_pre_shot_gk_position", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pre_shot_gk_angle", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_gk_influence", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_cover_shadows", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_shape_graph", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_obso", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pausa", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_space_creation", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_elastic_sync", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_sync_score", _PASSTHROUGH),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_tracking_match(actions, tracking, mock_xt, "t1")
    finally:
        for p in patches:
            p.stop()

    # link_actions_to_frames called once
    mock_link_fn.assert_called_once()
    # pitch_control called 3 times (spearman, fernandez_bornn, voronoi)
    assert mock_pc.call_count == 3
    assert isinstance(result, pd.DataFrame)

    # Verify critical kwargs are propagated
    _, def_kwargs = mock_def_line.call_args
    assert def_kwargs.get("home_team_id") == "t1", "home_team_id not propagated to add_defensive_line"
    assert def_kwargs.get("links") is not None, "links not propagated to add_defensive_line"
    _, ctx_kwargs = mock_action_ctx.call_args
    assert ctx_kwargs.get("links") is not None, "links not propagated to add_action_context"
    _, das_kwargs = mock_das.call_args
    assert das_kwargs.get("chunk_size") == 10, "chunk_size not propagated to add_das"


def test_enrich_sb360_calls_snapshot_converter_and_positional_features() -> None:
    """SB360 chain must call snapshot_to_tracking_frames then single-frame features."""
    actions = _make_actions(3)
    freeze_frames = pd.DataFrame(
        {
            "action_id": [0, 0, 0, 0, 1, 1, 1, 1],
            "team_id": ["t1", "t1", "t2", "t2", "t1", "t1", "t2", "t2"],
            "is_goalkeeper": [True, False, True, False, True, False, True, False],
            "x": [5.0, 40.0, 100.0, 60.0, 5.0, 45.0, 100.0, 55.0],
            "y": [34.0, 20.0, 34.0, 50.0, 34.0, 30.0, 34.0, 40.0],
        }
    )

    mock_frames = pd.DataFrame({"frame_id": [0], "is_ball": [False]})
    mock_links = _make_mock_links(actions.iloc[:2])

    mock_converter = MagicMock(return_value=(mock_frames, mock_links))
    mock_line_break = MagicMock(side_effect=_PASSTHROUGH)
    mock_team_shape = MagicMock(side_effect=_PASSTHROUGH)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.snapshot_to_tracking_frames", mock_converter),
        patch("silly_kicks.tracking.add_action_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_defensive_line", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_line_break", mock_line_break),
        patch("silly_kicks.tracking.add_team_shape", mock_team_shape),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_sb360_match(actions, freeze_frames, "t1")
    finally:
        for p in patches:
            p.stop()

    mock_converter.assert_called_once()
    _, lb_kwargs = mock_line_break.call_args
    assert lb_kwargs.get("method") == "ward", "method='ward' not propagated to add_line_break"
    assert lb_kwargs.get("home_team_id") == "t1", "home_team_id not propagated to add_line_break"
    mock_team_shape.assert_called_once()
    assert isinstance(result, pd.DataFrame)


def test_enrich_sb360_empty_freeze_frames_fallback() -> None:
    """SB360 chain falls back to event-only when converter returns empty frames."""
    actions = _make_actions(2)
    empty_ff = pd.DataFrame(
        {
            "action_id": pd.Series([], dtype="int64"),
            "team_id": pd.Series([], dtype="object"),
            "is_goalkeeper": pd.Series([], dtype="bool"),
            "x": pd.Series([], dtype="float64"),
            "y": pd.Series([], dtype="float64"),
        }
    )

    mock_empty_frames = pd.DataFrame()
    mock_empty_links = pd.DataFrame()
    mock_converter = MagicMock(return_value=(mock_empty_frames, mock_empty_links))
    mock_line_break = MagicMock(side_effect=_PASSTHROUGH)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.snapshot_to_tracking_frames", mock_converter),
        patch("silly_kicks.tracking.add_line_break", mock_line_break),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_sb360_match(actions, empty_ff, "t1")
    finally:
        for p in patches:
            p.stop()

    mock_converter.assert_called_once()
    mock_line_break.assert_not_called()
    assert isinstance(result, pd.DataFrame)


def test_build_output_column_selection() -> None:
    """_build_output must return exactly _RESULT_COLUMNS (minus _ingested_at), NaN-filling missing cols."""
    actions = pd.DataFrame(
        {
            "game_id": ["m1"],
            "action_id": [0],
            "period_id": [1],
            "time_seconds": [10.0],
            "team_id": ["t1"],
            "player_id": ["p1"],
            "type_id": [0],
            "result_id": [1],
            "bodypart_id": [0],
            "start_x": [50.0],
            "start_y": [34.0],
            "end_x": [60.0],
            "end_y": [34.0],
            "game_state": ["drawing"],
            "defending_gk_player_id": ["gk1"],
        }
    )
    with patch("ingestion.action_context._restore_native_identity", side_effect=lambda df: df):
        result = _build_output(actions, match_id_native="native_m1", data_source="statsbomb")
    expected_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    assert list(result.columns) == expected_cols
    assert result["match_id"].iloc[0] == "native_m1"
    assert result["data_source"].iloc[0] == "statsbomb"
    assert "defending_gk_player_id_native" in result.columns
    assert pd.isna(result["pitch_control_at_ball__spearman"].iloc[0])


def test_build_output_type_id_to_type_name() -> None:
    """_build_output must map type_id -> type_name when type_name is absent."""
    actions = pd.DataFrame(
        {
            "game_id": ["m1"],
            "action_id": [0],
            "period_id": [1],
            "time_seconds": [10.0],
            "team_id": ["t1"],
            "player_id": ["p1"],
            "type_id": [0],
            "result_id": [1],
            "bodypart_id": [0],
            "start_x": [50.0],
            "start_y": [34.0],
            "end_x": [60.0],
            "end_y": [34.0],
        }
    )
    with patch("ingestion.action_context._restore_native_identity", side_effect=lambda df: df):
        result = _build_output(actions, match_id_native="native_m1", data_source="idsse")
    assert result["type_name"].iloc[0] == "pass"


def test_provider_tier_classification() -> None:
    """Provider dispatch helpers must correctly classify all 6 providers (3 tiers)."""
    for p in ("idsse", "metrica", "skillcorner", "gradientsports"):
        assert _is_tracking_provider(p), f"{p} should be tracking"
        assert not _is_event_only_provider(p), f"{p} should NOT be event-only"
    for p in ("statsbomb", "wyscout"):
        assert _is_event_only_provider(p), f"{p} should be event-only"
        assert not _is_tracking_provider(p), f"{p} should NOT be tracking"
