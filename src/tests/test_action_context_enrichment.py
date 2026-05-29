# src/tests/test_action_context_enrichment.py
"""Unit tests for action_context enrichment chains.

Mock-patches all silly-kicks add_* calls to verify:
- call ordering and links propagation (tracking chain)
- event-only chain produces game_state + GK resolution only
- output column selection matches _RESULT_COLUMNS
"""

from __future__ import annotations

import logging
import sys
from collections import namedtuple
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from analytics.action_context.enrich import _enrich_tracking_match
from analytics.action_context.schema import RESULT_COLUMNS as _RESULT_COLUMNS
from ingestion.action_context import (
    _ActionContextGuard,
    _build_output,
    _enrich_event_only_match,
    _enrich_sb360_match,
    _find_event_only_new_ids,
    _find_idsse_new_period_pairs,
    _find_tracking_new_ids,
    _is_event_only_provider,
    _is_tracking_provider,
    _load_xt_grid_from_delta,
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
        # DAS setup (added with the team_in_possession fix): stubbed so the synthetic
        # frames (no is_ball) don't reach the real ball-carrier inference.
        patch("silly_kicks.tracking.infer_ball_carrier", MagicMock(return_value=tracking)),
        patch("silly_kicks.tracking.derive_team_in_possession", MagicMock(return_value=tracking)),
        patch("silly_kicks.tracking.add_das", mock_das),
        patch("silly_kicks.tracking.add_pre_shot_gk_position", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pre_shot_gk_angle", _PASSTHROUGH),
        # Ghost-GK (silly-kicks 3.24.0+) — patched so the real bundled model isn't loaded here.
        patch("silly_kicks.tracking.features.add_ghost_gk", _PASSTHROUGH),
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
    with patch("analytics.action_context.schema._restore_native_identity", side_effect=lambda df: df):
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
    with patch("analytics.action_context.schema._restore_native_identity", side_effect=lambda df: df):
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


# ── _load_xt_grid_from_delta tests ───────────────────────────────────


Row = namedtuple("Row", ["zone_x", "zone_y", "xt_value"])


def test_load_xt_grid_from_delta_returns_correct_shape() -> None:
    """Grid loaded from Delta must have correct dimensions and values."""
    mock_rows = [Row(x, y, round(0.01 * (x + 1), 5)) for x in range(16) for y in range(12)]
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = mock_rows
    task_logger = logging.getLogger("test")

    grid_data, xt_l, xt_w = _load_xt_grid_from_delta(mock_spark, "soccer_analytics", "bronze", task_logger)

    assert xt_l == 16
    assert xt_w == 12
    assert len(grid_data) == 12  # outer dimension is w (rows)
    assert len(grid_data[0]) == 16  # inner dimension is l (cols)
    # zone_x=0, zone_y=0 should be 0.01
    assert grid_data[0][0] == pytest.approx(0.01)
    # zone_x=15, zone_y=11 should be 0.16
    assert grid_data[11][15] == pytest.approx(0.16)


def test_load_xt_grid_from_delta_queries_global_grid() -> None:
    """Must query bronze.expected_threat_grids WHERE competition_id = 'global'."""
    mock_rows = [Row(0, 0, 0.05)]
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = mock_rows
    task_logger = logging.getLogger("test")

    _load_xt_grid_from_delta(mock_spark, "cat", "sch", task_logger)

    sql_arg = mock_spark.sql.call_args[0][0]
    assert "cat.sch.expected_threat_grids" in sql_arg
    assert "competition_id = 'global'" in sql_arg


def test_load_xt_grid_from_delta_raises_on_empty_result() -> None:
    """Must raise RuntimeError when no global grid exists (bootstrap case)."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []
    task_logger = logging.getLogger("test")

    with pytest.raises(RuntimeError, match="No global xT grid found"):
        _load_xt_grid_from_delta(mock_spark, "soccer_analytics", "bronze", task_logger)


def test_load_xt_grid_from_delta_raises_on_missing_table() -> None:
    """Must propagate Spark exception when the table does not exist."""
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = Exception("Table or view not found")
    task_logger = logging.getLogger("test")

    with pytest.raises(Exception, match="Table or view not found"):
        _load_xt_grid_from_delta(mock_spark, "soccer_analytics", "bronze", task_logger)


# ── Guard query functions tests ───────────────────────────────────────
# These verify the Spark-native join logic used by the preflight guard.
# Mock DataFrames simulate the join/filter/collect chain.


@pytest.fixture(autouse=False)
def _mock_pyspark():
    """Inject mock pyspark.sql.functions so Spark-native helpers can import it."""
    mock_functions = MagicMock()
    # F.col(...).cast(...).alias(...) must chain — returns MagicMock (passthrough)
    mock_functions.col.return_value = MagicMock()

    prev = {
        "pyspark": sys.modules.get("pyspark"),
        "pyspark.sql": sys.modules.get("pyspark.sql"),
        "pyspark.sql.functions": sys.modules.get("pyspark.sql.functions"),
    }
    sys.modules["pyspark"] = MagicMock()
    sys.modules["pyspark.sql"] = MagicMock()
    sys.modules["pyspark.sql.functions"] = mock_functions

    yield

    for key, val in prev.items():
        if val is None:
            sys.modules.pop(key, None)
        else:
            sys.modules[key] = val


class _MockDF:
    """Minimal DataFrame mock that supports chained Spark operations."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def select(self, *args, **kwargs) -> _MockDF:
        return self

    def filter(self, *args, **kwargs) -> _MockDF:
        return self

    def distinct(self) -> _MockDF:
        return self

    def join(self, other: _MockDF, on: object, how: str) -> _MockDF:
        """Simulate INNER / LEFT ANTI join on _join_id or [_mid, _period]."""
        if isinstance(on, list):
            # Multi-key join (IDSSE period pairs)
            other_keys = {tuple(r[k] for k in on) for r in other._rows}
        else:
            other_keys = {r.get(on, r.get("_join_id")) for r in other._rows}

        result = []
        for row in self._rows:
            if isinstance(on, list):
                key = tuple(row[k] for k in on)
            else:
                key = row.get(on, row.get("_join_id"))

            if how == "inner":
                if key in other_keys:
                    result.append(row)
            elif how == "left_anti":
                if key not in other_keys:
                    result.append(row)
        return _MockDF(result)

    def collect(self) -> list[dict]:
        return self._rows


class _MockSpark:
    """Mock SparkSession that returns configured DataFrames per table."""

    def __init__(self, tables: dict[str, _MockDF]) -> None:
        self._tables = tables

    def table(self, name: str) -> _MockDF:
        return self._tables.get(name, _MockDF([]))

    def sql(self, *args, **kwargs) -> _MockDF:
        return _MockDF([])


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_tracking_new_ids_three_way_join() -> None:
    """Tracking discovery: only matches in tracking ∩ spadl \\ results are returned."""
    tables = {
        "cat.bronze.metrica_tracking": _MockDF(
            [
                {"_join_id": "m1"},
                {"_join_id": "m2"},
                {"_join_id": "m3"},
            ]
        ),
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_join_id": "m1"},
                {"_join_id": "m2"},  # m3 has no SPADL
            ]
        ),
        "cat.bronze.spadl_action_context": _MockDF(
            [
                {"_join_id": "m1"},  # m1 already processed
            ]
        ),
    }
    spark = _MockSpark(tables)

    result = _find_tracking_new_ids(
        spark,
        "cat.bronze.metrica_tracking",
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "metrica",
    )
    assert sorted(result) == ["m2"]


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_tracking_new_ids_empty_results_cold_start() -> None:
    """Cold start: empty results table -> all tracking∩spadl matches returned."""
    tables = {
        "cat.bronze.skillcorner_tracking": _MockDF(
            [
                {"_join_id": "s1"},
                {"_join_id": "s2"},
                {"_join_id": "s3"},
            ]
        ),
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_join_id": "s1"},
                {"_join_id": "s2"},
                {"_join_id": "s3"},
            ]
        ),
        "cat.bronze.spadl_action_context": _MockDF([]),  # empty
    }
    spark = _MockSpark(tables)

    result = _find_tracking_new_ids(
        spark,
        "cat.bronze.skillcorner_tracking",
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "skillcorner",
    )
    assert sorted(result) == ["s1", "s2", "s3"]


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_event_only_new_ids_anti_join() -> None:
    """Event-only discovery: spadl_actions \\ results for a specific provider."""
    tables = {
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_join_id": "sb1"},
                {"_join_id": "sb2"},
                {"_join_id": "sb3"},
            ]
        ),
        "cat.bronze.spadl_action_context": _MockDF(
            [
                {"_join_id": "sb1"},  # already done
            ]
        ),
    }
    spark = _MockSpark(tables)

    result = _find_event_only_new_ids(
        spark,
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "statsbomb",
    )
    assert sorted(result) == ["sb2", "sb3"]


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_event_only_new_ids_cold_start_5000_matches() -> None:
    """Cold start: 5000 unprocessed matches returned without driver OOM."""
    many_ids = [{"_join_id": f"sb{i}"} for i in range(5000)]
    tables = {
        "cat.bronze.spadl_actions": _MockDF(many_ids),
        "cat.bronze.spadl_action_context": _MockDF([]),  # empty
    }
    spark = _MockSpark(tables)

    result = _find_event_only_new_ids(
        spark,
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "statsbomb",
    )
    assert len(result) == 5000


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_idsse_new_period_pairs_three_way() -> None:
    """IDSSE period-level: tracking(mid,period) ∩ spadl(mid) \\ results(mid,period)."""
    tables = {
        "cat.bronze.idsse_tracking": _MockDF(
            [
                {"_mid": "i1", "_period": 1},
                {"_mid": "i1", "_period": 2},
                {"_mid": "i2", "_period": 1},
                {"_mid": "i2", "_period": 2},
            ]
        ),
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_mid": "i1"},
                {"_mid": "i2"},
            ]
        ),
        "cat.bronze.spadl_action_context": _MockDF(
            [
                {"_mid": "i1", "_period": 1},  # half 1 done
            ]
        ),
    }
    spark = _MockSpark(tables)

    result = _find_idsse_new_period_pairs(
        spark,
        "cat.bronze.idsse_tracking",
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
    )
    assert sorted(result) == [("i1", 2), ("i2", 1), ("i2", 2)]


def test_guard_chunk_sizes_keep_task_value_under_limit() -> None:
    """With 5488 matches (cold start), chunk count must stay under task value size limit.

    Databricks task values are limited to ~48 KB. At chunk_size=200 for
    event-only providers, 5488 matches produce ~45 chunks — well under limit.
    This test validates the chunk_sizes produce a manageable number of chunks.
    """
    guard = _ActionContextGuard()

    # Simulate: 3463 StatsBomb + 1941 Wyscout + 64 GradientSports +
    # 10 SkillCorner + 3 Metrica + 14 IDSSE halves = real-world cold start
    provider_counts = {
        "statsbomb": 3463,
        "wyscout": 1941,
        "gradientsports": 64,
        "skillcorner": 10,
        "metrica": 3,
    }
    total_chunks = 14  # IDSSE halves (1:1)
    for prov, count in provider_counts.items():
        cs = guard.chunk_sizes.get(prov, 2)
        total_chunks += -(-count // cs)  # ceiling division

    # Each chunk string ~ 50 chars max. Task value overhead ~ 2 bytes/element.
    # Conservative estimate: 60 bytes per chunk entry in JSON.
    estimated_bytes = total_chunks * 60

    assert total_chunks < 200, f"Too many chunks ({total_chunks}) — will exceed task value limit"
    assert estimated_bytes < 48_000, f"Estimated size {estimated_bytes} exceeds 48 KB limit"
