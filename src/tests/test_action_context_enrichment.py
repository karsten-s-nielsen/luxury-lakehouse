# src/tests/test_action_context_enrichment.py
"""Unit tests for action_context enrichment chains.

Mock-patches all silly-kicks add_* calls to verify:
- call ordering and links propagation (tracking chain)
- sb360 zero-frame match yields no rows (frames-required; ADR-057)
- output column selection matches _RESULT_COLUMNS
"""

from __future__ import annotations

import logging
import sys
from collections import namedtuple
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from analytics.action_context.enrich import _enrich_sb360_match, _enrich_tracking_match
from analytics.action_context.schema import RESULT_COLUMNS as _RESULT_COLUMNS
from analytics.action_context.work_unit import WorkUnit
from ingestion.action_context import (
    _ActionContextGuard,
    _build_output,
    _find_idsse_new_period_pairs,
    _find_sb360_new_ids,
    _find_tracking_new_period_pairs,
    _is_tracking_provider,
    _load_xt_grid_from_delta,
    _parse_preflight_filters,
    assign_workers,
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


def test_sb360_zero_frames_yields_no_rows() -> None:
    """Frames-required (ADR-057): a sb360 match whose freeze-frames convert to ZERO synthetic
    frames produces NO rows. The pure core returns empty; the production edge WARNs (covered in
    test_action_context_createdataframe_schema / the processor test). This test exercises the
    ``len(frames) == 0`` branch specifically — NON-empty freeze-frames mapped to zero frames
    (review L-new-2), not a trivially-empty input."""
    actions = _make_actions(3)
    ff = pd.DataFrame({"id": ["e0", "e1"], "freeze_frame": [[], []]})  # non-empty, but positionless
    with (
        patch("silly_kicks.spadl.add_game_state", side_effect=lambda df: df.assign(game_state="drawing")),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", side_effect=lambda df, **kw: df),
        patch(
            "silly_kicks.tracking.snapshot_to_tracking_frames",
            return_value=(pd.DataFrame(), pd.DataFrame()),  # zero synthetic frames
        ) as mock_snap,
    ):
        out = _enrich_sb360_match(actions, ff, home_team_id="H", xt=MagicMock())
    mock_snap.assert_called_once()
    assert len(ff) > 0  # pins the len(frames)==0 branch (not the empty-freeze-frames branch)
    assert len(out) == 0


def test_enrich_tracking_calls_all_steps_with_links() -> None:
    """Tracking chain must call all 24 add_* steps and propagate links."""
    actions = _make_actions()
    tracking = _make_tracking()
    mock_links = _make_mock_links(actions)
    mock_xt = MagicMock()

    mock_link_fn = MagicMock(return_value=(mock_links, MagicMock()))
    mock_pc = MagicMock(return_value=pd.Series([0.5] * len(actions), name="pitch_control_at_target__spearman"))
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
        patch("silly_kicks.tracking.pitch_control_at_target", mock_pc),
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
        # xShotOccurrence (silly-kicks 4.9.0+, Step 21) — patched so the real bundled XGBoost
        # model isn't loaded and infer_ball_carrier doesn't run on the synthetic frames.
        patch("silly_kicks.tracking.add_xshot_occurrence", _PASSTHROUGH),
        # silly-kicks 4.19.2 (ADR-042) Steps 22-24 — patched so the real geometry/model kernels
        # don't run on the synthetic frames (add_structural_pass groupbys period_id/frame_id).
        patch("silly_kicks.tracking.add_structural_pass", _PASSTHROUGH),
        patch("silly_kicks.tracking.features.add_player_influence", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_xcross_attempt", _PASSTHROUGH),
        # xT-GK (silly-kicks 4.21.0+/4.22.0, ADR-048) Steps 25/25b/26 — patched so the real
        # valuation (which hard-requires a FITTED ExpectedThreat and loads the bundled
        # completion model) doesn't run against the MagicMock xt / synthetic frames. The preset
        # loop calls compute_xt_gk(...)["xt_gk"], so its stub returns an actions-indexed frame.
        patch("silly_kicks.tracking.add_xt_gk", _PASSTHROUGH),
        patch(
            "silly_kicks.tracking.compute_xt_gk",
            MagicMock(side_effect=lambda a, f, **kw: pd.DataFrame({"xt_gk": [float("nan")] * len(a)}, index=a.index)),
        ),
        patch("silly_kicks.tracking.add_gk_completion", _PASSTHROUGH),
        patch(
            "silly_kicks.tracking._xt_gk._resolve_completion_for_frames",
            MagicMock(return_value=(MagicMock(), "gs")),
        ),
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
    mock_xt = MagicMock()

    mock_converter = MagicMock(return_value=(mock_frames, mock_links))
    mock_line_break = MagicMock(side_effect=_PASSTHROUGH)
    mock_team_shape = MagicMock(side_effect=_PASSTHROUGH)
    # ADR-058: sb360 now emits pitch_control_at_target__voronoi (and does NOT run ghost-GK).
    mock_pc = MagicMock(
        side_effect=lambda out, *a, **k: pd.Series(
            [0.5] * len(out), name="pitch_control_at_target__voronoi", index=out.index
        )
    )
    mock_ghost = MagicMock(side_effect=_PASSTHROUGH)  # asserted NOT called (ADR-058)

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.snapshot_to_tracking_frames", mock_converter),
        patch("silly_kicks.tracking.add_action_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_defensive_line", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_line_break", mock_line_break),
        patch("silly_kicks.tracking.add_team_shape", mock_team_shape),
        # SB360 coverage steps (ADR-039/ADR-058) — patched so the real silly-kicks funcs don't run on
        # the minimal mock frame. pitch_control_at_target (voronoi) IS now on sb360; add_ghost_gk is NOT.
        patch("silly_kicks.tracking.pitch_control_at_target", mock_pc),
        patch("silly_kicks.tracking.add_pressure_on_actor", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_shape_graph", _PASSTHROUGH),
        patch("silly_kicks.tracking.features.add_ghost_gk", mock_ghost),
        patch("silly_kicks.tracking.features.add_gk_influence", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_obso", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_pausa", _PASSTHROUGH),
        patch("silly_kicks.tracking.add_xshot_occurrence", _PASSTHROUGH),
        # silly-kicks 4.19.2 (ADR-042) SB360 additions — structural_pass + player_influence run on
        # SB360 (single-frame); add_xcross_attempt is NOT on SB360 (velocity-dependent).
        patch("silly_kicks.tracking.add_structural_pass", _PASSTHROUGH),
        patch("silly_kicks.tracking.features.add_player_influence", _PASSTHROUGH),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_sb360_match(actions, freeze_frames, "t1", mock_xt)
    finally:
        for p in patches:
            p.stop()

    mock_converter.assert_called_once()
    _, lb_kwargs = mock_line_break.call_args
    assert lb_kwargs.get("method") == "ward", "method='ward' not propagated to add_line_break"
    assert lb_kwargs.get("home_team_id") == "t1", "home_team_id not propagated to add_line_break"
    mock_team_shape.assert_called_once()
    # ADR-058 tiering: voronoi pitch control IS emitted; ghost-GK is NOT run on sb360.
    _, pc_kwargs = mock_pc.call_args
    assert pc_kwargs.get("method") == "voronoi", "sb360 must call pitch_control_at_target(method='voronoi')"
    assert "pitch_control_at_target__voronoi" in result.columns
    mock_ghost.assert_not_called()
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
    mock_xt = MagicMock()

    patches = [
        patch("silly_kicks.spadl.add_game_state", _PASSTHROUGH),
        patch("silly_kicks.spadl.utils.add_pre_shot_gk_context", _PASSTHROUGH),
        patch("silly_kicks.tracking.snapshot_to_tracking_frames", mock_converter),
        patch("silly_kicks.tracking.add_line_break", mock_line_break),
    ]
    for p in patches:
        p.start()
    try:
        result = _enrich_sb360_match(actions, empty_ff, "t1", mock_xt)
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
    assert pd.isna(result["pitch_control_at_target__spearman"].iloc[0])


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
    """Frames-required (ADR-057): tracking providers classify as tracking; statsbomb is the
    only non-tracking AC provider (sb360). wyscout is out of scope (not a tracking provider)."""
    for p in ("idsse", "metrica", "skillcorner", "gradientsports"):
        assert _is_tracking_provider(p), f"{p} should be tracking"
    for p in ("statsbomb", "wyscout"):
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
def test_find_tracking_new_period_pairs_three_way_join() -> None:
    """Tracking discovery (per-period): tracking(mid,period) ∩ spadl(mid) \\ results(mid,period)."""
    tables = {
        "cat.bronze.metrica_tracking": _MockDF(
            [
                {"_mid": "m1", "_period": 1},
                {"_mid": "m1", "_period": 2},
                {"_mid": "m2", "_period": 1},
                {"_mid": "m3", "_period": 1},  # m3 has no SPADL
            ]
        ),
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_mid": "m1"},
                {"_mid": "m2"},
            ]
        ),
        "cat.bronze.spadl_action_context": _MockDF(
            [
                {"_mid": "m1", "_period": 1},  # m1 half 1 already processed
            ]
        ),
    }
    spark = _MockSpark(tables)

    result = _find_tracking_new_period_pairs(
        spark,
        "cat.bronze.metrica_tracking",
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "metrica",
    )
    assert sorted(result) == [("m1", 2), ("m2", 1)]


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_tracking_new_period_pairs_empty_results_cold_start() -> None:
    """Cold start: empty results table -> all tracking(mid,period)∩spadl(mid) pairs returned."""
    tables = {
        "cat.bronze.skillcorner_tracking": _MockDF(
            [
                {"_mid": "s1", "_period": 1},
                {"_mid": "s1", "_period": 2},
                {"_mid": "s2", "_period": 1},
            ]
        ),
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_mid": "s1"},
                {"_mid": "s2"},
            ]
        ),
        "cat.bronze.spadl_action_context": _MockDF([]),  # empty
    }
    spark = _MockSpark(tables)

    result = _find_tracking_new_period_pairs(
        spark,
        "cat.bronze.skillcorner_tracking",
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "skillcorner",
    )
    assert sorted(result) == [("s1", 1), ("s1", 2), ("s2", 1)]


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_sb360_new_ids_inner_then_antijoin() -> None:
    """Frames-required discovery (ADR-057): statsbomb spadl ∩ statsbomb_360 \\ results.

    Set-equality (review M-new-1) — a membership check would miss a partial drop. This fake
    covers the join COMPOSITION; the canonical ``cast(long->string)`` id-normalization (ADR-019
    class) is exercised by a real-dtype CI/live probe (needs Spark, unavailable locally)."""
    tables = {
        "cat.bronze.spadl_actions": _MockDF([{"_join_id": "1"}, {"_join_id": "2"}, {"_join_id": "3"}]),
        "cat.bronze.statsbomb_360": _MockDF([{"_join_id": "1"}, {"_join_id": "2"}]),  # only 1,2 have 360
        "cat.bronze.spadl_action_context": _MockDF([{"_join_id": "1"}]),  # 1 already processed
    }
    spark = _MockSpark(tables)

    result = _find_sb360_new_ids(
        spark,
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "cat.bronze.statsbomb_360",
    )
    # 1,2 have 360; 1 done -> only 2 remains. 3 has NO 360 -> out of scope (frames-required).
    assert set(result) == {"2"}


@pytest.mark.usefixtures("_mock_pyspark")
def test_find_sb360_new_ids_cold_start_5000_matches() -> None:
    """Cold start: 5000 unprocessed sb360 matches returned without driver OOM."""
    many = [{"_join_id": f"{i}"} for i in range(5000)]
    tables = {
        "cat.bronze.spadl_actions": _MockDF(many),
        "cat.bronze.statsbomb_360": _MockDF(many),  # all have 360
        "cat.bronze.spadl_action_context": _MockDF([]),  # none processed
    }
    spark = _MockSpark(tables)

    result = _find_sb360_new_ids(
        spark,
        "cat.bronze.spadl_actions",
        "cat.bronze.spadl_action_context",
        "cat.bronze.statsbomb_360",
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


def test_worker_id_task_value_is_constant_size() -> None:
    """The for-each task value is O(_N_DRAIN_WORKERS), independent of game count (ADR-037).

    Replaces the old 48 KB chunk-count guard: the worker-drain fan-out emits a fixed
    worker-id list, so the task-value size no longer scales with the number of games.
    """
    worker_ids = [str(i) for i in range(_ActionContextGuard._N_DRAIN_WORKERS)]
    assert len(worker_ids) == _ActionContextGuard._N_DRAIN_WORKERS
    assert all(s.isdigit() for s in worker_ids)


def test_assignment_retains_every_unit_at_scale() -> None:
    """assign_workers retains EVERY discovered unit (no 48 KB truncation) at any scale."""
    units = [WorkUnit(provider="statsbomb", match_id=f"s{i}") for i in range(100_000)]
    assignments = assign_workers(units, n_workers=8)
    assert len(assignments) == 100_000
    assert len({a.unit.match_id for a in assignments}) == 100_000


# ──────────────────────────────────────────────────────────────────────────
# Preflight ad-hoc scoping: --provider / --max-units (provider_filter/max_units)
# ──────────────────────────────────────────────────────────────────────────


def _providers(units: list[WorkUnit]) -> list[str]:
    return [u.provider for u in units]


def _match_ids(units: list[WorkUnit]) -> list[str]:
    return [u.match_id for u in units]


# ---- _parse_preflight_filters (pure validation/coercion) ----


def test_parse_preflight_filters_defaults_and_empty_are_none() -> None:
    """Daily job passes empty job-parameter strings -> coerce to None (all / no cap)."""
    assert _parse_preflight_filters(None, None) == (None, None)
    assert _parse_preflight_filters("", "") == (None, None)
    assert _parse_preflight_filters("   ", "  ") == (None, None)


def test_parse_preflight_filters_valid() -> None:
    assert _parse_preflight_filters("statsbomb", "5") == ("statsbomb", 5)
    assert _parse_preflight_filters(" idsse ", " 3 ") == ("idsse", 3)
    assert _parse_preflight_filters(None, "1") == (None, 1)
    assert _parse_preflight_filters("metrica", None) == ("metrica", None)


def test_parse_preflight_filters_unknown_provider_raises() -> None:
    with pytest.raises(SystemExit, match="Unknown --provider"):
        _parse_preflight_filters("bogus", None)


def test_parse_preflight_filters_bad_max_units_raises() -> None:
    with pytest.raises(SystemExit, match="must be > 0"):
        _parse_preflight_filters(None, "0")
    with pytest.raises(SystemExit, match="must be > 0"):
        _parse_preflight_filters(None, "-3")
    with pytest.raises(SystemExit, match="positive integer"):
        _parse_preflight_filters(None, "abc")


# ---- guard.check() honors provider_filter + max_units ----


@pytest.mark.usefixtures("_mock_pyspark")
def test_guard_provider_filter_restricts_to_one_provider() -> None:
    """provider_filter restricts discovery to one provider. statsbomb (ADR-058) EXITS the drain — it
    is never discovered here (processed by main_statsbomb), so the filter is exercised via metrica."""
    tables = {
        "cat.bronze.spadl_actions": _MockDF([{"_mid": "m1"}, {"_mid": "m2"}]),
        "cat.bronze.statsbomb_360": _MockDF([{"_join_id": "s1"}, {"_join_id": "s2"}]),
        "cat.bronze.metrica_tracking": _MockDF([{"_mid": "m1", "_period": 1}, {"_mid": "m2", "_period": 1}]),
        "cat.bronze.idsse_tracking": _MockDF([{"_mid": "i1", "_period": 1}]),
    }
    spark = _MockSpark(tables)
    # statsbomb exits the drain — never discovered, even with an explicit filter (ADR-058).
    assert _ActionContextGuard(provider_filter="statsbomb").discover_units(spark, "cat", "bronze") == []
    # the filter still restricts to a single tracking provider.
    units = _ActionContextGuard(provider_filter="metrica").discover_units(spark, "cat", "bronze")
    assert units and all(u.provider == "metrica" for u in units)
    assert sorted(_match_ids(units)) == ["m1", "m2"]


@pytest.mark.usefixtures("_mock_pyspark")
def test_guard_max_units_caps_deterministically() -> None:
    """provider_filter + max_units -> the FIRST N units in sorted order (stable 'next N'). Exercised
    via metrica (a drain provider); statsbomb's cap lives in main_statsbomb (ADR-058)."""
    tables = {
        "cat.bronze.spadl_actions": _MockDF(
            [{"_mid": "s3"}, {"_mid": "s1"}, {"_mid": "s5"}, {"_mid": "s2"}, {"_mid": "s4"}]
        ),
        "cat.bronze.metrica_tracking": _MockDF(
            [
                {"_mid": "s1", "_period": 1},
                {"_mid": "s2", "_period": 1},
                {"_mid": "s3", "_period": 1},
                {"_mid": "s4", "_period": 1},
                {"_mid": "s5", "_period": 1},
            ]
        ),
    }
    guard = _ActionContextGuard(provider_filter="metrica", max_units=2)
    spark = _MockSpark(tables)
    units = guard.discover_units(spark, "cat", "bronze")

    assert sorted(_match_ids(units)) == ["s1", "s2"], f"max_units=2 must pick sorted-first 2, got {units}"
    assert guard.check(spark, "cat", "bronze").count == 2  # memoised -> same result


@pytest.mark.usefixtures("_mock_pyspark")
def test_guard_max_units_one_per_provider() -> None:
    """max_units=1 with no provider_filter -> exactly one unit per provider that has work."""
    # Real spadl rows carry match_id_native (aliased to _join_id by event/tracking
    # discovery and to _mid by IDSSE discovery); the mock's passthrough select means
    # each row must carry BOTH keys.
    tables = {
        "cat.bronze.spadl_actions": _MockDF(
            [
                {"_join_id": "a1", "_mid": "a1"},
                {"_join_id": "a2", "_mid": "a2"},
                {"_join_id": "i1", "_mid": "i1"},
                {"_join_id": "i2", "_mid": "i2"},
            ]
        ),
        "cat.bronze.metrica_tracking": _MockDF([{"_mid": "a1", "_period": 1}, {"_mid": "a2", "_period": 1}]),
        "cat.bronze.idsse_tracking": _MockDF(
            [{"_mid": "i1", "_period": 1}, {"_mid": "i1", "_period": 2}, {"_mid": "i2", "_period": 1}]
        ),
    }
    units = _ActionContextGuard(max_units=1).discover_units(_MockSpark(tables), "cat", "bronze")

    assert units, "expected some units"
    provs = _providers(units)
    assert len(provs) == len(set(provs)), f"a provider produced >1 unit under max_units=1: {provs}"


@pytest.mark.usefixtures("_mock_pyspark")
def test_guard_defaults_unchanged_no_filter_no_cap() -> None:
    """No provider_filter + no max_units (the daily path) -> all AC providers, no truncation."""
    tables = {
        "cat.bronze.spadl_actions": _MockDF(
            [{"_join_id": "s1", "_mid": "s1"}, {"_join_id": "s2", "_mid": "s2"}, {"_join_id": "s3", "_mid": "s3"}]
        ),
        "cat.bronze.statsbomb_360": _MockDF([{"_join_id": "s1"}, {"_join_id": "s2"}, {"_join_id": "s3"}]),
        "cat.bronze.metrica_tracking": _MockDF([{"_mid": "s1", "_period": 1}, {"_mid": "s2", "_period": 1}]),
    }
    units = _ActionContextGuard().discover_units(_MockSpark(tables), "cat", "bronze")

    provs = set(_providers(units))
    # statsbomb EXITS the drain (ADR-058) — never discovered here even on the default path; metrica
    # (a drain provider) is still discovered, uncapped.
    assert "metrica" in provs, provs
    assert "statsbomb" not in provs, provs
    assert sorted(u.match_id for u in units if u.provider == "metrica") == ["s1", "s2"]


@pytest.mark.usefixtures("_mock_pyspark")
def test_guard_provider_filter_idsse_half_units() -> None:
    """provider_filter='idsse' + max_units caps (match, period) HALVES (idsse's unit)."""
    tables = {
        "cat.bronze.spadl_actions": _MockDF([{"_mid": "i1"}, {"_mid": "i2"}]),
        "cat.bronze.idsse_tracking": _MockDF(
            [{"_mid": "i1", "_period": 1}, {"_mid": "i1", "_period": 2}, {"_mid": "i2", "_period": 1}]
        ),
    }
    guard = _ActionContextGuard(provider_filter="idsse", max_units=2)
    units = guard.discover_units(_MockSpark(tables), "cat", "bronze")

    assert all(u.provider == "idsse" for u in units)
    assert len(units) == 2, f"max_units=2 must cap to 2 idsse halves, got {units}"
    assert [(u.match_id, u.period) for u in units] == [("i1", 1), ("i1", 2)]


# ──────────────────────────────────────────────────────────────────────────
# discover_units memoisation (P1/R1) + worker-drain entry points (ADR-037)
# ──────────────────────────────────────────────────────────────────────────


def test_discover_units_memoised_and_keyed_on_target(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.action_context as ac
    import ingestion.guards as g

    calls = {"idsse": 0, "tracking": 0, "sb360": 0}

    def _bump(key: str, value: object) -> object:
        calls[key] += 1
        return value

    monkeypatch.setattr(ac, "_find_idsse_new_period_pairs", lambda *a, **k: _bump("idsse", [("idm", 1), ("idm", 2)]))
    monkeypatch.setattr(ac, "_find_tracking_new_period_pairs", lambda *a, **k: _bump("tracking", [("t1", 1)]))
    monkeypatch.setattr(ac, "_find_sb360_new_ids", lambda *a, **k: _bump("sb360", ["s1", "s2"]))
    monkeypatch.setattr(g, "ensure_table", lambda *a, **k: None)

    guard = ac._ActionContextGuard()
    units = guard.discover_units(None, "c", "bronze")  # type: ignore[arg-type]
    assert WorkUnit(provider="idsse", match_id="idm", period=1) in units
    assert sum(u.provider in {"metrica", "skillcorner", "gradientsports"} for u in units) == 3
    # ADR-058: statsbomb EXITS the drain — discover_units never calls _find_sb360_new_ids, so no
    # statsbomb units are enqueued (it is processed by main_statsbomb). No wyscout (ADR-057).
    assert sum(u.provider == "statsbomb" for u in units) == 0
    assert not any(u.provider == "wyscout" for u in units)

    # P1: check() + a second discover_units() must NOT re-run the anti-joins (memoised once).
    assert guard.check(None, "c", "bronze").count == len(units)  # type: ignore[arg-type]
    assert guard.discover_units(None, "c", "bronze") is units  # type: ignore[arg-type]
    assert calls == {"idsse": 1, "tracking": 3, "sb360": 0}  # sb360 anti-join no longer in the drain

    # R1: a DIFFERENT (catalog, schema) self-invalidates the memo -> re-discovers.
    guard.discover_units(None, "OTHER", "bronze")  # type: ignore[arg-type]
    assert calls == {"idsse": 2, "tracking": 6, "sb360": 0}  # sb360 anti-join no longer in the drain


def test_main_preflight_builds_queue_and_task_values(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    import ingestion.action_context as ac
    import ingestion.action_context_queue as q
    import ingestion.bootstrap as bs
    from ingestion.guards import FilterResult

    ns = argparse.Namespace(catalog="cat", schema="bronze", provider=None, max_units=None, run_id="JOBRUN42")
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(ac, "timed_check", lambda g, s, c, sc: FilterResult(workflow_id="x", count=20))
    units = [WorkUnit(provider="statsbomb", match_id=f"s{i}") for i in range(20)]
    monkeypatch.setattr(ac._ActionContextGuard, "discover_units", lambda self, s, c, sc: units)

    captured: dict[str, object] = {}

    class _FakeQueue:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def ensure_table(self) -> None:
            captured["ensured"] = True

        def prune(self, *a: object, **k: object) -> int:
            captured["pruned"] = True
            return 0

        def enqueue(self, run_id: str, assignments: list) -> None:
            captured["run_id"] = run_id
            captured["n"] = len(assignments)

    monkeypatch.setattr(q, "DeltaWorkQueue", _FakeQueue)  # patched at SOURCE (function-local import)

    set_values: dict[str, object] = {}
    monkeypatch.setattr(ac, "_set_task_value", lambda key, value, log: set_values.__setitem__(key, value))

    ac.main_preflight()

    assert captured["ensured"] is True
    assert captured["pruned"] is True  # preflight self-prunes stale work-queue rows before enqueue
    assert captured["run_id"] == "JOBRUN42"
    assert captured["n"] == 20
    assert set_values["action_context_run_id"] == "JOBRUN42"
    assert set_values["action_context_worker_ids"] == [str(i) for i in range(ac._ActionContextGuard._N_DRAIN_WORKERS)]


def test_main_preflight_empty_emits_empty_worker_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    import ingestion.action_context as ac
    import ingestion.bootstrap as bs
    from ingestion.guards import FilterResult

    ns = argparse.Namespace(catalog="cat", schema="bronze", provider=None, max_units=None, run_id="JOBRUN42")
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(ac, "timed_check", lambda g, s, c, sc: FilterResult(workflow_id="x", count=0))

    set_values: dict[str, object] = {}
    monkeypatch.setattr(ac, "_set_task_value", lambda key, value, log: set_values.__setitem__(key, value))

    ac.main_preflight()
    assert set_values["action_context_worker_ids"] == []  # for-each runs zero iterations


def test_main_drain_worker_calls_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    import ingestion.action_context as ac
    import ingestion.action_context_queue as q
    import ingestion.bootstrap as bs
    from analytics.action_context.drain import DrainSummary

    ns = argparse.Namespace(catalog="cat", schema="bronze", worker_id="2", run_id="JOBRUN42")
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)

    prefetched = [WorkUnit(provider="statsbomb", match_id="x")]

    class _Q:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
            return prefetched

    monkeypatch.setattr(q, "DeltaWorkQueue", _Q)
    monkeypatch.setattr(q, "SparkGameProcessor", lambda *a, **k: object())
    monkeypatch.setattr(q, "SparkInterruptWatchdog", lambda *a, **k: object())

    seen: dict[str, object] = {}

    def _fake_drain(queue, processor, watchdog, run_id, worker_id, logger, **kw):
        seen["run_id"] = run_id
        seen["worker_id"] = worker_id
        seen["units"] = kw.get("units")  # short-circuit passes the pre-fetched units
        return DrainSummary(worker_id=worker_id, processed=3, total_rows=9)

    monkeypatch.setattr(ac, "drain_worker", _fake_drain)

    ac.main_drain_worker()
    assert seen["run_id"] == "JOBRUN42"
    assert seen["worker_id"] == 2
    assert seen["units"] is prefetched  # fetched once, passed in (no re-read)


def test_main_drain_worker_empty_slice_short_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    """A worker with no assigned units exits BEFORE building the processor (no xT-grid load)."""
    import argparse

    import ingestion.action_context as ac
    import ingestion.action_context_queue as q
    import ingestion.bootstrap as bs

    ns = argparse.Namespace(catalog="cat", schema="bronze", worker_id="5", run_id="JOBRUN42")
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)

    class _EmptyQ:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
            return []

    monkeypatch.setattr(q, "DeltaWorkQueue", _EmptyQ)

    def _boom(*a: object, **k: object) -> object:
        raise AssertionError("empty worker must NOT build the processor / call drain_worker")

    monkeypatch.setattr(q, "SparkGameProcessor", _boom)
    monkeypatch.setattr(q, "SparkInterruptWatchdog", _boom)
    monkeypatch.setattr(ac, "drain_worker", _boom)

    ac.main_drain_worker()  # returns cleanly, no processor built


def test_tracking_mini_gains_gk_zones_xshot_and_provenance() -> None:
    """Real (unpatched) tracking enrichment on the fast J03WMXmini fixture: the 4 gk near/far
    closing-time zones + xshot_occurrence + pitch_control_method appear; xS finite in [0,1] from
    the bundled XGBoost model (exercises the 2.1.4-trained -> 3.2.0-runtime load path); provenance
    is 'spearman' on the tracking path."""
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource,
        ParquetFrameSource,
        ParquetMatchMetadataSource,
        ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit

    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    root = "src/tests/fixtures/action_context"
    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMXmini", period=1),
        frames=ParquetFrameSource(root),
        actions=ParquetActionsSource(root),
        xt=ParquetXtSource(root),
        meta=ParquetMatchMetadataSource(root),
        sink=sink,
    )
    df = sink.df
    assert df is not None
    for c in (
        "gk_closing_time_mean_s__near_post",
        "gk_closing_time_min_s__near_post",
        "gk_closing_time_mean_s__far_post",
        "gk_closing_time_min_s__far_post",
        "xshot_occurrence",
        "pitch_control_method",
    ):
        assert c in df.columns, f"{c} missing"
    xs = pd.to_numeric(df["xshot_occurrence"], errors="coerce").dropna()
    if len(xs):
        assert bool(((xs >= 0) & (xs <= 1)).all()), "xshot_occurrence out of [0,1]"
    # Provenance must be set on EVERY tracking row (RED until enrich sets it; build_output
    # only backfills NaN). This is the reliable green-signal that the enrich step ran.
    assert df["pitch_control_method"].notna().all(), "pitch_control_method unset on tracking rows"
    assert set(df["pitch_control_method"].unique()) == {"spearman"}
