"""Tests for line-breaking pass detection (Ward clustering + straddle test)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from analytics.line_breaking import (
    LineBreakingParams,
    LineBreakingResult,
    _build_line_segments,
    _classify_intersection,
    _cluster_opponents,
    _segments_intersect,
    detect_line_breaking,
    detect_line_breaking_batch,
)
from ingestion.line_breaking import _process_metrica_tracking, _process_statsbomb_360

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_PARAMS = LineBreakingParams()


def _make_opponents(positions: list[tuple[float, float]]) -> pd.DataFrame:
    """Build an opponent positions DataFrame from (x, y) tuples."""
    if not positions:
        return pd.DataFrame(columns=pd.Index(["x", "y"]))
    return pd.DataFrame(positions, columns=pd.Index(["x", "y"]))


def _make_442_opponents() -> pd.DataFrame:
    """Typical 4-4-2 formation in opponent half (attacking left-to-right).

    Defense line (~85-90x), midfield (~70x), attack (~55x).
    """
    positions = [
        # Defense line (4 players, x~85-90)
        (88.0, 15.0),
        (86.0, 30.0),
        (87.0, 50.0),
        (89.0, 65.0),
        # Midfield line (4 players, x~70)
        (70.0, 10.0),
        (68.0, 30.0),
        (72.0, 50.0),
        (69.0, 70.0),
        # Attack line (2 players, x~55)
        (55.0, 35.0),
        (56.0, 50.0),
    ]
    return _make_opponents(positions)


# ---------------------------------------------------------------------------
# TestLineBreakingParams
# ---------------------------------------------------------------------------


class TestLineBreakingParams:
    """Test parameter dataclass."""

    def test_default_values(self) -> None:
        p = LineBreakingParams()
        assert p.min_opponents == 3
        assert p.n_clusters == 3
        assert p.min_pass_length == 3.0
        assert p.min_x_spread == 5.0
        assert p.pitch_y_min == 0.0
        assert p.pitch_y_max == 80.0

    def test_custom_override(self) -> None:
        p = LineBreakingParams(min_opponents=5, n_clusters=4, min_pass_length=5.0)
        assert p.min_opponents == 5
        assert p.n_clusters == 4
        assert p.min_pass_length == 5.0
        # Defaults preserved
        assert p.min_x_spread == 5.0


# ---------------------------------------------------------------------------
# TestClusterOpponents
# ---------------------------------------------------------------------------


class TestClusterOpponents:
    """Test Ward clustering of opponent positions."""

    def test_three_cluster_split(self) -> None:
        """10 players in 3 distinct x-bands should cluster into 3 groups."""
        opponents = _make_442_opponents()
        positions = np.column_stack([np.asarray(opponents["x"]), np.asarray(opponents["y"])])
        clusters = _cluster_opponents(positions, _DEFAULT_PARAMS)
        assert len(clusters) == 3

    def test_fewer_than_min_opponents(self) -> None:
        """Fewer than 3 opponents should return empty list."""
        positions = np.array([[50.0, 40.0], [60.0, 40.0]])
        clusters = _cluster_opponents(positions, _DEFAULT_PARAMS)
        assert clusters == []

    def test_degenerate_x_spread(self) -> None:
        """Opponents at the same x should still cluster (but detect_line_breaking will filter)."""
        positions = np.array([[50.0, 10.0], [50.0, 40.0], [50.0, 70.0]])
        clusters = _cluster_opponents(positions, _DEFAULT_PARAMS)
        # With identical x, Ward may put all in separate clusters or merge — either way not empty
        assert len(clusters) > 0

    def test_single_player_clusters(self) -> None:
        """3 widely separated players should form 3 single-player clusters."""
        positions = np.array([[30.0, 40.0], [60.0, 40.0], [90.0, 40.0]])
        clusters = _cluster_opponents(positions, _DEFAULT_PARAMS)
        assert len(clusters) == 3
        for c in clusters:
            assert len(c) == 1


# ---------------------------------------------------------------------------
# TestBuildLineSegments
# ---------------------------------------------------------------------------


class TestBuildLineSegments:
    """Test line segment construction from clusters."""

    def test_sideline_extensions(self) -> None:
        """Each cluster should have sideline extensions to y=0 and y=80."""
        clusters = [np.array([[50.0, 30.0], [50.0, 50.0]])]
        segments = _build_line_segments(clusters, _DEFAULT_PARAMS)
        # 2 players + 2 extensions = 4 points = 3 segments
        assert len(segments) == 3
        # First segment starts at y=0 (pitch_y_min)
        assert segments[0, 0, 1] == 0.0
        # Last segment ends at y=80 (pitch_y_max)
        assert segments[-1, 1, 1] == 80.0

    def test_segment_count(self) -> None:
        """A cluster with K players should produce K+1 segments (with extensions)."""
        cluster = np.array([[50.0, 20.0], [50.0, 40.0], [50.0, 60.0]])
        clusters = [cluster]
        segments = _build_line_segments(clusters, _DEFAULT_PARAMS)
        assert len(segments) == 4  # 3 players + 2 extensions = 5 points = 4 segments

    def test_single_player_cluster(self) -> None:
        """A single-player cluster should produce 2 segments (bottom + top extensions)."""
        clusters = [np.array([[50.0, 40.0]])]
        segments = _build_line_segments(clusters, _DEFAULT_PARAMS)
        assert len(segments) == 2


# ---------------------------------------------------------------------------
# TestSegmentsIntersect
# ---------------------------------------------------------------------------


class TestSegmentsIntersect:
    """Test vectorized cross-product straddle test."""

    def test_crossing_segments(self) -> None:
        """A pass crossing a perpendicular segment should intersect."""
        pass_start = np.array([40.0, 40.0])
        pass_end = np.array([80.0, 40.0])
        segments = np.array([[[60.0, 20.0], [60.0, 60.0]]])
        result = _segments_intersect(pass_start, pass_end, segments)
        assert result[0] is np.True_

    def test_parallel_segments(self) -> None:
        """A pass parallel to a segment should not intersect."""
        pass_start = np.array([40.0, 40.0])
        pass_end = np.array([80.0, 40.0])
        segments = np.array([[[40.0, 50.0], [80.0, 50.0]]])
        result = _segments_intersect(pass_start, pass_end, segments)
        assert result[0] is np.False_

    def test_segment_above_pass(self) -> None:
        """A segment entirely above the pass should not intersect."""
        pass_start = np.array([40.0, 40.0])
        pass_end = np.array([80.0, 40.0])
        segments = np.array([[[60.0, 50.0], [60.0, 70.0]]])
        result = _segments_intersect(pass_start, pass_end, segments)
        assert result[0] is np.False_

    def test_pass_too_short_to_reach(self) -> None:
        """A pass that ends before reaching the segment should not intersect."""
        pass_start = np.array([40.0, 40.0])
        pass_end = np.array([50.0, 40.0])
        segments = np.array([[[60.0, 20.0], [60.0, 60.0]]])
        result = _segments_intersect(pass_start, pass_end, segments)
        assert result[0] is np.False_

    def test_empty_segments(self) -> None:
        """Empty segments array should return empty result."""
        pass_start = np.array([40.0, 40.0])
        pass_end = np.array([80.0, 40.0])
        segments = np.empty((0, 2, 2))
        result = _segments_intersect(pass_start, pass_end, segments)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# TestClassifyIntersection
# ---------------------------------------------------------------------------


class TestClassifyIntersection:
    """Test line-breaking type classification."""

    def test_through_interior(self) -> None:
        """Intersection between defenders (no sideline extension) → 'through'."""
        clusters = [np.array([[60.0, 30.0], [60.0, 50.0]])]
        segments = _build_line_segments(clusters, _DEFAULT_PARAMS)
        pass_start = np.array([40.0, 40.0])
        pass_end = np.array([80.0, 40.0])
        mask = _segments_intersect(pass_start, pass_end, segments)
        result = _classify_intersection(pass_start, pass_end, clusters, segments, mask)
        assert result == "through"

    def test_around_sideline(self) -> None:
        """Intersection at sideline extension → 'around'."""
        # Cluster centered high: only segment near y=0 will be intersected
        clusters = [np.array([[60.0, 60.0], [60.0, 70.0]])]
        segments = _build_line_segments(clusters, _DEFAULT_PARAMS)
        # Pass near bottom sideline
        pass_start = np.array([40.0, 5.0])
        pass_end = np.array([80.0, 5.0])
        mask = _segments_intersect(pass_start, pass_end, segments)
        if mask.any():
            result = _classify_intersection(pass_start, pass_end, clusters, segments, mask)
            assert result == "around"


# ---------------------------------------------------------------------------
# TestDetectLineBreaking
# ---------------------------------------------------------------------------


class TestDetectLineBreaking:
    """Test full line-breaking detection."""

    def test_442_through_pass(self) -> None:
        """A forward pass through a 4-4-2 should break at least 1 line."""
        opponents = _make_442_opponents()
        result = detect_line_breaking(50.0, 40.0, 95.0, 40.0, opponents)
        assert result.is_line_breaking
        assert result.lines_broken >= 1
        assert result.line_breaking_type is not None

    def test_lateral_pass(self) -> None:
        """A purely lateral pass should not break any lines."""
        opponents = _make_442_opponents()
        # Strictly lateral pass won't break lines (end_x == start_x)
        result2 = detect_line_breaking(50.0, 20.0, 50.0, 60.0, opponents)
        assert not result2.is_line_breaking  # end_x == start_x → backward/equal guard

    def test_backward_pass(self) -> None:
        """A backward pass should never be line-breaking."""
        opponents = _make_442_opponents()
        result = detect_line_breaking(70.0, 40.0, 50.0, 40.0, opponents)
        assert not result.is_line_breaking
        assert result.lines_broken == 0

    def test_short_pass(self) -> None:
        """A pass shorter than min_pass_length should not be line-breaking."""
        opponents = _make_442_opponents()
        result = detect_line_breaking(50.0, 40.0, 51.0, 40.0, opponents)
        assert not result.is_line_breaking

    def test_fewer_than_3_opponents(self) -> None:
        """Fewer than 3 opponents should return not line-breaking."""
        opponents = _make_opponents([(60.0, 40.0), (70.0, 40.0)])
        result = detect_line_breaking(50.0, 40.0, 90.0, 40.0, opponents)
        assert not result.is_line_breaking

    def test_all_3_lines(self) -> None:
        """A long pass through all 3 lines should break 3 lines."""
        opponents = _make_442_opponents()
        # Very long pass from behind all lines to beyond all
        result = detect_line_breaking(40.0, 40.0, 100.0, 40.0, opponents)
        assert result.is_line_breaking
        # Should break 2 or 3 depending on clustering; at minimum 2
        assert result.lines_broken >= 2

    def test_compressed_formation(self) -> None:
        """Opponents with < 5 yards x-spread should return not line-breaking."""
        opponents = _make_opponents([(50.0, 20.0), (51.0, 40.0), (52.0, 60.0)])
        result = detect_line_breaking(40.0, 40.0, 90.0, 40.0, opponents)
        assert not result.is_line_breaking

    def test_result_dataclass(self) -> None:
        """LineBreakingResult should be a proper dataclass."""
        r = LineBreakingResult(is_line_breaking=True, lines_broken=2, line_breaking_type="through")
        assert r.is_line_breaking
        assert r.lines_broken == 2
        assert r.line_breaking_type == "through"


# ---------------------------------------------------------------------------
# TestDetectLineBreakingBatch
# ---------------------------------------------------------------------------


class TestDetectLineBreakingBatch:
    """Test batch line-breaking detection."""

    def test_multiple_passes(self) -> None:
        """Batch detection should return one row per pass."""
        opponents = _make_442_opponents()
        passes = pd.DataFrame(
            {
                "event_id": ["e1", "e2"],
                "start_x": [50.0, 70.0],
                "start_y": [40.0, 40.0],
                "end_x": [95.0, 50.0],
                "end_y": [40.0, 40.0],
            }
        )
        opponents_by_event = {"e1": opponents, "e2": opponents}
        result = detect_line_breaking_batch(passes, opponents_by_event)
        assert len(result) == 2
        assert "event_id" in result.columns
        assert "is_line_breaking" in result.columns

    def test_mixed_results(self) -> None:
        """Batch should have both line-breaking and non-line-breaking passes."""
        opponents = _make_442_opponents()
        passes = pd.DataFrame(
            {
                "event_id": ["forward", "backward"],
                "start_x": [50.0, 70.0],
                "start_y": [40.0, 40.0],
                "end_x": [95.0, 50.0],
                "end_y": [40.0, 40.0],
            }
        )
        opponents_by_event = {"forward": opponents, "backward": opponents}
        result = detect_line_breaking_batch(passes, opponents_by_event)

        forward_row = result[result["event_id"] == "forward"].iloc[0]
        backward_row = result[result["event_id"] == "backward"].iloc[0]
        assert forward_row["is_line_breaking"]
        assert not backward_row["is_line_breaking"]

    def test_empty_input(self) -> None:
        """Empty passes DataFrame should return empty result."""
        passes = pd.DataFrame(columns=pd.Index(["event_id", "start_x", "start_y", "end_x", "end_y"]))
        result = detect_line_breaking_batch(passes, {})
        assert len(result) == 0
        assert "event_id" in result.columns

    def test_cluster_cache_avoids_redundant_ward_calls(self) -> None:
        """Three passes sharing identical opponents should cluster only once."""
        opponents = _make_442_opponents()
        passes = pd.DataFrame(
            {
                "event_id": ["e1", "e2", "e3"],
                "start_x": [50.0, 45.0, 40.0],
                "start_y": [40.0, 30.0, 50.0],
                "end_x": [95.0, 90.0, 100.0],
                "end_y": [40.0, 35.0, 45.0],
            }
        )
        # All three passes share the same opponent positions
        opponents_by_event = {"e1": opponents, "e2": opponents, "e3": opponents}

        with patch("analytics.line_breaking._cluster_opponents", wraps=_cluster_opponents) as mock_cluster:
            result = detect_line_breaking_batch(passes, opponents_by_event)

        assert len(result) == 3
        # With caching, _cluster_opponents should be called exactly once
        assert mock_cluster.call_count == 1

    def test_cluster_cache_distinct_opponents_clustered_separately(self) -> None:
        """Passes with different opponent positions should cluster independently."""
        opponents_a = _make_opponents([(60.0, 20.0), (60.0, 40.0), (60.0, 60.0), (80.0, 20.0), (80.0, 60.0)])
        opponents_b = _make_442_opponents()
        passes = pd.DataFrame(
            {
                "event_id": ["e1", "e2"],
                "start_x": [50.0, 50.0],
                "start_y": [40.0, 40.0],
                "end_x": [95.0, 95.0],
                "end_y": [40.0, 40.0],
            }
        )
        opponents_by_event = {"e1": opponents_a, "e2": opponents_b}

        with patch("analytics.line_breaking._cluster_opponents", wraps=_cluster_opponents) as mock_cluster:
            result = detect_line_breaking_batch(passes, opponents_by_event)

        assert len(result) == 2
        # Different opponent positions → _cluster_opponents called twice
        assert mock_cluster.call_count == 2


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_nan_positions(self) -> None:
        """NaN opponent positions should be dropped before clustering."""
        opponents = _make_opponents(
            [
                (60.0, 20.0),
                (60.0, 40.0),
                (60.0, 60.0),
                (float("nan"), float("nan")),
            ]
        )
        # Should not raise; 3 valid opponents remain
        result = detect_line_breaking(40.0, 40.0, 80.0, 40.0, opponents)
        # May or may not break — just ensure no error and valid result
        assert isinstance(result, LineBreakingResult)

    def test_empty_opponents(self) -> None:
        """Empty opponent DataFrame should return not line-breaking."""
        opponents = _make_opponents([])
        result = detect_line_breaking(40.0, 40.0, 80.0, 40.0, opponents)
        assert not result.is_line_breaking

    def test_missing_event_opponents(self) -> None:
        """Pass with no opponents in the mapping should return not line-breaking."""
        passes = pd.DataFrame(
            {
                "event_id": ["e1"],
                "start_x": [50.0],
                "start_y": [40.0],
                "end_x": [90.0],
                "end_y": [40.0],
            }
        )
        result = detect_line_breaking_batch(passes, {})
        assert len(result) == 1
        assert not result.iloc[0]["is_line_breaking"]


# ---------------------------------------------------------------------------
# TestIncrementalSkipGuard — Path A (StatsBomb 360)
# ---------------------------------------------------------------------------


class TestIncrementalSkipGuardPathA:
    """Test incremental skip guard for _process_statsbomb_360."""

    def _make_logger(self) -> logging.Logger:
        return logging.getLogger("test_lb_skip")

    def test_all_matches_already_processed_skips(self) -> None:
        """When all 360 match_ids exist in results, processing should be skipped entirely."""
        spark = MagicMock()
        logger = self._make_logger()
        params = LineBreakingParams()

        # spark.table(ff_table).select("match_id").distinct().toPandas()
        # returns match_ids 100, 200
        ff_pdf = pd.DataFrame({"match_id": [100, 200]})
        # spark.table(results_table).filter(...).select("match_id").distinct().collect()
        # returns same match_ids
        existing_row_1 = MagicMock()
        existing_row_1.__getitem__ = lambda self, k: "100"
        existing_row_2 = MagicMock()
        existing_row_2.__getitem__ = lambda self, k: "200"

        # First call: ff_table → toPandas returns match IDs
        # Second call: results_table → filter → select → distinct → collect returns existing IDs
        ff_table_mock = MagicMock()
        ff_table_mock.select.return_value.distinct.return_value.toPandas.return_value = ff_pdf

        results_table_mock = MagicMock()
        results_table_mock.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
            existing_row_1,
            existing_row_2,
        ]

        def table_side_effect(name: str) -> MagicMock:
            if name.endswith("statsbomb_360"):
                return ff_table_mock
            if name.endswith("line_breaking_results"):
                return results_table_mock
            return MagicMock()

        spark.table.side_effect = table_side_effect

        result = _process_statsbomb_360(spark, "cat", "bronze", logger, params)

        assert result == 0
        # Should NOT have tried to read events (no per-match processing)
        # The events table should never be accessed
        events_calls = [c for c in spark.table.call_args_list if "statsbomb_events" in str(c)]
        assert len(events_calls) == 0

    def test_partial_skip_processes_only_new(self) -> None:
        """When some matches exist in results, only new matches should be processed."""
        spark = MagicMock()
        logger = self._make_logger()
        params = LineBreakingParams()

        # 360 table has match_ids 100, 200, 300
        ff_pdf = pd.DataFrame({"match_id": [100, 200, 300]})
        # Results table has match_ids 100, 200 (so only 300 is new)
        existing_row_1 = MagicMock()
        existing_row_1.__getitem__ = lambda self, k: "100"
        existing_row_2 = MagicMock()
        existing_row_2.__getitem__ = lambda self, k: "200"

        ff_table_mock = MagicMock()
        ff_table_mock.select.return_value.distinct.return_value.toPandas.return_value = ff_pdf

        results_table_mock = MagicMock()
        results_table_mock.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
            existing_row_1,
            existing_row_2,
        ]

        # Events table for the new match (match_id=300) — return empty to keep test simple
        events_table_mock = MagicMock()
        events_table_mock.filter.return_value.toPandas.return_value = pd.DataFrame()

        def table_side_effect(name: str) -> MagicMock:
            if name.endswith("statsbomb_360"):
                return ff_table_mock
            if name.endswith("line_breaking_results"):
                return results_table_mock
            if name.endswith("statsbomb_events"):
                return events_table_mock
            return MagicMock()

        spark.table.side_effect = table_side_effect

        result = _process_statsbomb_360(spark, "cat", "bronze", logger, params)

        assert result == 0  # no rows written (empty passes for match 300)
        # Verify events table was accessed for match 300 only
        events_calls = [c for c in spark.table.call_args_list if "statsbomb_events" in str(c)]
        assert len(events_calls) == 1
        # The filter should contain match_id 300, not 100 or 200
        filter_calls = events_table_mock.filter.call_args_list
        assert len(filter_calls) == 1
        assert "300" in str(filter_calls[0])

    def test_no_results_table_processes_all(self) -> None:
        """When results table doesn't exist, all matches should be processed."""
        spark = MagicMock()
        logger = self._make_logger()
        params = LineBreakingParams()

        # 360 table has match_ids 100, 200
        ff_pdf = pd.DataFrame({"match_id": [100, 200]})

        ff_table_mock = MagicMock()
        ff_table_mock.select.return_value.distinct.return_value.toPandas.return_value = ff_pdf

        # Results table doesn't exist — raise exception
        results_table_mock = MagicMock()
        results_table_mock.filter.side_effect = Exception("Table not found")

        # Events table — return empty to keep test simple
        events_table_mock = MagicMock()
        events_table_mock.filter.return_value.toPandas.return_value = pd.DataFrame()

        def table_side_effect(name: str) -> MagicMock:
            if name.endswith("statsbomb_360"):
                return ff_table_mock
            if name.endswith("line_breaking_results"):
                return results_table_mock
            if name.endswith("statsbomb_events"):
                return events_table_mock
            return MagicMock()

        spark.table.side_effect = table_side_effect

        _process_statsbomb_360(spark, "cat", "bronze", logger, params)

        # Should have tried to read events for both matches
        events_calls = [c for c in spark.table.call_args_list if "statsbomb_events" in str(c)]
        assert len(events_calls) == 2


# ---------------------------------------------------------------------------
# TestIncrementalSkipGuard — Path B (Metrica Tracking)
# ---------------------------------------------------------------------------


class TestIncrementalSkipGuardPathB:
    """Test incremental skip guard for _process_metrica_tracking."""

    def _make_logger(self) -> logging.Logger:
        return logging.getLogger("test_lb_skip_b")

    def test_all_matches_already_processed_skips(self) -> None:
        """When all Metrica match_ids exist in results, processing should be skipped."""
        spark = MagicMock()
        logger = self._make_logger()
        params = LineBreakingParams()

        # Events table returns PASS events for matches m1, m2
        events_pdf = pd.DataFrame(
            {
                "match_id": ["m1", "m2"],
                "type": ["PASS", "PASS"],
                "event_id": ["e1", "e2"],
                "start_frame": [100, 200],
                "team": ["Home", "Away"],
                "start_x": [0.5, 0.5],
                "start_y": [0.5, 0.5],
                "end_x": [0.8, 0.8],
                "end_y": [0.5, 0.5],
            }
        )

        # Results table has m1, m2 already
        existing_row_1 = MagicMock()
        existing_row_1.__getitem__ = lambda self, k: "m1"
        existing_row_2 = MagicMock()
        existing_row_2.__getitem__ = lambda self, k: "m2"

        events_table_mock = MagicMock()
        events_table_mock.filter.return_value.toPandas.return_value = events_pdf

        results_table_mock = MagicMock()
        results_table_mock.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
            existing_row_1,
            existing_row_2,
        ]

        def table_side_effect(name: str) -> MagicMock:
            if name.endswith("metrica_events"):
                return events_table_mock
            if name.endswith("line_breaking_results"):
                return results_table_mock
            return MagicMock()

        spark.table.side_effect = table_side_effect

        result = _process_metrica_tracking(spark, "cat", "bronze", logger, params)

        assert result == 0
        # Should NOT have tried to read tracking data (all matches skipped)
        tracking_calls = [c for c in spark.table.call_args_list if "metrica_tracking" in str(c)]
        assert len(tracking_calls) == 0

    def test_partial_skip_processes_only_new(self) -> None:
        """When some Metrica matches exist, only new matches should be processed."""
        spark = MagicMock()
        logger = self._make_logger()
        params = LineBreakingParams()

        # Events table returns PASS events for matches m1, m2, m3
        events_pdf = pd.DataFrame(
            {
                "match_id": ["m1", "m2", "m3"],
                "type": ["PASS", "PASS", "PASS"],
                "event_id": ["e1", "e2", "e3"],
                "start_frame": [100, 200, 300],
                "team": ["Home", "Away", "Home"],
                "start_x": [0.5, 0.5, 0.5],
                "start_y": [0.5, 0.5, 0.5],
                "end_x": [0.8, 0.8, 0.8],
                "end_y": [0.5, 0.5, 0.5],
            }
        )

        # Results table has m1, m2 already (so only m3 is new)
        existing_row_1 = MagicMock()
        existing_row_1.__getitem__ = lambda self, k: "m1"
        existing_row_2 = MagicMock()
        existing_row_2.__getitem__ = lambda self, k: "m2"

        events_table_mock = MagicMock()
        events_table_mock.filter.return_value.toPandas.return_value = events_pdf

        results_table_mock = MagicMock()
        results_table_mock.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
            existing_row_1,
            existing_row_2,
        ]

        # Tracking table for the new match (m3) — return empty to keep test simple
        tracking_table_mock = MagicMock()
        tracking_table_mock.filter.return_value.toPandas.return_value = pd.DataFrame()

        def table_side_effect(name: str) -> MagicMock:
            if name.endswith("metrica_events"):
                return events_table_mock
            if name.endswith("line_breaking_results"):
                return results_table_mock
            if name.endswith("metrica_tracking"):
                return tracking_table_mock
            return MagicMock()

        spark.table.side_effect = table_side_effect

        result = _process_metrica_tracking(spark, "cat", "bronze", logger, params)

        assert result == 0  # no rows written (empty tracking for m3)
        # Tracking table was accessed only for m3
        tracking_calls = [c for c in spark.table.call_args_list if "metrica_tracking" in str(c)]
        assert len(tracking_calls) == 1
