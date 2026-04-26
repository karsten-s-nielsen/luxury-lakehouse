"""Tests for the Expected Threat (xT) Markov chain computation."""

from __future__ import annotations

import pytest

pytest.importorskip("jax")

import numpy as np
import pandas as pd

from analytics.expected_threat import (
    _MOVE_TYPES,
    _SHOT_TYPES,
    ExpectedThreatParams,
    XTGrid,
    _assign_zones,
    _build_transition_matrix,
    _value_iteration_numpy,
    compute_expected_threat_grid,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_actions(
    types: list[str],
    results: list[str],
    start_positions: list[tuple[float, float]],
    end_positions: list[tuple[float, float]],
) -> pd.DataFrame:
    """Build a synthetic SPADL actions DataFrame."""
    return pd.DataFrame(
        {
            "type_name": types,
            "result_name": results,
            "start_x": [p[0] for p in start_positions],
            "start_y": [p[1] for p in start_positions],
            "end_x": [p[0] for p in end_positions],
            "end_y": [p[1] for p in end_positions],
        }
    )


_DEFAULT_PARAMS = ExpectedThreatParams()


def _make_xt_grid(
    *,
    n_zones_x: int = 12,
    n_zones_y: int = 8,
    pitch_length: float = 105.0,
    pitch_width: float = 68.0,
    coord_system: str = "spadl",
    competition_id: str | None = None,
    pattern: str = "linear_x",
    max_value: float = 0.30,
) -> XTGrid:
    """Build an XTGrid with one of a few synthetic value patterns.

    pattern="linear_x":   monotonically increasing in x — the production-shape
                          assumption (attacking goal at high-x).
    pattern="constant":   all cells equal to ``max_value``.
    pattern="reverse_x":  monotonically decreasing in x (for monotonicity tests).
    """
    if pattern == "linear_x":
        values = np.array([[max_value * (x + 1) / n_zones_x for _y in range(n_zones_y)] for x in range(n_zones_x)])
    elif pattern == "constant":
        values = np.full((n_zones_x, n_zones_y), max_value, dtype=np.float64)
    elif pattern == "reverse_x":
        values = np.array(
            [[max_value * (n_zones_x - x) / n_zones_x for _y in range(n_zones_y)] for x in range(n_zones_x)]
        )
    else:  # pragma: no cover — guard against typos
        msg = f"Unknown pattern: {pattern}"
        raise ValueError(msg)

    return XTGrid(
        values=values,
        pitch_length=pitch_length,
        pitch_width=pitch_width,
        coord_system=coord_system,  # type: ignore[arg-type]
        competition_id=competition_id,
    )


# ---------------------------------------------------------------------------
# Zone assignment
# ---------------------------------------------------------------------------


class TestAssignZones:
    """Test zone assignment logic."""

    def test_origin_maps_to_zone_zero(self) -> None:
        zones = _assign_zones(np.array([0.0]), np.array([0.0]), _DEFAULT_PARAMS)
        assert zones[0] == 0

    def test_far_corner_maps_to_last_zone(self) -> None:
        zones = _assign_zones(
            np.array([104.9]),
            np.array([67.9]),
            _DEFAULT_PARAMS,
        )
        assert zones[0] == _DEFAULT_PARAMS.n_zones_x * _DEFAULT_PARAMS.n_zones_y - 1

    def test_clamps_out_of_bounds(self) -> None:
        zones = _assign_zones(
            np.array([-1.0, 200.0]),
            np.array([0.0, 0.0]),
            _DEFAULT_PARAMS,
        )
        assert zones[0] == 0  # clamped to min
        assert zones[1] >= 0  # clamped to max

    def test_batch_assignment(self) -> None:
        x = np.array([0.0, 52.5, 104.9])
        y = np.array([0.0, 34.0, 67.9])
        zones = _assign_zones(x, y, _DEFAULT_PARAMS)
        assert len(zones) == 3
        assert zones[0] != zones[2]  # opposite corners


# ---------------------------------------------------------------------------
# Transition matrix
# ---------------------------------------------------------------------------


class TestTransitionMatrix:
    """Test transition matrix construction."""

    def test_rows_sum_to_one(self) -> None:
        starts = np.array([0, 0, 0, 1, 1])
        ends = np.array([1, 2, 1, 2, 0])
        mat = _build_transition_matrix(starts, ends, n_zones=3)

        row_sums = mat.sum(axis=1)
        np.testing.assert_allclose(row_sums[:2], 1.0)

    def test_empty_row_stays_zero(self) -> None:
        starts = np.array([0, 0])
        ends = np.array([1, 1])
        mat = _build_transition_matrix(starts, ends, n_zones=3)

        # Zone 2 has no outgoing moves
        assert mat[2].sum() == 0.0

    def test_single_transition(self) -> None:
        starts = np.array([0])
        ends = np.array([1])
        mat = _build_transition_matrix(starts, ends, n_zones=2)

        assert mat[0, 1] == 1.0
        assert mat[0, 0] == 0.0


# ---------------------------------------------------------------------------
# Value iteration
# ---------------------------------------------------------------------------


class TestValueIteration:
    """Test NumPy value iteration convergence."""

    def test_converges_simple(self) -> None:
        shot_prob = np.array([0.1, 0.5])
        goal_prob = np.array([0.5, 0.3])
        move_prob = np.array([0.9, 0.5])
        transition = np.array([[0.3, 0.7], [0.6, 0.4]])

        xt, iters = _value_iteration_numpy(
            shot_prob,
            goal_prob,
            move_prob,
            transition,
            max_iterations=100,
            tolerance=1e-5,
        )

        assert iters < 100
        assert xt[0] > 0  # zone 0 has positive threat
        assert xt[1] > 0  # zone 1 has positive threat

    def test_zero_shots_zero_xt(self) -> None:
        shot_prob = np.array([0.0, 0.0])
        goal_prob = np.array([0.0, 0.0])
        move_prob = np.array([1.0, 1.0])
        transition = np.array([[0.5, 0.5], [0.5, 0.5]])

        xt, _ = _value_iteration_numpy(
            shot_prob,
            goal_prob,
            move_prob,
            transition,
            max_iterations=100,
            tolerance=1e-5,
        )

        np.testing.assert_allclose(xt, 0.0, atol=1e-10)


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------


class TestComputeGrid:
    """Test end-to-end grid computation. Now returns XTGrid."""

    def test_output_shape_default(self) -> None:
        actions = _make_actions(
            types=["pass"] * 50 + ["shot"] * 10,
            results=["success"] * 50 + ["success"] * 5 + ["fail"] * 5,
            start_positions=[(float(i * 2), float(i % 8) * 8) for i in range(60)],
            end_positions=[(float(i * 2 + 1), float(i % 8) * 8) for i in range(60)],
        )

        grid = compute_expected_threat_grid(actions)
        assert isinstance(grid, XTGrid)
        assert grid.shape == (12, 8)

    def test_values_non_negative(self) -> None:
        actions = _make_actions(
            types=["pass"] * 100 + ["shot"] * 20,
            results=["success"] * 90 + ["fail"] * 10 + ["success"] * 10 + ["fail"] * 10,
            start_positions=[(float(i % 12) * 8.75, float(i % 8) * 8.5) for i in range(120)],
            end_positions=[(float((i + 1) % 12) * 8.75, float((i + 1) % 8) * 8.5) for i in range(120)],
        )

        grid = compute_expected_threat_grid(actions)
        assert np.all(grid.values >= 0)

    def test_custom_params(self) -> None:
        params = ExpectedThreatParams(n_zones_x=6, n_zones_y=4)
        actions = _make_actions(
            types=["pass"] * 50 + ["shot"] * 10,
            results=["success"] * 50 + ["success"] * 5 + ["fail"] * 5,
            start_positions=[(float(i * 2), float(i % 8) * 8) for i in range(60)],
            end_positions=[(float(i * 2 + 1), float(i % 8) * 8) for i in range(60)],
        )

        grid = compute_expected_threat_grid(actions, params)
        assert grid.shape == (6, 4)
        # Wrapper carries the resolution-derived metadata
        assert grid.n_zones_x == 6
        assert grid.n_zones_y == 4

    def test_empty_dataframe(self) -> None:
        actions = pd.DataFrame(
            {
                "type_name": pd.Series([], dtype=str),
                "result_name": pd.Series([], dtype=str),
                "start_x": pd.Series([], dtype=float),
                "start_y": pd.Series([], dtype=float),
                "end_x": pd.Series([], dtype=float),
                "end_y": pd.Series([], dtype=float),
            }
        )

        grid = compute_expected_threat_grid(actions)
        assert grid.shape == (12, 8)
        np.testing.assert_allclose(grid.values, 0.0)

    def test_returns_xt_grid_with_metadata(self) -> None:
        """Producer embeds SPADL coord_system + optional competition_id."""
        actions = _make_actions(
            types=["pass"] * 50 + ["shot"] * 10,
            results=["success"] * 50 + ["success"] * 5 + ["fail"] * 5,
            start_positions=[(float(i * 2), float(i % 8) * 8) for i in range(60)],
            end_positions=[(float(i * 2 + 1), float(i % 8) * 8) for i in range(60)],
        )

        grid = compute_expected_threat_grid(actions, competition_id="test_comp")
        assert grid.coord_system == "spadl"
        assert grid.pitch_length == 105.0
        assert grid.pitch_width == 68.0
        assert grid.competition_id == "test_comp"


# ---------------------------------------------------------------------------
# XTGrid wrapper — construction, properties, validation guards
# ---------------------------------------------------------------------------


class TestXTGrid:
    """Tests for the XTGrid typed wrapper class."""

    def test_construction_minimal(self) -> None:
        grid = _make_xt_grid()
        assert grid.shape == (12, 8)
        assert grid.n_zones_x == 12
        assert grid.n_zones_y == 8
        assert grid.coord_system == "spadl"
        assert grid.competition_id is None

    def test_construction_with_competition_id(self) -> None:
        grid = _make_xt_grid(competition_id="global")
        assert grid.competition_id == "global"

    def test_rejects_1d_values(self) -> None:
        with pytest.raises(ValueError, match="2D"):
            XTGrid(
                values=np.zeros(12),
                pitch_length=105.0,
                pitch_width=68.0,
                coord_system="spadl",
            )

    def test_rejects_unknown_coord_system(self) -> None:
        with pytest.raises(ValueError, match="Unknown coord_system"):
            XTGrid(
                values=np.zeros((12, 8)),
                pitch_length=105.0,
                pitch_width=68.0,
                coord_system="metric",  # type: ignore[arg-type]
            )

    def test_arbitrary_resolution_24x16(self) -> None:
        """Wrapper supports ExT v2's planned 24x16 resolution out of the box."""
        grid = _make_xt_grid(n_zones_x=24, n_zones_y=16)
        assert grid.shape == (24, 16)
        assert grid.n_zones_x == 24
        assert grid.n_zones_y == 16


# ---------------------------------------------------------------------------
# XTGrid.lookup
# ---------------------------------------------------------------------------


class TestXTGridLookup:
    """Tests for XTGrid.lookup with same-system and cross-system queries."""

    def test_lookup_same_coord_system_mid_pitch(self) -> None:
        grid = _make_xt_grid()  # SPADL, max_value=0.30, linear_x pattern
        # SPADL (52.5, 34) → cell [6, 4], value 0.30 * (6+1) / 12 = 0.175
        xt = grid.lookup(52.5, 34.0, input_coord_system="spadl")
        assert xt == pytest.approx(0.30 * 7 / 12)

    def test_lookup_cross_coord_system_mid_pitch(self) -> None:
        """StatsBomb (60, 40) → physical mid-pitch → same cell as SPADL (52.5, 34)."""
        grid = _make_xt_grid()
        xt_spadl = grid.lookup(52.5, 34.0, input_coord_system="spadl")
        xt_sb = grid.lookup(60.0, 40.0, input_coord_system="statsbomb")
        assert xt_sb == pytest.approx(xt_spadl)

    def test_lookup_cross_system_at_corners(self) -> None:
        """Corner positions in either system map to the same end-cells."""
        grid = _make_xt_grid()
        # Origin
        assert grid.lookup(0.0, 0.0, input_coord_system="spadl") == pytest.approx(
            grid.lookup(0.0, 0.0, input_coord_system="statsbomb")
        )
        # Attacking corner: SPADL (104.9, 67.9) ≈ StatsBomb (119.9, 79.9)
        assert grid.lookup(104.9, 67.9, input_coord_system="spadl") == pytest.approx(
            grid.lookup(119.9, 79.9, input_coord_system="statsbomb")
        )

    def test_lookup_clamps_out_of_bounds_high(self) -> None:
        grid = _make_xt_grid()
        # SPADL x=110 > pitch_length 105 → clamps to last zone (zone_x=11)
        xt = grid.lookup(110.0, 67.0, input_coord_system="spadl")
        # Last cell value: 0.30 * 12 / 12 = 0.30
        assert xt == pytest.approx(0.30)

    def test_lookup_clamps_out_of_bounds_negative(self) -> None:
        grid = _make_xt_grid()
        xt = grid.lookup(-5.0, -5.0, input_coord_system="spadl")
        # First cell value: 0.30 * 1 / 12 = 0.025
        assert xt == pytest.approx(0.30 / 12)

    def test_lookup_nan_x_returns_zero(self) -> None:
        grid = _make_xt_grid()
        assert grid.lookup(float("nan"), 40.0, input_coord_system="spadl") == 0.0

    def test_lookup_nan_y_returns_zero(self) -> None:
        grid = _make_xt_grid()
        assert grid.lookup(40.0, float("nan"), input_coord_system="spadl") == 0.0

    def test_lookup_arbitrary_resolution(self) -> None:
        """Wrapper derives binning from grid shape — supports ExT v2's 24x16."""
        grid = _make_xt_grid(n_zones_x=24, n_zones_y=16, max_value=0.40)
        # SPADL (52.5, 34) on 24x16 grid → cell [12, 8]
        # Value: 0.40 * (12+1) / 24 = 0.21666...
        xt = grid.lookup(52.5, 34.0, input_coord_system="spadl")
        assert xt == pytest.approx(0.40 * 13 / 24)


# ---------------------------------------------------------------------------
# XTGrid.to_dataframe
# ---------------------------------------------------------------------------


class TestXTGridToDataFrame:
    """Tests for XTGrid.to_dataframe serialization."""

    def test_default_columns(self) -> None:
        grid = _make_xt_grid()
        df = grid.to_dataframe()
        assert set(df.columns) == {"zone_x", "zone_y", "xt_value"}
        assert len(df) == 96

    def test_with_competition_id_column(self) -> None:
        grid = _make_xt_grid(competition_id="test_comp")
        df = grid.to_dataframe()
        assert "competition_id" in df.columns
        assert all(df["competition_id"] == "test_comp")

    def test_values_rounded_to_5_decimals(self) -> None:
        values = np.array([[0.123456789]])
        grid = XTGrid(
            values=values,
            pitch_length=105.0,
            pitch_width=68.0,
            coord_system="spadl",
        )
        df = grid.to_dataframe()
        assert df.iloc[0]["xt_value"] == 0.12346

    def test_arbitrary_resolution_serializes(self) -> None:
        grid = _make_xt_grid(n_zones_x=24, n_zones_y=16)
        df = grid.to_dataframe()
        assert len(df) == 24 * 16


# ---------------------------------------------------------------------------
# XTGrid.validate_structural
# ---------------------------------------------------------------------------


class TestXTGridStructuralValidation:
    """Tests for XTGrid.validate_structural method."""

    def test_passes_valid_grid_no_max_value(self) -> None:
        grid = _make_xt_grid(max_value=0.30)
        grid.validate_structural()  # should not raise

    def test_passes_valid_grid_with_legacy_v1_max_value(self) -> None:
        grid = _make_xt_grid(max_value=0.30)
        grid.validate_structural(max_value=0.50)  # v1 default

    def test_rejects_negative_values(self) -> None:
        values = np.full((12, 8), -0.1)
        grid = XTGrid(
            values=values,
            pitch_length=105.0,
            pitch_width=68.0,
            coord_system="spadl",
        )
        with pytest.raises(ValueError, match="negative"):
            grid.validate_structural()

    def test_rejects_value_exceeding_max(self) -> None:
        # Build a monotonic grid with peak at 0.6 (exceeds 0.50 cap)
        grid = _make_xt_grid(max_value=0.60)
        with pytest.raises(ValueError, match="exceeds max_value"):
            grid.validate_structural(max_value=0.50)

    def test_no_max_value_allows_v2_high_values(self) -> None:
        """ExT v2 conditional grids may exceed 0.50; opt-out via max_value=None."""
        grid = _make_xt_grid(max_value=0.85)
        grid.validate_structural()  # no max_value passed → no upper bound check

    def test_rejects_range_too_narrow(self) -> None:
        # Constant grid — range == 0, fails the 0.05 narrowness threshold
        grid = _make_xt_grid(pattern="constant", max_value=0.10)
        with pytest.raises(ValueError, match="range too narrow"):
            grid.validate_structural()

    def test_rejects_non_monotonic_rows(self) -> None:
        # Reversed gradient → row means decrease left-to-right
        grid = _make_xt_grid(pattern="reverse_x", max_value=0.30)
        with pytest.raises(ValueError, match="monoton"):
            grid.validate_structural()


# ---------------------------------------------------------------------------
# XTGrid.validate_differential
# ---------------------------------------------------------------------------


class TestXTGridDifferentialValidation:
    """Tests for XTGrid.validate_differential method."""

    def test_no_previous_skips_check(self) -> None:
        new = _make_xt_grid(max_value=0.30)
        new.validate_differential(None)  # should not raise

    def test_passes_when_change_within_threshold(self) -> None:
        previous = _make_xt_grid(max_value=0.30)
        new = _make_xt_grid(max_value=0.33)  # +10%
        new.validate_differential(previous, max_relative_change=0.30)

    def test_rejects_when_increase_exceeds_threshold(self) -> None:
        previous = _make_xt_grid(max_value=0.30)
        new = _make_xt_grid(max_value=0.50)  # +67%
        with pytest.raises(ValueError, match="changed by"):
            new.validate_differential(previous, max_relative_change=0.30)

    def test_rejects_when_decrease_exceeds_threshold(self) -> None:
        previous = _make_xt_grid(max_value=0.30)
        new = _make_xt_grid(max_value=0.10)  # -67%
        with pytest.raises(ValueError, match="changed by"):
            new.validate_differential(previous, max_relative_change=0.30)

    def test_handles_zero_previous_baseline(self) -> None:
        # Degenerate previous (all zeros) — relative change undefined; skip.
        previous_values = np.zeros((12, 8))
        previous = XTGrid(
            values=previous_values,
            pitch_length=105.0,
            pitch_width=68.0,
            coord_system="spadl",
        )
        new = _make_xt_grid(max_value=0.30)
        new.validate_differential(previous)  # should not raise

    def test_custom_threshold(self) -> None:
        previous = _make_xt_grid(max_value=0.30)
        new = _make_xt_grid(max_value=0.45)  # +50%
        # Tighter threshold rejects
        with pytest.raises(ValueError, match="changed by"):
            new.validate_differential(previous, max_relative_change=0.20)
        # Looser threshold accepts
        new.validate_differential(previous, max_relative_change=0.60)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Test module constants are correct."""

    def test_move_types_are_frozenset(self) -> None:
        assert isinstance(_MOVE_TYPES, frozenset)

    def test_shot_types_are_frozenset(self) -> None:
        assert isinstance(_SHOT_TYPES, frozenset)

    def test_no_overlap(self) -> None:
        assert _MOVE_TYPES.isdisjoint(_SHOT_TYPES)
