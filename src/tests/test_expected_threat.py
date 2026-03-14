"""Tests for the Expected Threat (xT) Markov chain computation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.expected_threat import (
    _MOVE_TYPES,
    _SHOT_TYPES,
    ExpectedThreatParams,
    _assign_zones,
    _build_transition_matrix,
    _value_iteration_numpy,
    compute_expected_threat_grid,
    grid_to_dataframe,
    validate_xt_grid,
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
    """Test end-to-end grid computation."""

    def test_output_shape_default(self) -> None:
        actions = _make_actions(
            types=["pass"] * 50 + ["shot"] * 10,
            results=["success"] * 50 + ["success"] * 5 + ["fail"] * 5,
            start_positions=[(float(i * 2), float(i % 8) * 8) for i in range(60)],
            end_positions=[(float(i * 2 + 1), float(i % 8) * 8) for i in range(60)],
        )

        grid = compute_expected_threat_grid(actions)
        assert grid.shape == (12, 8)

    def test_values_non_negative(self) -> None:
        actions = _make_actions(
            types=["pass"] * 100 + ["shot"] * 20,
            results=["success"] * 90 + ["fail"] * 10 + ["success"] * 10 + ["fail"] * 10,
            start_positions=[(float(i % 12) * 8.75, float(i % 8) * 8.5) for i in range(120)],
            end_positions=[(float((i + 1) % 12) * 8.75, float((i + 1) % 8) * 8.5) for i in range(120)],
        )

        grid = compute_expected_threat_grid(actions)
        assert np.all(grid >= 0)

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
        np.testing.assert_allclose(grid, 0.0)


# ---------------------------------------------------------------------------
# Grid to DataFrame
# ---------------------------------------------------------------------------


class TestGridToDataFrame:
    """Test grid-to-DataFrame conversion."""

    def test_default_columns(self) -> None:
        grid = np.random.default_rng(42).random((12, 8))
        df = grid_to_dataframe(grid)
        assert set(df.columns) == {"zone_x", "zone_y", "xt_value"}
        assert len(df) == 96

    def test_with_competition_id(self) -> None:
        grid = np.zeros((2, 2))
        df = grid_to_dataframe(grid, competition_id="test_comp")
        assert "competition_id" in df.columns
        assert all(df["competition_id"] == "test_comp")

    def test_values_rounded(self) -> None:
        grid = np.array([[0.123456789]])
        df = grid_to_dataframe(grid)
        assert df.iloc[0]["xt_value"] == 0.12346  # rounded to 5 decimal places


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


# ---------------------------------------------------------------------------
# Grid validation
# ---------------------------------------------------------------------------


class TestGridValidation:
    """Tests for pipeline-level data quality checks."""

    def test_validate_grid_passes_valid_grid(self) -> None:
        grid = np.array([[0.01 * (x + 1) for y in range(8)] for x in range(12)])
        validate_xt_grid(grid)  # Should not raise

    def test_validate_grid_rejects_out_of_range(self) -> None:
        grid = np.full((12, 8), 0.1)
        grid[0, 0] = 0.0001  # Below 0.001 lower bound
        with pytest.raises(ValueError, match="out of expected range"):
            validate_xt_grid(grid)

    def test_validate_grid_rejects_wrong_shape(self) -> None:
        grid = np.zeros((10, 6))
        with pytest.raises(ValueError, match="shape"):
            validate_xt_grid(grid)

    def test_validate_grid_checks_monotonicity(self) -> None:
        # Reversed gradient: high values at x=0, low at x=11 — fails monotonicity
        grid = np.array([[0.3 - 0.02 * x for y in range(8)] for x in range(12)])
        with pytest.raises(ValueError, match="monoton"):
            validate_xt_grid(grid)
