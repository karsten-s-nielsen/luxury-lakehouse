"""Tests for the ExT v2 transition model.

Phase 0's ``SinghTransitionMatrix`` reimplements v1's
``analytics.expected_threat._build_transition_matrix`` independently and
must match it numerically given equivalent inputs (Phase 0 stop condition,
design spec §6).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.expected_threat import (
    _MOVE_TYPES as _V1_MOVE_TYPES,
)
from analytics.expected_threat import (
    ExpectedThreatParams as _V1Params,
)
from analytics.expected_threat import (
    _assign_zones as _v1_assign_zones,
)
from analytics.expected_threat import (
    _build_transition_matrix as _v1_build_transition,
)
from analytics.ext_v2.transition import (
    REQUIRED_COLUMNS,
    SINGH_MOVE_TYPES,
    GridSpec,
    SinghTransitionMatrix,
    TransitionModel,
    _assign_zones,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_synthetic_actions(
    n: int = 200,
    *,
    seed: int = 0,
    pitch_length: float = 105.0,
    pitch_width: float = 68.0,
) -> pd.DataFrame:
    """Build a synthetic SPADL actions DataFrame spanning multiple types + results."""
    rng = np.random.default_rng(seed)
    types = rng.choice(
        [*SINGH_MOVE_TYPES, "shot", "shot_freekick"],
        size=n,
    )
    # Make most moves successful but include some failures to exercise the filter.
    results = rng.choice(["success", "fail"], size=n, p=[0.7, 0.3])
    return pd.DataFrame(
        {
            "type_name": types,
            "result_name": results,
            "start_x": rng.uniform(0, pitch_length, n),
            "start_y": rng.uniform(0, pitch_width, n),
            "end_x": rng.uniform(0, pitch_length, n),
            "end_y": rng.uniform(0, pitch_width, n),
        }
    )


# ---------------------------------------------------------------------------
# v1 ↔ v2 parity (constants + helpers)
# ---------------------------------------------------------------------------


class TestParityWithV1:
    """v2's SPADL-domain constants and binning must equal v1's exactly."""

    def test_move_types_equal(self) -> None:
        assert SINGH_MOVE_TYPES == _V1_MOVE_TYPES

    def test_assign_zones_equal_default_grid(self) -> None:
        rng = np.random.default_rng(0)
        x = rng.uniform(0, 105.0, 1000)
        y = rng.uniform(0, 68.0, 1000)
        v2_zones = _assign_zones(x, y, GridSpec())
        v1_zones = _v1_assign_zones(x, y, _V1Params())
        np.testing.assert_array_equal(v2_zones, v1_zones)

    def test_assign_zones_equal_custom_grid(self) -> None:
        rng = np.random.default_rng(1)
        x = rng.uniform(0, 105.0, 500)
        y = rng.uniform(0, 68.0, 500)
        v2_zones = _assign_zones(x, y, GridSpec(n_zones_x=24, n_zones_y=16))
        v1_zones = _v1_assign_zones(x, y, _V1Params(n_zones_x=24, n_zones_y=16))
        np.testing.assert_array_equal(v2_zones, v1_zones)


# ---------------------------------------------------------------------------
# SinghTransitionMatrix — numerical match vs v1 (Phase 0 stop condition)
# ---------------------------------------------------------------------------


class TestSinghMatchesV1:
    """SinghTransitionMatrix(grid).fit(actions).matrix == v1's pipeline output."""

    @pytest.mark.parametrize("seed", [0, 1, 7, 42])
    def test_default_grid_matches(self, seed: int) -> None:
        actions = _make_synthetic_actions(n=500, seed=seed)
        v2_matrix = SinghTransitionMatrix().fit(actions).matrix

        # Reproduce v1's pipeline manually.
        type_names = actions["type_name"].to_numpy()
        result_names = actions["result_name"].to_numpy()
        is_move = np.array([t in _V1_MOVE_TYPES for t in type_names])
        is_success = result_names == "success"
        mask = is_move & is_success
        params = _V1Params()
        start_zones = _v1_assign_zones(actions["start_x"].to_numpy(), actions["start_y"].to_numpy(), params)
        end_zones = _v1_assign_zones(actions["end_x"].to_numpy(), actions["end_y"].to_numpy(), params)
        v1_matrix = _v1_build_transition(start_zones[mask], end_zones[mask], params.n_zones_x * params.n_zones_y)

        np.testing.assert_array_equal(v2_matrix, v1_matrix)

    def test_24x16_grid_matches(self) -> None:
        actions = _make_synthetic_actions(n=2000, seed=3)
        grid = GridSpec(n_zones_x=24, n_zones_y=16)
        v2_matrix = SinghTransitionMatrix(grid=grid).fit(actions).matrix

        type_names = actions["type_name"].to_numpy()
        result_names = actions["result_name"].to_numpy()
        mask = np.array([t in _V1_MOVE_TYPES for t in type_names]) & (result_names == "success")
        params = _V1Params(n_zones_x=24, n_zones_y=16)
        start_zones = _v1_assign_zones(actions["start_x"].to_numpy(), actions["start_y"].to_numpy(), params)
        end_zones = _v1_assign_zones(actions["end_x"].to_numpy(), actions["end_y"].to_numpy(), params)
        v1_matrix = _v1_build_transition(start_zones[mask], end_zones[mask], 24 * 16)

        np.testing.assert_array_equal(v2_matrix, v1_matrix)


# ---------------------------------------------------------------------------
# SinghTransitionMatrix — structural invariants
# ---------------------------------------------------------------------------


class TestSinghStructure:
    def test_matrix_shape(self) -> None:
        actions = _make_synthetic_actions()
        m = SinghTransitionMatrix().fit(actions).matrix
        assert m.shape == (96, 96)

    def test_matrix_shape_custom_grid(self) -> None:
        actions = _make_synthetic_actions(n=2000)
        m = SinghTransitionMatrix(grid=GridSpec(n_zones_x=6, n_zones_y=4)).fit(actions).matrix
        assert m.shape == (24, 24)

    def test_rows_with_outgoing_moves_sum_to_one(self) -> None:
        actions = _make_synthetic_actions(n=5000, seed=5)
        m = SinghTransitionMatrix().fit(actions).matrix
        row_sums = m.sum(axis=1)
        # Rows with at least one outgoing move should sum to 1.0; rows with zero
        # outgoing moves stay at 0.0 (matches v1 behavior — see test below).
        nonzero_rows = row_sums > 0
        np.testing.assert_allclose(row_sums[nonzero_rows], 1.0, atol=1e-12)

    def test_empty_rows_stay_zero(self) -> None:
        # Construct actions that touch only zone 0; other rows should be zero.
        actions = pd.DataFrame(
            {
                "type_name": ["pass"] * 5,
                "result_name": ["success"] * 5,
                "start_x": [0.5] * 5,
                "start_y": [0.5] * 5,
                "end_x": [10.0] * 5,
                "end_y": [10.0] * 5,
            }
        )
        m = SinghTransitionMatrix().fit(actions).matrix
        # Zone 0 has outgoing rows; everything else is zero
        assert m[0].sum() > 0
        for i in range(1, 96):
            assert m[i].sum() == 0.0

    def test_single_transition_lands_in_correct_cell(self) -> None:
        # One pass from corner zone to far corner zone
        grid = GridSpec(n_zones_x=2, n_zones_y=2)
        actions = pd.DataFrame(
            {
                "type_name": ["pass"],
                "result_name": ["success"],
                "start_x": [0.0],
                "start_y": [0.0],
                "end_x": [104.9],
                "end_y": [67.9],
            }
        )
        m = SinghTransitionMatrix(grid=grid).fit(actions).matrix
        # Zone (0,0) flat index = 0; zone (1,1) flat index = 1*2+1 = 3
        assert m[0, 3] == 1.0
        assert m[0, 0] == 0.0
        assert m[0, 1] == 0.0
        assert m[0, 2] == 0.0


# ---------------------------------------------------------------------------
# SinghTransitionMatrix — filter behaviour
# ---------------------------------------------------------------------------


class TestSinghFiltering:
    def test_failed_moves_excluded(self) -> None:
        actions = pd.DataFrame(
            {
                "type_name": ["pass", "pass"],
                "result_name": ["success", "fail"],
                "start_x": [10.0, 10.0],
                "start_y": [10.0, 10.0],
                "end_x": [20.0, 20.0],
                "end_y": [20.0, 20.0],
            }
        )
        m = SinghTransitionMatrix().fit(actions).matrix
        # Two actions sharing zones; only one (success) contributes
        # The single successful pass produces a row-normalized 1.0
        assert m.sum() == 1.0

    def test_shots_excluded(self) -> None:
        actions = pd.DataFrame(
            {
                "type_name": ["shot", "shot", "shot"],
                "result_name": ["success", "fail", "success"],
                "start_x": [50.0, 60.0, 70.0],
                "start_y": [30.0, 30.0, 30.0],
                "end_x": [100.0, 100.0, 100.0],
                "end_y": [40.0, 40.0, 40.0],
            }
        )
        m = SinghTransitionMatrix().fit(actions).matrix
        # Shots are not moves — matrix is all zero
        assert m.sum() == 0.0


# ---------------------------------------------------------------------------
# SinghTransitionMatrix — edge cases
# ---------------------------------------------------------------------------


class TestSinghEdgeCases:
    def test_empty_dataframe_yields_zero_matrix(self) -> None:
        empty = pd.DataFrame(
            {col: pd.Series([], dtype=float if "x" in col or "y" in col else str) for col in REQUIRED_COLUMNS}
        )
        m = SinghTransitionMatrix().fit(empty).matrix
        assert m.shape == (96, 96)
        np.testing.assert_array_equal(m, np.zeros((96, 96)))


# ---------------------------------------------------------------------------
# SinghTransitionMatrix — API contract
# ---------------------------------------------------------------------------


class TestSinghAPI:
    def test_fit_returns_self(self) -> None:
        actions = _make_synthetic_actions(n=10)
        result = SinghTransitionMatrix().fit(actions)
        assert isinstance(result, SinghTransitionMatrix)

    def test_matrix_before_fit_raises(self) -> None:
        with pytest.raises(RuntimeError, match="fit"):
            SinghTransitionMatrix().matrix  # noqa: B018 — intentional access

    def test_subclass_of_transition_model(self) -> None:
        assert issubclass(SinghTransitionMatrix, TransitionModel)

    def test_rejects_missing_columns(self) -> None:
        actions = pd.DataFrame({"type_name": ["pass"], "start_x": [1.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            SinghTransitionMatrix().fit(actions)


# ---------------------------------------------------------------------------
# GridSpec
# ---------------------------------------------------------------------------


class TestGridSpec:
    def test_default_matches_v1_params(self) -> None:
        spec = GridSpec()
        v1 = _V1Params()
        assert spec.n_zones_x == v1.n_zones_x
        assert spec.n_zones_y == v1.n_zones_y
        assert spec.pitch_length == v1.pitch_length
        assert spec.pitch_width == v1.pitch_width

    def test_n_zones_property(self) -> None:
        assert GridSpec().n_zones == 96
        assert GridSpec(n_zones_x=24, n_zones_y=16).n_zones == 384

    def test_frozen(self) -> None:
        spec = GridSpec()
        with pytest.raises((AttributeError, TypeError)):
            spec.n_zones_x = 99  # type: ignore[misc]
