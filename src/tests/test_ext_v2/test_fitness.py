"""Tests for the ExT v2 held-out NLL fitness function.

Per design spec §5.1: ``NLL = -mean(log P(actual_destination | source))``
on a held-out set of passes. For Phase 0, ``P`` is the producer's
transition matrix.

Synthetic-truth tests:

- uniform transition → NLL = log(n_zones)
- point-mass transition on observed ends → NLL = 0
- intermediate → bounded between the two
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from analytics.ext_v2.fitness import (
    NLL_REQUIRED_COLUMNS,
    compute_holdout_nll,
    compute_holdout_nll_per_competition,
)
from analytics.ext_v2.transition import GridSpec

# ---------------------------------------------------------------------------
# Synthetic Producer stub
# ---------------------------------------------------------------------------


class _StubProducer:
    """Minimal producer-shaped object exposing ``transition_matrix`` for tests."""

    def __init__(self, matrix: np.ndarray) -> None:
        self.transition_matrix = matrix


def _passes_in_zone(
    n: int,
    *,
    start_zone: tuple[int, int],
    end_zone: tuple[int, int],
    grid: GridSpec,
    competition_id: str = "X",
) -> pd.DataFrame:
    """Build n passes whose start/end land in the given zones (cell centers)."""
    sx_cell_w = grid.pitch_length / grid.n_zones_x
    sy_cell_w = grid.pitch_width / grid.n_zones_y
    return pd.DataFrame(
        {
            "competition_id": [competition_id] * n,
            "start_x": [(start_zone[0] + 0.5) * sx_cell_w] * n,
            "start_y": [(start_zone[1] + 0.5) * sy_cell_w] * n,
            "end_x": [(end_zone[0] + 0.5) * sx_cell_w] * n,
            "end_y": [(end_zone[1] + 0.5) * sy_cell_w] * n,
        }
    )


# ---------------------------------------------------------------------------
# compute_holdout_nll — synthetic truth
# ---------------------------------------------------------------------------


class TestNLLSyntheticTruth:
    """Hand-computed cases with known NLL."""

    def test_uniform_transition_yields_log_n_zones(self) -> None:
        grid = GridSpec(n_zones_x=4, n_zones_y=4)  # 16 zones
        n = grid.n_zones
        transition = np.full((n, n), 1.0 / n)
        producer = _StubProducer(transition)
        passes = _passes_in_zone(50, start_zone=(0, 0), end_zone=(2, 2), grid=grid)
        nll = compute_holdout_nll(producer, passes, grid=grid)
        assert nll == pytest.approx(math.log(n), abs=1e-10)

    def test_point_mass_transition_yields_zero(self) -> None:
        """If transition puts all mass on the actual end zone, NLL is exactly 0."""
        grid = GridSpec(n_zones_x=3, n_zones_y=3)
        n = grid.n_zones
        transition = np.zeros((n, n))
        # All passes start in zone (0,0) → flat 0; end in zone (2,2) → flat 8
        transition[0, 8] = 1.0
        # Other rows can stay at 0; we won't query them
        producer = _StubProducer(transition)
        passes = _passes_in_zone(20, start_zone=(0, 0), end_zone=(2, 2), grid=grid)
        nll = compute_holdout_nll(producer, passes, grid=grid)
        assert nll == pytest.approx(0.0, abs=1e-10)

    def test_intermediate_transition_yields_between(self) -> None:
        """Half-mass on actual end → NLL = -log(0.5)."""
        grid = GridSpec(n_zones_x=3, n_zones_y=3)
        n = grid.n_zones
        transition = np.zeros((n, n))
        transition[0, 8] = 0.5
        transition[0, 0] = 0.5
        producer = _StubProducer(transition)
        passes = _passes_in_zone(10, start_zone=(0, 0), end_zone=(2, 2), grid=grid)
        nll = compute_holdout_nll(producer, passes, grid=grid)
        assert nll == pytest.approx(-math.log(0.5), abs=1e-10)


# ---------------------------------------------------------------------------
# compute_holdout_nll — eps clipping for unobserved transitions
# ---------------------------------------------------------------------------


class TestNLLEpsilonClipping:
    """When training never saw a (s, d) pair, transition[s, d] = 0 → log(0). Clip via eps."""

    def test_zero_probability_clamps_to_eps(self) -> None:
        grid = GridSpec(n_zones_x=3, n_zones_y=3)
        n = grid.n_zones
        transition = np.zeros((n, n))
        # All zeros — every holdout row's prob is 0 → should clamp to eps
        producer = _StubProducer(transition)
        passes = _passes_in_zone(5, start_zone=(1, 1), end_zone=(2, 2), grid=grid)
        eps = 1e-10
        nll = compute_holdout_nll(producer, passes, grid=grid, eps=eps)
        assert nll == pytest.approx(-math.log(eps), abs=1e-10)

    def test_custom_eps(self) -> None:
        grid = GridSpec(n_zones_x=3, n_zones_y=3)
        transition = np.zeros((grid.n_zones, grid.n_zones))
        producer = _StubProducer(transition)
        passes = _passes_in_zone(3, start_zone=(0, 0), end_zone=(1, 1), grid=grid)
        nll = compute_holdout_nll(producer, passes, grid=grid, eps=1e-6)
        assert nll == pytest.approx(-math.log(1e-6), abs=1e-10)


# ---------------------------------------------------------------------------
# compute_holdout_nll — input handling
# ---------------------------------------------------------------------------


class TestNLLInputHandling:
    def test_empty_holdout_returns_nan(self) -> None:
        grid = GridSpec(n_zones_x=3, n_zones_y=3)
        transition = np.full((grid.n_zones, grid.n_zones), 1.0 / grid.n_zones)
        producer = _StubProducer(transition)
        empty = pd.DataFrame(
            {
                "competition_id": pd.Series([], dtype=str),
                "start_x": pd.Series([], dtype=float),
                "start_y": pd.Series([], dtype=float),
                "end_x": pd.Series([], dtype=float),
                "end_y": pd.Series([], dtype=float),
            }
        )
        nll = compute_holdout_nll(producer, empty, grid=grid)
        assert math.isnan(nll)

    def test_rejects_missing_columns(self) -> None:
        grid = GridSpec()
        transition = np.zeros((grid.n_zones, grid.n_zones))
        producer = _StubProducer(transition)
        passes = pd.DataFrame({"start_x": [1.0]})  # missing start_y, end_x, end_y
        with pytest.raises(ValueError, match="missing required columns"):
            compute_holdout_nll(producer, passes, grid=grid)


# ---------------------------------------------------------------------------
# compute_holdout_nll_per_competition
# ---------------------------------------------------------------------------


class TestNLLPerCompetition:
    """Per-comp aggregation; skips empty/zero-row groups gracefully."""

    def test_returns_dict_keyed_by_competition_id(self) -> None:
        grid = GridSpec(n_zones_x=3, n_zones_y=3)
        n = grid.n_zones
        transition = np.full((n, n), 1.0 / n)
        producer = _StubProducer(transition)
        # Two comps with different counts
        p_a = _passes_in_zone(10, start_zone=(0, 0), end_zone=(1, 1), grid=grid, competition_id="A")
        p_b = _passes_in_zone(5, start_zone=(2, 2), end_zone=(0, 0), grid=grid, competition_id="B")
        passes = pd.concat([p_a, p_b], ignore_index=True)
        per_comp = compute_holdout_nll_per_competition(producer, passes, grid=grid)
        assert set(per_comp.keys()) == {"A", "B"}
        # Both should be log(n_zones) under uniform transition
        assert per_comp["A"] == pytest.approx(math.log(n), abs=1e-10)
        assert per_comp["B"] == pytest.approx(math.log(n), abs=1e-10)

    def test_empty_holdout_returns_empty_dict(self) -> None:
        grid = GridSpec()
        transition = np.zeros((grid.n_zones, grid.n_zones))
        producer = _StubProducer(transition)
        empty = pd.DataFrame(
            {
                "competition_id": pd.Series([], dtype=str),
                "start_x": pd.Series([], dtype=float),
                "start_y": pd.Series([], dtype=float),
                "end_x": pd.Series([], dtype=float),
                "end_y": pd.Series([], dtype=float),
            }
        )
        per_comp = compute_holdout_nll_per_competition(producer, empty, grid=grid)
        assert per_comp == {}

    def test_rejects_missing_competition_id(self) -> None:
        grid = GridSpec()
        transition = np.zeros((grid.n_zones, grid.n_zones))
        producer = _StubProducer(transition)
        passes = pd.DataFrame(
            {
                "start_x": [1.0],
                "start_y": [1.0],
                "end_x": [2.0],
                "end_y": [2.0],
            }
        )
        with pytest.raises(ValueError, match="competition_id"):
            compute_holdout_nll_per_competition(producer, passes, grid=grid)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


class TestPublicSurface:
    def test_required_columns_constant(self) -> None:
        for col in ("start_x", "start_y", "end_x", "end_y"):
            assert col in NLL_REQUIRED_COLUMNS
