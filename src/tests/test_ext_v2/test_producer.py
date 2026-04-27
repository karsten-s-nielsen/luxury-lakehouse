"""Tests for the ExT v2 SinghProducer — Phase 0 stop condition.

The producer composes ``SinghTransitionMatrix`` + value iteration + per-zone
shot/goal/move-probability aggregation into an ``XTGrid``. The Phase 0 stop
condition (per design spec §6) requires byte-equivalent ``XTGrid.values``
to ``analytics.expected_threat.compute_expected_threat_grid`` on identical
inputs across multiple grid resolutions and seeds.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.expected_threat import (
    ExpectedThreatParams,
    XTGrid,
    compute_expected_threat_grid,
)
from analytics.ext_v2.producer import (
    Producer,
    SinghProducer,
)
from analytics.ext_v2.transition import (
    SINGH_MOVE_TYPES,
    GridSpec,
    SinghTransitionMatrix,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_SHOT_TYPES = ("shot", "shot_freekick", "shot_penalty")


def _make_realistic_actions(
    n: int = 1000,
    *,
    seed: int = 0,
    pitch_length: float = 105.0,
    pitch_width: float = 68.0,
) -> pd.DataFrame:
    """Build a synthetic SPADL actions DataFrame mixing moves and shots."""
    rng = np.random.default_rng(seed)
    types = rng.choice(
        [*SINGH_MOVE_TYPES, *_SHOT_TYPES],
        size=n,
        # Roughly realistic: ~95% moves, ~5% shots
        p=([0.95 / len(SINGH_MOVE_TYPES)] * len(SINGH_MOVE_TYPES)) + ([0.05 / len(_SHOT_TYPES)] * len(_SHOT_TYPES)),
    )
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
# SinghProducer numerical match vs v1 — Phase 0 stop condition
# ---------------------------------------------------------------------------


class TestSinghProducerMatchesV1:
    """Producer's XTGrid.values must reproduce v1's compute_expected_threat_grid.

    v1 routes value iteration to JAX when ``n_zones > 200``; v2 always uses
    numpy. JAX vs numpy reduction-ordering produces ~1e-7 float noise on
    larger grids, so:

    - Grids ≤ 200 zones (production 12x8 included) → strict equality.
    - Grids > 200 zones → ``allclose(rtol=1e-5, atol=1e-7)`` — looser by
      design, catches real implementation bugs while tolerating cross-
      backend float arithmetic.
    """

    @pytest.mark.parametrize("seed", [0, 1, 7, 42])
    @pytest.mark.parametrize(
        "n_zones_x,n_zones_y",
        [(12, 8), (16, 12), (6, 4)],
    )
    def test_xt_grid_values_match_numpy_path(self, seed: int, n_zones_x: int, n_zones_y: int) -> None:
        """Production-scale grids (≤200 zones): byte-for-byte numpy equality."""
        actions = _make_realistic_actions(n=2000, seed=seed)
        grid = GridSpec(n_zones_x=n_zones_x, n_zones_y=n_zones_y)
        v1_grid = compute_expected_threat_grid(
            actions,
            ExpectedThreatParams(n_zones_x=n_zones_x, n_zones_y=n_zones_y),
        )
        v2_grid = SinghProducer(grid=grid).fit(actions).xt_grid
        np.testing.assert_array_equal(v2_grid.values, v1_grid.values)

    @pytest.mark.parametrize("seed", [0, 1, 7, 42])
    def test_xt_grid_values_match_jax_path_24x16(self, seed: int) -> None:
        """Larger grids (>200 zones, v1 uses JAX): allclose to absorb float noise."""
        actions = _make_realistic_actions(n=2000, seed=seed)
        grid = GridSpec(n_zones_x=24, n_zones_y=16)
        v1_grid = compute_expected_threat_grid(actions, ExpectedThreatParams(n_zones_x=24, n_zones_y=16))
        v2_grid = SinghProducer(grid=grid).fit(actions).xt_grid
        np.testing.assert_allclose(v2_grid.values, v1_grid.values, rtol=1e-5, atol=1e-7)

    def test_xt_grid_metadata_matches(self) -> None:
        actions = _make_realistic_actions(n=500, seed=2)
        v1_grid = compute_expected_threat_grid(actions, competition_id="test_comp")
        v2_grid = SinghProducer().fit(actions, competition_id="test_comp").xt_grid

        assert v2_grid.shape == v1_grid.shape
        assert v2_grid.coord_system == v1_grid.coord_system
        assert v2_grid.pitch_length == v1_grid.pitch_length
        assert v2_grid.pitch_width == v1_grid.pitch_width
        assert v2_grid.competition_id == v1_grid.competition_id

    def test_empty_actions_match(self) -> None:
        empty = pd.DataFrame(
            {
                "type_name": pd.Series([], dtype=str),
                "result_name": pd.Series([], dtype=str),
                "start_x": pd.Series([], dtype=float),
                "start_y": pd.Series([], dtype=float),
                "end_x": pd.Series([], dtype=float),
                "end_y": pd.Series([], dtype=float),
            }
        )
        v1_grid = compute_expected_threat_grid(empty)
        v2_grid = SinghProducer().fit(empty).xt_grid
        np.testing.assert_array_equal(v2_grid.values, v1_grid.values)


# ---------------------------------------------------------------------------
# SinghProducer — return types
# ---------------------------------------------------------------------------


class TestSinghProducerReturnTypes:
    def test_xt_grid_is_xtgrid_instance(self) -> None:
        actions = _make_realistic_actions(n=200)
        grid = SinghProducer().fit(actions).xt_grid
        assert isinstance(grid, XTGrid)

    def test_transition_matrix_property(self) -> None:
        actions = _make_realistic_actions(n=200)
        producer = SinghProducer().fit(actions)
        m = producer.transition_matrix
        assert isinstance(m, np.ndarray)
        assert m.shape == (96, 96)

    def test_fit_returns_self(self) -> None:
        actions = _make_realistic_actions(n=10)
        producer = SinghProducer()
        result = producer.fit(actions)
        assert result is producer


# ---------------------------------------------------------------------------
# SinghProducer — composition with sub-modules
# ---------------------------------------------------------------------------


class TestSinghProducerComposition:
    def test_transition_matrix_matches_singh_transition_matrix(self) -> None:
        """Producer's internal transition matrix should equal SinghTransitionMatrix.fit(...).matrix."""
        actions = _make_realistic_actions(n=1000, seed=4)
        grid = GridSpec(n_zones_x=12, n_zones_y=8)
        from_producer = SinghProducer(grid=grid).fit(actions).transition_matrix
        from_helper = SinghTransitionMatrix(grid=grid).fit(actions).matrix
        np.testing.assert_array_equal(from_producer, from_helper)


# ---------------------------------------------------------------------------
# SinghProducer — API contract
# ---------------------------------------------------------------------------


class TestSinghProducerAPI:
    def test_xt_grid_before_fit_raises(self) -> None:
        with pytest.raises(RuntimeError, match="fit"):
            SinghProducer().xt_grid  # noqa: B018 — intentional access

    def test_transition_matrix_before_fit_raises(self) -> None:
        with pytest.raises(RuntimeError, match="fit"):
            SinghProducer().transition_matrix  # noqa: B018 — intentional access

    def test_subclass_of_producer(self) -> None:
        assert issubclass(SinghProducer, Producer)

    def test_rejects_missing_columns(self) -> None:
        actions = pd.DataFrame({"type_name": ["pass"], "start_x": [1.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            SinghProducer().fit(actions)


# ---------------------------------------------------------------------------
# SinghProducer — convergence parameters
# ---------------------------------------------------------------------------


class TestSinghProducerParams:
    def test_custom_max_iterations_propagates(self) -> None:
        """A tight max_iterations cap should still produce a (possibly partial) grid."""
        actions = _make_realistic_actions(n=500)
        grid = SinghProducer(max_iterations=3).fit(actions).xt_grid
        assert isinstance(grid, XTGrid)
        # Same shape regardless of convergence
        assert grid.shape == (12, 8)

    def test_custom_tolerance_propagates(self) -> None:
        actions = _make_realistic_actions(n=500)
        grid = SinghProducer(tolerance=1e-3).fit(actions).xt_grid
        assert isinstance(grid, XTGrid)


class TestKDESmoothedProducerComposition:
    """KDESmoothedProducer wraps KDESmoothedTransition; transition_matrix is delegation."""

    def test_subclass_of_producer(self) -> None:
        from analytics.ext_v2.producer import KDESmoothedProducer

        assert issubclass(KDESmoothedProducer, Producer)

    def test_transition_matrix_matches_kde_smoothed_transition(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition
        from analytics.ext_v2.producer import KDESmoothedProducer

        actions = _make_realistic_actions(n=5000, seed=29)
        from_producer = (
            KDESmoothedProducer(kernel="gaussian", bandwidth=2.0, adaptive=False).fit(actions).transition_matrix
        )
        from_helper = KDESmoothedTransition(kernel="gaussian", bandwidth=2.0, adaptive=False).fit(actions).matrix
        np.testing.assert_array_equal(from_producer, from_helper)

    def test_xt_grid_is_xtgrid_instance(self) -> None:
        from analytics.ext_v2.producer import KDESmoothedProducer

        actions = _make_realistic_actions(n=2000)
        grid = KDESmoothedProducer(bandwidth=1.5).fit(actions).xt_grid
        assert isinstance(grid, XTGrid)
        assert grid.shape == (12, 8)

    def test_kde_kwargs_propagate(self) -> None:
        """kernel, bandwidth, adaptive must round-trip through producer to its transition."""
        from analytics.ext_v2.producer import KDESmoothedProducer

        actions = _make_realistic_actions(n=2000, seed=31)
        p_a = KDESmoothedProducer(kernel="gaussian", bandwidth=0.5).fit(actions)
        p_b = KDESmoothedProducer(kernel="gaussian", bandwidth=2.0).fit(actions)
        # Different bandwidths -> different matrices (sanity that bandwidth wires through).
        assert not np.allclose(p_a.transition_matrix, p_b.transition_matrix)


class TestKDESmoothedProducerAPI:
    def test_xt_grid_before_fit_raises(self) -> None:
        from analytics.ext_v2.producer import KDESmoothedProducer

        with pytest.raises(RuntimeError, match="fit"):
            KDESmoothedProducer().xt_grid  # noqa: B018

    def test_transition_matrix_before_fit_raises(self) -> None:
        from analytics.ext_v2.producer import KDESmoothedProducer

        with pytest.raises(RuntimeError, match="fit"):
            KDESmoothedProducer().transition_matrix  # noqa: B018

    def test_fit_returns_self(self) -> None:
        from analytics.ext_v2.producer import KDESmoothedProducer

        actions = _make_realistic_actions(n=200)
        producer = KDESmoothedProducer()
        result = producer.fit(actions)
        assert result is producer

    def test_rejects_missing_columns(self) -> None:
        from analytics.ext_v2.producer import KDESmoothedProducer

        actions = pd.DataFrame({"type_name": ["pass"], "start_x": [1.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            KDESmoothedProducer().fit(actions)
