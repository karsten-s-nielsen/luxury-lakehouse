"""Tests for the ExT v2 KDE-smoothed transition model (Phase 1).

Phase 1's ``KDESmoothedTransition`` is a per-source-zone 2D
``sklearn.neighbors.KernelDensity`` wrapper with optional Silverman-scaled
per-row bandwidth. The Phase 1 stop condition (per design spec §10.3) is
held-out NLL < 3.7513; these tests enforce the per-row contracts that
make that stop condition trustworthy.

Locked design decisions exercised here (per spec §10.3):

- Library: sklearn.neighbors.KernelDensity (Q1)
- Per-source-zone destination KDE, point evaluation at zone centers (Q2)
- Per-row Silverman with global multiplier; isotropic sigma proxy (Q3)
- Eps treatment: primary 1e-10, diagnostic 1e-300 (Q4)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.ext_v2.transition import (
    GridSpec,
)


def _make_clustered_actions(
    *,
    n_per_source_zone: int = 50,
    cluster_destination: bool = True,
    seed: int = 0,
) -> pd.DataFrame:
    """Build synthetic SPADL successful-pass actions with destinations clustered per source zone.

    Each source zone in a 12x8 grid gets ``n_per_source_zone`` events. When
    ``cluster_destination=True``, destinations are tightly clustered around
    a per-source-zone destination centroid (so KDE smoothing has a clear
    target); when False, destinations are pitch-uniform (KDE is degenerate).
    """
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    grid = GridSpec()  # 12x8, SPADL 105x68
    cell_w = grid.pitch_length / grid.n_zones_x  # 8.75 m
    cell_h = grid.pitch_width / grid.n_zones_y  # 8.50 m
    for zx in range(grid.n_zones_x):
        for zy in range(grid.n_zones_y):
            sx_centre = (zx + 0.5) * cell_w
            sy_centre = (zy + 0.5) * cell_h
            # Source positions: jittered around source-zone centre (small noise).
            start_x = rng.normal(sx_centre, 0.5, n_per_source_zone)
            start_y = rng.normal(sy_centre, 0.5, n_per_source_zone)
            if cluster_destination:
                # Destination centroid: shifted +25m forward (clipped to pitch).
                dx_centre = min(sx_centre + 25.0, grid.pitch_length - 1.0)
                dy_centre = sy_centre
                end_x = rng.normal(dx_centre, 2.0, n_per_source_zone)
                end_y = rng.normal(dy_centre, 2.0, n_per_source_zone)
            else:
                end_x = rng.uniform(0, grid.pitch_length, n_per_source_zone)
                end_y = rng.uniform(0, grid.pitch_width, n_per_source_zone)
            # Clip to pitch bounds
            end_x = np.clip(end_x, 0.01, grid.pitch_length - 0.01)
            end_y = np.clip(end_y, 0.01, grid.pitch_width - 0.01)
            start_x = np.clip(start_x, 0.01, grid.pitch_length - 0.01)
            start_y = np.clip(start_y, 0.01, grid.pitch_width - 0.01)
            for i in range(n_per_source_zone):
                rows.append(
                    {
                        "type_name": "pass",
                        "result_name": "success",
                        "start_x": float(start_x[i]),
                        "start_y": float(start_y[i]),
                        "end_x": float(end_x[i]),
                        "end_y": float(end_y[i]),
                    }
                )
    return pd.DataFrame(rows)


class TestKDESmoothedTransitionContract:
    """KDESmoothedTransition skeleton — constructor, ABC, fit/matrix guards."""

    def test_subclass_of_transition_model(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition
        from analytics.ext_v2.transition import TransitionModel

        assert issubclass(KDESmoothedTransition, TransitionModel)

    def test_default_constructor(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        kde = KDESmoothedTransition()
        assert kde.kernel == "gaussian"
        assert kde.bandwidth == 1.0
        assert kde.adaptive is False
        assert kde.grid.n_zones_x == 12
        assert kde.grid.n_zones_y == 8

    def test_custom_constructor(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        kde = KDESmoothedTransition(kernel="epanechnikov", bandwidth=0.5, adaptive=True)
        assert kde.kernel == "epanechnikov"
        assert kde.bandwidth == 0.5
        assert kde.adaptive is True

    def test_matrix_before_fit_raises(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        with pytest.raises(RuntimeError, match="fit"):
            KDESmoothedTransition().matrix  # noqa: B018 — intentional access

    def test_fit_returns_self(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=2)
        result = KDESmoothedTransition().fit(actions)
        assert isinstance(result, KDESmoothedTransition)

    def test_fit_rejects_missing_columns(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        bad = pd.DataFrame({"type_name": ["pass"], "start_x": [1.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            KDESmoothedTransition().fit(bad)


class TestRowStochasticGaussian:
    """Default gaussian KDE produces row-stochastic float64 (n_zones, n_zones)."""

    def test_matrix_shape_default_grid(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=20)
        m = KDESmoothedTransition().fit(actions).matrix
        assert m.shape == (96, 96)

    def test_matrix_shape_custom_grid(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=20)
        kde = KDESmoothedTransition(grid=GridSpec(n_zones_x=6, n_zones_y=4))
        # Re-make actions to span the smaller grid (24 source zones)
        m = kde.fit(actions).matrix
        assert m.shape == (24, 24)

    def test_matrix_dtype(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=20)
        m = KDESmoothedTransition().fit(actions).matrix
        assert m.dtype == np.float64

    def test_rows_sum_to_one(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=20)
        m = KDESmoothedTransition().fit(actions).matrix
        row_sums = m.sum(axis=1)
        np.testing.assert_allclose(row_sums, 1.0, atol=1e-10)

    def test_gaussian_positive_at_observed_destinations(self) -> None:
        """Gaussian kernel: transitions to *observed* destination zones are positive.

        Note: gaussian density is mathematically unbounded but underflows to 0.0
        in float64 at destinations many bandwidths away from training events.
        The practical assertion is that transitions to zones containing
        training events are strictly positive — this is what makes the eps
        floor dormant on most rows in the spec §10.3 Q4 diagnostic.
        """
        from analytics.ext_v2.kde import KDESmoothedTransition
        from analytics.ext_v2.transition import _assign_zones

        actions = _make_clustered_actions(n_per_source_zone=20)
        m = KDESmoothedTransition(kernel="gaussian", bandwidth=1.0).fit(actions).matrix

        grid = GridSpec()
        successful = actions[(actions["type_name"] == "pass") & (actions["result_name"] == "success")]
        start_zones = _assign_zones(
            np.asarray(successful["start_x"], dtype=np.float64),
            np.asarray(successful["start_y"], dtype=np.float64),
            grid,
        )
        end_zones = _assign_zones(
            np.asarray(successful["end_x"], dtype=np.float64),
            np.asarray(successful["end_y"], dtype=np.float64),
            grid,
        )
        for s, d in zip(start_zones.tolist(), end_zones.tolist(), strict=True):
            assert m[s, d] > 0, f"Observed transition ({s}, {d}) has zero probability"


class TestKernelCorrectness:
    """For each named kernel, fitted matrix matches a hand-built sklearn reference."""

    @pytest.mark.parametrize("kernel", ["gaussian", "epanechnikov", "tophat"])
    def test_kernel_matches_hand_built_reference(self, kernel: str) -> None:
        from sklearn.neighbors import KernelDensity

        from analytics.ext_v2.kde import KDESmoothedTransition
        from analytics.ext_v2.transition import _assign_zones

        actions = _make_clustered_actions(n_per_source_zone=20, seed=7)
        bw = 1.5
        kde_v2 = KDESmoothedTransition(kernel=kernel, bandwidth=bw).fit(actions)  # type: ignore[arg-type]
        m_v2 = kde_v2.matrix

        # Hand-built reference for source zone 0 only — same kernel, same bw,
        # same destination-zone centre evaluation, same row-normalization.
        grid = GridSpec()
        successful = actions[(actions["type_name"] == "pass") & (actions["result_name"] == "success")]
        start_zones = _assign_zones(
            np.asarray(successful["start_x"], dtype=np.float64),
            np.asarray(successful["start_y"], dtype=np.float64),
            grid,
        )
        zone0_mask = start_zones == 0
        if zone0_mask.sum() == 0:
            pytest.skip("seed produced no zone-0 events")
        end_xy = np.column_stack(
            [
                np.asarray(successful["end_x"], dtype=np.float64)[zone0_mask],
                np.asarray(successful["end_y"], dtype=np.float64)[zone0_mask],
            ]
        )
        ref_kde = KernelDensity(kernel=kernel, bandwidth=bw).fit(end_xy)
        cell_w = grid.pitch_length / grid.n_zones_x
        cell_h = grid.pitch_width / grid.n_zones_y
        zone_centres = np.array(
            [[(dz // grid.n_zones_y + 0.5) * cell_w, (dz % grid.n_zones_y + 0.5) * cell_h] for dz in range(96)]
        )
        ref_log_density = ref_kde.score_samples(zone_centres)
        ref_density = np.exp(ref_log_density)
        ref_row = ref_density / ref_density.sum() if ref_density.sum() > 0 else np.zeros(96)

        np.testing.assert_allclose(m_v2[0], ref_row, rtol=1e-10, atol=1e-12)

    def test_unknown_kernel_raises(self) -> None:
        """Truly-unknown kernel names propagate sklearn's ValueError at fit time.

        Note: sklearn supports more kernels than our Optuna axis names
        (gaussian/tophat/epanechnikov/exponential/linear/cosine). We don't
        validate kernel choice at the KDESmoothedTransition layer — that's
        the Optuna axis's job to constrain to the design-spec set. This
        test just confirms invalid garbage propagates as ValueError, not as
        a silent fit-time success.
        """
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=20)
        with pytest.raises(ValueError):
            KDESmoothedTransition(kernel="not-a-real-kernel-asdf", bandwidth=1.0).fit(actions)  # type: ignore[arg-type]


class TestSmoothingConvergesToSingh:
    """As bandwidth → 0, KDE-smoothed matrix approaches Singh's discrete count.

    With a tiny bandwidth and the gaussian kernel, the KDE concentrates
    almost all density at training-event destinations, so the smoothed
    matrix's row-normalized values approach the empirical conditional
    distribution that Singh computes via discrete counts. The match is
    not exact (Singh bins by zone, KDE smooths by position), but they
    agree on which destination zones get most of the mass.
    """

    def test_tiny_bandwidth_concentrates_mass_on_observed_destinations(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition
        from analytics.ext_v2.transition import SinghTransitionMatrix

        actions = _make_clustered_actions(n_per_source_zone=50, seed=11)
        kde = KDESmoothedTransition(kernel="gaussian", bandwidth=0.05).fit(actions)
        singh = SinghTransitionMatrix().fit(actions)

        # Per source zone with at least one event, the *argmax* destination zone
        # under tiny-bandwidth KDE should match Singh's argmax destination
        # (both pick whichever destination zone has the most observed events).
        for s in range(96):
            kde_argmax = int(np.argmax(kde.matrix[s]))
            singh_argmax = int(np.argmax(singh.matrix[s]))
            if singh.matrix[s].sum() == 0:
                continue  # zero-event source zone — both rows are empty, skip
            assert kde_argmax == singh_argmax, (
                f"bandwidth → 0 should recover Singh's argmax at source {s}: KDE={kde_argmax}, Singh={singh_argmax}"
            )


class TestSilvermanAdaptive:
    """adaptive=True applies per-row Silverman scaling; multiplier semantics hold."""

    def test_silverman_2d_formula_exact(self) -> None:
        """silverman_2d(n, sigma) = n^(-1/6) * sigma."""
        from analytics.ext_v2.kde import silverman_2d

        # Spot-check several (n, sigma) pairs.
        assert silverman_2d(64, 1.0) == pytest.approx(64 ** (-1 / 6))
        assert silverman_2d(1000, 2.5) == pytest.approx(1000 ** (-1 / 6) * 2.5)
        assert silverman_2d(1, 1.0) == pytest.approx(1.0)  # n=1 → bandwidth = sigma

    def test_silverman_2d_zero_n_raises(self) -> None:
        """silverman_2d undefined at n=0; caller must use the row-mean fallback."""
        from analytics.ext_v2.kde import silverman_2d

        with pytest.raises(ValueError, match="n must be"):
            silverman_2d(0, 1.0)

    def test_silverman_2d_smaller_n_yields_wider_bandwidth(self) -> None:
        """silverman_2d's defining property: smaller n → wider bandwidth (for fixed sigma).

        This is the Silverman 1986 §4.3 design property — sparse rows get
        widened automatically. Asserting it directly on the helper rather
        than indirectly via matrix entropy because matrix entropy at the
        Phase 1 grid scale conflates adaptive-bandwidth widening with
        data-spread effects (a row with 500 events naturally spans more
        zones than a row with 5 even at the same bandwidth, because the 500
        events themselves cover +/-3 sigma of the destination distribution).
        """
        from analytics.ext_v2.kde import silverman_2d

        sigma = 2.0
        bw_sparse = silverman_2d(5, sigma)
        bw_medium = silverman_2d(50, sigma)
        bw_dense = silverman_2d(500, sigma)
        assert bw_sparse > bw_medium > bw_dense, (
            f"silverman_2d should be monotone-decreasing in n: "
            f"bw_sparse={bw_sparse:.4f}, bw_medium={bw_medium:.4f}, bw_dense={bw_dense:.4f}"
        )


class TestZeroEventSourceFallback:
    """Source zones with no train events get row equal to mean of populated rows."""

    def test_zero_event_source_uses_row_mean_fallback(self) -> None:
        from analytics.ext_v2.kde import KDESmoothedTransition

        # Build actions that ONLY populate source zones 0..47 (left half of pitch).
        # Right-half source zones (48..95) get zero events → fallback fires.
        rng = np.random.default_rng(19)
        rows = []
        for zx in range(6):  # left 6 columns of 12-wide grid
            for zy in range(8):
                cell_w = 105.0 / 12
                cell_h = 68.0 / 8
                sx_centre = (zx + 0.5) * cell_w
                sy_centre = (zy + 0.5) * cell_h
                for _ in range(20):
                    rows.append(
                        {
                            "type_name": "pass",
                            "result_name": "success",
                            "start_x": sx_centre + rng.normal(0, 0.3),
                            "start_y": sy_centre + rng.normal(0, 0.3),
                            "end_x": sx_centre + 25.0 + rng.normal(0, 1.0),
                            "end_y": sy_centre + rng.normal(0, 1.0),
                        }
                    )
        actions = pd.DataFrame(rows)
        m = KDESmoothedTransition().fit(actions).matrix

        # Source zones 48-95 should have rows equal to mean of rows 0-47.
        populated_rows = m[:48]
        expected_fallback = populated_rows.mean(axis=0)
        for s in range(48, 96):
            np.testing.assert_allclose(m[s], expected_fallback, atol=1e-12)

    def test_zero_event_fallback_row_still_stochastic(self) -> None:
        """The fallback row must itself sum to 1.0 (mean of stochastic rows is stochastic)."""
        from analytics.ext_v2.kde import KDESmoothedTransition

        rng = np.random.default_rng(23)
        rows = []
        for _ in range(100):
            rows.append(
                {
                    "type_name": "pass",
                    "result_name": "success",
                    "start_x": 4.4 + rng.normal(0, 0.3),
                    "start_y": 4.25 + rng.normal(0, 0.3),
                    "end_x": 30.0 + rng.normal(0, 1.0),
                    "end_y": 30.0 + rng.normal(0, 1.0),
                }
            )
        actions = pd.DataFrame(rows)
        m = KDESmoothedTransition().fit(actions).matrix
        row_sums = m.sum(axis=1)
        np.testing.assert_allclose(row_sums, 1.0, atol=1e-10)

    def test_all_zero_actions_yields_uniform_fallback(self) -> None:
        """Empty actions → all-zero rows → fallback computes mean of empties = uniform.

        Edge case: if every source zone has zero events, the row-mean of all
        rows is also zero. We resolve to a uniform 1/n_zones fallback.
        """
        from analytics.ext_v2.kde import KDESmoothedTransition

        # All actions filtered out (no successful Singh moves).
        actions = pd.DataFrame(
            {
                "type_name": ["shot"] * 5,
                "result_name": ["fail"] * 5,
                "start_x": [50.0] * 5,
                "start_y": [30.0] * 5,
                "end_x": [60.0] * 5,
                "end_y": [30.0] * 5,
            }
        )
        m = KDESmoothedTransition().fit(actions).matrix
        # Every row should be uniform 1/96 (all source zones are zero-event).
        np.testing.assert_allclose(m, np.full((96, 96), 1 / 96), atol=1e-10)

    def test_adaptive_false_uses_constant_bandwidth(self) -> None:
        """adaptive=False applies the same bandwidth to all rows regardless of n_s."""
        from analytics.ext_v2.kde import KDESmoothedTransition

        actions = _make_clustered_actions(n_per_source_zone=50, seed=17)
        kde = KDESmoothedTransition(bandwidth=1.0, adaptive=False).fit(actions)
        # We can't directly inspect per-row bandwidth from the public API, but we
        # can compare adaptive=True vs adaptive=False on the same fit to confirm
        # they produce different matrices (sanity check that adaptive is wired).
        kde_adapt = KDESmoothedTransition(bandwidth=1.0, adaptive=True).fit(actions)
        assert not np.allclose(kde.matrix, kde_adapt.matrix), "adaptive flag had no effect"
