"""Tests for the ExT v2 numpy value-iteration primitive.

The v2 implementation is an independent rewrite of v1's
``analytics.expected_threat._value_iteration_numpy``; the Phase 0 stop
condition (per design spec §6) requires bit-for-bit-equivalent output to
the v1 producer. Both the tolerance-match test and the analytic
convergence tests below enforce that contract.
"""

from __future__ import annotations

import numpy as np
import pytest

from analytics.expected_threat import _value_iteration_numpy as _v1_iterate
from analytics.ext_v2.value_iteration import iterate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _random_singh_inputs(
    n_zones: int,
    *,
    seed: int = 0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Build a random but plausible Singh-shaped (shot, goal, move, transition) tuple.

    All probability vectors are in [0, 1]; transition is row-stochastic.
    """
    rng = np.random.default_rng(seed)
    shot = rng.random(n_zones).astype(np.float64) * 0.3
    goal = rng.random(n_zones).astype(np.float64) * 0.3
    move = (1.0 - shot).astype(np.float64)
    raw = rng.random((n_zones, n_zones)).astype(np.float64)
    transition = raw / raw.sum(axis=1, keepdims=True)
    return shot, goal, move, transition


# ---------------------------------------------------------------------------
# Numerical-tolerance match against v1 — Phase 0 stop condition
# ---------------------------------------------------------------------------


class TestV1NumericalMatch:
    """v2 must reproduce v1's output bit-for-bit on identical inputs."""

    @pytest.mark.parametrize("n_zones", [4, 16, 96, 384])
    @pytest.mark.parametrize("seed", [0, 1, 42])
    def test_match_random_inputs(self, n_zones: int, seed: int) -> None:
        shot, goal, move, transition = _random_singh_inputs(n_zones, seed=seed)
        xt_v1, iters_v1 = _v1_iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        xt_v2, iters_v2 = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        np.testing.assert_array_equal(xt_v1, xt_v2)
        assert iters_v1 == iters_v2

    def test_match_zero_inputs(self) -> None:
        n = 8
        shot = np.zeros(n)
        goal = np.zeros(n)
        move = np.ones(n)
        transition = np.full((n, n), 1.0 / n)
        xt_v1, _ = _v1_iterate(shot, goal, move, transition, 100, 1e-5)
        xt_v2, _ = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        np.testing.assert_array_equal(xt_v1, xt_v2)


# ---------------------------------------------------------------------------
# Analytic convergence cases
# ---------------------------------------------------------------------------


class TestAnalyticConvergence:
    """Hand-computed cases with known fixed points."""

    def test_absorbing_goal_zone(self) -> None:
        """2-zone setup where every move from zone 0 lands in zone 1, which always scores."""
        # zone 0 = midfield, zone 1 = goal
        shot = np.array([0.0, 1.0])
        goal = np.array([0.0, 1.0])
        move = np.array([1.0, 0.0])
        transition = np.array([[0.0, 1.0], [0.0, 1.0]])
        xt, iters = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        # xt[1] = 1*1 = 1; xt[0] = 1 * (0*xt[0] + 1*xt[1]) = 1
        np.testing.assert_allclose(xt, [1.0, 1.0], atol=1e-10)
        assert iters < 10

    def test_zero_shots_zero_xt(self) -> None:
        """No shooting anywhere → no terminal value → all xt = 0."""
        n = 4
        shot = np.zeros(n)
        goal = np.zeros(n)
        move = np.ones(n)
        transition = np.full((n, n), 1.0 / n)
        xt, _ = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        np.testing.assert_allclose(xt, 0.0, atol=1e-12)

    def test_immediate_shot_only(self) -> None:
        """Pure shooting (no moves) → xt = shot_prob * goal_prob."""
        n = 4
        shot = np.array([0.1, 0.2, 0.3, 0.4])
        goal = np.array([0.5, 0.5, 0.5, 0.5])
        move = np.zeros(n)
        transition = np.full((n, n), 1.0 / n)
        xt, iters = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        expected = shot * goal
        np.testing.assert_allclose(xt, expected, atol=1e-10)
        assert iters == 2  # converges on iteration 2 (delta vs initial zero hits tol)


# ---------------------------------------------------------------------------
# Return shape + types
# ---------------------------------------------------------------------------


class TestReturnShape:
    def test_returns_tuple_of_array_and_int(self) -> None:
        shot, goal, move, transition = _random_singh_inputs(4)
        out = iterate(shot, goal, move, transition, max_iterations=10, tolerance=1e-5)
        assert isinstance(out, tuple)
        assert len(out) == 2
        xt, iters = out
        assert isinstance(xt, np.ndarray)
        assert isinstance(iters, int)
        assert xt.shape == (4,)


# ---------------------------------------------------------------------------
# Bailout at max_iterations
# ---------------------------------------------------------------------------


class TestMaxIterationBailout:
    def test_bailout_returns_max_iterations(self) -> None:
        """If tolerance is unreachable in budget, returns max_iterations."""
        shot, goal, move, transition = _random_singh_inputs(64, seed=7)
        _, iters = iterate(
            shot,
            goal,
            move,
            transition,
            max_iterations=2,
            tolerance=1e-15,  # unreachable in 2 iterations
        )
        assert iters == 2


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_input_same_output(self) -> None:
        shot, goal, move, transition = _random_singh_inputs(32, seed=3)
        xt1, _ = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        xt2, _ = iterate(shot, goal, move, transition, max_iterations=100, tolerance=1e-5)
        np.testing.assert_array_equal(xt1, xt2)


# ---------------------------------------------------------------------------
# Default parameters match v1
# ---------------------------------------------------------------------------


class TestDefaultParameters:
    def test_default_max_iterations_matches_v1(self) -> None:
        from analytics.expected_threat import ExpectedThreatParams

        # v1's default ExpectedThreatParams.max_iterations is the contract
        # the v2 iterate() default should mirror.
        v1_default = ExpectedThreatParams().max_iterations
        # Use inspect to read the default from iterate's signature
        import inspect

        v2_default = inspect.signature(iterate).parameters["max_iterations"].default
        assert v2_default == v1_default

    def test_default_tolerance_matches_v1(self) -> None:
        from analytics.expected_threat import ExpectedThreatParams

        v1_default = ExpectedThreatParams().tolerance
        import inspect

        v2_default = inspect.signature(iterate).parameters["tolerance"].default
        assert v2_default == v1_default
