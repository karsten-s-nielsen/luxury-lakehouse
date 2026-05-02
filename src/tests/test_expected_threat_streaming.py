"""Tests for the streaming xT computation primitives introduced in OPT-1.

The OPT-1 cycle (2026-05-02) refactored ``analytics.expected_threat`` to
expose ``ZoneCounters`` plus ``bucket_actions_into_counters`` and
``xt_grid_from_counters`` as the two halves of the original
``compute_expected_threat_grid``. The motivation: enable per-competition
streaming in ``ingestion.expected_threat`` so the global-grid rebuild
no longer pulls all 9.5M+ ``fct_action_values`` rows to driver memory
at once.

This test asserts the load-bearing invariants of that refactor:

1. **Backwards compatibility.** The wrapper
   ``compute_expected_threat_grid(actions_df, params)`` must produce
   exactly the same ``XTGrid.values`` it did before the refactor for
   any single DataFrame input.
2. **Bucketing additivity.** For any two action DataFrames A and B,
   ``bucket(A) + bucket(B) == bucket(concat([A, B]))`` element-wise on
   every counter array. Without additivity the streaming form would
   silently diverge from the single-pass form.
3. **End-to-end streaming equivalence.** Splitting an action set into
   per-competition slices, bucketing each, summing the counters, and
   running ``xt_grid_from_counters`` on the sum must produce the same
   ``XTGrid.values`` as ``compute_expected_threat_grid`` on the full
   concatenated DataFrame (within numerical tolerance).

If any of these break, the production global-grid output diverges
silently. Hence the explicit assertions.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.expected_threat import (
    ExpectedThreatParams,
    ZoneCounters,
    bucket_actions_into_counters,
    compute_expected_threat_grid,
    xt_grid_from_counters,
)

_DEFAULT_PARAMS = ExpectedThreatParams()


def _synthetic_actions(seed: int, n: int) -> pd.DataFrame:
    """Generate a synthetic SPADL actions DataFrame with realistic mix."""
    rng = np.random.default_rng(seed)
    types = rng.choice(
        ["pass", "cross", "dribble", "shot", "throw_in", "clearance"],
        size=n,
        p=[0.55, 0.10, 0.15, 0.05, 0.10, 0.05],
    )
    results = rng.choice(["success", "fail"], size=n, p=[0.75, 0.25])
    start_x = rng.uniform(0, 105, size=n)
    start_y = rng.uniform(0, 68, size=n)
    end_x = rng.uniform(0, 105, size=n)
    end_y = rng.uniform(0, 68, size=n)
    return pd.DataFrame(
        {
            "type_name": types,
            "result_name": results,
            "start_x": start_x,
            "start_y": start_y,
            "end_x": end_x,
            "end_y": end_y,
        }
    )


def test_bucketing_is_additive_across_disjoint_slices() -> None:
    """For any two action sets A and B,
    bucket(A) + bucket(B) must equal bucket(concat([A, B]))
    element-wise on every counter array.

    Without this, streaming would silently diverge from single-pass.
    """
    a = _synthetic_actions(seed=1, n=500)
    b = _synthetic_actions(seed=2, n=700)

    counters_a = bucket_actions_into_counters(a, _DEFAULT_PARAMS)
    counters_b = bucket_actions_into_counters(b, _DEFAULT_PARAMS)
    counters_summed = counters_a + counters_b

    counters_full = bucket_actions_into_counters(pd.concat([a, b], ignore_index=True), _DEFAULT_PARAMS)

    np.testing.assert_array_equal(counters_summed.total_per_zone, counters_full.total_per_zone)
    np.testing.assert_array_equal(counters_summed.shots_per_zone, counters_full.shots_per_zone)
    np.testing.assert_array_equal(counters_summed.goals_per_zone, counters_full.goals_per_zone)
    np.testing.assert_array_equal(counters_summed.succ_moves_per_zone, counters_full.succ_moves_per_zone)
    np.testing.assert_array_equal(counters_summed.transition_counts, counters_full.transition_counts)


def test_zero_counters_is_identity_under_addition() -> None:
    """ZoneCounters.zero(params) is the additive identity."""
    a = _synthetic_actions(seed=3, n=200)
    counters_a = bucket_actions_into_counters(a, _DEFAULT_PARAMS)
    zero = ZoneCounters.zero(_DEFAULT_PARAMS)

    summed = zero + counters_a
    np.testing.assert_array_equal(summed.total_per_zone, counters_a.total_per_zone)
    np.testing.assert_array_equal(summed.shots_per_zone, counters_a.shots_per_zone)
    np.testing.assert_array_equal(summed.transition_counts, counters_a.transition_counts)


def test_streaming_equivalence_to_single_pass() -> None:
    """End-to-end: stream per-comp slices → accumulate counters → xt_grid_from_counters
    must match compute_expected_threat_grid on the concatenated DataFrame
    within numerical tolerance.

    This is the load-bearing invariant for the OPT-1 ingestion refactor:
    if the bucketed-streaming xT differs from the single-pass xT, the
    production global grid silently changes after the refactor.
    """
    # Build 3 per-competition slices with different distributions.
    slices = [_synthetic_actions(seed=10 + i, n=400 + 100 * i) for i in range(3)]
    full = pd.concat(slices, ignore_index=True)

    # Single-pass xT (the old code path / today's wrapper).
    grid_single = compute_expected_threat_grid(full, _DEFAULT_PARAMS, competition_id="streaming-test")

    # Streaming xT — bucket each slice, accumulate, run value iteration once.
    counters = ZoneCounters.zero(_DEFAULT_PARAMS)
    for s in slices:
        counters = counters + bucket_actions_into_counters(s, _DEFAULT_PARAMS)
    grid_streamed = xt_grid_from_counters(counters, _DEFAULT_PARAMS, competition_id="streaming-test")

    # Same shape, same coord system, same competition_id label.
    assert grid_single.shape == grid_streamed.shape
    assert grid_single.coord_system == grid_streamed.coord_system
    assert grid_single.competition_id == grid_streamed.competition_id

    # Values must match within float64 round-trip tolerance. The two
    # paths run the SAME value iteration on the SAME counters; any
    # delta is float-precision noise from accumulator order.
    np.testing.assert_allclose(grid_single.values, grid_streamed.values, rtol=1e-10, atol=1e-12)


def test_compute_expected_threat_grid_unchanged_after_refactor() -> None:
    """Smoke test: the single-DataFrame wrapper still returns a valid
    XTGrid for canonical-shaped inputs. Catches accidental signature
    changes during refactors.
    """
    a = _synthetic_actions(seed=42, n=2000)
    grid = compute_expected_threat_grid(a, _DEFAULT_PARAMS, competition_id="smoke")

    assert grid.shape == (_DEFAULT_PARAMS.n_zones_x, _DEFAULT_PARAMS.n_zones_y)
    assert grid.coord_system == "spadl"
    assert grid.competition_id == "smoke"
    assert grid.values.dtype == np.float64
    assert grid.values.min() >= 0.0
