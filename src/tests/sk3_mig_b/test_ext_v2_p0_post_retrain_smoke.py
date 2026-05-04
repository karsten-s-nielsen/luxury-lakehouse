"""ExT v2 Phase 0 (Singh baseline) post-retrain smoke gate. Spec §3.

Re-runs the phase-0 NLL computation against the post-retrain
fct_action_values. Threshold pre-registered at 3.7892 + 1%
(PR #206 production baseline + tolerance).
"""

from __future__ import annotations

import pytest

_PHASE_0_BASELINE_NLL = 3.7892
_TOLERANCE_PCT = 0.01
_THRESHOLD = _PHASE_0_BASELINE_NLL * (1 + _TOLERANCE_PCT)


def test_phase_0_nll_within_threshold() -> None:
    """Re-run phase-0 NLL computation against current fct_action_values."""
    try:
        from analytics.ext_v2.phase_0 import compute_phase_0_nll
    except ImportError as exc:
        pytest.skip(
            f"analytics.ext_v2.phase_0.compute_phase_0_nll not yet available: {exc}. "
            "If missing at Phase 9 runtime: extract a thin wrapper around the "
            "existing P0 invocation logic in src/analytics/ext_v2/."
        )
    nll = compute_phase_0_nll()
    assert nll <= _THRESHOLD, (
        f"Phase 0 NLL = {nll:.6f} > threshold {_THRESHOLD:.6f} "
        f"(baseline {_PHASE_0_BASELINE_NLL} + {_TOLERANCE_PCT * 100:.0f}%). "
        "Halt + investigate before Phase 1 dispatch."
    )
