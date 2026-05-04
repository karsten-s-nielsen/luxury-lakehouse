"""ExT v2 Phase 1 (KDE-smoothed Singh) post-retrain smoke gate. Spec §3."""

from __future__ import annotations

import pytest

_PHASE_1_BASELINE_NLL = 3.7482
_TOLERANCE_PCT = 0.01
_THRESHOLD = _PHASE_1_BASELINE_NLL * (1 + _TOLERANCE_PCT)


def test_phase_1_nll_within_threshold() -> None:
    try:
        from analytics.ext_v2.phase_1 import compute_phase_1_nll
    except ImportError as exc:
        pytest.skip(f"analytics.ext_v2.phase_1.compute_phase_1_nll not yet available: {exc}")
    nll = compute_phase_1_nll()
    assert nll <= _THRESHOLD, f"Phase 1 NLL = {nll:.6f} > threshold {_THRESHOLD:.6f}. Halt."
