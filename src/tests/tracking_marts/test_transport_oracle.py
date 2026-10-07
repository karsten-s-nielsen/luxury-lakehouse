"""Task 1 — the pinned-sk-4.123 transport-neutrality baseline reference.

Establishes that the six pure scorers run on every tracking provider's fixture unit and produce a
deterministic baseline. Task 3b's executor-path tests compare the refactored build-spill-score pipeline
to ``pure_mart_outputs`` (the baseline) to prove transport neutrality. See ``_oracle`` for the seam.
"""

from __future__ import annotations

import pytest

from tests.tracking_marts import _oracle

_PROVIDERS = sorted(_oracle.UNITS)


@pytest.mark.parametrize("provider", _PROVIDERS)
def test_baseline_runs_and_has_all_marts(provider: str) -> None:
    out = _oracle.pure_mart_outputs(provider)
    missing = set(_oracle.MART_KEYS) - set(out)
    assert not missing, f"{provider}: baseline missing marts {missing}"
    for mart, df in out.items():
        assert df is not None, f"{provider}:{mart} is None"


@pytest.mark.parametrize("provider", _PROVIDERS)
def test_baseline_is_deterministic(provider: str) -> None:
    """Two independent builds of the same fixture unit are byte-identical (validates the oracle compare)."""
    a = _oracle.pure_mart_outputs(provider)
    b = _oracle.pure_mart_outputs(provider)
    for mart in _oracle.MART_KEYS:
        _oracle.assert_mart_equal(a[mart], b[mart], f"{provider}:{mart}")


def test_skillcorner_off_ball_nonempty() -> None:
    """Primary fixture must actually detect runs (guards a silent mis-orientation in the seam)."""
    out = _oracle.pure_mart_outputs("skillcorner", include_gkdv=False)
    assert not out["off_ball_runs"].empty, "no off-ball runs on the SkillCorner fixture — mis-oriented build?"
