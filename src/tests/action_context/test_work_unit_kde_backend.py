"""WorkUnit.kde_backend default + __post_init__ validation."""

import pytest

from analytics.action_context.work_unit import WorkUnit


def test_kde_backend_defaults_to_fft_cic():
    assert WorkUnit(provider="skillcorner", match_id="1899585").kde_backend == "fft-cic"


def test_kde_backend_explicit_valid():
    u = WorkUnit(provider="metrica", match_id="X", period=1, kde_backend="cpu-numba")
    assert u.kde_backend == "cpu-numba"
    assert u.period == 1


def test_invalid_kde_backend_rejected_before_queue():
    with pytest.raises(ValueError, match="Unknown ghost-GK backend"):
        WorkUnit(provider="metrica", match_id="X", kde_backend="typo")
