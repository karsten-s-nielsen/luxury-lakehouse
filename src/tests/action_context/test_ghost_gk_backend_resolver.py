"""Truth-table for the ghost-GK backend resolver (precedence + fail-loud)."""

import pytest

from analytics.action_context.ghost_gk_backend import (
    DEFAULT_GHOST_GK_BACKEND,
    GHOST_GK_KDE_BACKENDS,
    resolve_ghost_gk_backend,
)


def test_default_when_all_unset():
    assert resolve_ghost_gk_backend(None, None) == "fft-cic"
    assert resolve_ghost_gk_backend("", "") == "fft-cic"
    assert DEFAULT_GHOST_GK_BACKEND == "fft-cic"


def test_explicit_wins_over_installation_default():
    assert resolve_ghost_gk_backend("cpu-numba", "vectorized") == "cpu-numba"


def test_installation_default_when_no_explicit():
    assert resolve_ghost_gk_backend(None, "scipy") == "scipy"
    assert resolve_ghost_gk_backend("  ", "scipy") == "scipy"  # whitespace-only == unset


def test_all_five_backends_accepted():
    assert GHOST_GK_KDE_BACKENDS == {"scipy", "vectorized", "cpu-numba", "fft", "fft-cic"}
    for b in GHOST_GK_KDE_BACKENDS:
        assert resolve_ghost_gk_backend(b, None) == b


def test_unknown_backend_raises_valueerror_not_systemexit():
    # ValueError, NOT SystemExit — the domain layer must not raise a process-control exception.
    with pytest.raises(ValueError, match="Unknown ghost-GK backend"):
        resolve_ghost_gk_backend("gpu-magic", None)
    with pytest.raises(ValueError, match="Unknown ghost-GK backend"):
        resolve_ghost_gk_backend(None, "bogus")
