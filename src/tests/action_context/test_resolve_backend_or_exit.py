"""The CLI boundary translates the domain ValueError into operator fail-loud SystemExit (pure)."""

import pytest

from ingestion.action_context import _resolve_backend_or_exit


def test_invalid_backend_becomes_systemexit():
    with pytest.raises(SystemExit, match="Unknown ghost-GK backend"):
        _resolve_backend_or_exit("nope", None)


def test_valid_backend_returns_resolved():
    assert _resolve_backend_or_exit("cpu-numba", None) == "cpu-numba"
    assert _resolve_backend_or_exit(None, "scipy") == "scipy"
    assert _resolve_backend_or_exit(None, None) == "fft-cic"
