"""Tests for the executor env-drift guard (ADR-044).

The guard (``ingestion.exec_visibility.assert_executor_silly_kicks_sane``) fails loud
inside the serverless ``applyInPandas`` UDF when its silly-kicks sandbox is stale OR
split across two installs — the 2026-06-09 GS dual-GK ghost-GK crash, where the executor
ran silly-kicks 4.12.0 submodules while ``__version__`` reported the healthy 4.20.1.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ingestion import exec_visibility as ev

_PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"
_FLOOR_RE = re.compile(r"silly-kicks\[[^\]]*\]>=(\d+)\.(\d+)\.(\d+)")


@pytest.fixture(autouse=True)
def _reset_guard():
    """Each test starts with the process-local short-circuit cleared."""
    ev.reset_silly_kicks_guard()
    yield
    ev.reset_silly_kicks_guard()


def test_clean_env_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A healthy sandbox (version at/above floor, all submodules from one root) is silent."""
    import silly_kicks

    monkeypatch.setattr(silly_kicks, "__version__", ".".join(map(str, ev._REQUIRED_SK_MIN)), raising=False)
    # Must not raise.
    ev.assert_executor_silly_kicks_sane(batch_key="clean")


def test_stale_version_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A uniformly-stale sandbox (version below floor) fails loud, naming the versions."""
    import silly_kicks

    monkeypatch.setattr(silly_kicks, "__version__", "4.11.0", raising=False)
    floor = ".".join(map(str, ev._REQUIRED_SK_MIN))
    with pytest.raises(RuntimeError, match=rf"silly-kicks 4\.11\.0 < required {re.escape(floor)}.*stale"):
        ev.assert_executor_silly_kicks_sane(batch_key="m10504_p1_b277")


def test_split_install_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """The __version__-lying split: __init__ healthy but a submodule from a foreign root."""
    import silly_kicks
    import silly_kicks.tracking._ghost_gk as ghost

    monkeypatch.setattr(silly_kicks, "__version__", ".".join(map(str, ev._REQUIRED_SK_MIN)), raising=False)
    # Simulate the stale layer: _ghost_gk resolves from a DIFFERENT install root.
    monkeypatch.setattr(ghost, "__file__", "/stale/pythonEnv-5f055717/silly_kicks/tracking/_ghost_gk.py")
    with pytest.raises(RuntimeError, match=r"split install on executor.*_ghost_gk"):
        ev.assert_executor_silly_kicks_sane(batch_key="m10504_p1_b277")


def test_idempotent_short_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    """After a clean pass, a subsequently-poisoned version is NOT re-checked (process-stable)."""
    import silly_kicks

    monkeypatch.setattr(silly_kicks, "__version__", ".".join(map(str, ev._REQUIRED_SK_MIN)), raising=False)
    ev.assert_executor_silly_kicks_sane(batch_key="first")  # marks checked
    monkeypatch.setattr(silly_kicks, "__version__", "4.11.0", raising=False)
    ev.assert_executor_silly_kicks_sane(batch_key="second")  # no-op — must NOT raise


def test_required_sk_min_matches_pyproject_floor() -> None:
    """_REQUIRED_SK_MIN must equal the silly-kicks floor in pyproject.toml (lockstep)."""
    text = _PYPROJECT.read_text(encoding="utf-8")
    m = _FLOOR_RE.search(text)
    assert m is not None, "could not find silly-kicks floor pin in pyproject.toml"
    floor = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    assert ev._REQUIRED_SK_MIN == floor, (
        f"executor guard floor {ev._REQUIRED_SK_MIN} drifted from pyproject silly-kicks floor {floor}; "
        f"bump _REQUIRED_SK_MIN in exec_visibility.py to match."
    )
