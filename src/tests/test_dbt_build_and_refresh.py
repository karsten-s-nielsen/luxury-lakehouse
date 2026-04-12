"""Tests for scripts/dbt_build_and_refresh.py — fail-fast wrapper semantics."""

from __future__ import annotations

import subprocess

import pytest


def _make_completed(returncode: int) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr="")


def test_dbt_failure_aborts_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """If dbt build fails, refresh_synced_tables must NOT run, exit code propagates."""
    calls: list[list[str]] = []

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        if any("dbt" in part for part in cmd):
            return _make_completed(1)
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()

    assert exit_code == 1, "dbt failure exit code must propagate"
    refresh_calls = [c for c in calls if any("refresh_synced_tables" in part for part in c)]
    assert refresh_calls == [], "refresh_synced_tables must not be invoked after dbt failure"


def test_dbt_success_triggers_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """If dbt build succeeds, refresh_synced_tables --wait must run and exit propagates."""
    calls: list[list[str]] = []

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()

    assert exit_code == 0
    refresh_calls = [c for c in calls if any("refresh_synced_tables" in part for part in c)]
    assert len(refresh_calls) == 1, "refresh_synced_tables must run exactly once"
    assert any("--wait" in part for part in refresh_calls[0]), "refresh must use --wait"


def test_refresh_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """If dbt succeeds but refresh fails, the wrapper exit code must reflect refresh failure."""

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if any("refresh_synced_tables" in part for part in cmd):
            return _make_completed(2)
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()
    assert exit_code == 2


def test_extra_args_forwarded_to_dbt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Args after the script name must be forwarded to dbt build."""
    calls: list[list[str]] = []

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py", "--select", "tag:cost", "--target", "dev"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()
    assert exit_code == 0

    dbt_calls = [c for c in calls if any("dbt" in part for part in c)]
    assert len(dbt_calls) == 1
    assert "--select" in dbt_calls[0]
    assert "tag:cost" in dbt_calls[0]
    assert "--target" in dbt_calls[0]
    assert "dev" in dbt_calls[0]
