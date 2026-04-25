"""Contract tests for the new ingest_teams path in wyscout.py (PR 5a)."""

from __future__ import annotations

import inspect

from ingestion import wyscout


def test_teams_url_constant_exists() -> None:
    assert hasattr(wyscout, "_TEAMS_URL"), "_TEAMS_URL constant must exist"
    assert wyscout._TEAMS_URL.startswith("https://ndownloader.figshare.com/files/"), (
        f"URL format mismatch: {wyscout._TEAMS_URL}"
    )


def test_ingest_teams_function_exists() -> None:
    assert hasattr(wyscout, "ingest_teams"), "ingest_teams function must exist"
    sig = inspect.signature(wyscout.ingest_teams)
    params = set(sig.parameters.keys())
    assert {"spark", "catalog", "schema"}.issubset(params), f"ingest_teams signature mismatch: {params}"


def test_guard_check_includes_teams_table() -> None:
    """Guard must check teams table presence alongside events/matches/players."""
    src = inspect.getsource(wyscout._WyscoutGuard)
    assert "wyscout_teams" in src, "guard must consider wyscout_teams in skip check"


def test_teams_expected_cols_snapshot_loaded() -> None:
    cols = wyscout._WYSCOUT_TEAMS_EXPECTED_COLS
    assert "wyId" in cols
    assert "officialName" in cols
    assert "area" in cols


def test_run_pipeline_calls_ingest_teams() -> None:
    src = inspect.getsource(wyscout.run_pipeline)
    assert "ingest_teams(" in src, "run_pipeline must dispatch ingest_teams"
