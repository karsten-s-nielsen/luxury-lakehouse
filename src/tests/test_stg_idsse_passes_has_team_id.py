"""stg_idsse__passes hydrates team_id_native via the per-(match, side) bridge.

PR 5a originally introduced this via stg_idsse__home_away_teams. PR 7 hotfix #3
deleted that view and subsumed it under int_tracking__match_side_team_bridge
(generalised across all 3 tracking providers). Test updated to verify the
consumer reads the new bridge filtered to source_provider='idsse'.
"""

from pathlib import Path

MODEL = Path("dbt_project/models/staging/idsse/stg_idsse__passes.sql")


def test_joins_match_side_team_bridge() -> None:
    src = MODEL.read_text()
    assert "int_tracking__match_side_team_bridge" in src
    # Filter must restrict to IDSSE rows (the bridge is multi-provider).
    assert "source_provider = 'idsse'" in src or "source_provider='idsse'" in src


def test_team_id_native_reads_from_bridge() -> None:
    src = MODEL.read_text()
    assert "bridge_team_id" in src
    assert "as team_id_native" in src


def test_native_match_id_cte_present() -> None:
    src = MODEL.read_text()
    assert "native_match_id" in src
