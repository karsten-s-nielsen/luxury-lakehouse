"""stg_idsse__passes hydrates team_id_native via home_away bridge (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/staging/idsse/stg_idsse__passes.sql")


def test_joins_home_away_bridge() -> None:
    src = MODEL.read_text()
    assert "stg_idsse__home_away_teams" in src


def test_team_id_native_reads_from_bridge() -> None:
    src = MODEL.read_text()
    assert "bridge_team_id" in src
    assert "as team_id_native" in src


def test_native_match_id_cte_present() -> None:
    src = MODEL.read_text()
    assert "native_match_id" in src
