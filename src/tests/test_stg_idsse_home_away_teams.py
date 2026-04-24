"""Shape test for stg_idsse__home_away_teams bridge (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql")


def test_model_file_exists() -> None:
    assert MODEL.exists()


def test_model_selects_match_id_side_team_id() -> None:
    src = MODEL.read_text()
    assert "match_id" in src
    assert "as side" in src or " side" in src
    assert "team_id" in src


def test_model_references_stg_idsse_tracking() -> None:
    src = MODEL.read_text()
    assert "stg_idsse__tracking" in src


def test_model_filters_home_away_only() -> None:
    src = MODEL.read_text()
    assert "'home'" in src or "'away'" in src
    assert "in ('home', 'away')" in src or "in ('home','away')" in src


def test_model_strips_idsse_prefix_from_match_id() -> None:
    src = MODEL.read_text()
    assert "regexp_replace" in src
    assert "idsse_" in src
