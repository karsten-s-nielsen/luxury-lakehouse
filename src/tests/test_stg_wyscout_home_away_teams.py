"""Shape test for stg_wyscout__home_away_teams bridge (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/staging/wyscout/stg_wyscout__home_away_teams.sql")


def test_model_file_exists() -> None:
    assert MODEL.exists()


def test_model_has_explode_primary_path() -> None:
    src = MODEL.read_text().lower()
    assert "lateral view" in src or "explode(" in src


def test_model_has_synth_fallback_branch() -> None:
    src = MODEL.read_text()
    assert "wyscout_unresolved" in src
    assert "is_synthesized" in src
    assert "synthesis_reason" in src


def test_model_emits_native_team_id() -> None:
    src = MODEL.read_text()
    assert "native_team_id" in src


def test_synth_native_team_id_pattern() -> None:
    src = MODEL.read_text()
    assert "concat('wyscout_unresolved_'" in src
