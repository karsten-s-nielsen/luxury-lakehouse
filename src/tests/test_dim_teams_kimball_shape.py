"""Shape test — dim_teams.sql has Kimball 4-provider structure (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/marts/dim_teams.sql")


def test_uses_generate_team_key_macro() -> None:
    assert "generate_team_key" in MODEL.read_text()


def test_has_provider_column() -> None:
    src = MODEL.read_text()
    assert "as provider" in src


def test_has_native_team_id() -> None:
    assert "native_team_id" in MODEL.read_text()


def test_has_is_synthesized_and_synthesis_reason() -> None:
    src = MODEL.read_text()
    assert "is_synthesized" in src
    assert "synthesis_reason" in src
    assert "is_anonymized" in src


def test_has_all_four_provider_literals() -> None:
    src = MODEL.read_text()
    for provider in ("statsbomb", "wyscout", "idsse", "metrica"):
        assert f"'{provider}'" in src, f"Provider literal missing: {provider}"


def test_preserves_legacy_team_id() -> None:
    src = MODEL.read_text()
    assert "team_id_legacy" in src


def test_has_canonical_team_key() -> None:
    assert "canonical_team_key" in MODEL.read_text()


def test_joins_int_team_xref_when_entity_resolution_on() -> None:
    assert "int_team_xref" in MODEL.read_text()


def test_joins_stg_wyscout_teams_for_team_name() -> None:
    assert "stg_wyscout__teams" in MODEL.read_text()
