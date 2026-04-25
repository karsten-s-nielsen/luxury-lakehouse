"""Shape test — dim_players.sql has Kimball 4-provider structure (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/marts/dim_players.sql")


def test_uses_generate_player_key_macro() -> None:
    assert "generate_player_key" in MODEL.read_text()


def test_preserves_canonical_player_id_legacy_hash() -> None:
    src = MODEL.read_text()
    assert "canonical_player_id" in src
    assert "generate_surrogate_key" in src


def test_has_canonical_player_key_bigint() -> None:
    assert "canonical_player_key" in MODEL.read_text()


def test_has_all_four_provider_literals() -> None:
    src = MODEL.read_text()
    for provider in ("statsbomb", "wyscout", "idsse", "metrica"):
        assert f"'{provider}'" in src


def test_has_synthesis_flags() -> None:
    src = MODEL.read_text()
    for col in ("is_synthesized", "is_anonymized", "synthesis_reason"):
        assert col in src


def test_joins_int_player_xref_when_entity_resolution_on() -> None:
    assert "int_player_xref" in MODEL.read_text()


def test_metrica_documented_as_siloed_by_design() -> None:
    src = MODEL.read_text()
    assert "Metrica" in src
    lowered = src.lower()
    assert "anonymised" in lowered or "anonymized" in lowered
    assert "sample" in lowered


def test_legacy_player_id_preserved_for_sb_ws() -> None:
    src = MODEL.read_text()
    assert "player_id_legacy" in src
