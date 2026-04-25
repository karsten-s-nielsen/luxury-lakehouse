"""Shape test for stg_metrica__team_players + metrica-sample pseudo-comp (PR 5a)."""

from pathlib import Path

TP_MODEL = Path("dbt_project/models/staging/metrica/stg_metrica__team_players.sql")
MATCHES = Path("dbt_project/models/staging/metrica/stg_metrica__matches.sql")


def test_team_players_exists() -> None:
    assert TP_MODEL.exists()


def test_emits_native_team_id_and_native_player_id() -> None:
    src = TP_MODEL.read_text()
    assert "native_team_id" in src
    assert "native_player_id" in src


def test_carries_is_anonymized_from_bronze() -> None:
    src = TP_MODEL.read_text()
    assert "is_anonymized" in src


def test_synth_pattern_uses_match_and_side() -> None:
    src = TP_MODEL.read_text()
    assert "'metrica_'" in src
    assert "match_id" in src
    assert "'home'" in src.lower()
    assert "'away'" in src.lower()


def test_has_synthesis_reason_metrica_anonymized() -> None:
    src = TP_MODEL.read_text()
    assert "'metrica_anonymized'" in src


def test_metrica_matches_has_pseudo_competition() -> None:
    src = MATCHES.read_text()
    assert "'metrica-sample'" in src
    assert "as competition_id" in src
