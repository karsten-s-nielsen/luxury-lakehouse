"""Player resolution, ported from IRIS (spec §15.1; plan Task 22): three progressive layers, TF-IDF candidates,
rapidfuzz scoring, bidirectional validation, every provider pair, and the runner-up score for the margin."""

from __future__ import annotations

from lakehouse.ids.keys import player_key
from lakehouse.ids.native import Provider
from lakehouse.resolve.model import EntityRecord, RecordRef
from lakehouse.resolve.players import encode_dob, match_dob, normalize_name, player_candidates, score_candidate


def _player(provider: Provider, native: str, name: str, **attributes: str | None) -> EntityRecord:
    key = player_key(provider, native)
    assert key is not None
    return EntityRecord(ref=RecordRef(provider, native), key=key, name=name, **attributes)  # type: ignore[arg-type]


def test_normalize_name() -> None:
    assert normalize_name("Thomas Müller") == "thomas muller"
    assert normalize_name("  Martin Ødegaard ") == "martin odegaard"
    assert normalize_name("N'Golo  Kanté") == "n golo kante"
    assert normalize_name(None) == ""


def test_encode_dob() -> None:
    assert encode_dob("1993-12-28") == "28L93"
    assert encode_dob("1993-13-28") == ""
    assert encode_dob(None) == ""


def test_match_dob() -> None:
    assert match_dob("1990-03-05", "1990-03-05") == 100.0
    assert match_dob("1990-03-05", "1990-03-06") == 90.0
    assert match_dob("1990-03-05", "1990-05-03") == 80.0  # month and day swapped
    assert match_dob("1990-03-05", "1991-03-05") == 0.0
    assert match_dob(None, "1990-03-05") is None


def test_score_candidate_redistributes_missing_weights() -> None:
    assert score_candidate("mats hummels", "mats hummels") == 100.0
    full = score_candidate("mats hummels", "mats hummels", dob_a="1988-12-16", dob_b="1988-12-16")
    assert full == 100.0
    assert score_candidate("a b", "a b", position_a="Goalkeeper", position_b="Forward") < 100.0


def test_layer1_name_dob_jersey_team() -> None:
    shared = {"birth_date": "1989-09-13", "jersey": "25", "team": "FC Bayern München", "position": "Forward"}
    candidates = player_candidates(
        {
            "statsbomb": [_player("statsbomb", "5203", "Thomas Müller", **shared)],
            "idsse": [_player("idsse", "DFL-OBJ-0000O3", "Thomas Müller", **shared)],
        }
    )
    [found] = candidates
    assert {found.a.provider, found.b.provider} == {"statsbomb", "idsse"}
    assert (found.layer, found.method, found.score) == (1, "layer1_tfidf_rapidfuzz_bidirectional", 100.0)


def test_layer2_detects_a_swapped_birth_date() -> None:
    candidates = player_candidates(
        {
            "statsbomb": [_player("statsbomb", "1", "Joshua Kimmich", birth_date="1995-02-08", position="Midfield")],
            "wyscout": [_player("wyscout", "2", "Joshua Kimmich", birth_date="1995-08-02", position="Midfielder")],
        }
    )
    [found] = candidates
    assert found.layer == 2
    assert found.score == 95.0  # name 60 % x 100, birth date 25 % x 80 (swap), position 15 % x 100


def test_layer3_matches_on_name_and_position_when_birth_dates_disagree() -> None:
    candidates = player_candidates(
        {
            "statsbomb": [_player("statsbomb", "1", "Mats Hummels", birth_date="1988-12-16", position="Center Back")],
            "skillcorner": [_player("skillcorner", "9", "Mats Hummels", birth_date="1980-01-01", position="Defender")],
        }
    )
    [found] = candidates
    assert (found.layer, found.score) == (3, 100.0)


def test_bidirectional_best_only_with_runner_up() -> None:
    candidates = player_candidates(
        {
            "statsbomb": [_player("statsbomb", "1", "Leon Goretzka", birth_date="1995-02-06")],
            "wyscout": [
                _player("wyscout", "10", "Leon Goretzka", birth_date="1995-02-06"),
                _player("wyscout", "11", "Leon Goretzke", birth_date="1995-02-06"),
            ],
        }
    )
    [found] = candidates
    assert found.b == RecordRef("wyscout", "10") or found.a == RecordRef("wyscout", "10")
    assert found.runner_up is not None
    assert 0.0 < found.runner_up < found.score  # the near-namesake is the runner-up


def test_bidirectional_rejects_a_one_sided_best() -> None:
    """'Leon Goretzka' prefers 'Leon Goretzke', but 'Leon Goretzke' prefers its exact namesake: only that pair."""
    candidates = player_candidates(
        {
            "statsbomb": [
                _player("statsbomb", "1", "Leon Goretzka", birth_date="1995-02-06"),
                _player("statsbomb", "2", "Leon Goretzke", birth_date="1995-02-06"),
            ],
            "wyscout": [_player("wyscout", "10", "Leon Goretzke", birth_date="1995-02-06")],
        }
    )
    [found] = candidates
    assert {found.a, found.b} == {RecordRef("statsbomb", "2"), RecordRef("wyscout", "10")}


def test_every_provider_pair() -> None:
    records = {
        provider: [_player(provider, f"{provider}-1", "Manuel Neuer", birth_date="1986-03-27")]
        for provider in ("statsbomb", "wyscout", "idsse")
    }
    pairs = {frozenset({c.a.provider, c.b.provider}) for c in player_candidates(records)}  # type: ignore[arg-type]
    assert pairs == {
        frozenset({"statsbomb", "wyscout"}),
        frozenset({"statsbomb", "idsse"}),
        frozenset({"wyscout", "idsse"}),
    }


def test_unrelated_players_give_no_candidate() -> None:
    candidates = player_candidates(
        {
            "statsbomb": [_player("statsbomb", "1", "Harry Kane", birth_date="1993-07-28")],
            "wyscout": [_player("wyscout", "2", "Luka Modric", birth_date="1985-09-09")],
        }
    )
    assert candidates == []
