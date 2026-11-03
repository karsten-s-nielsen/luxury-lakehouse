"""Team and competition resolution (spec §15.1, §15.2; plan Task 22): the IRIS rapidfuzz team matcher, extended from
StatsBomb ↔ Wyscout to every provider pair, and the same matcher on competition names."""

from __future__ import annotations

from lakehouse.ids.keys import competition_key, team_key
from lakehouse.ids.native import Provider
from lakehouse.resolve.competitions import competition_candidates
from lakehouse.resolve.model import EntityRecord, RecordRef
from lakehouse.resolve.teams import team_candidates


def _team(provider: Provider, native: str, name: str) -> EntityRecord:
    key = team_key(provider, native)
    assert key is not None
    return EntityRecord(ref=RecordRef(provider, native), key=key, name=name)


def _competition(provider: Provider, native: str, name: str, area: str | None = None) -> EntityRecord:
    key = competition_key(provider, native)
    assert key is not None
    return EntityRecord(ref=RecordRef(provider, native), key=key, name=name, area=area)


def test_teams_all_pairs() -> None:
    """More than StatsBomb ↔ Wyscout: every pair of providers is matched."""
    providers: list[Provider] = ["statsbomb", "wyscout", "idsse", "skillcorner"]
    records = {provider: [_team(provider, f"{provider[:2]}1", "Germany")] for provider in providers}
    pairs = {frozenset({c.a.provider, c.b.provider}) for c in team_candidates(records)}  # type: ignore[arg-type]
    assert len(pairs) == 6
    assert all(c.score == 100.0 and c.method == "team_name_rapidfuzz" for c in team_candidates(records))  # type: ignore[arg-type]


def test_team_collision_keeps_one_to_one() -> None:
    """'Barcelona' and 'Barcelona B' both best-match 'FC Barcelona': only the higher-confidence edge stays."""
    found = team_candidates(
        {
            "statsbomb": [_team("statsbomb", "217", "Barcelona"), _team("statsbomb", "218", "Barcelona B")],
            "wyscout": [_team("wyscout", "676", "FC Barcelona")],
        }
    )
    [only] = found
    assert RecordRef("statsbomb", "217") in (only.a, only.b)


def test_team_cutoff_and_runner_up() -> None:
    assert (
        team_candidates(
            {"statsbomb": [_team("statsbomb", "1", "Arsenal")], "wyscout": [_team("wyscout", "2", "Chelsea")]}
        )
        == []
    )
    [found] = team_candidates(
        {
            "statsbomb": [_team("statsbomb", "1", "Bayern Munich")],
            "idsse": [
                _team("idsse", "DFL-CLU-00000G", "Bayern Munich"),
                _team("idsse", "DFL-CLU-00000H", "Bayern Munich II"),
            ],
        }
    )
    assert found.score == 100.0
    assert found.runner_up is not None and 70.0 <= found.runner_up < 100.0


def test_competitions_by_name_and_area() -> None:
    found = competition_candidates(
        {
            "statsbomb": [_competition("statsbomb", "43", "FIFA World Cup", "International")],
            "wyscout": [_competition("wyscout", "28", "World Cup", "International")],
            "idsse": [_competition("idsse", "DFL-COM-000002", "2. Bundesliga", "Germany")],
        }
    )
    [link] = found
    assert {link.a, link.b} == {RecordRef("statsbomb", "43"), RecordRef("wyscout", "28")}
    assert (link.layer, link.method) == (1, "competition_name_rapidfuzz")  # same area: layer 1


def test_competitions_in_other_areas_are_layer_2() -> None:
    [link] = competition_candidates(
        {
            "statsbomb": [_competition("statsbomb", "2", "Premier League", "England")],
            "skillcorner": [_competition("skillcorner", "7", "Premier League", "Australia")],
        }
    )
    assert link.layer == 2
