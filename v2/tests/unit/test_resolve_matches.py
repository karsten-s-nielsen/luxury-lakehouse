"""Match resolution (spec §15.2; plan Task 22): the same real match across providers, found by its date (one day of
tolerance for time zones) and its teams — by the team clusters of the run when both teams are linked (layer 1), by
the team names otherwise (layer 2), and with home and away swapped (layer 3)."""

from __future__ import annotations

from datetime import date

from lakehouse.ids.keys import match_key
from lakehouse.ids.native import Provider
from lakehouse.resolve.matches import match_candidates
from lakehouse.resolve.model import EntityRecord, RecordRef


def _match(provider: Provider, native: str, day: date, home: str, away: str) -> EntityRecord:
    return EntityRecord(
        ref=RecordRef(provider, native),
        key=match_key(provider, native),
        name=f"{home} - {away}",
        match_date=day,
        home=home,
        away=away,
        home_team=RecordRef(provider, f"{native}-{home}"),
        away_team=RecordRef(provider, f"{native}-{away}"),
    )


SB = _match("statsbomb", "7525", date(2018, 6, 17), "Germany", "Mexico")


def test_matches_by_date_and_teams() -> None:
    [found] = match_candidates(
        {"statsbomb": [SB], "wyscout": [_match("wyscout", "2057976", date(2018, 6, 17), "Germany", "Mexico")]}, {}
    )
    assert (found.layer, found.method, found.score) == (2, "date_team_names", 100.0)


def test_linked_teams_give_layer_1() -> None:
    ws = _match("wyscout", "2057976", date(2018, 6, 17), "Deutschland", "Mexiko")  # names that do not match
    clusters = {
        SB.home_team: SB.home_team,
        SB.away_team: SB.away_team,
        ws.home_team: SB.home_team,
        ws.away_team: SB.away_team,
    }
    [found] = match_candidates({"statsbomb": [SB], "wyscout": [ws]}, clusters)  # type: ignore[arg-type]
    assert (found.layer, found.method, found.score) == (1, "date_team_links", 100.0)


def test_one_day_apart_scores_lower() -> None:
    [found] = match_candidates(
        {"statsbomb": [SB], "wyscout": [_match("wyscout", "1", date(2018, 6, 18), "Germany", "Mexico")]}, {}
    )
    assert found.score == 96.0  # date 40 % x 90, teams 60 % x 100


def test_two_days_apart_or_other_teams_give_nothing() -> None:
    far = _match("wyscout", "1", date(2018, 6, 19), "Germany", "Mexico")
    other = _match("wyscout", "2", date(2018, 6, 17), "Brazil", "Switzerland")
    assert match_candidates({"statsbomb": [SB], "wyscout": [far, other]}, {}) == []


def test_swapped_home_and_away_is_layer_3() -> None:
    [found] = match_candidates(
        {"statsbomb": [SB], "gradientsports": [_match("gradientsports", "3", date(2018, 6, 17), "Mexico", "Germany")]},
        {},
    )
    assert (found.layer, found.method) == (3, "date_team_names_swapped")


def test_one_partner_per_provider_with_runner_up() -> None:
    same = _match("wyscout", "1", date(2018, 6, 17), "Germany", "Mexico")
    near = _match("wyscout", "2", date(2018, 6, 18), "Germany", "Mexico")
    [found] = match_candidates({"statsbomb": [SB], "wyscout": [same, near]}, {})
    assert RecordRef("wyscout", "1") in (found.a, found.b)
    assert found.runner_up == 96.0
