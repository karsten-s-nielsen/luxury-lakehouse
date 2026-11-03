"""The resolution records on the real bronze schemas (plan Task 22): every provider's fixture bronze gives records
for every entity it holds, with the key that the one generator gives (ADR-018), and a full `resolve_all` runs."""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest
from bronzekit import fixture_lake

from lakehouse.config.model import load_defaults
from lakehouse.ids.keys import competition_key, match_key, player_key, season_key, team_key
from lakehouse.resolve.decisions import DecisionSet
from lakehouse.resolve.links import resolve_all
from lakehouse.resolve.model import ENTITIES, Entity
from lakehouse.resolve.records import load_records

EXPECTED: dict[Entity, set[str]] = {
    "player": {"statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"},
    "team": {"statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"},
    "competition": {"statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"},
    "match": {"statsbomb", "wyscout", "idsse", "skillcorner", "gradientsports"},
}
KEYS = {"player": player_key, "team": team_key, "competition": competition_key}


@pytest.fixture(scope="module")
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = fixture_lake()
    yield connection
    connection.close()


@pytest.mark.parametrize("entity", ENTITIES)
def test_every_provider_gives_records(conn: duckdb.DuckDBPyConnection, entity: Entity) -> None:
    records = load_records(conn, entity)
    assert set(records) == EXPECTED[entity]
    for provider, found in records.items():
        assert found, f"{provider}: no {entity} records"
        assert len({r.ref for r in found}) == len(found)  # one record per native id
        for record in found:
            assert record.ref.provider == provider
            assert record.name.strip(), f"{provider} {entity} {record.ref.native_id}: no name"
            expected = (
                match_key(provider, record.ref.native_id)
                if entity == "match"
                else KEYS[entity](provider, record.ref.native_id)
            )
            assert record.key == expected


def test_player_records_carry_the_attributes(conn: duckdb.DuckDBPyConnection) -> None:
    records = load_records(conn, "player")
    with_birth = {provider for provider, found in records.items() if any(r.birth_date for r in found)}
    assert {"wyscout", "skillcorner", "gradientsports"} <= with_birth  # the StatsBomb open lineups have none
    assert all(r.matches is None or r.matches >= 1 for found in records.values() for r in found)
    sb = records["statsbomb"]
    assert any(r.jersey and r.team and r.position for r in sb)


def test_match_records_carry_date_and_teams(conn: duckdb.DuckDBPyConnection) -> None:
    for provider, found in load_records(conn, "match").items():
        for record in found:
            assert record.match_date is not None, provider
            assert record.home and record.away, provider
            assert record.home_team is not None and record.home_team.provider == provider


def test_match_records_carry_competition_and_season(conn: duckdb.DuckDBPyConnection) -> None:
    """The season key of a match record is the bronze one where bronze has it (one generator, ADR-018)."""
    bronze = {
        "statsbomb": "SELECT CAST(match_id AS VARCHAR), season_key, competition_key FROM lake.main.statsbomb_matches",
        "wyscout": "SELECT CAST(match_id AS VARCHAR), season_key, competition_key FROM lake.main.wyscout_matches",
        "idsse": "SELECT native_match_id, season_key, competition_key FROM lake.main.idsse_match_information",
        "gradientsports": "SELECT native_match_id, season_key, competition_key FROM lake.main.gradientsports_matches",
    }
    for provider, found in load_records(conn, "match").items():
        keys = {str(m): (s, c) for m, s, c in conn.execute(bronze[provider]).fetchall()} if provider in bronze else {}
        for record in found:
            assert record.competition_key is not None and record.season_key is not None, provider
            assert record.native_season_id is not None and ":" in record.native_season_id, provider
            assert record.season_key == season_key(provider, record.native_season_id)
            if record.ref.native_id in keys:
                assert (record.season_key, record.competition_key) == keys[record.ref.native_id], provider


def test_resolve_all_runs_on_fixture_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    decisions = DecisionSet(must_links=[], cannot_links=[], accept_requests=[], undo_requests=[])
    result = resolve_all(
        conn, decisions=decisions, bands=dict.fromkeys(ENTITIES), settings=load_defaults("laptop"), run_id="r1"
    )
    assert sum(result.links_written.values()) == 0  # no measured bands yet: nothing links automatically
    assert set(result.queued) == set(ENTITIES)
