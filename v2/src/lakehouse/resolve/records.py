"""The resolution records, read from bronze (spec §15.1, §14.3, §14.4; plan Task 22).

One query per provider and entity type reads the raw columns of the bronze tables that exist in the lake; a missing
table means that provider has no records of that type. The rows are grouped in Python by native id, and each
attribute takes its most frequent value (ties: the smallest), so a record never depends on row order. Native ids
and keys come from the one generator of each format (`lakehouse.ids`, ADR-018).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Final

import duckdb

from lakehouse.ids import native
from lakehouse.ids.keys import competition_key, match_key, player_key, season_key, team_key
from lakehouse.ids.native import Provider
from lakehouse.resolve.model import Entity, EntityRecord, RecordRef

__all__ = ["load_records"]

_PLAYER_IDS: Final[dict[Provider, Callable[[Any], str]]] = {
    "statsbomb": native.statsbomb_native_player_id,
    "wyscout": native.wyscout_native_player_id,
    "idsse": native.idsse_native_player_id,
    "skillcorner": native.skillcorner_native_player_id,
    "gradientsports": native.gradientsports_native_player_id,
}
_TEAM_IDS: Final[dict[Provider, Callable[[Any], str]]] = {
    "statsbomb": native.statsbomb_native_team_id,
    "wyscout": native.wyscout_native_team_id,
    "idsse": native.idsse_native_team_id,
    "skillcorner": native.skillcorner_native_team_id,
    "gradientsports": native.gradientsports_native_team_id,
}
_COMPETITION_IDS: Final[dict[Provider, Callable[[Any], str]]] = {
    "statsbomb": native.statsbomb_native_competition_id,
    "wyscout": native.wyscout_native_competition_id,
    "idsse": native.idsse_native_competition_id,
    "skillcorner": native.skillcorner_native_competition_id,
    "gradientsports": native.gradientsports_native_competition_id,
}
_MATCH_IDS: Final[dict[Provider, Callable[[Any], str]]] = {
    "statsbomb": native.statsbomb_native_match_id,
    "wyscout": native.wyscout_native_match_id,
    "idsse": native.idsse_native_match_id,
    "skillcorner": native.skillcorner_native_match_id,
    "gradientsports": native.gradientsports_native_match_id,
}


@dataclass(frozen=True)
class _Query:
    """One bronze query: the tables it needs, its SQL, and the optional tables whose SQL variant it prefers."""

    needs: tuple[str, ...]
    sql: str
    richer: tuple[tuple[tuple[str, ...], str], ...] = field(default_factory=tuple)

    def text(self, tables: set[str]) -> str | None:
        if not set(self.needs) <= tables:
            return None
        for extra, sql in self.richer:
            if set(extra) <= tables:
                return sql
        return self.sql


# Each player query gives: raw id, name, short name, birth date (text), jersey, position, team, match key.
_PLAYERS: Final[dict[Provider, Sequence[_Query]]] = {
    "statsbomb": (
        _Query(
            ("statsbomb_lineups",),
            "SELECT player_id, player_name, player_nickname, CAST(birth_date AS VARCHAR), "
            "CAST(jersey_number AS VARCHAR), positions[1].position, team_name, match_key "
            "FROM lake.main.statsbomb_lineups",
        ),
    ),
    "wyscout": (
        _Query(
            ("wyscout_players",),
            "SELECT p.player_id, concat_ws(' ', p.first_name, nullif(p.middle_name, ''), p.last_name), p.short_name, "
            "CAST(p.birth_date AS VARCHAR), NULL, p.role_name, NULL, NULL FROM lake.main.wyscout_players p",
            (
                (
                    ("wyscout_teams", "wyscout_match_formations"),
                    "SELECT p.player_id, concat_ws(' ', p.first_name, nullif(p.middle_name, ''), p.last_name), "
                    "p.short_name, CAST(p.birth_date AS VARCHAR), NULL, p.role_name, t.name, f.match_key "
                    "FROM lake.main.wyscout_players p "
                    "LEFT JOIN lake.main.wyscout_teams t ON t.team_id = TRY_CAST(p.current_team_id AS BIGINT) "
                    "LEFT JOIN lake.main.wyscout_match_formations f ON f.player_id = p.player_id",
                ),
                (
                    ("wyscout_teams",),
                    "SELECT p.player_id, concat_ws(' ', p.first_name, nullif(p.middle_name, ''), p.last_name), "
                    "p.short_name, CAST(p.birth_date AS VARCHAR), NULL, p.role_name, t.name, NULL "
                    "FROM lake.main.wyscout_players p "
                    "LEFT JOIN lake.main.wyscout_teams t ON t.team_id = TRY_CAST(p.current_team_id AS BIGINT)",
                ),
            ),
        ),
    ),
    "idsse": (
        _Query(
            ("idsse_persons",),
            "SELECT p.person_id, concat_ws(' ', p.first_name, p.last_name), p.shortname, NULL, "
            "CAST(p.shirt_number AS VARCHAR), p.playing_position, NULL, p.match_key "
            "FROM lake.main.idsse_persons p WHERE p.person_kind = 'player'",
            (
                (
                    ("idsse_teams",),
                    "SELECT p.person_id, concat_ws(' ', p.first_name, p.last_name), p.shortname, NULL, "
                    "CAST(p.shirt_number AS VARCHAR), p.playing_position, t.team_name, p.match_key "
                    "FROM lake.main.idsse_persons p LEFT JOIN lake.main.idsse_teams t "
                    "ON t.match_key = p.match_key AND t.team_id = p.team_id WHERE p.person_kind = 'player'",
                ),
            ),
        ),
    ),
    "skillcorner": (
        _Query(
            ("skillcorner_players",),
            "SELECT p.player_id, concat_ws(' ', p.first_name, p.last_name), p.short_name, CAST(p.birthday AS VARCHAR), "
            "CAST(p.number AS VARCHAR), p.player_role_position_group, NULL, p.match_key "
            "FROM lake.main.skillcorner_players p",
            (
                (
                    ("skillcorner_matches",),
                    "SELECT p.player_id, concat_ws(' ', p.first_name, p.last_name), p.short_name, "
                    "CAST(p.birthday AS VARCHAR), CAST(p.number AS VARCHAR), p.player_role_position_group, "
                    "CASE WHEN p.team_id = m.home_team_id THEN m.home_team_name "
                    "WHEN p.team_id = m.away_team_id THEN m.away_team_name END, p.match_key "
                    "FROM lake.main.skillcorner_players p LEFT JOIN lake.main.skillcorner_matches m "
                    "ON m.match_key = p.match_key",
                ),
            ),
        ),
    ),
    "gradientsports": (
        _Query(
            ("gradientsports_rosters",),
            "SELECT player_id, player_nickname, player_nickname, NULL, CAST(shirt_number AS VARCHAR), "
            "position_group_type, team_name, match_key FROM lake.main.gradientsports_rosters",
        ),
        _Query(
            ("gradientsports_player_list",),
            "SELECT id, concat_ws(' ', firstName, lastName), nickname, CAST(TRY_CAST(dob AS DATE) AS VARCHAR), NULL, "
            "positionGroupType, NULL, NULL FROM lake.main.gradientsports_player_list",
        ),
    ),
}

# Each team query gives: raw id, name, area.
_TEAMS: Final[dict[Provider, Sequence[_Query]]] = {
    "statsbomb": (
        _Query(("statsbomb_lineup_teams",), "SELECT team_id, team_name, NULL FROM lake.main.statsbomb_lineup_teams"),
        _Query(
            ("statsbomb_matches",),
            "SELECT home_team_id, home_team_name, home_team_country_name FROM lake.main.statsbomb_matches UNION ALL "
            "SELECT away_team_id, away_team_name, away_team_country_name FROM lake.main.statsbomb_matches",
        ),
    ),
    "wyscout": (_Query(("wyscout_teams",), "SELECT team_id, name, area_name FROM lake.main.wyscout_teams"),),
    "idsse": (
        _Query(("idsse_teams",), "SELECT team_id, team_name, NULL FROM lake.main.idsse_teams"),
        _Query(
            ("idsse_match_information",),
            "SELECT general_home_team_id, general_home_team_name, environment_country "
            "FROM lake.main.idsse_match_information UNION ALL "
            "SELECT general_guest_team_id, general_guest_team_name, NULL FROM lake.main.idsse_match_information",
        ),
    ),
    "skillcorner": (
        _Query(
            ("skillcorner_matches",),
            "SELECT home_team_id, home_team_name, competition_area FROM lake.main.skillcorner_matches UNION ALL "
            "SELECT away_team_id, away_team_name, competition_area FROM lake.main.skillcorner_matches",
        ),
    ),
    "gradientsports": (
        _Query(
            ("gradientsports_matches",),
            "SELECT home_team_id, home_team_name, NULL FROM lake.main.gradientsports_matches UNION ALL "
            "SELECT away_team_id, away_team_name, NULL FROM lake.main.gradientsports_matches",
        ),
        _Query(("gradientsports_rosters",), "SELECT team_id, team_name, NULL FROM lake.main.gradientsports_rosters"),
    ),
}

# Each competition query gives: raw id, name, area.
_COMPETITIONS: Final[dict[Provider, Sequence[_Query]]] = {
    "statsbomb": (
        _Query(
            ("statsbomb_competitions",),
            "SELECT competition_id, competition_name, country_name FROM lake.main.statsbomb_competitions",
        ),
        _Query(
            ("statsbomb_matches",),
            "SELECT competition_id, competition_name, competition_country_name FROM lake.main.statsbomb_matches",
        ),
    ),
    "wyscout": (
        _Query(("wyscout_competitions",), "SELECT competition_id, name, area_name FROM lake.main.wyscout_competitions"),
    ),
    "idsse": (
        _Query(
            ("idsse_match_information",),
            "SELECT general_competition_id, general_competition_name, environment_country "
            "FROM lake.main.idsse_match_information",
        ),
    ),
    "skillcorner": (
        _Query(
            ("skillcorner_matches",),
            "SELECT competition_id, competition_name, competition_area FROM lake.main.skillcorner_matches",
        ),
    ),
    "gradientsports": (
        _Query(("gradientsports_competitions",), "SELECT id, name, NULL FROM lake.main.gradientsports_competitions"),
        _Query(
            ("gradientsports_matches",),
            "SELECT competition_id, competition_name, NULL FROM lake.main.gradientsports_matches",
        ),
    ),
}

# Each match query gives: raw id, date, home team id, home team name, away team id, away team name, raw competition
# id, raw season id, season name. IDSSE gives the bronze `native_match_id` (the bare DFL id): its raw `MatchId` carries
# the `DFL-MAT-` prefix. The native season id is `native_season_id(provider, native competition id, str(raw season))`,
# as every parser builds it (spec §14.4); Gradient Sports has only the season label.
_MATCHES: Final[dict[Provider, Sequence[_Query]]] = {
    "statsbomb": (
        _Query(
            ("statsbomb_matches",),
            "SELECT match_id, match_date, home_team_id, home_team_name, away_team_id, away_team_name, "
            "competition_id, season_id, season_name FROM lake.main.statsbomb_matches",
        ),
    ),
    "wyscout": (
        _Query(
            ("wyscout_matches", "wyscout_match_teams", "wyscout_teams"),
            "SELECT m.match_id, CAST(m.dateutc AS DATE), h.team_id, home_t.name, a.team_id, away_t.name, "
            "m.competition_id, m.season_id, NULL "
            "FROM lake.main.wyscout_matches m "
            "JOIN lake.main.wyscout_match_teams h ON h.match_id = m.match_id AND h.side = 'home' "
            "JOIN lake.main.wyscout_match_teams a ON a.match_id = m.match_id AND a.side = 'away' "
            "LEFT JOIN lake.main.wyscout_teams home_t ON home_t.team_id = h.team_id "
            "LEFT JOIN lake.main.wyscout_teams away_t ON away_t.team_id = a.team_id",
        ),
    ),
    "idsse": (
        _Query(
            ("idsse_match_information",),
            "SELECT native_match_id, CAST(general_kickoff_time AS DATE), general_home_team_id, "
            "general_home_team_name, general_guest_team_id, general_guest_team_name, general_competition_id, "
            "general_season_id, general_season FROM lake.main.idsse_match_information",
        ),
    ),
    "skillcorner": (
        _Query(
            ("skillcorner_matches",),
            "SELECT match_id, CAST(date_time AS DATE), home_team_id, home_team_name, away_team_id, away_team_name, "
            "competition_id, season_id, season_name FROM lake.main.skillcorner_matches",
        ),
    ),
    "gradientsports": (
        _Query(
            ("gradientsports_matches",),
            "SELECT id, CAST(date AS DATE), home_team_id, home_team_name, away_team_id, away_team_name, "
            "competition_id, season, season FROM lake.main.gradientsports_matches",
        ),
    ),
}


def _mode(values: Iterable[object]) -> str | None:
    """The most frequent non-empty value as text (ties: the smallest)."""
    counts = Counter(str(v).strip() for v in values if v is not None and str(v).strip())
    if not counts:
        return None
    top = max(counts.values())
    return min(value for value, count in counts.items() if count == top)


def _tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main'"
    ).fetchall()
    return {str(name) for (name,) in rows}


def _rows(conn: duckdb.DuckDBPyConnection, queries: Sequence[_Query], tables: set[str]) -> list[tuple[Any, ...]]:
    found: list[tuple[Any, ...]] = []
    for query in queries:
        sql = query.text(tables)
        if sql is not None:
            found.extend(conn.execute(sql).fetchall())
    return found


def _group(rows: Iterable[tuple[Any, ...]], to_native: Callable[[Any], str]) -> dict[str, list[tuple[Any, ...]]]:
    grouped: dict[str, list[tuple[Any, ...]]] = {}
    for row in rows:
        if row[0] is None:
            continue
        grouped.setdefault(to_native(row[0]), []).append(row)
    return grouped


def _players(provider: Provider, rows: Sequence[tuple[Any, ...]]) -> list[EntityRecord]:
    records: list[EntityRecord] = []
    for native_id, group in sorted(_group(rows, _PLAYER_IDS[provider]).items()):
        key = player_key(provider, native_id)
        name = _mode(r[1] for r in group)
        if key is None or name is None:
            continue
        matches = {r[7] for r in group if r[7] is not None}
        records.append(
            EntityRecord(
                ref=RecordRef(provider, native_id),
                key=key,
                name=name,
                short_name=_mode(r[2] for r in group),
                birth_date=_mode(r[3] for r in group),
                jersey=_mode(r[4] for r in group),
                position=_mode(r[5] for r in group),
                team=_mode(r[6] for r in group),
                matches=len(matches) or None,
            )
        )
    return records


def _named(
    provider: Provider,
    rows: Sequence[tuple[Any, ...]],
    to_native: Callable[[Any], str],
    to_key: Callable[[Provider, str], int | None],
) -> list[EntityRecord]:
    records: list[EntityRecord] = []
    for native_id, group in sorted(_group(rows, to_native).items()):
        key = to_key(provider, native_id)
        name = _mode(r[1] for r in group)
        if key is None or name is None:
            continue
        records.append(
            EntityRecord(ref=RecordRef(provider, native_id), key=key, name=name, area=_mode(r[2] for r in group))
        )
    return records


def _as_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def _matches(provider: Provider, rows: Sequence[tuple[Any, ...]]) -> list[EntityRecord]:
    to_team = _TEAM_IDS[provider]
    records: list[EntityRecord] = []
    for native_id, group in sorted(_group(rows, _MATCH_IDS[provider]).items()):
        _, day, home_id, home, away_id, away, competition_raw, season_raw, season_name = group[0]
        if day is None or home_id is None or away_id is None:
            continue
        competition = None if competition_raw is None else _COMPETITION_IDS[provider](competition_raw)
        season = (
            native.native_season_id(provider, competition, str(season_raw))
            if competition is not None and season_raw is not None
            else None
        )
        records.append(
            EntityRecord(
                ref=RecordRef(provider, native_id),
                key=match_key(provider, native_id),
                name=f"{home} - {away}",
                match_date=_as_date(day),
                home=None if home is None else str(home),
                away=None if away is None else str(away),
                home_team=RecordRef(provider, to_team(home_id)),
                away_team=RecordRef(provider, to_team(away_id)),
                native_competition_id=competition,
                competition_key=None if competition is None else competition_key(provider, competition),
                native_season_id=season,
                season_key=None if season is None else season_key(provider, season),
                season_name=None if season_name is None else str(season_name),
            )
        )
    return records


def load_records(conn: duckdb.DuckDBPyConnection, entity: Entity) -> dict[Provider, list[EntityRecord]]:
    """The records of `entity` per provider that has any, from the bronze tables of the lake."""
    tables = _tables(conn)
    queries: Mapping[Provider, Sequence[_Query]] = {
        "player": _PLAYERS,
        "team": _TEAMS,
        "competition": _COMPETITIONS,
        "match": _MATCHES,
    }[entity]
    result: dict[Provider, list[EntityRecord]] = {}
    for provider, provider_queries in queries.items():
        rows = _rows(conn, provider_queries, tables)
        if entity == "player":
            records = _players(provider, rows)
        elif entity == "team":
            records = _named(provider, rows, _TEAM_IDS[provider], team_key)
        elif entity == "competition":
            records = _named(provider, rows, _COMPETITION_IDS[provider], competition_key)
        else:
            records = _matches(provider, rows)
        if records:
            result[provider] = records
    return result
