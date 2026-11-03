"""The Wyscout parser: the public dataset's matches and events files to bronze (spec §11, §12, §29.15; plan Task 19).

The source delivers one matches file and one events file per competition (members of matches.zip and events.zip), so
a match's deliveries are its competition's files: the parser takes the match whose `wyId` is the entry's id and the
events whose `matchId` is that id. Each file is parsed and its inventory checked once per content hash (a small cache
of the last file of each kind), so the 64 matches of one competition share one parse.

`teamsData` is a map keyed by team id. It becomes `wyscout_match_teams` (one row per team; the key must equal the
team's `teamId`), `wyscout_match_formations` (one row per lineup and bench entry) and `wyscout_match_substitutions`.
The source writes some formations' `substitutions` as the string `"null"` instead of a list; that text is kept in
`substitutions_text`. The players, teams, competitions and coaches files and the two id-to-name mappings are
source-level listings (§10.5, `listing`).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Final, Literal, cast

import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.entries import SourceEntry
from lakehouse.errors import IntegrityError, UnexpectedPayloadError
from lakehouse.exact import loads_exact
from lakehouse.ids.keys import competition_key, match_key, player_key, season_key, team_key
from lakehouse.ids.native import (
    native_season_id,
    wyscout_native_competition_id,
    wyscout_native_match_id,
    wyscout_native_player_id,
    wyscout_native_team_id,
)
from lakehouse.parsers.base import CoordinateSystem, NamesBundle, ParsedMatch
from lakehouse.parsers.columns import append_columns, constant, key_array, with_audit
from lakehouse.parsers.csvmap import CsvColumns, csv_header
from lakehouse.parsers.inventory import check_mapped, json_field_paths
from lakehouse.parsers.jsonmap import Field, convert_text, describe_fields, mapped_paths, records_to_table
from lakehouse.parsers.wyscout_fields import (
    COACH_FIELDS,
    COMPETITION_FIELDS,
    EVENT_FIELDS,
    EVENT_NAMES_CSV,
    FORMATION_KEYS,
    MATCH_FIELDS,
    MATCH_TEAM_KEYS,
    PLAYER_FIELDS,
    SUBSTITUTION_KEYS,
    TAG_NAMES_CSV,
    TEAM_FIELDS,
)
from lakehouse.raw.archive import Delivery

__all__ = [
    "ARTIFACTS",
    "COACHES",
    "COMPETITIONS",
    "EVENTS",
    "EVENT_NAMES",
    "LISTINGS",
    "LISTING_TABLES",
    "MATCHES",
    "MATCH_FORMATIONS",
    "MATCH_SUBSTITUTIONS",
    "MATCH_TEAMS",
    "PLAYERS",
    "TAG_NAMES",
    "TEAMS",
    "WyscoutParser",
    "bronze_docs",
]

PROVIDER: Final = "wyscout"
PARSER_VERSION: Final = "1"  # the bronze output version (spec §13.2)
Artifact = Literal["matches", "events"]
Listing = Literal["competitions", "teams", "players", "coaches", "event_names", "tag_names"]
ARTIFACTS: Final[tuple[Artifact, ...]] = ("matches", "events")
LISTINGS: Final[tuple[Listing, ...]] = ("competitions", "teams", "players", "coaches", "event_names", "tag_names")

MATCHES: Final = bronze_table("wyscout_matches")
MATCH_TEAMS: Final = bronze_table("wyscout_match_teams")
MATCH_FORMATIONS: Final = bronze_table("wyscout_match_formations")
MATCH_SUBSTITUTIONS: Final = bronze_table("wyscout_match_substitutions")
EVENTS: Final = bronze_table("wyscout_events")
COMPETITIONS: Final = bronze_table("wyscout_competitions")
TEAMS: Final = bronze_table("wyscout_teams")
PLAYERS: Final = bronze_table("wyscout_players")
COACHES: Final = bronze_table("wyscout_coaches")
EVENT_NAMES: Final = bronze_table("wyscout_event_names")
TAG_NAMES: Final = bronze_table("wyscout_tag_names")

_STR: Final = pa.string()
_I64: Final = pa.int64()
_TEAM_MAP: Final = "teamsData.<id>"
_MATCH_RAW_PATHS: Final = (
    mapped_paths(MATCH_FIELDS)
    | frozenset(f"{_TEAM_MAP}.{key}" for key in MATCH_TEAM_KEYS if key != "formation")
    | frozenset(f"{_TEAM_MAP}.formation.{role}[].{key}" for role in ("lineup", "bench") for key in FORMATION_KEYS)
    | frozenset(f"{_TEAM_MAP}.formation.substitutions[].{key}" for key in SUBSTITUTION_KEYS)
    | frozenset(f"{_TEAM_MAP}.formation.{name}" for name in ("lineup", "bench", "substitutions"))  # empty, or text
    | frozenset({_TEAM_MAP + ".formation"})
)
_EVENT_RAW_PATHS: Final = mapped_paths(EVENT_FIELDS)
_LISTING_FIELDS: Final[dict[str, tuple[Field, ...]]] = {
    "competitions": COMPETITION_FIELDS,
    "teams": TEAM_FIELDS,
    "players": PLAYER_FIELDS,
    "coaches": COACH_FIELDS,
}
LISTING_TABLES: Final[dict[Listing, BronzeTable]] = {
    "competitions": COMPETITIONS,
    "teams": TEAMS,
    "players": PLAYERS,
    "coaches": COACHES,
    "event_names": EVENT_NAMES,
    "tag_names": TAG_NAMES,
}
_CSV_LISTINGS: Final[dict[str, CsvColumns]] = {
    "event_names": CsvColumns(EVENT_NAMES_CSV),
    "tag_names": CsvColumns(TAG_NAMES_CSV),
}
_COORDINATES: Final = CoordinateSystem(
    origin="top_left",
    units="pct",
    pitch_length=None,
    pitch_width=None,
    x_direction="0 to 100: the event's nearness to the opponent's goal, from the acting team's perspective",
    y_direction="0 to 100: the event's nearness to the right side of the field, from the acting team's perspective",
    play_direction="every event is given from the perspective of the team that performs it (the attacking team)",
    source_ref="Pappalardo et al. 2019, Sci Data 6:236, doi:10.1038/s41597-019-0247-7, 'Data Records > Events' "
    "(positions)",
)


def _competition_key(raw: object) -> int | None:
    return competition_key(PROVIDER, wyscout_native_competition_id(int(cast("int", raw))))


def _player_key(raw: object) -> int | None:
    return player_key(PROVIDER, wyscout_native_player_id(int(cast("int", raw))))


def _team_key(raw: object) -> int | None:
    return team_key(PROVIDER, wyscout_native_team_id(int(cast("int", raw))))


# The surrogate key of each listing: (key column, id column, key function); coaches have no key (spec §14).
_LISTING_KEYS: Final[dict[str, tuple[str, str, Callable[[object], int | None]]]] = {
    "competitions": ("competition_key", "competition_id", _competition_key),
    "teams": ("team_key", "team_id", _team_key),
    "players": ("player_key", "player_id", _player_key),
}


def _keys(rows: int, native: str) -> dict[str, pa.Array[Any]]:
    return {
        "provider": constant(PROVIDER, rows, _STR),
        "native_match_id": constant(native, rows, _STR),
        "match_key": constant(match_key(PROVIDER, native), rows, _I64),
    }


def _list(decoded: bytes, artifact: str) -> list[dict[str, Any]]:
    payload = loads_exact(decoded)
    if not isinstance(payload, list):
        raise UnexpectedPayloadError(f"a Wyscout {artifact} file must hold a JSON list")
    return cast("list[dict[str, Any]]", payload)


def _fold_team_keys(paths: frozenset[str]) -> frozenset[str]:
    """`teamsData.<team id>.…` paths with the id folded to `<id>`, so a map key is not a field."""
    folded: set[str] = set()
    for path in paths:
        if path.startswith("teamsData."):
            head, _, rest = path[len("teamsData.") :].partition(".")
            path = f"{_TEAM_MAP}.{rest}" if head.isdigit() and rest else (_TEAM_MAP if head.isdigit() else path)
        folded.add(path)
    return frozenset(folded)


def _inventory(artifact: str, records: list[dict[str, Any]]) -> frozenset[str]:
    paths = json_field_paths(records)
    return _fold_team_keys(paths) if artifact == "matches" else paths


def _count(entry: Mapping[str, Any], key: str, where: str, *, null_text: bool = False) -> int | None:
    """A count that the source writes as text (`"0"`); a missing key is null.

    With `null_text`, the source writes the text `"null"` for some entries (formation `goals`: 39,700 of 74,098 in
    the whole dataset) and never omits the key or writes a JSON null, so the text `"null"` becomes null and a missing
    key or a JSON null fails: the mapping stays one to one.
    """
    if key not in entry:
        if null_text:
            raise UnexpectedPayloadError(f"{where}.{key} is missing; the source always writes it")
        return None
    value = entry[key]
    if null_text and value == "null":
        return None
    if not isinstance(value, str):
        raise UnexpectedPayloadError(f"{where}.{key}={value!r} is not a count written as text")
    return cast("int", convert_text(value, "int_text"))


class WyscoutParser:
    """The Wyscout `ProviderParser`."""

    def __init__(self) -> None:
        self._matches: tuple[str, dict[str, list[dict[str, Any]]]] | None = None
        self._events: tuple[str, pa.Table, dict[int, list[int]]] | None = None

    @property
    def provider(self) -> Literal["wyscout"]:
        return "wyscout"

    @property
    def version(self) -> str:
        """The version of this parser's bronze output: bump `PARSER_VERSION` with any change that alters bronze, and
        `lakehouse reparse --all` re-parses the matches of an older version from the raw archive (spec §13.2)."""
        return PARSER_VERSION

    def bronze_columns(self, artifact: str) -> frozenset[str]:
        if artifact == "matches":
            return _MATCH_RAW_PATHS
        if artifact == "events":
            return _EVENT_RAW_PATHS
        if artifact in _LISTING_FIELDS:
            return mapped_paths(_LISTING_FIELDS[artifact])
        if artifact in _CSV_LISTINGS:
            return frozenset(_CSV_LISTINGS[artifact].kinds)
        raise ValueError(f"unknown Wyscout artifact {artifact!r}")

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]:
        if artifact in _CSV_LISTINGS:
            return csv_header(decoded)
        return _inventory(artifact, _list(decoded, artifact))

    def _checked(self, delivery: Delivery, artifact: Artifact) -> list[dict[str, Any]]:
        records = _list(delivery.decoded, artifact)
        check_mapped(artifact, _inventory(artifact, records), self.bronze_columns(artifact))
        return records

    def _match_index(self, delivery: Delivery) -> dict[str, list[dict[str, Any]]]:
        """The matches of one competition file by match id, read once per content hash."""
        if self._matches is None or self._matches[0] != delivery.content_sha256:
            index: dict[str, list[dict[str, Any]]] = {}
            for record in self._checked(delivery, "matches"):
                index.setdefault(str(record.get("wyId")), []).append(record)
            self._matches = (delivery.content_sha256, index)
        return self._matches[1]

    def _event_rows(self, delivery: Delivery, match_id: int) -> pa.Table:
        """The events of one match, in file order; the competition file is converted once per content hash, and
        the row positions of each match are found in one pass (no per-match scan of the file)."""
        if self._events is None or self._events[0] != delivery.content_sha256:
            table = records_to_table(self._checked(delivery, "events"), EVENT_FIELDS, artifact="events")
            positions: dict[int, list[int]] = {}
            for row, value in enumerate(table.column("match_id").to_pylist()):
                positions.setdefault(cast("int", value), []).append(row)
            self._events = (delivery.content_sha256, table, positions)
        _, table, positions = self._events
        return table.take(pa.array(positions.get(match_id, []), pa.int64()))

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:
        native = wyscout_native_match_id(int(entry.native_match_id))
        matches = deliveries.get("matches")
        if matches is None:
            raise IntegrityError(f"Wyscout match {native} has no matches delivery")
        at = datetime.now(UTC)
        found = self._match_index(matches).get(native, [])
        if len(found) != 1:
            raise IntegrityError(f"the Wyscout matches file holds {len(found)} matches with id {native}")
        match = found[0]
        record = {key: value for key, value in match.items() if key != "teamsData"}
        table = records_to_table([record], MATCH_FIELDS, artifact="matches")
        competition = wyscout_native_competition_id(int(match["competitionId"]))
        season = native_season_id(PROVIDER, competition, str(match["seasonId"]))
        table = append_columns(
            table,
            {
                **_keys(1, native),
                "competition_key": key_array([competition], lambda c: competition_key(PROVIDER, c)),
                "season_key": key_array([season], lambda s: season_key(PROVIDER, s)),
            },
        )
        tables: dict[BronzeTable, pa.Table] = {
            name: with_audit(built, run_id=run_id, source_hash=matches.content_sha256, at=at)
            for name, built in {MATCHES: table, **_team_tables(match, native)}.items()
        }

        events = deliveries.get("events")
        if events is not None:
            table = self._event_rows(events, int(native))
            table = append_columns(
                table,
                {
                    "player_key": key_array(table.column("player_id").to_pylist(), _player_key),
                    "team_key": key_array(table.column("team_id").to_pylist(), _team_key),
                    **_keys(table.num_rows, native),
                },
            )
            tables[EVENTS] = with_audit(table, run_id=run_id, source_hash=events.content_sha256, at=at)

        systems = {"events": _COORDINATES} if events is not None else {}
        return ParsedMatch(tables=tables, coordinate_systems=systems, names=_names(tables, match))

    def listing_table(self, kind: str) -> BronzeTable:
        if kind not in LISTING_TABLES:
            raise ValueError(f"unknown Wyscout listing {kind!r}")
        return LISTING_TABLES[kind]

    def listing_visibility(self, kind: str) -> str | None:
        self.listing_table(kind)
        return None  # open data: every record is public

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table:
        """A source-level listing (spec §10.5) with its keys and audit columns."""
        at = datetime.now(UTC)
        if kind in _CSV_LISTINGS:
            table = _CSV_LISTINGS[kind].table(decoded, artifact=kind)
            table = table.rename_columns([name.lower() for name in table.column_names])
        elif kind in _LISTING_FIELDS:
            table = records_to_table(_list(decoded, kind), _LISTING_FIELDS[kind], artifact=kind)
        else:
            raise ValueError(f"unknown Wyscout listing {kind!r}")
        extra: dict[str, pa.Array[Any]] = {"provider": constant(PROVIDER, table.num_rows, _STR)}
        if kind in _LISTING_KEYS:
            column, source, make = _LISTING_KEYS[kind]
            extra[column] = key_array(table.column(source).to_pylist(), make)
        return with_audit(append_columns(table, extra), run_id=run_id, source_hash=source_hash, at=at)


def _team_tables(match: Mapping[str, Any], native: str) -> dict[BronzeTable, pa.Table]:
    teams = cast("Mapping[str, Mapping[str, Any]]", match.get("teamsData") or {})
    team_rows: list[dict[str, Any]] = []
    formation_rows: list[dict[str, Any]] = []
    substitution_rows: list[dict[str, Any]] = []
    for key, team in teams.items():
        if str(team.get("teamId")) != key:
            raise IntegrityError(f"match {native}: teamsData key {key} differs from its teamId {team.get('teamId')}")
        team_id = int(key)
        formation = cast("Mapping[str, Any]", team.get("formation") or {})
        substitutions = formation.get("substitutions")
        team_rows.append(
            {
                "team_id": team_id,
                "side": team.get("side"),
                "score": team.get("score"),
                "score_ht": team.get("scoreHT"),
                "score_et": team.get("scoreET"),
                "score_p": team.get("scoreP"),
                "coach_id": team.get("coachId"),
                "has_formation": team.get("hasFormation"),
                "substitutions_text": substitutions if isinstance(substitutions, str) else None,
            }
        )
        for role in ("lineup", "bench"):
            for index, entry in enumerate(cast("list[Mapping[str, Any]]", formation.get(role) or [])):
                where = f"teamsData.{key}.formation.{role}[{index}]"
                formation_rows.append(
                    {
                        "team_id": team_id,
                        "role": role,
                        "list_index": index,
                        "player_id": entry.get("playerId"),
                        "goals": _count(entry, "goals", where, null_text=True),
                        "own_goals": _count(entry, "ownGoals", where),
                        "assists": _count(entry, "assists", where),
                        "red_cards": _count(entry, "redCards", where),
                        "yellow_cards": _count(entry, "yellowCards", where),
                    }
                )
        if isinstance(substitutions, list):
            for index, entry in enumerate(cast("list[Mapping[str, Any]]", substitutions)):
                substitution_rows.append(
                    {
                        "team_id": team_id,
                        "list_index": index,
                        "player_in": entry.get("playerIn"),
                        "player_out": entry.get("playerOut"),
                        "minute": entry.get("minute"),
                        "assists": _count(entry, "assists", f"teamsData.{key}.formation.substitutions[{index}]"),
                    }
                )
    team_types = {
        "team_id": _I64,
        "side": _STR,
        "score": _I64,
        "score_ht": _I64,
        "score_et": _I64,
        "score_p": _I64,
        "coach_id": _I64,
        "has_formation": _I64,
        "substitutions_text": _STR,
    }
    formation_types = {
        "team_id": _I64,
        "role": _STR,
        "list_index": pa.int16(),
        "player_id": _I64,
        "goals": _I64,
        "own_goals": _I64,
        "assists": _I64,
        "red_cards": _I64,
        "yellow_cards": _I64,
    }
    substitution_types = {
        "team_id": _I64,
        "list_index": pa.int16(),
        "player_in": _I64,
        "player_out": _I64,
        "minute": _I64,
        "assists": _I64,
    }
    built: dict[BronzeTable, pa.Table] = {}
    for name, rows, types, keys in (
        (MATCH_TEAMS, team_rows, team_types, {"team_key": ("team_id", _team_key)}),
        (
            MATCH_FORMATIONS,
            formation_rows,
            formation_types,
            {"player_key": ("player_id", _player_key), "team_key": ("team_id", _team_key)},
        ),
        (
            MATCH_SUBSTITUTIONS,
            substitution_rows,
            substitution_types,
            {
                "player_in_key": ("player_in", _player_key),
                "player_out_key": ("player_out", _player_key),
                "team_key": ("team_id", _team_key),
            },
        ),
    ):
        table = pa.table({column: pa.array([r[column] for r in rows], t) for column, t in types.items()})
        table = table.add_column(0, "match_id", pa.array([int(native)] * len(rows), _I64))
        extra: dict[str, pa.Array[Any]] = {
            column: key_array(table.column(source).to_pylist(), cast("Callable[[Any], int | None]", make))
            for column, (source, make) in keys.items()
        }
        built[name] = append_columns(table, {**extra, **_keys(len(rows), native)})
    return built


def _names(tables: Mapping[BronzeTable, pa.Table], match: Mapping[str, Any]) -> NamesBundle:
    formations = tables[MATCH_FORMATIONS]
    players = pa.table({"player_id": pa.array(sorted(set(formations.column("player_id").to_pylist())), _I64)})
    team_ids = cast("Mapping[str, Any]", match.get("teamsData") or {})
    teams = pa.table({"team_id": pa.array(sorted(int(key) for key in team_ids), _I64)})
    competitions = pa.table(
        {
            "competition_id": pa.array([match.get("competitionId")], _I64),
            "season_id": pa.array([match.get("seasonId")], _I64),
        }
    )
    return NamesBundle(players=players, teams=teams, competitions=competitions)


_AUDIT_DOCS: Final = [
    ("_ingested_at", "When the run parsed the delivery (UTC)."),
    ("_run_id", "The run that wrote the row."),
    ("_source_hash", "The content_sha256 of the delivered file that the row comes from (spec §13.1)."),
]
_MATCH_KEY_DOCS: Final = [
    ("provider", "The provider name (`wyscout`)."),
    ("native_match_id", "The canonical native match id, the Wyscout `wyId` as text (Task 2)."),
    ("match_key", "Surrogate match key: xxhash64 of `wyscout|native_match_id`, seed 42, signed (spec §14)."),
]
_LISTING_DOCS: Final = [("provider", "The provider name (`wyscout`).")]
# The players, teams and matches files escape non-ASCII letters twice (`"Rakiti\\u0107"`), so the text holds a
# literal `\uXXXX`; bronze keeps it as delivered and the Wyscout staging view decodes it (Task 23).
_ESCAPED: Final = " The source escapes non-ASCII letters twice, so the text holds literal `\\uXXXX`; kept as delivered."
_NOTES: Final[dict[str, dict[str, str]]] = {
    "matches": {
        "referees": " The referee ids and roles of the match; the referees file is out of cycle 1 (spec §29.15).",
        "label": _ESCAPED,
        "venue": _ESCAPED,
    },
    "events": {
        "sub_event_id": ' An integer, or the text "" for an event without a subtype; kept as text.',
        "positions": " Percentages of the pitch (0 to 100) from the acting team's view, as delivered (spec §11.4).",
    },
    "teams": {"name": _ESCAPED, "official_name": _ESCAPED, "city": _ESCAPED},
    "players": {
        "short_name": _ESCAPED,
        "first_name": _ESCAPED,
        "last_name": _ESCAPED,
        "birth_area_name": _ESCAPED,
        "passport_area_name": _ESCAPED,
        "current_national_team_id": ' An integer or the text "null" in the source; kept as text.',
        "current_team_id": " An integer or text in the source; kept as text.",
    },
}


def _described(fields: tuple[Field, ...], artifact: str = "") -> list[tuple[str, str]]:
    notes = _NOTES.get(artifact, {})
    return [(column, text + notes.get(column, "")) for column, text in describe_fields(fields)]


def _csv_docs(kind: str) -> list[tuple[str, str]]:
    return [(column.lower(), text) for column, text in _CSV_LISTINGS[kind].describe()]


def _count_doc(column: str, key: str) -> tuple[str, str]:
    return (column, f'Source field `{key}`, a count written as text ("0"), as exact int64; null when missing.')


def bronze_docs() -> list[tuple[BronzeTable, str, list[tuple[str, str]]]]:
    """Every bronze table with the description of each of its columns, in table order (spec §12.8)."""
    team = "Source field `teamsData.<id>"
    match_id = ("match_id", "Source field `wyId` of the match, as int64.")
    return [
        (
            MATCHES,
            "One row per match of a competition's matches file (`matches_<competition>.json` in matches.zip); "
            "`teamsData` goes to the match-team, formation and substitution tables.",
            [
                *_described(MATCH_FIELDS, "matches"),
                *_MATCH_KEY_DOCS,
                ("competition_key", "Surrogate competition key of `competition_id` (spec §14)."),
                ("season_key", "Surrogate season key of `competition_id:season_id` (spec §14, Task 2)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            MATCH_TEAMS,
            "One row per team of a match (`teamsData`, a map keyed by team id; the key equals `teamId`).",
            [
                match_id,
                ("team_id", f"{team}.teamId`, as int64."),
                ("side", f"{team}.side` (home or away)."),
                ("score", f"{team}.score`, as int64."),
                ("score_ht", f"{team}.scoreHT`, as int64 (the half-time score)."),
                ("score_et", f"{team}.scoreET`, as int64 (the extra-time score)."),
                ("score_p", f"{team}.scoreP`, as int64 (the penalty-shootout score)."),
                ("coach_id", f"{team}.coachId`, as int64."),
                ("has_formation", f"{team}.hasFormation`, as int64 (0 or 1, as delivered)."),
                (
                    "substitutions_text",
                    f"{team}.formation.substitutions` where the source writes it as text (the text `null`) instead "
                    "of a list; null where it is a list.",
                ),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            MATCH_FORMATIONS,
            "One row per lineup and bench entry of a team of a match (`teamsData.<id>.formation`).",
            [
                match_id,
                ("team_id", "The `teamsData` key, as int64."),
                ("role", "`lineup` or `bench`: the list the entry comes from."),
                ("list_index", "The 0-based position in that list."),
                ("player_id", "Source field `playerId`, as int64."),
                (
                    "goals",
                    "Source field `goals`, a count written as text, as exact int64; null where the source writes the "
                    "text `null` (it never omits the field or writes a JSON null).",
                ),
                _count_doc("own_goals", "ownGoals"),
                _count_doc("assists", "assists"),
                _count_doc("red_cards", "redCards"),
                _count_doc("yellow_cards", "yellowCards"),
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            MATCH_SUBSTITUTIONS,
            "One row per substitution of a team of a match (`teamsData.<id>.formation.substitutions[]`).",
            [
                match_id,
                ("team_id", "The `teamsData` key, as int64."),
                ("list_index", "The 0-based position in the substitution list."),
                ("player_in", "Source field `playerIn`, as int64."),
                ("player_out", "Source field `playerOut`, as int64."),
                ("minute", "Source field `minute`, as int64."),
                _count_doc("assists", "assists"),
                ("player_in_key", "Surrogate player key of `player_in` (spec §14)."),
                ("player_out_key", "Surrogate player key of `player_out` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            EVENTS,
            "One row per event of a match, in file order (`events_<competition>.json` in events.zip).",
            [
                *_described(EVENT_FIELDS, "events"),
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            COMPETITIONS,
            "One row per competition (`competitions.json`, a source-level listing, spec §10.5).",
            [
                *_described(COMPETITION_FIELDS),
                *_LISTING_DOCS,
                ("competition_key", "Surrogate competition key of `competition_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            TEAMS,
            "One row per team (`teams.json`, a source-level listing, spec §10.5).",
            [
                *_described(TEAM_FIELDS, "teams"),
                *_LISTING_DOCS,
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            PLAYERS,
            "One row per player (`players.json`, a source-level listing, spec §10.5).",
            [
                *_described(PLAYER_FIELDS, "players"),
                *_LISTING_DOCS,
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            COACHES,
            "One row per coach (`coaches.json`, a source-level listing, spec §29.15); coaches have no surrogate key.",
            [*_described(COACH_FIELDS), *_LISTING_DOCS, *_AUDIT_DOCS],
        ),
        (
            EVENT_NAMES,
            "One row per event and subevent id with its labels (`eventid2name.csv`, spec §29.15).",
            [*_csv_docs("event_names"), *_LISTING_DOCS, *_AUDIT_DOCS],
        ),
        (
            TAG_NAMES,
            "One row per event tag id with its label and description (`tags2name.csv`, spec §29.15).",
            [*_csv_docs("tag_names"), *_LISTING_DOCS, *_AUDIT_DOCS],
        ),
    ]
