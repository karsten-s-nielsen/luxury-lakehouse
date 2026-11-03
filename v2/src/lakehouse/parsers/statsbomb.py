"""StatsBomb parser: events, lineups and 360 frames per match, plus the competitions and matches listings.

The pining owner tier (spec §7.2) delivers the same feed files (`events`, `roster` = lineups, `freeze_frames` = 360
frames) and a per-match `metadata` object in the feed's match shape. Its commercial deliveries (provenance
`original`) carry more fields: OBV values, pass clusters and goalkeeper and shot models on the events, team-level
`events` and `formations` and player details and `stats` on the lineups, and seven 360 measures; the metadata adds
`attendance`, `behind_closed_doors`, `neutral_ground`, `collection_status` and `play_status` (measured on the owner
tier, 2026-10-27). They land in the same tables, plus `statsbomb_lineup_teams` and `statsbomb_match_metadata`.

Every field of the StatsBomb open-data JSON lands as its own typed column (spec §12.1): nested `{id, name}` objects
become `<prefix>_id` / `<prefix>_name`, flags become booleans, locations stay lists of float64 in the provider's
yards, and lists of objects (tactics lineups, shot freeze frames, 360 frames) become typed lists of structs. The
field set is the union of the open-data samples and the StatsBomb event specification; any other field raises
`UnmappedFieldError`. Coordinates are float64 as delivered: every JSON number must round-trip exactly (`loads_exact`);
360 locations carry up to 17 significant digits, which an integer of hundredths cannot hold (ledger ruling, Task 15).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Final, Literal, cast

import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.entries import SourceEntry
from lakehouse.errors import IntegrityError, UnexpectedPayloadError
from lakehouse.ids.keys import competition_key, match_key, player_key, season_key, team_key
from lakehouse.ids.native import (
    native_season_id,
    statsbomb_native_competition_id,
    statsbomb_native_match_id,
    statsbomb_native_player_id,
    statsbomb_native_team_id,
)
from lakehouse.parsers.base import CoordinateSystem, NamesBundle, ParsedMatch
from lakehouse.parsers.columns import append_columns, key_array, with_audit
from lakehouse.parsers.inventory import check_mapped, json_field_paths
from lakehouse.parsers.jsonmap import Field, loads_exact, mapped_paths, records_to_table
from lakehouse.raw.archive import Delivery

__all__ = [
    "COMPETITIONS",
    "COORDINATES",
    "EVENTS",
    "LINEUPS",
    "LINEUP_TEAMS",
    "MATCHES",
    "MATCH_METADATA",
    "THREE_SIXTY",
    "StatsBombParser",
]

PROVIDER: Final = "statsbomb"
PARSER_VERSION: Final = "1"  # the bronze output version (spec §13.2)
_MATCH_LIST_KIND: Final = re.compile(r"^matches/\d+/\d+$")
EVENTS: Final = bronze_table("statsbomb_events")
LINEUPS: Final = bronze_table("statsbomb_lineups")
LINEUP_TEAMS: Final = bronze_table("statsbomb_lineup_teams")
MATCH_METADATA: Final = bronze_table("statsbomb_match_metadata")
THREE_SIXTY: Final = bronze_table("statsbomb_three_sixty")
MATCHES: Final = bronze_table("statsbomb_matches")
COMPETITIONS: Final = bronze_table("statsbomb_competitions")

Artifact = Literal["events", "lineups", "three_sixty", "matches", "competitions", "metadata"]

COORDINATES: Final = CoordinateSystem(
    origin="top_left",
    units="yd",
    pitch_length=120.0,
    pitch_width=80.0,
    x_direction="left to right along the pitch length (0 to 120)",
    y_direction="top to bottom across the pitch width (0 to 80)",
    play_direction="each event is given from the acting team's view, attacking left to right",
    source_ref="statsbomb/open-data doc/Open Data Events v4.0.0.pdf, section 'Pitch Coordinates'",
)

_F64: Final = pa.float64()
_I64: Final = pa.int64()
_STR: Final = pa.string()
_BOOL: Final = pa.bool_()
_LOC: Final = pa.list_(_F64)
_ID_NAME: Final = pa.struct([("id", _I64), ("name", _STR)])

Kind = Literal["bool", "idname", "float", "loc", "str", "int"]

# Type-specific event objects: key -> {sub-field: kind}, from the open-data samples and the event specification.
_TYPE_OBJECTS: Final[dict[str, dict[str, Kind]]] = {
    "50_50": {"outcome": "idname", "counterpress": "bool"},
    "bad_behaviour": {"card": "idname"},
    "ball_receipt": {"outcome": "idname"},
    "ball_recovery": {"offensive": "bool", "recovery_failure": "bool"},
    "block": {"deflection": "bool", "offensive": "bool", "save_block": "bool", "counterpress": "bool"},
    "carry": {"end_location": "loc"},
    "clearance": {
        "aerial_won": "bool",
        "body_part": "idname",
        "head": "bool",
        "left_foot": "bool",
        "right_foot": "bool",
        "other": "bool",
    },
    "dribble": {"overrun": "bool", "nutmeg": "bool", "outcome": "idname", "no_touch": "bool"},
    "dribbled_past": {"counterpress": "bool"},
    "duel": {"counterpress": "bool", "type": "idname", "outcome": "idname"},
    "foul_committed": {
        "counterpress": "bool",
        "offensive": "bool",
        "type": "idname",
        "advantage": "bool",
        "penalty": "bool",
        "card": "idname",
    },
    "foul_won": {"defensive": "bool", "advantage": "bool", "penalty": "bool"},
    "goalkeeper": {
        "position": "idname",
        "technique": "idname",
        "body_part": "idname",
        "type": "idname",
        "outcome": "idname",
        "end_location": "loc",
        "shot_saved_to_post": "bool",
        "punched_out": "bool",
        "success_in_play": "bool",
        "lost_in_play": "bool",
        "lost_out": "bool",
        "saved_to_post": "bool",
        "shot_saved_off_target": "bool",
        "penalty_saved_to_post": "bool",
        "success_out": "bool",
    },
    "half_end": {"early_video_end": "bool", "match_suspended": "bool"},
    "half_start": {"late_video_start": "bool"},
    "injury_stoppage": {"in_chain": "bool"},
    "interception": {"outcome": "idname"},
    "miscontrol": {"aerial_won": "bool"},
    "pass": {
        "recipient": "idname",
        "length": "float",
        "angle": "float",
        "height": "idname",
        "end_location": "loc",
        "assisted_shot_id": "str",
        "backheel": "bool",
        "deflected": "bool",
        "miscommunication": "bool",
        "cross": "bool",
        "cut_back": "bool",
        "switch": "bool",
        "shot_assist": "bool",
        "goal_assist": "bool",
        "body_part": "idname",
        "type": "idname",
        "outcome": "idname",
        "technique": "idname",
        "inswinging": "bool",
        "outswinging": "bool",
        "straight": "bool",
        "through_ball": "bool",
        "aerial_won": "bool",
        "no_touch": "bool",
        "pass_cluster_id": "int",  # commercial
        "pass_cluster_label": "str",
        "pass_cluster_probability": "float",
        "pass_success_probability": "float",
        "xclaim": "float",
    },
    "player_off": {"permanent": "bool"},
    "pressure": {"counterpress": "bool"},
    "shot": {
        "key_pass_id": "str",
        "end_location": "loc",
        "aerial_won": "bool",
        "follows_dribble": "bool",
        "first_time": "bool",
        "open_goal": "bool",
        "statsbomb_xg": "float",
        "deflected": "bool",
        "technique": "idname",
        "body_part": "idname",
        "type": "idname",
        "outcome": "idname",
        "one_on_one": "bool",
        "redirect": "bool",
        "saved_off_target": "bool",
        "saved_to_post": "bool",
        "kick_off": "bool",
        "gk_positioning_xg_suppression": "float",  # commercial
        "gk_save_difficulty_xg": "float",
        "gk_shot_stopping_xg_suppression": "float",
        "shot_execution_xg": "float",
        "shot_execution_xg_uplift": "float",
        "shot_goal_assist": "bool",
        "shot_shot_assist": "bool",
    },
    "substitution": {"replacement": "idname", "outcome": "idname"},
}
_KIND_TYPES: Final[dict[Kind, pa.DataType]] = {
    "bool": _BOOL,
    "float": _F64,
    "loc": _LOC,
    "str": _STR,
    "int": _I64,
}


def _prefix(key: str) -> str:
    return "fifty_fifty" if key == "50_50" else key


def _fields_of(prefix: str, path: str, spec: Mapping[str, Kind]) -> list[Field]:
    out: list[Field] = []
    for sub, kind in spec.items():
        if kind == "idname":
            out.append(Field(f"{prefix}_{sub}_id", f"{path}.{sub}.id", _I64))
            out.append(Field(f"{prefix}_{sub}_name", f"{path}.{sub}.name", _STR))
        else:
            out.append(Field(f"{prefix}_{sub}", f"{path}.{sub}", _KIND_TYPES[kind]))
    return out


def _idname(column: str, path: str) -> list[Field]:
    return [Field(f"{column}_id", f"{path}.id", _I64), Field(f"{column}_name", f"{path}.name", _STR)]


EVENT_FIELDS: Final[tuple[Field, ...]] = (
    Field("event_id", "id", _STR),
    Field("index", "index", _I64),
    Field("period", "period", _I64),
    Field("timestamp", "timestamp", pa.duration("ms"), convert="clock_ms"),
    Field("minute", "minute", _I64),
    Field("second", "second", _I64),
    *_idname("type", "type"),
    Field("possession", "possession", _I64),
    *_idname("possession_team", "possession_team"),
    *_idname("play_pattern", "play_pattern"),
    *_idname("team", "team"),
    *_idname("player", "player"),
    *_idname("position", "position"),
    Field("location", "location", _LOC),
    Field("duration", "duration", _F64),
    Field("under_pressure", "under_pressure", _BOOL),
    Field("off_camera", "off_camera", _BOOL),
    Field("out", "out", _BOOL),
    Field("counterpress", "counterpress", _BOOL),
    Field("related_events", "related_events", pa.list_(_STR)),
    Field("tactics_formation", "tactics.formation", _I64),
    Field(
        "tactics_lineup",
        "tactics.lineup",
        pa.list_(pa.struct([("jersey_number", _I64), ("player", _ID_NAME), ("position", _ID_NAME)])),
    ),
    Field(
        "shot_freeze_frame",
        "shot.freeze_frame",
        pa.list_(pa.struct([("location", _LOC), ("player", _ID_NAME), ("position", _ID_NAME), ("teammate", _BOOL)])),
    ),
    *(field for key, spec in _TYPE_OBJECTS.items() for field in _fields_of(_prefix(key), key, spec)),
    *(  # On-Ball Value (commercial)
        Field(name, name, _F64)
        for name in (
            "obv_for_after",
            "obv_for_before",
            "obv_for_net",
            "obv_against_after",
            "obv_against_before",
            "obv_against_net",
            "obv_total_net",
        )
    ),
)

_PLAYER_ROW_FIELDS: Final[tuple[Field, ...]] = (
    Field("player_id", "player_id", _I64),
    Field("player_name", "player_name", _STR),
    Field("player_nickname", "player_nickname", _STR),
    Field("jersey_number", "jersey_number", _I64),
    *_idname("country", "country"),
    Field(
        "cards",
        "cards",
        pa.list_(pa.struct([("time", pa.duration("s")), ("card_type", _STR), ("reason", _STR), ("period", _I64)])),
        nested={"[].time": "clock_s"},
    ),
    Field(
        "positions",
        "positions",
        pa.list_(
            pa.struct(
                [
                    ("position_id", _I64),
                    ("position", _STR),
                    ("from", pa.duration("ms")),
                    ("to", pa.duration("ms")),
                    ("from_period", _I64),
                    ("to_period", _I64),
                    ("start_reason", _STR),
                    ("end_reason", _STR),
                    ("counterpart_id", _I64),  # commercial
                    ("counterpart_name", _STR),
                ]
            )
        ),
        nested={"[].from": "clock_ms_or_s", "[].to": "clock_ms_or_s"},  # MM:SS open, HH:MM:SS.fff commercial
    ),
    Field("birth_date", "birth_date", pa.date32(), convert="date"),  # commercial
    Field("player_gender", "player_gender", _STR),
    Field("player_height", "player_height", _F64),
    Field("player_weight", "player_weight", _F64),
    Field(
        "stats",
        "stats",
        pa.struct(
            [
                (name, _I64)
                for name in ("assists", "goals", "own_goals", "penalties_missed", "penalties_saved", "penalties_scored")
            ]
        ),
    ),
)
# Team-level lists of the commercial lineups: one row per team (`statsbomb_lineup_teams`).
LINEUP_TEAM_FIELDS: Final[tuple[Field, ...]] = (
    Field("team_id", "team_id", _I64),
    Field("team_name", "team_name", _STR),
    Field(
        "events",
        "events",
        pa.list_(
            pa.struct(
                [
                    ("outcome", _STR),
                    ("period", _I64),
                    ("player_id", _I64),
                    ("player_name", _STR),
                    ("timestamp", pa.duration("ms")),
                    ("type", _STR),
                ]
            )
        ),
        nested={"[].timestamp": "clock_ms"},
    ),
    Field(
        "formations",
        "formations",
        pa.list_(
            pa.struct([("formation", _I64), ("period", _I64), ("reason", _STR), ("timestamp", pa.duration("ms"))])
        ),
        nested={"[].timestamp": "clock_ms"},
    ),
)
LINEUP_FIELDS: Final[tuple[Field, ...]] = (
    Field("team_id", "team_id", _I64),
    Field("team_name", "team_name", _STR),
    *_PLAYER_ROW_FIELDS,
)
_LINEUP_TEAM_LISTS: Final = frozenset({"events", "formations"})

THREE_SIXTY_FIELDS: Final[tuple[Field, ...]] = (
    Field("event_uuid", "event_uuid", _STR),
    Field("visible_area", "visible_area", _LOC),
    Field(
        "freeze_frame",
        "freeze_frame",
        pa.list_(pa.struct([("actor", _BOOL), ("keeper", _BOOL), ("location", _LOC), ("teammate", _BOOL)])),
    ),
    Field("ball_receipt_exceeds_distance", "ball_receipt_exceeds_distance", _I64),  # commercial
    Field("ball_receipt_in_space", "ball_receipt_in_space", _BOOL),
    Field("distance_to_nearest_defender", "distance_to_nearest_defender", _F64),
    Field(
        "distances_from_edge_of_visible_area",
        "distances_from_edge_of_visible_area",
        pa.list_(pa.struct([("distance", _F64), ("point_id", _I64)])),
    ),
    Field("line_breaking_pass", "line_breaking_pass", _BOOL),
    Field("num_defenders_on_goal_side_of_actor", "num_defenders_on_goal_side_of_actor", _I64),
    Field("visible_player_counts", "visible_player_counts", pa.list_(pa.struct([("count", _I64), ("team_id", _I64)]))),
)

_MANAGERS: Final = pa.list_(
    pa.struct([("id", _I64), ("name", _STR), ("nickname", _STR), ("dob", pa.date32()), ("country", _ID_NAME)])
)


def _team(side: str) -> list[Field]:
    return [
        Field(f"{side}_team_id", f"{side}_team.{side}_team_id", _I64),
        Field(f"{side}_team_name", f"{side}_team.{side}_team_name", _STR),
        Field(f"{side}_team_gender", f"{side}_team.{side}_team_gender", _STR),
        Field(f"{side}_team_group", f"{side}_team.{side}_team_group", _STR),
        *_idname(f"{side}_team_country", f"{side}_team.country"),
        Field(f"{side}_team_managers", f"{side}_team.managers", _MANAGERS, nested={"[].dob": "date"}),
    ]


MATCH_FIELDS: Final[tuple[Field, ...]] = (
    Field("match_id", "match_id", _I64),
    Field("match_date", "match_date", pa.date32(), convert="date"),
    Field("kick_off", "kick_off", pa.time32("ms"), convert="time_ms"),
    Field("competition_id", "competition.competition_id", _I64),
    Field("competition_name", "competition.competition_name", _STR),
    Field("competition_country_name", "competition.country_name", _STR),
    Field("season_id", "season.season_id", _I64),
    Field("season_name", "season.season_name", _STR),
    *_team("home"),
    *_team("away"),
    Field("home_score", "home_score", _I64),
    Field("away_score", "away_score", _I64),
    Field("match_status", "match_status", _STR),
    Field("match_status_360", "match_status_360", _STR),
    Field("last_updated", "last_updated", pa.timestamp("us"), convert="timestamp"),
    Field("last_updated_360", "last_updated_360", pa.timestamp("us"), convert="timestamp"),
    Field("data_version", "metadata.data_version", _STR),
    Field("shot_fidelity_version", "metadata.shot_fidelity_version", _STR),
    Field("xy_fidelity_version", "metadata.xy_fidelity_version", _STR),
    Field("match_week", "match_week", _I64),
    *_idname("competition_stage", "competition_stage"),
    *_idname("stadium", "stadium"),
    *_idname("stadium_country", "stadium.country"),
    *_idname("referee", "referee"),
    *_idname("referee_country", "referee.country"),
)
# The pining per-match `metadata` artifact: the match shape plus the credentialed fields (commercial; null in the
# redistributed open data).
METADATA_FIELDS: Final[tuple[Field, ...]] = (
    *MATCH_FIELDS,
    Field("attendance", "attendance", _F64),
    Field("behind_closed_doors", "behind_closed_doors", _BOOL),
    Field("neutral_ground", "neutral_ground", _BOOL),
    Field("collection_status", "collection_status", _STR),
    Field("play_status", "play_status", _STR),
)

COMPETITION_FIELDS: Final[tuple[Field, ...]] = (
    Field("competition_id", "competition_id", _I64),
    Field("season_id", "season_id", _I64),
    Field("country_name", "country_name", _STR),
    Field("competition_name", "competition_name", _STR),
    Field("competition_gender", "competition_gender", _STR),
    Field("competition_youth", "competition_youth", _BOOL),
    Field("competition_international", "competition_international", _BOOL),
    Field("season_name", "season_name", _STR),
    Field("match_updated", "match_updated", pa.timestamp("us"), convert="timestamp"),
    Field("match_updated_360", "match_updated_360", pa.timestamp("us"), convert="timestamp"),
    Field("match_available_360", "match_available_360", pa.timestamp("us"), convert="timestamp"),
    Field("match_available", "match_available", pa.timestamp("us"), convert="timestamp"),
)

_FIELDS: Final[dict[Artifact, tuple[Field, ...]]] = {
    "events": EVENT_FIELDS,
    "lineups": LINEUP_FIELDS,
    "three_sixty": THREE_SIXTY_FIELDS,
    "matches": MATCH_FIELDS,
    "competitions": COMPETITION_FIELDS,
    "metadata": METADATA_FIELDS,
}
_LINEUP_RAW_PATHS: Final = mapped_paths(LINEUP_TEAM_FIELDS) | frozenset(
    f"lineup[].{path}" for path in mapped_paths(_PLAYER_ROW_FIELDS)
)


def _artifact(name: str) -> Artifact:
    if name not in _FIELDS:
        raise ValueError(f"unknown StatsBomb artifact {name!r}")
    return name


def _records(decoded: bytes, artifact: str) -> list[dict[str, Any]]:
    payload = loads_exact(decoded)
    if artifact == "metadata":  # one match object, not a list (pining owner tier)
        if not isinstance(payload, dict):
            raise IntegrityError("a StatsBomb metadata file must hold one JSON object")
        return [cast("dict[str, Any]", payload)]
    if not isinstance(payload, list):
        raise IntegrityError(f"a StatsBomb {artifact} file must hold a JSON list")
    return cast("list[dict[str, Any]]", payload)


def _player_row(team: Mapping[str, Any], player: Mapping[str, Any]) -> dict[str, Any]:
    """One lineup row; the commercial source writes `stats: []` for a player without stats, which becomes null
    (it never writes a null there, and open data has no `stats`)."""
    row = {"team_id": team.get("team_id"), "team_name": team.get("team_name"), **player}
    stats = row.get("stats")
    if isinstance(stats, list):
        if stats:
            raise UnexpectedPayloadError(f"lineups: player {row.get('player_id')} has stats that are a non-empty list")
        row["stats"] = None
    elif stats is not None and not isinstance(stats, dict):
        raise UnexpectedPayloadError(f"lineups: player {row.get('player_id')} has stats that are not an object")
    return row


def _player_key(raw: object) -> int | None:
    return player_key(PROVIDER, statsbomb_native_player_id(int(cast("int", raw))))


def _team_key(raw: object) -> int | None:
    return team_key(PROVIDER, statsbomb_native_team_id(int(cast("int", raw))))


class StatsBombParser:
    """The StatsBomb `ProviderParser`. It never knows which channel delivered the files (spec §6)."""

    @property
    def provider(self) -> Literal["statsbomb"]:
        return "statsbomb"

    @property
    def version(self) -> str:
        """The version of this parser's bronze output: bump `PARSER_VERSION` with any change that alters bronze, and
        `lakehouse reparse --all` re-parses the matches of an older version from the raw archive (spec §13.2)."""
        return PARSER_VERSION

    def bronze_columns(self, artifact: str) -> frozenset[str]:
        kind = _artifact(artifact)
        return _LINEUP_RAW_PATHS if kind == "lineups" else mapped_paths(_FIELDS[kind])

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]:
        _artifact(artifact)
        return json_field_paths(_records(decoded, artifact))

    def table(self, artifact: str, decoded: bytes) -> pa.Table:
        """The typed columns of one artifact, before keys and audit columns."""
        kind = _artifact(artifact)
        records = _records(decoded, artifact)
        if kind == "lineups":
            check_mapped("lineups", json_field_paths(records), _LINEUP_RAW_PATHS)
            rows = [
                _player_row(team, player)
                for team in records
                for player in cast("list[dict[str, Any]]", team.get("lineup") or [])
            ]
            return records_to_table(rows, LINEUP_FIELDS, artifact="lineups")
        return records_to_table(records, _FIELDS[kind], artifact=artifact)

    def lineup_teams(self, decoded: bytes) -> pa.Table:
        """One row per team of a lineups file, with its team-level lists (commercial; null in open data)."""
        records = _records(decoded, "lineups")
        check_mapped("lineups", json_field_paths(records), _LINEUP_RAW_PATHS)
        teams = [{key: value for key, value in team.items() if key != "lineup"} for team in records]
        return records_to_table(teams, LINEUP_TEAM_FIELDS, artifact="lineups")

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:
        native = statsbomb_native_match_id(int(entry.native_match_id))
        key = match_key(PROVIDER, native)
        at = datetime.now(UTC)
        tables: dict[BronzeTable, pa.Table] = {}
        metadata_keys: dict[str, pa.Array[Any]] = {}
        for artifact, name in (
            ("events", EVENTS),
            ("lineups", LINEUPS),
            ("lineup_teams", LINEUP_TEAMS),
            ("three_sixty", THREE_SIXTY),
            ("metadata", MATCH_METADATA),
        ):
            delivery = deliveries.get("lineups" if artifact == "lineup_teams" else artifact)
            if delivery is None:
                continue
            if artifact == "lineup_teams":
                table = self.lineup_teams(delivery.decoded)
            else:
                table = self.table(artifact, delivery.decoded)
            if artifact == "metadata":
                found = table.column("match_id").to_pylist()
                if found != [int(native)]:
                    raise IntegrityError(f"StatsBomb match {native}: the metadata names match {found}")
                metadata_keys = {
                    k: v for k, v in _listing_keys(table, "matches").items() if k not in ("provider", "match_key")
                }
            rows = table.num_rows
            extra: dict[str, pa.Array[Any]] = {
                "provider": pa.array([PROVIDER] * rows, _STR),
                "native_match_id": pa.array([native] * rows, _STR),
                "match_key": pa.array([key] * rows, _I64),
            }
            if artifact == "events":
                extra["player_key"] = key_array(table.column("player_id").to_pylist(), _player_key)
                extra["team_key"] = key_array(table.column("team_id").to_pylist(), _team_key)
                extra["possession_team_key"] = key_array(table.column("possession_team_id").to_pylist(), _team_key)
            elif artifact == "lineups":
                extra["player_key"] = key_array(table.column("player_id").to_pylist(), _player_key)
                extra["team_key"] = key_array(table.column("team_id").to_pylist(), _team_key)
            elif artifact == "lineup_teams":
                extra["team_key"] = key_array(table.column("team_id").to_pylist(), _team_key)
            elif artifact == "metadata":
                extra.update(metadata_keys)
            tables[name] = with_audit(
                append_columns(table, extra), run_id=run_id, source_hash=delivery.content_sha256, at=at
            )
        if EVENTS not in tables:
            raise IntegrityError(f"StatsBomb match {native} has no events delivery")
        systems = {artifact: COORDINATES for artifact in ("events", "three_sixty") if artifact in deliveries}
        return ParsedMatch(tables=tables, coordinate_systems=systems, names=self._names(entry, tables))

    @staticmethod
    def _names(entry: SourceEntry, tables: Mapping[BronzeTable, pa.Table]) -> NamesBundle:
        lineups = tables.get(LINEUPS)
        if lineups is not None:
            players = lineups.select(["player_id", "player_name", "player_nickname", "country_name"])
            teams = lineups.select(["team_id", "team_name"])
        else:
            events = tables[EVENTS]
            players = events.select(["player_id", "player_name"])
            teams = events.select(["team_id", "team_name"])
        metadata = tables.get(MATCH_METADATA)
        if metadata is not None:  # the match's own metadata (pining) names its competition and season
            row = metadata.to_pylist()[0]
            named = [str(row["competition_id"]), row["competition_name"], str(row["season_id"]), row["season_name"]]
        else:
            named = [entry.competition_id, entry.competition_name, entry.season_id, entry.season_name]
        competitions = pa.table(
            {
                name: pa.array([value], _STR)
                for name, value in zip(
                    ("competition_id", "competition_name", "season_id", "season_name"), named, strict=True
                )
            }
        )
        return NamesBundle(players=players, teams=teams, competitions=competitions)

    def listing_table(self, kind: str) -> BronzeTable:
        return COMPETITIONS if _listing_kind(kind) == "competitions" else MATCHES

    def listing_visibility(self, kind: str) -> str | None:
        _listing_kind(kind)
        return None  # open data: every record is public

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table:
        """A source-level listing (spec §10.5) with its keys and audit columns: `competitions`, or a match list
        (`matches` or `matches/<competition>/<season>`)."""
        artifact = _listing_kind(kind)
        table = self.table(artifact, decoded)
        return with_audit(
            append_columns(table, _listing_keys(table, artifact)),
            run_id=run_id,
            source_hash=source_hash,
            at=datetime.now(UTC),
        )


def _listing_kind(kind: str) -> Literal["matches", "competitions"]:
    if kind == "competitions":
        return "competitions"
    if kind == "matches" or _MATCH_LIST_KIND.match(kind):
        return "matches"
    raise ValueError(f"unknown StatsBomb listing {kind!r}")


def _listing_keys(table: pa.Table, artifact: Literal["matches", "competitions"]) -> dict[str, pa.Array[Any]]:
    """The provider, competition and season keys of a listing (and the match and team keys of a match row)."""
    comps = table.column("competition_id").to_pylist()
    seasons = table.column("season_id").to_pylist()
    native_comps = [statsbomb_native_competition_id(int(cast("int", c))) for c in comps]
    extra: dict[str, pa.Array[Any]] = {
        "provider": pa.array([PROVIDER] * table.num_rows, _STR),
        "competition_key": pa.array([competition_key(PROVIDER, c) for c in native_comps], _I64),
        "season_key": pa.array(
            [
                season_key(PROVIDER, native_season_id(PROVIDER, c, str(s)))
                for c, s in zip(native_comps, seasons, strict=True)
            ],
            _I64,
        ),
    }
    if artifact == "matches":
        extra["match_key"] = pa.array(
            [
                match_key(PROVIDER, statsbomb_native_match_id(int(cast("int", m))))
                for m in table.column("match_id").to_pylist()
            ],
            _I64,
        )
        extra["home_team_key"] = key_array(table.column("home_team_id").to_pylist(), _team_key)
        extra["away_team_key"] = key_array(table.column("away_team_id").to_pylist(), _team_key)
    return extra
