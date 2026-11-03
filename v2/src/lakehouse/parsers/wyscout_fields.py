"""The Wyscout public-dataset fields and their types (spec §12.1, §12.2, §29.15; plan Task 19).

These declarations are the field inventory of figshare collection 4415000 (Pappalardo and Massucco 2019, CC BY 4.0),
read on 2026-10-27: every match file of matches.zip, the World Cup and European Championship members of events.zip,
and the players, teams, competitions and coaches files, plus the event-name and tag-name mappings. A field outside
them fails loudly (`UnmappedFieldError`, §12.3); the live tier reads every competition. The source has three traps
that the types handle exactly: an id that is an integer in one record and text in another (`area.id`,
`currentTeamId`, `currentNationalTeamId`, which also holds the text `"null"`, and `subEventId`) is a text column
(`convert="text"`); counts that the formations write as text (`"0"`) are integers (`convert="int_text"`); and
`dateutc` is a UTC time written with a space (`convert="timestamp_utc_space"`). Event positions are percentages of
the pitch, as delivered (data paper, "Data Records > Events").
"""

from __future__ import annotations

from typing import Final

import pyarrow as pa

from lakehouse.parsers.csvmap import CsvKind
from lakehouse.parsers.jsonmap import Field

__all__ = [
    "COACH_FIELDS",
    "COMPETITION_FIELDS",
    "EVENT_FIELDS",
    "EVENT_NAMES_CSV",
    "FORMATION_KEYS",
    "MATCH_FIELDS",
    "MATCH_TEAM_KEYS",
    "PLAYER_FIELDS",
    "SUBSTITUTION_KEYS",
    "TAG_NAMES_CSV",
    "TEAM_FIELDS",
]

_I64: Final = pa.int64()
_STR: Final = pa.string()


def _area(prefix: str, column: str) -> tuple[Field, ...]:
    return (
        Field(f"{column}_id", f"{prefix}.id", _STR, convert="text"),
        Field(f"{column}_name", f"{prefix}.name", _STR),
        Field(f"{column}_alpha2code", f"{prefix}.alpha2code", _STR),
        Field(f"{column}_alpha3code", f"{prefix}.alpha3code", _STR),
    )


COMPETITION_FIELDS: Final = (
    Field("competition_id", "wyId", _I64),
    Field("name", "name", _STR),
    Field("format", "format", _STR),
    Field("type", "type", _STR),
    *_area("area", "area"),
)
TEAM_FIELDS: Final = (
    Field("team_id", "wyId", _I64),
    Field("name", "name", _STR),
    Field("official_name", "officialName", _STR),
    Field("city", "city", _STR),
    Field("type", "type", _STR),
    *_area("area", "area"),
)
PLAYER_FIELDS: Final = (
    Field("player_id", "wyId", _I64),
    Field("short_name", "shortName", _STR),
    Field("first_name", "firstName", _STR),
    Field("middle_name", "middleName", _STR),
    Field("last_name", "lastName", _STR),
    Field("birth_date", "birthDate", pa.date32(), convert="date"),
    Field("height", "height", _I64),
    Field("weight", "weight", _I64),
    Field("foot", "foot", _STR),
    Field("current_team_id", "currentTeamId", _STR, convert="text"),
    Field("current_national_team_id", "currentNationalTeamId", _STR, convert="text"),
    Field("role_name", "role.name", _STR),
    Field("role_code2", "role.code2", _STR),
    Field("role_code3", "role.code3", _STR),
    *_area("birthArea", "birth_area"),
    *_area("passportArea", "passport_area"),
)
COACH_FIELDS: Final = (
    Field("coach_id", "wyId", _I64),
    Field("short_name", "shortName", _STR),
    Field("first_name", "firstName", _STR),
    Field("middle_name", "middleName", _STR),
    Field("last_name", "lastName", _STR),
    Field("birth_date", "birthDate", pa.date32(), convert="date"),
    Field("current_team_id", "currentTeamId", _I64),
    *_area("birthArea", "birth_area"),
    *_area("passportArea", "passport_area"),
)
# One row per match; `teamsData` (a map keyed by team id) becomes the match-team, formation and substitution tables.
MATCH_FIELDS: Final = (
    Field("match_id", "wyId", _I64),
    Field("label", "label", _STR),
    Field("date", "date", _STR),  # "July 15, 2018 at 5:00:00 PM GMT+2", as delivered (the UTC time is `dateutc`)
    Field("dateutc", "dateutc", pa.timestamp("us", tz="UTC"), convert="timestamp_utc_space"),
    Field("status", "status", _STR),
    Field("duration", "duration", _STR),
    Field("winner", "winner", _I64),
    Field("venue", "venue", _STR),
    Field("gameweek", "gameweek", _I64),
    Field("group_name", "groupName", _STR),
    Field("round_id", "roundId", _I64),
    Field("season_id", "seasonId", _I64),
    Field("competition_id", "competitionId", _I64),
    Field("referees", "referees", pa.list_(pa.struct([("refereeId", _I64), ("role", _STR)]))),
)
# `teamsData.<team id>` keys, and the keys of its formation entries and substitutions.
MATCH_TEAM_KEYS: Final = frozenset(
    {"teamId", "side", "score", "scoreHT", "scoreET", "scoreP", "coachId", "hasFormation", "formation"}
)
FORMATION_KEYS: Final = frozenset({"playerId", "goals", "ownGoals", "assists", "redCards", "yellowCards"})
SUBSTITUTION_KEYS: Final = frozenset({"playerIn", "playerOut", "minute", "assists"})
EVENT_FIELDS: Final = (
    Field("id", "id", _I64),
    Field("event_id", "eventId", _I64),
    Field("event_name", "eventName", _STR),
    Field("sub_event_id", "subEventId", _STR, convert="text"),  # an integer, or "" for an event without subtype
    Field("sub_event_name", "subEventName", _STR),
    Field("event_sec", "eventSec", pa.float64()),
    Field("match_id", "matchId", _I64),
    Field("match_period", "matchPeriod", _STR),
    Field("player_id", "playerId", _I64),
    Field("team_id", "teamId", _I64),
    Field("positions", "positions", pa.list_(pa.struct([("x", _I64), ("y", _I64)]))),
    Field("tags", "tags", pa.list_(pa.struct([("id", _I64)]))),
)
EVENT_NAMES_CSV: Final[dict[str, CsvKind]] = {
    "event": "int",
    "subevent": "int",
    "event_label": "str",
    "subevent_label": "str",
}
TAG_NAMES_CSV: Final[dict[str, CsvKind]] = {"Tag": "int", "Label": "str", "Description": "str"}
