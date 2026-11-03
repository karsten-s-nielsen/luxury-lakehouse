"""Wyscout field-inventory contract (spec §12.2). The fixtures are small real slices of the CC BY 4.0 public dataset
(Pappalardo and Massucco 2019; `tests/fixtures/wyscout/README.md`)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse.errors import UnmappedFieldError
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.parsers.wyscout import WyscoutParser

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "wyscout"
FIXTURE_FILES: dict[str, tuple[str, ...]] = {
    "matches": ("matches_World_Cup_slice.json", "matches_cover.json"),
    "events": ("events_World_Cup_slice.json",),
    "competitions": ("competitions.json",),
    "teams": ("teams.json",),
    "players": ("players_slice.json",),
    "coaches": ("coaches_slice.json",),
    "event_names": ("eventid2name.csv",),
    "tag_names": ("tags2name.csv",),
}

# Reuse audit (spec §12.6). IRIS `providers/wyscout.py` keeps from the matches only the ids and `teamsData` as one
# JSON text column, from the events only `matchId` and `teamId`, and from the players and teams a few flat columns
# (it decodes the twice-escaped names in place). v1 `src/ingestion/wyscout.py` keeps the top-level fields with
# `positions`, `tags`, `role` and the areas as JSON text, which §12.1 forbids, casts mixed-type columns to text, and
# also decodes the names in place. Neither reads the coaches or the two id-to-name mappings, and neither splits
# `teamsData` into typed rows. v2 keeps every field below as its own typed column and the names as delivered.
# The raw fields of figshare collection 4415000 read on 2026-10-27: every member of matches.zip, the World Cup and
# European Championship members of events.zip, and the whole listing files; the live tier reads every member.
# `teamsData` is a map keyed by team id, so its key is written `<id>`.
_TEAM = "teamsData.<id>"
EXPECTED_FIELDS: dict[str, frozenset[str]] = {
    "matches": frozenset(
        {
            "competitionId",
            "date",
            "dateutc",
            "duration",
            "gameweek",
            "groupName",
            "label",
            "referees",
            "referees[].refereeId",
            "referees[].role",
            "roundId",
            "seasonId",
            "status",
            "venue",
            "winner",
            "wyId",
            *(
                f"{_TEAM}.{key}"
                for key in ("coachId", "hasFormation", "score", "scoreET", "scoreHT", "scoreP", "side", "teamId")
            ),
            *(
                f"{_TEAM}.formation.{role}[].{key}"
                for role in ("bench", "lineup")
                for key in ("assists", "goals", "ownGoals", "playerId", "redCards", "yellowCards")
            ),
            f"{_TEAM}.formation.substitutions",
            *(f"{_TEAM}.formation.substitutions[].{key}" for key in ("assists", "minute", "playerIn", "playerOut")),
        }
    ),
    "events": frozenset(
        {
            "eventId",
            "eventName",
            "eventSec",
            "id",
            "matchId",
            "matchPeriod",
            "playerId",
            "positions[].x",
            "positions[].y",
            "subEventId",
            "subEventName",
            "tags",
            "tags[].id",
            "teamId",
        }
    ),
    "competitions": frozenset(
        {"area.alpha2code", "area.alpha3code", "area.id", "area.name", "format", "name", "type", "wyId"}
    ),
    "teams": frozenset(
        {"area.alpha2code", "area.alpha3code", "area.id", "area.name", "city", "name", "officialName", "type", "wyId"}
    ),
    "players": frozenset(
        {
            *(f"{area}.{key}" for area in ("birthArea", "passportArea") for key in ("alpha2code", "alpha3code", "id")),
            "birthArea.name",
            "passportArea.name",
            "birthDate",
            "currentNationalTeamId",
            "currentTeamId",
            "firstName",
            "foot",
            "height",
            "lastName",
            "middleName",
            "role.code2",
            "role.code3",
            "role.name",
            "shortName",
            "weight",
            "wyId",
        }
    ),
    "coaches": frozenset(
        {
            *(
                f"{area}.{key}"
                for area in ("birthArea", "passportArea")
                for key in ("alpha2code", "alpha3code", "id", "name")
            ),
            "birthDate",
            "currentTeamId",
            "firstName",
            "lastName",
            "middleName",
            "shortName",
            "wyId",
        }
    ),
    "event_names": frozenset({"event", "event_label", "subevent", "subevent_label"}),
    "tag_names": frozenset({"Description", "Label", "Tag"}),
}


def _inventory(artifact: str) -> frozenset[str]:
    parser = WyscoutParser()
    return frozenset[str]().union(
        *(parser.field_inventory(artifact, (FIXTURES / name).read_bytes()) for name in FIXTURE_FILES[artifact])
    )


@pytest.mark.parametrize("artifact", sorted(FIXTURE_FILES))
def test_fixture_holds_every_expected_field(artifact: str) -> None:
    assert _inventory(artifact) == EXPECTED_FIELDS[artifact]


@pytest.mark.parametrize("artifact", sorted(FIXTURE_FILES))
def test_field_inventory_complete(artifact: str) -> None:
    assert unmapped_fields(EXPECTED_FIELDS[artifact], WyscoutParser().bronze_columns(artifact)) == []


@pytest.mark.parametrize(
    ("artifact", "name", "anchor", "extra"),
    [
        ("events", "events_World_Cup_slice.json", b'"eventId": ', b'"zone": 3, "eventId": '),
        ("players", "players_slice.json", b'"wyId": ', b'"nickname": "x", "wyId": '),
        ("competitions", "competitions.json", b'"wyId": ', b'"zone": 3, "wyId": '),
    ],
)
def test_unmapped_field_fails_loudly(artifact: str, name: str, anchor: bytes, extra: bytes) -> None:
    raw = (FIXTURES / name).read_bytes()
    assert anchor in raw
    parser = WyscoutParser()
    inventory = parser.field_inventory(artifact, raw.replace(anchor, extra, 1))
    unmapped = unmapped_fields(inventory, parser.bronze_columns(artifact))
    assert len(unmapped) == 1
    assert unmapped[0].lower().endswith(("zone", "nickname"))
    if artifact != "events":
        with pytest.raises(UnmappedFieldError, match=unmapped[0]):
            parser.listing(artifact, raw.replace(anchor, extra, 1), run_id="r", source_hash="h" * 64)  # type: ignore[arg-type]


def test_unmapped_csv_column_fails_loudly() -> None:
    lines = (FIXTURES / "tags2name.csv").read_bytes().splitlines()
    raw = b"\n".join([lines[0] + b",Zone", *(line + b",3" for line in lines[1:] if line)]) + b"\n"
    parser = WyscoutParser()
    assert unmapped_fields(parser.field_inventory("tag_names", raw), parser.bronze_columns("tag_names")) == ["Zone"]
    with pytest.raises(UnmappedFieldError, match="Zone"):
        parser.listing("tag_names", raw, run_id="r", source_hash="h" * 64)
