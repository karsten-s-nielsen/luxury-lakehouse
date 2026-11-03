"""Every bronze column is documented in the dbt sources (spec §12.8), and the docs match the parsed tables."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import yaml
from gskit import parse_with as parse_gs
from idssekit import parse_with
from skillcornerkit import ENTRY as SKILLCORNER_ENTRY
from skillcornerkit import delivery as skillcorner_delivery
from skillcornerkit import parse_with as parse_skillcorner

from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.parsers.gradientsports import COMPETITIONS as GS_COMPETITIONS
from lakehouse.parsers.gradientsports import PLAYER_LIST as GS_PLAYER_LIST
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.gradientsports import bronze_docs as gs_docs
from lakehouse.parsers.idsse import bronze_docs
from lakehouse.parsers.pining import LISTING_TABLES as PINING_TABLES
from lakehouse.parsers.pining import PiningListingParser
from lakehouse.parsers.pining import bronze_docs as pining_docs
from lakehouse.parsers.skillcorner import MATCH_LIST, SkillCornerParser
from lakehouse.parsers.skillcorner import PHYSICAL as SC_PHYSICAL
from lakehouse.parsers.skillcorner import bronze_docs as skillcorner_docs
from lakehouse.parsers.statsbomb import (
    COMPETITIONS,
    EVENTS,
    LINEUP_TEAMS,
    LINEUPS,
    MATCH_METADATA,
    MATCHES,
    THREE_SIXTY,
    StatsBombParser,
)
from lakehouse.parsers.wyscout import LISTING_TABLES as WS_LISTINGS
from lakehouse.parsers.wyscout import WyscoutParser
from lakehouse.parsers.wyscout import bronze_docs as ws_docs
from lakehouse.raw.archive import Delivery

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "statsbomb"


def _documented(provider: str) -> dict[str, list[str]]:
    document = cast(
        "dict[str, Any]",
        yaml.safe_load(
            (ROOT / "dbt" / "models" / "staging" / provider / f"_{provider}__sources.yml").read_text(encoding="utf-8")
        ),
    )
    source = cast("dict[str, Any]", document["sources"][0])
    assert (source["database"], source["schema"]) == ("lake", "main")  # dbt reads bronze in the DuckLake attach
    tables = cast("list[dict[str, Any]]", source["tables"])
    for table in tables:
        for column in cast("list[dict[str, str]]", table["columns"]):
            assert column["description"].strip(), f"{table['name']}.{column['name']} has no description"
    return {str(t["name"]): [str(c["name"]) for c in t["columns"]] for t in tables}


def test_statsbomb_columns_documented() -> None:
    parser = StatsBombParser()

    def delivery(artifact: str, name: str) -> Delivery:
        return Delivery.received(
            provider="statsbomb",
            channel="public",
            native_match_id="9100001",
            artifact=artifact,
            raw=(FIXTURES / name).read_bytes(),
            visibility="public",
            change_signal=None,
            fetched_at=datetime.now(UTC),
        )

    entry = SourceEntry(
        module_id="statsbomb_public",
        native_match_id="9100001",
        competition_id="43",
        competition_name="x",
        season_id="106",
        season_name="y",
        visibility="public",
        artifacts={"events": ArtifactRef(key="e", change_signal=None, expected_sha256=None, size=None)},
        listed_at=datetime.now(UTC),
    )
    parsed = parser.parse(
        entry,
        {
            "events": delivery("events", "events_9100001.json"),
            "lineups": delivery("lineups", "lineups_9100001.json"),
            "three_sixty": delivery("three_sixty", "three_sixty_9100001.json"),
            "metadata": delivery("metadata", "commercial_metadata_9100001.json"),
        },
        run_id="r",
    )
    actual = {name: table.column_names for name, table in parsed.tables.items()}
    actual[MATCHES] = parser.listing(
        "matches", (FIXTURES / "matches_43_106.json").read_bytes(), run_id="r", source_hash="h" * 64
    ).column_names
    actual[COMPETITIONS] = parser.listing(
        "competitions", (FIXTURES / "competitions.json").read_bytes(), run_id="r", source_hash="h" * 64
    ).column_names
    assert set(actual) == {EVENTS, LINEUPS, LINEUP_TEAMS, THREE_SIXTY, MATCH_METADATA, MATCHES, COMPETITIONS}
    assert _documented("statsbomb") == actual


def test_idsse_columns_documented() -> None:
    parsed = parse_with()
    actual = {name: table.column_names for name, table in parsed.tables.items()}
    assert _documented("idsse") == actual
    assert {name: [c for c, _ in columns] for name, _, columns in bronze_docs()} == actual


def test_skillcorner_columns_documented() -> None:
    parsed = parse_skillcorner()
    actual = {name: table.column_names for name, table in parsed.tables.items()}
    listing = SkillCornerParser().listing(
        "matches",
        (ROOT / "tests" / "fixtures" / "skillcorner" / "matches.json").read_bytes(),
        run_id="r",
        source_hash="h" * 64,
    )
    actual[MATCH_LIST] = listing.column_names
    physical = (ROOT / "tests" / "fixtures" / "skillcorner" / "physical_1899585_synthetic.parquet").read_bytes()
    with_physical = SkillCornerParser().parse(
        SKILLCORNER_ENTRY,
        {"match": skillcorner_delivery("match"), "physical": skillcorner_delivery("match", physical)},
        run_id="r",
    )
    actual[SC_PHYSICAL] = with_physical.tables[SC_PHYSICAL].column_names
    assert _documented("skillcorner") == actual
    assert {name: [c for c, _ in columns] for name, _, columns in skillcorner_docs()} == actual


def test_gradientsports_columns_documented() -> None:
    parsed = parse_gs()
    actual = {name: table.column_names for name, table in parsed.tables.items()}
    parser = GradientSportsParser()
    folder = ROOT / "tests" / "fixtures" / "gradientsports"
    actual[GS_COMPETITIONS] = parser.listing(
        "competitions", (folder / "competitions.csv").read_bytes(), run_id="r", source_hash="h" * 64
    ).column_names
    actual[GS_PLAYER_LIST] = parser.listing(
        "players", (folder / "players.csv").read_bytes(), run_id="r", source_hash="h" * 64
    ).column_names
    assert _documented("gradientsports") == actual
    assert {name: [c for c, _ in columns] for name, _, columns in gs_docs()} == actual


def test_wyscout_columns_documented() -> None:
    folder = ROOT / "tests" / "fixtures" / "wyscout"
    now = datetime.now(UTC)

    def delivery(artifact: str, name: str) -> Delivery:
        return Delivery.received(
            provider="wyscout",
            channel="public",
            native_match_id="2058017",
            artifact=artifact,
            raw=(folder / name).read_bytes(),
            visibility="public",
            change_signal=None,
            fetched_at=now,
        )

    entry = SourceEntry(
        module_id="wyscout_public",
        native_match_id="2058017",
        competition_id="28",
        competition_name=None,
        season_id="10078",
        season_name=None,
        visibility="public",
        artifacts={},
        listed_at=now,
    )
    parser = WyscoutParser()
    deliveries = {
        "matches": delivery("matches", "matches_World_Cup_slice.json"),
        "events": delivery("events", "events_World_Cup_slice.json"),
    }
    actual = {name: table.column_names for name, table in parser.parse(entry, deliveries, run_id="r").tables.items()}
    files = {
        "competitions": "competitions.json",
        "teams": "teams.json",
        "players": "players_slice.json",
        "coaches": "coaches_slice.json",
        "event_names": "eventid2name.csv",
        "tag_names": "tags2name.csv",
    }
    for kind, table in WS_LISTINGS.items():
        listing = parser.listing(kind, (folder / files[kind]).read_bytes(), run_id="r", source_hash="h" * 64)
        actual[table] = listing.column_names
    assert _documented("wyscout") == actual
    assert {name: [c for c, _ in columns] for name, _, columns in ws_docs()} == actual


def test_pining_columns_documented() -> None:
    folder = ROOT / "tests" / "fixtures" / "pining"
    entry = json.loads((folder / "idsse_matches_public.json").read_bytes())["matches"][0]
    player = json.loads((folder / "skillcorner_players_public.json").read_bytes())["players"][0]
    ids = {"idsse": "DFL-MAT-J03WPY", "skillcorner": "1899585", "statsbomb": "3788741", "gradientsports": "3812"}
    player_ids = {"idsse": "DFL-OBJ-0000XT", "skillcorner": "1000644", "statsbomb": "5503", "gradientsports": "1234"}
    actual: dict[str, list[str]] = {}
    for provider in ("gradientsports", "idsse", "skillcorner", "statsbomb"):
        parser = PiningListingParser(provider)
        matches = {"provider": provider, "matches": [{**entry, "id": ids[provider], "format_version": 2}]}
        players = {"provider": provider, "players": [{**player, "id": player_ids[provider], "height": 180.0}]}
        for kind, payload in (("matches", matches), ("players", players)):
            table = parser.listing(kind, json.dumps(payload).encode(), run_id="r", source_hash="h" * 64)  # type: ignore[arg-type]
            actual[PINING_TABLES[(provider, kind)]] = table.column_names
    assert _documented("pining") == actual
    assert {name: [c for c, _ in columns] for name, _, columns in pining_docs()} == actual
