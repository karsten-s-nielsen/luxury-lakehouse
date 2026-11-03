"""The bronze tables that the parsers give for the repository's fixtures, loaded into an in-memory `lake` catalog
(plan Task 22): the resolution queries run on the real bronze schemas, not on hand-made tables."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
from gskit import parse_with as parse_gs
from idssekit import parse_with as parse_idsse
from skillcornerkit import parse_with as parse_skillcorner

from lakehouse.bronze.schemas import BronzeTable
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.ids.native import Provider
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.parsers.wyscout import LISTING_TABLES as WYSCOUT_LISTINGS
from lakehouse.parsers.wyscout import WyscoutParser
from lakehouse.raw.archive import Delivery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
_HASH = "h" * 64
_WYSCOUT_FILES = {
    "competitions": "competitions.json",
    "teams": "teams.json",
    "players": "players_slice.json",
    "coaches": "coaches_slice.json",
    "event_names": "eventid2name.csv",
    "tag_names": "tags2name.csv",
}


def _named(tables: Mapping[BronzeTable, pa.Table]) -> dict[str, pa.Table]:
    return {str(name): table for name, table in tables.items()}


def _delivery(provider: Provider, native: str, artifact: str, path: Path) -> Delivery:
    return Delivery.received(
        provider=provider,
        channel="public",
        native_match_id=native,
        artifact=artifact,
        raw=path.read_bytes(),
        visibility="public",
        change_signal=None,
        fetched_at=datetime.now(UTC),
    )


def _entry(module_id: str, native: str, competition: str, season: str) -> SourceEntry:
    return SourceEntry(
        module_id=module_id,  # type: ignore[arg-type]
        native_match_id=native,
        competition_id=competition,
        competition_name=None,
        season_id=season,
        season_name=None,
        visibility="public",
        artifacts={"events": ArtifactRef(key="e", change_signal=None, expected_sha256=None, size=None)},
        listed_at=datetime.now(UTC),
    )


def statsbomb_tables() -> dict[str, pa.Table]:
    folder = FIXTURES / "statsbomb"
    parser = StatsBombParser()
    deliveries = {
        artifact: _delivery("statsbomb", "9100001", artifact, folder / name)
        for artifact, name in (
            ("events", "events_9100001.json"),
            ("lineups", "lineups_9100001.json"),
            ("metadata", "commercial_metadata_9100001.json"),
        )
    }
    tables: dict[str, pa.Table] = _named(
        parser.parse(_entry("statsbomb_public", "9100001", "43", "106"), deliveries, run_id="r").tables
    )
    tables["statsbomb_matches"] = parser.listing(
        "matches/43/106", (folder / "matches_43_106.json").read_bytes(), run_id="r", source_hash=_HASH
    )
    tables["statsbomb_competitions"] = parser.listing(
        "competitions", (folder / "competitions.json").read_bytes(), run_id="r", source_hash=_HASH
    )
    return tables


def wyscout_tables() -> dict[str, pa.Table]:
    folder = FIXTURES / "wyscout"
    parser = WyscoutParser()
    deliveries = {
        "matches": _delivery("wyscout", "2058017", "matches", folder / "matches_World_Cup_slice.json"),
        "events": _delivery("wyscout", "2058017", "events", folder / "events_World_Cup_slice.json"),
    }
    tables = _named(parser.parse(_entry("wyscout_public", "2058017", "28", "10078"), deliveries, run_id="r").tables)
    for kind, table in WYSCOUT_LISTINGS.items():
        tables[table] = parser.listing(
            kind, (folder / _WYSCOUT_FILES[kind]).read_bytes(), run_id="r", source_hash=_HASH
        )
    return tables


def skillcorner_tables() -> dict[str, pa.Table]:
    tables = _named(parse_skillcorner().tables)
    tables["skillcorner_match_list"] = SkillCornerParser().listing(
        "matches", (FIXTURES / "skillcorner" / "matches.json").read_bytes(), run_id="r", source_hash=_HASH
    )
    return tables


def gradientsports_tables() -> dict[str, pa.Table]:
    folder = FIXTURES / "gradientsports"
    parser = GradientSportsParser()
    tables: dict[str, pa.Table] = _named(parse_gs().tables)
    for kind in ("competitions", "players"):
        listing = parser.listing(kind, (folder / f"{kind}.csv").read_bytes(), run_id="r", source_hash=_HASH)
        tables[str(parser.listing_table(kind))] = listing
    return tables


def fixture_lake() -> duckdb.DuckDBPyConnection:
    """An in-memory `lake` catalog with every fixture bronze table of the five providers (tracking tables left out:
    resolution does not read them)."""
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS lake")
    tables = {
        **statsbomb_tables(),
        **wyscout_tables(),
        **_named(parse_idsse().tables),
        **skillcorner_tables(),
        **gradientsports_tables(),
    }
    for name, table in sorted(tables.items()):
        if "tracking" in name:
            continue
        conn.register("_incoming", table)
        conn.execute(f'CREATE TABLE lake.main."{name}" AS SELECT * FROM _incoming')  # noqa: S608 - a parser table name
        conn.unregister("_incoming")
    return conn
