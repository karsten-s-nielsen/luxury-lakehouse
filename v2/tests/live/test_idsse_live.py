"""IDSSE live tier (marker `live`): HF `pysport/idsse-data`, the proof of field completeness on all 7 matches (spec
§12.2). The positions files (about 370 MB each) are read only by the tests that also carry the marker `heavy`, which
run on the Mac Mini only."""

from __future__ import annotations

import pytest

from lakehouse.config.model import AllClause, ModuleConfig
from lakehouse.entries import SourceEntry
from lakehouse.net.http import HttpClient
from lakehouse.parsers.idsse import TRACKING_FRAMES, TRACKING_PLAYERS, IdsseParser
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.idsse_public import IdssePublicAdapter

pytestmark = pytest.mark.live

# Recorded on 2026-10-27 from each match's matchinformation file.
COMPETITIONS = {
    "J03WMX": ("DFL-COM-000001", "Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
    "J03WN1": ("DFL-COM-000001", "Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
    "J03WOH": ("DFL-COM-000002", "2. Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
    "J03WOY": ("DFL-COM-000002", "2. Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
    "J03WPY": ("DFL-COM-000002", "2. Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
    "J03WQQ": ("DFL-COM-000002", "2. Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
    "J03WR9": ("DFL-COM-000002", "2. Bundesliga", "DFL-SEA-0001K6", "2022/2023"),
}
# J03WPY in the spike data (spec §25): 3,216,642 player rows and 146,211 frames.
J03WPY_PLAYER_ROWS = 3_216_642
J03WPY_FRAMES = 146_211


@pytest.fixture(scope="module")
def adapter() -> IdssePublicAdapter:
    module = ModuleConfig(module_id="idsse_public", active=True, filter=[AllClause()])
    return IdssePublicAdapter(HttpClient(), module, MemoryCache())


@pytest.fixture(scope="module")
def entries(adapter: IdssePublicAdapter) -> list[SourceEntry]:
    return list(adapter.list_entries())


def test_list_all_seven_competitions(adapter: IdssePublicAdapter, entries: list[SourceEntry]) -> None:
    found = {e.native_match_id: (e.competition_id, e.competition_name, e.season_id, e.season_name) for e in entries}
    assert found == COMPETITIONS
    assert adapter.warnings == []
    for entry in entries:
        assert set(entry.artifacts) == {"metadata", "events", "positions"}
        assert entry.artifacts["positions"].expected_sha256 is not None  # Git LFS: the SHA-256 is the change signal


def test_live_inventory(adapter: IdssePublicAdapter, entries: list[SourceEntry]) -> None:
    parser = IdsseParser()
    for entry in entries:
        for artifact in ("metadata", "events"):
            inventory = parser.field_inventory(artifact, adapter.fetch(entry, artifact).decoded)
            assert unmapped_fields(inventory, parser.bronze_columns(artifact)) == [], (entry.native_match_id, artifact)


@pytest.mark.heavy
def test_live_positions_inventory(adapter: IdssePublicAdapter, entries: list[SourceEntry]) -> None:
    parser = IdsseParser()
    for entry in entries:
        inventory = parser.field_inventory("positions", adapter.fetch(entry, "positions").decoded)
        assert unmapped_fields(inventory, parser.bronze_columns("positions")) == [], entry.native_match_id


@pytest.mark.heavy
def test_live_full_parse_j03wpy(adapter: IdssePublicAdapter, entries: list[SourceEntry]) -> None:
    entry = next(e for e in entries if e.native_match_id == "J03WPY")
    deliveries = {artifact: adapter.fetch(entry, artifact) for artifact in ("metadata", "events", "positions")}
    parsed = IdsseParser().parse(entry, deliveries, run_id="live")
    assert parsed.tables[TRACKING_PLAYERS].num_rows == J03WPY_PLAYER_ROWS
    assert parsed.tables[TRACKING_FRAMES].num_rows == J03WPY_FRAMES
