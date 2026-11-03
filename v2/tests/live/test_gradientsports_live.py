"""Gradient Sports live tier (marker `live`): the public Drive folder, the proof of field completeness for the synthetic
fixtures (spec §12.2). The event files (about 16 MB each, about 1 GB in all) and the tracking files (about 50 MB each,
bz2) are read only by the tests that also carry the marker `heavy`, which run on the Mac Mini only."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from lakehouse.config.model import AllClause, ModuleConfig
from lakehouse.entries import SourceEntry
from lakehouse.net.http import HttpClient
from lakehouse.parsers.gradientsports import TRACKING_FRAMES, TRACKING_PLAYERS, GradientSportsParser
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.gradientsports_public import GradientSportsPublicAdapter

pytestmark = pytest.mark.live

MATCH_COUNT = 64  # FIFA Men's World Cup 2022


@pytest.fixture(scope="module")
def adapter() -> GradientSportsPublicAdapter:
    module = ModuleConfig(module_id="gradientsports_public", active=True, filter=[AllClause()])
    return GradientSportsPublicAdapter(HttpClient(), module, MemoryCache())


@pytest.fixture(scope="module")
def entries(adapter: GradientSportsPublicAdapter) -> list[SourceEntry]:
    return list(adapter.list_entries())


def test_drive_folder_listing(
    adapter: GradientSportsPublicAdapter, entries: list[SourceEntry], record_property: Callable[[str, object], None]
) -> None:
    """§26 item 8: the file count and total size of the folder (the listing gives no sizes)."""
    assert len(entries) == MATCH_COUNT
    assert adapter.warnings == []
    assert {(e.competition_id, e.season_id) for e in entries} == {("38", "2022")}
    sizes: dict[str, int] = {}
    for artifact in ("metadata", "roster", "events", "tracking"):
        sizes[artifact] = sum(adapter.content_length(e.artifacts[artifact].key) for e in entries)
    record_property("gradientsports_bytes", sizes)
    print(f"GS Drive folder: {MATCH_COUNT} matches x 4 files; bytes per artifact {sizes}; total {sum(sizes.values())}")
    assert all(size > 0 for size in sizes.values())


def test_live_inventory(adapter: GradientSportsPublicAdapter, entries: list[SourceEntry]) -> None:
    parser = GradientSportsParser()
    for entry in entries:
        for artifact in ("metadata", "roster"):
            inventory = parser.field_inventory(artifact, adapter.fetch(entry, artifact).decoded)
            assert unmapped_fields(inventory, parser.bronze_columns(artifact)) == [], (entry.native_match_id, artifact)


@pytest.mark.heavy
def test_live_events_inventory(adapter: GradientSportsPublicAdapter, entries: list[SourceEntry]) -> None:
    parser = GradientSportsParser()
    for entry in entries:
        inventory = parser.field_inventory("events", adapter.fetch(entry, "events").decoded)
        assert unmapped_fields(inventory, parser.bronze_columns("events")) == [], entry.native_match_id


@pytest.mark.heavy
def test_live_tracking_inventory_and_parse(adapter: GradientSportsPublicAdapter, entries: list[SourceEntry]) -> None:
    parser = GradientSportsParser()
    for entry in entries:
        deliveries = {artifact: adapter.fetch(entry, artifact) for artifact in ("metadata", "roster", "tracking")}
        inventory = parser.field_inventory("tracking", deliveries["tracking"].decoded)
        assert unmapped_fields(inventory, parser.bronze_columns("tracking")) == [], entry.native_match_id
        parsed = parser.parse(entry, deliveries, run_id="live")
        assert parsed.tables[TRACKING_FRAMES].num_rows > 0
        assert parsed.tables[TRACKING_PLAYERS].num_rows > 0
