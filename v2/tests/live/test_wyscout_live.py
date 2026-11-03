"""Wyscout live tier (marker `live`): figshare collection 4415000, the proof of field completeness (spec §12.2).
The listing reads matches.zip (0.6 MB) and the listing files (2 MB in all). events.zip (77 MB, members up to a few
hundred MB once extracted) is read only by the test that also carries the marker `heavy`, which runs on the Mac Mini."""

from __future__ import annotations

import pytest

from lakehouse.config.model import AllClause, CompetitionClause, ModuleConfig, load_defaults
from lakehouse.net.http import HttpClient
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.parsers.wyscout import EVENTS, MATCH_FORMATIONS, MATCHES, WyscoutParser
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.wyscout_public import WyscoutPublicAdapter

pytestmark = pytest.mark.live

MATCH_COUNT = 1941  # every member of matches.zip, measured 2026-10-27
WORLD_CUP_MATCHES = 64
LISTINGS = ("competitions", "teams", "players", "coaches", "event_names", "tag_names")


def _adapter(module: ModuleConfig) -> WyscoutPublicAdapter:
    return WyscoutPublicAdapter(HttpClient(), module, MemoryCache())


@pytest.fixture(scope="module")
def adapter() -> WyscoutPublicAdapter:
    return _adapter(ModuleConfig(module_id="wyscout_public", active=True, filter=[AllClause()]))


def test_wc2018_competition_id_live() -> None:
    """The defaults' Wyscout clause (competition 28, every season) selects exactly the 64 matches of the 2018 World
    Cup, the only edition in the dataset (plan Task 6, spec §8.2)."""
    module = load_defaults("laptop").module("wyscout_public")
    assert module.filter == [CompetitionClause(competition_id="28", seasons="all")]
    entries = list(_adapter(module).list_entries())
    assert len(entries) == WORLD_CUP_MATCHES
    assert {(e.competition_id, e.competition_name, e.season_id, e.season_name) for e in entries} == {
        ("28", "World Cup", "10078", "2018")
    }


def test_live_listing(adapter: WyscoutPublicAdapter) -> None:
    entries = list(adapter.list_entries())
    assert len(entries) == MATCH_COUNT
    assert adapter.warnings == []
    assert len({e.native_match_id for e in entries}) == MATCH_COUNT
    preview = adapter.preview()
    assert sum(t.match_count or 0 for t in preview.tournaments) == MATCH_COUNT
    assert len(preview.tournaments) == 7


def test_live_inventory(adapter: WyscoutPublicAdapter) -> None:
    """Every member of matches.zip and every listing file; each World Cup match parses."""
    parser = WyscoutParser()
    members: dict[str, bytes] = {}
    for entry in adapter.list_entries():
        key = entry.artifacts["matches"].key
        if key not in members:
            members[key] = adapter.fetch(entry, "matches").decoded
            inventory = parser.field_inventory("matches", members[key])
            assert unmapped_fields(inventory, parser.bronze_columns("matches")) == [], key
        if entry.competition_id == "28":
            parsed = parser.parse(entry, {"matches": adapter.fetch(entry, "matches")}, run_id="live")
            assert parsed.tables[MATCHES].num_rows == 1
            assert parsed.tables[MATCH_FORMATIONS].num_rows > 0
    assert len(members) == 7
    for kind in LISTINGS:
        delivery = adapter.fetch_listing(kind)  # type: ignore[arg-type]
        inventory = parser.field_inventory(kind, delivery.decoded)
        assert unmapped_fields(inventory, parser.bronze_columns(kind)) == [], kind
        assert parser.listing(kind, delivery.decoded, run_id="live", source_hash=delivery.content_sha256).num_rows > 0  # type: ignore[arg-type]


@pytest.mark.heavy
def test_live_events_inventory_and_parse(adapter: WyscoutPublicAdapter) -> None:
    """Every member of events.zip; each World Cup match parses with its events."""
    parser = WyscoutParser()
    seen: set[str] = set()
    for entry in adapter.list_entries():
        key = entry.artifacts["events"].key
        if key not in seen:
            seen.add(key)
            inventory = parser.field_inventory("events", adapter.fetch(entry, "events").decoded)
            assert unmapped_fields(inventory, parser.bronze_columns("events")) == [], key
        if entry.competition_id == "28":
            deliveries = {artifact: adapter.fetch(entry, artifact) for artifact in ("matches", "events")}
            assert parser.parse(entry, deliveries, run_id="live").tables[EVENTS].num_rows > 0
    assert len(seen) == 7
