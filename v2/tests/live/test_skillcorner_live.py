"""SkillCorner live tier (marker `live`): GitHub SkillCorner/opendata, the proof of field completeness on all 20
matches (spec §12.2). The tracking files (about 85 MB each, Git LFS, SkillCorner's bandwidth quota) are read only by
the tests that also carry the marker `heavy`, which run on the Mac Mini only."""

from __future__ import annotations

import os

import pytest

from lakehouse.config.model import AllClause, ModuleConfig
from lakehouse.entries import SourceEntry
from lakehouse.net.http import HttpClient
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.parsers.skillcorner import TRACKING_FRAMES, TRACKING_PLAYERS, SkillCornerParser
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.github import GitHubClient
from lakehouse.sources.skillcorner_public import SkillCornerPublicAdapter

pytestmark = pytest.mark.live

MATCH_COUNT = 20  # A-League 2024/25 (spec §7.1)
# 1899585 in the spike data (spec §25): 894,982 player rows and 60,530 frames.
SPIKE_PLAYER_ROWS = 894_982
SPIKE_FRAMES = 60_530


@pytest.fixture(scope="module")
def adapter() -> SkillCornerPublicAdapter:
    module = ModuleConfig(module_id="skillcorner_public", active=True, filter=[AllClause()])
    return SkillCornerPublicAdapter(GitHubClient(HttpClient(), os.environ.get("GITHUB_TOKEN")), module, MemoryCache())


@pytest.fixture(scope="module")
def entries(adapter: SkillCornerPublicAdapter) -> list[SourceEntry]:
    return list(adapter.list_entries())


def test_list_all_twenty_matches(adapter: SkillCornerPublicAdapter, entries: list[SourceEntry]) -> None:
    assert len(entries) == MATCH_COUNT
    assert {(e.competition_id, e.competition_name, e.season_id, e.season_name) for e in entries} == {
        ("61", "A-League", "95", "2024/2025")
    }
    assert all(set(e.artifacts) == {"match", "tracking", "dynamic_events", "phases_of_play"} for e in entries)
    assert all(e.artifacts["tracking"].expected_sha256 for e in entries)  # every tracking file is an LFS object
    assert not any("data/matches/" in w for w in adapter.warnings)  # every match file is a known kind


def test_live_inventory(adapter: SkillCornerPublicAdapter, entries: list[SourceEntry]) -> None:
    parser = SkillCornerParser()
    for entry in entries:
        for artifact in ("match", "dynamic_events", "phases_of_play"):
            inventory = parser.field_inventory(artifact, adapter.fetch(entry, artifact).decoded)
            assert unmapped_fields(inventory, parser.bronze_columns(artifact)) == [], (entry.native_match_id, artifact)


@pytest.mark.heavy
def test_live_tracking_inventory(adapter: SkillCornerPublicAdapter, entries: list[SourceEntry]) -> None:
    parser = SkillCornerParser()
    for entry in entries:
        inventory = parser.field_inventory("tracking", adapter.fetch(entry, "tracking").decoded)
        assert unmapped_fields(inventory, parser.bronze_columns("tracking")) == [], entry.native_match_id


@pytest.mark.heavy
def test_live_full_parse_1899585(adapter: SkillCornerPublicAdapter, entries: list[SourceEntry]) -> None:
    entry = next(e for e in entries if e.native_match_id == "1899585")
    deliveries = {artifact: adapter.fetch(entry, artifact) for artifact in entry.artifacts}
    parsed = SkillCornerParser().parse(entry, deliveries, run_id="live")
    assert parsed.tables[TRACKING_PLAYERS].num_rows == SPIKE_PLAYER_ROWS
    assert parsed.tables[TRACKING_FRAMES].num_rows == SPIKE_FRAMES
