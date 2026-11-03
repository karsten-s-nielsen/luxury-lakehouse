"""StatsBomb live tier (marker `live`): the proof of field completeness for the synthetic fixtures (spec §12.2)."""

from __future__ import annotations

import os

import pytest

from lakehouse.config.model import CompetitionClause, ModuleConfig
from lakehouse.net.http import HttpClient
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.github import GitHubClient
from lakehouse.sources.statsbomb_public import StatsBombPublicAdapter

pytestmark = pytest.mark.live

DEFAULTS = {
    ("9", "281"): ("1. Bundesliga", "2023/2024"),
    ("43", "3"): ("FIFA World Cup", "2018"),
    ("43", "106"): ("FIFA World Cup", "2022"),
}
MATCHES_PER_SEASON = 2


@pytest.fixture(scope="module")
def adapter() -> StatsBombPublicAdapter:
    module = ModuleConfig(
        module_id="statsbomb_public",
        active=True,
        filter=[CompetitionClause(competition_id=c, seasons=[s]) for c, s in DEFAULTS],
    )
    github = GitHubClient(HttpClient(), os.environ.get("GITHUB_TOKEN"))
    return StatsBombPublicAdapter(github, module, MemoryCache())


def test_defaults_ids_match_live(adapter: StatsBombPublicAdapter) -> None:
    tournaments = {(t.competition_id, t.season_id): t for t in adapter.preview().tournaments}
    for key, (competition, season) in DEFAULTS.items():
        assert (tournaments[key].competition_name, tournaments[key].season_name) == (competition, season)
    assert tournaments[("43", "3")].competition_available_360 is None  # WC 2018 has no 360 data (§8.2)
    assert tournaments[("43", "106")].competition_available_360 is not None
    assert tournaments[("9", "281")].competition_available_360 is not None


def test_live_inventory(adapter: StatsBombPublicAdapter) -> None:
    parser = StatsBombParser()
    entries = list(adapter.list_entries())
    by_season: dict[tuple[str | None, str | None], int] = {}
    checked: dict[str, int] = {}
    for entry in entries:
        season = (entry.competition_id, entry.season_id)
        if by_season.get(season, 0) >= MATCHES_PER_SEASON:
            continue
        by_season[season] = by_season.get(season, 0) + 1
        for artifact in entry.artifacts:
            delivery = adapter.fetch(entry, artifact)
            inventory = parser.field_inventory(artifact, delivery.decoded)
            assert unmapped_fields(inventory, parser.bronze_columns(artifact)) == [], (entry.native_match_id, artifact)
            parser.table(artifact, delivery.decoded)  # every value converts exactly
            checked[artifact] = checked.get(artifact, 0) + 1
    assert set(by_season) == set(DEFAULTS)
    assert checked["events"] == len(DEFAULTS) * MATCHES_PER_SEASON
    assert checked.get("three_sixty", 0) >= 2  # the two seasons with 360 data
