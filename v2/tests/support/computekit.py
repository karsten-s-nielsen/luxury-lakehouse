"""The five providers' fixture adapters for e2e runs on a local lake (moved from tests/e2e/test_run_local.py).

Real parsers read the repository's fixtures, served by fake adapters (no network), with their source-level listings.
`fixture_adapters()` gives one adapter per provider; `gradientsports_tracking` and `statsbomb_events` let a test add
records (for example extra-time frames, or an event with a known clock) to those bytes in memory, without changing
the fixture files.
"""

from __future__ import annotations

import bz2
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from runkit import FakeAdapter, ListingAdapter, entry

from lakehouse.config.model import ModuleId
from lakehouse.ids.native import Provider
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.parsers.wyscout import WyscoutParser

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
PARSERS: dict[Provider, Any] = {
    "statsbomb": StatsBombParser(),
    "idsse": IdsseParser(),
    "skillcorner": SkillCornerParser(),
    "wyscout": WyscoutParser(),
    "gradientsports": GradientSportsParser(),
}
WYSCOUT_LISTINGS = {
    "competitions": "competitions.json",
    "teams": "teams.json",
    "players": "players_slice.json",
    "coaches": "coaches_slice.json",
    "event_names": "eventid2name.csv",
    "tag_names": "tags2name.csv",
}


def read_fixture(folder: str, name: str) -> bytes:
    return (FIXTURES / folder / name).read_bytes()


def fixture_adapter(
    module: ModuleId,
    provider: Provider,
    native: str,
    files: Mapping[str, bytes],
    *,
    competition: tuple[str, str],
    listings: Mapping[str, bytes],
    has_360: bool | None = None,
) -> ListingAdapter:
    listed = entry(
        module,
        native,
        signals=dict.fromkeys(files, "s1"),
        competition=competition,
        has_360=has_360,  # type: ignore[arg-type]
    )
    raw = {(native, artifact): data for artifact, data in files.items()}
    return ListingAdapter(module, provider, "public", [listed], raw, listings=listings)


def fixture_adapters(
    *,
    gradientsports_tracking: bytes | None = None,
    statsbomb_events: bytes | None = None,
    skillcorner_match: bytes | None = None,
) -> dict[ModuleId, FakeAdapter]:
    """One fixture adapter per provider. `gradientsports_tracking` replaces the (uncompressed) GS tracking bytes;
    `statsbomb_events` replaces the statsbomb events bytes; `skillcorner_match` replaces the skillcorner match file."""
    tracking = (
        gradientsports_tracking
        if gradientsports_tracking is not None
        else read_fixture("gradientsports", "tracking_9001.jsonl")
    )
    statsbomb = fixture_adapter(
        "statsbomb_public",
        "statsbomb",
        "9100001",
        {
            "events": statsbomb_events
            if statsbomb_events is not None
            else read_fixture("statsbomb", "events_9100001.json"),
            "lineups": read_fixture("statsbomb", "lineups_9100001.json"),
            "three_sixty": read_fixture("statsbomb", "three_sixty_9100001.json"),
        },
        competition=("43", "106"),
        listings={
            "competitions": read_fixture("statsbomb", "competitions.json"),
            "matches/43/106": read_fixture("statsbomb", "matches_43_106.json"),
        },
        has_360=True,
    )
    idsse = fixture_adapter(
        "idsse_public",
        "idsse",
        "J03WPY",
        {
            "metadata": read_fixture("idsse", "matchinformation_J03WPY.xml"),
            "events": read_fixture("idsse", "events_J03WPY_slice.xml"),
            "positions": read_fixture("idsse", "positions_J03WPY_slice.xml"),
        },
        competition=("DFL-COM-000002", "DFL-SEA-0001K6"),
        listings={},
    )
    skillcorner = fixture_adapter(
        "skillcorner_public",
        "skillcorner",
        "1899585",
        {
            "match": skillcorner_match
            if skillcorner_match is not None
            else read_fixture("skillcorner", "match_1899585.json"),
            "tracking": read_fixture("skillcorner", "tracking_1899585_slice.jsonl"),
            "dynamic_events": read_fixture("skillcorner", "dynamic_events_1899585_slice.csv"),
            "phases_of_play": read_fixture("skillcorner", "phases_of_play_1899585_slice.csv"),
        },
        competition=("61", "95"),
        listings={"matches": read_fixture("skillcorner", "matches.json")},
    )
    wyscout = fixture_adapter(
        "wyscout_public",
        "wyscout",
        "2058017",
        {
            "matches": read_fixture("wyscout", "matches_World_Cup_slice.json"),
            "events": read_fixture("wyscout", "events_World_Cup_slice.json"),
        },
        competition=("28", "10078"),
        listings={kind: read_fixture("wyscout", name) for kind, name in WYSCOUT_LISTINGS.items()},
    )
    gradientsports = fixture_adapter(
        "gradientsports_public",
        "gradientsports",
        "9001",
        {
            "metadata": read_fixture("gradientsports", "metadata_9001.json"),
            "roster": read_fixture("gradientsports", "roster_9001.json"),
            "events": read_fixture("gradientsports", "events_9001.json"),
            "tracking": bz2.compress(tracking),  # as the Drive folder delivers it
        },
        competition=("77", "2030"),
        listings={
            "competitions": read_fixture("gradientsports", "competitions.csv"),
            "players": read_fixture("gradientsports", "players.csv"),
        },
    )
    return {a.module_id: a for a in (statsbomb, idsse, skillcorner, wyscout, gradientsports)}
