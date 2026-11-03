"""SkillCorner test helpers: the 1899585 fixture deliveries and a parse with changed files (plan Task 17)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from lakehouse.entries import SourceEntry
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.raw.archive import Delivery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "skillcorner"
NOW = dt.datetime(2026, 10, 27, 12, tzinfo=dt.UTC)
FILES = {
    "match": "match_1899585.json",
    "tracking": "tracking_1899585_slice.jsonl",
    "dynamic_events": "dynamic_events_1899585_slice.csv",
    "phases_of_play": "phases_of_play_1899585_slice.csv",
}
ENTRY = SourceEntry(
    module_id="skillcorner_public",
    native_match_id="1899585",
    competition_id="61",
    competition_name="A-League",
    season_id="95",
    season_name="2024/2025",
    visibility="public",
    artifacts={},
    listed_at=NOW,
)


def fixture_bytes(artifact: str) -> bytes:
    return (FIXTURES / FILES[artifact]).read_bytes()


def delivery(artifact: str, raw: bytes | None = None) -> Delivery:
    return Delivery.received(
        provider="skillcorner",
        channel="public",
        native_match_id="1899585",
        artifact=artifact,
        raw=fixture_bytes(artifact) if raw is None else raw,
        visibility="public",
        change_signal=None,
        fetched_at=NOW,
    )


def parse_with(**changed: bytes) -> ParsedMatch:
    """Parse the four 1899585 files, with the given artifacts replaced by other bytes."""
    deliveries = {artifact: delivery(artifact, changed.get(artifact)) for artifact in FILES}
    return SkillCornerParser().parse(ENTRY, deliveries, run_id="run-1")
