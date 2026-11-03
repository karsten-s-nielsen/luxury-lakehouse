"""IDSSE test helpers: the J03WPY fixture deliveries and a parse with changed files (plan Task 16)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from lakehouse.entries import SourceEntry
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.raw.archive import Delivery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "idsse"
NOW = dt.datetime(2026, 10, 27, 12, tzinfo=dt.UTC)
FILES_BY_ARTIFACT = {
    "metadata": "matchinformation_J03WPY.xml",
    "events": "events_J03WPY_slice.xml",
    "positions": "positions_J03WPY_slice.xml",
}
ENTRY = SourceEntry(
    module_id="idsse_public",
    native_match_id="J03WPY",
    competition_id="DFL-COM-000002",
    competition_name="2. Bundesliga",
    season_id="DFL-SEA-0001K6",
    season_name="2022/2023",
    visibility="public",
    artifacts={},
    listed_at=NOW,
)


def fixture_bytes(artifact: str) -> bytes:
    return (FIXTURES / FILES_BY_ARTIFACT[artifact]).read_bytes()


def delivery(artifact: str, raw: bytes | None = None) -> Delivery:
    return Delivery.received(
        provider="idsse",
        channel="public",
        native_match_id="J03WPY",
        artifact=artifact,
        raw=fixture_bytes(artifact) if raw is None else raw,
        visibility="public",
        change_signal=None,
        fetched_at=NOW,
    )


def parse_with(**changed: bytes) -> ParsedMatch:
    """Parse the three J03WPY files, with the given artifacts replaced by other bytes."""
    deliveries = {artifact: delivery(artifact, changed.get(artifact)) for artifact in FILES_BY_ARTIFACT}
    return IdsseParser().parse(ENTRY, deliveries, run_id="run-1")
