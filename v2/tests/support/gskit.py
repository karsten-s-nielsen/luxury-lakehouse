"""Gradient Sports test helpers: the synthetic match 9001 deliveries and a parse with changed files (plan Task 18)."""

from __future__ import annotations

import bz2
import datetime as dt
from pathlib import Path

from lakehouse.entries import SourceEntry
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.raw.archive import Delivery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gradientsports"
NOW = dt.datetime(2026, 10, 27, 12, tzinfo=dt.UTC)
FILES = {
    "metadata": "metadata_9001.json",
    "roster": "roster_9001.json",
    "events": "events_9001.json",
    "tracking": "tracking_9001.jsonl",
}
ENTRY = SourceEntry(
    module_id="gradientsports_public",
    native_match_id="9001",
    competition_id="77",
    competition_name="Invented Cup",
    season_id="2030",
    season_name="2030",
    visibility="public",
    artifacts={},
    listed_at=NOW,
)


def fixture_bytes(artifact: str) -> bytes:
    return (FIXTURES / FILES[artifact]).read_bytes()


def delivery(artifact: str, raw: bytes | None = None) -> Delivery:
    """A delivery of one fixture file; tracking arrives bz2-wrapped, as the Drive folder delivers it."""
    data = fixture_bytes(artifact) if raw is None else raw
    if artifact == "tracking":
        data = bz2.compress(data)
    return Delivery.received(
        provider="gradientsports",
        channel="public",
        native_match_id="9001",
        artifact=artifact,
        raw=data,
        visibility="public",
        change_signal=None,
        fetched_at=NOW,
    )


def parse_with(**changed: bytes) -> ParsedMatch:
    """Parse the four 9001 files, with the given artifacts replaced by other (decoded) bytes."""
    deliveries = {artifact: delivery(artifact, changed.get(artifact)) for artifact in FILES}
    return GradientSportsParser().parse(ENTRY, deliveries, run_id="run-1")
