"""Bug #6 — IDSSE 2-pass parser tests (ADR-018 P2).

Pre-fix: state-machine ``current_period`` tags secondary-block events with
the period that was active when their stream-order position was
processed. Post-fix: per-event period derivation by event_time vs the
``{period: kickoff_time}`` map built in pass 1.

Synthetic XML fixture: ``src/tests/fixtures/idsse_interleaved_periods.xml``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from ingestion.idsse import _parse_events_xml, _scan_kickoff_times

_FIXTURE = Path(__file__).parent / "fixtures" / "idsse_interleaved_periods.xml"


def _to_df(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestKickoffScanPassOne:
    """Pass 1 — build {period: kickoff_event_time} map."""

    def test_pass_one_collects_both_kickoffs(self) -> None:
        result = _scan_kickoff_times(str(_FIXTURE))
        assert set(result.keys()) == {1, 2}
        # Both KickOff timestamps are in UTC (the source uses Z suffix);
        # _scan_kickoff_times normalises to UTC.
        assert result[1].isoformat().startswith("2026-01-01T15:00:00")
        assert result[2].isoformat().startswith("2026-01-01T16:00:00")


class TestSecondaryBlockEventGetsCorrectPeriod:
    """Pass 2 — events derive period from event_time, not stream-order."""

    def test_secondary_block_first_half_event_lands_in_period_1(self) -> None:
        rows = _parse_events_xml(
            str(_FIXTURE),
            player_team_map={
                "p1": "home",
                "p2": "away",
                "p3": "home",
                "p4": "home",
                "p5": "away",
            },
            match_id="TEST",
            logger=logging.getLogger("test"),
        )
        df = _to_df(rows)
        # Find the BallClaiming events from the secondary block (e6, e7) — they
        # appear AFTER the secondHalf KickOff in stream order but their event_times
        # are in the first half.
        e6 = df[df["event_id"] == "e6"]
        e7 = df[df["event_id"] == "e7"]
        assert not e6.empty, "e6 row missing from parsed output"
        assert not e7.empty, "e7 row missing from parsed output"
        assert e6["period"].iloc[0] == 1, (
            f"Bug #6: e6 (event_time 15:05 in first half) tagged period {e6['period'].iloc[0]} "
            "(expected 1) — 2-pass parser period derivation broken"
        )
        assert e7["period"].iloc[0] == 1
        # Their timestamp_seconds must be NON-NEGATIVE.
        assert e6["timestamp_seconds"].iloc[0] >= 0
        assert e7["timestamp_seconds"].iloc[0] >= 0

    def test_period_1_events_have_correct_timestamps(self) -> None:
        rows = _parse_events_xml(
            str(_FIXTURE),
            player_team_map={"p1": "home", "p2": "away"},
            match_id="TEST",
            logger=logging.getLogger("test"),
        )
        df = _to_df(rows)
        # KickOff at 15:00; e2 at 15:10 → 600s; e3 at 15:20 → 1200s
        e2 = df[df["event_id"] == "e2"]
        assert e2["period"].iloc[0] == 1
        assert e2["timestamp_seconds"].iloc[0] == pytest.approx(600.0, abs=1.0)

    def test_period_2_events_have_correct_timestamps(self) -> None:
        rows = _parse_events_xml(
            str(_FIXTURE),
            player_team_map={"p3": "home"},
            match_id="TEST",
            logger=logging.getLogger("test"),
        )
        df = _to_df(rows)
        # secondHalf KickOff at 16:00; e5 at 16:10 → 600s
        e5 = df[df["event_id"] == "e5"]
        assert e5["period"].iloc[0] == 2
        assert e5["timestamp_seconds"].iloc[0] == pytest.approx(600.0, abs=1.0)
