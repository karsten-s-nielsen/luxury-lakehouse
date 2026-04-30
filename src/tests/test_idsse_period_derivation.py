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

    def test_pass_one_handles_production_timestamp_format(self, tmp_path: Path) -> None:
        """Production DFL XML uses milliseconds + non-UTC offsets like '+02:00'.

        Regression guard for the regex-based pass-1 scan: the byte regex must
        capture the whole ISO-8601 value including the fractional-second and
        timezone-offset suffix, and ``datetime.fromisoformat`` must convert
        the local-time offset to UTC.
        """
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <EventList>
    <Event MatchId="DFL-MAT-PROD" EventId="e1" EventTime="2023-05-27T15:30:00.123+02:00">
      <KickOff GameSection="firstHalf"/>
    </Event>
    <Event MatchId="DFL-MAT-PROD" EventId="e2" EventTime="2023-05-27T16:34:56.789+02:00">
      <KickOff GameSection="secondHalf"/>
    </Event>
  </EventList>
</PutDataRequest>
"""
        fixture = tmp_path / "prod.xml"
        fixture.write_bytes(xml)
        result = _scan_kickoff_times(str(fixture))
        assert set(result.keys()) == {1, 2}
        # +02:00 → UTC: 15:30:00 → 13:30:00, 16:34:56 → 14:34:56.
        assert result[1].isoformat().startswith("2023-05-27T13:30:00")
        assert result[2].isoformat().startswith("2023-05-27T14:34:56")

    def test_pass_one_tolerates_arbitrary_event_attribute_order(self, tmp_path: Path) -> None:
        """``EventTime`` may not be the last attribute on the ``<Event>`` open tag.

        The lazy ``[^>]*?`` patterns in ``_KICKOFF_REGEX`` allow EventTime
        anywhere in the attribute list. Regression guard for any future
        attempt to anchor the EventTime capture.
        """
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <EventList>
    <Event EventTime="2023-05-27T15:30:00+00:00" MatchId="DFL-MAT-PROD" EventId="e1">
      <KickOff Foo="bar" GameSection="firstHalf"/>
    </Event>
  </EventList>
</PutDataRequest>
"""
        fixture = tmp_path / "reorder.xml"
        fixture.write_bytes(xml)
        result = _scan_kickoff_times(str(fixture))
        assert set(result.keys()) == {1}
        assert result[1].isoformat().startswith("2023-05-27T15:30:00")

    def test_pass_one_returns_empty_when_no_kickoffs(self, tmp_path: Path) -> None:
        """Empty dict for inputs with no recognized KickOffs."""
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <EventList>
    <Event MatchId="DFL-MAT-PROD" EventId="e1" EventTime="2023-05-27T15:30:00+00:00">
      <TacklingGame Winner="p1" WinnerTeam="DFL-CLU-A"/>
    </Event>
  </EventList>
</PutDataRequest>
"""
        fixture = tmp_path / "no_kickoff.xml"
        fixture.write_bytes(xml)
        assert _scan_kickoff_times(str(fixture)) == {}


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
