"""Tests for ingestion.idsse — IDSSE tracking and event data DFL XML parsing."""

from __future__ import annotations

import logging
import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from ingestion.idsse import IDSSE_MATCH_IDS, _parse_events_xml, _parse_positions_xml, _parse_teams, _smooth_tracking

_logger = logging.getLogger("test_idsse")

# Minimal DFL match info XML for testing (includes PlayingPosition for GK detection)
_MATCH_INFO_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <MatchInformation>
    <General MatchId="DFL-MAT-J03WMX" HomeTeamId="DFL-CLU-000008"
             GuestTeamId="DFL-CLU-00000G" />
    <Teams>
      <Team TeamId="DFL-CLU-000008" Role="home">
        <Players>
          <Player PersonId="H001" ShirtNumber="1" PlayingPosition="TW" FirstName="A" LastName="B" />
          <Player PersonId="H002" ShirtNumber="2" PlayingPosition="IV" FirstName="C" LastName="D" />
        </Players>
      </Team>
      <Team TeamId="DFL-CLU-00000G" Role="guest">
        <Players>
          <Player PersonId="A001" ShirtNumber="1" PlayingPosition="TW" FirstName="E" LastName="F" />
          <Player PersonId="A002" ShirtNumber="10" PlayingPosition="RA" FirstName="G" LastName="H" />
        </Players>
      </Team>
    </Teams>
  </MatchInformation>
</PutDataRequest>
"""

# Minimal DFL position XML for testing
_POSITIONS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="0" X="0.5" Y="1.0"/>
<Frame N="1" X="1.5" Y="2.0"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="0" X="-10.5" Y="5.2"/>
<Frame N="1" X="-10.0" Y="5.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H002">
<Frame N="0" X="20.0" Y="-15.0"/>
<Frame N="1" X="20.5" Y="-14.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-00000G" PersonId="A001">
<Frame N="0" X="30.0" Y="10.0"/>
<Frame N="1" X="30.5" Y="10.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="referee" PersonId="REF001">
<Frame N="0" X="0.0" Y="0.0"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="0" X="-5.0" Y="-3.0"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-00000G" PersonId="A002">
<Frame N="0" X="45.0" Y="25.0"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""


# Ball FrameSet appears AFTER player FrameSets — tests the ordering assumption (TD#26)
_POSITIONS_XML_BALL_LAST = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="0" X="-10.5" Y="5.2"/>
<Frame N="1" X="-10.0" Y="5.5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="0" X="0.5" Y="1.0"/>
<Frame N="1" X="1.5" Y="2.0"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""


# PR 1.6 regression fixture: DFL position XML with NON-ZERO period-start frame
# numbers. Real DFL data starts period 1 at frame ~10000 and period 2 at
# ~100000 (frame numbers are absolute across the match, not reset per period).
# Under the old absolute-timestamp logic (timestamp = n / 25), frame 10000
# → 400.0s, which never matches the event side's period-relative 0s timestamp.
# Under the fixed period-relative logic, frame 10000 → 0.0s.
_POSITIONS_XML_ABSOLUTE_FRAMES = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="10000" X="0.5" Y="1.0"/>
<Frame N="10001" X="1.5" Y="2.0"/>
<Frame N="10002" X="2.5" Y="3.0"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="10000" X="-10.5" Y="5.2"/>
<Frame N="10001" X="-10.0" Y="5.5"/>
<Frame N="10002" X="-9.5" Y="5.8"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="100000" X="0.0" Y="0.0"/>
<Frame N="100001" X="0.1" Y="0.1"/>
</FrameSet>
<FrameSet GameSection="secondHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="100000" X="-5.0" Y="0.0"/>
<Frame N="100001" X="-5.5" Y="0.5"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""


def _write_temp_xml(content: str) -> str:
    """Write XML content to a temporary file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".xml")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class TestParseTeams:
    """Tests for _parse_teams XML parsing."""

    def test_identifies_home_team(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            home_id, _away_id, _ptm, _gk = _parse_teams(path)
            assert home_id == "DFL-CLU-000008"
        finally:
            os.unlink(path)

    def test_identifies_away_team(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _home_id, away_id, _ptm, _gk = _parse_teams(path)
            assert away_id == "DFL-CLU-00000G"
        finally:
            os.unlink(path)

    def test_maps_players_to_teams(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _h, _a, ptm, _gk = _parse_teams(path)
            assert ptm["H001"] == "home"
            assert ptm["H002"] == "home"
            assert ptm["A001"] == "away"
            assert ptm["A002"] == "away"
        finally:
            os.unlink(path)

    def test_player_count(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _h, _a, ptm, _gk = _parse_teams(path)
            assert len(ptm) == 4
        finally:
            os.unlink(path)

    def test_extracts_goalkeeper_ids(self) -> None:
        """PlayingPosition='TW' identifies goalkeepers from both teams."""
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _h, _a, _ptm, gk_ids = _parse_teams(path)
            assert gk_ids == {"H001", "A001"}
        finally:
            os.unlink(path)

    def test_non_goalkeepers_excluded(self) -> None:
        """Players without PlayingPosition='TW' are not in gk_player_ids."""
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _h, _a, _ptm, gk_ids = _parse_teams(path)
            assert "H002" not in gk_ids
            assert "A002" not in gk_ids
        finally:
            os.unlink(path)


class TestParsePositionsXML:
    """Tests for _parse_positions_xml DFL XML parsing."""

    def _get_rows(self) -> list[dict[str, object]]:
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(_POSITIONS_XML)
        try:
            _h, _a, ptm, gk_ids = _parse_teams(info_path)
            rows_by_period = _parse_positions_xml(pos_path, ptm, "J03WMX", _logger, gk_player_ids=gk_ids)
            # Flatten period-bucketed rows into a single list for test assertions
            return [row for period_rows in rows_by_period.values() for row in period_rows]
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)

    def test_produces_rows(self) -> None:
        rows = self._get_rows()
        assert len(rows) > 0

    def test_match_id_prefixed(self) -> None:
        rows = self._get_rows()
        assert all(r["match_id"] == "idsse_J03WMX" for r in rows)

    def test_frame_rate_always_25(self) -> None:
        rows = self._get_rows()
        assert all(r["frame_rate"] == 25 for r in rows)

    def test_home_away_separation(self) -> None:
        rows = self._get_rows()
        teams = {r["team"] for r in rows}
        assert teams == {"home", "away"}

    def test_player_ids_from_xml(self) -> None:
        rows = self._get_rows()
        home_pids = {r["player_id"] for r in rows if r["team"] == "home"}
        assert "H001" in home_pids
        assert "H002" in home_pids

    def test_periods_present(self) -> None:
        rows = self._get_rows()
        periods = {r["period"] for r in rows}
        assert periods == {1, 2}

    def test_coordinates_preserved(self) -> None:
        """Verify center-origin meter coordinates pass through."""
        rows = self._get_rows()
        h001_f0 = [r for r in rows if r["player_id"] == "H001" and r["frame"] == 0 and r["period"] == 1]
        assert len(h001_f0) == 1
        assert h001_f0[0]["x"] == -10.5
        assert h001_f0[0]["y"] == 5.2

    def test_ball_coordinates_extracted(self) -> None:
        rows = self._get_rows()
        first_frame = [r for r in rows if r["period"] == 1 and r["frame"] == 0]
        assert len(first_frame) > 0
        assert first_frame[0]["ball_x"] == 0.5
        assert first_frame[0]["ball_y"] == 1.0

    def test_referee_excluded(self) -> None:
        """Referee FrameSets should be excluded."""
        rows = self._get_rows()
        pids = {r["player_id"] for r in rows}
        assert "REF001" not in pids

    def test_timestamp_calculated(self) -> None:
        rows = self._get_rows()
        frame1 = [r for r in rows if r["period"] == 1 and r["frame"] == 1]
        assert len(frame1) > 0
        assert frame1[0]["timestamp"] == round(1 / 25, 4)

    def test_expected_columns(self) -> None:
        """Tracking rows carry the full bronze-completeness DFL schema.

        Pre-PR-1.8 (2026-04-21) this was 12 cols; now 25 cols — every DFL
        ``<Frame>`` attribute on player rows plus every ball-frame attribute
        (including ``ball_z`` for aerial duels and ``ball_possession`` /
        ``ball_status`` for live-play detection).
        """
        rows = self._get_rows()
        expected = {
            # Derived / join keys
            "period",
            "frame",
            "timestamp",
            "player_id",
            "team",
            "team_id",
            "match_id",
            "frame_rate",
            "is_goalkeeper",
            # DFL per-player Frame attrs
            "x",
            "y",
            "t",
            "s",
            "a",
            "d",
            "m",
            # Ball-joined DFL Frame attrs
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_s",
            "ball_a",
            "ball_d",
            "ball_m",
            "ball_t",
            "ball_possession",
            "ball_status",
        }
        assert set(rows[0].keys()) == expected

    def test_is_goalkeeper_flag(self) -> None:
        """H001 and A001 are GKs (PlayingPosition=TW); H002 and A002 are not."""
        rows = self._get_rows()
        gk_rows = [r for r in rows if r["is_goalkeeper"] is True]
        non_gk_rows = [r for r in rows if r["is_goalkeeper"] is False]
        gk_pids = {r["player_id"] for r in gk_rows}
        non_gk_pids = {r["player_id"] for r in non_gk_rows}
        assert gk_pids == {"H001", "A001"}
        assert non_gk_pids == {"H002", "A002"}


class TestBallFrameSetOrdering:
    """PR 1.7: two-pass parser tolerates any FrameSet ordering.

    Before PR 1.7, a single-pass parser assumed ball FrameSets appeared
    BEFORE player FrameSets in the XML — wrong for real DFL data, where
    ball FrameSets come AFTER referees + all 22 players (typically at
    FrameSet position #26 per period). Single-pass under real ordering
    produced 100% NULL ball_x/ball_y in bronze, silently breaking the
    line-breaking UDF's ball-end-location estimation.

    The two-pass fix: pass 1 collects ball_coords, pass 2 emits player
    rows with lookup. Works for any FrameSet ordering.
    """

    def test_ball_after_players_now_populates_ball_coords(self) -> None:
        """Ball FrameSet AFTER player FrameSets — ball coords MUST populate.

        Inverted semantic from the pre-PR-1.7 test: we now expect ball
        coords to be present regardless of XML FrameSet ordering.
        """
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(_POSITIONS_XML_BALL_LAST)
        try:
            _h, _a, ptm, _gk = _parse_teams(info_path)
            rows_by_period = _parse_positions_xml(pos_path, ptm, "J03WMX", _logger)
            rows = rows_by_period[1]
            assert len(rows) == 2
            # Ball coords SHOULD be present even though ball FrameSet came later.
            assert rows[0]["ball_x"] == 0.5, f"expected 0.5 (from ball frame N=0), got {rows[0]['ball_x']!r}"
            assert rows[0]["ball_y"] == 1.0
            assert rows[1]["ball_x"] == 1.5
            assert rows[1]["ball_y"] == 2.0
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)

    def test_ball_after_players_does_not_log_ordering_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """With two-pass parsing, the ball-ordering warning must NOT fire."""
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(_POSITIONS_XML_BALL_LAST)
        try:
            _h, _a, ptm, _gk = _parse_teams(info_path)
            with caplog.at_level(logging.WARNING):
                _parse_positions_xml(pos_path, ptm, "J03WMX", _logger)
            # Previous single-pass parser logged this warning when ball came last.
            # Two-pass eliminates the condition.
            assert "Ball coordinate lookup missed" not in caplog.text
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)

    def test_normal_order_has_ball_coords(self) -> None:
        """Sanity check: ball-first ordering (the pre-PR-1.7 happy path) still works."""
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(_POSITIONS_XML)
        try:
            _h, _a, ptm, _gk = _parse_teams(info_path)
            rows_by_period = _parse_positions_xml(pos_path, ptm, "J03WMX", _logger)
            rows = [row for period_rows in rows_by_period.values() for row in period_rows]
            period1_f0 = [r for r in rows if r["period"] == 1 and r["frame"] == 0]
            assert all(r["ball_x"] is not None for r in period1_f0)
            assert all(r["ball_y"] is not None for r in period1_f0)
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)

    def test_real_dfl_ordering_with_referees_first(self) -> None:
        """Real-DFL-shaped ordering: referees → players → ball. Ball coords must populate.

        Mirrors the real J03WMX.xml structure observed 2026-04-21:
        FrameSets 0-2 are referees, 3-24 are 22 players (2 teams x 11 positions),
        25 is the ball. Under the pre-PR-1.7 single-pass parser, this produced
        100% NULL ball coords in bronze.
        """
        real_order_xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
<Positions>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="referee" PersonId="REF001">
<Frame N="10000" X="0" Y="0"/>
<Frame N="10001" X="0" Y="0"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-000008" PersonId="H001">
<Frame N="10000" X="-10" Y="5"/>
<Frame N="10001" X="-10" Y="5"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-00000G" PersonId="A001">
<Frame N="10000" X="30" Y="10"/>
<Frame N="10001" X="30" Y="10"/>
</FrameSet>
<FrameSet GameSection="firstHalf" MatchId="DFL-MAT-J03WMX" TeamId="BALL" PersonId="DFL-OBJ-0000XT">
<Frame N="10000" X="0.5" Y="1.0"/>
<Frame N="10001" X="1.5" Y="2.0"/>
</FrameSet>
</Positions>
</PutDataRequest>
"""
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(real_order_xml)
        try:
            _h, _a, ptm, _gk = _parse_teams(info_path)
            rows_by_period = _parse_positions_xml(pos_path, ptm, "J03WMX", _logger)
            rows = rows_by_period[1]
            # 2 players x 2 frames = 4 rows; referee FrameSet skipped; ball FrameSet
            # processed in pass 1 (coords stored) + skipped in pass 2 (not emitted).
            assert len(rows) == 4
            for r in rows:
                assert r["ball_x"] is not None, (
                    f"Row with frame={r['frame']}, player={r['player_id']} has NULL ball_x — "
                    "ball-after-players ordering regressed"
                )
                assert r["ball_y"] is not None
            # Period-relative timestamp invariant (PR 1.6): frame 10000 → 0.0.
            assert all(r["frame"] in (10000, 10001) for r in rows)
            frame_10000 = [r for r in rows if r["frame"] == 10000]
            frame_10001 = [r for r in rows if r["frame"] == 10001]
            assert all(r["timestamp"] == 0.0 for r in frame_10000)
            assert all(r["timestamp"] == round(1 / 25, 4) for r in frame_10001)
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)


class TestPeriodRelativeTimestamps:
    """PR 1.6 regression: tracking timestamps must be period-relative, not absolute.

    Pre-PR-1.6, ``_parse_positions_xml`` computed ``timestamp = frame_n / 25``,
    which produced absolute-frame-seconds (e.g. 400s for frame 10000) that never
    aligned with events' period-relative ``timestamp_seconds`` (0-2820s range).
    The temporal join in ``line_breaking_tracking._process_idsse_tracking``
    consequently produced zero rows for the idsse_tracking data source — a
    silent failure exposed only when PR 1.5 fixed the upstream event_type
    filter. These tests use a fixture with DFL-realistic non-zero starting
    frames (period 1 @ 10000, period 2 @ 100000) and would FAIL under the
    old absolute-timestamp logic.
    """

    def _get_rows(self) -> dict[int, list[dict[str, object]]]:
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(_POSITIONS_XML_ABSOLUTE_FRAMES)
        try:
            _h, _a, ptm, _gk = _parse_teams(info_path)
            return _parse_positions_xml(pos_path, ptm, "J03WMX", _logger)
        finally:
            os.unlink(info_path)
            os.unlink(pos_path)

    def test_period_1_first_frame_is_timestamp_zero(self) -> None:
        """Frame 10000 (period 1's first frame) must emit timestamp = 0.0, not 400.0."""
        rows = self._get_rows()[1]
        first_frame = [r for r in rows if r["frame"] == 10000]
        assert len(first_frame) > 0
        assert first_frame[0]["timestamp"] == 0.0, (
            f"Expected period-relative timestamp 0.0 for period 1's first frame, "
            f"got {first_frame[0]['timestamp']!r}. If this is 400.0, the absolute-"
            f"frame-second bug from PR 1.5 regressed."
        )

    def test_period_1_subsequent_frames_are_period_relative(self) -> None:
        """Frames 10001, 10002 must emit 0.04s, 0.08s (step of 1/25 per frame)."""
        rows = self._get_rows()[1]
        by_frame = {r["frame"]: r["timestamp"] for r in rows}
        assert by_frame[10000] == 0.0
        assert by_frame[10001] == round(1 / 25, 4)  # 0.04
        assert by_frame[10002] == round(2 / 25, 4)  # 0.08

    def test_period_2_first_frame_is_timestamp_zero(self) -> None:
        """Period 2's first frame (100000) must emit timestamp = 0.0, not 4000.0.

        Period 2's period_first_frame tracking is independent from period 1 —
        both periods reset to timestamp 0.0 at their respective first frames.
        """
        rows = self._get_rows()[2]
        first_frame = [r for r in rows if r["frame"] == 100000]
        assert len(first_frame) > 0
        assert first_frame[0]["timestamp"] == 0.0

    def test_period_2_subsequent_frames_are_period_relative(self) -> None:
        """Period 2 frame 100001 must emit timestamp = 0.04 (1/25), not 4000.04."""
        rows = self._get_rows()[2]
        by_frame = {r["frame"]: r["timestamp"] for r in rows}
        assert by_frame[100000] == 0.0
        assert by_frame[100001] == round(1 / 25, 4)

    def test_absolute_frame_number_preserved_in_frame_column(self) -> None:
        """The ``frame`` column still carries the ABSOLUTE DFL frame number.

        Bronze-completeness: the period-relative timestamp is a derived
        convenience; the absolute frame number remains the truth.
        Downstream consumers can always recover absolute time via ``frame / 25``.
        """
        rows_p1 = self._get_rows()[1]
        frames_p1 = {r["frame"] for r in rows_p1}
        assert 10000 in frames_p1
        assert 10001 in frames_p1
        assert 10002 in frames_p1
        rows_p2 = self._get_rows()[2]
        frames_p2 = {r["frame"] for r in rows_p2}
        assert 100000 in frames_p2
        assert 100001 in frames_p2


class TestSmoothTracking:
    """Tests for _smooth_tracking integration."""

    def test_reduces_noise_in_parsed_data(self) -> None:
        """Smoothing reduces frame-to-frame jitter on realistic tracking data."""
        rng = np.random.default_rng(42)
        n = 50
        df = pd.DataFrame(
            {
                "player_id": "H001",
                "period": 1,
                "frame": range(n),
                "timestamp": [i / 25.0 for i in range(n)],
                "team": "home",
                "x": np.linspace(-10, 10, n) + rng.normal(0, 0.02, n),
                "y": np.linspace(5, 15, n) + rng.normal(0, 0.02, n),
                "ball_x": 0.0,
                "ball_y": 0.0,
                "match_id": "idsse_J03WMX",
                "frame_rate": 25,
            }
        )
        raw_jitter = df["x"].diff().std()

        smoothed = _smooth_tracking(df)

        smooth_jitter = smoothed["x"].diff().std()
        assert smooth_jitter < raw_jitter
        assert len(smoothed) == len(df)

    def test_short_sequence_unchanged(self) -> None:
        """Sequences shorter than window_length pass through unmodified."""
        df = pd.DataFrame(
            {
                "player_id": ["H001"] * 3,
                "period": [1] * 3,
                "frame": [0, 1, 2],
                "timestamp": [0.0, 0.04, 0.08],
                "team": ["home"] * 3,
                "x": [1.0, 2.0, 3.0],
                "y": [4.0, 5.0, 6.0],
                "ball_x": [0.0] * 3,
                "ball_y": [0.0] * 3,
                "match_id": ["idsse_J03WMX"] * 3,
                "frame_rate": [25] * 3,
            }
        )

        smoothed = _smooth_tracking(df)

        pd.testing.assert_series_equal(smoothed["x"], df["x"], check_names=False)


class TestIDSSEMatchIDs:
    """Tests for match ID constants."""

    def test_seven_match_ids(self) -> None:
        assert len(IDSSE_MATCH_IDS) == 7

    def test_known_match_id_present(self) -> None:
        assert "J03WMX" in IDSSE_MATCH_IDS


# Minimal DFL event XML for testing (DFL_03_02 series — actual format)
# Mirrors the real PutDataRequest > Event > {child type} structure.
# Coordinates are DFL pitch-origin meters: x in (0, 105), y in (0, 68).
_EVENTS_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
<PutDataRequest RequestId="TEST" MessageTime="2023-05-27T15:00:00.000+02:00" \
TransmissionComplete="true" DataStatus="postmatch">
<Event MatchId="DFL-MAT-J03WMX" X-Source-Position="52.50" \
EventTime="2023-05-27T15:30:12.230+02:00" Y-Source-Position="34.00" \
EventId="18226500000006" X-Position="52.50" Y-Position="34.00">
 <KickOff TeamLeft="DFL-CLU-00000G" TeamRight="DFL-CLU-000008" GameSection="firstHalf">
  <Play SemiField="false" Player="DFL-OBJ-0027G6" Team="DFL-CLU-00000G" \
FromOpenPlay="false" Recipient="DFL-OBJ-0027KL" Evaluation="successfullyCompleted">
   <Pass FreeKickLayup="false"/>
  </Play>
 </KickOff>
</Event>
<Event MatchId="DFL-MAT-J03WMX" X-Source-Position="38.27" \
EventTime="2023-05-27T15:30:15.059+02:00" Y-Source-Position="33.80" \
EventId="18226500000007" X-Position="38.27" Y-Position="33.80">
 <Play SemiField="false" Player="H001" Team="DFL-CLU-000008" \
FromOpenPlay="true" Evaluation="unsuccessful">
  <Pass FreeKickLayup="false" Direction="diagonalBall"/>
 </Play>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T15:30:34.498+02:00" \
EventId="18226500000009" X-Position="50.55" Y-Position="59.11">
 <TacklingGame WinnerTeam="DFL-CLU-00000G" Winner="A001" \
Loser="H002" LoserTeam="DFL-CLU-000008" Type="air"/>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T16:35:00.000+02:00" \
EventId="18226500000050" X-Position="52.50" Y-Position="34.00">
 <KickOff TeamLeft="DFL-CLU-000008" TeamRight="DFL-CLU-00000G" GameSection="secondHalf">
  <Play SemiField="false" Player="H001" Team="DFL-CLU-000008" \
FromOpenPlay="false" Recipient="H002" Evaluation="successfullyCompleted">
   <Pass FreeKickLayup="false"/>
  </Play>
 </KickOff>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T16:35:05.500+02:00" \
EventId="18226500000051" X-Position="65.00" Y-Position="40.00">
 <Play SemiField="false" Player="A001" Team="DFL-CLU-00000G" \
FromOpenPlay="true" Evaluation="successfullyCompleted">
  <Pass FreeKickLayup="false"/>
 </Play>
</Event>
<Event MatchId="DFL-MAT-J03WMX" \
EventTime="2023-05-27T16:35:10.000+02:00" \
EventId="18226500000099">
 <OtherBallAction Player="A002" Team="DFL-CLU-00000G"/>
</Event>
</PutDataRequest>
"""


class TestParseEventsXML:
    """Tests for _parse_events_xml DFL event XML parsing."""

    def _get_rows(self) -> list[dict[str, object]]:
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        event_path = _write_temp_xml(_EVENTS_XML)
        try:
            _h, _a, ptm, _gk = _parse_teams(info_path)
            return _parse_events_xml(event_path, ptm, "J03WMX", _logger)
        finally:
            os.unlink(info_path)
            os.unlink(event_path)

    def test_produces_rows(self) -> None:
        """All 6 events land in bronze — the position-less OtherBallAction included.

        After the 2026-04-20 rewrite for bronze-completeness, events without
        X-Position/Y-Position are no longer skipped: they emit with
        ``x = y = None``. Downstream staging may filter on ``x IS NOT NULL``.
        """
        rows = self._get_rows()
        assert len(rows) == 6

    def test_match_id_prefixed(self) -> None:
        rows = self._get_rows()
        assert all(r["match_id"] == "idsse_J03WMX" for r in rows)

    def test_event_id_preserved(self) -> None:
        rows = self._get_rows()
        event_ids = {r["event_id"] for r in rows}
        assert "18226500000006" in event_ids
        assert "18226500000007" in event_ids
        assert "18226500000009" in event_ids

    def test_event_type_from_child_tag(self) -> None:
        """Event type is the first child element tag name."""
        rows = self._get_rows()
        evt_ko = [r for r in rows if r["event_id"] == "18226500000006"]
        assert len(evt_ko) == 1
        assert evt_ko[0]["event_type"] == "KickOff"

        evt_play = [r for r in rows if r["event_id"] == "18226500000007"]
        assert evt_play[0]["event_type"] == "Play"

        evt_tackle = [r for r in rows if r["event_id"] == "18226500000009"]
        assert evt_tackle[0]["event_type"] == "TacklingGame"

    def test_coordinates_pitch_origin(self) -> None:
        """DFL pitch-origin meter coordinates (0-105, 0-68) pass through to bronze."""
        rows = self._get_rows()
        evt_ko = [r for r in rows if r["event_id"] == "18226500000006"]
        assert evt_ko[0]["x"] == 52.5
        assert evt_ko[0]["y"] == 34.0

        evt_play = [r for r in rows if r["event_id"] == "18226500000007"]
        assert evt_play[0]["x"] == 38.27
        assert evt_play[0]["y"] == 33.8

    def test_period_tracked_from_kickoff(self) -> None:
        """Period state updates when KickOff with GameSection is encountered."""
        rows = self._get_rows()
        # First 3 events are firstHalf (period 1)
        first_half = [r for r in rows if r["event_id"] in {"18226500000006", "18226500000007", "18226500000009"}]
        assert all(r["period"] == 1 for r in first_half)

        # Events after second KickOff are period 2
        second_half = [r for r in rows if r["event_id"] in {"18226500000050", "18226500000051"}]
        assert all(r["period"] == 2 for r in second_half)

    def test_timestamp_relative_to_period_start(self) -> None:
        """Timestamps are computed as seconds from the first event in each period."""
        rows = self._get_rows()
        # First event of period 1 → 0.0 seconds
        evt_first = [r for r in rows if r["event_id"] == "18226500000006"]
        assert evt_first[0]["timestamp_seconds"] == 0.0

        # Second event is 2.829s after the first (15:30:15.059 - 15:30:12.230)
        evt_second = [r for r in rows if r["event_id"] == "18226500000007"]
        assert evt_second[0]["timestamp_seconds"] == 2.829

        # First event of period 2 → 0.0 seconds (new period start)
        evt_2h_first = [r for r in rows if r["event_id"] == "18226500000050"]
        assert evt_2h_first[0]["timestamp_seconds"] == 0.0

        # Second event of period 2 → 5.5 seconds
        evt_2h_second = [r for r in rows if r["event_id"] == "18226500000051"]
        assert evt_2h_second[0]["timestamp_seconds"] == 5.5

    def test_player_from_play_element(self) -> None:
        """Player ID extracted from the Player attribute on <Play>/<KickOff> children."""
        rows = self._get_rows()
        evt_play = [r for r in rows if r["event_id"] == "18226500000007"]
        assert evt_play[0]["player_id"] == "H001"

    def test_player_from_tackling_game(self) -> None:
        """TacklingGame uses Winner attribute as the primary player."""
        rows = self._get_rows()
        evt_tackle = [r for r in rows if r["event_id"] == "18226500000009"]
        assert evt_tackle[0]["player_id"] == "A001"

    def test_team_mapped_from_player_team_map(self) -> None:
        """Team label resolved via player_team_map (PersonId → home/away)."""
        rows = self._get_rows()
        # H001 is in the home team
        evt_play = [r for r in rows if r["event_id"] == "18226500000007"]
        assert evt_play[0]["team"] == "home"

        # A001 is in the away team
        evt_tackle = [r for r in rows if r["event_id"] == "18226500000009"]
        assert evt_tackle[0]["team"] == "away"

    def test_events_without_position_included_with_null_coords(self) -> None:
        """Events without position attrs ARE captured in bronze (x = y = None).

        Bronze-completeness: non-positional events (Substitution, Caution,
        FinalWhistle, OtherBallAction without position, etc.) land in
        bronze with NULL coordinates rather than being dropped.
        """
        rows = self._get_rows()
        event_ids = {r["event_id"] for r in rows}
        assert "18226500000099" in event_ids
        no_pos = next(r for r in rows if r["event_id"] == "18226500000099")
        assert no_pos["x"] is None
        assert no_pos["y"] is None

    def test_core_event_level_columns_present(self) -> None:
        """Core event-level columns must be on every row (bronze-completeness
        parser emits many more cols; full coverage is asserted by
        test_idsse_bronze_coverage.py — this test only guards the core nine).
        """
        rows = self._get_rows()
        core = {
            "match_id",
            "event_id",
            "event_type",
            "timestamp_seconds",
            "period",
            "player_id",
            "team",
            "x",
            "y",
        }
        for row in rows:
            missing = core - set(row.keys())
            assert not missing, f"row {row.get('event_id')} missing core cols: {sorted(missing)}"
