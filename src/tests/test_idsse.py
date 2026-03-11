"""Tests for ingestion.idsse — IDSSE tracking data DFL XML parsing."""

from __future__ import annotations

import logging
import os
import tempfile

import numpy as np
import pandas as pd

from ingestion.idsse import IDSSE_MATCH_IDS, _parse_positions_xml, _parse_teams, _smooth_tracking

_logger = logging.getLogger("test_idsse")

# Minimal DFL match info XML for testing
_MATCH_INFO_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<PutDataRequest>
  <MatchInformation>
    <General MatchId="DFL-MAT-J03WMX" HomeTeamId="DFL-CLU-000008"
             GuestTeamId="DFL-CLU-00000G" />
    <Teams>
      <Team TeamId="DFL-CLU-000008" Role="home">
        <Players>
          <Player PersonId="H001" ShirtNumber="1" FirstName="A" LastName="B" />
          <Player PersonId="H002" ShirtNumber="2" FirstName="C" LastName="D" />
        </Players>
      </Team>
      <Team TeamId="DFL-CLU-00000G" Role="guest">
        <Players>
          <Player PersonId="A001" ShirtNumber="9" FirstName="E" LastName="F" />
          <Player PersonId="A002" ShirtNumber="10" FirstName="G" LastName="H" />
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
            home_id, _away_id, _ptm = _parse_teams(path)
            assert home_id == "DFL-CLU-000008"
        finally:
            os.unlink(path)

    def test_identifies_away_team(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _home_id, away_id, _ptm = _parse_teams(path)
            assert away_id == "DFL-CLU-00000G"
        finally:
            os.unlink(path)

    def test_maps_players_to_teams(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _h, _a, ptm = _parse_teams(path)
            assert ptm["H001"] == "home"
            assert ptm["H002"] == "home"
            assert ptm["A001"] == "away"
            assert ptm["A002"] == "away"
        finally:
            os.unlink(path)

    def test_player_count(self) -> None:
        path = _write_temp_xml(_MATCH_INFO_XML)
        try:
            _h, _a, ptm = _parse_teams(path)
            assert len(ptm) == 4
        finally:
            os.unlink(path)


class TestParsePositionsXML:
    """Tests for _parse_positions_xml DFL XML parsing."""

    def _get_rows(self) -> list[dict[str, object]]:
        info_path = _write_temp_xml(_MATCH_INFO_XML)
        pos_path = _write_temp_xml(_POSITIONS_XML)
        try:
            _h, _a, ptm = _parse_teams(info_path)
            rows_by_period = _parse_positions_xml(pos_path, ptm, "J03WMX", _logger)
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
        rows = self._get_rows()
        expected = {
            "period",
            "frame",
            "timestamp",
            "player_id",
            "team",
            "x",
            "y",
            "ball_x",
            "ball_y",
            "match_id",
            "frame_rate",
        }
        assert set(rows[0].keys()) == expected


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
