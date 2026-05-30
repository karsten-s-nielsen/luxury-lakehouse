"""Tests for Gradient Sports ingestion modules."""

from __future__ import annotations

import bz2
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from ingestion.guards import FilterResult

if TYPE_CHECKING:
    from ingestion.gradientsports_common import MatchInfo


class TestMatchInfo:
    def test_valid_match_id(self) -> None:
        from ingestion.gradientsports_common import MatchInfo

        m = MatchInfo(
            id="12345",
            artifacts={"events": "events.json"},
            home="Qatar",
            away="Ecuador",
            date="2022-11-20",
            updated_at=datetime(2022, 11, 20, tzinfo=timezone.utc),
            visibility="public",
        )
        assert m.id == "12345"

    def test_non_numeric_id_rejected(self) -> None:
        from ingestion.gradientsports_common import MatchInfo

        with pytest.raises(ValueError, match="numeric"):
            MatchInfo(
                id="abc",
                artifacts={},
                home="A",
                away="B",
                date="2022-01-01",
                updated_at=datetime(2022, 1, 1, tzinfo=timezone.utc),
                visibility="public",
            )


class TestMatchInfoSerialization:
    """Verify MatchInfo survives JSON round-trip via model_dump_json (spec §4.2 item 5)."""

    def test_round_trip_preserves_all_fields(self) -> None:
        """model_dump_json -> model_validate_json must produce an identical MatchInfo."""
        from ingestion.gradientsports_common import MatchInfo

        original = MatchInfo(
            id="10508",
            artifacts={"10508_events": "events.json", "10508_tracking": "tracking.jsonl.bz2"},
            home="Morocco",
            away="Spain",
            date="2022-12-06",
            updated_at=datetime(2022, 12, 6, 15, 30, 0, tzinfo=timezone.utc),
            visibility="public",
        )

        json_str = original.model_dump_json()
        restored = MatchInfo.model_validate_json(json_str)

        assert restored == original

    def test_model_dump_json_not_json_dumps(self) -> None:
        """json.dumps(model_dump()) crashes on datetime; model_dump_json() must be used."""
        from ingestion.gradientsports_common import MatchInfo

        m = MatchInfo(
            id="10508",
            artifacts={},
            home="Morocco",
            away="Spain",
            date="2022-12-06",
            updated_at=datetime(2022, 12, 6, 15, 30, 0, tzinfo=timezone.utc),
            visibility="public",
        )

        # model_dump_json works
        json_str = m.model_dump_json()
        assert isinstance(json_str, str)

        # json.dumps(model_dump()) crashes on datetime
        import json as json_mod

        with pytest.raises(TypeError, match="not JSON serializable"):
            json_mod.dumps(m.model_dump())


class TestParseEvents:
    def test_parse_list_format(self) -> None:
        from ingestion.gradientsports_events import parse_events

        events = [
            {"gameId": 10502, "gameEventId": 1, "startTime": 179.0},
            {"gameId": 10502, "gameEventId": 2, "startTime": 180.0},
        ]
        df = parse_events(events, match_id="10502")
        assert len(df) == 2
        assert "match_id" in df.columns
        assert df["match_id"].iloc[0] == "10502"
        assert "_ingested_at" in df.columns

    def test_parse_dict_format(self) -> None:
        from ingestion.gradientsports_events import parse_events

        data = {"events": [{"gameId": 10502, "gameEventId": 1}]}
        df = parse_events(data, match_id="10502")
        assert len(df) == 1

    def test_list_fields_serialized_as_json(self) -> None:
        from ingestion.gradientsports_events import parse_events

        events = [
            {
                "gameId": 10502,
                "gameEventId": 1,
                "homePlayers": [{"jerseyNum": 8, "x": -7.1, "y": 28.3}],
                "awayPlayers": [{"jerseyNum": 10, "x": 0.5, "y": -0.3}],
                "ball": [{"visibility": "VISIBLE", "x": -1.4, "y": -0.3}],
            }
        ]
        df = parse_events(events, match_id="10502")
        # List fields should be JSON strings, not raw Python lists
        assert isinstance(df["homePlayers"].iloc[0], str)
        assert json.loads(df["homePlayers"].iloc[0])[0]["jerseyNum"] == 8

    def test_int_columns_widened_to_float(self) -> None:
        """Integer JSON values must become float64 so Spark always infers DOUBLE.

        Regression test: match 10502 had startGameClock=0 (int64) while
        match 10511 had startGameClock=0.0 (float64). Per-match replaceWhere
        with mergeSchema cannot widen BIGINT → DOUBLE, causing
        DELTA_FAILED_TO_MERGE_FIELDS on the second match.
        """
        from ingestion.gradientsports_events import parse_events

        # Simulate match with integer value (would produce int64 without fix)
        events_int = [{"gameId": 10502, "gameEventId": 1, "startGameClock": 0}]
        df_int = parse_events(events_int, match_id="10502")

        # Simulate match with float value (would produce float64)
        events_float = [{"gameId": 10511, "gameEventId": 1, "startGameClock": 0.0}]
        df_float = parse_events(events_float, match_id="10511")

        # Both must produce the same dtype — float64
        assert df_int["startGameClock"].dtype.name == "float64"
        assert df_float["startGameClock"].dtype.name == "float64"


class TestParseTracking:
    """Tests against the real Gradient Sports tracking schema (JSONL.bz2)."""

    @staticmethod
    def _make_frame(
        frame_num: int = 5366,
        period: int = 1,
        home_players: list | None = None,
        away_players: list | None = None,
        balls: list | None = None,
    ) -> dict:
        """Build a single tracking frame matching the real API schema."""
        return {
            "frameNum": frame_num,
            "period": period,
            "periodElapsedTime": 0.0,
            "periodGameClockTime": 0.0,
            "videoTimeMs": 179045.7,
            "gameRefId": 10502.0,
            "version": "4.1.0",
            "generatedTime": "2023-07-12T07:26:52Z",
            "smoothedTime": "2024-02-02T14:01:56Z",
            "game_event_id": 6629601.0,
            "possession_event_id": 6510902.0,
            "game_event": {
                "game_id": 10502,
                "game_event_type": "FIRSTKICKOFF",
            },
            "possession_event": {
                "game_id": 10502,
                "possession_event_type": "PA",
            },
            "homePlayers": home_players
            or [
                {"jerseyNum": "8", "confidence": "HIGH", "visibility": "VISIBLE", "x": -7.1, "y": 28.3},
            ],
            "homePlayersSmoothed": [
                {"jerseyNum": "8", "confidence": "HIGH", "visibility": "VISIBLE", "x": -8.9, "y": 27.0},
            ],
            "awayPlayers": away_players
            or [
                {"jerseyNum": "10", "confidence": "HIGH", "visibility": "VISIBLE", "x": 0.5, "y": -0.3},
            ],
            "awayPlayersSmoothed": [
                {"jerseyNum": "10", "confidence": "HIGH", "visibility": "VISIBLE", "x": 2.5, "y": -0.1},
            ],
            "balls": balls or [{"visibility": "VISIBLE", "x": -1.4, "y": -0.3, "z": 0.0}],
            "ballsSmoothed": {"visibility": "VISIBLE", "x": -1.7, "y": 6.9, "z": 0.0},
        }

    def _compress_frames(self, frames: list[dict]) -> bytes:
        """Compress frame dicts to bz2 JSONL bytes (matching API format)."""
        jsonl = "\n".join(json.dumps(f) for f in frames)
        return bz2.compress(jsonl.encode("utf-8"))

    def test_parse_bz2_bytes(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        compressed = self._compress_frames([self._make_frame()])
        df = parse_tracking(compressed, match_id="10502")
        # 1 home + 1 away + 1 ball = 3 rows
        assert len(df) == 3
        assert set(df.columns) == {
            "match_id",
            "game_ref_id",
            "frame_num",
            "period",
            "period_elapsed_time",
            "period_game_clock_time",
            "video_time_ms",
            "version",
            "generated_time",
            "smoothed_time",
            "game_event_id",
            "possession_event_id",
            "_game_event_json",
            "_possession_event_json",
            "team_side",
            "is_ball",
            "jersey_num",
            "confidence",
            "visibility",
            "x",
            "y",
            "z",
            "x_smoothed",
            "y_smoothed",
            "z_smoothed",
            "_ingested_at",
        }

    def test_smoothed_coords_matched(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        compressed = self._compress_frames([self._make_frame()])
        df = parse_tracking(compressed, match_id="10502")
        home = df[(df["team_side"] == "home") & (~df["is_ball"])]
        assert home.iloc[0]["x"] == pytest.approx(-7.1)
        assert home.iloc[0]["x_smoothed"] == pytest.approx(-8.9)
        import pandas as pd

        assert pd.isna(home.iloc[0]["z"])

    def test_ball_rows(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        compressed = self._compress_frames([self._make_frame()])
        df = parse_tracking(compressed, match_id="10502")
        balls = df[df["is_ball"]]
        assert len(balls) == 1
        assert balls.iloc[0]["x"] == pytest.approx(-1.4)
        assert balls.iloc[0]["z"] == pytest.approx(0.0)
        assert balls.iloc[0]["z_smoothed"] == pytest.approx(0.0)
        assert balls.iloc[0]["team_side"] is None

    def test_none_smoothed_players(self) -> None:
        """Frames where *Smoothed keys are None (observed in real data)."""
        from ingestion.gradientsports_tracking import parse_tracking

        frame = self._make_frame()
        frame["homePlayersSmoothed"] = None
        frame["awayPlayersSmoothed"] = None
        compressed = self._compress_frames([frame])
        df = parse_tracking(compressed, match_id="10502")
        players = df[~df["is_ball"]]
        assert all(players["x_smoothed"].isna())

    def test_game_event_json_captured(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        compressed = self._compress_frames([self._make_frame()])
        df = parse_tracking(compressed, match_id="10502")
        ge = json.loads(df["_game_event_json"].iloc[0])
        assert ge["game_event_type"] == "FIRSTKICKOFF"

    def test_multiple_frames(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        frames = [self._make_frame(frame_num=i) for i in range(5)]
        compressed = self._compress_frames(frames)
        df = parse_tracking(compressed, match_id="10502")
        # 5 frames x 3 entities = 15 rows
        assert len(df) == 15
        assert df["frame_num"].nunique() == 5

    def test_int_columns_widened_to_float(self) -> None:
        """Integer frame fields must become float64 to prevent cross-match schema conflicts."""
        from ingestion.gradientsports_tracking import parse_tracking

        # frameNum as int in JSON (json.loads("5366") → int)
        compressed = self._compress_frames([self._make_frame(frame_num=5366)])
        df = parse_tracking(compressed, match_id="10502")
        assert df["frame_num"].dtype.name == "float64"
        assert df["period"].dtype.name == "float64"


class TestFrameDedup:
    """Dedup keep-first on (period, frameNum) — silly-kicks PR-S72 heads-up + ADR-004 contract.

    GS provider ships duplicate (period, frameNum) records (up to 16 copies of
    one frame observed in match 10502). Without dedup at the bronze-writer
    boundary, every entity at the affected frame fans out N times, crashing
    silly-kicks _pressure_bekkers AND silently inflating ~15 other downstream
    features (pitch-control, DAS, team-shape, GK-influence, ...).
    """

    @staticmethod
    def _make_frame(*, frame_num: int = 5366, period: int = 1, x: float = -7.1) -> dict:
        """Frame builder parametrized for dedup tests. x lets us prove keep-first kept the FIRST copy."""
        return {
            "gameRefId": 10502.0,
            "frameNum": frame_num,
            "period": period,
            "periodElapsedTime": 100.0,
            "periodGameClockTime": 100.0,
            "videoTimeMs": 100000,
            "version": "1.0",
            "generatedTime": "2026-01-01T00:00:00",
            "smoothedTime": "2026-01-01T00:00:00",
            "game_event_id": None,
            "possession_event_id": None,
            "game_event": None,
            "possession_event": None,
            "homePlayers": [{"jerseyNum": "10", "confidence": "HIGH", "visibility": "VISIBLE", "x": x, "y": -0.3}],
            "homePlayersSmoothed": None,
            "awayPlayers": [{"jerseyNum": "10", "confidence": "HIGH", "visibility": "VISIBLE", "x": 0.5, "y": -0.3}],
            "awayPlayersSmoothed": None,
            "balls": [{"visibility": "VISIBLE", "x": -1.4, "y": -0.3, "z": 0.0}],
            "ballsSmoothed": None,
        }

    def test_parse_tracking_dedupes_repeated_frame_keys(self) -> None:
        """16 content-divergent copies of one frame (the worst observed case in match 10502)
        must produce only ONE frame's worth of rows (1 home + 1 away + 1 ball = 3)."""
        from ingestion.gradientsports_tracking import parse_tracking

        # 16 copies of (period=1, frame_num=5366), each with a different x to prove
        # they're content-divergent. Plus one unique (period=1, frame_num=5367) sentinel.
        copies = [self._make_frame(frame_num=5366, x=-7.1 + i * 0.01) for i in range(16)]
        unique_sentinel = self._make_frame(frame_num=5367, x=99.0)
        df = parse_tracking([*copies, unique_sentinel], match_id="10502")

        # 2 unique frames x (1 home + 1 away + 1 ball) = 6 rows total
        assert len(df) == 6, f"expected 6 rows post-dedup; got {len(df)}: {df}"
        # Keep-first invariant: the first copy's x value (-7.1) survived for frame 5366
        home_5366 = df[(df["frame_num"] == 5366.0) & (df["team_side"] == "home")]
        assert len(home_5366) == 1
        assert home_5366.iloc[0]["x"] == pytest.approx(-7.1)
        # Sentinel survived untouched
        home_5367 = df[(df["frame_num"] == 5367.0) & (df["team_side"] == "home")]
        assert len(home_5367) == 1
        assert home_5367.iloc[0]["x"] == pytest.approx(99.0)

    def test_parse_tracking_dedupes_across_periods_independently(self) -> None:
        """Same frame_num in different periods are DIFFERENT keys — must NOT be deduped together."""
        from ingestion.gradientsports_tracking import parse_tracking

        p1 = self._make_frame(frame_num=5366, period=1, x=-7.1)
        p2 = self._make_frame(frame_num=5366, period=2, x=11.0)
        df = parse_tracking([p1, p2], match_id="10502")

        # Both kept: 2 frames x 3 rows = 6
        assert len(df) == 6
        assert set(df["period"].unique()) == {1.0, 2.0}

    def test_parse_tracking_passes_through_frames_with_missing_keys(self) -> None:
        """Frames missing period/frameNum are caller's schema issue, not a dedup concern — pass through."""
        from ingestion.gradientsports_tracking import parse_tracking

        normal = self._make_frame(frame_num=5366)
        no_period = self._make_frame(frame_num=5366)
        no_period["period"] = None
        no_frame_num = self._make_frame(frame_num=5366)
        no_frame_num["frameNum"] = None
        df = parse_tracking([normal, no_period, no_frame_num], match_id="10502")

        # 3 frames x 3 rows (none deduped - only the first has a valid key, the other two
        # bypass dedup because their keys are incomplete).
        assert len(df) == 9

    def test_iter_unique_frames_logs_drop_count(self) -> None:
        """The dedup helper must log dropped duplicates so silent inflation is observable."""
        import logging

        from ingestion.gradientsports_tracking import _iter_unique_frames

        captured: list[str] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record.getMessage())

        log = logging.getLogger("test_iter_unique_frames")
        log.handlers = [_Capture()]
        log.setLevel(logging.WARNING)

        copies = [self._make_frame(frame_num=5366, x=float(i)) for i in range(5)]
        list(_iter_unique_frames(iter(copies), log=log))

        warnings = [m for m in captured if "dedup" in m and "dropped 4" in m]
        assert warnings, f"expected a 'dropped 4' warning; got: {captured}"


class TestStreamTrackingToParquet:
    """Tests for the streaming bz2→Parquet path (production OOM fix)."""

    @staticmethod
    def _make_frame(frame_num: int = 5366) -> dict:
        return TestParseTracking._make_frame(frame_num=frame_num)

    def test_multi_chunk_bz2_round_trip(self, tmp_path: Path) -> None:
        """Multi-frame bz2 stream → Parquet → read-back preserves rows and schema."""
        import pyarrow.parquet as pq

        from ingestion.gradientsports_tracking import (
            _ARROW_SCHEMA,
            stream_tracking_to_parquet,
        )

        frames = [self._make_frame(frame_num=i) for i in range(25)]
        jsonl = "\n".join(json.dumps(f) for f in frames)
        compressed = bz2.compress(jsonl.encode("utf-8"))

        # Simulate streaming response with small chunks to exercise multi-chunk path
        chunk_size = 64
        chunks = [compressed[i : i + chunk_size] for i in range(0, len(compressed), chunk_size)]
        mock_response = MagicMock()
        mock_response.iter_content.return_value = iter(chunks)

        parquet_path = str(tmp_path / "test.parquet")
        import logging

        total_rows = stream_tracking_to_parquet(
            mock_response,
            match_id="10502",
            parquet_path=parquet_path,
            log=logging.getLogger("test"),
        )

        # 25 frames x 3 entities (1 home + 1 away + 1 ball) = 75 rows
        assert total_rows == 75

        # Read back and verify schema + data
        table = pq.read_table(parquet_path)
        assert table.num_rows == 75
        assert table.schema.equals(_ARROW_SCHEMA)
        assert set(table.column("match_id").to_pylist()) == {"10502"}
        assert len(set(table.column("frame_num").to_pylist())) == 25

    def test_empty_stream_produces_empty_parquet(self, tmp_path: Path) -> None:
        """An empty bz2 stream produces a valid but empty Parquet file."""
        import pyarrow.parquet as pq

        from ingestion.gradientsports_tracking import stream_tracking_to_parquet

        compressed = bz2.compress(b"")
        mock_response = MagicMock()
        mock_response.iter_content.return_value = iter([compressed])

        parquet_path = str(tmp_path / "empty.parquet")
        import logging

        total_rows = stream_tracking_to_parquet(
            mock_response,
            match_id="10502",
            parquet_path=parquet_path,
            log=logging.getLogger("test"),
        )

        assert total_rows == 0
        table = pq.read_table(parquet_path)
        assert table.num_rows == 0


def _make_match(mid: str = "10502") -> MatchInfo:
    """Build a MatchInfo with both event and tracking artifacts."""
    from ingestion.gradientsports_common import MatchInfo

    return MatchInfo(
        id=mid,
        artifacts={"events": "events.json", "tracking": "tracking.jsonl.bz2"},
        home="Qatar",
        away="Ecuador",
        date="2022-11-20",
        updated_at=datetime(2022, 11, 20, tzinfo=timezone.utc),
        visibility="public",
    )


class TestIngestAtomicity:
    """Verify write ordering and parse-phase atomicity.

    The skip guard reads MAX(_ingested_at) from the EVENTS table. Events
    must always be the LAST write so the watermark only advances when both
    artifacts are committed. If tracking succeeds but events fails, the
    watermark stays put and the match is re-discovered on retry.
    """

    @patch("ingestion.utils.ensure_volume_directory")
    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.stream_tracking_to_parquet", return_value=100)
    @patch("ingestion.gradientsports.parse_events")
    def test_tracking_written_before_events(
        self,
        mock_parse_events: MagicMock,
        mock_stream_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
        mock_ensure_dir: MagicMock,
    ) -> None:
        """Tracking must be written BEFORE events (watermark ordering).

        The skip guard derives its watermark from events._ingested_at.
        If events were written first and tracking then failed, the watermark
        would advance past the match, making it unrecoverable on retry.
        """
        import logging

        import pandas as pd

        from ingestion.gradientsports import ingest_gradientsports

        call_order: list[str] = []
        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]')
        mock_parse_events.return_value = pd.DataFrame({"match_id": ["10502"]})
        mock_write_tracking.side_effect = lambda *a, **kw: call_order.append("tracking")
        mock_write_events.side_effect = lambda *a, **kw: call_order.append("events")

        ingest_gradientsports(
            spark=MagicMock(),
            catalog="cat",
            schema="bronze",
            logger=logging.getLogger("test"),
            matches=[_make_match()],
        )

        assert call_order == ["tracking", "events"], (
            f"Write order must be tracking-first, events-last; got {call_order}"
        )

    @patch("ingestion.utils.ensure_volume_directory")
    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.stream_tracking_to_parquet", return_value=100)
    @patch("ingestion.gradientsports.parse_events")
    def test_tracking_write_failure_prevents_event_write(
        self,
        mock_parse_events: MagicMock,
        mock_stream_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
        mock_ensure_dir: MagicMock,
    ) -> None:
        """If tracking WRITE fails, events must NOT be written.

        Since tracking is written first, a tracking write failure raises
        before events write is reached. The guard watermark stays put.
        """
        import logging

        import pandas as pd

        from ingestion.gradientsports import ingest_gradientsports

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]')
        mock_parse_events.return_value = pd.DataFrame({"match_id": ["10502"]})
        mock_write_tracking.side_effect = RuntimeError("DELTA_FAILED_TO_MERGE_FIELDS")

        with pytest.raises(RuntimeError, match="DELTA_FAILED_TO_MERGE_FIELDS"):
            ingest_gradientsports(
                spark=MagicMock(),
                catalog="cat",
                schema="bronze",
                logger=logging.getLogger("test"),
                matches=[_make_match()],
            )

        mock_write_tracking.assert_called_once()
        mock_write_events.assert_not_called()

    @patch("ingestion.utils.ensure_volume_directory")
    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.stream_tracking_to_parquet")
    @patch("ingestion.gradientsports.parse_events")
    def test_tracking_stream_failure_prevents_all_writes(
        self,
        mock_parse_events: MagicMock,
        mock_stream_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
        mock_ensure_dir: MagicMock,
    ) -> None:
        """If tracking streaming fails, neither artifact is written."""
        import logging

        from ingestion.gradientsports import ingest_gradientsports

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]')
        mock_parse_events.return_value = MagicMock()
        mock_stream_tracking.side_effect = RuntimeError("bz2 decompress failed")

        with pytest.raises(RuntimeError, match="bz2 decompress failed"):
            ingest_gradientsports(
                spark=MagicMock(),
                catalog="cat",
                schema="bronze",
                logger=logging.getLogger("test"),
                matches=[_make_match()],
            )

        mock_write_events.assert_not_called()
        mock_write_tracking.assert_not_called()

    @patch("ingestion.utils.ensure_volume_directory")
    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.stream_tracking_to_parquet", return_value=100)
    @patch("ingestion.gradientsports.parse_events")
    def test_event_parse_failure_prevents_all_writes(
        self,
        mock_parse_events: MagicMock,
        mock_stream_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
        mock_ensure_dir: MagicMock,
    ) -> None:
        """If event parsing fails, neither artifact is written."""
        import logging

        from ingestion.gradientsports import ingest_gradientsports

        mock_fetch_artifact.return_value = MagicMock(text="bad json")
        mock_parse_events.side_effect = json.JSONDecodeError("bad", "", 0)

        with pytest.raises(json.JSONDecodeError):
            ingest_gradientsports(
                spark=MagicMock(),
                catalog="cat",
                schema="bronze",
                logger=logging.getLogger("test"),
                matches=[_make_match()],
            )

        mock_write_events.assert_not_called()
        mock_write_tracking.assert_not_called()


class TestParquetStaging:
    """Regression guards for the Parquet staging fix (spec §4.1)."""

    def test_no_create_dataframe_in_tracking_module(self) -> None:
        """AST guard: spark.createDataFrame must never appear in gradientsports_tracking.py.

        The OOM fix replaces createDataFrame with Parquet staging. This test
        prevents silent reintroduction of the RPC-bound path.
        """
        import ast

        source_path = Path(__file__).resolve().parents[1] / "ingestion" / "gradientsports_tracking.py"
        tree = ast.parse(source_path.read_text())

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "createDataFrame":
                pytest.fail(
                    f"spark.createDataFrame found at line {node.lineno} in gradientsports_tracking.py. "
                    "Use Parquet staging via UC Volume instead (spec §2.1)."
                )

    def test_parquet_schema_round_trip(self, tmp_path: Path) -> None:
        """Pandas DF -> Parquet -> Pandas preserves column names and dtypes.

        Validates the pandas-to-Parquet layer. Spark's Parquet reader is
        Spark's responsibility — this test catches int64/float64 widening
        and string/object dtype issues at the boundary we control.
        """
        import numpy as np
        import pandas as pd

        n = 100
        df = pd.DataFrame(
            {
                "match_id": ["10502"] * n,
                "game_ref_id": [10502.0] * n,
                "frame_num": np.arange(n, dtype="float64"),
                "period": [1.0] * n,
                "period_elapsed_time": np.random.default_rng(42).uniform(0, 5400, n),
                "period_game_clock_time": np.random.default_rng(42).uniform(0, 5400, n),
                "video_time_ms": np.random.default_rng(42).uniform(0, 5_400_000, n),
                "version": ["4.1.0"] * n,
                "generated_time": ["2023-07-12T07:26:52Z"] * n,
                "smoothed_time": ["2024-02-02T14:01:56Z"] * n,
                "game_event_id": [6629601.0] * n,
                "possession_event_id": [6510902.0] * n,
                "_game_event_json": ['{"type": "FIRSTKICKOFF"}'] * n,
                "_possession_event_json": ['{"type": "PA"}'] * n,
                "team_side": ["home"] * n,
                "is_ball": [False] * n,
                "jersey_num": ["8"] * n,
                "confidence": ["HIGH"] * n,
                "visibility": ["VISIBLE"] * n,
                "x": np.random.default_rng(42).uniform(-55, 55, n),
                "y": np.random.default_rng(42).uniform(-34, 34, n),
                "z": [np.nan] * n,
                "x_smoothed": np.random.default_rng(42).uniform(-55, 55, n),
                "y_smoothed": np.random.default_rng(42).uniform(-34, 34, n),
                "z_smoothed": [np.nan] * n,
                "_ingested_at": pd.Timestamp.now(tz="UTC"),
            }
        )

        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path, index=False)
        df_back = pd.read_parquet(parquet_path)

        assert list(df_back.columns) == list(df.columns)
        assert len(df_back) == len(df)
        for col in ["frame_num", "period", "x", "y"]:
            assert df_back[col].dtype.name == "float64", f"{col} dtype changed to {df_back[col].dtype}"

    def test_staging_path_format(self) -> None:
        """_staging_path produces the expected UC Volume path format."""
        from ingestion.gradientsports_tracking import _staging_path

        expected_1 = "/Volumes/cat/bronze/_staging/gradientsports_tracking/10502.parquet"
        assert _staging_path("cat", "bronze", "10502") == expected_1
        expected_2 = "/Volumes/soccer_analytics/dev_bronze/_staging/gradientsports_tracking/10508.parquet"
        assert _staging_path("soccer_analytics", "dev_bronze", "10508") == expected_2

    @patch("ingestion.gradientsports_tracking.write_delta_table")
    @patch("ingestion.gradientsports_tracking.validate_dataframe")
    @patch("ingestion.gradientsports_tracking.ensure_volume_directory")
    def test_write_tracking_uses_parquet_staging(
        self,
        mock_ensure_dir: MagicMock,
        mock_validate: MagicMock,
        mock_write_delta: MagicMock,
        tmp_path: Path,
    ) -> None:
        """write_tracking() must stage via Parquet, not createDataFrame (spec §4.1 item 2)."""
        import pandas as pd

        from ingestion.gradientsports_tracking import write_tracking

        mock_spark = MagicMock()
        mock_validate.return_value = 5
        df = pd.DataFrame({"match_id": ["10502"] * 5, "frame_num": [1.0] * 5, "period": [1.0] * 5})

        staging_path = str(tmp_path / "staging" / "10502.parquet")
        # Create parent directory manually — ensure_volume_directory is mocked out,
        # but df.to_parquet() needs the directory to exist on the local filesystem.
        (tmp_path / "staging").mkdir()
        with patch("ingestion.gradientsports_tracking._staging_path", return_value=staging_path):
            write_tracking(mock_spark, "cat", "bronze", "10502", MagicMock(), df=df)

        # createDataFrame must NOT be called
        mock_spark.createDataFrame.assert_not_called()
        # ensure_volume_directory must be called for the parent dir
        mock_ensure_dir.assert_called_once()
        # spark.read.parquet must be called with the staging path
        mock_spark.read.parquet.assert_called_once_with(staging_path)
        # Delta write must happen
        mock_write_delta.assert_called_once()


class TestWriteTaskValue:
    """Tests for the shared write_task_value() helper in utils.py."""

    def test_graceful_fallback_outside_databricks(self) -> None:
        """write_task_value logs warning when DBUtils is unavailable (local/CI)."""
        from ingestion.utils import write_task_value

        mock_logger = MagicMock()
        write_task_value("test_key", ["a", "b"], mock_logger)
        # Outside Databricks, pyspark.dbutils ImportError fires → warning logged
        mock_logger.warning.assert_called_once()
        assert "not available" in str(mock_logger.warning.call_args)

    def test_no_active_session_warns(self) -> None:
        """write_task_value warns when SparkSession.getActiveSession() returns None."""
        import sys

        from ingestion.utils import write_task_value

        # Temporarily make pyspark.dbutils importable but SparkSession returns None
        mock_dbutils_mod = MagicMock()
        mock_spark_mod = MagicMock()
        mock_spark_mod.SparkSession.getActiveSession.return_value = None
        mock_logger = MagicMock()

        with patch.dict(sys.modules, {"pyspark.dbutils": mock_dbutils_mod, "pyspark.sql": mock_spark_mod}):
            write_task_value("test_key", ["a", "b"], mock_logger)

        mock_logger.warning.assert_called_once()
        assert "No active SparkSession" in str(mock_logger.warning.call_args)


class TestPreflight:
    """Tests for main_preflight() — spec §4.2."""

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list")
    def test_preflight_emits_json_array(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Preflight emits a JSON array where each element is a valid MatchInfo JSON string."""
        from ingestion.gradientsports import main_preflight
        from ingestion.gradientsports_common import MatchInfo

        matches = [_make_match("10502"), _make_match("10503"), _make_match("10504")]
        mock_fetch.return_value = matches

        emitted: list[list[str]] = []

        def capture_task_value(key: str, value: list[str], logger: object = None) -> None:
            assert key == "gradientsports_matches"
            emitted.append(value)

        mock_spark = MagicMock()
        with (
            patch("ingestion.gradientsports.timed_check") as mock_check,
            patch("ingestion.gradientsports.write_task_value", side_effect=capture_task_value),
            patch("ingestion.gradientsports.get_spark_session", return_value=mock_spark),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.bootstrap.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(
                catalog="cat", schema="bronze", match_json=None, backfill_artifacts=False
            )
            mock_check.return_value = FilterResult(
                workflow_id="wf-gradientsports",
                count=3,
                metadata={"matches": [m.model_dump() for m in matches]},
            )
            main_preflight()

        assert len(emitted) == 1
        task_value = emitted[0]
        assert len(task_value) == 3

        # Each element must be deserializable to MatchInfo
        for json_str in task_value:
            restored = MatchInfo.model_validate_json(json_str)
            assert restored.id in {"10502", "10503", "10504"}

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list", return_value=[])
    def test_preflight_empty_guard_emits_empty_list(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """When guard finds no matches, preflight emits [] (spec §4.2 item 6)."""
        from ingestion.gradientsports import main_preflight

        emitted: list[list[str]] = []

        def capture_task_value(key: str, value: list[str], logger: object = None) -> None:
            emitted.append(value)

        mock_spark = MagicMock()
        with (
            patch("ingestion.gradientsports.timed_check") as mock_check,
            patch("ingestion.gradientsports.write_task_value", side_effect=capture_task_value),
            patch("ingestion.gradientsports.get_spark_session", return_value=mock_spark),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.bootstrap.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(
                catalog="cat", schema="bronze", match_json=None, backfill_artifacts=False
            )
            mock_check.return_value = FilterResult(
                workflow_id="wf-gradientsports",
                count=0,
            )
            main_preflight()

        assert len(emitted) == 1
        assert emitted[0] == []


class TestMatchJsonIteration:
    """Tests for the --match-json single-match iteration mode (spec §4.3)."""

    @patch("ingestion.utils.ensure_volume_directory")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.stream_tracking_to_parquet", return_value=100)
    @patch("ingestion.gradientsports.parse_events")
    def test_match_json_deserializes_and_ingests(
        self,
        mock_parse_events: MagicMock,
        mock_stream_tracking: MagicMock,
        mock_token: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_ensure_dir: MagicMock,
    ) -> None:
        """--match-json mode deserializes MatchInfo and calls ingest_gradientsports."""
        import pandas as pd

        from ingestion.gradientsports import main

        match = _make_match("10508")
        match_json = match.model_dump_json()

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]')
        mock_parse_events.return_value = pd.DataFrame({"match_id": ["10508"]})

        with (
            patch("ingestion.gradientsports.get_spark_session", return_value=MagicMock()),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.bootstrap.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(
                catalog="cat", schema="bronze", match_json=match_json, backfill_artifacts=False
            )
            main()

        mock_write_tracking.assert_called_once()
        mock_write_events.assert_called_once()

    @patch("ingestion.utils.ensure_volume_directory")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.stream_tracking_to_parquet", return_value=100)
    @patch("ingestion.gradientsports.parse_events")
    def test_match_json_preserves_write_ordering(
        self,
        mock_parse_events: MagicMock,
        mock_stream_tracking: MagicMock,
        mock_token: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_ensure_dir: MagicMock,
    ) -> None:
        """Write-ordering invariant: tracking before events, even in --match-json mode."""
        import pandas as pd

        from ingestion.gradientsports import main

        call_order: list[str] = []
        match = _make_match("10508")
        match_json = match.model_dump_json()

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]')
        mock_parse_events.return_value = pd.DataFrame({"match_id": ["10508"]})
        mock_write_tracking.side_effect = lambda *a, **kw: call_order.append("tracking")
        mock_write_events.side_effect = lambda *a, **kw: call_order.append("events")

        with (
            patch("ingestion.gradientsports.get_spark_session", return_value=MagicMock()),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.bootstrap.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(
                catalog="cat", schema="bronze", match_json=match_json, backfill_artifacts=False
            )
            main()

        assert call_order == ["tracking", "events"], (
            f"Write order must be tracking-first, events-last; got {call_order}"
        )


class TestGradientSportsGuard:
    """Tests for the two-phase skip guard (anti-join + updatedSince)."""

    def _make_matches(self, ids: list[str]) -> list:
        """Build MatchInfo list for given IDs."""
        from ingestion.gradientsports_common import MatchInfo

        return [
            MatchInfo(
                id=mid,
                artifacts={"events": "e.json", "tracking": "t.bz2"},
                home="Home",
                away="Away",
                date="2022-11-20",
                updated_at=datetime(2022, 11, 20, tzinfo=timezone.utc),
                visibility="public",
            )
            for mid in ids
        ]

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list")
    def test_phase_a_discovers_missing_matches(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Phase A: matches in API but not in bronze are scheduled for ingestion."""
        from ingestion.gradientsports import skip_guard

        api_matches = self._make_matches(["10502", "10503", "10504"])
        mock_fetch.return_value = api_matches

        # Mock Spark: bronze has only match 10502
        mock_spark = MagicMock()
        mock_table = MagicMock()
        mock_table.select.return_value.distinct.return_value.collect.return_value = [
            {"match_id": "10502"},
        ]
        mock_spark.table.return_value = mock_table

        result = skip_guard.check(mock_spark, "cat", "bronze")

        assert result.count == 2
        match_ids = {m["id"] for m in result.metadata["matches"]}
        assert match_ids == {"10503", "10504"}
        # fetch_match_list called once (Phase A only, no Phase B)
        mock_fetch.assert_called_once_with("fake-token", updated_since=None)

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list")
    def test_phase_a_all_missing_when_table_absent(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Phase A: if bronze table doesn't exist, all matches are scheduled."""
        from ingestion.gradientsports import skip_guard

        api_matches = self._make_matches(["10502", "10503"])
        mock_fetch.return_value = api_matches

        # Mock Spark: table query raises AnalysisException (table not found)
        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception(
            "[TABLE_OR_VIEW_NOT_FOUND] The table or view `cat`.`bronze`.`gradientsports_events` cannot be found."
        )

        result = skip_guard.check(mock_spark, "cat", "bronze")

        assert result.count == 2
        match_ids = {m["id"] for m in result.metadata["matches"]}
        assert match_ids == {"10502", "10503"}

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list")
    def test_phase_b_checks_updated_since(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Phase B: when all matches ingested, uses updatedSince to find re-processed."""
        import sys

        from ingestion.gradientsports import skip_guard

        api_matches = self._make_matches(["10502", "10503"])
        updated_matches = self._make_matches(["10503"])

        # First call (Phase A): return all matches
        # Second call (Phase B with updatedSince): return re-processed match
        mock_fetch.side_effect = [api_matches, updated_matches]

        # Mock Spark: bronze has both matches (Phase A) + MAX(_ingested_at) (Phase B)
        mock_spark = MagicMock()
        mock_table = MagicMock()
        mock_table.select.return_value.distinct.return_value.collect.return_value = [
            {"match_id": "10502"},
            {"match_id": "10503"},
        ]

        mock_max_table = MagicMock()
        max_ts = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
        mock_max_table.select.return_value.collect.return_value = [{"max_ts": max_ts}]

        mock_spark.table.side_effect = [mock_table, mock_max_table]

        # Mock pyspark.sql.functions so Phase B's `from pyspark.sql import functions` works
        mock_pyspark_sql = MagicMock()
        with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_pyspark_sql}):
            result = skip_guard.check(mock_spark, "cat", "bronze")

        assert result.count == 1
        assert result.metadata["matches"][0]["id"] == "10503"
        # Two fetch_match_list calls: Phase A (no filter) + Phase B (with updatedSince)
        assert mock_fetch.call_count == 2
        second_call_kwargs = mock_fetch.call_args_list[1]
        assert "2026-05-19T12:00:00Z" in str(second_call_kwargs)

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list")
    def test_phase_b_no_updates_returns_zero(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Phase B: no provider updates → count=0."""
        import sys

        from ingestion.gradientsports import skip_guard

        api_matches = self._make_matches(["10502", "10503"])

        # Phase A: all matches, Phase B: empty (no updates)
        mock_fetch.side_effect = [api_matches, []]

        mock_spark = MagicMock()
        mock_table = MagicMock()
        mock_table.select.return_value.distinct.return_value.collect.return_value = [
            {"match_id": "10502"},
            {"match_id": "10503"},
        ]
        mock_max_table = MagicMock()
        max_ts = datetime(2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc)
        mock_max_table.select.return_value.collect.return_value = [{"max_ts": max_ts}]
        mock_spark.table.side_effect = [mock_table, mock_max_table]

        mock_pyspark_sql = MagicMock()
        with patch.dict(sys.modules, {"pyspark": MagicMock(), "pyspark.sql": mock_pyspark_sql}):
            result = skip_guard.check(mock_spark, "cat", "bronze")

        assert result.count == 0

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list", return_value=[])
    def test_api_returns_empty_list(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """If API returns 0 matches, guard returns count=0 immediately."""
        from ingestion.gradientsports import skip_guard

        mock_spark = MagicMock()
        result = skip_guard.check(mock_spark, "cat", "bronze")

        assert result.count == 0
        # Spark never queried
        mock_spark.table.assert_not_called()


class TestEnsureVolumeDirectoryDbutils:
    """Tests for the dbutils fallback path in ensure_volume_directory."""

    def test_dbutils_fallback_when_no_env_vars(self) -> None:
        """On serverless (no DATABRICKS_HOST/TOKEN), dbutils.fs.mkdirs is used."""
        import sys

        from ingestion.utils import ensure_volume_directory

        mock_dbutils_mod = MagicMock()
        mock_spark_mod = MagicMock()
        mock_spark = MagicMock()
        mock_spark_mod.SparkSession.getActiveSession.return_value = mock_spark

        mock_dbutils_cls = MagicMock()
        mock_dbutils_instance = MagicMock()
        mock_dbutils_cls.return_value = mock_dbutils_instance
        mock_dbutils_mod.DBUtils = mock_dbutils_cls

        with (
            patch.dict(os.environ, {}, clear=False),
            patch.dict(
                sys.modules,
                {"pyspark.dbutils": mock_dbutils_mod, "pyspark.sql": mock_spark_mod},
            ),
        ):
            # Ensure DATABRICKS_HOST/TOKEN are absent
            os.environ.pop("DATABRICKS_HOST", None)
            os.environ.pop("DATABRICKS_TOKEN", None)
            ensure_volume_directory("/Volumes/cat/bronze/_staging/gradientsports_tracking")

        mock_dbutils_instance.fs.mkdirs.assert_called_once_with("/Volumes/cat/bronze/_staging/gradientsports_tracking")

    def test_os_makedirs_fallback_when_no_dbutils(self, tmp_path: Path) -> None:
        """Outside Databricks (no dbutils), os.makedirs is used."""
        from ingestion.utils import ensure_volume_directory

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DATABRICKS_HOST", None)
            os.environ.pop("DATABRICKS_TOKEN", None)
            # Use a real path but trick the validator
            with patch("ingestion.utils.os.makedirs") as mock_makedirs:
                ensure_volume_directory("/Volumes/cat/bronze/_staging")

            mock_makedirs.assert_called_once_with("/Volumes/cat/bronze/_staging", exist_ok=True)
