"""Tests for Gradient Sports ingestion modules."""

from __future__ import annotations

import bz2
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

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
    """Verify parse-both-then-write-both guarantees no partial writes."""

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.parse_tracking")
    @patch("ingestion.gradientsports.parse_events")
    def test_tracking_parse_failure_prevents_event_write(
        self,
        mock_parse_events: MagicMock,
        mock_parse_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """If tracking parsing fails, events must NOT be written."""
        import logging

        from ingestion.gradientsports import ingest_gradientsports

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]', content=b"bad")
        mock_parse_events.return_value = MagicMock()  # events parse OK
        mock_parse_tracking.side_effect = RuntimeError("bz2 decompress failed")

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

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.parse_tracking")
    @patch("ingestion.gradientsports.parse_events")
    def test_event_parse_failure_prevents_tracking_write(
        self,
        mock_parse_events: MagicMock,
        mock_parse_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """If event parsing fails, tracking must NOT be written."""
        import logging

        from ingestion.gradientsports import ingest_gradientsports

        mock_fetch_artifact.return_value = MagicMock(text="bad json", content=b"data")
        mock_parse_events.side_effect = json.JSONDecodeError("bad", "", 0)
        mock_parse_tracking.return_value = MagicMock()  # tracking parse OK

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

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.parse_tracking")
    @patch("ingestion.gradientsports.parse_events")
    def test_both_artifacts_written_on_success(
        self,
        mock_parse_events: MagicMock,
        mock_parse_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """When both parse successfully, both must be written."""
        import logging

        import pandas as pd

        from ingestion.gradientsports import ingest_gradientsports

        events_df = pd.DataFrame({"match_id": ["10502"]})
        tracking_df = pd.DataFrame({"match_id": ["10502"]})
        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]', content=b"data")
        mock_parse_events.return_value = events_df
        mock_parse_tracking.return_value = tracking_df

        ingest_gradientsports(
            spark=MagicMock(),
            catalog="cat",
            schema="bronze",
            logger=logging.getLogger("test"),
            matches=[_make_match()],
        )

        mock_write_events.assert_called_once()
        mock_write_tracking.assert_called_once()
