"""Unit tests for SkillCorner tracking ingestion (JSONL parser)."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.skillcorner_tracking import parse_tracking_jsonl


def _make_frame(frame_num: int, period: int, timestamp: str) -> dict:
    """Build a single JSONL frame dict for testing."""
    return {
        "frame": frame_num,
        "period": period,
        "timestamp": timestamp,
        "player_data": [
            {
                "player_id": 38673,
                "x": 10.5,
                "y": -5.2,
                "is_detected": True,
            },
            {
                "player_id": 44001,
                "x": -20.1,
                "y": 3.4,
                "is_detected": False,
            },
        ],
        "ball_data": {
            "x": 5.0,
            "y": -1.0,
            "z": 0.3,
            "is_detected": True,
        },
    }


class TestParseTrackingJsonl:
    def test_basic_parse(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.20"), _make_frame(2, 1, "00:00:01.30")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text("\n".join(json.dumps(f) for f in frames))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        # 2 frames x 2 players = 4 rows
        assert len(df) == 4
        assert set(df["player_id"].unique()) == {38673, 44001}

    def test_timestamp_parsed_to_float(self, tmp_path: Path) -> None:
        """timestamp 'HH:MM:SS.ms' must be parsed to float seconds."""
        frames = [_make_frame(1, 1, "00:12:34.90")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert df["timestamp"].dtype == "Float64"
        # 0*3600 + 12*60 + 34.90 = 754.9
        assert abs(df["timestamp"].iloc[0] - 754.9) < 0.01

    def test_is_detected_renamed_to_is_visible(self, tmp_path: Path) -> None:
        """Raw API field 'is_detected' becomes bronze column 'is_visible'."""
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert "is_visible" in df.columns
        assert "is_detected" not in df.columns
        # First player is_detected=True
        row = df[df["player_id"] == 38673].iloc[0]
        assert row["is_visible"] == True  # noqa: E712 — nullable boolean

    def test_match_id_is_raw_native(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert df["match_id"].iloc[0] == "1886347"
        assert not df["match_id"].iloc[0].startswith("skillcorner_")

    def test_ball_columns_present(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert "ball_x" in df.columns
        assert "ball_y" in df.columns
        assert "ball_z" in df.columns
        assert "ball_is_detected" in df.columns
        assert df["ball_x"].iloc[0] == 5.0
        assert df["ball_z"].iloc[0] == 0.3

    def test_frame_rate_is_10(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        assert df["frame_rate"].iloc[0] == 10

    def test_schema_columns(self, tmp_path: Path) -> None:
        frames = [_make_frame(1, 1, "00:00:01.00")]
        jsonl_path = tmp_path / "tracking.jsonl"
        jsonl_path.write_text(json.dumps(frames[0]))

        df = parse_tracking_jsonl(str(jsonl_path), match_id="1886347")

        expected = {
            "match_id",
            "period",
            "frame",
            "timestamp",
            "player_id",
            "x",
            "y",
            "is_visible",
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_is_detected",
            "frame_rate",
            "_ingested_at",
        }
        assert set(df.columns) == expected
