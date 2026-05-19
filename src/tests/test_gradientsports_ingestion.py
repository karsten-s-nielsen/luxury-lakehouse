"""Tests for Gradient Sports ingestion modules."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


class TestMatchInfo:
    def test_valid_match_id(self) -> None:
        from ingestion.gradientsports_common import MatchInfo

        m = MatchInfo(
            id="12345",
            artifacts={"12345_events": "events.json"},
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
            {"event_id": 1, "type": "pass", "team_id": 10, "player_id": 5},
            {"event_id": 2, "type": "shot", "team_id": 10, "player_id": 7},
        ]
        df = parse_events(events, match_id="99999")
        assert len(df) == 2
        assert "match_id" in df.columns
        assert df["match_id"].iloc[0] == "99999"
        assert "_ingested_at" in df.columns

    def test_parse_dict_format(self) -> None:
        from ingestion.gradientsports_events import parse_events

        data = {"events": [{"event_id": 1, "type": "pass"}]}
        df = parse_events(data, match_id="99999")
        assert len(df) == 1


class TestParseTracking:
    def test_parse_frame_list(self) -> None:
        from ingestion.gradientsports_tracking import parse_tracking

        frames = [
            {
                "frame_id": 1,
                "period_id": 1,
                "time_seconds": 0.0,
                "frame_rate": 30,
                "ball_state": "alive",
                "players": [
                    {
                        "player_id": 5,
                        "team_id": 10,
                        "is_goalkeeper": False,
                        "x": 10.0,
                        "y": 5.0,
                        "z": 0.0,
                        "speed": 2.1,
                    },
                    {
                        "player_id": 7,
                        "team_id": 20,
                        "is_goalkeeper": True,
                        "x": -40.0,
                        "y": 0.0,
                        "z": 0.0,
                        "speed": 0.5,
                    },
                ],
                "ball": {"x": 0.0, "y": 0.0, "z": 0.5, "speed": 15.0},
            }
        ]
        df = parse_tracking(frames, match_id="99999")
        assert len(df) == 3  # 2 players + 1 ball
        assert df[df["is_ball"]].iloc[0]["x_centered"] == 0.0
        assert "match_id" in df.columns
        assert "_ingested_at" in df.columns
