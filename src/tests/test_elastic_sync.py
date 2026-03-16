"""Tests for analytics.elastic_sync — ELASTIC event-tracking synchronization.

Reference: Kim, H.S. et al. (2025). "ELASTIC: Event-Tracking Data
Synchronization in Soccer Without Annotated Event Locations." ECML-PKDD
MLSA 2025. arXiv:2508.09238.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.elastic_sync import (
    ElasticSyncParams,
    align_events_to_frames,
    extract_ball_features,
)


def _make_tracking_df(
    n_frames: int = 100,
    frame_rate: int = 25,
    period: int = 1,
    n_players: int = 2,
) -> pd.DataFrame:
    """Create a synthetic tracking DataFrame for testing.

    Ball moves linearly from (0, 0) to (50, 30) with an acceleration
    spike at the midpoint frame (simulating an event like a pass).
    """
    rows: list[dict[str, object]] = []
    mid_frame = n_frames // 2

    for f in range(n_frames):
        t = f / frame_rate
        # Ball position: linear motion with a direction change at midpoint
        if f <= mid_frame:
            bx = f * 0.5
            by = f * 0.3
        else:
            # After event: ball changes direction (creates acceleration spike)
            bx = mid_frame * 0.5 - (f - mid_frame) * 0.3
            by = mid_frame * 0.3 + (f - mid_frame) * 0.2

        for p in range(n_players):
            player_id = f"P{p:03d}"
            # Players positioned near the ball at the event frame
            px = bx + (p + 1) * 2.0 + np.sin(f * 0.1) * 0.5
            py = by + (p + 1) * 1.5 + np.cos(f * 0.1) * 0.5

            rows.append(
                {
                    "frame": f,
                    "period": period,
                    "timestamp": round(t, 4),
                    "player_id": player_id,
                    "team": "home" if p == 0 else "away",
                    "x": round(px, 4),
                    "y": round(py, 4),
                    "ball_x": round(bx, 4),
                    "ball_y": round(by, 4),
                    "match_id": "idsse_TEST",
                    "frame_rate": frame_rate,
                }
            )

    return pd.DataFrame(rows)


def _make_events_df(
    event_frames: list[int],
    frame_rate: int = 25,
    period: int = 1,
    player_id: str = "P000",
) -> pd.DataFrame:
    """Create a synthetic events DataFrame for testing."""
    rows: list[dict[str, object]] = []
    for i, f in enumerate(event_frames):
        rows.append(
            {
                "event_id": f"evt_{i}",
                "event_type": "pass",
                "timestamp_seconds": round(f / frame_rate, 4),
                "period": period,
                "player_id": player_id,
                "match_id": "idsse_TEST",
            }
        )
    return pd.DataFrame(rows)


class TestExtractBallFeatures:
    """Tests for extract_ball_features."""

    def test_acceleration_computed(self) -> None:
        """Ball acceleration column is present in output."""
        tracking = _make_tracking_df()
        result = extract_ball_features(tracking, frame_rate=25)
        assert "ball_accel" in result.columns

    def test_acceleration_at_direction_change(self) -> None:
        """Acceleration spike occurs near the ball direction change point."""
        tracking = _make_tracking_df(n_frames=100)
        result = extract_ball_features(tracking, frame_rate=25)
        mid_frame = 50

        # Skip the first few frames (initial start-from-rest creates a large
        # acceleration spike). Focus on the interior where the direction change
        # should dominate.
        interior = pd.DataFrame(result[result["frame"] >= 5]).reset_index(drop=True)
        accel_series: pd.Series[float] = interior["ball_accel"]  # type: ignore[assignment]
        max_accel_idx = accel_series.idxmax()
        max_accel_frame = int(interior.loc[max_accel_idx, "frame"])  # type: ignore[call-overload]

        # The max interior acceleration should be near the direction change (±5 frames)
        assert abs(max_accel_frame - mid_frame) <= 5

    def test_speed_columns_present(self) -> None:
        """Velocity and speed columns are computed."""
        tracking = _make_tracking_df()
        result = extract_ball_features(tracking, frame_rate=25)
        assert "ball_vx" in result.columns
        assert "ball_vy" in result.columns
        assert "ball_speed" in result.columns

    def test_empty_tracking(self) -> None:
        """Empty tracking data returns empty result with correct columns."""
        empty_df = pd.DataFrame(columns=pd.Index(["frame", "period", "ball_x", "ball_y"]))
        result = extract_ball_features(empty_df)
        assert result.empty
        assert "ball_accel" in result.columns

    def test_no_ball_data(self) -> None:
        """Tracking with no ball coordinates returns empty result."""
        df = pd.DataFrame(
            {
                "frame": [0, 1, 2],
                "period": [1, 1, 1],
                "ball_x": [None, None, None],
                "ball_y": [None, None, None],
            }
        )
        result = extract_ball_features(df)
        assert result.empty

    def test_one_row_per_frame(self) -> None:
        """Output has one row per unique (period, frame) pair."""
        tracking = _make_tracking_df(n_frames=50, n_players=5)
        result = extract_ball_features(tracking, frame_rate=25)
        assert len(result) == 50

    def test_acceleration_non_negative(self) -> None:
        """Ball acceleration values are non-negative."""
        tracking = _make_tracking_df()
        result = extract_ball_features(tracking, frame_rate=25)
        assert (result["ball_accel"] >= 0).all()


class TestAlignEventsToFrames:
    """Tests for align_events_to_frames."""

    def test_known_alignment(self) -> None:
        """Event at the direction change frame is aligned near that frame."""
        frame_rate = 25
        tracking = _make_tracking_df(n_frames=100, frame_rate=frame_rate)
        mid_frame = 50
        events = _make_events_df([mid_frame], frame_rate=frame_rate, player_id="P000")

        result = align_events_to_frames(events, tracking, frame_rate=frame_rate)

        assert len(result) == 1
        aligned_frame = int(result.iloc[0]["frame_id"])
        # Should be within the search window of the actual event frame
        assert abs(aligned_frame - mid_frame) <= frame_rate  # within 1 second

    def test_output_schema(self) -> None:
        """Output has the expected columns."""
        tracking = _make_tracking_df()
        events = _make_events_df([50])

        result = align_events_to_frames(events, tracking)

        expected_cols = {"event_id", "frame_id", "alignment_confidence", "alignment_error_seconds"}
        assert set(result.columns) == expected_cols

    def test_window_bounds(self) -> None:
        """Events near period boundaries are handled without errors."""
        tracking = _make_tracking_df(n_frames=100)

        # Event at frame 0 (start of period) and frame 99 (end of period)
        events = _make_events_df([0, 99])
        result = align_events_to_frames(events, tracking)

        # Both events should produce results (they have tracking data nearby)
        assert len(result) >= 1

    def test_confidence_bounded(self) -> None:
        """Alignment confidence is in [0, 1]."""
        tracking = _make_tracking_df()
        events = _make_events_df([25, 50, 75])

        result = align_events_to_frames(events, tracking)

        assert len(result) > 0
        assert (result["alignment_confidence"] >= 0.0).all()
        assert (result["alignment_confidence"] <= 1.0).all()

    def test_multiple_events(self) -> None:
        """Handles multiple events per match."""
        tracking = _make_tracking_df(n_frames=200)
        events = _make_events_df([25, 50, 75, 100, 150])

        result = align_events_to_frames(events, tracking)

        assert len(result) == 5
        assert result["event_id"].nunique() == 5

    def test_empty_events(self) -> None:
        """Empty events DataFrame returns empty result."""
        tracking = _make_tracking_df()
        events = pd.DataFrame(columns=pd.Index(["event_id", "event_type", "timestamp_seconds", "period", "player_id"]))

        result = align_events_to_frames(events, tracking)
        assert result.empty

    def test_empty_tracking(self) -> None:
        """Empty tracking DataFrame returns empty result."""
        events = _make_events_df([50])
        tracking = pd.DataFrame(columns=pd.Index(["frame", "period", "player_id", "x", "y", "ball_x", "ball_y"]))

        result = align_events_to_frames(events, tracking)
        assert result.empty

    def test_error_seconds_non_negative(self) -> None:
        """Alignment error is non-negative."""
        tracking = _make_tracking_df()
        events = _make_events_df([50])

        result = align_events_to_frames(events, tracking)

        assert len(result) > 0
        assert (result["alignment_error_seconds"] >= 0.0).all()

    def test_custom_params(self) -> None:
        """Custom ElasticSyncParams are respected."""
        tracking = _make_tracking_df()
        events = _make_events_df([50])

        params = ElasticSyncParams(
            window_seconds=0.5,
            accel_weight=0.8,
            proximity_weight=0.2,
        )
        result = align_events_to_frames(events, tracking, params=params)

        assert len(result) > 0
        # With a tighter window, the aligned frame should be closer to the nominal
        aligned_frame = int(result.iloc[0]["frame_id"])
        nominal_frame = 50
        window_frames = int(0.5 * 25)
        assert abs(aligned_frame - nominal_frame) <= window_frames

    def test_different_periods(self) -> None:
        """Events in different periods are aligned independently."""
        tracking_p1 = _make_tracking_df(n_frames=50, period=1)
        tracking_p2 = _make_tracking_df(n_frames=50, period=2)
        tracking = pd.concat([tracking_p1, tracking_p2], ignore_index=True)

        events = pd.DataFrame(
            [
                {
                    "event_id": "evt_p1",
                    "event_type": "pass",
                    "timestamp_seconds": 1.0,
                    "period": 1,
                    "player_id": "P000",
                },
                {
                    "event_id": "evt_p2",
                    "event_type": "pass",
                    "timestamp_seconds": 1.0,
                    "period": 2,
                    "player_id": "P000",
                },
            ]
        )

        result = align_events_to_frames(events, tracking)
        assert len(result) == 2
        assert set(result["event_id"]) == {"evt_p1", "evt_p2"}
