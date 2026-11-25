"""A canonical silly-kicks tracking frame built the real way, for the tracking-compute unit tests.

v2-view-shaped rows -> `shape_long_input` -> roster goalkeepers -> gradientsports `convert_to_frames` (LTR). Two
teams of three players (one keeper each) and a ball, moving +x over `n_frames` frames at 25 Hz.
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import Final

import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking.gradientsports import convert_to_frames  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.compute.frames import resolve_roster_goalkeepers, shape_long_input

HOME: Final = 100
AWAY: Final = 200
HOME_GK: Final = 1
AWAY_GK: Final = 11
MATCH_KEY: Final = 7

_PLAYERS: Final = (
    (HOME_GK, HOME, -50_000, 0),
    (2, HOME, -20_000, 10_000),
    (3, HOME, 5_000, -5_000),
    (AWAY_GK, AWAY, 50_000, 0),
    (12, AWAY, 20_000, -10_000),
    (13, AWAY, -5_000, 5_000),
)


def canonical_frame(n_frames: int = 10) -> pd.DataFrame:
    """One match's canonical `TRACKING_FRAMES_COLUMNS` frame (gradientsports, LTR), `n_frames` frames long."""
    frame_rows: list[dict[str, object]] = []
    ball_rows: list[dict[str, object]] = []
    for frame_id in range(n_frames):
        for pid, team, x_mm, y_mm in _PLAYERS:
            frame_rows.append(
                {
                    "canonical_match_key": MATCH_KEY,
                    "canonical_player_key": pid,
                    "team_key": team,
                    "canonical_team_key": team,
                    "provider": "gradientsports",
                    "period": 1,
                    "frame": frame_id,
                    "x_mm": x_mm + frame_id * 400,
                    "y_mm": y_mm,
                    "is_visible": True,
                    "_visibility": "visible",
                }
            )
        ball_rows.append(
            {
                "canonical_match_key": MATCH_KEY,
                "period": 1,
                "frame": frame_id,
                "ball_x_mm": frame_id * 400,
                "ball_y_mm": 0,
                "ball_detected": True,
                "possession_team_key": None,
                "time_seconds": frame_id / 25.0,
            }
        )
    frames = pd.DataFrame(frame_rows).astype({"canonical_player_key": "Int64", "canonical_team_key": "Int64"})
    raw = shape_long_input(frames, pd.DataFrame(ball_rows), frame_rate=25.0)
    raw = resolve_roster_goalkeepers(raw, {(HOME, HOME_GK), (AWAY, AWAY_GK)})
    frame, _ = convert_to_frames(raw, home_team_id=HOME, home_team_start_left=True, output_convention="ltr")
    return frame
