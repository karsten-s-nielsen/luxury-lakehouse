"""Off-Ball Expected Threat (xT) analytics module.

Combines pitch control (Spearman 2017) with Expected Threat zones (Karun Singh
2018) to quantify each player's off-ball positional value. A player's Off-Ball
xT contribution equals the pitch control probability at their location multiplied
by the xT value of their zone.

Concept follows Soccermatics Lesson 7 by David Sumpter.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from analytics.pitch_control import PitchControlParams, compute_pitch_control_at_point


@dataclass(frozen=True)
class OffBallXtParams:
    """Parameters for Off-Ball xT computation."""

    pitch_length: float = 120.0  # StatsBomb pitch length
    pitch_width: float = 80.0  # StatsBomb pitch width
    sample_fps: float = 1.0  # Sample 1 frame per second


def _col_f64(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract a DataFrame column as a float64 numpy array (pyright-safe)."""
    return np.asarray(df[col], dtype=np.float64)


def _lookup_xt(
    x: float,
    y: float,
    xt_grid: np.ndarray,
    pitch_length: float = 120.0,
    pitch_width: float = 80.0,
) -> float:
    """Look up xT value from grid based on (x, y) position.

    Args:
        x: Player X coordinate (StatsBomb 120 scale).
        y: Player Y coordinate (StatsBomb 80 scale).
        xt_grid: 12x8 numpy array of xT values.
        pitch_length: Pitch length in coordinate units.
        pitch_width: Pitch width in coordinate units.

    Returns:
        xT probability for the zone containing (x, y).
    """
    if np.isnan(x) or np.isnan(y):
        return 0.0
    zone_x = min(int(x / (pitch_length / 12)), 11)
    zone_y = min(int(y / (pitch_width / 8)), 7)
    zone_x = max(zone_x, 0)
    zone_y = max(zone_y, 0)
    return float(xt_grid[zone_x, zone_y])


def compute_off_ball_xt_frame(
    players_df: pd.DataFrame,
    xt_grid: np.ndarray,
    pitch_control_params: PitchControlParams | None = None,
) -> pd.DataFrame:
    """Compute per-player Off-Ball xT for a single frame.

    Args:
        players_df: DataFrame with columns [player_id, team, x, y, velocity_x,
            velocity_y]. Must contain players from both teams.
        xt_grid: 12x8 numpy array of xT values (zone_x, zone_y).
        pitch_control_params: Optional pitch control parameters.

    Returns:
        DataFrame with columns [player_id, team, x, y, xt_value, pitch_control,
        off_ball_xt] — one row per player.
    """
    if players_df.empty:
        return pd.DataFrame(
            columns=pd.Index(["player_id", "team", "x", "y", "xt_value", "pitch_control", "off_ball_xt"])
        )

    if pitch_control_params is None:
        pitch_control_params = PitchControlParams()

    xs = _col_f64(players_df, "x")
    ys = _col_f64(players_df, "y")

    results: list[dict[str, object]] = []
    for i in range(len(players_df)):
        x_val = float(xs[i])
        y_val = float(ys[i])

        xt_val = _lookup_xt(x_val, y_val, xt_grid)
        pc = compute_pitch_control_at_point(players_df, x_val, y_val, pitch_control_params)

        row = players_df.iloc[i]
        team = str(row["team"])
        # For home players, PC is their control; for away, invert
        player_pc = pc if team == "home" else 1.0 - pc

        results.append(
            {
                "player_id": row["player_id"],
                "team": team,
                "x": x_val,
                "y": y_val,
                "xt_value": xt_val,
                "pitch_control": player_pc,
                "off_ball_xt": player_pc * xt_val,
            }
        )

    return pd.DataFrame(results)


def compute_off_ball_xt_match(
    tracking_df: pd.DataFrame,
    xt_grid: np.ndarray,
    params: OffBallXtParams | None = None,
    pitch_control_params: PitchControlParams | None = None,
) -> pd.DataFrame:
    """Compute per-player Off-Ball xT aggregated over a full match.

    Samples frames at the specified fps rate and aggregates xT contributions
    per player.

    Args:
        tracking_df: Full match tracking DataFrame with columns [player_id,
            team, x, y, velocity_x, velocity_y, frame, period, frame_rate].
        xt_grid: 12x8 numpy array of xT values.
        params: Off-Ball xT parameters (sampling rate, pitch dims).
        pitch_control_params: Optional pitch control parameters.

    Returns:
        DataFrame with columns [player_id, match_id, total_off_ball_xt,
        avg_off_ball_xt, frames_sampled] — one row per player.
    """
    if params is None:
        params = OffBallXtParams()

    if tracking_df.empty:
        return pd.DataFrame(
            columns=pd.Index(["player_id", "match_id", "total_off_ball_xt", "avg_off_ball_xt", "frames_sampled"])
        )

    match_id = str(tracking_df["match_id"].iloc[0])

    # Sample frames at desired fps
    frame_rate = int(tracking_df["frame_rate"].iloc[0])
    sample_every = max(1, int(frame_rate / params.sample_fps))

    # Get unique (period, frame) combinations, then sample
    period_frames = pd.DataFrame(tracking_df[["period", "frame"]].drop_duplicates()).sort_values(by=["period", "frame"])
    sampled_pf = period_frames.iloc[::sample_every]

    # Accumulate per-player xT
    player_xt: dict[str, float] = {}
    player_count: dict[str, int] = {}
    frames_sampled = 0

    for _, pf_row in sampled_pf.iterrows():
        period = pf_row["period"]
        frame = pf_row["frame"]
        frame_df = pd.DataFrame(tracking_df[(tracking_df["period"] == period) & (tracking_df["frame"] == frame)])

        if frame_df.empty:
            continue

        # Only process frames with players from both teams
        teams_present = list(frame_df["team"].unique())
        if len(teams_present) < 2:
            continue

        frame_results = compute_off_ball_xt_frame(frame_df, xt_grid, pitch_control_params)
        frames_sampled += 1

        for _, row in frame_results.iterrows():
            pid = str(row["player_id"])
            xt_contribution = float(row["off_ball_xt"])
            # Skip NaN contributions — pitch control can produce NaN at
            # boundary conditions (e.g., first frame with no velocity data).
            # Without this guard, a single NaN poisons the entire sum.
            if math.isnan(xt_contribution):
                continue
            player_xt[pid] = player_xt.get(pid, 0.0) + xt_contribution
            player_count[pid] = player_count.get(pid, 0) + 1

    # Build output
    rows: list[dict[str, object]] = []
    for pid, total_xt in player_xt.items():
        count = player_count[pid]
        rows.append(
            {
                "player_id": pid,
                "match_id": match_id,
                "total_off_ball_xt": total_xt,
                "avg_off_ball_xt": total_xt / count if count > 0 else 0.0,
                "frames_sampled": frames_sampled,
            }
        )

    return pd.DataFrame(rows)
