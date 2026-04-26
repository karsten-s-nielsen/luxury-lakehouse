"""Off-Ball Expected Threat (xT) analytics module.

Combines pitch control (Spearman 2017) with Expected Threat zones (Karun Singh
2018) to quantify each player's off-ball positional value. A player's Off-Ball
xT contribution equals the pitch control probability at their location multiplied
by the xT value of their zone.

Concept follows Soccermatics Lesson 7 by David Sumpter.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from analytics.array_utils import _col_f64
from analytics.expected_threat import XTGrid
from analytics.pitch_control import PitchControlParams, compute_pitch_control_at_points


@dataclass(frozen=True)
class OffBallXtParams:
    """Parameters for Off-Ball xT computation.

    Pitch dimensions and coordinate system are carried on the ``XTGrid``
    passed to compute functions, not duplicated here.
    """

    sample_fps: float = 1.0  # Sample 1 frame per second


def compute_off_ball_xt_frame(
    players_df: pd.DataFrame,
    xt_grid: XTGrid,
    pitch_control_params: PitchControlParams | None = None,
) -> pd.DataFrame:
    """Compute per-player Off-Ball xT for a single frame.

    Args:
        players_df: DataFrame with columns [player_id, team, x, y, velocity_x,
            velocity_y]. Player positions in StatsBomb 120x80. Must contain
            players from both teams.
        xt_grid: ``XTGrid`` carrying values + coordinate system. The wrapper
            handles cross-coordinate-system lookup; positions in
            ``players_df`` are interpreted as StatsBomb 120x80.
        pitch_control_params: Optional pitch control parameters.

    Returns:
        DataFrame with columns [player_id, team, x, y, xt_value, pitch_control,
        off_ball_xt] — one row per player.
    """
    if players_df.empty:
        return pd.DataFrame(
            columns=pd.Index(["player_id", "team", "x", "y", "xt_value", "pitch_control", "off_ball_xt"])
        )

    xs = _col_f64(players_df, "x")
    ys = _col_f64(players_df, "y")
    target_points = np.column_stack([xs, ys])  # (N, 2)

    # Single batched call — one matrix setup for all players
    pc_values = compute_pitch_control_at_points(players_df, target_points, pitch_control_params)

    # xT lookup per player (positions are StatsBomb; grid handles conversion)
    xt_values = np.array([xt_grid.lookup(x, y, input_coord_system="statsbomb") for x, y in zip(xs, ys, strict=True)])

    # Adjust PC for away team (pitch control is from home perspective)
    teams = np.asarray(players_df["team"].values)
    adjusted_pc = np.where(teams == "home", pc_values, 1.0 - pc_values)

    return pd.DataFrame(
        {
            "player_id": players_df["player_id"].values,
            "team": teams,
            "x": xs,
            "y": ys,
            "xt_value": xt_values,
            "pitch_control": adjusted_pc,
            "off_ball_xt": xt_values * adjusted_pc,
        }
    )


def compute_off_ball_xt_match(
    tracking_df: pd.DataFrame,
    xt_grid: XTGrid,
    params: OffBallXtParams | None = None,
    pitch_control_params: PitchControlParams | None = None,
) -> pd.DataFrame:
    """Compute per-player Off-Ball xT aggregated over a full match.

    Samples frames at the specified fps rate and aggregates xT contributions
    per player.

    Args:
        tracking_df: Full match tracking DataFrame with columns [player_id,
            team, x, y, velocity_x, velocity_y, frame, period, frame_rate].
        xt_grid: ``XTGrid`` carrying values + coordinate system.
        params: Off-Ball xT parameters (sampling rate).
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

    # Accumulate per-player xT via concat + groupby (vectorized)
    all_frame_results: list[pd.DataFrame] = []
    frames_sampled = 0

    # Pre-build frame index (CLAUDE.md: no boolean mask filter inside loops)
    _frame_groups = dict(iter(tracking_df.groupby(["period", "frame"])))

    for _, pf_row in sampled_pf.iterrows():
        period = pf_row["period"]
        frame = pf_row["frame"]
        frame_df = pd.DataFrame(_frame_groups.get((period, frame), pd.DataFrame()))

        if frame_df.empty:
            continue

        # Only process frames with players from both teams
        teams_present = list(frame_df["team"].unique())
        if len(teams_present) < 2:
            continue

        frame_results = compute_off_ball_xt_frame(frame_df, xt_grid, pitch_control_params)
        frames_sampled += 1
        all_frame_results.append(frame_results)

    if not all_frame_results:
        return pd.DataFrame(
            columns=pd.Index(["player_id", "match_id", "total_off_ball_xt", "avg_off_ball_xt", "frames_sampled"])
        )

    # Concatenate all frame results and aggregate per player
    combined = pd.concat(all_frame_results, ignore_index=True)
    # Filter NaN contributions — pitch control can produce NaN at
    # boundary conditions (e.g., first frame with no velocity data).
    combined = combined[combined["off_ball_xt"].notna()]
    combined["player_id"] = combined["player_id"].astype(str)

    agg = combined.groupby("player_id")["off_ball_xt"].agg(["sum", "count"]).reset_index()
    agg.columns = pd.Index(["player_id", "total_off_ball_xt", "frame_count"])

    # Build output
    rows: list[dict[str, object]] = []
    for _, agg_row in agg.iterrows():
        count = int(agg_row["frame_count"])
        total_xt = float(agg_row["total_off_ball_xt"])
        rows.append(
            {
                "player_id": agg_row["player_id"],
                "match_id": match_id,
                "total_off_ball_xt": total_xt,
                "avg_off_ball_xt": total_xt / count if count > 0 else 0.0,
                "frames_sampled": frames_sampled,
            }
        )

    return pd.DataFrame(rows)
