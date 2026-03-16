"""ELASTIC event-tracking synchronization analytics module.

Pure compute functions for aligning event data with tracking frames using
ball acceleration spikes and player-ball proximity features. No Spark
dependency — operates on pandas DataFrames.

Implements a simplified version of the ELASTIC algorithm:
  Kim, H.S. et al. (2025). "ELASTIC: Event-Tracking Data Synchronization
  in Soccer Without Annotated Event Locations." ECML-PKDD MLSA 2025.
  arXiv:2508.09238.

The core idea: events in soccer (passes, shots, tackles) cause observable
signatures in tracking data — primarily ball acceleration spikes and
player-ball proximity changes. By matching these signatures, we can align
event timestamps to the exact tracking frame where the event occurred.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ElasticSyncParams:
    """Parameters for the ELASTIC event-tracking sync algorithm."""

    window_seconds: float = 1.0  # Search window: ±1s around event timestamp
    accel_weight: float = 0.6  # Weight for ball acceleration feature
    proximity_weight: float = 0.4  # Weight for player-ball distance feature
    min_confidence: float = 0.1  # Minimum confidence to report an alignment
    frame_rate: int = 25  # Tracking data frame rate (fps)


def _col_f64(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract a DataFrame column as a float64 numpy array (pyright-safe)."""
    return np.asarray(df[col], dtype=np.float64)


def extract_ball_features(
    tracking_df: pd.DataFrame,
    frame_rate: int = 25,
) -> pd.DataFrame:
    """Extract ball acceleration and speed features from tracking data.

    Computes ball velocity and acceleration from consecutive ball positions.
    Ball acceleration spikes are the primary signal for event detection in
    the ELASTIC algorithm — events (passes, shots, tackles) cause abrupt
    changes in ball direction/speed.

    Args:
        tracking_df: Tracking DataFrame with columns ``frame``, ``ball_x``,
            ``ball_y``, and ``period``. Must be sorted by ``(period, frame)``.
        frame_rate: Frames per second in the tracking data.

    Returns:
        DataFrame with columns: ``frame``, ``period``, ``ball_x``, ``ball_y``,
        ``ball_vx``, ``ball_vy``, ``ball_speed``, ``ball_accel``.
        One row per unique (period, frame) combination.
    """
    ball_feature_cols = pd.Index(
        ["frame", "period", "ball_x", "ball_y", "ball_vx", "ball_vy", "ball_speed", "ball_accel"]
    )

    if tracking_df.empty:
        return pd.DataFrame(columns=ball_feature_cols)

    # Deduplicate to one row per (period, frame) — take first ball position
    ball_df = (
        tracking_df[["frame", "period", "ball_x", "ball_y"]]
        .dropna(subset=["ball_x", "ball_y"])  # type: ignore[arg-type]
        .drop_duplicates(subset=["period", "frame"])
        .sort_values(["period", "frame"])
        .reset_index(drop=True)
    )

    if ball_df.empty:
        return pd.DataFrame(columns=ball_feature_cols)

    dt = 1.0 / frame_rate

    # Compute velocity components via finite differences within each period
    ball_x = _col_f64(ball_df, "ball_x")
    ball_y = _col_f64(ball_df, "ball_y")
    periods = _col_f64(ball_df, "period")

    vx = np.zeros_like(ball_x)
    vy = np.zeros_like(ball_y)

    # Mask for same-period consecutive frames
    same_period = periods[1:] == periods[:-1]

    vx[1:] = np.where(same_period, (ball_x[1:] - ball_x[:-1]) / dt, 0.0)
    vy[1:] = np.where(same_period, (ball_y[1:] - ball_y[:-1]) / dt, 0.0)

    speed = np.sqrt(vx**2 + vy**2)

    # Acceleration = change in speed per frame
    accel = np.zeros_like(speed)
    accel[1:] = np.where(same_period, np.abs(speed[1:] - speed[:-1]) / dt, 0.0)

    result = ball_df.copy()
    result["ball_vx"] = vx
    result["ball_vy"] = vy
    result["ball_speed"] = speed
    result["ball_accel"] = accel

    return result


def _build_player_ball_distance_lookup(
    tracking_df: pd.DataFrame,
) -> dict[tuple[int, int, str], float]:
    """Pre-compute player-ball distances for all (period, frame, player) triples.

    Single vectorized pass through the tracking data. Replaces per-event
    ``_compute_player_ball_distance()`` scans -- converts O(events * candidates * rows)
    to O(rows) pre-processing + O(1) per lookup.

    Args:
        tracking_df: Full tracking DataFrame with ``player_id``, ``frame``,
            ``period``, ``x``, ``y``, ``ball_x``, ``ball_y``.

    Returns:
        Dict mapping ``(period, frame, player_id)`` to Euclidean distance.
        Missing ball coordinates produce ``np.inf``.
    """
    if tracking_df.empty:
        return {}

    required_cols = ["period", "frame", "player_id", "x", "y", "ball_x", "ball_y"]
    df = pd.DataFrame(tracking_df[required_cols])

    # Vectorized distance: sqrt((x - ball_x)^2 + (y - ball_y)^2)
    px = _col_f64(df, "x")
    py = _col_f64(df, "y")
    bx = _col_f64(df, "ball_x")
    by = _col_f64(df, "ball_y")

    dist = np.sqrt((px - bx) ** 2 + (py - by) ** 2)

    # NaN ball coords → inf distance
    ball_missing = np.isnan(bx) | np.isnan(by)
    dist[ball_missing] = np.inf

    # Build lookup dict — one entry per row
    periods = np.asarray(df["period"], dtype=np.int64)
    frames = np.asarray(df["frame"], dtype=np.int64)
    player_ids = np.asarray(df["player_id"], dtype=object)

    lookup: dict[tuple[int, int, str], float] = {}
    for i in range(len(dist)):
        key = (int(periods[i]), int(frames[i]), str(player_ids[i]))
        lookup[key] = float(dist[i])

    return lookup


def align_events_to_frames(
    events_df: pd.DataFrame,
    tracking_df: pd.DataFrame,
    frame_rate: int = 25,
    params: ElasticSyncParams | None = None,
) -> pd.DataFrame:
    """Align each event to the best-matching tracking frame.

    Implements the core ELASTIC algorithm (Kim et al. 2025): for each event,
    search a time window around the event timestamp in the tracking data,
    score candidate frames by ball acceleration magnitude and player-ball
    proximity, and return the best match.

    Args:
        events_df: Event DataFrame with columns ``event_id``, ``event_type``,
            ``timestamp_seconds``, ``period``, ``player_id``.
        tracking_df: Tracking DataFrame with columns ``frame``, ``period``,
            ``player_id``, ``x``, ``y``, ``ball_x``, ``ball_y``.
        frame_rate: Frames per second in the tracking data.
        params: Algorithm parameters. Defaults to :class:`ElasticSyncParams`.

    Returns:
        DataFrame with columns: ``event_id``, ``frame_id``,
        ``alignment_confidence``, ``alignment_error_seconds``.
    """
    if params is None:
        params = ElasticSyncParams()

    result_idx = pd.Index(["event_id", "frame_id", "alignment_confidence", "alignment_error_seconds"])

    if events_df.empty or tracking_df.empty:
        return pd.DataFrame(columns=result_idx)

    # Pre-compute ball features from tracking
    ball_features = extract_ball_features(tracking_df, frame_rate)

    if ball_features.empty:
        return pd.DataFrame(columns=result_idx)

    # Pre-build indexed lookups — single pass through data, O(1) access per candidate frame.
    # This replaces the O(n) per-frame _compute_player_ball_distance() scan.

    # Lookup 1: (period, frame) -> ball_accel — from vectorized ball features
    bf_periods = np.asarray(ball_features["period"], dtype=np.int64)
    bf_frames = np.asarray(ball_features["frame"], dtype=np.int64)
    bf_accels = np.asarray(ball_features["ball_accel"], dtype=np.float64)
    accel_lookup: dict[tuple[int, int], float] = {
        (int(bf_periods[i]), int(bf_frames[i])): float(bf_accels[i]) for i in range(len(bf_periods))
    }

    # Lookup 2: (period, frame, player_id) -> distance — vectorized over all rows
    distance_lookup = _build_player_ball_distance_lookup(tracking_df)

    # Pre-build period index (CLAUDE.md: no boolean mask filter inside loops)
    _period_groups = dict(iter(ball_features.groupby("period")))
    frames_by_period: dict[int, np.ndarray] = {
        int(k): np.sort(np.asarray(g["frame"].values, dtype=np.int64))  # type: ignore[arg-type]
        for k, g in _period_groups.items()
    }

    window_frames = int(params.window_seconds * frame_rate)
    results: list[dict[str, object]] = []

    for _, event_row in events_df.iterrows():
        event_id = str(event_row["event_id"])
        event_ts = float(event_row["timestamp_seconds"])
        event_period = int(event_row["period"])
        event_player = str(event_row["player_id"])

        # Find the nominal frame for this event timestamp
        nominal_frame = round(event_ts * frame_rate)

        # Get candidate frames within the search window
        period_frames = frames_by_period.get(event_period)
        if period_frames is None or len(period_frames) == 0:
            continue

        frame_min = nominal_frame - window_frames
        frame_max = nominal_frame + window_frames

        # Binary search for efficient window extraction
        idx_lo = int(np.searchsorted(period_frames, frame_min, side="left"))
        idx_hi = int(np.searchsorted(period_frames, frame_max, side="right"))
        candidate_frames = period_frames[idx_lo:idx_hi]

        if len(candidate_frames) == 0:
            continue

        # Score each candidate frame
        best_frame = int(candidate_frames[0])
        best_score = -1.0

        # Collect acceleration values for normalization
        accels = np.array([accel_lookup.get((event_period, int(f)), 0.0) for f in candidate_frames])
        max_accel = float(np.max(accels)) if len(accels) > 0 else 1.0
        if max_accel < 1e-9:
            max_accel = 1.0

        for i, frame_val in enumerate(candidate_frames):
            frame_int = int(frame_val)

            # Feature 1: Normalized ball acceleration (higher = more likely event boundary)
            accel_score = accels[i] / max_accel

            # Feature 2: Inverse player-ball distance — O(1) dict lookup
            dist = distance_lookup.get((event_period, frame_int, event_player), float(np.inf))
            # Normalize distance: use sigmoid-like mapping, 0m -> 1.0, large -> 0.0
            proximity_score = 1.0 / (1.0 + dist) if dist < float(np.inf) else 0.0

            # Combined score
            score = params.accel_weight * accel_score + params.proximity_weight * proximity_score

            if score > best_score:
                best_score = score
                best_frame = frame_int

        # Compute confidence: normalize score to [0, 1]
        # Max possible score = accel_weight * 1.0 + proximity_weight * 1.0 = 1.0
        max_possible = params.accel_weight + params.proximity_weight
        confidence = min(1.0, max(0.0, best_score / max_possible)) if max_possible > 0 else 0.0

        if confidence < params.min_confidence:
            continue

        # Alignment error: difference between aligned frame time and event timestamp
        aligned_ts = best_frame / frame_rate
        error_seconds = abs(aligned_ts - event_ts)

        results.append(
            {
                "event_id": event_id,
                "frame_id": best_frame,
                "alignment_confidence": round(confidence, 4),
                "alignment_error_seconds": round(error_seconds, 4),
            }
        )

    if not results:
        return pd.DataFrame(columns=result_idx)

    return pd.DataFrame(results, columns=result_idx)
