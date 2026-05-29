"""Position smoothing for tracking data using Savitzky-Golay filtering.

Tracking sensors introduce ~5-10mm RMS noise per frame in x,y positions.
Frame-to-frame differencing amplifies this noise 2x for velocity and 4x for
acceleration.  Smoothing positions once at the ingestion layer makes all
downstream derivatives (speed, acceleration in ``fct_tracking_frames``)
naturally cleaner.

Parameters are tuned for human kinematics:
  - ``window_length=7`` at 25fps ≈ 280ms, at 10fps ≈ 700ms
  - ``polyorder=2`` (quadratic) preserves true acceleration features
"""

from __future__ import annotations

import pandas as pd
from scipy.signal import savgol_filter


def smooth_positions(
    df: pd.DataFrame,
    window_length: int = 7,
    polyorder: int = 2,
    group_cols: tuple[str, ...] = ("player_id", "period"),
    sort_col: str = "frame",
    x_col: str = "x",
    y_col: str = "y",
) -> pd.DataFrame:
    """Apply Savitzky-Golay smoothing to x,y positions per player per period.

    Smoothing is applied independently per group (default: each player in
    each period) to prevent cross-boundary artifacts.  Groups shorter than
    *window_length* are returned unmodified — SavGol requires at least
    *window_length* data points.

    Args:
        df: DataFrame with position columns and grouping columns.
        window_length: Number of frames in the smoothing window (must be odd).
        polyorder: Polynomial order for the local fit.
        group_cols: Columns to group by before smoothing.
        sort_col: Column to sort by within each group (frame order).
        x_col: Name of the x-coordinate column.
        y_col: Name of the y-coordinate column.

    Returns:
        A copy of *df* with smoothed x and y columns, original row order
        preserved.
    """
    if len(df) == 0:
        return df.copy()

    result = df.copy()
    result["_orig_idx"] = range(len(result))

    for _group_key, group in result.groupby(list(group_cols), sort=False):
        if len(group) < window_length:
            continue

        sorted_group = group.sort_values(sort_col)
        idx = sorted_group.index

        result.loc[idx, x_col] = savgol_filter(sorted_group[x_col].to_numpy(), window_length, polyorder)  # type: ignore[call-overload]  # savgol returns ndarray; pandas .loc setitem stub over-narrows the value type
        result.loc[idx, y_col] = savgol_filter(sorted_group[y_col].to_numpy(), window_length, polyorder)  # type: ignore[call-overload]  # savgol returns ndarray; pandas .loc setitem stub over-narrows the value type

    result = result.sort_values("_orig_idx").drop(columns="_orig_idx").reset_index(drop=True)
    return result
