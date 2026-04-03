"""Shared constants and helpers for line-breaking pass detection.

Used by both the 360 and tracking data paths.
"""

from __future__ import annotations

import json

import pandas as pd

_TABLE_NAME = "line_breaking_results"
_XY_COLS = pd.Index(["x", "y"])
_RESULT_COLUMNS = ["event_id", "match_id", "is_line_breaking", "lines_broken", "line_breaking_type", "data_source"]


# ---------------------------------------------------------------------------
# JSON parsing helpers
# ---------------------------------------------------------------------------


def _parse_location(loc: object) -> tuple[float, float] | None:
    """Parse a location value to ``(x, y)`` tuple.

    Handles JSON strings (``'[x, y]'``) and Python lists/tuples.
    """
    if loc is None:
        return None
    if isinstance(loc, float) and pd.isna(loc):
        return None
    if isinstance(loc, str) and loc.strip() in ("", "null", "None"):
        return None
    if isinstance(loc, (list, tuple)) and len(loc) >= 2:
        return (float(loc[0]), float(loc[1]))
    try:
        coords = json.loads(str(loc))
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            return (float(coords[0]), float(coords[1]))
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None


def _parse_locations_series(loc_series: pd.Series) -> pd.DataFrame:  # type: ignore[type-arg]
    """Parse a Series of location values to a DataFrame with ``x, y`` columns."""
    positions: list[dict[str, float]] = []
    for loc in loc_series:
        parsed = _parse_location(loc)
        if parsed is not None:
            positions.append({"x": parsed[0], "y": parsed[1]})
    return pd.DataFrame(positions) if positions else pd.DataFrame(columns=_XY_COLS)


def _parse_tracking_json(json_str: object) -> pd.DataFrame:
    """Parse a Metrica tracking JSON dict to DataFrame with ``x, y`` columns.

    Input format: ``'{"11": {"x": 0.43, "y": 0.62}, "7": {"x": 0.51, "y": 0.33}}'``

    Returns DataFrame with one row per player.
    """
    if json_str is None:
        return pd.DataFrame(columns=_XY_COLS)
    if isinstance(json_str, float) and pd.isna(json_str):
        return pd.DataFrame(columns=_XY_COLS)
    try:
        players = json.loads(str(json_str))
        if not isinstance(players, dict):
            return pd.DataFrame(columns=_XY_COLS)
        positions: list[dict[str, float]] = []
        for _jersey, coords in players.items():
            if isinstance(coords, dict) and "x" in coords and "y" in coords:
                x_val, y_val = coords["x"], coords["y"]
                if x_val is not None and y_val is not None:
                    positions.append({"x": float(x_val), "y": float(y_val)})
        return pd.DataFrame(positions) if positions else pd.DataFrame(columns=_XY_COLS)
    except (json.JSONDecodeError, ValueError, TypeError):
        return pd.DataFrame(columns=_XY_COLS)
