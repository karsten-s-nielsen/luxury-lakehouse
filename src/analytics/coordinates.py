"""Coordinate transformations for multi-provider pitch data.

All providers are normalized to the StatsBomb coordinate system:
  - Pitch: 120 x 80 yards (origin at top-left)
  - x: 0 = own goal line, 120 = opponent goal line
  - y: 0 = right touchline, 80 = left touchline

Supported coordinate systems:

  metrica   — Metrica Sports: [0, 1] normalized, y-axis flipped
              x -> x * 120,  y -> (1 - y) * 80

  center_m  — IDSSE / SkillCorner: center-origin real-world meters
              x in [-52.5, 52.5], y in [-34, 34]
              x -> (x + 52.5) / 105 * 120
              y -> (y + 34)   / 68  * 80

  pitch_m   — Pitch-origin meters (0-105, 0-68), used by IDSSE events
              x -> x / 105 * 120
              y -> y / 68  * 80

  pct       — Percentage [0, 100], used by Wyscout
              x -> x / 100 * 120
              y -> y / 100 * 80

The ``statsbomb_to_meters`` reverse transform converts StatsBomb coordinates
back to real-world meters (0-105, 0-68) for analytics modules that operate in
metric units (e.g., pitch control, team shape distance calculations).

Parallel dbt macro: ``dbt_project/macros/normalize_coordinates.sql``

Reference: docs/coordinate-systems.md
"""

from __future__ import annotations

from typing import TypeVar

_T = TypeVar("_T")

# StatsBomb coordinate extents (yards)
STATSBOMB_LENGTH: float = 120.0
STATSBOMB_WIDTH: float = 80.0

# Real-world pitch dimensions (meters) — FIFA standard
PITCH_LENGTH_M: float = 105.0
PITCH_WIDTH_M: float = 68.0


def metrica_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Convert Metrica Sports [0, 1] coordinates to StatsBomb 120x80.

    Metrica uses a unit-square coordinate system with y-axis pointing
    downward (top of pitch = 0, bottom = 1).  StatsBomb y-axis is
    flipped: top = 80, bottom = 0.

    Args:
        x: Horizontal coordinate(s) in [0, 1].
        y: Vertical coordinate(s) in [0, 1] (flipped).

    Returns:
        Tuple of (x_sb, y_sb) in StatsBomb coordinates.
    """
    x_sb: _T = x * STATSBOMB_LENGTH  # type: ignore[assignment]
    y_sb: _T = (1 - y) * STATSBOMB_WIDTH  # type: ignore[assignment]
    return x_sb, y_sb


def center_m_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Convert center-origin meters to StatsBomb 120x80.

    IDSSE (Bundesliga) tracking and SkillCorner (A-League) tracking use
    center-origin meters: x in [-52.5, 52.5], y in [-34, 34].

    Args:
        x: Horizontal coordinate(s) in meters, center-origin.
        y: Vertical coordinate(s) in meters, center-origin.

    Returns:
        Tuple of (x_sb, y_sb) in StatsBomb coordinates.
    """
    x_sb: _T = (x + PITCH_LENGTH_M / 2) / PITCH_LENGTH_M * STATSBOMB_LENGTH  # type: ignore[assignment]
    y_sb: _T = (y + PITCH_WIDTH_M / 2) / PITCH_WIDTH_M * STATSBOMB_WIDTH  # type: ignore[assignment]
    return x_sb, y_sb


def pitch_m_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Convert pitch-origin meters (0-105, 0-68) to StatsBomb 120x80.

    Used by IDSSE event data where the origin is at the corner of the pitch.

    Args:
        x: Horizontal coordinate(s) in meters [0, 105].
        y: Vertical coordinate(s) in meters [0, 68].

    Returns:
        Tuple of (x_sb, y_sb) in StatsBomb coordinates.
    """
    x_sb: _T = x / PITCH_LENGTH_M * STATSBOMB_LENGTH  # type: ignore[assignment]
    y_sb: _T = y / PITCH_WIDTH_M * STATSBOMB_WIDTH  # type: ignore[assignment]
    return x_sb, y_sb


def pct_to_statsbomb(x: _T, y: _T) -> tuple[_T, _T]:
    """Convert percentage [0, 100] coordinates to StatsBomb 120x80.

    Wyscout uses percentage-based coordinates where (0, 0) is one corner
    and (100, 100) is the diagonally opposite corner.

    Args:
        x: Horizontal coordinate(s) in [0, 100].
        y: Vertical coordinate(s) in [0, 100].

    Returns:
        Tuple of (x_sb, y_sb) in StatsBomb coordinates.
    """
    x_sb: _T = x / 100.0 * STATSBOMB_LENGTH  # type: ignore[assignment]
    y_sb: _T = y / 100.0 * STATSBOMB_WIDTH  # type: ignore[assignment]
    return x_sb, y_sb


def statsbomb_to_meters(x: _T, y: _T) -> tuple[_T, _T]:
    """Convert StatsBomb 120x80 coordinates to real-world meters (0-105, 0-68).

    Reverse of ``pitch_m_to_statsbomb``.  Used by analytics modules that
    compute physical distances (pitch control, team shape, space creation).

    Args:
        x: Horizontal coordinate(s) in StatsBomb units [0, 120].
        y: Vertical coordinate(s) in StatsBomb units [0, 80].

    Returns:
        Tuple of (x_m, y_m) in meters.
    """
    x_m: _T = x / STATSBOMB_LENGTH * PITCH_LENGTH_M  # type: ignore[assignment]
    y_m: _T = y / STATSBOMB_WIDTH * PITCH_WIDTH_M  # type: ignore[assignment]
    return x_m, y_m
