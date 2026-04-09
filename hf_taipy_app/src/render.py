"""Shared rendering helpers — pitch-to-file, chart-to-file, constants."""

from __future__ import annotations

import atexit
import os
import tempfile
import time

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")

# Theme constants
PITCH_BG_COLOR = "#1a1a2e"
PITCH_LINE_COLOR = "#e0e0e0"
AMBER = "#f59e0b"
AMBER_DARK = "#d97706"
GRAY = "#888888"
TEXT_COLOR = "#e0e0e0"
PLAYER_COLORS = ["#e63946", "#457b9d", "#2a9d8f"]

# Home / Away — canonical convention across all pages and surfaces.
# Red = Home, Steel-blue = Away (Kirk audit K-2).
HOME_COLOR = "#e63946"
AWAY_COLOR = "#457b9d"
# Plotly rgba variants for team shape hulls and lines
HULL_HOME_COLOR = "rgba(230,57,70,0.15)"
HULL_AWAY_COLOR = "rgba(69,123,157,0.15)"
LINE_HOME_COLOR = "rgba(230,57,70,0.5)"
LINE_AWAY_COLOR = "rgba(69,123,157,0.5)"

# DEFCON credit type colours — canonical mapping across surfaces.
# Semantic: red = decisive intercept, orange = negative concede,
# steel-blue = disrupt, teal = deter (Kirk audit K-1).
DEFCON_COLORS = {
    "Intercept": "#e63946",
    "Concede": "#f4a261",
    "Disturb": "#457b9d",
    "Deter": "#2a9d8f",
}


def fmt_int(value: int) -> str:
    """Format integer with thousands separators."""
    return f"{value:,}"


_TMP_DIR = tempfile.gettempdir()

# Monotonic counter for cache-busting file paths.
# Taipy only re-renders an <|image|> element when the bound state variable
# *value* changes.  If we reuse the same file path, Taipy sees an identical
# string and skips the update even though the file contents have changed.
_render_counter: int = 0

# Track temp files for cleanup on process exit (A28/OB2).
_temp_files: list[str] = []


def _cleanup_temp_files() -> None:
    """Remove accumulated temp files on process exit."""
    for path in _temp_files:
        try:
            os.unlink(path)
        except OSError:
            pass


atexit.register(_cleanup_temp_files)


def _unique_path(name: str) -> str:
    """Return a temp file path with a cache-busting suffix."""
    global _render_counter
    _render_counter += 1
    ts = int(time.time() * 1000) % 1_000_000
    path = os.path.join(_TMP_DIR, f"{name}_{ts}_{_render_counter}.png")
    _temp_files.append(path)
    return path


def pitch_to_file(fig: matplotlib.figure.Figure, name: str) -> str:
    """Save a matplotlib figure to a temp PNG at 150 DPI, return the file path."""
    path = _unique_path(name)
    fig.savefig(path, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


def chart_to_file(fig: matplotlib.figure.Figure, name: str) -> str:
    """Save a matplotlib chart to a temp PNG at 150 DPI, return the file path."""
    path = _unique_path(name)
    fig.savefig(path, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return path
