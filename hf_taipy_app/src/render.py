"""Shared rendering helpers — pitch-to-file, chart-to-file, constants."""

from __future__ import annotations

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


def fmt_int(value: int) -> str:
    """Format integer with thousands separators."""
    return f"{value:,}"


_TMP_DIR = tempfile.gettempdir()

# Monotonic counter for cache-busting file paths.
# Taipy only re-renders an <|image|> element when the bound state variable
# *value* changes.  If we reuse the same file path, Taipy sees an identical
# string and skips the update even though the file contents have changed.
_render_counter: int = 0


def _unique_path(name: str) -> str:
    """Return a temp file path with a cache-busting suffix."""
    global _render_counter
    _render_counter += 1
    ts = int(time.time() * 1000) % 1_000_000
    return os.path.join(_TMP_DIR, f"{name}_{ts}_{_render_counter}.png")


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
