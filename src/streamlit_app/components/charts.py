"""matplotlib chart wrappers for radar and bar comparison visualizations."""

from __future__ import annotations

from typing import Any

import matplotlib.figure
import matplotlib.pyplot as plt
import numpy as np
from mplsoccer import Radar

_BG_COLOR = "#1a1a2e"
_LINE_COLOR = "#e0e0e0"
_PLAYER_COLORS = ["#e63946", "#457b9d", "#2a9d8f"]


def plot_player_radar(
    players: list[dict[str, float]],
    metrics: list[str],
    labels: list[str],
    ranges: list[tuple[float, float]],
    title: str = "Player Comparison",
) -> matplotlib.figure.Figure:
    """Plot a radar chart comparing 1-3 players across metrics.

    Args:
        players: List of dicts mapping metric keys to values (1-3 players).
        metrics: List of metric column names (order matches labels/ranges).
        labels: Display labels for each metric spoke.
        ranges: (low, high) tuple per metric for normalization.
        title: Chart title.

    Returns a matplotlib Figure.
    """
    low = [r[0] for r in ranges]
    high = [r[1] for r in ranges]

    radar = Radar(labels, low, high, round_int=[False] * len(labels), num_rings=4)

    result: Any = radar.setup_axis(figsize=(8, 8), facecolor=_BG_COLOR)
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    radar.draw_circles(ax=ax, facecolor=_BG_COLOR, edgecolor="#333355")

    radar.draw_param_labels(ax=ax, color=_LINE_COLOR, fontsize=10)

    for i, player in enumerate(players[:3]):
        values = [player.get(m, 0.0) for m in metrics]
        color = _PLAYER_COLORS[i % len(_PLAYER_COLORS)]
        radar.draw_radar(
            values,
            ax=ax,
            kwargs_radar={"facecolor": color, "alpha": 0.2},
            kwargs_rings={"facecolor": color, "alpha": 0.1},
        )
        # Draw the outline
        radar.draw_radar(
            values,
            ax=ax,
            kwargs_radar={"facecolor": "none", "edgecolor": color, "linewidth": 2},
            kwargs_rings={"facecolor": "none"},
        )

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=20, fontweight="bold")
    plt.close(fig)
    return fig


def plot_match_comparison_bars(
    home_vals: list[float],
    away_vals: list[float],
    labels: list[str],
    home_name: str = "Home",
    away_name: str = "Away",
) -> matplotlib.figure.Figure:
    """Plot horizontal bar chart comparing home vs away match stats.

    Returns a matplotlib Figure.
    """
    fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.6)))
    fig.set_facecolor(_BG_COLOR)
    ax.set_facecolor(_BG_COLOR)

    y_pos = np.arange(len(labels))

    ax.barh(y_pos + 0.15, home_vals, height=0.3, color="#e63946", alpha=0.85, label=home_name)
    ax.barh(y_pos - 0.15, away_vals, height=0.3, color="#457b9d", alpha=0.85, label=away_name)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, color=_LINE_COLOR, fontsize=11)
    ax.tick_params(axis="x", colors=_LINE_COLOR)
    ax.legend(loc="lower right", facecolor=_BG_COLOR, edgecolor="#333355", labelcolor=_LINE_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")

    ax.set_title("Match Comparison", color=_LINE_COLOR, fontsize=14, pad=10)
    plt.tight_layout()
    plt.close(fig)
    return fig
