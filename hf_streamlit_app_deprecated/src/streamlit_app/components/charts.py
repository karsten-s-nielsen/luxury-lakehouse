"""matplotlib chart wrappers for radar, bar, scatter, physical, and VAEP visualizations."""

from __future__ import annotations

from typing import Any

import matplotlib.colors as mcolors
import matplotlib.figure
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
    player_names: list[str] | None = None,
) -> matplotlib.figure.Figure:
    """Plot a radar chart comparing 1-3 players across metrics.

    Args:
        players: List of dicts mapping metric keys to values (1-3 players).
        metrics: List of metric column names (order matches labels/ranges).
        labels: Display labels for each metric spoke.
        ranges: (low, high) tuple per metric for normalization.
        title: Chart title.
        player_names: Display names for legend (must match len of players).

    Returns a matplotlib Figure.
    """
    low = [r[0] for r in ranges]
    high = [r[1] for r in ranges]

    radar = Radar(labels, low, high, round_int=[False] * len(labels), num_rings=4)

    result: Any = radar.setup_axis(figsize=(6, 6), facecolor=_BG_COLOR)
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    radar.draw_circles(ax=ax, facecolor=_BG_COLOR, edgecolor="#333355")

    radar.draw_param_labels(ax=ax, color=_LINE_COLOR, fontsize=8)

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

    # Legend mapping colors to player names
    if player_names:
        handles = [
            mpatches.Patch(color=_PLAYER_COLORS[i % len(_PLAYER_COLORS)], alpha=0.6, label=name)
            for i, name in enumerate(player_names[: len(players)])
        ]
        ax.legend(
            handles=handles,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.06),
            ncol=len(handles),
            fontsize=8,
            frameon=False,
            labelcolor=_LINE_COLOR,
        )

    ax.set_title(title, color=_LINE_COLOR, fontsize=11, pad=15, fontweight="bold")
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
    ax.barh(y_pos - 0.15, away_vals, height=0.3, color="#457b9d", alpha=0.85, label=away_name, hatch="///")

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


def plot_stat_group_bars(
    home_vals: list[float],
    away_vals: list[float],
    labels: list[str],
    home_name: str = "Home",
    away_name: str = "Away",
    title: str = "",
) -> matplotlib.figure.Figure:
    """Plot a small-multiples horizontal bar chart for one stat group.

    Unlike ``plot_match_comparison_bars``, this renders a single group
    of related stats on their own scale — avoiding the Cleveland & McGill
    violation of mixing incompatible scales on a shared axis (H15).

    Returns a matplotlib Figure.
    """
    fig, ax = plt.subplots(figsize=(6, max(2, len(labels) * 0.6)))
    fig.set_facecolor(_BG_COLOR)
    ax.set_facecolor(_BG_COLOR)

    y_pos = np.arange(len(labels))

    ax.barh(y_pos + 0.15, home_vals, height=0.3, color="#e63946", alpha=0.85, label=home_name)
    ax.barh(y_pos - 0.15, away_vals, height=0.3, color="#457b9d", alpha=0.85, label=away_name, hatch="///")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, color=_LINE_COLOR, fontsize=10)
    ax.tick_params(axis="x", colors=_LINE_COLOR)
    ax.legend(loc="lower right", facecolor=_BG_COLOR, edgecolor="#333355", labelcolor=_LINE_COLOR, fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")

    if title:
        ax.set_title(title, color=_LINE_COLOR, fontsize=11, pad=8, fontweight="bold")
    plt.tight_layout()
    plt.close(fig)
    return fig


def plot_action_value_timeline(
    actions: pd.DataFrame,
    title: str = "Action Value Timeline",
) -> matplotlib.figure.Figure:
    """Plot VAEP values over match time as a scatter chart.

    Args:
        actions: DataFrame with ``time_seconds`` and ``vaep_value`` columns.
        title: Chart title.

    Returns a matplotlib Figure.
    """
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.set_facecolor(_BG_COLOR)
    ax.set_facecolor(_BG_COLOR)

    if not actions.empty and "time_seconds" in actions.columns and "vaep_value" in actions.columns:
        minutes = actions["time_seconds"] / 60.0
        values = actions["vaep_value"]

        # Diverging colormap: blue (negative) → white → orange (positive) — colorblind-safe (F13)
        cmap = mcolors.LinearSegmentedColormap.from_list("vaep", ["#457b9d", "#ffffff", "#e76f51"])
        v_abs_max = float(max(abs(values.min()), abs(values.max()), 0.01))
        norm = mcolors.TwoSlopeNorm(vmin=-v_abs_max, vcenter=0, vmax=v_abs_max)

        # Marker differentiation: positive = triangle-up, negative = triangle-down (WCAG 1.4.1)
        pos_mask = values >= 0
        neg_mask = ~pos_mask
        if pos_mask.any():
            ax.scatter(
                minutes[pos_mask],
                values[pos_mask],
                c=values[pos_mask],
                cmap=cmap,
                norm=norm,
                s=14,
                alpha=0.7,
                edgecolors="none",
                marker="^",
            )
        if neg_mask.any():
            ax.scatter(
                minutes[neg_mask],
                values[neg_mask],
                c=values[neg_mask],
                cmap=cmap,
                norm=norm,
                s=14,
                alpha=0.7,
                edgecolors="none",
                marker="v",
            )

        # Halftime line
        ax.axvline(x=45, color="#555577", linestyle="--", linewidth=1, alpha=0.6)
        # Zero line
        ax.axhline(y=0, color="#555577", linestyle="-", linewidth=0.5, alpha=0.5)

    ax.set_xlabel("Match Minute", color=_LINE_COLOR, fontsize=11)
    ax.set_ylabel("VAEP Value", color=_LINE_COLOR, fontsize=11)
    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10, fontweight="bold")
    ax.tick_params(axis="both", colors=_LINE_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()
    plt.close(fig)
    return fig


def plot_action_type_breakdown(
    action_types: pd.DataFrame,
    title: str = "VAEP by Action Type",
) -> matplotlib.figure.Figure:
    """Plot total VAEP by action type as a horizontal bar chart.

    Args:
        action_types: DataFrame with ``action_type`` and ``total_vaep`` columns.
        title: Chart title.

    Returns a matplotlib Figure.
    """
    fig, ax = plt.subplots(figsize=(10, max(4, len(action_types) * 0.4)))
    fig.set_facecolor(_BG_COLOR)
    ax.set_facecolor(_BG_COLOR)

    if not action_types.empty and "action_type" in action_types.columns and "total_vaep" in action_types.columns:
        # Sort by absolute value descending
        sorted_df = action_types.sort_values("total_vaep", key=abs, ascending=True)
        colors = ["#2a9d8f" if v >= 0 else "#e63946" for v in sorted_df["total_vaep"]]

        ax.barh(sorted_df["action_type"], sorted_df["total_vaep"], color=colors, alpha=0.85)
        ax.axvline(x=0, color="#555577", linestyle="-", linewidth=0.5)

    ax.set_xlabel("Total VAEP", color=_LINE_COLOR, fontsize=11)
    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10, fontweight="bold")
    ax.tick_params(axis="both", colors=_LINE_COLOR, labelcolor=_LINE_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()
    plt.close(fig)
    return fig


def plot_physical_bars(
    data: pd.DataFrame,
    metric: str,
    label: str,
    title: str = "Physical Performance",
    label_col: str = "player_id",
) -> matplotlib.figure.Figure:
    """Plot a horizontal bar chart of a physical metric per player.

    Args:
        data: DataFrame with ``player_id`` (or ``label_col``) and the given metric column.
        metric: Column name to plot (e.g., ``total_distance_km``).
        label: X-axis label for the metric.
        title: Chart title.
        label_col: Column to use for Y-axis labels (default: ``player_id``).
            Pass ``player_display_name`` when available to show human-readable names.

    Returns a matplotlib Figure.
    """
    # Cap at 20 players to prevent label overflow (L4)
    display_data = data.head(20) if len(data) > 20 else data
    n = len(display_data) if not display_data.empty else 1
    fig, ax = plt.subplots(figsize=(8, max(2, min(n * 0.22, 6))), dpi=72)
    fig.set_facecolor(_BG_COLOR)
    ax.set_facecolor(_BG_COLOR)

    if not display_data.empty and metric in display_data.columns:
        sorted_df = display_data.sort_values(metric, ascending=True)
        # Use human-readable labels when available, fall back to player_id
        if label_col in sorted_df.columns:
            player_labels = sorted_df[label_col].astype(str)
        else:
            player_labels = sorted_df["player_id"].astype(str)
        values = sorted_df[metric].astype(float)
        ax.barh(player_labels, values, color="#2a9d8f", alpha=0.85, height=0.6)

    if len(data) > 20:
        ax.annotate(
            f"Showing top 20 of {len(data)} players",
            xy=(0.5, -0.06),
            xycoords="axes fraction",
            ha="center",
            fontsize=7,
            color="#888888",
        )

    ax.set_xlabel(label, color=_LINE_COLOR, fontsize=8)
    ax.set_title(title, color=_LINE_COLOR, fontsize=10, pad=6, fontweight="bold")
    ax.tick_params(axis="both", colors=_LINE_COLOR, labelcolor=_LINE_COLOR, labelsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()
    plt.close(fig)
    return fig


def plot_ppda_bars(
    data: pd.DataFrame,
    title: str = "PPDA by Match",
) -> matplotlib.figure.Figure:
    """Plot grouped bar chart of home vs away PPDA per match.

    Args:
        data: DataFrame with ``match_id``, ``home_ppda``, ``away_ppda``,
            ``home_team_name``, ``away_team_name`` columns.
        title: Chart title.

    Returns a matplotlib Figure.
    """
    # Limit to most recent matches to keep chart readable
    plot_data = data.tail(25) if len(data) > 25 else data

    fig, ax = plt.subplots(figsize=(12, max(4, min(len(plot_data) * 0.5, 14))))
    fig.set_facecolor(_BG_COLOR)
    ax.set_facecolor(_BG_COLOR)

    if not plot_data.empty and "home_ppda" in plot_data.columns and "away_ppda" in plot_data.columns:
        labels = [
            f"{row.get('home_team_name', 'Home')} v {row.get('away_team_name', 'Away')}"
            for _, row in plot_data.iterrows()
        ]
        y_pos = np.arange(len(labels))
        home_vals = plot_data["home_ppda"].fillna(0).astype(float)
        away_vals = plot_data["away_ppda"].fillna(0).astype(float)

        ax.barh(y_pos + 0.15, home_vals, height=0.3, color="#e63946", alpha=0.85, label="Home PPDA")
        ax.barh(y_pos - 0.15, away_vals, height=0.3, color="#457b9d", alpha=0.85, label="Away PPDA")

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, color=_LINE_COLOR, fontsize=9)
        ax.legend(loc="upper right", facecolor=_BG_COLOR, edgecolor="#333355", labelcolor=_LINE_COLOR)

        if len(data) > 25:
            ax.annotate(
                f"Showing last 25 of {len(data)} matches",
                xy=(0.5, -0.04),
                xycoords="axes fraction",
                ha="center",
                fontsize=8,
                color="#888888",
            )

    ax.set_xlabel("PPDA (lower = more aggressive press)", color=_LINE_COLOR, fontsize=11)
    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10, fontweight="bold")
    ax.tick_params(axis="x", colors=_LINE_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()
    plt.close(fig)
    return fig
