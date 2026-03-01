"""mplsoccer pitch wrappers for shot, pass, and pitch control visualizations."""

from __future__ import annotations

from typing import Any

import matplotlib.figure
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from mplsoccer import Pitch, VerticalPitch
from scipy.spatial import Voronoi  # type: ignore[import-untyped]

# Dark theme colors
_BG_COLOR = "#1a1a2e"
_LINE_COLOR = "#e0e0e0"
_GOAL_COLOR = "#e63946"
_NO_GOAL_COLOR = "#457b9d"
_PROGRESSIVE_COLOR = "#2a9d8f"
_COMPLETE_COLOR = "#457b9d"
_INCOMPLETE_COLOR = "#6c757d"
_HOME_COLOR = "#457b9d"
_AWAY_COLOR = "#e63946"
_BALL_COLOR = "#f4d03f"
_NETWORK_NODE_COLOR = "#f4d03f"
_NETWORK_EDGE_COLOR = "#e0e0e0"


def plot_shot_map(shots: pd.DataFrame, title: str = "Shot Map") -> matplotlib.figure.Figure:
    """Plot shots on a half-pitch, sized by xG and colored by outcome.

    Expected columns: location_x, location_y, statsbomb_xg, is_goal.
    Returns a matplotlib Figure.
    """
    pitch = VerticalPitch(half=True, pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(8, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if shots.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        return fig

    goals = shots[shots["is_goal"] == 1]
    no_goals = shots[shots["is_goal"] != 1]

    # Scale marker size by xG (min 20, max 500)
    def _sizes(df: Any) -> Any:
        return (df["statsbomb_xg"].fillna(0.05) * 500).clip(lower=20, upper=500)

    if not no_goals.empty:
        pitch.scatter(
            no_goals["location_x"],
            no_goals["location_y"],
            s=_sizes(no_goals),
            color=_NO_GOAL_COLOR,
            alpha=0.6,
            edgecolors=_LINE_COLOR,
            linewidth=0.5,
            ax=ax,
            zorder=2,
        )
    if not goals.empty:
        pitch.scatter(
            goals["location_x"],
            goals["location_y"],
            s=_sizes(goals),
            color=_GOAL_COLOR,
            alpha=0.9,
            edgecolors=_LINE_COLOR,
            linewidth=0.5,
            ax=ax,
            zorder=3,
            marker="*",
        )

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig


def plot_pass_map(
    passes: pd.DataFrame,
    highlight_progressive: bool = True,
    title: str = "Pass Map",
) -> matplotlib.figure.Figure:
    """Plot passes as arrows on a full pitch.

    Expected columns: start_x, start_y, end_x, end_y, is_complete, is_progressive.
    Returns a matplotlib Figure.
    """
    pitch = Pitch(pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if passes.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        return fig

    if highlight_progressive and "is_progressive" in passes.columns:
        prog = passes[passes["is_progressive"] == 1]
        non_prog = passes[passes["is_progressive"] != 1]
    else:
        prog = pd.DataFrame()
        non_prog = passes

    # Split non-progressive by completion
    complete = non_prog[non_prog["is_complete"] == 1] if "is_complete" in non_prog.columns else non_prog
    incomplete = non_prog[non_prog["is_complete"] != 1] if "is_complete" in non_prog.columns else pd.DataFrame()

    for subset, color, alpha, width in [
        (incomplete, _INCOMPLETE_COLOR, 0.3, 1.0),
        (complete, _COMPLETE_COLOR, 0.5, 1.5),
        (prog, _PROGRESSIVE_COLOR, 0.8, 2.0),
    ]:
        if not subset.empty:
            pitch.arrows(
                subset["start_x"],
                subset["start_y"],
                subset["end_x"],
                subset["end_y"],
                color=color,
                alpha=alpha,
                width=width,
                ax=ax,
                headwidth=5,
                headlength=5,
            )

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig


def _clip_voronoi_to_pitch(
    vor: Voronoi,
    pitch_bounds: tuple[float, float, float, float],
) -> list[np.ndarray]:
    """Clip Voronoi regions to a rectangular pitch boundary.

    Returns a list of polygon vertex arrays, one per input point.
    Unbounded regions are clipped to the pitch rectangle.
    """
    x_min, x_max, y_min, y_max = pitch_bounds
    regions: list[np.ndarray] = []

    for point_idx in range(len(vor.points)):
        region_idx = vor.point_region[point_idx]
        region = vor.regions[region_idx]

        if not region or -1 in region:
            # Unbounded region — approximate with pitch-sized polygon
            regions.append(np.array([[x_min, y_min], [x_max, y_min], [x_max, y_max], [x_min, y_max]]))
            continue

        polygon = np.array([vor.vertices[i] for i in region])
        # Clip to pitch bounds
        polygon[:, 0] = np.clip(polygon[:, 0], x_min, x_max)
        polygon[:, 1] = np.clip(polygon[:, 1], y_min, y_max)
        regions.append(polygon)

    return regions


def plot_pitch_control(
    players: pd.DataFrame,
    ball_x: float | None = None,
    ball_y: float | None = None,
    show_velocity: bool = False,
    title: str = "Pitch Control",
) -> matplotlib.figure.Figure:
    """Plot Voronoi-based pitch control with player positions.

    Expected columns: x, y, team, player_id.
    Optional columns: velocity_x, velocity_y (for arrow overlay).

    Returns a matplotlib Figure.
    """
    pitch = Pitch(pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if players.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        plt.close(fig)
        return fig

    home = players[players["team"] == "home"]
    away = players[players["team"] == "away"]

    # Voronoi tessellation (requires >= 3 points)
    all_points: np.ndarray = np.asarray(players[["x", "y"]].dropna().values)
    if len(all_points) >= 3:
        vor = Voronoi(all_points)
        pitch_bounds = (0.0, 120.0, 0.0, 80.0)
        clipped_regions = _clip_voronoi_to_pitch(vor, pitch_bounds)

        teams_array = players.loc[players[["x", "y"]].dropna().index, "team"].values
        for i, polygon in enumerate(clipped_regions):
            color = _HOME_COLOR if teams_array[i] == "home" else _AWAY_COLOR
            ax.fill(*polygon.T, alpha=0.15, color=color, zorder=1)

    # Player scatter
    if not home.empty:
        pitch.scatter(
            home["x"],
            home["y"],
            color=_HOME_COLOR,
            s=120,
            edgecolors=_LINE_COLOR,
            linewidth=0.8,
            ax=ax,
            zorder=3,
            label="Home",
        )
    if not away.empty:
        pitch.scatter(
            away["x"],
            away["y"],
            color=_AWAY_COLOR,
            s=120,
            edgecolors=_LINE_COLOR,
            linewidth=0.8,
            ax=ax,
            zorder=3,
            label="Away",
        )

    # Ball position
    if ball_x is not None and ball_y is not None:
        pitch.scatter(
            [ball_x], [ball_y], color=_BALL_COLOR, s=200, edgecolors="white", linewidth=1.5, ax=ax, zorder=4, marker="h"
        )

    # Velocity arrows
    if show_velocity and "velocity_x" in players.columns and "velocity_y" in players.columns:
        vel_df = players.dropna(subset=["velocity_x", "velocity_y"])
        if not vel_df.empty:
            scale = 2.0  # Scale factor for visibility
            pitch.arrows(
                vel_df["x"],
                vel_df["y"],
                vel_df["x"] + vel_df["velocity_x"] * scale,
                vel_df["y"] + vel_df["velocity_y"] * scale,
                color=_LINE_COLOR,
                alpha=0.6,
                width=1.0,
                ax=ax,
                zorder=2,
                headwidth=4,
                headlength=4,
            )

    ax.set_xlim(-2, 122)
    ax.set_ylim(-2, 82)

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig


def plot_heatmap(
    actions: pd.DataFrame,
    title: str = "Heat Map",
    bins: tuple[int, int] = (12, 8),
    cmap: str = "hot",
) -> matplotlib.figure.Figure:
    """Plot action density heat map on a full pitch.

    Expected columns: x, y (generic coordinates in StatsBomb 120x80 system).
    Returns a matplotlib Figure.
    """
    pitch = Pitch(pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if actions.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        plt.close(fig)
        return fig

    bin_stats = pitch.bin_statistic(actions["x"], actions["y"], statistic="count", bins=bins)
    pitch.heatmap(bin_stats, ax=ax, cmap=cmap, edgecolors=_BG_COLOR)

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig


def plot_pass_network(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    title: str = "Pass Network",
) -> matplotlib.figure.Figure:
    """Plot pass network with nodes (players) and edges (pass connections).

    Expected node columns: player_id, player_display_name, avg_x, avg_y, pass_count.
    Expected edge columns: passer_id, receiver_id, pair_count, avg_start_x, avg_start_y,
                           avg_end_x, avg_end_y.
    Returns a matplotlib Figure.
    """
    pitch = Pitch(pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if nodes.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        plt.close(fig)
        return fig

    # Draw edges
    if not edges.empty:
        max_pair = edges["pair_count"].max()
        min_pair = edges["pair_count"].min()
        pair_range = max(max_pair - min_pair, 1)

        for _, edge in edges.iterrows():
            weight = (edge["pair_count"] - min_pair) / pair_range
            lw = 0.5 + weight * 4.5
            alpha = 0.3 + weight * 0.7

            pitch.lines(
                edge["avg_start_x"],
                edge["avg_start_y"],
                edge["avg_end_x"],
                edge["avg_end_y"],
                lw=lw,
                color=_NETWORK_EDGE_COLOR,
                alpha=alpha,
                ax=ax,
                zorder=2,
            )

    # Draw nodes
    max_passes = nodes["pass_count"].max()
    min_passes = nodes["pass_count"].min()
    pass_range = max(max_passes - min_passes, 1)
    sizes = 50 + (nodes["pass_count"] - min_passes) / pass_range * 450

    pitch.scatter(
        nodes["avg_x"],
        nodes["avg_y"],
        s=sizes,
        color=_NETWORK_NODE_COLOR,
        edgecolors=_LINE_COLOR,
        linewidth=0.8,
        ax=ax,
        zorder=3,
    )

    # Labels
    for _, node in nodes.iterrows():
        ax.annotate(
            node["player_display_name"],
            (node["avg_x"], node["avg_y"]),
            color="white",
            fontsize=8,
            ha="center",
            va="bottom",
            xytext=(0, 8),
            textcoords="offset points",
            zorder=4,
        )

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig
