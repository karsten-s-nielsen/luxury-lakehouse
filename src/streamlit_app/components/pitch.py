"""mplsoccer pitch wrappers for shot, pass, and pitch control visualizations."""

from __future__ import annotations

from typing import Any

import matplotlib.figure
import matplotlib.patches as mpatches
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
_LINE_BREAKING_COLOR = "#f4a261"
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


def categorize_passes(
    passes: pd.DataFrame,
    highlight_progressive: bool = True,
    highlight_line_breaking: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Categorize passes into rendering groups: incomplete, complete, progressive, line-breaking.

    Incomplete passes are always categorized as incomplete regardless of is_progressive
    or is_line_breaking flags. Progressive and line-breaking categories only apply to
    completed passes.

    Returns (incomplete, complete, progressive, line_breaking) DataFrames.
    """
    has_lb = highlight_line_breaking and "is_line_breaking" in passes.columns

    # Split by completion first — incomplete passes are always grey
    if "is_complete" in passes.columns:
        incomplete = pd.DataFrame(passes[passes["is_complete"] != 1])
        completed = pd.DataFrame(passes[passes["is_complete"] == 1])
    else:
        incomplete = pd.DataFrame()
        completed = passes

    # Among completed passes, apply line-breaking > progressive > complete hierarchy
    if has_lb:
        lb = pd.DataFrame(completed[completed["is_line_breaking"] == 1])
        remaining = pd.DataFrame(completed[completed["is_line_breaking"] != 1])
    else:
        lb = pd.DataFrame()
        remaining = completed

    if highlight_progressive and "is_progressive" in remaining.columns:
        prog = pd.DataFrame(remaining[remaining["is_progressive"] == 1])
        complete = pd.DataFrame(remaining[remaining["is_progressive"] != 1])
    else:
        prog = pd.DataFrame()
        complete = remaining

    return incomplete, complete, prog, lb


def plot_pass_map(
    passes: pd.DataFrame,
    highlight_progressive: bool = True,
    highlight_line_breaking: bool = True,
    title: str = "Pass Map",
) -> matplotlib.figure.Figure:
    """Plot passes as arrows on a full pitch.

    Expected columns: start_x, start_y, end_x, end_y, is_complete, is_progressive.
    Optional: is_line_breaking (for line-breaking highlight).
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

    incomplete, complete, prog, lb = categorize_passes(passes, highlight_progressive, highlight_line_breaking)

    for subset, color, alpha, width in [
        (incomplete, _INCOMPLETE_COLOR, 0.3, 1.0),
        (complete, _COMPLETE_COLOR, 0.5, 1.5),
        (prog, _PROGRESSIVE_COLOR, 0.8, 2.0),
        (lb, _LINE_BREAKING_COLOR, 0.9, 2.5),
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

    # Build legend showing active pass categories
    legend_entries: list[tuple[str, str, float]] = [
        ("Incomplete", _INCOMPLETE_COLOR, 0.5),
        ("Complete", _COMPLETE_COLOR, 0.7),
    ]
    if highlight_progressive:
        legend_entries.append(("Progressive", _PROGRESSIVE_COLOR, 0.9))
    if highlight_line_breaking and "is_line_breaking" in passes.columns:
        legend_entries.append(("Line-Breaking", _LINE_BREAKING_COLOR, 0.95))

    handles = [mpatches.Patch(color=c, alpha=a, label=lbl) for lbl, c, a in legend_entries]
    ax.legend(
        handles=handles,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=len(handles),
        fontsize=9,
        frameon=False,
        labelcolor=_LINE_COLOR,
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


def _draw_players_and_ball(
    pitch_obj: Pitch,
    ax: Any,
    players: pd.DataFrame,
    ball_x: float | None = None,
    ball_y: float | None = None,
    show_velocity: bool = False,
) -> None:
    """Draw player scatter, ball marker, and optional velocity arrows.

    Shared helper used by both Voronoi and physics pitch control plots.
    """
    home = players[players["team"] == "home"]
    away = players[players["team"] == "away"]

    if not home.empty:
        pitch_obj.scatter(
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
        pitch_obj.scatter(
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

    if ball_x is not None and ball_y is not None:
        pitch_obj.scatter(
            [ball_x], [ball_y], color=_BALL_COLOR, s=200, edgecolors="white", linewidth=1.5, ax=ax, zorder=4, marker="h"
        )

    if show_velocity and "velocity_x" in players.columns and "velocity_y" in players.columns:
        vel_df = players.dropna(subset=["velocity_x", "velocity_y"])
        if not vel_df.empty:
            scale = 2.0
            pitch_obj.arrows(
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

    _draw_players_and_ball(pitch, ax, players, ball_x, ball_y, show_velocity)

    ax.set_xlim(-2, 122)
    ax.set_ylim(82, -2)

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig


def plot_physics_pitch_control(
    players: pd.DataFrame,
    control_surface: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
    ball_x: float | None = None,
    ball_y: float | None = None,
    show_velocity: bool = False,
    title: str = "Pitch Control (Physics)",
) -> matplotlib.figure.Figure:
    """Plot physics-based pitch control with continuous heatmap overlay.

    Parameters
    ----------
    players : DataFrame with x, y, team, player_id (and optionally velocity_x, velocity_y).
    control_surface : (ny, nx) array from compute_pitch_control_frame, values in [0, 1].
    grid_x : (nx,) StatsBomb x-coordinates of grid columns.
    grid_y : (ny,) StatsBomb y-coordinates of grid rows.
    ball_x, ball_y : Optional ball position in StatsBomb coordinates.
    show_velocity : If True, draw velocity arrows.
    title : Plot title.

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

    # Continuous heatmap overlay using imshow
    # surface: 1.0 = home (blue), 0.0 = away (red), 0.5 = contested (white)
    cmap = plt.get_cmap("RdBu")
    x_min, x_max = float(grid_x[0]), float(grid_x[-1])
    y_min, y_max = float(grid_y[0]), float(grid_y[-1])
    im = ax.imshow(
        control_surface,
        extent=[x_min, x_max, y_min, y_max],
        origin="lower",
        cmap=cmap,
        vmin=0.0,
        vmax=1.0,
        alpha=0.5,
        aspect="auto",
        zorder=1,
        interpolation="bilinear",
    )

    # Colorbar
    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_ticks([0.0, 0.5, 1.0])
    cbar.set_ticklabels(["Away", "Contested", "Home"])
    cbar.ax.tick_params(colors=_LINE_COLOR, labelsize=9)

    _draw_players_and_ball(pitch, ax, players, ball_x, ball_y, show_velocity)

    ax.set_xlim(-2, 122)
    ax.set_ylim(82, -2)

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


def plot_pass_network_interactive(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    title: str = "Pass Network",
) -> Any:
    """Plot an interactive pass network using Plotly with hover tooltips.

    Expected node columns: player_id, player_display_name, avg_x, avg_y, pass_count.
    Expected edge columns: passer_id, receiver_id, pair_count.
    Returns a plotly Figure.
    """
    import plotly.graph_objects as go

    fig = go.Figure()

    # --- Pitch markings (StatsBomb 120x80) ---
    pitch_shapes: list[dict[str, Any]] = []
    line_opts: dict[str, Any] = {"color": _LINE_COLOR, "width": 1}

    # Outer boundary
    pitch_shapes.append({"type": "rect", "x0": 0, "y0": 0, "x1": 120, "y1": 80, "line": line_opts})
    # Halfway line
    pitch_shapes.append({"type": "line", "x0": 60, "y0": 0, "x1": 60, "y1": 80, "line": line_opts})
    # Left penalty area (18-yard box)
    pitch_shapes.append({"type": "rect", "x0": 0, "y0": 18, "x1": 18, "y1": 62, "line": line_opts})
    # Right penalty area
    pitch_shapes.append({"type": "rect", "x0": 102, "y0": 18, "x1": 120, "y1": 62, "line": line_opts})
    # Left 6-yard box
    pitch_shapes.append({"type": "rect", "x0": 0, "y0": 30, "x1": 6, "y1": 50, "line": line_opts})
    # Right 6-yard box
    pitch_shapes.append({"type": "rect", "x0": 114, "y0": 30, "x1": 120, "y1": 50, "line": line_opts})
    # Centre circle
    pitch_shapes.append(
        {"type": "circle", "x0": 60 - 10, "y0": 40 - 10, "x1": 60 + 10, "y1": 40 + 10, "line": line_opts}
    )

    # --- Edges + midpoint hover targets ---
    edge_mid_x: list[float] = []
    edge_mid_y: list[float] = []
    edge_hover: list[str] = []

    if not edges.empty and not nodes.empty:
        node_pos = nodes.set_index("player_id")[["avg_x", "avg_y", "player_display_name"]]
        max_pair = edges["pair_count"].max()
        min_pair = edges["pair_count"].min()
        pair_range = max(max_pair - min_pair, 1)

        for _, edge in edges.iterrows():
            pid = edge["passer_id"]
            rid = edge["receiver_id"]
            if pid not in node_pos.index or rid not in node_pos.index:
                continue

            px, py = float(node_pos.loc[pid, "avg_x"]), float(node_pos.loc[pid, "avg_y"])
            rx, ry = float(node_pos.loc[rid, "avg_x"]), float(node_pos.loc[rid, "avg_y"])
            p_name = str(node_pos.loc[pid, "player_display_name"])
            r_name = str(node_pos.loc[rid, "player_display_name"])
            count = int(edge["pair_count"])
            weight = (count - min_pair) / pair_range
            width = 1 + weight * 6
            opacity = 0.3 + weight * 0.5

            # Visible line (hover disabled — midpoint marker handles it)
            fig.add_trace(
                go.Scatter(
                    x=[px, rx],
                    y=[py, ry],
                    mode="lines",
                    line={"color": _NETWORK_EDGE_COLOR, "width": width},
                    opacity=opacity,
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

            # Collect midpoint for hover
            edge_mid_x.append((px + rx) / 2)
            edge_mid_y.append((py + ry) / 2)
            edge_hover.append(f"{p_name} \u2192 {r_name}<br>Passes: {count}")

    # Invisible midpoint markers for edge hover
    if edge_mid_x:
        fig.add_trace(
            go.Scatter(
                x=edge_mid_x,
                y=edge_mid_y,
                mode="markers",
                marker={"size": 15, "color": "rgba(0,0,0,0)"},
                hoverinfo="text",
                hovertext=edge_hover,
                showlegend=False,
            )
        )

    # --- Nodes ---
    if not nodes.empty:
        max_passes = nodes["pass_count"].max()
        min_passes = nodes["pass_count"].min()
        pass_range = max(max_passes - min_passes, 1)
        sizes = 10 + (nodes["pass_count"] - min_passes) / pass_range * 25

        fig.add_trace(
            go.Scatter(
                x=nodes["avg_x"],
                y=nodes["avg_y"],
                mode="markers+text",
                marker={
                    "size": sizes,
                    "color": _NETWORK_NODE_COLOR,
                    "line": {"color": _LINE_COLOR, "width": 1},
                },
                text=nodes["player_display_name"],
                textposition="top center",
                textfont={"color": "white", "size": 10},
                hoverinfo="text",
                hovertext=[
                    f"{row['player_display_name']}<br>Involvements: {row['pass_count']}" for _, row in nodes.iterrows()
                ],
                showlegend=False,
            )
        )

    fig.update_layout(
        title={"text": title, "font": {"color": _LINE_COLOR, "size": 18}, "x": 0.5},
        plot_bgcolor=_BG_COLOR,
        paper_bgcolor=_BG_COLOR,
        xaxis={
            "range": [-2, 122],
            "showgrid": False,
            "zeroline": False,
            "showticklabels": False,
            "constrain": "domain",
        },
        yaxis={
            "range": [-2, 82],
            "showgrid": False,
            "zeroline": False,
            "showticklabels": False,
            "scaleanchor": "x",
            "scaleratio": 1,
        },
        shapes=pitch_shapes,
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
        hoverlabel={"bgcolor": "#333355", "font_size": 13, "font_color": "white"},
        height=700,
    )

    return fig
