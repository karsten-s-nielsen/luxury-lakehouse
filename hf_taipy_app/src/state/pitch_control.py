"""Pitch Control state module — all variables prefixed with pc_.

Loads tracking frame data for a selected match + frame, computes physics-based
or Voronoi pitch control surfaces, renders to mplsoccer pitch images.
INTEGER slider for elapsed seconds — converts to frame via fps.
Registered as the Pitch-Control page refresher via shared.register_page_refresher.

PR 6 (ADR-011): stg_pitch_control__values now carries match_key + data_source
(promoted to first-class). This module computes pitch-control on-demand from
raw tracking frames (does not consume the staging mart) — annotation only.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from filters import fetch_data_freshness
from mplsoccer import Pitch
from queries.tracking import fetch_pc_frame_data, fetch_pc_frame_range, fetch_pc_frame_rate, fetch_pc_match_label
from render import AWAY_COLOR, HOME_COLOR, PITCH_BG_COLOR, PITCH_LINE_COLOR, pitch_to_file
from scipy.spatial import Voronoi  # type: ignore[import-untyped]

from analytics.pitch_control import compute_pitch_control_at_point, compute_pitch_control_frame
from state.shared import get_tracking_match_id, register_page_refresher

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ── Team / ball colors ───────────────────────────────────────────────────────
_BALL_COLOR = "#f4d03f"
_TEXT_COLOR = "#e0e0e0"

# ── Exported state variables (all pc_ prefixed) ─────────────────────────────

# Time / frame controls
pc_elapsed_seconds: int = 0
pc_time_display: str = "00:00"
pc_min_seconds: int = 0
pc_max_seconds: int = 1
pc_time_label: str = "Time"  # e.g., "Time (25 fps)"
pc_max_time_display: str = "0:00"  # e.g., "47:30" for slider end label

# Page-level controls (NOT sidebar)
pc_half: str = "1st Half"
pc_half_lov: list[str] = ["1st Half", "2nd Half"]
pc_model: str = "Physics-based"
pc_model_lov: list[str] = ["Physics-based", "Voronoi"]
pc_show_velocity: bool = True

# Metrics
pc_player_count: str = "--"
pc_home_control: str = "--"
pc_away_control: str = "--"
pc_control_at_ball: str = "--"
pc_avg_speed: str = "--"
pc_max_speed: str = "--"
pc_avg_dist_to_ball: str = "--"

# Pitch image
pc_pitch_image: str = ""

# Status message (model info for Voronoi)
pc_status: str = ""

pc_data_freshness: str = ""

pc_warning_text: str = ""

__all__ = [
    "pc_avg_dist_to_ball",
    "pc_avg_speed",
    "pc_data_freshness",
    "pc_away_control",
    "pc_control_at_ball",
    "pc_elapsed_seconds",
    "pc_half",
    "pc_half_lov",
    "pc_home_control",
    "pc_max_seconds",
    "pc_max_speed",
    "pc_max_time_display",
    "pc_min_seconds",
    "pc_model",
    "pc_model_lov",
    "pc_on_half_change",
    "pc_on_model_change",
    "pc_on_seconds_change",
    "pc_on_velocity_change",
    "pc_pitch_image",
    "pc_player_count",
    "pc_refresh",
    "pc_show_velocity",
    "pc_status",
    "pc_time_display",
    "pc_time_label",
    "pc_warning_text",
]


# ── Internal frame state (not exported to UI) ───────────────────────────────
_min_frame: int = 0
_max_frame: int = 0
_fps: int = 25
_cached_frame_data: pd.DataFrame = pd.DataFrame()


# ── Voronoi helpers ──────────────────────────────────────────────────────────


def _clip_voronoi_to_pitch(
    vor: Voronoi,
    pitch_bounds: tuple[float, float, float, float],
) -> list[np.ndarray]:
    """Clip Voronoi regions to a rectangular pitch boundary."""
    x_min, x_max, y_min, y_max = pitch_bounds
    regions: list[np.ndarray] = []

    for point_idx in range(len(vor.points)):
        region_idx = vor.point_region[point_idx]
        region = vor.regions[region_idx]

        if not region or -1 in region:
            regions.append(np.array([[x_min, y_min], [x_max, y_min], [x_max, y_max], [x_min, y_max]]))
            continue

        polygon = np.array([vor.vertices[i] for i in region])
        polygon[:, 0] = np.clip(polygon[:, 0], x_min, x_max)
        polygon[:, 1] = np.clip(polygon[:, 1], y_min, y_max)
        regions.append(polygon)

    return regions


# ── Pitch rendering helpers ──────────────────────────────────────────────────


def _draw_players_and_ball(
    pitch_obj: Pitch,
    ax: Any,
    players: pd.DataFrame,
    ball_x: float | None = None,
    ball_y: float | None = None,
    show_velocity: bool = False,
) -> None:
    """Draw player scatter, ball marker, and optional velocity arrows."""
    home = players[players["team"] == "home"]
    away = players[players["team"] == "away"]

    if not home.empty:
        pitch_obj.scatter(
            home["x"],
            home["y"],
            color=HOME_COLOR,
            s=120,
            edgecolors=PITCH_LINE_COLOR,
            linewidth=0.8,
            ax=ax,
            zorder=3,
            label="Home",
            marker="o",
        )
    if not away.empty:
        pitch_obj.scatter(
            away["x"],
            away["y"],
            color=AWAY_COLOR,
            s=120,
            edgecolors=PITCH_LINE_COLOR,
            linewidth=0.8,
            ax=ax,
            zorder=3,
            label="Away",
            marker="s",
        )

    if ball_x is not None and ball_y is not None:
        pitch_obj.scatter(
            [ball_x],
            [ball_y],
            color=_BALL_COLOR,
            s=200,
            edgecolors="white",
            linewidth=1.5,
            ax=ax,
            zorder=4,
            marker="h",
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
                color=PITCH_LINE_COLOR,
                alpha=0.6,
                width=1.0,
                ax=ax,
                zorder=2,
                headwidth=4,
                headlength=4,
            )


def _render_voronoi(
    players: pd.DataFrame,
    ball_x: float | None,
    ball_y: float | None,
    show_velocity: bool,
    title: str,
) -> str:
    """Render Voronoi-based pitch control to temp PNG, return file path."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if players.empty:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "pc_voronoi")

    # Voronoi tessellation (requires >= 3 points)
    all_points: np.ndarray = np.asarray(players[["x", "y"]].dropna().values)
    if len(all_points) >= 3:
        vor = Voronoi(all_points)
        pitch_bounds = (0.0, 120.0, 0.0, 80.0)
        clipped_regions = _clip_voronoi_to_pitch(vor, pitch_bounds)

        teams_array = players.loc[players[["x", "y"]].dropna().index, "team"].values
        for i, polygon in enumerate(clipped_regions):
            color = HOME_COLOR if teams_array[i] == "home" else AWAY_COLOR
            ax.fill(*polygon.T, alpha=0.15, color=color, zorder=1)

    _draw_players_and_ball(pitch, ax, players, ball_x, ball_y, show_velocity)

    ax.set_xlim(-2, 122)
    ax.set_ylim(82, -2)
    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)

    return pitch_to_file(fig, "pc_voronoi")


def _render_physics(
    players: pd.DataFrame,
    surface: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
    ball_x: float | None,
    ball_y: float | None,
    show_velocity: bool,
    title: str,
) -> str:
    """Render physics-based pitch control heatmap to temp PNG, return file path."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if players.empty:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "pc_physics")

    # Continuous heatmap overlay
    cmap = plt.get_cmap("RdBu")
    x_min, x_max = float(grid_x[0]), float(grid_x[-1])
    y_min, y_max = float(grid_y[0]), float(grid_y[-1])
    im = ax.imshow(
        surface,
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
    cbar.set_label("Control (0=Away, 1=Home)", color=_TEXT_COLOR, fontsize=8)
    cbar.set_ticks([0.0, 0.5, 1.0])
    cbar.set_ticklabels(["Away", "Contested", "Home"])
    cbar.ax.tick_params(colors=_TEXT_COLOR, labelsize=7)

    _draw_players_and_ball(pitch, ax, players, ball_x, ball_y, show_velocity)

    ax.set_xlim(-2, 122)
    ax.set_ylim(82, -2)
    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)

    return pitch_to_file(fig, "pc_physics")


# ── Core refresh logic ──────────────────────────────────────────────────────


def _clear_state(state: Any) -> None:
    """Reset all pc_ state to defaults."""
    state.pc_player_count = "--"
    state.pc_home_control = "--"
    state.pc_away_control = "--"
    state.pc_control_at_ball = "--"
    state.pc_avg_speed = "--"
    state.pc_max_speed = "--"
    state.pc_avg_dist_to_ball = "--"
    state.pc_pitch_image = ""
    state.pc_status = ""
    state.pc_time_display = "00:00"
    state.pc_time_label = "Time"
    state.pc_max_time_display = "0:00"


def _refresh_frame(state: Any) -> None:
    """Load frame data and render pitch for the current slider position.

    Called by pc_refresh and by the seconds/model/velocity/half change callbacks.
    """
    global _cached_frame_data

    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_state(state)
        return

    # Compute frame number from elapsed seconds
    elapsed = int(state.pc_elapsed_seconds)
    frame = _min_frame + elapsed * _fps

    # Time display
    mm, ss = divmod(elapsed, 60)
    state.pc_time_display = f"{mm:02d}:{ss:02d}"

    # Fetch frame data
    frame_data = fetch_pc_frame_data(match_id, frame)
    _cached_frame_data = frame_data

    if frame_data.empty:
        _clear_state(state)
        state.pc_status = "No pitch control data for this frame. Try selecting a different frame or half."
        state.pc_warning_text = "No pitch control data for this frame. Try selecting a different frame or half."
        return

    # Extract ball position
    ball_x = float(frame_data.iloc[0]["ball_x"]) if frame_data.iloc[0]["ball_x"] is not None else None
    ball_y = float(frame_data.iloc[0]["ball_y"]) if frame_data.iloc[0]["ball_y"] is not None else None

    # Build title
    period = 1 if state.pc_half == "1st Half" else 2
    match_label = fetch_pc_match_label(match_id)
    title = f"Pitch Control \u2014 {match_label} H{period} {mm:02d}:{ss:02d}"

    show_vel = bool(state.pc_show_velocity)
    model = state.pc_model or "Physics-based"

    if model == "Physics-based":
        # Fill NaN velocities with 0
        physics_data = frame_data.copy()
        physics_data["velocity_x"] = physics_data["velocity_x"].fillna(0.0)
        physics_data["velocity_y"] = physics_data["velocity_y"].fillna(0.0)

        grid_x, grid_y, surface = compute_pitch_control_frame(physics_data)

        state.pc_pitch_image = _render_physics(physics_data, surface, grid_x, grid_y, ball_x, ball_y, show_vel, title)

        # Metrics — physics mode
        home_pct = float(surface.mean()) * 100
        away_pct = 100.0 - home_pct
        state.pc_home_control = f"{home_pct:.1f}%"
        state.pc_away_control = f"{away_pct:.1f}%"

        if ball_x is not None and ball_y is not None:
            ball_control = compute_pitch_control_at_point(physics_data, ball_x, ball_y)
            state.pc_control_at_ball = f"{ball_control:.2f}"
        else:
            state.pc_control_at_ball = "N/A"

        state.pc_status = ""

    else:
        # Voronoi mode
        state.pc_pitch_image = _render_voronoi(frame_data, ball_x, ball_y, show_vel, title)

        state.pc_home_control = "--"
        state.pc_away_control = "--"
        state.pc_control_at_ball = "--"
        state.pc_status = (
            "Voronoi model shows spatial dominance only \u2014 control percentages require physics-based mode."
        )

    # Common metrics (both modes)
    state.pc_player_count = str(len(frame_data))

    if "speed" in frame_data.columns:
        valid_speed = frame_data["speed"].dropna()
        if not valid_speed.empty:
            state.pc_avg_speed = f"{valid_speed.mean():.1f}"
            state.pc_max_speed = f"{valid_speed.max():.1f}"
        else:
            state.pc_avg_speed = "--"
            state.pc_max_speed = "--"
    else:
        state.pc_avg_speed = "--"
        state.pc_max_speed = "--"

    if "distance_to_ball" in frame_data.columns:
        valid_dist = frame_data["distance_to_ball"].dropna()
        state.pc_avg_dist_to_ball = f"{valid_dist.mean():.1f}" if not valid_dist.empty else "--"
    else:
        state.pc_avg_dist_to_ball = "--"

    state.pc_warning_text = ""

    logger.info(
        "Pitch control rendered: model=%s, frame=%d, players=%d",
        model,
        frame,
        len(frame_data),
    )


# ── Main refresh callback ───────────────────────────────────────────────────


def pc_refresh(state: Any) -> None:
    """Full refresh: load frame range for selected match+half, then render.

    Called when tracking match or provider changes via shared callbacks.
    """
    global _min_frame, _max_frame, _fps

    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_state(state)
        state.pc_min_seconds = 0
        state.pc_max_seconds = 1
        state.pc_elapsed_seconds = 0
        # Check if no tracking matches exist at all
        tracking_lov = getattr(state, "tracking_match_lov", [])
        if not tracking_lov:
            state.pc_warning_text = (
                "Pitch control requires player tracking data (~20 matches from Metrica, IDSSE, SkillCorner)."
            )
        else:
            state.pc_warning_text = ""
        return

    period = 1 if state.pc_half == "1st Half" else 2

    # Load frame range and fps
    _min_frame, _max_frame = fetch_pc_frame_range(match_id, period)
    if _min_frame == _max_frame == 0:
        _clear_state(state)
        state.pc_status = "No frames found for this match and period. Try a different half or match."
        state.pc_warning_text = ""
        state.pc_min_seconds = 0
        state.pc_max_seconds = 1
        state.pc_elapsed_seconds = 0
        return

    _fps = fetch_pc_frame_rate(match_id)
    duration_secs = (_max_frame - _min_frame) // _fps if _fps > 0 else 0

    state.pc_min_seconds = 0
    state.pc_max_seconds = max(duration_secs, 1)
    state.pc_elapsed_seconds = 0
    state.pc_time_label = f"Time ({_fps} fps)"
    mm_max, ss_max = divmod(state.pc_max_seconds, 60)
    state.pc_max_time_display = f"{mm_max}:{ss_max:02d}"

    # Render first frame
    _refresh_frame(state)

    state.pc_data_freshness = fetch_data_freshness()


# ── Page-level control callbacks ─────────────────────────────────────────────


def pc_on_half_change(state: Any, var_name: str, var_value: Any) -> None:
    """Half radio changed — reload frame range for the new period."""
    pc_refresh(state)


def pc_on_seconds_change(state: Any, var_name: str, var_value: Any) -> None:
    """Time slider moved — re-render with new frame.

    Explicitly set pc_elapsed_seconds so Taipy marks it as callback-changed
    and includes it in the state push. Without this, the slider snaps back
    to the old position during the expensive render, then recovers.
    """
    state.pc_elapsed_seconds = int(var_value)
    _refresh_frame(state)


def pc_on_model_change(state: Any, var_name: str, var_value: Any) -> None:
    """Model radio changed — re-render with same frame data."""
    _refresh_frame(state)


def pc_on_velocity_change(state: Any, var_name: str, var_value: Any) -> None:
    """Velocity toggle changed — re-render with same frame data."""
    _refresh_frame(state)


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Pitch-Control", pc_refresh)
