"""Team Shape state module — all variables prefixed with ts_.

TWO sub-views: Snapshot (single frame or phase-averaged formation diagram)
and Timeline (match-long shape metrics over time).
ts_refresh dispatches to the correct sub-view based on selected_sub_view.
Registered as the Team-Shape page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from filters import fetch_data_freshness
from queries.team_shape import (
    fetch_formation_labels,
    fetch_full_match_averages,
    fetch_match_events,
    fetch_phase_averages,
    fetch_sampled_positions,
    fetch_ts_frame_data,
    fetch_ts_frame_range,
)

from analytics.team_shape import TeamShapeResult, compute_team_shape
from state.shared import get_tracking_match_id, register_page_refresher

logger = logging.getLogger(__name__)

# ── Sub-view options ─────────────────────────────────────────────────────────
TS_SUB_VIEWS: list[str] = ["Snapshot", "Timeline"]

# ── Chart colors ─────────────────────────────────────────────────────────────
_HOME_COLOR = "#3b82f6"
_AWAY_COLOR = "#ef4444"
_HULL_HOME_COLOR = "rgba(59,130,246,0.15)"
_HULL_AWAY_COLOR = "rgba(239,68,68,0.15)"
_LINE_HOME_COLOR = "rgba(59,130,246,0.5)"
_LINE_AWAY_COLOR = "rgba(239,68,68,0.5)"
_PITCH_BG = "#1a3a2a"
_PITCH_LINE = "rgba(255,255,255,0.25)"

# ── Exported state variables (all ts_ prefixed) ─────────────────────────────

# Controls
ts_selected_team: str | None = None
ts_selected_half: str | None = None
ts_phase_average: bool = False
ts_elapsed_seconds: int = 0
ts_show_hull: bool = True
ts_show_formation_lines: bool = True

# LOVs
ts_team_lov: list[str] = []
ts_half_lov: list[str] = ["1st Half", "2nd Half", "Full Match"]

# Snapshot metrics
ts_team_length: str = "--"
ts_team_width: str = "--"
ts_def_line_height: str = "--"
ts_hull_area: str = "--"
ts_stretch_index: str = "--"
ts_inter_line_gaps: str = "--"
ts_length_delta: str = ""
ts_width_delta: str = ""

# Timeline metrics
ts_avg_length: str = "--"
ts_avg_width: str = "--"
ts_formation_changes: str = "--"
ts_compactness_delta: str = "--"
ts_compactness_delta_fmt: str = ""

# Charts (Plotly)
ts_snapshot_figure: go.Figure | None = None
ts_timeline_figure: go.Figure | None = None
ts_formation_figure: go.Figure | None = None

# Tables
ts_phase_comparison: pd.DataFrame = pd.DataFrame()

# Status
ts_warning_text: str = ""
ts_snapshot_caption: str = ""
ts_data_freshness: str = ""
ts_max_seconds: int = 0
ts_time_label: str = "Time"
ts_max_time_display: str = "0:00"

__all__ = [
    "ts_avg_length",
    "ts_avg_width",
    "ts_compactness_delta",
    "ts_compactness_delta_fmt",
    "ts_data_freshness",
    "ts_def_line_height",
    "ts_elapsed_seconds",
    "ts_formation_changes",
    "ts_formation_figure",
    "ts_half_lov",
    "ts_hull_area",
    "ts_inter_line_gaps",
    "ts_length_delta",
    "ts_max_seconds",
    "ts_max_time_display",
    "ts_on_formation_lines_toggle",
    "ts_on_half_change",
    "ts_on_hull_toggle",
    "ts_on_phase_toggle",
    "ts_on_seconds_change",
    "ts_on_team_change",
    "ts_phase_average",
    "ts_phase_comparison",
    "ts_refresh",
    "ts_selected_half",
    "ts_selected_team",
    "ts_show_formation_lines",
    "ts_show_hull",
    "ts_snapshot_caption",
    "ts_snapshot_figure",
    "ts_stretch_index",
    "ts_team_length",
    "ts_team_lov",
    "ts_team_width",
    "ts_time_label",
    "ts_timeline_figure",
    "ts_warning_text",
    "ts_width_delta",
]


# ── Pitch rendering (Plotly) ────────────────────────────────────────────────


def _add_pitch_shapes(fig: go.Figure) -> None:
    """Add StatsBomb pitch lines (120x80) as Plotly shapes on dark green bg.

    Reuses the same coordinate system and elements as pass_network._add_pitch_shapes.
    """
    line = dict(color=_PITCH_LINE, width=1.5)
    # Outer boundary
    fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=80, line=line)
    # Halfway line
    fig.add_shape(type="line", x0=60, y0=0, x1=60, y1=80, line=line)
    # Center circle
    fig.add_shape(type="circle", x0=60 - 9.15, y0=40 - 9.15, x1=60 + 9.15, y1=40 + 9.15, line=line)
    # Center spot
    fig.add_shape(type="circle", x0=59.5, y0=39.5, x1=60.5, y1=40.5, line=line, fillcolor=_PITCH_LINE)
    # Left penalty area
    fig.add_shape(type="rect", x0=0, y0=18, x1=18, y1=62, line=line)
    # Right penalty area
    fig.add_shape(type="rect", x0=102, y0=18, x1=120, y1=62, line=line)
    # Left goal area
    fig.add_shape(type="rect", x0=0, y0=30, x1=6, y1=50, line=line)
    # Right goal area
    fig.add_shape(type="rect", x0=114, y0=30, x1=120, y1=50, line=line)
    # Left penalty spot
    fig.add_shape(type="circle", x0=11.5, y0=39.5, x1=12.5, y1=40.5, line=line, fillcolor=_PITCH_LINE)
    # Right penalty spot
    fig.add_shape(type="circle", x0=107.5, y0=39.5, x1=108.5, y1=40.5, line=line, fillcolor=_PITCH_LINE)


def _make_pitch_layout() -> dict[str, Any]:
    """Return shared Plotly layout kwargs for pitch figures.

    The pitch is 120x80 units (1.5:1 aspect ratio). We set explicit width
    and height to guarantee the full pitch + legend is always visible
    without cropping, matching the 12x8 figsize used by mplsoccer on
    other pages (Pitch Control, Movement).
    """
    return dict(
        plot_bgcolor=_PITCH_BG,
        paper_bgcolor="rgba(0,0,0,0)",
        height=520,
        xaxis=dict(range=[-2, 122], showgrid=False, zeroline=False, visible=False, fixedrange=True),
        yaxis=dict(range=[82, -2], showgrid=False, zeroline=False, visible=False, scaleanchor="x", fixedrange=True),
        dragmode=False,
        margin=dict(l=5, r=5, t=35, b=45),
        font=dict(color="white"),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.02,
            xanchor="center",
            x=0.5,
            font=dict(color="white"),
        ),
    )


# ── Snapshot rendering ──────────────────────────────────────────────────────


def _build_snapshot_figure(
    team_players: pd.DataFrame,
    team_side: str,
    shape: TeamShapeResult,
    show_hull: bool,
    show_lines: bool,
    title: str,
) -> go.Figure:
    """Build the Plotly snapshot figure: pitch + players + hull + formation lines."""
    fig = go.Figure()
    _add_pitch_shapes(fig)

    px_arr = team_players["x"].to_numpy(dtype=np.float64)
    py_arr = team_players["y"].to_numpy(dtype=np.float64)
    color = _HOME_COLOR if team_side == "home" else _AWAY_COLOR
    hull_color = _HULL_HOME_COLOR if team_side == "home" else _HULL_AWAY_COLOR
    line_color = _LINE_HOME_COLOR if team_side == "home" else _LINE_AWAY_COLOR

    # Player IDs for hover
    player_ids = team_players["player_id"].astype(str).tolist()

    # Player scatter
    fig.add_trace(
        go.Scatter(
            x=px_arr.tolist(),
            y=py_arr.tolist(),
            mode="markers",
            marker=dict(size=14, color=color, line=dict(width=1, color="white")),
            text=player_ids,
            hovertemplate="Player: %{text}<br>x: %{x:.1f}<br>y: %{y:.1f}<extra></extra>",
            name=team_side.title(),
            showlegend=True,
        )
    )

    # Centroid marker
    if not math.isnan(shape.centroid_x):
        fig.add_trace(
            go.Scatter(
                x=[shape.centroid_x],
                y=[shape.centroid_y],
                mode="markers",
                marker=dict(size=12, color=color, symbol="x", line=dict(width=2, color="white")),
                name="Centroid",
                hovertemplate="Centroid<br>x: %{x:.1f}<br>y: %{y:.1f}<extra></extra>",
                showlegend=True,
            )
        )

    # Convex hull (use pre-computed vertices from analytics to avoid recomputing ConvexHull)
    if show_hull:
        hull_verts_available = shape.hull_vertices_x is not None and shape.hull_vertices_y is not None
        if hull_verts_available:
            hx = list(shape.hull_vertices_x)  # type: ignore[arg-type]
            hy = list(shape.hull_vertices_y)  # type: ignore[arg-type]
            fig.add_trace(
                go.Scatter(
                    x=hx,
                    y=hy,
                    mode="lines",
                    fill="toself",
                    fillcolor=hull_color,
                    line=dict(color=color, width=1.5, dash="dot"),
                    name="Convex Hull",
                    hoverinfo="skip",
                    showlegend=True,
                )
            )

    # Formation lines (use pre-computed Ward cluster labels from analytics)
    if show_lines and len(px_arr) >= 3 and shape.cluster_labels is not None:
        # Group player indices by cluster label
        cluster_groups: dict[int, list[int]] = {}
        for idx, lab in enumerate(shape.cluster_labels):
            cluster_groups.setdefault(lab, []).append(idx)
        # Sort groups by mean x (back to front) to match original ordering
        sorted_clusters = sorted(cluster_groups.values(), key=lambda idxs: float(np.mean(px_arr[idxs])))
        for ci, group in enumerate(sorted_clusters):
            if len(group) < 2:
                continue
            # Sort within cluster by y (left to right)
            sorted_idxs = sorted(group, key=lambda i: py_arr[i])
            xs = [float(px_arr[i]) for i in sorted_idxs]
            ys = [float(py_arr[i]) for i in sorted_idxs]
            fig.add_trace(
                go.Scatter(
                    x=xs,
                    y=ys,
                    mode="lines",
                    line=dict(color=line_color, width=2, dash="dash"),
                    name=f"Line {ci + 1}" if ci == 0 else "",
                    showlegend=ci == 0,
                    legendgroup="formation_lines",
                    hoverinfo="skip",
                )
            )

    fig.update_layout(
        title=dict(text=title, font=dict(color="white", size=14)),
        **_make_pitch_layout(),
    )

    return fig


def _format_metric(value: float, unit: str, decimals: int = 1) -> str:
    """Format a numeric metric with unit. Returns '--' for NaN values."""
    if math.isnan(value):
        return "--"
    if unit == "m\u00b2":
        return f"{value:,.0f} m\u00b2"
    if unit == "%":
        return f"{value:.{decimals}f}%"
    return f"{value:.{decimals}f}{unit}"


def _format_delta(current: float, baseline: float) -> str:
    """Format a delta value as '+X.Xm' or '-X.Xm'. Returns '' for NaN."""
    if math.isnan(current) or math.isnan(baseline):
        return ""
    diff = current - baseline
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.1f}m"


def _render_snapshot(state: Any) -> None:
    """Render the snapshot sub-view: single-frame or phase-averaged formation.

    Data fetching strategy (per-frame or server-side aggregation):
    - Single frame mode: fetch_ts_frame_data() returns ~22 rows for one frame
    - Phase average mode: fetch_phase_averages() returns ~22 rows (server-side AVG)
    - Match-average baseline: fetch_full_match_averages() returns ~22 rows
    No bulk tracking fetch — every query is bounded.
    """
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_snapshot(state)
        return

    # Determine period from half selection
    half = state.ts_selected_half or "1st Half"
    period: int = 1
    if half == "2nd Half":
        period = 2
    # "Full Match" uses period=1 for slider range; phase average uses both halves

    # Determine team side from selection
    team_side = _resolve_team_side(state)
    if team_side is None:
        _clear_snapshot(state)
        state.ts_warning_text = "Select a team to view team shape."
        return

    # Frame range for slider (small aggregate query — 1 row)
    min_f, max_f, fps = fetch_ts_frame_range(match_id, period)
    if min_f == max_f == 0:
        _clear_snapshot(state)
        state.ts_warning_text = "No tracking data for the selected match and half."
        return

    duration_secs = (max_f - min_f) // fps if fps > 0 else 0
    new_max = max(duration_secs, 1)
    if state.ts_max_seconds != new_max:
        state.ts_max_seconds = new_max
    mm_max, ss_max = divmod(new_max, 60)
    state.ts_time_label = f"Time ({fps} fps)"
    state.ts_max_time_display = f"{mm_max}:{ss_max:02d}"

    try:
        if state.ts_phase_average:
            # Phase average: server-side AVG per player (~22 rows)
            if half == "Full Match":
                # Average across both halves via _fetch_full_match_averages
                avg_positions = fetch_full_match_averages(match_id)
            else:
                avg_positions = fetch_phase_averages(match_id, period)

            # Filter to selected team
            avg_positions = avg_positions[avg_positions["team"] == team_side].copy()
            frame_label = "Phase Average"
        else:
            # Single frame: convert elapsed_seconds to frame number (~22 rows)
            elapsed = int(state.ts_elapsed_seconds)
            target_frame = min_f + elapsed * fps
            frame_data = fetch_ts_frame_data(match_id, target_frame)

            # Filter to selected team
            avg_positions = frame_data[frame_data["team"] == team_side][["player_id", "x", "y", "team"]].copy()
            mm, ss = divmod(elapsed, 60)
            frame_label = f"{mm:02d}:{ss:02d}"
    except Exception:
        logger.exception("Failed to fetch tracking data for snapshot")
        _clear_snapshot(state)
        state.ts_warning_text = "Failed to load tracking data."
        return

    if avg_positions.empty or len(avg_positions) < 3:
        _clear_snapshot(state)
        state.ts_warning_text = "Insufficient player positions for shape analysis (need >= 3 players)."
        return

    # Compute team shape
    px = avg_positions["x"].to_numpy(dtype=np.float64)
    py = avg_positions["y"].to_numpy(dtype=np.float64)
    shape = compute_team_shape(px, py)

    # Compute match-average shape for deltas (server-side aggregation — ~22 rows)
    try:
        all_avg = fetch_full_match_averages(match_id)
        team_avg = all_avg[all_avg["team"] == team_side]
        if len(team_avg) >= 3:
            match_avg_shape = compute_team_shape(
                team_avg["x"].to_numpy(dtype=np.float64),
                team_avg["y"].to_numpy(dtype=np.float64),
            )
        else:
            match_avg_shape = shape  # fallback: no delta
    except Exception:
        logger.warning("Could not fetch match averages for delta computation")
        match_avg_shape = shape

    # Build match label for title
    events = fetch_match_events(match_id)
    if not events.empty:
        r = events.iloc[0]
        match_label = f"{r['home_team_name']} v {r['away_team_name']}"
    else:
        match_label = match_id

    title = f"Team Shape \u2014 {match_label} \u2014 {team_side.title()} ({frame_label})"

    # Render figure
    state.ts_snapshot_figure = _build_snapshot_figure(
        avg_positions, team_side, shape, bool(state.ts_show_hull), bool(state.ts_show_formation_lines), title
    )

    # Update metrics
    state.ts_team_length = _format_metric(shape.team_length, "m")
    state.ts_team_width = _format_metric(shape.team_width, "m")
    state.ts_hull_area = _format_metric(shape.convex_hull_area, "m\u00b2")
    state.ts_stretch_index = _format_metric(shape.stretch_index, "m")

    # Defensive line height as pitch percentage (0% = own goal, 100% = opponent goal)
    if not math.isnan(shape.defensive_line_height):
        def_pct = (shape.defensive_line_height / 120.0) * 100.0
        state.ts_def_line_height = _format_metric(def_pct, "%")
    else:
        state.ts_def_line_height = "--"

    # Inter-line gaps
    if shape.inter_line_gaps:
        avg_gap = sum(shape.inter_line_gaps) / len(shape.inter_line_gaps)
        state.ts_inter_line_gaps = _format_metric(avg_gap, "m")
    else:
        state.ts_inter_line_gaps = "--"

    # Deltas vs match average
    state.ts_length_delta = _format_delta(shape.team_length, match_avg_shape.team_length)
    state.ts_width_delta = _format_delta(shape.team_width, match_avg_shape.team_width)

    # Caption
    n_players = len(avg_positions)
    state.ts_snapshot_caption = (
        f"{n_players} players \u00b7 {'Phase average' if state.ts_phase_average else f'Frame at {frame_label}'}"
    )
    state.ts_warning_text = ""

    logger.info(
        "Snapshot rendered: team=%s, players=%d, hull_area=%.0f",
        team_side,
        n_players,
        shape.convex_hull_area,
    )


def _clear_snapshot(state: Any) -> None:
    """Reset snapshot state to defaults."""
    state.ts_snapshot_figure = None
    state.ts_team_length = "--"
    state.ts_team_width = "--"
    state.ts_def_line_height = "--"
    state.ts_hull_area = "--"
    state.ts_stretch_index = "--"
    state.ts_inter_line_gaps = "--"
    state.ts_length_delta = ""
    state.ts_width_delta = ""
    state.ts_snapshot_caption = ""


# ── Timeline rendering ──────────────────────────────────────────────────────


def _render_timeline(state: Any) -> None:
    """Render the timeline sub-view: shape metrics over match duration.

    Data fetching strategy: fetch_sampled_positions() uses server-side
    GROUP BY on time buckets (5-second intervals). For a 45-min half at
    5s intervals: 540 buckets x ~11 players (one team) = ~6K rows per half.
    Two halves = ~12K rows total — vs the old 500K bulk fetch.
    """
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_timeline(state)
        return

    team_side = _resolve_team_side(state)
    if team_side is None:
        _clear_timeline(state)
        state.ts_warning_text = "Select a team to view timeline."
        return

    sample_interval_s = 5

    try:
        # Fetch sampled positions for both halves (server-side aggregation)
        h1_data = fetch_sampled_positions(match_id, period=1, sample_interval_s=sample_interval_s)
        h2_data = fetch_sampled_positions(match_id, period=2, sample_interval_s=sample_interval_s)
    except Exception:
        logger.exception("Failed to fetch sampled tracking data for timeline")
        _clear_timeline(state)
        state.ts_warning_text = "Failed to load tracking data."
        return

    if h1_data.empty and h2_data.empty:
        _clear_timeline(state)
        state.ts_warning_text = "No tracking data for the selected match."
        return

    # Combine halves and filter to selected team
    all_sampled = pd.concat([h1_data, h2_data], ignore_index=True)
    team_data = all_sampled[all_sampled["team"] == team_side]
    if team_data.empty:
        _clear_timeline(state)
        state.ts_warning_text = f"No tracking data for {team_side} team."
        return

    # Group by time_bucket to compute shape per time bucket
    bucket_groups: dict[tuple[int, float], pd.DataFrame] = {}
    for (period_val, bucket_val), grp in team_data.groupby(["period", "time_bucket"]):
        bucket_groups[(int(period_val), float(bucket_val))] = grp

    # Compute shape metrics per bucket
    timestamps: list[float] = []
    lengths: list[float] = []
    widths: list[float] = []
    def_heights: list[float] = []
    hull_areas: list[float] = []
    stretches: list[float] = []

    for (period_val, bucket_val), bucket_df in sorted(bucket_groups.items()):
        if len(bucket_df) < 3:
            continue

        px = bucket_df["x"].to_numpy(dtype=np.float64)
        py = bucket_df["y"].to_numpy(dtype=np.float64)
        shape_result = compute_team_shape(px, py)

        # Offset period 2 timestamps by 45 minutes for continuous x-axis
        offset = 45.0 * 60.0 if period_val == 2 else 0.0
        ts_sec = float(bucket_val) + offset

        timestamps.append(ts_sec)
        lengths.append(shape_result.team_length)
        widths.append(shape_result.team_width)
        def_heights.append((shape_result.defensive_line_height / 120.0) * 100.0)
        hull_areas.append(shape_result.convex_hull_area)
        stretches.append(shape_result.stretch_index)

    if not timestamps:
        _clear_timeline(state)
        state.ts_warning_text = "Insufficient data to compute timeline."
        return

    # Convert timestamps to minutes for display
    minutes = [t_s / 60.0 for t_s in timestamps]

    # Build timeline figure
    state.ts_timeline_figure = _build_timeline_figure(minutes, lengths, widths, def_heights, match_id)

    # Build formation figure
    formation_labels = fetch_formation_labels(match_id, team_side)
    state.ts_formation_figure = _build_formation_figure(formation_labels)

    # Build phase comparison table (1st Half vs 2nd Half)
    state.ts_phase_comparison = _build_phase_comparison(timestamps, lengths, widths, hull_areas, def_heights, stretches)

    # Update timeline metrics
    valid_lengths = [v for v in lengths if not math.isnan(v)]
    valid_widths = [v for v in widths if not math.isnan(v)]

    state.ts_avg_length = _format_metric(sum(valid_lengths) / len(valid_lengths) if valid_lengths else math.nan, "m")
    state.ts_avg_width = _format_metric(sum(valid_widths) / len(valid_widths) if valid_widths else math.nan, "m")

    # Formation changes count (vectorized — compare each label to its predecessor)
    if not formation_labels.empty:
        transitions = int(
            (formation_labels["formation_label"] != formation_labels["formation_label"].shift()).sum() - 1
        )
        state.ts_formation_changes = str(max(transitions, 0))
    else:
        state.ts_formation_changes = "--"

    # Compactness delta (1st half hull area vs 2nd half hull area)
    h1_hulls = [hull_areas[i] for i, ts_v in enumerate(timestamps) if ts_v < 45 * 60 and not math.isnan(hull_areas[i])]
    h2_hulls = [hull_areas[i] for i, ts_v in enumerate(timestamps) if ts_v >= 45 * 60 and not math.isnan(hull_areas[i])]
    if h1_hulls and h2_hulls:
        h1_avg = sum(h1_hulls) / len(h1_hulls)
        h2_avg = sum(h2_hulls) / len(h2_hulls)
        delta = h2_avg - h1_avg
        sign = "+" if delta >= 0 else ""
        state.ts_compactness_delta = f"{sign}{delta:,.0f} m\u00b2"
        state.ts_compactness_delta_fmt = f"1H: {h1_avg:,.0f} m\u00b2 / 2H: {h2_avg:,.0f} m\u00b2"
    else:
        state.ts_compactness_delta = "--"
        state.ts_compactness_delta_fmt = ""

    state.ts_warning_text = ""

    logger.info(
        "Timeline rendered: team=%s, buckets_computed=%d",
        team_side,
        len(timestamps),
    )


def _build_timeline_figure(
    minutes: list[float],
    lengths: list[float],
    widths: list[float],
    def_heights: list[float],
    match_id: str,
) -> go.Figure:
    """Build Plotly line chart of shape metrics over time."""
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=minutes,
            y=lengths,
            mode="lines",
            name="Team Length (m)",
            line=dict(color="#3b82f6", width=2),
            hovertemplate="Min %{x:.0f}: %{y:.1f}m<extra>Length</extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=minutes,
            y=widths,
            mode="lines",
            name="Team Width (m)",
            line=dict(color="#10b981", width=2),
            hovertemplate="Min %{x:.0f}: %{y:.1f}m<extra>Width</extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=minutes,
            y=def_heights,
            mode="lines",
            name="Def. Line Height (%)",
            line=dict(color="#f59e0b", width=2, dash="dot"),
            hovertemplate="Min %{x:.0f}: %{y:.1f}%<extra>Def Line</extra>",
            yaxis="y2",
        )
    )

    # Half-time marker
    fig.add_vline(x=45, line_dash="dash", line_color="rgba(255,255,255,0.4)", annotation_text="HT")

    fig.update_layout(
        title=dict(text="Shape Metrics Over Time", font=dict(color="white", size=14)),
        plot_bgcolor="#1a1a2e",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(
            title="Match Minutes",
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis=dict(
            title="Distance (m)",
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis2=dict(
            title="Def. Line Height (%)",
            title_font=dict(color="#f59e0b"),
            tickfont=dict(color="#f59e0b"),
            overlaying="y",
            side="right",
            range=[0, 100],
            gridcolor="rgba(245,158,11,0.1)",
        ),
        font=dict(color="white"),
        height=400,
        margin=dict(l=60, r=60, t=50, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5,
            font=dict(color="white"),
        ),
    )

    return fig


def _build_formation_figure(labels: pd.DataFrame) -> go.Figure | None:
    """Build a horizontal strip chart showing formation labels over 5-min windows."""
    if labels.empty:
        return None

    # Color palette for formations
    formation_colors: dict[str, str] = {}
    palette = [
        "#3b82f6",
        "#ef4444",
        "#10b981",
        "#f59e0b",
        "#8b5cf6",
        "#ec4899",
        "#06b6d4",
        "#84cc16",
        "#f97316",
        "#6366f1",
    ]
    unique_formations = labels["formation_label"].unique()
    for i, f in enumerate(unique_formations):
        formation_colors[str(f)] = palette[i % len(palette)]

    fig = go.Figure()

    for _, row in labels.iterrows():
        start = float(row["window_start_s"])
        end = float(row["window_end_s"])
        period_val = int(row["period"])
        offset = 45.0 * 60.0 if period_val == 2 else 0.0
        x0 = (start + offset) / 60.0
        x1 = (end + offset) / 60.0
        formation = str(row["formation_label"])
        color = formation_colors.get(formation, "#888888")

        fig.add_shape(
            type="rect",
            x0=x0,
            y0=0,
            x1=x1,
            y1=1,
            fillcolor=color,
            opacity=0.7,
            line=dict(width=0.5, color="white"),
        )
        # Label in center of block
        mid_x = (x0 + x1) / 2.0
        fig.add_annotation(
            x=mid_x,
            y=0.5,
            text=formation,
            showarrow=False,
            font=dict(color="white", size=9),
        )

    # Half-time marker
    fig.add_vline(x=45, line_dash="dash", line_color="rgba(255,255,255,0.6)")

    # Legend entries via invisible traces
    for formation, color in formation_colors.items():
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(size=10, color=color),
                name=formation,
                showlegend=True,
            )
        )

    fig.update_layout(
        title=dict(text="Formation Labels (5-min windows)", font=dict(color="white", size=14)),
        plot_bgcolor="#1a1a2e",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(
            title="Match Minutes",
            title_font=dict(color="white"),
            tickfont=dict(color="white"),
            range=[0, 95],
        ),
        yaxis=dict(visible=False, range=[0, 1]),
        height=150,
        margin=dict(l=60, r=20, t=50, b=40),
        font=dict(color="white"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.8,
            xanchor="center",
            x=0.5,
            font=dict(color="white"),
        ),
    )

    return fig


def _build_phase_comparison(
    timestamps: list[float],
    lengths: list[float],
    widths: list[float],
    hull_areas: list[float],
    def_heights: list[float],
    stretches: list[float],
) -> pd.DataFrame:
    """Build a comparison table: 1st Half vs 2nd Half shape metrics."""
    half_split = 45.0 * 60.0  # 45 minutes in seconds

    def _half_avg(values: list[float], ts: list[float], is_first: bool) -> float:
        filtered = [
            values[i]
            for i, t_v in enumerate(ts)
            if (t_v < half_split if is_first else t_v >= half_split) and not math.isnan(values[i])
        ]
        return sum(filtered) / len(filtered) if filtered else math.nan

    metrics = ["Length (m)", "Width (m)", "Hull Area (m\u00b2)", "Def. Line (x)", "Stretch Index (m)"]
    data_lists = [lengths, widths, hull_areas, def_heights, stretches]

    rows: list[dict[str, str]] = []
    for name, data in zip(metrics, data_lists, strict=True):
        h1 = _half_avg(data, timestamps, is_first=True)
        h2 = _half_avg(data, timestamps, is_first=False)
        delta = h2 - h1 if not (math.isnan(h1) or math.isnan(h2)) else math.nan

        unit = "m\u00b2" if "Area" in name else "m" if "m)" in name else ""
        rows.append(
            {
                "Metric": name,
                "1st Half": _format_metric(h1, unit) if not math.isnan(h1) else "--",
                "2nd Half": _format_metric(h2, unit) if not math.isnan(h2) else "--",
                "\u0394": _format_delta_simple(delta, unit) if not math.isnan(delta) else "--",
            }
        )

    return pd.DataFrame(rows)


def _format_delta_simple(delta: float, unit: str) -> str:
    """Format a delta value with sign and unit."""
    if math.isnan(delta):
        return "--"
    sign = "+" if delta >= 0 else ""
    if unit == "m\u00b2":
        return f"{sign}{delta:,.0f} m\u00b2"
    return f"{sign}{delta:.1f}{unit}"


def _clear_timeline(state: Any) -> None:
    """Reset timeline state to defaults."""
    state.ts_timeline_figure = None
    state.ts_formation_figure = None
    state.ts_phase_comparison = pd.DataFrame()
    state.ts_avg_length = "--"
    state.ts_avg_width = "--"
    state.ts_formation_changes = "--"
    state.ts_compactness_delta = "--"
    state.ts_compactness_delta_fmt = ""


# ── Helpers ─────────────────────────────────────────────────────────────────


def _resolve_team_side(state: Any) -> str | None:
    """Map ts_selected_team label to 'home' or 'away'. Returns None if unset.

    Handles both plain labels ("Home") and enriched labels ("Home — Arsenal").
    """
    sel = state.ts_selected_team
    if sel and sel.startswith("Home"):
        return "home"
    if sel and sel.startswith("Away"):
        return "away"
    return None


def _init_team_lov(state: Any) -> None:
    """Set the team LOV with actual team names and auto-select Home.

    Uses match summary to get real team names (e.g., "Home — Arsenal").
    Falls back to plain "Home"/"Away" when match summary unavailable.
    """
    match_id = get_tracking_match_id(state.selected_tracking_match)
    home_label, away_label = "Home", "Away"
    if match_id:
        events = fetch_match_events(match_id)
        if not events.empty:
            r = events.iloc[0]
            home_name = r.get("home_team_name", "")
            away_name = r.get("away_team_name", "")
            if home_name:
                home_label = f"Home — {home_name}"
            if away_name:
                away_label = f"Away — {away_name}"
    state.ts_team_lov = [home_label, away_label]
    if not state.ts_selected_team or state.ts_selected_team not in state.ts_team_lov:
        state.ts_selected_team = home_label


# ── Callbacks ───────────────────────────────────────────────────────────────


def ts_on_team_change(state: Any, var_name: str, var_value: Any) -> None:
    """Team selector changed — re-render for the new team."""
    _dispatch_refresh(state)


def ts_on_half_change(state: Any, var_name: str, var_value: Any) -> None:
    """Half selector changed — reload data for the new period."""
    state.ts_elapsed_seconds = 0
    _dispatch_refresh(state)


def ts_on_seconds_change(state: Any, var_name: str, var_value: Any) -> None:
    """Time slider moved — re-render snapshot at the new time.

    Explicitly set ts_elapsed_seconds so Taipy marks it as callback-changed
    and includes it in the state push.
    """
    state.ts_elapsed_seconds = int(var_value)
    if state.selected_sub_view == "Snapshot" and not state.ts_phase_average:
        _render_snapshot(state)


def ts_on_phase_toggle(state: Any, var_name: str, var_value: Any) -> None:
    """Phase average toggle changed — re-render snapshot."""
    if state.selected_sub_view == "Snapshot":
        _render_snapshot(state)


def ts_on_hull_toggle(state: Any, var_name: str, var_value: Any) -> None:
    """Hull visibility toggle changed — re-render snapshot."""
    if state.selected_sub_view == "Snapshot":
        _render_snapshot(state)


def ts_on_formation_lines_toggle(state: Any, var_name: str, var_value: Any) -> None:
    """Formation lines visibility toggle changed — re-render snapshot."""
    if state.selected_sub_view == "Snapshot":
        _render_snapshot(state)


# ── Main refresh dispatcher ─────────────────────────────────────────────────


def _dispatch_refresh(state: Any) -> None:
    """Dispatch to snapshot or timeline based on selected_sub_view."""
    view = state.selected_sub_view
    if view == "Timeline":
        _render_timeline(state)
    else:
        _render_snapshot(state)


def ts_refresh(state: Any) -> None:
    """Full refresh: configure sub-views, LOVs, then render.

    Called when tracking match or provider changes via shared callbacks.
    """
    # Claim this page's sub-view LOV
    if not getattr(state, "sub_view_lov", None) or state.sub_view_lov != TS_SUB_VIEWS:
        state.sub_view_lov = TS_SUB_VIEWS
    if not state.selected_sub_view or state.selected_sub_view not in TS_SUB_VIEWS:
        state.selected_sub_view = TS_SUB_VIEWS[0]

    # Initialize team LOV
    _init_team_lov(state)

    # Initialize half selection
    if not state.ts_selected_half or state.ts_selected_half not in ts_half_lov:
        state.ts_selected_half = "1st Half"

    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_snapshot(state)
        _clear_timeline(state)
        # Check if any tracking matches exist
        tracking_lov = getattr(state, "tracking_match_lov", [])
        if not tracking_lov:
            state.ts_warning_text = (
                "Team shape requires player tracking data (~20 matches from Metrica, IDSSE, SkillCorner)."
            )
        else:
            state.ts_warning_text = ""
        return

    # Pre-compute slider max so Taipy evaluates the slider condition
    # (`ts_max_seconds > 1`) correctly on the FIRST render after match
    # selection — before _render_snapshot runs.
    half = state.ts_selected_half or "1st Half"
    period = 2 if half == "2nd Half" else 1
    min_f, max_f, fps = fetch_ts_frame_range(match_id, period)
    if fps > 0 and max_f > min_f:
        state.ts_max_seconds = max((max_f - min_f) // fps, 1)

    _dispatch_refresh(state)

    state.ts_data_freshness = fetch_data_freshness()


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Team-Shape", ts_refresh)
