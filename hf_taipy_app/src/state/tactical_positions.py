"""Tactical Positions state — position plots, formation comparison, position maps.

Prefix: tac_
Three sub-views controlled by shared.selected_sub_view:
  - "Position Plots": per-frame position label timeline (heatmap)
  - "Formation Comparison": dual-detector formation label comparison (EFPI vs Shape Graph)
  - "Position Maps": 5x5 heatmap of player positional distribution

Tracking-scoped page — uses get_tracking_match_id, not get_match_id.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from filters import fetch_data_freshness
from plotly.subplots import make_subplots
from queries.tactical_positions import (
    fetch_formation_labels_dual,
    fetch_position_maps,
    fetch_position_timeline,
    fetch_tp_players,
    fetch_tracking_teams,
)
from queries.team_shape import fetch_match_events

from state.shared import get_tracking_match_id, register_page_refresher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sub-view list of values
# ---------------------------------------------------------------------------
TAC_SUB_VIEW_LOV: list[str] = ["Position Plots", "Formation Comparison", "Position Maps"]

# ---------------------------------------------------------------------------
# Vertical level color palette
# ---------------------------------------------------------------------------
_LEVEL_COLORS: dict[str, str] = {
    "B": "#3b82f6",  # blue
    "DM": "#06b6d4",  # cyan
    "M": "#22c55e",  # green
    "AM": "#f59e0b",  # amber
    "F": "#ef4444",  # red
}

_LEVEL_ORDER: list[str] = ["B", "DM", "M", "AM", "F"]
_LEVEL_NUMERIC: dict[str, int] = {v: i for i, v in enumerate(_LEVEL_ORDER)}

# Human-readable level labels for chart axes and legends
_LEVEL_LABELS: dict[str, str] = {
    "B": "Back (B)",
    "DM": "Def Mid (DM)",
    "M": "Mid (M)",
    "AM": "Att Mid (AM)",
    "F": "Forward (F)",
}

# Horizontal level order for 5x5 position maps
_HORIZ_ORDER: list[str] = ["L", "LC", "C", "RC", "R"]
_HORIZ_LABELS: dict[str, str] = {
    "L": "Left (L)",
    "LC": "Left-Center (LC)",
    "C": "Center (C)",
    "RC": "Right-Center (RC)",
    "R": "Right (R)",
}

# Background for Plotly charts (matches dark theme)
_CHART_BG = "rgba(0,0,0,0)"

# Default FPS for frame-to-minute conversion
_DEFAULT_FPS = 25

# ---------------------------------------------------------------------------
# Formation color palette — deterministic assignment by label
# ---------------------------------------------------------------------------
_FORMATION_PALETTE: list[str] = [
    "#3b82f6",  # blue
    "#ef4444",  # red
    "#22c55e",  # green
    "#f59e0b",  # amber
    "#8b5cf6",  # violet
    "#06b6d4",  # cyan
    "#f97316",  # orange
    "#ec4899",  # pink
    "#14b8a6",  # teal
    "#a855f7",  # purple
]


def _formation_color(label: str, palette_map: dict[str, str]) -> str:
    """Get a deterministic color for a formation label."""
    if label not in palette_map:
        idx = len(palette_map) % len(_FORMATION_PALETTE)
        palette_map[label] = _FORMATION_PALETTE[idx]
    return palette_map[label]


# ---------------------------------------------------------------------------
# Exported state variables (all tac_ prefixed)
# ---------------------------------------------------------------------------

# Position Plots
tac_position_plot_figure: go.Figure | None = None
tac_most_common_formation: str = "\u2014"
tac_formation_stability: str = "\u2014"

# Formation Comparison
tac_formation_comparison_figure: go.Figure | None = None
tac_agreement_rate: str = "\u2014"
tac_efpi_changes: str = "\u2014"
tac_sg_changes: str = "\u2014"
tac_efpi_dominant: str = "\u2014"
tac_sg_dominant: str = "\u2014"

# Position Maps
tac_position_map_figure: go.Figure | None = None
tac_position_map_compare_figure: go.Figure | None = None
tac_primary_position: str = "\u2014"
tac_position_versatility: str = "\u2014"
tac_vertical_range: str = "\u2014"

# Player selection for Position Maps
tac_player_lov: list[str] = []
tac_selected_player: str | None = None
tac_compare_player_lov: list[str] = []
tac_selected_compare_player: str | None = None

# General
tac_data_freshness: str = ""
tac_scope_label: str = ""
tac_warning_text: str = ""

# Team selector (tracking page)
tac_selected_team: str | None = None
tac_team_lov: list[str] = []

__all__ = [
    "TAC_SUB_VIEW_LOV",
    "tac_agreement_rate",
    "tac_compare_player_lov",
    "tac_data_freshness",
    "tac_efpi_changes",
    "tac_efpi_dominant",
    "tac_formation_comparison_figure",
    "tac_formation_stability",
    "tac_most_common_formation",
    "tac_on_compare_player_change",
    "tac_on_player_change",
    "tac_on_team_change",
    "tac_player_lov",
    "tac_position_map_compare_figure",
    "tac_position_map_figure",
    "tac_position_plot_figure",
    "tac_position_versatility",
    "tac_primary_position",
    "tac_refresh",
    "tac_scope_label",
    "tac_selected_compare_player",
    "tac_selected_player",
    "tac_selected_team",
    "tac_sg_changes",
    "tac_sg_dominant",
    "tac_team_lov",
    "tac_vertical_range",
    "tac_warning_text",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_team_side(state: Any) -> str | None:
    """Map tac_selected_team label to 'home' or 'away'. Returns None if unset.

    Handles both plain labels ("Home") and enriched labels ("Home — Arsenal").
    """
    sel = state.tac_selected_team
    if sel and sel.startswith("Home"):
        return "home"
    if sel and sel.startswith("Away"):
        return "away"
    return None


def _init_team_lov(state: Any) -> None:
    """Set the team LOV with actual team names and auto-select Home.

    Resolution order:
    1. Match events (StatsBomb): home_team_name / away_team_name
    2. Tracking metadata: team_display_name from fct_position_maps
    3. Fallback: plain "Home" / "Away"
    """
    match_id = get_tracking_match_id(state.selected_tracking_match)
    home_label, away_label = "Home", "Away"
    if match_id:
        # Try StatsBomb events first (has team names for SB matches)
        events = fetch_match_events(match_id)
        if not events.empty:
            r = events.iloc[0]
            home_name = r.get("home_team_name", "")
            away_name = r.get("away_team_name", "")
            if home_name:
                home_label = f"Home \u2014 {home_name}"
            if away_name:
                away_label = f"Away \u2014 {away_name}"
        else:
            # Fallback: tracking metadata (IDSSE/SkillCorner team names)
            teams_df = fetch_tracking_teams(match_id)
            if not teams_df.empty:
                for _, row in teams_df.iterrows():
                    side = row.get("team", "")
                    name = row.get("team_display_name", "")
                    if side == "home" and name and name != "Home":
                        home_label = f"Home \u2014 {name}"
                    elif side == "away" and name and name != "Away":
                        away_label = f"Away \u2014 {name}"
    state.tac_team_lov = [home_label, away_label]
    if not state.tac_selected_team or state.tac_selected_team not in state.tac_team_lov:
        state.tac_selected_team = home_label


def _get_player_id_from_label(label: str | None, players_df: pd.DataFrame) -> str | None:
    """Resolve a player display-name label back to player_id."""
    if not label or players_df.empty:
        return None
    match_rows = players_df[players_df["player_display_name"] == label]
    if match_rows.empty:
        return None
    return str(match_rows.iloc[0]["player_id"])


# ---------------------------------------------------------------------------
# Position Plots sub-view
# ---------------------------------------------------------------------------


def _build_position_timeline(df: pd.DataFrame) -> go.Figure:
    """Build a scatter timeline of per-frame player positions.

    x-axis: time in minutes (frame_id / fps).
    y-axis: one row per player (categorical).
    Color: vertical level mapped to the 5-color palette.
    """
    fig = go.Figure()

    if df.empty:
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=_CHART_BG,
            title="Position Timeline (no data)",
        )
        return fig

    # Convert frame_id to minutes
    fps = _DEFAULT_FPS
    df = df.copy()
    df["minute"] = df["frame_id"] / fps / 60.0

    # Sample down for rendering performance (max ~10K points)
    max_points = 10_000
    total_frames = len(df)
    sampled = False
    if len(df) > max_points:
        df = df.sample(n=max_points, random_state=42).sort_values("minute")
        sampled = True

    players = sorted(df["player_display_name"].unique())

    # Marker shapes per level for WCAG 1.4.1 color-independence
    _level_shapes: dict[str, str] = {
        "B": "circle",
        "DM": "diamond",
        "M": "square",
        "AM": "triangle-up",
        "F": "star",
    }

    for level, color in _LEVEL_COLORS.items():
        subset = df[df["vertical_level"] == level]
        if subset.empty:
            continue
        label = _LEVEL_LABELS.get(level, level)
        fig.add_trace(
            go.Scatter(
                x=subset["minute"],
                y=subset["player_display_name"],
                mode="markers",
                marker=dict(size=5, color=color, opacity=0.7, symbol=_level_shapes.get(level, "circle")),
                name=label,
                hovertemplate=("<b>%{y}</b><br>Time: %{x:.1f} min<br>Level: " + label + "<br><extra></extra>"),
            )
        )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_CHART_BG,
        plot_bgcolor="rgba(26,26,46,1)",
        title=dict(
            text=(
                "Player Position Labels Over Time (vertical level by frame)"
                + (f"<br><sub>Showing {max_points:,} of {total_frames:,} data points</sub>" if sampled else "")
            ),
            font=dict(color="white", size=14),
        ),
        xaxis=dict(title="Match Time (min)", color="white", gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(
            title="",
            color="white",
            categoryorder="array",
            categoryarray=list(reversed(players)),
        ),
        margin=dict(l=140, r=20, t=50, b=50),
        height=max(400, len(players) * 35),
        legend=dict(
            title="Vertical Level",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        font=dict(color="white"),
    )

    return fig


def _compute_formation_metrics(df: pd.DataFrame) -> tuple[str, str]:
    """Compute most-common formation label and stability score.

    Stability: percentage of frames where the most common formation holds.
    Returns (most_common_formation, stability_pct_str).
    """
    if df.empty or "position_label" not in df.columns:
        return "\u2014", "\u2014"

    # Group players by frame to get formation string
    frame_formations: list[str] = []
    for _frame, grp in df.groupby("frame_id"):
        levels = sorted(grp["vertical_level"].dropna().tolist())
        # Count players per level to form a formation string
        level_counts: dict[str, int] = {}
        for lv in levels:
            level_counts[lv] = level_counts.get(lv, 0) + 1
        # Build formation string in tactical order (B-DM-M-AM-F)
        parts = [str(level_counts.get(lv, 0)) for lv in _LEVEL_ORDER if level_counts.get(lv, 0) > 0]
        frame_formations.append("-".join(parts))

    if not frame_formations:
        return "\u2014", "\u2014"

    formation_series = pd.Series(frame_formations)
    most_common = formation_series.mode().iloc[0]
    stability = (formation_series == most_common).mean() * 100.0
    return most_common, f"{stability:.0f}%"


def _refresh_position_plots(state: Any) -> None:
    """Refresh the Position Plots sub-view."""
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_position_plots(state)
        state.tac_warning_text = "Select a tracking match to view position plots."
        return

    team_side = _resolve_team_side(state)
    if team_side is None:
        _clear_position_plots(state)
        state.tac_warning_text = "Select a team to view position plots."
        return

    try:
        timeline = fetch_position_timeline(match_id, team_side)
    except Exception:
        logger.exception("Failed to fetch position timeline")
        _clear_position_plots(state)
        state.tac_warning_text = "Something went wrong loading position data. Try refreshing the page."
        return

    if timeline.empty:
        _clear_position_plots(state)
        state.tac_warning_text = "No position data for this match and team. Try selecting a different match."
        return

    fig = _build_position_timeline(timeline)
    state.tac_position_plot_figure = fig

    most_common, stability = _compute_formation_metrics(timeline)
    state.tac_most_common_formation = most_common
    state.tac_formation_stability = stability
    state.tac_warning_text = ""

    logger.info("Position plots: %d rows, formation=%s, stability=%s", len(timeline), most_common, stability)


def _clear_position_plots(state: Any) -> None:
    """Reset position plots state to defaults."""
    state.tac_position_plot_figure = None
    state.tac_most_common_formation = "\u2014"
    state.tac_formation_stability = "\u2014"


# ---------------------------------------------------------------------------
# Formation Comparison sub-view
# ---------------------------------------------------------------------------


def _build_formation_comparison(df: pd.DataFrame) -> go.Figure | None:
    """Build dual-lane timeline comparing EFPI and Shape Graph formation detectors.

    Two rows of colored horizontal bars — same formation label = same color.
    Uses make_subplots(rows=2, cols=1, shared_xaxes=True).
    """
    if df.empty:
        return None

    efpi = df[df["detector"] == "efpi"].copy()
    sg = df[df["detector"] == "shape_graph"].copy()

    if efpi.empty and sg.empty:
        return None

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=["EFPI (Elastic Formation Pattern Index)", "Shape Graph (Sotudeh)"],
    )

    # Build consistent color map across both detectors
    palette_map: dict[str, str] = {}
    all_labels = sorted(set(df["formation_label"].dropna().unique()))
    for label in all_labels:
        _formation_color(label, palette_map)

    # Track which labels have been added to the legend
    legend_added: set[str] = set()

    for _row_idx, (detector_df, row_num) in enumerate([(efpi, 1), (sg, 2)]):
        if detector_df.empty:
            continue
        for _, seg in detector_df.iterrows():
            label = str(seg["formation_label"])
            start = float(seg["window_start_s"]) / 60.0  # convert to minutes
            end = float(seg["window_end_s"]) / 60.0
            color = _formation_color(label, palette_map)
            show_legend = label not in legend_added
            if show_legend:
                legend_added.add(label)
            # Show formation label as text on bars wide enough to fit (>2 min)
            bar_text = label if (end - start) > 2.0 else ""
            fig.add_trace(
                go.Bar(
                    x=[end - start],
                    y=[0],
                    base=[start],
                    orientation="h",
                    marker_color=color,
                    name=label,
                    showlegend=show_legend,
                    legendgroup=label,
                    text=[bar_text],
                    textposition="inside",
                    textfont=dict(color="white", size=10),
                    hovertemplate=(f"<b>{label}</b><br>Time: {start:.1f}\u2013{end:.1f} min<br><extra></extra>"),
                ),
                row=row_num,
                col=1,
            )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_CHART_BG,
        plot_bgcolor="rgba(26,26,46,1)",
        title=dict(
            text="Formation Detection Comparison (same color = same formation)",
            font=dict(color="white", size=14),
        ),
        barmode="stack",
        height=350,
        margin=dict(l=40, r=20, t=70, b=50),
        font=dict(color="white"),
        legend=dict(
            title="Formation",
            orientation="h",
            yanchor="bottom",
            y=1.08,
            xanchor="right",
            x=1,
        ),
    )

    # Style subplots
    fig.update_xaxes(title_text="Match Time (min)", row=2, col=1, color="white")
    fig.update_yaxes(visible=False, row=1, col=1)
    fig.update_yaxes(visible=False, row=2, col=1)

    # Style subplot titles
    for annotation in fig.layout.annotations:
        annotation.font = dict(color="white", size=12)

    return fig


def _compute_comparison_metrics(
    df: pd.DataFrame,
) -> tuple[str, str, str, str, str]:
    """Compute formation comparison metrics.

    Returns (agreement_rate, efpi_changes, sg_changes, efpi_dominant, sg_dominant).
    """
    if df.empty:
        return "\u2014", "\u2014", "\u2014", "\u2014", "\u2014"

    efpi = df[df["detector"] == "efpi"].sort_values(["period", "window_start_s"])
    sg = df[df["detector"] == "shape_graph"].sort_values(["period", "window_start_s"])

    # Dominant formation per detector (mode)
    efpi_dom = str(efpi["formation_label"].mode().iloc[0]) if not efpi.empty else "\u2014"
    sg_dom = str(sg["formation_label"].mode().iloc[0]) if not sg.empty else "\u2014"

    # Formation changes: count distinct consecutive label transitions
    def _count_transitions(series: pd.Series) -> int:  # type: ignore[type-arg]
        if len(series) < 2:
            return 0
        return int((series != series.shift()).sum() - 1)

    efpi_ch = str(_count_transitions(efpi["formation_label"])) if not efpi.empty else "\u2014"
    sg_ch = str(_count_transitions(sg["formation_label"])) if not sg.empty else "\u2014"

    # Agreement rate: weighted by duration where both assign the same formation
    # Match windows by overlapping time ranges
    if efpi.empty or sg.empty:
        return "\u2014", efpi_ch, sg_ch, efpi_dom, sg_dom

    total_overlap_s = 0.0
    agree_s = 0.0

    for _, e_row in efpi.iterrows():
        e_start = float(e_row["window_start_s"])
        e_end = float(e_row["window_end_s"])
        e_label = str(e_row["formation_label"])

        for _, s_row in sg.iterrows():
            s_start = float(s_row["window_start_s"])
            s_end = float(s_row["window_end_s"])
            s_label = str(s_row["formation_label"])

            # Compute overlap
            overlap_start = max(e_start, s_start)
            overlap_end = min(e_end, s_end)
            if overlap_end <= overlap_start:
                continue

            duration = overlap_end - overlap_start
            total_overlap_s += duration
            if e_label == s_label:
                agree_s += duration

    if total_overlap_s > 0:
        agreement = (agree_s / total_overlap_s) * 100.0
        agreement_str = f"{agreement:.0f}%"
    else:
        agreement_str = "\u2014"

    return agreement_str, efpi_ch, sg_ch, efpi_dom, sg_dom


def _refresh_formation_comparison(state: Any) -> None:
    """Refresh the Formation Comparison sub-view."""
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_formation_comparison(state)
        state.tac_warning_text = "Select a tracking match to view formation comparison."
        return

    team_side = _resolve_team_side(state)
    if team_side is None:
        _clear_formation_comparison(state)
        state.tac_warning_text = "Select a team to view formation comparison."
        return

    try:
        labels = fetch_formation_labels_dual(match_id, team_side)
    except Exception:
        logger.exception("Failed to fetch formation labels")
        _clear_formation_comparison(state)
        state.tac_warning_text = "Something went wrong loading formations. Try refreshing the page."
        return

    if labels.empty:
        _clear_formation_comparison(state)
        state.tac_warning_text = "No formation data for this match and team. Try selecting a different match."
        return

    state.tac_formation_comparison_figure = _build_formation_comparison(labels)

    agreement, efpi_ch, sg_ch, efpi_dom, sg_dom = _compute_comparison_metrics(labels)
    state.tac_agreement_rate = agreement
    state.tac_efpi_changes = efpi_ch
    state.tac_sg_changes = sg_ch
    state.tac_efpi_dominant = efpi_dom
    state.tac_sg_dominant = sg_dom
    state.tac_warning_text = ""

    logger.info("Formation comparison: %d rows, agreement=%s", len(labels), agreement)


def _clear_formation_comparison(state: Any) -> None:
    """Reset formation comparison state to defaults."""
    state.tac_formation_comparison_figure = None
    state.tac_agreement_rate = "\u2014"
    state.tac_efpi_changes = "\u2014"
    state.tac_sg_changes = "\u2014"
    state.tac_efpi_dominant = "\u2014"
    state.tac_sg_dominant = "\u2014"


# ---------------------------------------------------------------------------
# Position Maps sub-view
# ---------------------------------------------------------------------------


def _build_position_map_heatmap(
    player_data: pd.DataFrame,
    player_name: str,
) -> go.Figure:
    """Build a 5x5 position map heatmap for a single player.

    Rows: vertical levels (F at top, B at bottom — attacking direction up).
    Columns: horizontal levels (L, LC, C, RC, R).
    Cell values: pct_time with text annotations.
    """
    # Build 5x5 grid, default zero
    grid = np.zeros((len(_LEVEL_ORDER), len(_HORIZ_ORDER)), dtype=np.float64)

    for _, row in player_data.iterrows():
        v_level = str(row.get("vertical_level", ""))
        h_level = str(row.get("horizontal_level", ""))
        pct = float(row.get("pct_time", 0.0))

        if v_level in _LEVEL_NUMERIC and h_level in _HORIZ_ORDER:
            v_idx = _LEVEL_NUMERIC[v_level]
            h_idx = _HORIZ_ORDER.index(h_level)
            grid[v_idx, h_idx] = pct

    # Reverse vertical order so F is at top (display convention: attacking direction up)
    display_grid = grid[::-1]
    display_levels = [_LEVEL_LABELS.get(lv, lv) for lv in reversed(_LEVEL_ORDER)]
    display_horiz = [_HORIZ_LABELS.get(h, h) for h in _HORIZ_ORDER]

    # Text annotations showing pct_time
    text_annotations = [[f"{v:.1f}%" if v > 0.5 else "" for v in row] for row in display_grid]

    fig = go.Figure(
        data=go.Heatmap(
            z=display_grid.tolist(),
            x=display_horiz,
            y=display_levels,
            text=text_annotations,
            texttemplate="%{text}",
            textfont=dict(color="white", size=12),
            colorscale="Blues",
            showscale=True,
            colorbar=dict(title="% Time", ticksuffix="%"),
            hovertemplate=("<b>%{y} / %{x}</b><br>Time: %{z:.1f}%<br><extra></extra>"),
        )
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=_CHART_BG,
        title=dict(
            text=f"Position Map \u2014 {player_name}",
            font=dict(color="white", size=14),
        ),
        xaxis=dict(title="Horizontal Position", color="white", side="bottom"),
        yaxis=dict(title="Vertical Level", color="white"),
        height=400,
        width=500,
        margin=dict(l=60, r=80, t=50, b=50),
        font=dict(color="white"),
    )

    return fig


def _compute_position_map_metrics(player_data: pd.DataFrame) -> tuple[str, str, str]:
    """Compute position map metrics for a single player.

    Returns (primary_position, versatility, vertical_range).
    - primary_position: position label with highest pct_time
    - versatility: count of cells with pct_time > 10%
    - vertical_range: count of distinct vertical levels with pct_time > 5%
    """
    if player_data.empty:
        return "\u2014", "\u2014", "\u2014"

    # Primary position: max pct_time
    max_row = player_data.loc[player_data["pct_time"].idxmax()]
    primary = f"{max_row['vertical_level']}/{max_row['horizontal_level']} ({max_row['pct_time']:.1f}%)"

    # Versatility: cells > 10%
    versatile_count = int((player_data["pct_time"] > 10.0).sum())
    if versatile_count <= 1:
        versatility_label = "specialist"
    elif versatile_count <= 2:
        versatility_label = "moderate"
    elif versatile_count <= 3:
        versatility_label = "flexible"
    else:
        versatility_label = "versatile"
    versatility = f"{versatile_count} positions ({versatility_label})"

    # Vertical range: distinct levels > 5%
    level_pcts = player_data.groupby("vertical_level")["pct_time"].sum()
    active_levels = level_pcts[level_pcts > 5.0]
    if len(active_levels) > 0:
        range_labels = [lv for lv in _LEVEL_ORDER if lv in active_levels.index]
        vertical_range = f"{range_labels[0]}\u2013{range_labels[-1]} ({len(range_labels)} levels)"
    else:
        vertical_range = "\u2014"

    return primary, versatility, vertical_range


def _refresh_position_maps(state: Any) -> None:
    """Refresh the Position Maps sub-view."""
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_position_maps(state)
        state.tac_warning_text = "Select a tracking match to view position maps."
        return

    team_side = _resolve_team_side(state)
    if team_side is None:
        _clear_position_maps(state)
        state.tac_warning_text = "Select a team to view position maps."
        return

    # Populate player LOV
    try:
        players_df = fetch_tp_players(match_id, team_side)
    except Exception:
        logger.exception("Failed to fetch players for position maps")
        _clear_position_maps(state)
        state.tac_warning_text = "Something went wrong loading the player list. Try refreshing the page."
        return

    if players_df.empty:
        _clear_position_maps(state)
        state.tac_warning_text = "No position map data for this match and team. Try selecting a different team."
        return

    player_names = players_df["player_display_name"].tolist()
    state.tac_player_lov = player_names
    state.tac_compare_player_lov = player_names

    # Auto-select first player if none selected or selection no longer valid
    if not state.tac_selected_player or state.tac_selected_player not in player_names:
        state.tac_selected_player = player_names[0] if player_names else None

    if state.tac_selected_player is None:
        _clear_position_maps(state)
        state.tac_warning_text = "No players available for this selection."
        return

    # Fetch position map for selected player
    player_id = _get_player_id_from_label(state.tac_selected_player, players_df)
    try:
        pm_data = fetch_position_maps(match_id, team_side, player_id)
    except Exception:
        logger.exception("Failed to fetch position maps for player %s", player_id)
        _clear_position_maps(state)
        state.tac_warning_text = "Something went wrong loading position maps. Try refreshing the page."
        return

    if pm_data.empty:
        _clear_position_maps(state)
        state.tac_warning_text = "No position map data for this player. Try selecting a different player."
        return

    # Build primary heatmap
    state.tac_position_map_figure = _build_position_map_heatmap(pm_data, state.tac_selected_player)

    # Compute metrics
    primary, versatility, vert_range = _compute_position_map_metrics(pm_data)
    state.tac_primary_position = primary
    state.tac_position_versatility = versatility
    state.tac_vertical_range = vert_range

    # Build comparison heatmap if a compare player is selected
    if (
        state.tac_selected_compare_player
        and state.tac_selected_compare_player in player_names
        and state.tac_selected_compare_player != state.tac_selected_player
    ):
        compare_id = _get_player_id_from_label(state.tac_selected_compare_player, players_df)
        try:
            compare_data = fetch_position_maps(match_id, team_side, compare_id)
            if not compare_data.empty:
                state.tac_position_map_compare_figure = _build_position_map_heatmap(
                    compare_data, state.tac_selected_compare_player
                )
            else:
                state.tac_position_map_compare_figure = None
        except Exception:
            logger.exception("Failed to fetch compare position map for %s", compare_id)
            state.tac_position_map_compare_figure = None
    else:
        state.tac_position_map_compare_figure = None

    state.tac_warning_text = ""
    logger.info("Position maps: player=%s, %d rows", state.tac_selected_player, len(pm_data))


def _clear_position_maps(state: Any) -> None:
    """Reset position maps state to defaults."""
    state.tac_position_map_figure = None
    state.tac_position_map_compare_figure = None
    state.tac_primary_position = "\u2014"
    state.tac_position_versatility = "\u2014"
    state.tac_vertical_range = "\u2014"


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def tac_on_team_change(state: Any, var_name: str, var_value: Any) -> None:
    """Team selector changed — re-render current sub-view."""
    _dispatch_refresh(state)


def tac_on_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Player selector changed — re-render position maps."""
    if state.selected_sub_view == "Position Maps":
        _refresh_position_maps(state)


def tac_on_compare_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Compare player selector changed — re-render position maps."""
    if state.selected_sub_view == "Position Maps":
        _refresh_position_maps(state)


# ---------------------------------------------------------------------------
# Dispatch + main refresh
# ---------------------------------------------------------------------------


def _dispatch_refresh(state: Any) -> None:
    """Dispatch to the correct sub-view refresh."""
    view = state.selected_sub_view
    if view == "Position Plots":
        _refresh_position_plots(state)
    elif view == "Formation Comparison":
        _refresh_formation_comparison(state)
    elif view == "Position Maps":
        _refresh_position_maps(state)
    else:
        logger.warning("Unknown Tactical Positions sub-view: %r", view)


def tac_refresh(state: Any) -> None:
    """Full refresh: configure sub-views, LOVs, then render.

    Called when tracking match or provider changes via shared callbacks.
    """
    # Claim this page's sub-view LOV
    if not getattr(state, "sub_view_lov", None) or state.sub_view_lov != TAC_SUB_VIEW_LOV:
        state.sub_view_lov = TAC_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in TAC_SUB_VIEW_LOV:
        state.selected_sub_view = TAC_SUB_VIEW_LOV[0]

    # Initialize team LOV
    _init_team_lov(state)

    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        _clear_position_plots(state)
        _clear_formation_comparison(state)
        _clear_position_maps(state)
        # Check if any tracking matches exist
        tracking_lov = getattr(state, "tracking_match_lov", [])
        if not tracking_lov:
            state.tac_warning_text = (
                "Tactical positions requires player tracking data (~20 matches from Metrica, IDSSE, SkillCorner)."
            )
        else:
            state.tac_warning_text = ""
        return

    _dispatch_refresh(state)

    state.tac_data_freshness = fetch_data_freshness()

    # Scope label for display
    events = fetch_match_events(match_id)
    if not events.empty:
        r = events.iloc[0]
        state.tac_scope_label = f"{r['home_team_name']} v {r['away_team_name']}"
    else:
        # Fallback: tracking metadata team names
        teams_df = fetch_tracking_teams(match_id)
        home_name, away_name = "", ""
        if not teams_df.empty:
            for _, row in teams_df.iterrows():
                if row.get("team") == "home":
                    home_name = row.get("team_display_name", "")
                elif row.get("team") == "away":
                    away_name = row.get("team_display_name", "")
        if home_name and away_name and home_name != "Home":
            state.tac_scope_label = f"{home_name} v {away_name}"
        else:
            state.tac_scope_label = match_id


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Tactical-Positions", tac_refresh)
