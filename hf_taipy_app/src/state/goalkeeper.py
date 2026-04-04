"""Goalkeeper Analytics state — rankings, shot stopping, distribution.

Prefix: gk_
Three sub-views controlled by shared.selected_sub_view:
  - "Rankings": GK leaderboard with four-pillar stats
  - "Shot Stopping": goalmouth scatter + goals prevented bar chart
  - "Distribution": half-pitch pass map colored by distance category
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from filters import fetch_data_freshness, fetch_scope_label
from mplsoccer import VerticalPitch
from queries.goalkeepers import (
    fetch_gk_passes,
    fetch_gk_player_lov,
    fetch_gk_rankings,
    fetch_gk_shots,
    resolve_gk_team_id,
)
from render import PITCH_BG_COLOR, PITCH_LINE_COLOR, pitch_to_file

from state.shared import (
    get_comp_id,
    get_team_id,
    register_page_refresher,
)

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sub-view list of values (set on page navigate)
# ---------------------------------------------------------------------------
GK_SUB_VIEW_LOV: list[str] = ["Rankings", "Shot Stopping", "Distribution"]

# ---------------------------------------------------------------------------
# Pass distance thresholds (mirrors analytics.goalkeeper constants)
# ---------------------------------------------------------------------------
_SHORT_THRESHOLD = 32.0
_LONG_THRESHOLD = 60.0

# ---------------------------------------------------------------------------
# Chart color palette (consistent with app dark theme)
# ---------------------------------------------------------------------------
_SAVED_COLOR = "#3b82f6"  # blue
_GOAL_COLOR = "#ef4444"  # red
_POSITIVE_COLOR = "#2a9d8f"  # teal-green
_NEGATIVE_COLOR = "#e63946"  # coral-red
_SHORT_COLOR = "#3b82f6"  # blue
_MEDIUM_COLOR = "#f59e0b"  # amber
_LONG_COLOR = "#ef4444"  # red

# ---------------------------------------------------------------------------
# Exported state variables (all gk_ prefixed)
# ---------------------------------------------------------------------------

# Rankings sub-view
_GK_RANKINGS_COLS = [
    "Player",
    "Matches",
    "Minutes",
    "Saves",
    "Save %",
    "Goals Prevented",
    "Launch Rate",
    "xT / Pass",
    "Sweeper Distance",
    "Outside Box/90",
]

gk_rankings_df: pd.DataFrame = pd.DataFrame(columns=_GK_RANKINGS_COLS)
gk_scope_label: str = ""
gk_warning_text: str = ""

# Shot Stopping sub-view
gk_goalmouth_figure: go.Figure | None = None
gk_goals_prevented_figure: go.Figure | None = None
gk_psxg_faced: str = "\u2014"
gk_goals_prevented_val: str = "\u2014"
gk_save_pct_val: str = "\u2014"

# Distribution sub-view
gk_distribution_image: str = ""
gk_short_pct: str = "\u2014"
gk_medium_pct: str = "\u2014"
gk_long_pct: str = "\u2014"
gk_launch_rate_val: str = "\u2014"
gk_xt_per_pass_val: str = "\u2014"  # noqa: S105
gk_xt_total_val: str = "\u2014"

# GK-specific player selector (only goalkeepers, not all players)
gk_player_lov: list[str] = []
gk_selected_player: str | None = None

# Freshness
gk_data_freshness: str = ""

# Internal map: display label -> player_id for GK selector
_gk_player_map: dict[str, int] = {}

__all__ = [
    "GK_SUB_VIEW_LOV",
    "gk_data_freshness",
    "gk_distribution_image",
    "gk_goals_prevented_figure",
    "gk_goals_prevented_val",
    "gk_goalmouth_figure",
    "gk_launch_rate_val",
    "gk_long_pct",
    "gk_medium_pct",
    "gk_on_gk_player_change",
    "gk_on_rankings_action",
    "gk_player_lov",
    "gk_psxg_faced",
    "gk_rankings_df",
    "gk_refresh",
    "gk_save_pct_val",
    "gk_scope_label",
    "gk_selected_player",
    "gk_short_pct",
    "gk_warning_text",
    "gk_xt_per_pass_val",
    "gk_xt_total_val",
]


def _get_gk_player_id(state: Any) -> int | None:
    """Resolve GK-specific player selection to player_id."""
    label = state.gk_selected_player
    return _gk_player_map.get(label) if label else None


def gk_on_gk_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """GK player selector changed — refresh current sub-view."""
    _dispatch_refresh(state)


def _populate_gk_player_lov(state: Any, comp_id: int, team_id: int | None = None) -> None:
    """Populate GK-only player dropdown from fct_goalkeeper_stats."""
    global _gk_player_map
    try:
        players = fetch_gk_player_lov(comp_id, team_id)
        _gk_player_map = {label: pid for label, pid in players}
        state.gk_player_lov = [label for label, _ in players]
        # Clear selection if no longer in LOV
        if state.gk_selected_player and state.gk_selected_player not in _gk_player_map:
            state.gk_selected_player = None
    except Exception:
        logger.exception("Failed to fetch GK player LOV")
        _gk_player_map = {}
        state.gk_player_lov = []


# ---------------------------------------------------------------------------
# Rankings formatter
# ---------------------------------------------------------------------------


def _format_rankings_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format GK rankings DataFrame for Taipy table display.

    Returns a renamed DataFrame with human-readable column names.
    """
    if df.empty:
        return pd.DataFrame(columns=_GK_RANKINGS_COLS)

    # psycopg2 returns PostgreSQL numeric as Python Decimal — Taipy tables
    # render Decimal as blank. Convert all Decimal columns to float.
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

    display = df.rename(
        columns={
            "player_display_name": "Player",
            "matches": "Matches",
            "minutes_played": "Minutes",
            "saves": "Saves",
            "save_pct": "Save %",
            "goals_prevented": "Goals Prevented",
            "psxg_faced": "PSxG Faced",
            "launch_rate": "Launch Rate",
            "gk_xt_per_pass": "xT / Pass",
            "claim_success_rate": "Claim Success %",
            "avg_defensive_action_distance": "Sweeper Distance",
            "actions_outside_box_per_90": "Outside Box/90",
        }
    )

    # Round numeric columns for display
    for col in [
        "Save %",
        "Goals Prevented",
        "Launch Rate",
        "xT / Pass",
        "Sweeper Distance",
        "Outside Box/90",
    ]:
        if col in display.columns:
            display[col] = display[col].round(2)

    # Keep player_id for cross-link but don't display it; select desired columns
    keep = ["player_id"] + [c for c in _GK_RANKINGS_COLS if c in display.columns]
    display = display[[c for c in keep if c in display.columns]]

    return display


# ---------------------------------------------------------------------------
# Shot Stopping charts
# ---------------------------------------------------------------------------


def _build_goalmouth_scatter(shots: pd.DataFrame) -> go.Figure | None:
    """Build a shot scatter plot of on-target shots faced.

    Uses pitch coordinates (end_x, end_y) in the StatsBomb 120x80 system.
    Zooms into the goal area (penalty box region) for a GK-centric view.
    """
    fig = go.Figure()

    if not shots.empty and "end_x" in shots.columns and "end_y" in shots.columns:
        df = shots.copy()

        # Separate saved vs goal
        saved_mask = df["shot_outcome"].str.lower().isin(["saved", "saved off target", "saved to post"])
        goal_mask = df["shot_outcome"].str.lower() == "goal"

        for mask, color, name in [
            (saved_mask, _SAVED_COLOR, "Saved"),
            (goal_mask, _GOAL_COLOR, "Goal"),
        ]:
            subset = df[mask]
            if subset.empty:
                continue
            fig.add_trace(
                go.Scatter(
                    x=subset["end_x"],
                    y=subset["end_y"],
                    mode="markers",
                    marker=dict(size=8, color=color, opacity=0.7),
                    name=name,
                    hovertemplate=(
                        "<b>%{customdata}</b><br>Outcome: " + name + "<br>Position: (%{x:.1f}, %{y:.1f})<extra></extra>"
                    ),
                    customdata=subset["shooter_name"],
                )
            )

    # Goal line and penalty area outline
    fig.add_shape(type="rect", x0=102, y0=18, x1=120, y1=62, line=dict(color="rgba(255,255,255,0.3)", width=1))
    fig.add_shape(type="line", x0=120, y0=36, x1=120, y1=44, line=dict(color="#e0e0e0", width=3))

    fig.update_layout(
        title="Shot Map — On-Target Shots Faced (penalty area view)",
        title_font=dict(color="white", size=14),
        template="plotly_dark",
        plot_bgcolor=PITCH_BG_COLOR,
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(
            title="Distance to Goal Line (m)",
            range=[95, 122],
            showgrid=False,
            zeroline=False,
            color="white",
        ),
        yaxis=dict(
            title="Across Goal (m)",
            range=[10, 70],
            showgrid=False,
            zeroline=False,
            color="white",
        ),
        margin=dict(l=50, r=20, t=50, b=50),
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(color="white"),
    )

    return fig


def _build_goals_prevented_chart(rankings: pd.DataFrame, selected_player_id: int | None = None) -> go.Figure | None:
    """Build horizontal bar chart of goals prevented per GK.

    Positive = GK outperformed PSxG (green), negative = underperformed (red).
    The selected GK's bar is highlighted with full opacity; others are dimmed.
    """
    if rankings.empty or "goals_prevented" not in rankings.columns:
        return None

    df = rankings.dropna(subset=["goals_prevented"]).copy()
    if df.empty:
        return None

    # Use display name if available, fall back to player_id
    name_col = "player_display_name" if "player_display_name" in df.columns else "player_id"
    df = df.sort_values("goals_prevented", ascending=True).tail(20)

    # Color bars: green/red by sign, dimmed if a player is selected and this isn't them
    colors = []
    for _, row in df.iterrows():
        base = _POSITIVE_COLOR if row["goals_prevented"] >= 0 else _NEGATIVE_COLOR
        if selected_player_id is not None and row["player_id"] != selected_player_id:
            # Dim non-selected bars
            base = base.replace("1)", "0.3)") if "rgba" in base else base
            colors.append("rgba(128,128,128,0.3)")
        else:
            colors.append(base)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=df[name_col],
            x=df["goals_prevented"],
            orientation="h",
            marker_color=colors,
            hovertemplate="<b>%{y}</b><br>Goals Prevented: %{x:.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        title="Goals Prevented (PSxG - Goals Conceded, higher = better)",
        title_font=dict(color="white", size=14),
        template="plotly_dark",
        plot_bgcolor=PITCH_BG_COLOR,
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="Goals Prevented", color="white", gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(color="white"),
        margin=dict(l=140, r=20, t=50, b=50),
        height=max(300, len(df) * 28),
        font=dict(color="white"),
        showlegend=False,
    )

    return fig


# ---------------------------------------------------------------------------
# Distribution pitch render
# ---------------------------------------------------------------------------


def _compute_pass_distance(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    """Compute Euclidean pass distance from start to end coordinates."""
    dx = df["end_x"] - df["start_x"]
    dy = df["end_y"] - df["start_y"]
    return np.sqrt(dx**2 + dy**2)


def _categorise_distance(distance: pd.Series) -> pd.Series:  # type: ignore[type-arg]
    """Categorise pass distance into short/medium/long."""
    return pd.Series(
        np.where(
            distance < _SHORT_THRESHOLD,
            "short",
            np.where(distance > _LONG_THRESHOLD, "long", "medium"),
        ),
        index=distance.index,
    )


def _render_distribution_pitch(passes: pd.DataFrame) -> str:
    """Render GK distribution passes on a half-pitch, colored by distance category.

    Returns file path to the rendered PNG.
    """
    pitch = VerticalPitch(half=True, pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    fig, ax = pitch.draw(figsize=(8, 10))
    ax.set_title(
        "GK Distribution \u2014 Pass Origins and Destinations by Distance", color=PITCH_LINE_COLOR, fontsize=14, pad=10
    )

    if not passes.empty and all(c in passes.columns for c in ("start_x", "start_y", "end_x", "end_y")):
        df = passes.copy()
        df["distance"] = _compute_pass_distance(df)
        df["category"] = _categorise_distance(df["distance"])

        color_map = {"short": _SHORT_COLOR, "medium": _MEDIUM_COLOR, "long": _LONG_COLOR}

        for cat, color in color_map.items():
            subset = df[df["category"] == cat]
            if subset.empty:
                continue
            # Draw pass arrows (start -> end)
            pitch.arrows(
                subset["start_x"],
                subset["start_y"],
                subset["end_x"],
                subset["end_y"],
                ax=ax,
                color=color,
                width=1.5,
                headwidth=4,
                headlength=3,
                alpha=0.6,
                label=f"{cat.title()} (n={len(subset)})",
            )

        ax.legend(
            loc="upper left",
            fontsize=9,
            facecolor=PITCH_BG_COLOR,
            edgecolor="#555577",
            labelcolor=PITCH_LINE_COLOR,
        )

    return pitch_to_file(fig, "gk_distribution")


# ---------------------------------------------------------------------------
# Sub-view refresh functions
# ---------------------------------------------------------------------------

# Module-level cache for rankings data (shared between Rankings and Shot Stopping)
_cached_rankings: pd.DataFrame = pd.DataFrame()


def _refresh_rankings(state: Any) -> None:
    """Refresh the Rankings sub-view."""
    global _cached_rankings

    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        state.gk_rankings_df = pd.DataFrame(columns=_GK_RANKINGS_COLS)
        state.gk_scope_label = ""
        state.gk_warning_text = ""
        _cached_rankings = pd.DataFrame()
        return

    team_id = get_team_id(state.selected_team)
    state.gk_scope_label = fetch_scope_label(comp_id, team_id)

    min_min = int(state.min_minutes) if hasattr(state, "min_minutes") else 90

    try:
        rankings = fetch_gk_rankings(comp_id, team_id, min_min)
    except Exception:
        logger.exception("Failed to fetch GK rankings")
        state.gk_rankings_df = pd.DataFrame(columns=_GK_RANKINGS_COLS)
        state.gk_warning_text = "Error loading GK rankings."
        _cached_rankings = pd.DataFrame()
        return

    _cached_rankings = rankings
    table = _format_rankings_table(rankings)
    state.gk_rankings_df = table
    state.gk_warning_text = "" if not table.empty else "No GK data available for the selected filters."

    logger.info("GK rankings: %d rows", len(table))


def _refresh_shot_stopping(state: Any) -> None:
    """Refresh the Shot Stopping sub-view."""
    global _cached_rankings

    comp_id = get_comp_id(state.selected_competition)
    player_id = _get_gk_player_id(state)

    if comp_id is None:
        state.gk_goalmouth_figure = None
        state.gk_goals_prevented_figure = None
        state.gk_psxg_faced = "\u2014"
        state.gk_goals_prevented_val = "\u2014"
        state.gk_save_pct_val = "\u2014"
        state.gk_warning_text = ""
        return

    team_id = get_team_id(state.selected_team)
    state.gk_scope_label = fetch_scope_label(comp_id, team_id)

    # Resolve GK's team for shot filtering (scoped to selected competition)
    gk_team_id = resolve_gk_team_id(player_id, comp_id) if player_id else None

    # Fetch shots faced
    try:
        shots = fetch_gk_shots(comp_id, player_id, gk_team_id)
    except Exception:
        logger.exception("Failed to fetch GK shots")
        state.gk_goalmouth_figure = None
        state.gk_goals_prevented_figure = None
        state.gk_warning_text = "Error loading shot stopping data."
        return

    state.gk_goalmouth_figure = _build_goalmouth_scatter(shots)

    # Goals prevented chart — use cached rankings or re-fetch
    min_min = int(state.min_minutes) if hasattr(state, "min_minutes") else 90
    if _cached_rankings.empty:
        try:
            _cached_rankings = fetch_gk_rankings(comp_id, team_id, min_min)
        except Exception:
            logger.exception("Failed to fetch GK rankings for goals prevented chart")
            _cached_rankings = pd.DataFrame()

    state.gk_goals_prevented_figure = _build_goals_prevented_chart(_cached_rankings, player_id)

    # Set metric state vars from selected GK row, including shot count for context
    n_shots = len(shots)
    if player_id is not None and not _cached_rankings.empty:
        player_rows = _cached_rankings[_cached_rankings["player_id"] == player_id]
        if not player_rows.empty:
            row = player_rows.iloc[0]
            psxg = row.get("psxg_faced")
            state.gk_psxg_faced = f"{psxg:.2f} ({n_shots} shots)" if pd.notna(psxg) else "\u2014"
            gp = row.get("goals_prevented")
            state.gk_goals_prevented_val = f"{gp:+.2f}" if pd.notna(gp) else "\u2014"
            sp = row.get("save_pct")
            state.gk_save_pct_val = f"{sp:.1f}%" if pd.notna(sp) else "\u2014"
        else:
            state.gk_psxg_faced = "\u2014"
            state.gk_goals_prevented_val = "\u2014"
            state.gk_save_pct_val = "\u2014"
    else:
        state.gk_psxg_faced = "\u2014"
        state.gk_goals_prevented_val = "\u2014"
        state.gk_save_pct_val = "\u2014"

    state.gk_warning_text = "" if not shots.empty else "No on-target shots found for the selected filters."

    logger.info("GK shot stopping: %d shots", len(shots))


def _refresh_distribution(state: Any) -> None:
    """Refresh the Distribution sub-view."""
    comp_id = get_comp_id(state.selected_competition)
    player_id = _get_gk_player_id(state)

    if comp_id is None:
        state.gk_distribution_image = ""
        state.gk_short_pct = "\u2014"
        state.gk_medium_pct = "\u2014"
        state.gk_long_pct = "\u2014"
        state.gk_launch_rate_val = "\u2014"
        state.gk_xt_per_pass_val = "\u2014"  # noqa: S105
        state.gk_xt_total_val = "\u2014"
        state.gk_warning_text = ""
        return

    team_id = get_team_id(state.selected_team)
    state.gk_scope_label = fetch_scope_label(comp_id, team_id)

    try:
        passes = fetch_gk_passes(comp_id, player_id)
    except Exception:
        logger.exception("Failed to fetch GK passes")
        state.gk_distribution_image = ""
        state.gk_warning_text = "Error loading distribution data."
        return

    if passes.empty:
        state.gk_distribution_image = ""
        state.gk_short_pct = "0%"
        state.gk_medium_pct = "0%"
        state.gk_long_pct = "0%"
        state.gk_launch_rate_val = "\u2014"
        state.gk_xt_per_pass_val = "\u2014"  # noqa: S105
        state.gk_xt_total_val = "\u2014"
        state.gk_warning_text = "No GK distribution passes for the selected filters."
        return

    state.gk_warning_text = ""

    # Render pitch
    state.gk_distribution_image = _render_distribution_pitch(passes)

    # Compute distance categories for percentages
    distances = _compute_pass_distance(passes)
    categories = _categorise_distance(distances)
    total = len(passes)

    n_short = int((categories == "short").sum())
    n_medium = int((categories == "medium").sum())
    n_long = int((categories == "long").sum())

    state.gk_short_pct = f"{n_short / total * 100:.0f}%" if total > 0 else "0%"
    state.gk_medium_pct = f"{n_medium / total * 100:.0f}%" if total > 0 else "0%"
    state.gk_long_pct = f"{n_long / total * 100:.0f}%" if total > 0 else "0%"

    # Pull per-GK metrics from cached rankings if available
    if player_id is not None and not _cached_rankings.empty:
        player_rows = _cached_rankings[_cached_rankings["player_id"] == player_id]
        if not player_rows.empty:
            row = player_rows.iloc[0]
            lr = row.get("launch_rate")
            state.gk_launch_rate_val = f"{lr:.1f}%" if pd.notna(lr) else "\u2014"
            xtp = row.get("gk_xt_per_pass")
            state.gk_xt_per_pass_val = f"{xtp:.4f}" if pd.notna(xtp) else "\u2014"
            xtd = row.get("gk_xt_delta_total")
            state.gk_xt_total_val = f"{xtd:.3f}" if pd.notna(xtd) else "\u2014"
        else:
            state.gk_launch_rate_val = "\u2014"
            state.gk_xt_per_pass_val = "\u2014"  # noqa: S105
            state.gk_xt_total_val = "\u2014"
    else:
        state.gk_launch_rate_val = "\u2014"
        state.gk_xt_per_pass_val = "\u2014"  # noqa: S105
        state.gk_xt_total_val = "\u2014"

    logger.info("GK distribution: %d passes (short=%d, medium=%d, long=%d)", total, n_short, n_medium, n_long)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _dispatch_refresh(state: Any) -> None:
    """Dispatch to the correct sub-view refresh."""
    view = state.selected_sub_view
    if view == "Rankings":
        _refresh_rankings(state)
    elif view == "Shot Stopping":
        _refresh_shot_stopping(state)
    elif view == "Distribution":
        _refresh_distribution(state)
    else:
        logger.warning("Unknown GK sub-view: %r", view)


def gk_refresh(state: Any) -> None:
    """Full refresh: configure sub-views, LOVs, then dispatch."""
    # Ensure sub-view LOV is populated on page navigate
    if not getattr(state, "sub_view_lov", None) or state.sub_view_lov != GK_SUB_VIEW_LOV:
        state.sub_view_lov = GK_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in GK_SUB_VIEW_LOV:
        state.selected_sub_view = GK_SUB_VIEW_LOV[0]

    # Populate GK-only player dropdown (filtered by team if selected)
    comp_id = get_comp_id(state.selected_competition)
    team_id = get_team_id(state.selected_team)
    if comp_id is not None:
        _populate_gk_player_lov(state, comp_id, team_id)

    _dispatch_refresh(state)

    state.gk_data_freshness = fetch_data_freshness()


# ---------------------------------------------------------------------------
# Cross-link callback
# ---------------------------------------------------------------------------


def gk_on_rankings_action(state: Any, id: str, payload: dict[str, Any]) -> None:
    """Handle row click in GK rankings table — navigate to Player Similarity."""
    from taipy.gui import navigate

    idx = payload.get("index")
    if idx is None:
        return
    try:
        row = state.gk_rankings_df.iloc[idx]
    except (IndexError, KeyError):
        return
    player_id = row.get("player_id")
    if player_id is not None:
        state.selected_player = str(player_id)
        navigate(state, "Player-Similarity")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Goalkeeper-Analytics", gk_refresh)
