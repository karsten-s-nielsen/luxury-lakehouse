"""Action Values (VAEP) state — rankings, action breakdown, match timeline.

Prefix: av_
Three sub-views controlled by shared.selected_sub_view:
  - "Rankings": player VAEP leaderboard
  - "Breakdown": VAEP by action type (horizontal bar chart)
  - "Timeline": match action value scatter over time
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import pandas as pd
from filters import fetch_data_freshness, fetch_scope_label
from queries.defensive import fetch_vaep_breakdown, fetch_vaep_rankings, fetch_vaep_timeline
from render import PITCH_BG_COLOR, TEXT_COLOR, chart_to_file, fmt_int

from state.shared import (
    get_comp_id,
    get_match_id,
    get_player_id,
    get_team_id,
    register_page_refresher,
)

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sub-view list of values (set on page navigate)
# ---------------------------------------------------------------------------
AV_SUB_VIEW_LOV: list[str] = ["Rankings", "Breakdown", "Timeline"]

# ---------------------------------------------------------------------------
# Exported state variables
# ---------------------------------------------------------------------------

# Rankings sub-view
_AV_RANKINGS_COLS = [
    "Player",
    "Pos",
    "Min",
    "Total VAEP",
    "VAEP per 90",
    "Pctile",
    "Off per 90",
    "Def per 90",
    "Actions",
]
av_rankings_data: pd.DataFrame = pd.DataFrame(columns=_AV_RANKINGS_COLS)
av_rankings_empty_msg: str = ""

# Breakdown sub-view
av_total_vaep: str = "--"
av_total_actions: str = "--"
av_top_action: str = "--"
av_breakdown_image: str = ""

# Timeline sub-view
av_positive: str = "--"
av_negative: str = "--"
av_net_vaep: str = "--"
av_most_valuable: str = "--"
av_timeline_image: str = ""
av_timeline_data: pd.DataFrame = pd.DataFrame(
    columns=["Action", "Minute", "Second", "Period", "Result", "VAEP Value", "Offensive", "Defensive"]
)

av_data_freshness: str = ""
av_scope_label: str = ""
av_warning_text: str = ""

__all__ = [
    "av_breakdown_image",
    "av_data_freshness",
    "av_most_valuable",
    "av_negative",
    "av_net_vaep",
    "av_positive",
    "av_rankings_data",
    "av_rankings_empty_msg",
    "av_refresh",
    "av_scope_label",
    "av_timeline_data",
    "av_timeline_image",
    "av_top_action",
    "av_total_actions",
    "av_total_vaep",
    "av_warning_text",
]


# ---------------------------------------------------------------------------
# Chart rendering
# ---------------------------------------------------------------------------


def _render_breakdown_chart(breakdown: pd.DataFrame) -> str:
    """Render VAEP by action type as a horizontal bar chart. Returns file path."""
    n_types = max(len(breakdown), 1)
    fig, ax = plt.subplots(figsize=(10, max(4, n_types * 0.4)))
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    if not breakdown.empty and "action_type" in breakdown.columns and "total_vaep" in breakdown.columns:
        sorted_df = breakdown.sort_values("total_vaep", key=abs, ascending=True)
        colors = ["#2a9d8f" if v >= 0 else "#e63946" for v in sorted_df["total_vaep"]]

        ax.barh(sorted_df["action_type"], sorted_df["total_vaep"], color=colors, alpha=0.85)
        ax.axvline(x=0, color="#555577", linestyle="-", linewidth=0.5)

    ax.set_xlabel("Total VAEP (0-1 scale, higher = more impactful)", color=TEXT_COLOR, fontsize=11)
    ax.set_title("VAEP by Action Type", color=TEXT_COLOR, fontsize=14, pad=10, fontweight="bold")
    ax.tick_params(axis="both", colors=TEXT_COLOR, labelcolor=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()

    return chart_to_file(fig, "av_breakdown")


def _render_timeline_chart(actions: pd.DataFrame) -> str:
    """Render VAEP scatter timeline with diverging colors. Returns file path."""
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    if not actions.empty and "time_seconds" in actions.columns and "vaep_value" in actions.columns:
        minutes = actions["time_seconds"] / 60.0
        values = actions["vaep_value"]

        # Diverging colormap: blue (negative) -> white -> orange (positive)
        cmap = mcolors.LinearSegmentedColormap.from_list("vaep", ["#457b9d", "#ffffff", "#e76f51"])
        v_abs_max = float(max(abs(values.min()), abs(values.max()), 0.01))
        norm = mcolors.TwoSlopeNorm(vmin=-v_abs_max, vcenter=0, vmax=v_abs_max)

        # Marker differentiation: triangle-up for positive, triangle-down for negative
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

    ax.set_xlabel("Match Minute", color=TEXT_COLOR, fontsize=11)
    ax.set_ylabel("VAEP Value (positive = good, negative = bad)", color=TEXT_COLOR, fontsize=11)
    ax.set_title("Match Action Value Timeline", color=TEXT_COLOR, fontsize=14, pad=10, fontweight="bold")
    ax.tick_params(axis="both", colors=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()

    return chart_to_file(fig, "av_timeline")


# ---------------------------------------------------------------------------
# Rankings formatter
# ---------------------------------------------------------------------------


def _format_rankings_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format rankings DataFrame for Taipy <|table|> display.

    Returns a renamed DataFrame with human-readable column names.
    Includes Pctile column when percentile data is available.
    """
    empty_cols = [
        "Player",
        "Pos",
        "Min",
        "Total VAEP",
        "VAEP per 90",
        "Pctile",
        "Off per 90",
        "Def per 90",
        "Actions",
    ]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    # Taipy table rendering breaks on column names with "/" characters.
    display = df.rename(
        columns={
            "player_display_name": "Player",
            "position_group": "Pos",
            "minutes_played": "Min",
            "total_vaep": "Total VAEP",
            "vaep_per_90": "VAEP per 90",
            "offensive_vaep_per_90": "Off per 90",
            "defensive_vaep_per_90": "Def per 90",
            "total_actions": "Actions",
        }
    ).drop(columns=["player_id"], errors="ignore")

    # Format percentile column: 0.85 -> "85th", 0.51 -> "51st", etc.
    def _fmt_pctile(v: float | None) -> str:
        if pd.isna(v):
            return "--"
        n = int(v * 100)
        suffix = "th" if 11 <= n % 100 <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"

    if "vaep_per_90_pctile" in display.columns:
        display["Pctile"] = display["vaep_per_90_pctile"].apply(_fmt_pctile)
        display = display.drop(columns=["vaep_per_90_pctile"])
    else:
        display["Pctile"] = "--"

    # Round numeric columns for display
    for col in ["Total VAEP", "VAEP per 90", "Off per 90", "Def per 90"]:
        if col in display.columns:
            display[col] = display[col].round(3)

    # Reorder columns to place Pctile after VAEP per 90
    desired_order = [c for c in empty_cols if c in display.columns]
    remaining = [c for c in display.columns if c not in desired_order]
    display = display[desired_order + remaining]

    return display


# ---------------------------------------------------------------------------
# Sub-view refresh functions
# ---------------------------------------------------------------------------


def _refresh_rankings(state: Any) -> None:
    """Refresh the Rankings sub-view."""
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        state.av_rankings_data = pd.DataFrame(columns=_AV_RANKINGS_COLS)
        state.av_rankings_empty_msg = "Select a competition to see VAEP rankings."
        state.av_scope_label = ""
        state.av_warning_text = ""
        return

    team_id = get_team_id(state.selected_team)
    state.av_scope_label = fetch_scope_label(comp_id, team_id)

    min_min = int(state.min_minutes) if hasattr(state, "min_minutes") else 90

    try:
        rankings = fetch_vaep_rankings(comp_id, min_min)
    except Exception:
        logger.exception("Failed to fetch VAEP rankings")
        state.av_rankings_data = pd.DataFrame(columns=_AV_RANKINGS_COLS)
        state.av_rankings_empty_msg = "Something went wrong loading rankings. Try refreshing the page."
        state.av_warning_text = "Something went wrong loading VAEP rankings. Try refreshing the page."
        return

    table = _format_rankings_table(rankings)
    state.av_rankings_data = table
    state.av_rankings_empty_msg = (
        ""
        if not table.empty
        else "No VAEP data for this filter combination. Try selecting a different competition or removing player filters."
    )
    state.av_warning_text = (
        ""
        if not table.empty
        else "No VAEP data for this filter combination. Try selecting a different competition or removing player filters."
    )


def _refresh_breakdown(state: Any) -> None:
    """Refresh the Breakdown sub-view."""
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        state.av_total_vaep = "--"
        state.av_total_actions = "--"
        state.av_top_action = "--"
        state.av_breakdown_image = ""
        state.av_scope_label = ""
        state.av_warning_text = ""
        return

    team_id = get_team_id(state.selected_team)
    player_id = get_player_id(state.selected_player)
    state.av_scope_label = fetch_scope_label(comp_id, team_id)

    try:
        breakdown = fetch_vaep_breakdown(comp_id, team_id, player_id)
    except Exception:
        logger.exception("Failed to fetch action breakdown")
        state.av_total_vaep = "\u2013"
        state.av_total_actions = "\u2013"
        state.av_top_action = "\u2013"
        state.av_breakdown_image = ""
        state.av_warning_text = "Something went wrong loading the breakdown. Try refreshing the page."
        return

    if breakdown.empty:
        state.av_total_vaep = "0.00"
        state.av_total_actions = "0"
        state.av_top_action = "N/A"
        state.av_breakdown_image = ""
        state.av_warning_text = (
            "No VAEP data for this filter combination. Try selecting a different competition or match."
        )
        return

    state.av_warning_text = ""

    # Metrics
    total_vaep = float(breakdown["total_vaep"].sum())
    total_actions = int(breakdown["action_count"].sum())
    top_type = str(breakdown.iloc[0]["action_type"]) if not breakdown.empty else "N/A"

    state.av_total_vaep = f"{total_vaep:.2f}"
    state.av_total_actions = fmt_int(total_actions)
    state.av_top_action = top_type

    # Chart
    state.av_breakdown_image = _render_breakdown_chart(breakdown)
    logger.info("Breakdown: total_vaep=%.2f, %d actions, top=%s", total_vaep, total_actions, top_type)


def _refresh_timeline(state: Any) -> None:
    """Refresh the Timeline sub-view."""
    match_id = get_match_id(state.selected_match)
    if match_id is None:
        state.av_positive = "--"
        state.av_negative = "--"
        state.av_net_vaep = "--"
        state.av_most_valuable = "--"
        state.av_timeline_image = ""
        state.av_timeline_data = pd.DataFrame(
            columns=["Action", "Minute", "Second", "Period", "Result", "VAEP Value", "Offensive", "Defensive"]
        )
        state.av_scope_label = ""
        state.av_warning_text = ""
        return

    comp_id = get_comp_id(state.selected_competition)
    team_id = get_team_id(state.selected_team)
    if comp_id is not None:
        state.av_scope_label = fetch_scope_label(comp_id, team_id)

    try:
        actions = fetch_vaep_timeline(match_id, team_id)
    except Exception:
        logger.exception("Failed to fetch match timeline")
        state.av_positive = "\u2013"
        state.av_negative = "\u2013"
        state.av_net_vaep = "\u2013"
        state.av_most_valuable = "\u2013"
        state.av_timeline_image = ""
        state.av_timeline_data = pd.DataFrame(
            columns=["Action", "Minute", "Second", "Period", "Result", "VAEP Value", "Offensive", "Defensive"]
        )
        state.av_warning_text = "Something went wrong loading the timeline. Try refreshing the page."
        return

    if actions.empty:
        state.av_positive = "0"
        state.av_negative = "0"
        state.av_net_vaep = "0.000"
        state.av_most_valuable = "N/A"
        state.av_timeline_image = ""
        state.av_timeline_data = pd.DataFrame(
            columns=["Action", "Minute", "Second", "Period", "Result", "VAEP Value", "Offensive", "Defensive"]
        )
        state.av_warning_text = "No VAEP data for this match. Try selecting a different match."
        return

    state.av_warning_text = ""

    # Metrics
    positive = int((actions["vaep_value"] > 0).sum())
    negative = int((actions["vaep_value"] < 0).sum())
    net_vaep = float(actions["vaep_value"].sum())

    state.av_positive = fmt_int(positive)
    state.av_negative = fmt_int(negative)
    state.av_net_vaep = f"{net_vaep:.3f}"

    # Most valuable action
    if not actions.empty:
        best_idx = actions["vaep_value"].idxmax()
        best = actions.loc[best_idx]
        state.av_most_valuable = f"{best['action_type']} ({best['vaep_value']:.3f})"
    else:
        state.av_most_valuable = "N/A"

    # Chart
    state.av_timeline_image = _render_timeline_chart(actions)

    # Action Details table — display-ready copy
    display = (
        actions.drop(columns=["player_id", "time_seconds"], errors="ignore")
        .rename(
            columns={
                "action_type": "Action",
                "minute": "Minute",
                "second": "Second",
                "period": "Period",
                "action_result": "Result",
                "vaep_value": "VAEP Value",
                "offensive_value": "Offensive",
                "defensive_value": "Defensive",
            }
        )
        .head(200)
    )
    for col in ["VAEP Value", "Offensive", "Defensive"]:
        if col in display.columns:
            display[col] = display[col].round(4)
    state.av_timeline_data = display

    logger.info("Timeline: +%s / -%s, net=%.3f", positive, negative, net_vaep)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def av_refresh(state: Any) -> None:
    """Dispatch to the correct sub-view refresh based on selected_sub_view."""
    # Ensure sub-view LOV is populated on page navigate
    if not getattr(state, "sub_view_lov", None) or state.sub_view_lov != AV_SUB_VIEW_LOV:
        state.sub_view_lov = AV_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in AV_SUB_VIEW_LOV:
        state.selected_sub_view = AV_SUB_VIEW_LOV[0]

    view = state.selected_sub_view

    if view == "Rankings":
        _refresh_rankings(state)
    elif view == "Breakdown":
        _refresh_breakdown(state)
    elif view == "Timeline":
        _refresh_timeline(state)
    else:
        logger.warning("Unknown action values sub-view: %r", view)

    state.av_data_freshness = fetch_data_freshness()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Player-Impact", av_refresh)
