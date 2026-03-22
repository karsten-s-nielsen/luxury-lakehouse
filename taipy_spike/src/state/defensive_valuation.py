"""Defensive Impact (DEFCON) state module — rankings, breakdown, timeline.

All variables prefixed with dv_. Uses page-specific filter dropdowns
(independent of shared filters) since DEFCON data is limited to competitions
with StatsBomb 360 freeze-frame data.

Tier 3 (tabular heuristic, no GNN) approximation of the DEFCON framework.
Credits: Intercept, Concede, Disturb, Deter.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from cache import ttl_cache
from db import execute_query, t
from filters import fetch_data_freshness
from render import PITCH_BG_COLOR, TEXT_COLOR, chart_to_file

from state.shared import register_page_refresher

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page-specific filter state (NOT shared — DEFCON has its own comp/team lists)
# ---------------------------------------------------------------------------
dv_selected_comp: str | None = None
dv_selected_team: str | None = None
dv_current_view: str = "Rankings"

dv_comp_lov: list[str] = []
dv_team_lov: list[str] = []
dv_view_lov: list[str] = ["Rankings", "Breakdown", "Timeline"]

# Rankings view state
_DV_RANKINGS_COLS = [
    "Player",
    "Total Pressure",
    "Actions Faced",
    "Intercepted",
    "Shots Conceded",
    "Disturbed",
    "Deterred",
    "Matches",
]
dv_rankings_data: pd.DataFrame = pd.DataFrame(columns=_DV_RANKINGS_COLS)

# Breakdown view state
dv_breakdown_player_lov: list[str] = []
dv_selected_breakdown_player: str | None = None
dv_intercept: str = "--"
dv_concede: str = "--"
dv_disturb: str = "--"
dv_deter: str = "--"
dv_breakdown_image: str = ""

# Timeline view state
dv_timeline_player_lov: list[str] = []
dv_selected_timeline_player: str | None = None
dv_timeline_match_lov: list[str] = []
dv_selected_timeline_match: str | None = None
_DV_TIMELINE_COLS = [
    "Credit Type",
    "Confidence (0-1)",
    "DEFCON Value",
    "Action",
    "Pitch X (m)",
    "Pitch Y (m)",
    "Dist to Ball (m)",
]
dv_timeline_data: pd.DataFrame = pd.DataFrame(columns=_DV_TIMELINE_COLS)

dv_data_freshness: str = ""

dv_warning_text: str = ""

__all__ = [
    # Filter state
    "dv_data_freshness",
    "dv_selected_comp",
    "dv_selected_team",
    "dv_current_view",
    "dv_comp_lov",
    "dv_team_lov",
    "dv_view_lov",
    # Rankings
    "dv_rankings_data",
    # Breakdown
    "dv_breakdown_player_lov",
    "dv_selected_breakdown_player",
    "dv_intercept",
    "dv_concede",
    "dv_disturb",
    "dv_deter",
    "dv_breakdown_image",
    # Timeline
    "dv_timeline_player_lov",
    "dv_selected_timeline_player",
    "dv_timeline_match_lov",
    "dv_selected_timeline_match",
    "dv_timeline_data",
    # Callbacks
    "dv_on_comp_change",
    "dv_on_team_change",
    "dv_on_view_change",
    "dv_on_breakdown_player_change",
    "dv_on_timeline_player_change",
    "dv_on_timeline_match_change",
    "dv_refresh",
    "dv_warning_text",
]

# ---------------------------------------------------------------------------
# Internal lookup maps (NOT exported — not bound to UI)
# ---------------------------------------------------------------------------
_dv_comp_map: dict[str, int] = {}
_dv_team_map: dict[str, int] = {}
_dv_breakdown_player_map: dict[str, int] = {}
_dv_timeline_player_map: dict[str, int] = {}
_dv_timeline_match_map: dict[str, str] = {}

# Store full rankings DataFrame for player lookups in Breakdown/Timeline
_dv_rankings_full: pd.DataFrame = pd.DataFrame()


# ---------------------------------------------------------------------------
# Data fetching (queries adapted from Streamlit, parameterized, @ttl_cache)
# ---------------------------------------------------------------------------


@ttl_cache()
def _fetch_pressure_competitions() -> pd.DataFrame:
    """Load competitions with DEFCON pressure data.

    Uses recursive CTE loose index scan to avoid SELECT DISTINCT sequential scan.
    """
    dp = t("fct_defcon_pressure_synced")
    dc = t("dim_competitions_synced")
    return execute_query(
        f"WITH RECURSIVE pc AS ("  # noqa: S608
        f"  SELECT MIN(competition_id) AS competition_id FROM {dp}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(competition_id) FROM {dp}"
        f"          WHERE competition_id > pc.competition_id)"
        f"  FROM pc WHERE pc.competition_id IS NOT NULL"
        f") SELECT pc.competition_id, c.competition_name, c.country "
        f"FROM pc "
        f"JOIN {dc} c ON pc.competition_id = c.competition_id "
        f"WHERE pc.competition_id IS NOT NULL "
        f"ORDER BY c.country, c.competition_name",
    )


@ttl_cache()
def _fetch_pressure_teams(comp_id: int) -> pd.DataFrame:
    """Load teams with DEFCON pressure data in a competition.

    Recursive CTE for distinct match_ids, then join to match summary for teams.
    """
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    dim_t = t("dim_teams_synced")
    return execute_query(
        f"WITH RECURSIVE pressure_matches AS ("  # noqa: S608
        f"  SELECT MIN(match_id)::bigint AS match_id FROM {dp} WHERE competition_id = %s"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(match_id)::bigint FROM {dp}"
        f"          WHERE competition_id = %s AND match_id::bigint > pressure_matches.match_id)"
        f"  FROM pressure_matches WHERE pressure_matches.match_id IS NOT NULL"
        f") "
        f"SELECT DISTINCT dt.team_id, dt.team_name "
        f"FROM {dim_t} dt "
        f"JOIN {ms} ms"
        f"  ON ms.home_team_id = dt.team_id OR ms.away_team_id = dt.team_id "
        f"JOIN pressure_matches pm ON pm.match_id = ms.match_id "
        f"ORDER BY dt.team_name",
        (comp_id, comp_id),
    )


@ttl_cache()
def _fetch_pressure_rankings(comp_id: int, team_id: int | None) -> pd.DataFrame:
    """Ranked players by total defensive pressure received.

    With team filter: recursive CTE collects distinct player_ids from action_values
    for the team, then filters pressure rows to those players.
    Without team filter: direct aggregate on the pressure table.
    """
    dp = t("fct_defcon_pressure_synced")
    dim_p = t("dim_players_synced")
    av_tbl = t("fct_action_values_synced")

    if team_id is not None:
        return execute_query(
            f"WITH RECURSIVE team_players AS ("  # noqa: S608
            f"  SELECT MIN(player_id) AS player_id FROM {av_tbl}"
            f"  WHERE competition_id = %s AND team_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(player_id) FROM {av_tbl}"
            f"          WHERE competition_id = %s AND team_id = %s AND player_id > team_players.player_id)"
            f"  FROM team_players WHERE team_players.player_id IS NOT NULL"
            f") "
            f"SELECT dp.player_id, p.player_display_name, "
            f"  SUM(dp.total_pressure) as total_pressure, "
            f"  SUM(dp.total_defensive_actions) as total_actions, "
            f"  SUM(dp.intercept_count) as intercepts, "
            f"  SUM(dp.concede_count) as concedes, "
            f"  SUM(dp.disturb_count) as disturbs, "
            f"  SUM(dp.deter_count) as deters, "
            f"  COUNT(DISTINCT dp.match_id) as matches "
            f"FROM {dp} dp "
            f"JOIN {dim_p} p ON dp.player_id = p.player_id "
            f"JOIN team_players tp ON tp.player_id = dp.player_id "
            f"WHERE dp.competition_id = %s "
            f"GROUP BY dp.player_id, p.player_display_name "
            f"ORDER BY total_pressure DESC "
            f"LIMIT 50",
            (comp_id, team_id, comp_id, team_id, comp_id),
        )

    return execute_query(
        f"SELECT dp.player_id, p.player_display_name, "  # noqa: S608
        f"  SUM(dp.total_pressure) as total_pressure, "
        f"  SUM(dp.total_defensive_actions) as total_actions, "
        f"  SUM(dp.intercept_count) as intercepts, "
        f"  SUM(dp.concede_count) as concedes, "
        f"  SUM(dp.disturb_count) as disturbs, "
        f"  SUM(dp.deter_count) as deters, "
        f"  COUNT(DISTINCT dp.match_id) as matches "
        f"FROM {dp} dp "
        f"JOIN {dim_p} p ON dp.player_id = p.player_id "
        f"WHERE dp.competition_id = %s "
        f"GROUP BY dp.player_id, p.player_display_name "
        f"ORDER BY total_pressure DESC "
        f"LIMIT 50",
        (comp_id,),
    )


@ttl_cache()
def _fetch_pressure_breakdown(pid: int, comp_id: int, team_id: int | None) -> pd.DataFrame:
    """Per-match pressure breakdown for a specific attacker."""
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    conditions = ["dp.player_id = %s", "dp.competition_id = %s"]
    params: list[Any] = [pid, comp_id]

    if team_id is not None:
        conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
        params.extend([team_id, team_id])

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT dp.match_id, "  # noqa: S608
        f"  ms.home_team_name || ' v ' || ms.away_team_name as match_label, "
        f"  dp.intercept_pressure, dp.concede_pressure, "
        f"  dp.disturb_pressure, dp.deter_pressure, "
        f"  dp.total_pressure, dp.total_defensive_actions "
        f"FROM {dp} dp "
        f"LEFT JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
        f"WHERE {where} "
        f"ORDER BY dp.match_id "
        f"LIMIT 200",
        tuple(params),
    )


@ttl_cache()
def _fetch_player_defcon_matches(pid: int, comp_id: int, team_id: int | None) -> pd.DataFrame:
    """Matches where an attacker has DEFCON pressure data (for match dropdown)."""
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    conditions = ["dp.player_id = %s", "dp.competition_id = %s"]
    params: list[Any] = [pid, comp_id]

    if team_id is not None:
        conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
        params.extend([team_id, team_id])

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT dp.match_id, "  # noqa: S608
        f"  MAX(ms.match_date) as match_date, "
        f"  MAX(ms.home_team_name) as home_team_name, "
        f"  MAX(ms.away_team_name) as away_team_name, "
        f"  MAX(ms.home_score) as home_score, "
        f"  MAX(ms.away_score) as away_score "
        f"FROM {dp} dp "
        f"LEFT JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
        f"WHERE {where} "
        f"GROUP BY dp.match_id "
        f"ORDER BY MAX(ms.match_date) DESC "
        f"LIMIT 200",
        tuple(params),
    )


@ttl_cache()
def _fetch_match_timeline(match_id: str, pid: int) -> pd.DataFrame:
    """Per-action DEFCON credits for a player in a specific match."""
    da = t("fct_defcon_actions_synced")
    return execute_query(
        f"SELECT da.event_id, da.player_id as opposing_player_id, "  # noqa: S608
        f"  da.credit_type, da.confidence, da.defcon_value, "
        f"  da.action_type, da.action_x, da.action_y, "
        f"  da.dist_to_ball "
        f"FROM {da} da "
        f"WHERE da.match_id = %s AND da.action_player_id = %s "
        f"ORDER BY da.event_id "
        f"LIMIT 2000",
        (match_id, pid),
    )


@ttl_cache()
def _fetch_breakdown_player_ids(comp_id: int, team_id: int | None) -> set[int]:
    """Player IDs that have pressure breakdown rows for the given filters."""
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    if team_id is not None:
        result = execute_query(
            f"SELECT dp.player_id "  # noqa: S608
            f"FROM {dp} dp "
            f"JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
            f"WHERE dp.competition_id = %s "
            f"AND (ms.home_team_id = %s OR ms.away_team_id = %s) "
            f"GROUP BY dp.player_id",
            (comp_id, team_id, team_id),
        )
    else:
        result = execute_query(
            f"WITH RECURSIVE dp_players AS ("  # noqa: S608
            f"  SELECT MIN(player_id) AS player_id FROM {dp} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(player_id) FROM {dp}"
            f"          WHERE competition_id = %s AND player_id > dp_players.player_id)"
            f"  FROM dp_players WHERE dp_players.player_id IS NOT NULL"
            f") SELECT player_id FROM dp_players WHERE player_id IS NOT NULL",
            (comp_id, comp_id),
        )

    if result.empty:
        return set()
    return {int(x) for x in result["player_id"]}


@ttl_cache()
def _fetch_timeline_player_ids(comp_id: int, team_id: int | None) -> set[int]:
    """action_player_ids that have DEFCON action rows for the given filters."""
    da = t("fct_defcon_actions_synced")
    ms = t("fct_match_summary_synced")

    if team_id is not None:
        result = execute_query(
            f"SELECT da.action_player_id as player_id "  # noqa: S608
            f"FROM {da} da "
            f"JOIN {ms} ms ON da.match_id::bigint = ms.match_id "
            f"WHERE da.competition_id = %s "
            f"AND (ms.home_team_id = %s OR ms.away_team_id = %s) "
            f"GROUP BY da.action_player_id",
            (comp_id, team_id, team_id),
        )
    else:
        result = execute_query(
            f"WITH RECURSIVE da_players AS ("  # noqa: S608
            f"  SELECT MIN(action_player_id) AS player_id FROM {da} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(action_player_id) FROM {da}"
            f"          WHERE competition_id = %s AND action_player_id > da_players.player_id)"
            f"  FROM da_players WHERE da_players.player_id IS NOT NULL"
            f") SELECT player_id FROM da_players WHERE player_id IS NOT NULL",
            (comp_id, comp_id),
        )

    if result.empty:
        return set()
    return {int(x) for x in result["player_id"]}


# ---------------------------------------------------------------------------
# Chart rendering (Plotly → matplotlib conversion)
# ---------------------------------------------------------------------------

# Credit type color mapping (consistent across views)
_CREDIT_COLORS = {
    "intercept_pressure": "#2a9d8f",
    "concede_pressure": "#e63946",
    "disturb_pressure": "#457b9d",
    "deter_pressure": "#f4a261",
}
_CREDIT_LABELS = {
    "intercept_pressure": "Intercept",
    "concede_pressure": "Concede",
    "disturb_pressure": "Disturb",
    "deter_pressure": "Deter",
}


def _render_breakdown_chart(breakdown: pd.DataFrame, player_name: str) -> str:
    """Render grouped horizontal bar chart of pressure breakdown per match.

    Mirrors the Streamlit Plotly grouped bar chart: one group of 4 bars per match,
    credit types color-coded. Limits to 10 matches to prevent bar slivers.
    Returns file path for Taipy <|image|>.
    """
    if breakdown.empty:
        return ""

    label_col = "match_label" if breakdown["match_label"].notna().all() else "match_id"
    plot_data = breakdown.head(10) if len(breakdown) > 10 else breakdown

    credit_cols = ["intercept_pressure", "concede_pressure", "disturb_pressure", "deter_pressure"]
    n_matches = len(plot_data)
    n_credits = len(credit_cols)
    bar_height = 0.18
    group_gap = 0.15

    fig_height = max(4, n_matches * (n_credits * bar_height + group_gap) + 1.5)
    fig, ax = plt.subplots(figsize=(12, fig_height))
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    y_positions = np.arange(n_matches)
    offsets = np.linspace(
        -(n_credits - 1) * bar_height / 2,
        (n_credits - 1) * bar_height / 2,
        n_credits,
    )

    for i, col in enumerate(credit_cols):
        values = plot_data[col].fillna(0).values
        ax.barh(
            y_positions + offsets[i],
            values,
            height=bar_height,
            color=_CREDIT_COLORS[col],
            alpha=0.85,
            label=_CREDIT_LABELS[col],
        )

    ax.set_yticks(y_positions)
    ax.set_yticklabels(plot_data[label_col].astype(str).tolist(), fontsize=9)
    ax.invert_yaxis()

    ax.set_xlabel("Pressure Value (0-5 scale, higher = more defensive attention)", color=TEXT_COLOR, fontsize=11)
    title = f"Pressure Breakdown: {player_name}"
    if len(breakdown) > 10:
        title += f" (top 10 of {len(breakdown)} matches)"
    ax.set_title(title, color=TEXT_COLOR, fontsize=14, pad=10, fontweight="bold")

    ax.tick_params(axis="both", colors=TEXT_COLOR, labelcolor=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")

    ax.legend(
        loc="lower right",
        fontsize=9,
        facecolor=PITCH_BG_COLOR,
        edgecolor="#333355",
        labelcolor=TEXT_COLOR,
    )
    plt.tight_layout()

    return chart_to_file(fig, "dv_breakdown")


# ---------------------------------------------------------------------------
# Table formatters
# ---------------------------------------------------------------------------


def _format_rankings_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format rankings for Taipy <|table|>. Drops player_id, renames columns."""
    if df.empty:
        return pd.DataFrame(columns=_DV_RANKINGS_COLS)

    rename_map = {
        "player_display_name": "Player",
        "total_pressure": "Total Pressure",
        "total_actions": "Actions Faced",
        "intercepts": "Intercepted",
        "concedes": "Shots Conceded",
        "disturbs": "Disturbed",
        "deters": "Deterred",
        "matches": "Matches",
    }
    display = df.drop(columns=["player_id"], errors="ignore").rename(columns=rename_map)

    # Convert to numeric (SUM returns object/Decimal via psycopg2), fill NaN, round
    numeric_cols = ["Total Pressure", "Actions Faced", "Intercepted", "Shots Conceded", "Disturbed", "Deterred"]
    for col in numeric_cols:
        if col in display.columns:
            display[col] = pd.to_numeric(display[col], errors="coerce").fillna(0).round(2)

    return display


def _format_timeline_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format timeline for Taipy <|table|>. Drops internal IDs, renames columns."""
    if df.empty:
        return pd.DataFrame(columns=_DV_TIMELINE_COLS)

    display_cols = [c for c in df.columns if c not in ("opposing_player_id", "event_id")]
    rename_map = {
        "credit_type": "Credit Type",
        "confidence": "Confidence (0-1)",
        "defcon_value": "DEFCON Value",
        "action_type": "Action",
        "action_x": "Pitch X (m)",
        "action_y": "Pitch Y (m)",
        "dist_to_ball": "Dist to Ball (m)",
    }
    display = df[display_cols].rename(columns=rename_map)

    for col in ["Confidence (0-1)", "DEFCON Value", "Dist to Ball (m)"]:
        if col in display.columns:
            display[col] = display[col].round(3)

    return display


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_dv_comp_id(label: str | None) -> int | None:
    """Resolve DEFCON competition label to ID."""
    return _dv_comp_map.get(label) if label else None  # type: ignore[arg-type]


def _get_dv_team_id(label: str | None) -> int | None:
    """Resolve DEFCON team label to ID. Returns None for 'All teams' or empty."""
    if not label or label == "All teams":
        return None
    return _dv_team_map.get(label)  # type: ignore[arg-type]


def _build_player_options(rankings: pd.DataFrame) -> dict[str, int]:
    """Build player name -> id mapping from rankings DataFrame."""
    if rankings.empty:
        return {}
    return {str(row["player_display_name"]): int(row["player_id"]) for _, row in rankings.iterrows()}


def _reset_breakdown(state: Any) -> None:
    """Reset breakdown view to default state."""
    state.dv_selected_breakdown_player = None
    state.dv_breakdown_player_lov = []
    state.dv_intercept = "--"
    state.dv_concede = "--"
    state.dv_disturb = "--"
    state.dv_deter = "--"
    state.dv_breakdown_image = ""


def _reset_timeline(state: Any) -> None:
    """Reset timeline view to default state."""
    state.dv_selected_timeline_player = None
    state.dv_timeline_player_lov = []
    state.dv_timeline_match_lov = []
    state.dv_selected_timeline_match = None
    state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)


# ---------------------------------------------------------------------------
# Sub-view refresh functions
# ---------------------------------------------------------------------------


def _refresh_rankings(state: Any) -> None:
    """Refresh the Rankings sub-view."""
    global _dv_rankings_full
    comp_id = _get_dv_comp_id(state.dv_selected_comp)
    if comp_id is None:
        state.dv_rankings_data = pd.DataFrame(columns=_DV_RANKINGS_COLS)
        _dv_rankings_full = pd.DataFrame()
        state.dv_warning_text = ""
        return

    team_id = _get_dv_team_id(state.dv_selected_team)

    try:
        rankings = _fetch_pressure_rankings(comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch DEFCON rankings")
        state.dv_rankings_data = pd.DataFrame(columns=_DV_RANKINGS_COLS)
        _dv_rankings_full = pd.DataFrame()
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    if rankings.empty:
        state.dv_warning_text = "No defensive pressure data for the selected filters."
    else:
        state.dv_warning_text = ""

    _dv_rankings_full = rankings
    state.dv_rankings_data = _format_rankings_table(rankings)
    logger.info("Rankings: %d players loaded", len(rankings))


def _refresh_breakdown(state: Any) -> None:
    """Refresh the Breakdown sub-view — populate player LOV from rankings."""
    global _dv_breakdown_player_map

    comp_id = _get_dv_comp_id(state.dv_selected_comp)
    if comp_id is None:
        _reset_breakdown(state)
        state.dv_warning_text = ""
        return

    team_id = _get_dv_team_id(state.dv_selected_team)

    # Ensure rankings are loaded for player options
    if _dv_rankings_full.empty:
        _refresh_rankings(state)

    if _dv_rankings_full.empty:
        _reset_breakdown(state)
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    # Filter to players who have breakdown data
    try:
        bd_pids = _fetch_breakdown_player_ids(comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch breakdown player IDs")
        _reset_breakdown(state)
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    player_options = _build_player_options(_dv_rankings_full)
    filtered = {k: v for k, v in player_options.items() if v in bd_pids}

    if not filtered:
        _reset_breakdown(state)
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    _dv_breakdown_player_map = filtered
    state.dv_breakdown_player_lov = list(filtered.keys())

    # Auto-select first player if none selected or selection is invalid
    if state.dv_selected_breakdown_player not in filtered:
        state.dv_selected_breakdown_player = state.dv_breakdown_player_lov[0]

    # Load breakdown for selected player
    _load_breakdown_for_player(state)
    state.dv_warning_text = ""


def _load_breakdown_for_player(state: Any) -> None:
    """Load breakdown data and chart for the currently selected player."""
    comp_id = _get_dv_comp_id(state.dv_selected_comp)
    team_id = _get_dv_team_id(state.dv_selected_team)
    player_name = state.dv_selected_breakdown_player

    if comp_id is None or player_name is None:
        return

    player_id = _dv_breakdown_player_map.get(player_name)
    if player_id is None:
        return

    try:
        breakdown = _fetch_pressure_breakdown(player_id, comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch pressure breakdown for player %d", player_id)
        state.dv_intercept = "Error"
        state.dv_concede = "Error"
        state.dv_disturb = "Error"
        state.dv_deter = "Error"
        state.dv_breakdown_image = ""
        return

    if breakdown.empty:
        state.dv_intercept = "0.00"
        state.dv_concede = "0.00"
        state.dv_disturb = "0.00"
        state.dv_deter = "0.00"
        state.dv_breakdown_image = ""
        return

    # Compute summary metrics
    state.dv_intercept = f"{breakdown['intercept_pressure'].sum():.2f}"
    state.dv_concede = f"{breakdown['concede_pressure'].sum():.2f}"
    state.dv_disturb = f"{breakdown['disturb_pressure'].sum():.2f}"
    state.dv_deter = f"{breakdown['deter_pressure'].sum():.2f}"

    # Render chart
    state.dv_breakdown_image = _render_breakdown_chart(breakdown, player_name)
    logger.info(
        "Breakdown for %s: intercept=%s, concede=%s, disturb=%s, deter=%s",
        player_name,
        state.dv_intercept,
        state.dv_concede,
        state.dv_disturb,
        state.dv_deter,
    )


def _refresh_timeline(state: Any) -> None:
    """Refresh the Timeline sub-view — populate player LOV from rankings."""
    global _dv_timeline_player_map

    comp_id = _get_dv_comp_id(state.dv_selected_comp)
    if comp_id is None:
        _reset_timeline(state)
        state.dv_warning_text = ""
        return

    team_id = _get_dv_team_id(state.dv_selected_team)

    # Ensure rankings are loaded for player options
    if _dv_rankings_full.empty:
        _refresh_rankings(state)

    if _dv_rankings_full.empty:
        _reset_timeline(state)
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    # Filter to players who have timeline data
    try:
        tl_pids = _fetch_timeline_player_ids(comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch timeline player IDs")
        _reset_timeline(state)
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    player_options = _build_player_options(_dv_rankings_full)
    filtered = {k: v for k, v in player_options.items() if v in tl_pids}

    if not filtered:
        _reset_timeline(state)
        state.dv_warning_text = "No defensive pressure data for the selected filters."
        return

    _dv_timeline_player_map = filtered
    state.dv_timeline_player_lov = list(filtered.keys())

    # Auto-select first player if none selected or selection is invalid
    if state.dv_selected_timeline_player not in filtered:
        state.dv_selected_timeline_player = state.dv_timeline_player_lov[0]

    # Load matches for selected player
    _load_timeline_matches(state)
    state.dv_warning_text = ""


def _load_timeline_matches(state: Any) -> None:
    """Load match LOV for the currently selected timeline player."""
    global _dv_timeline_match_map

    comp_id = _get_dv_comp_id(state.dv_selected_comp)
    team_id = _get_dv_team_id(state.dv_selected_team)
    player_name = state.dv_selected_timeline_player

    if comp_id is None or player_name is None:
        state.dv_timeline_match_lov = []
        state.dv_selected_timeline_match = None
        state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)
        return

    player_id = _dv_timeline_player_map.get(player_name)
    if player_id is None:
        return

    try:
        matches = _fetch_player_defcon_matches(player_id, comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch DEFCON matches for player %d", player_id)
        state.dv_timeline_match_lov = []
        state.dv_selected_timeline_match = None
        state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)
        return

    if matches.empty:
        state.dv_timeline_match_lov = []
        state.dv_selected_timeline_match = None
        state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)
        return

    # Build match labels: "date — Home Score-Score Away" or match_id fallback
    match_records = matches.to_dict("records")
    labels: list[str] = []
    match_map: dict[str, str] = {}

    for r in match_records:
        if r.get("match_date") is not None:
            label = (
                f"{r['match_date']} \u2014 {r['home_team_name']}"
                f" {int(r.get('home_score', 0) or 0)}-{int(r.get('away_score', 0) or 0)}"
                f" {r['away_team_name']}"
            )
        else:
            label = str(r["match_id"])
        labels.append(label)
        match_map[label] = str(r["match_id"])

    _dv_timeline_match_map = match_map
    state.dv_timeline_match_lov = labels

    # Auto-select first match if none selected or selection is invalid
    if state.dv_selected_timeline_match not in match_map:
        state.dv_selected_timeline_match = labels[0] if labels else None

    # Load timeline for selected match
    _load_timeline_data(state)


def _load_timeline_data(state: Any) -> None:
    """Load action timeline for the currently selected match + player."""
    match_label = state.dv_selected_timeline_match
    player_name = state.dv_selected_timeline_player

    if not match_label or not player_name:
        state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)
        return

    match_id = _dv_timeline_match_map.get(match_label)
    player_id = _dv_timeline_player_map.get(player_name)

    if match_id is None or player_id is None:
        state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)
        return

    try:
        timeline = _fetch_match_timeline(match_id, player_id)
    except Exception:
        logger.exception("Failed to fetch DEFCON timeline for match=%s player=%d", match_id, player_id)
        state.dv_timeline_data = pd.DataFrame(columns=_DV_TIMELINE_COLS)
        return

    state.dv_timeline_data = _format_timeline_table(timeline)
    logger.info("Timeline: %d actions loaded for match=%s player=%s", len(timeline), match_id, player_name)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def dv_on_comp_change(state: Any, var_name: str, var_value: Any) -> None:
    """Competition changed — reload teams, rankings, reset dependent state."""
    global _dv_team_map

    comp_id = _get_dv_comp_id(var_value)
    if comp_id is None:
        state.dv_team_lov = []
        state.dv_selected_team = None
        state.dv_rankings_data = pd.DataFrame(columns=_DV_RANKINGS_COLS)
        _reset_breakdown(state)
        _reset_timeline(state)
        return

    # Load teams for this competition
    try:
        teams = _fetch_pressure_teams(comp_id)
        if not teams.empty:
            _dv_team_map = {str(r["team_name"]): int(r["team_id"]) for _, r in teams.iterrows()}
            state.dv_team_lov = ["All teams"] + list(_dv_team_map.keys())
        else:
            _dv_team_map = {}
            state.dv_team_lov = []
    except Exception:
        logger.exception("Failed to fetch DEFCON teams for competition %d", comp_id)
        _dv_team_map = {}
        state.dv_team_lov = []

    state.dv_selected_team = "All teams" if state.dv_team_lov else None

    # Refresh the active view
    _dispatch_view_refresh(state)


def dv_on_team_change(state: Any, var_name: str, var_value: Any) -> None:
    """Team changed — refresh the active view."""
    _dispatch_view_refresh(state)


def dv_on_view_change(state: Any, var_name: str, var_value: Any) -> None:
    """View selector changed — refresh the newly selected view."""
    _dispatch_view_refresh(state)


def dv_on_breakdown_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Breakdown player changed — reload breakdown data and chart."""
    _load_breakdown_for_player(state)


def dv_on_timeline_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Timeline player changed — reload matches for this player."""
    _load_timeline_matches(state)


def dv_on_timeline_match_change(state: Any, var_name: str, var_value: Any) -> None:
    """Timeline match changed — reload action timeline."""
    _load_timeline_data(state)


# ---------------------------------------------------------------------------
# Dispatch + entry point
# ---------------------------------------------------------------------------


def _dispatch_view_refresh(state: Any) -> None:
    """Refresh the currently active DEFCON sub-view."""
    view = state.dv_current_view

    if view == "Rankings":
        _refresh_rankings(state)
    elif view == "Breakdown":
        _refresh_breakdown(state)
    elif view == "Timeline":
        _refresh_timeline(state)
    else:
        logger.warning("Unknown DEFCON view: %r", view)


def dv_refresh(state: Any) -> None:
    """Entry point called by register_page_refresher on page navigate.

    Loads DEFCON competitions and refreshes the active view.
    """
    global _dv_comp_map

    # Load competitions with DEFCON data
    try:
        comps = _fetch_pressure_competitions()
        if not comps.empty:
            _dv_comp_map = {
                f"{r.get('country', '')} \u2014 {r['competition_name']}": int(r["competition_id"])
                for _, r in comps.iterrows()
            }
            state.dv_comp_lov = list(_dv_comp_map.keys())
        else:
            _dv_comp_map = {}
            state.dv_comp_lov = []
    except Exception:
        logger.exception("Failed to fetch DEFCON competitions")
        _dv_comp_map = {}
        state.dv_comp_lov = []

    # Auto-select first competition if none selected
    if state.dv_comp_lov and state.dv_selected_comp not in _dv_comp_map:
        state.dv_selected_comp = state.dv_comp_lov[0]
        # Trigger the comp change cascade (loads teams + data)
        dv_on_comp_change(state, "dv_selected_comp", state.dv_selected_comp)
    elif state.dv_selected_comp:
        # Competition already selected — just refresh the active view
        _dispatch_view_refresh(state)

    state.dv_data_freshness = fetch_data_freshness()
    logger.info("Defensive Impact page loaded (%d competitions)", len(state.dv_comp_lov))


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Defensive-Impact", dv_refresh)
