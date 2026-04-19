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

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from filters import NO_MATCHES_SENTINEL, fetch_data_freshness
from queries.defensive import (
    fetch_breakdown_player_ids,
    fetch_defcon_percentiles,
    fetch_match_timeline,
    fetch_player_defcon_matches,
    fetch_pressure_breakdown,
    fetch_pressure_competitions,
    fetch_pressure_rankings,
    fetch_pressure_teams,
    fetch_timeline_player_ids,
)
from render import DEFCON_COLORS

from state.shared import register_page_refresher

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
    "Pctile",
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
# Server-driven autocomplete query for the Breakdown player dropdown.
# Filter is in-memory over _dv_breakdown_player_map (already loaded from
# the rankings frame + breakdown_player_ids cross-reference) — no extra DB
# query because the candidate set is already in the page's working memory.
dv_breakdown_player_search_query: str = ""
dv_intercept: str = "--"
dv_concede: str = "--"
dv_disturb: str = "--"
dv_deter: str = "--"
dv_breakdown_figure: go.Figure | None = None

# Timeline view state
dv_timeline_player_lov: list[str] = []
dv_selected_timeline_player: str | None = None
# Server-driven autocomplete query for the Timeline player dropdown — same
# in-memory pattern as the Breakdown view.
dv_timeline_player_search_query: str = ""
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

dv_breakdown_caption: str = ""

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
    "dv_breakdown_player_search_query",
    "dv_selected_breakdown_player",
    "dv_intercept",
    "dv_concede",
    "dv_disturb",
    "dv_deter",
    "dv_breakdown_caption",
    "dv_breakdown_figure",
    # Timeline
    "dv_timeline_player_lov",
    "dv_timeline_player_search_query",
    "dv_selected_timeline_player",
    "dv_timeline_match_lov",
    "dv_selected_timeline_match",
    "dv_timeline_data",
    # Callbacks
    "dv_on_comp_change",
    "dv_on_team_change",
    "dv_on_view_change",
    "dv_on_breakdown_player_change",
    "dv_on_breakdown_player_search_change",
    "dv_on_timeline_player_change",
    "dv_on_timeline_player_search_change",
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
# Chart rendering (Plotly → matplotlib conversion)
# ---------------------------------------------------------------------------

# Credit type color mapping — derived from canonical render.DEFCON_COLORS (Kirk audit K-1)
_CREDIT_COLORS = {
    "intercept_pressure": DEFCON_COLORS["Intercept"],
    "concede_pressure": DEFCON_COLORS["Concede"],
    "disturb_pressure": DEFCON_COLORS["Disturb"],
    "deter_pressure": DEFCON_COLORS["Deter"],
}
_CREDIT_LABELS = {
    "intercept_pressure": "Intercept",
    "concede_pressure": "Concede",
    "disturb_pressure": "Disturb",
    "deter_pressure": "Deter",
}


def _build_breakdown_figure(breakdown: pd.DataFrame, player_name: str) -> go.Figure | None:
    """Build interactive Plotly grouped bar figure for pressure breakdown."""
    if breakdown.empty:
        return None
    credit_cols = ["intercept_pressure", "concede_pressure", "disturb_pressure", "deter_pressure"]
    labels = ["Intercept", "Concede", "Disturb", "Deter"]
    # Guard against null match_label
    label_col = "match_label" if breakdown["match_label"].notna().all() else "match_id"
    plot_data = breakdown.head(10).melt(
        id_vars=[label_col],
        value_vars=credit_cols,
        var_name="Credit Type",
        value_name="Pressure",
    )
    plot_data["Credit Type"] = plot_data["Credit Type"].map(dict(zip(credit_cols, labels, strict=True)))
    title = f"Pressure Breakdown: {player_name}"
    if len(breakdown) > 10:
        title += f" (top 10 of {len(breakdown)} matches)"
    fig = px.bar(
        plot_data,
        x=label_col,
        y="Pressure",
        color="Credit Type",
        pattern_shape="Credit Type",
        barmode="group",
        title=title,
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
        height=450,
        xaxis_title="",
        yaxis_title="Pressure Credits",
    )
    return fig


# ---------------------------------------------------------------------------
# Table formatters
# ---------------------------------------------------------------------------


def _format_rankings_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format rankings for Taipy <|table|>. Drops player_id, renames columns.

    Includes Pctile column when defcon_per_90_pctile data is available.
    """
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

    # Format percentile column: 0.85 -> "85th", 0.51 -> "51st", etc.
    def _fmt_pctile(v: float | None) -> str:
        if pd.isna(v):
            return "--"
        n = int(v * 100)
        suffix = "th" if 11 <= n % 100 <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"

    if "defcon_per_90_pctile" in display.columns:
        display["Pctile"] = display["defcon_per_90_pctile"].apply(_fmt_pctile)
        display = display.drop(columns=["defcon_per_90_pctile"])
    else:
        display["Pctile"] = "--"

    # Convert to numeric (SUM returns object/Decimal via psycopg2), fill NaN, round
    numeric_cols = ["Total Pressure", "Actions Faced", "Intercepted", "Shots Conceded", "Disturbed", "Deterred"]
    for col in numeric_cols:
        if col in display.columns:
            display[col] = pd.to_numeric(display[col], errors="coerce").fillna(0).round(2)

    # Reorder columns to match _DV_RANKINGS_COLS
    desired_order = [c for c in _DV_RANKINGS_COLS if c in display.columns]
    remaining = [c for c in display.columns if c not in desired_order]
    display = display[desired_order + remaining]

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
    state.dv_breakdown_figure = None
    state.dv_breakdown_caption = ""


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
        rankings = fetch_pressure_rankings(comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch DEFCON rankings")
        state.dv_rankings_data = pd.DataFrame(columns=_DV_RANKINGS_COLS)
        _dv_rankings_full = pd.DataFrame()
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
        return

    if rankings.empty:
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
    else:
        state.dv_warning_text = ""

    _dv_rankings_full = rankings

    # Enrich with percentile data (post-fetch lookup, graceful degradation)
    if not rankings.empty and comp_id is not None:
        player_ids = tuple(int(pid) for pid in rankings["player_id"])
        pctile_map = fetch_defcon_percentiles(comp_id, player_ids)
        if pctile_map:
            rankings["defcon_per_90_pctile"] = rankings["player_id"].apply(lambda pid: pctile_map.get(int(pid)))

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
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
        return

    # Filter to players who have breakdown data
    try:
        bd_pids = fetch_breakdown_player_ids(comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch breakdown player IDs")
        _reset_breakdown(state)
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
        return

    player_options = _build_player_options(_dv_rankings_full)
    filtered = {k: v for k, v in player_options.items() if v in bd_pids}

    if not filtered:
        _reset_breakdown(state)
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
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
        breakdown = fetch_pressure_breakdown(player_id, comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch pressure breakdown for player %d", player_id)
        state.dv_intercept = "\u2013"
        state.dv_concede = "\u2013"
        state.dv_disturb = "\u2013"
        state.dv_deter = "\u2013"
        state.dv_breakdown_figure = None
        state.dv_breakdown_caption = ""
        return

    if breakdown.empty:
        state.dv_intercept = "0.00"
        state.dv_concede = "0.00"
        state.dv_disturb = "0.00"
        state.dv_deter = "0.00"
        state.dv_breakdown_figure = None
        state.dv_breakdown_caption = ""
        return

    # Compute summary metrics
    state.dv_intercept = f"{breakdown['intercept_pressure'].sum():.2f}"
    state.dv_concede = f"{breakdown['concede_pressure'].sum():.2f}"
    state.dv_disturb = f"{breakdown['disturb_pressure'].sum():.2f}"
    state.dv_deter = f"{breakdown['deter_pressure'].sum():.2f}"

    # Top-10 caption (set before chart render which trims to head(10))
    if len(breakdown) > 10:
        state.dv_breakdown_caption = f"Showing top 10 of {len(breakdown)} matches."
    else:
        state.dv_breakdown_caption = ""

    # Render chart
    state.dv_breakdown_figure = _build_breakdown_figure(breakdown, player_name)
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
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
        return

    # Filter to players who have timeline data
    try:
        tl_pids = fetch_timeline_player_ids(comp_id, team_id)
    except Exception:
        logger.exception("Failed to fetch timeline player IDs")
        _reset_timeline(state)
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
        return

    player_options = _build_player_options(_dv_rankings_full)
    filtered = {k: v for k, v in player_options.items() if v in tl_pids}

    if not filtered:
        _reset_timeline(state)
        state.dv_warning_text = "No DEFCON data for this filter combination. DEFCON requires tracking data \u2014 try selecting an IDSSE Bundesliga match."
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
        matches = fetch_player_defcon_matches(player_id, comp_id, team_id)
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
        timeline = fetch_match_timeline(match_id, player_id)
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
        teams = fetch_pressure_teams(comp_id)
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


def dv_on_breakdown_player_search_change(state: Any, var_name: str, var_value: Any) -> None:
    """Server-driven autocomplete for the Breakdown player dropdown.

    In-memory substring filter over _dv_breakdown_player_map (already populated
    when the Breakdown view loads). No DB round-trip; the candidate set never
    leaves the server. Empty query restores the full list (top-50 if larger).
    """
    full = list(_dv_breakdown_player_map.keys())
    q = (var_value or "").strip().lower()
    matches = [p for p in full if q in p.lower()][:500] if q else full[:50]
    state.dv_breakdown_player_lov = matches if matches else [NO_MATCHES_SENTINEL]
    logger.info("DV breakdown search: query=%r -> %d results", q, len(matches))


def dv_on_timeline_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Timeline player changed — reload matches for this player."""
    _load_timeline_matches(state)


def dv_on_timeline_player_search_change(state: Any, var_name: str, var_value: Any) -> None:
    """Server-driven autocomplete for the Timeline player dropdown.

    In-memory substring filter over _dv_timeline_player_map (mirrors the
    Breakdown variant). The map only has rows for players with timeline data
    in the (comp, team) scope, so search results are pre-narrowed to valid picks.
    """
    full = list(_dv_timeline_player_map.keys())
    q = (var_value or "").strip().lower()
    matches = [p for p in full if q in p.lower()][:500] if q else full[:50]
    state.dv_timeline_player_lov = matches if matches else [NO_MATCHES_SENTINEL]
    logger.info("DV timeline search: query=%r -> %d results", q, len(matches))


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
        comps = fetch_pressure_competitions()
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
