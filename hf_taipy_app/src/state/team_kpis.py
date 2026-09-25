"""Team KPIs — state module (prefix: tk_).

Dashboard page over the team-match grain mart ``fct_team_metrics`` (team_metrics 44 + match_outcome 5).
Presented BY GRAIN: headline StatCards aggregated across the current scope + one wide team-match table.
team_metrics + match_outcome are per-TEAM aggregates — NOT per-player-evaluative.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from queries.team_kpis import TEAM_METRIC_LABELS, fetch_team_metrics_rows, fetch_team_metrics_summary

from state.shared import (
    _ALL_LABEL,
    get_competition_key,
    get_match_key,
    get_team_id,
    register_page_refresher,
)

logger = logging.getLogger(__name__)

# StatCards (headline — match_outcome + two flagship KPIs)
tk_matches: str = ""
tk_win_prob: str = ""
tk_win_prob_detail: str = ""
tk_xpoints: str = ""
tk_xpoints_detail: str = ""
tk_xg: str = ""
tk_xg_detail: str = ""
tk_field_tilt: str = ""
tk_field_tilt_detail: str = ""
tk_ppda: str = ""
tk_ppda_detail: str = ""

# Wide team-match grain table
tk_metrics_table: pd.DataFrame = pd.DataFrame()

# Scope + status
tk_scope_comp: str = ""
tk_scope_team: str = ""
tk_scope_match: str = ""
tk_data_freshness: str = ""
tk_warning_text: str = ""

__all__ = [
    "tk_matches",
    "tk_win_prob",
    "tk_win_prob_detail",
    "tk_xpoints",
    "tk_xpoints_detail",
    "tk_xg",
    "tk_xg_detail",
    "tk_field_tilt",
    "tk_field_tilt_detail",
    "tk_ppda",
    "tk_ppda_detail",
    "tk_metrics_table",
    "tk_scope_comp",
    "tk_scope_team",
    "tk_scope_match",
    "tk_data_freshness",
    "tk_warning_text",
]


def _clear_state(state: Any) -> None:
    state.tk_matches = ""
    state.tk_win_prob = ""
    state.tk_win_prob_detail = ""
    state.tk_xpoints = ""
    state.tk_xpoints_detail = ""
    state.tk_xg = ""
    state.tk_xg_detail = ""
    state.tk_field_tilt = ""
    state.tk_field_tilt_detail = ""
    state.tk_ppda = ""
    state.tk_ppda_detail = ""
    state.tk_metrics_table = pd.DataFrame()
    state.tk_scope_comp = ""
    state.tk_scope_team = ""
    state.tk_scope_match = ""
    state.tk_warning_text = ""


def _fmt(value: Any, spec: str) -> str:
    """Format a possibly-NULL numeric scope aggregate; em-dash when missing."""
    if value is None or pd.isna(value):
        return "—"
    return format(float(value), spec)


def _build_table(rows: pd.DataFrame) -> pd.DataFrame:
    """Shape the raw mart rows into a readable wide team-match table (no raw ids)."""
    out = pd.DataFrame()
    out["Team"] = rows["team_name"]
    # Human match label: "date — Home v Away" (surrogate match_key never shown).
    date = rows["match_date"].astype(str).str.slice(0, 10)
    home = rows["home_team_name"].fillna("")
    away = rows["away_team_name"].fillna("")
    out["Match"] = date + " — " + home + " v " + away
    for col, label in TEAM_METRIC_LABELS.items():
        if col in rows.columns:
            out[label] = pd.to_numeric(rows[col], errors="coerce").round(3)
    return out


def tk_refresh(state: Any) -> None:
    """Refresh Team KPIs for the selected competition / team / match."""
    comp_key = get_competition_key(state.selected_competition)
    if comp_key is None:
        _clear_state(state)
        return

    team_id = get_team_id(state.selected_team)
    match_key = get_match_key(state.selected_match)

    summary = fetch_team_metrics_summary(comp_key, team_id, match_key)
    rows = fetch_team_metrics_rows(comp_key, team_id, match_key)

    n_rows = int(summary.iloc[0]["n_rows"]) if not summary.empty else 0
    if n_rows == 0 or rows.empty:
        _clear_state(state)
        state.tk_scope_comp = str(state.selected_competition)
        state.tk_warning_text = (
            "No team KPIs for this filter combination yet. The fct_team_metrics mart populates after the "
            "sk4118 recompute; try another competition or team."
        )
        return

    s = summary.iloc[0]
    state.tk_matches = str(n_rows)
    state.tk_win_prob = _fmt(s["mean_p_win"], ".1%")
    state.tk_win_prob_detail = "0-100%, higher = better"
    state.tk_xpoints = _fmt(s["mean_xpoints"], ".2f")
    state.tk_xpoints_detail = "0-3 per match, higher = better"
    state.tk_xg = _fmt(s["mean_xg"], ".2f")
    state.tk_xg_detail = "expected goals per match, higher = better"
    state.tk_field_tilt = _fmt(s["mean_field_tilt"], ".1f")
    state.tk_field_tilt_detail = "% of final-third possession, higher = more territorial control"
    state.tk_ppda = _fmt(s["mean_ppda"], ".2f")
    state.tk_ppda_detail = "passes allowed per defensive action, LOWER = more intense press"

    state.tk_metrics_table = _build_table(rows)

    state.tk_scope_comp = str(state.selected_competition)
    state.tk_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    state.tk_scope_match = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "Full season"
    state.tk_data_freshness = f"{n_rows} team-match rows in scope."
    state.tk_warning_text = ""

    logger.info(
        "Team KPIs refreshed: rows=%d comp_key=%s team_id=%s match_key=%s", n_rows, comp_key, team_id, match_key
    )


register_page_refresher("Team-KPIs", tk_refresh, is_dashboard=True)
