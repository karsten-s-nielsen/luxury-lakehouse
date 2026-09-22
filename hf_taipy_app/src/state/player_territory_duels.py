"""Player Territory & Duels — state module (prefix: ptd_).

Two-view player leaderboard page over the player-match grain mart fct_player_match_metrics. Both
families are ranking-licensed, so each view is a ranked leaderboard (Competition, optional Team):
  * Territory — ranked by net xT defended; shows v1 and counterfactual xT-prevented SIDE BY SIDE.
  * Duels — ranked by the latest carried-forward Glicko-2 rating (the stabilised surface).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from queries.player_territory_duels import (
    fetch_duels_leaderboard,
    fetch_ptd_data_freshness,
    fetch_territory_leaderboard,
)

from state.shared import (
    _ALL_LABEL,
    get_competition_key,
    get_team_id,
    register_page_refresher,
)

logger = logging.getLogger(__name__)

PTD_SUB_VIEW_LOV: list[str] = ["Territory", "Duels"]

ptd_territory_table: pd.DataFrame = pd.DataFrame()
ptd_duels_table: pd.DataFrame = pd.DataFrame()
ptd_scope_comp: str = ""
ptd_scope_team: str = ""
ptd_data_freshness: str = ""
ptd_warning_text: str = ""

__all__ = [
    "PTD_SUB_VIEW_LOV",
    "ptd_data_freshness",
    "ptd_duels_table",
    "ptd_refresh",
    "ptd_scope_comp",
    "ptd_scope_team",
    "ptd_territory_table",
    "ptd_warning_text",
]


def _clear(state: Any) -> None:
    state.ptd_territory_table = pd.DataFrame()
    state.ptd_duels_table = pd.DataFrame()
    state.ptd_scope_team = ""
    state.ptd_warning_text = ""


def _build_territory_table(rows: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["Player"] = rows["player_display_name"]
    out["Matches"] = pd.to_numeric(rows["matches"], errors="coerce").astype("Int64")
    out["xT Net"] = pd.to_numeric(rows["xt_net"], errors="coerce").round(3)
    out["xT Prevented (v1)"] = pd.to_numeric(rows["xt_prevented_v1"], errors="coerce").round(3)
    out["xT Prevented Above Exp. (cf)"] = pd.to_numeric(rows["xt_prevented_cf"], errors="coerce").round(3)
    out["Passes into Hull"] = pd.to_numeric(rows["passes_into_hull"], errors="coerce").astype("Int64")
    out["Hull Area (m²)"] = pd.to_numeric(rows["hull_area_m2"], errors="coerce").round(0)
    return out


def _build_duels_table(rows: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["Player"] = rows["player_display_name"]
    out["Glicko-2 Rating"] = pd.to_numeric(rows["duel_rating"], errors="coerce").round(0)
    out["Rating ±"] = pd.to_numeric(rows["duel_rating_deviation"], errors="coerce").round(0)
    out["Matches"] = pd.to_numeric(rows["matches"], errors="coerce").astype("Int64")
    contested = pd.to_numeric(rows["contested"], errors="coerce")
    won = pd.to_numeric(rows["won"], errors="coerce")
    out["Contested"] = contested.astype("Int64")
    out["Won"] = won.astype("Int64")
    out["Lost"] = pd.to_numeric(rows["lost"], errors="coerce").astype("Int64")
    out["Win %"] = ((won / contested.where(contested > 0)) * 100).round(1)
    return out


def ptd_refresh(state: Any) -> None:
    """Refresh Territory / Duels leaderboards for the selected competition (+ optional team)."""
    current_lov = getattr(state, "sub_view_lov", None) or []
    if list(current_lov) != PTD_SUB_VIEW_LOV:
        state.sub_view_lov = PTD_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in PTD_SUB_VIEW_LOV:
        state.selected_sub_view = PTD_SUB_VIEW_LOV[0]

    comp_key = get_competition_key(state.selected_competition)
    if comp_key is None:
        _clear(state)
        return
    team_id = get_team_id(state.selected_team)

    state.ptd_scope_comp = str(state.selected_competition)
    state.ptd_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"

    try:
        if state.selected_sub_view == "Duels":
            rows = fetch_duels_leaderboard(comp_key, team_id)
            if rows.empty:
                state.ptd_duels_table = pd.DataFrame()
                state.ptd_warning_text = "No ground-duel ratings for this scope yet (populates after the recompute)."
            else:
                state.ptd_duels_table = _build_duels_table(rows)
                state.ptd_warning_text = ""
        else:
            rows = fetch_territory_leaderboard(comp_key, team_id)
            if rows.empty:
                state.ptd_territory_table = pd.DataFrame()
                state.ptd_warning_text = "No territory metrics for this scope yet (populates after the recompute)."
            else:
                state.ptd_territory_table = _build_territory_table(rows)
                state.ptd_warning_text = ""
    except Exception:
        logger.exception("Failed to refresh player territory/duels")
        state.ptd_warning_text = "Something went wrong loading player metrics. Try refreshing."
        return

    try:
        state.ptd_data_freshness = fetch_ptd_data_freshness()
    except Exception:
        state.ptd_data_freshness = ""


register_page_refresher("Player-Metrics", ptd_refresh)
