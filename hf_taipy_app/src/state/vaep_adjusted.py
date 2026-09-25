"""VAEP Adjusted — state module (prefix: va_).

Standard page: a per-player leaderboard over fct_action_values that shows RAW vs result-ADJUSTED VAEP
side by side (with the delta). The adjusted triple comes from silly-kicks VAEP.rate_adjusted (Phase D).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from queries.vaep_adjusted import fetch_vaep_adjusted_freshness, fetch_vaep_adjusted_leaderboard

from state.shared import (
    _ALL_LABEL,
    get_competition_key,
    get_team_id,
    register_page_refresher,
)

logger = logging.getLogger(__name__)

va_leaderboard: pd.DataFrame = pd.DataFrame()
va_scope_comp: str = ""
va_scope_team: str = ""
va_data_freshness: str = ""
va_warning_text: str = ""

__all__ = [
    "va_data_freshness",
    "va_leaderboard",
    "va_refresh",
    "va_scope_comp",
    "va_scope_team",
    "va_warning_text",
]


def _clear(state: Any) -> None:
    state.va_leaderboard = pd.DataFrame()
    state.va_scope_team = ""
    state.va_warning_text = ""


def _build_table(rows: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["Player"] = rows["player_display_name"]
    out["Actions"] = pd.to_numeric(rows["actions"], errors="coerce").astype("Int64")
    vaep_raw = pd.to_numeric(rows["vaep_raw"], errors="coerce")
    vaep_adj = pd.to_numeric(rows["vaep_adj"], errors="coerce")
    out["VAEP (raw)"] = vaep_raw.round(2)
    out["VAEP (adjusted)"] = vaep_adj.round(2)
    out["Δ VAEP"] = (vaep_adj - vaep_raw).round(2)
    out["Offensive (raw)"] = pd.to_numeric(rows["off_raw"], errors="coerce").round(2)
    out["Offensive (adjusted)"] = pd.to_numeric(rows["off_adj"], errors="coerce").round(2)
    out["Defensive (raw)"] = pd.to_numeric(rows["def_raw"], errors="coerce").round(2)
    out["Defensive (adjusted)"] = pd.to_numeric(rows["def_adj"], errors="coerce").round(2)
    out["Mean xSuccess"] = pd.to_numeric(rows["mean_xsuccess"], errors="coerce").round(3)
    return out


def va_refresh(state: Any) -> None:
    """Refresh the raw-vs-adjusted VAEP leaderboard for the selected competition (+ optional team)."""
    comp_key = get_competition_key(state.selected_competition)
    if comp_key is None:
        _clear(state)
        return
    team_id = get_team_id(state.selected_team)

    state.va_scope_comp = str(state.selected_competition)
    state.va_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"

    try:
        rows = fetch_vaep_adjusted_leaderboard(comp_key, team_id)
    except Exception:
        logger.exception("Failed to fetch adjusted-VAEP leaderboard")
        _clear(state)
        state.va_warning_text = "Something went wrong loading VAEP data. Try refreshing."
        return

    if rows.empty:
        state.va_leaderboard = pd.DataFrame()
        state.va_warning_text = (
            "No adjusted VAEP for this scope yet. The adjusted columns populate after the sk4118 recompute; "
            "IDSSE/Metrica players are excluded while player surrogates resolve only for StatsBomb/Wyscout."
        )
    else:
        state.va_leaderboard = _build_table(rows)
        state.va_warning_text = ""

    try:
        state.va_data_freshness = fetch_vaep_adjusted_freshness()
    except Exception:
        state.va_data_freshness = ""

    logger.info("VAEP-adjusted refreshed: rows=%d comp_key=%s team_id=%s", len(rows), comp_key, team_id)


register_page_refresher("VAEP-Adjusted", va_refresh)
