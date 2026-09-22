"""Goalkeeper Decisions & Shot-Stopping — state module (prefix: gkd_).

Two-view keeper page (tile-top, full-width, like Goalkeeper Analytics). Pick Competition -> Keeper.
  * Decision-Making — fct_gk_decision aggregated per keeper. Cohort POSITIONING only (reconstruction
    tier is not ranking-licensed): a scatter of the whole cohort with the selected keeper highlighted.
  * Shot-Stopping — fct_gk_shot_stopping_pooled goals-prevented + CI band. The mart defers percentile
    ranking (ranking_enabled=false everywhere), so this is cohort positioning + band, not a leaderboard.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from queries.gk_decision_shot_stopping import (
    fetch_decision_cohort,
    fetch_gkd_competitions,
    fetch_gkd_data_freshness,
    fetch_gkd_keepers,
    fetch_shot_stopping_cohort,
)
from render import PITCH_BG_COLOR, TEXT_COLOR

from state.shared import register_page_refresher

logger = logging.getLogger(__name__)

GKD_SUB_VIEW_LOV: list[str] = ["Decision-Making", "Shot-Stopping"]

_SELECTED_COLOR = "#f59e0b"  # theme gold — the selected keeper
_COHORT_COLOR = "#6b7280"  # muted grey — the rest of the competition cohort

# ---------------------------------------------------------------------------
# State variables (gkd_ prefixed)
# ---------------------------------------------------------------------------
gkd_competition_lov: list[str] = []
gkd_selected_competition: str | None = None
gkd_keeper_lov: list[str] = []
gkd_selected_keeper: str | None = None
gkd_scope_comp: str = ""
gkd_keeper_label: str = ""
gkd_data_freshness: str = ""
gkd_warning_text: str = ""
gkd_plot_config: dict[str, Any] = {"displayModeBar": False}

# Decision-Making view
gkd_dec_n: str = "—"
gkd_dec_quality: str = "—"
gkd_dec_quality_detail: str = ""
gkd_dec_optimal: str = "—"
gkd_dec_optimal_detail: str = ""
gkd_dec_value_left: str = "—"
gkd_dec_value_left_detail: str = ""
gkd_decision_figure: Any = None

# Shot-Stopping view
gkd_ss_gp: str = "—"
gkd_ss_gp_detail: str = ""
gkd_ss_psxg: str = "—"
gkd_ss_shots: str = "—"
gkd_ss_conceded: str = "—"
gkd_ss_figure: Any = None

_gkd_competition_map: dict[str, int] = {}
_gkd_keeper_map: dict[str, int] = {}

__all__ = [
    "GKD_SUB_VIEW_LOV",
    "gkd_competition_lov",
    "gkd_data_freshness",
    "gkd_dec_n",
    "gkd_dec_optimal",
    "gkd_dec_optimal_detail",
    "gkd_dec_quality",
    "gkd_dec_quality_detail",
    "gkd_dec_value_left",
    "gkd_dec_value_left_detail",
    "gkd_decision_figure",
    "gkd_keeper_label",
    "gkd_keeper_lov",
    "gkd_on_competition_change",
    "gkd_on_keeper_change",
    "gkd_plot_config",
    "gkd_refresh",
    "gkd_scope_comp",
    "gkd_selected_competition",
    "gkd_selected_keeper",
    "gkd_ss_conceded",
    "gkd_ss_figure",
    "gkd_ss_gp",
    "gkd_ss_gp_detail",
    "gkd_ss_psxg",
    "gkd_ss_shots",
    "gkd_warning_text",
]


def _fmt(value: Any, spec: str) -> str:
    if value is None or pd.isna(value):
        return "—"
    return format(float(value), spec)


def _cohort_scatter(points: list[tuple[str, float, float, bool]], x_title: str, y_title: str) -> go.Figure | None:
    """Cohort positioning scatter — one point per keeper, selected keeper highlighted (never a rank)."""
    if not points:
        return None
    fig = go.Figure()
    others = [p for p in points if not p[3]]
    if others:
        fig.add_trace(
            go.Scatter(
                x=[p[1] for p in others],
                y=[p[2] for p in others],
                mode="markers",
                name="Competition cohort",
                marker={"color": _COHORT_COLOR, "size": 9, "opacity": 0.6},
                text=[p[0] for p in others],
                hovertemplate="%{text}<br>" + x_title + ": %{x:.3f}<br>" + y_title + ": %{y:.3f}<extra></extra>",
            )
        )
    sel = [p for p in points if p[3]]
    if sel:
        fig.add_trace(
            go.Scatter(
                x=[p[1] for p in sel],
                y=[p[2] for p in sel],
                mode="markers",
                name="Selected keeper",
                marker={"color": _SELECTED_COLOR, "size": 18, "symbol": "star", "line": {"color": "white", "width": 1}},
                text=[p[0] for p in sel],
                hovertemplate="%{text}<br>" + x_title + ": %{x:.3f}<br>" + y_title + ": %{y:.3f}<extra></extra>",
            )
        )
    fig.update_layout(
        plot_bgcolor=PITCH_BG_COLOR,
        paper_bgcolor=PITCH_BG_COLOR,
        font_color=TEXT_COLOR,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "center", "x": 0.5},
        margin={"l": 60, "r": 30, "t": 40, "b": 50},
        xaxis={"title": x_title, "gridcolor": "#333", "zeroline": True, "zerolinecolor": "#444"},
        yaxis={"title": y_title, "gridcolor": "#333", "zeroline": True, "zerolinecolor": "#444"},
        height=430,
    )
    return fig


def _reset_decision(state: Any) -> None:
    state.gkd_dec_n = state.gkd_dec_quality = state.gkd_dec_optimal = state.gkd_dec_value_left = "—"
    state.gkd_dec_quality_detail = state.gkd_dec_optimal_detail = state.gkd_dec_value_left_detail = ""
    state.gkd_decision_figure = None


def _reset_shot_stopping(state: Any) -> None:
    state.gkd_ss_gp = state.gkd_ss_psxg = state.gkd_ss_shots = state.gkd_ss_conceded = "—"
    state.gkd_ss_gp_detail = ""
    state.gkd_ss_figure = None


def gkd_refresh(state: Any) -> None:
    """Refresh the keeper decision / shot-stopping views for the selected competition + keeper."""
    current_lov = getattr(state, "sub_view_lov", None) or []
    if list(current_lov) != GKD_SUB_VIEW_LOV:
        state.sub_view_lov = GKD_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in GKD_SUB_VIEW_LOV:
        state.selected_sub_view = GKD_SUB_VIEW_LOV[0]
    state.gkd_warning_text = ""

    global _gkd_competition_map, _gkd_keeper_map
    try:
        comps = fetch_gkd_competitions()
    except Exception:
        logger.exception("Failed to fetch GK decision competitions")
        state.gkd_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    if comps.empty:
        state.gkd_warning_text = "No goalkeeper decision/shot-stopping data available yet."
        return
    _gkd_competition_map = {
        str(n): int(k) for n, k in zip(comps["competition_name"], comps["competition_key"], strict=True)
    }
    state.gkd_competition_lov = list(_gkd_competition_map.keys())
    if not state.gkd_selected_competition or state.gkd_selected_competition not in _gkd_competition_map:
        state.gkd_selected_competition = state.gkd_competition_lov[0]
    state.gkd_scope_comp = state.gkd_selected_competition
    comp_key = _gkd_competition_map[state.gkd_selected_competition]

    try:
        keepers = fetch_gkd_keepers(comp_key)
    except Exception:
        logger.exception("Failed to fetch GK keepers for competition=%s", comp_key)
        state.gkd_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    _gkd_keeper_map = (
        {str(n): int(k) for n, k in zip(keepers["player_display_name"], keepers["player_key"], strict=True)}
        if not keepers.empty
        else {}
    )
    state.gkd_keeper_lov = list(_gkd_keeper_map.keys())
    if not state.gkd_keeper_lov:
        state.gkd_warning_text = f"No goalkeeper data for {state.gkd_selected_competition} yet."
        return
    if not state.gkd_selected_keeper or state.gkd_selected_keeper not in _gkd_keeper_map:
        state.gkd_selected_keeper = state.gkd_keeper_lov[0]
    state.gkd_keeper_label = state.gkd_selected_keeper
    gk_key = _gkd_keeper_map[state.gkd_selected_keeper]

    if state.selected_sub_view == "Shot-Stopping":
        _refresh_shot_stopping(state, comp_key, gk_key)
    else:
        _refresh_decision(state, comp_key, gk_key)

    try:
        state.gkd_data_freshness = fetch_gkd_data_freshness()
    except Exception:
        state.gkd_data_freshness = ""


def _refresh_decision(state: Any, comp_key: int, gk_key: int) -> None:
    try:
        cohort = fetch_decision_cohort(comp_key)
    except Exception:
        logger.exception("Failed to fetch decision cohort")
        state.gkd_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    mine = cohort[cohort["player_key"] == gk_key] if not cohort.empty else cohort
    if mine.empty:
        _reset_decision(state)
        state.gkd_warning_text = (
            f"{state.gkd_selected_keeper} has fewer than 10 reconstructed distribution decisions in "
            f"{state.gkd_selected_competition} — too few for a decision profile."
        )
        return

    r = mine.iloc[0]
    n = int(r["n_decisions"])
    quality = float(r["mean_sel_efficiency"])
    optimal = float(r["mean_decision_pct"])
    value_left = float(r["mean_best_ev"]) - float(r["mean_chosen_ev"])
    state.gkd_dec_n = str(n)
    state.gkd_dec_quality = f"{quality:.0%}"
    state.gkd_dec_quality_detail = "chosen ÷ best EV (0-100%, higher = better)"
    state.gkd_dec_optimal = f"{optimal:.0%}"
    state.gkd_dec_optimal_detail = "percentile of the chosen option (0-100%, ~50% = random, higher = better)"
    state.gkd_dec_value_left = f"{value_left:+.3f}"
    state.gkd_dec_value_left_detail = "best - chosen EV (threat units, LOWER = less value left on the table)"

    # Cohort positioning scatter: optimal-choice percentile (x) vs selection efficiency (y).
    x = pd.to_numeric(cohort["mean_decision_pct"], errors="coerce").tolist()
    y = pd.to_numeric(cohort["mean_sel_efficiency"], errors="coerce").tolist()
    names = cohort["player_display_name"].astype(str).tolist()
    sel = (cohort["player_key"] == gk_key).tolist()
    points = [
        (str(nm), float(px), float(py), bool(s))
        for nm, px, py, s in zip(names, x, y, sel, strict=True)
        if not (pd.isna(px) or pd.isna(py))
    ]
    state.gkd_decision_figure = _cohort_scatter(points, "Optimal-choice percentile (0-1)", "Selection efficiency (0-1)")


def _refresh_shot_stopping(state: Any, comp_key: int, gk_key: int) -> None:
    try:
        cohort = fetch_shot_stopping_cohort(comp_key)
    except Exception:
        logger.exception("Failed to fetch shot-stopping cohort")
        state.gkd_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    mine = cohort[cohort["player_key"] == gk_key] if not cohort.empty else cohort
    if mine.empty:
        _reset_shot_stopping(state)
        state.gkd_warning_text = (
            f"{state.gkd_selected_keeper} has no shot-stopping data in {state.gkd_selected_competition}."
        )
        return

    r = mine.iloc[0]
    gp = float(r["goals_prevented"])
    shots = int(r["shots_faced_total"])
    low_sample = bool(r["low_sample"])
    lo, hi = float(r["goals_prevented_ci_low"]), float(r["goals_prevented_ci_high"])
    straddle = " · band straddles 0 (inconclusive)" if lo <= 0 <= hi else ""
    sample_note = " · LOW SAMPLE" if low_sample else ""
    state.gkd_ss_gp = f"{gp:+.2f}"
    state.gkd_ss_gp_detail = f"n={shots} shots · higher = better, 0 = as expected{straddle}{sample_note}"
    state.gkd_ss_psxg = _fmt(r["psxg_faced"], ".2f")
    state.gkd_ss_shots = str(shots)
    state.gkd_ss_conceded = str(int(r["goals_conceded"]))

    x = pd.to_numeric(cohort["shots_faced_total"], errors="coerce").tolist()
    y = pd.to_numeric(cohort["goals_prevented"], errors="coerce").tolist()
    names = cohort["player_display_name"].astype(str).tolist()
    sel = (cohort["player_key"] == gk_key).tolist()
    points = [
        (str(nm), float(px), float(py), bool(s))
        for nm, px, py, s in zip(names, x, y, sel, strict=True)
        if not (pd.isna(px) or pd.isna(py))
    ]
    state.gkd_ss_figure = _cohort_scatter(points, "Shots faced", "Goals prevented (PSxG - goals conceded)")


def gkd_on_competition_change(state: Any, var_name: str, var_value: Any) -> None:
    state.gkd_selected_keeper = None  # new competition -> reselect its first keeper
    gkd_refresh(state)


def gkd_on_keeper_change(state: Any, var_name: str, var_value: Any) -> None:
    gkd_refresh(state)


register_page_refresher("GK-Decisions", gkd_refresh)
