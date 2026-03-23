"""Match Summary state module — all variables prefixed with ms_.

Loads match data, computes scorecard metrics, renders 4 stat bar charts.
Registered as the Match-Summary page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from cache import ttl_cache
from db import execute_query, t
from filters import fetch_data_freshness, fetch_scope_label
from render import PITCH_BG_COLOR, TEXT_COLOR, chart_to_file, fmt_int

from state.shared import get_comp_id, get_match_id, register_page_refresher

logger = logging.getLogger(__name__)

# ── Exported state variables (all ms_ prefixed) ─────────────────────────────
ms_home_name: str = ""
ms_away_name: str = ""
ms_home_score: str = "--"
ms_away_score: str = "--"
ms_home_xg: str = "--"
ms_away_xg: str = "--"
ms_home_xg_delta: str = ""
ms_away_xg_delta: str = ""

ms_shooting_chart: str = ""
ms_passing_chart: str = ""
ms_possession_chart: str = ""
ms_ppda_chart: str = ""

ms_warning_text: str = ""
ms_scope_label: str = ""
ms_data_freshness: str = ""

__all__ = [
    "ms_away_name",
    "ms_away_score",
    "ms_away_xg",
    "ms_away_xg_delta",
    "ms_data_freshness",
    "ms_home_name",
    "ms_home_score",
    "ms_home_xg",
    "ms_home_xg_delta",
    "ms_passing_chart",
    "ms_possession_chart",
    "ms_ppda_chart",
    "ms_scope_label",
    "ms_shooting_chart",
    "ms_warning_text",
]


# ── Data fetching ────────────────────────────────────────────────────────────


@ttl_cache()
def _fetch_match_summary(match_id: int) -> pd.DataFrame:
    """Fetch full match summary row for a single match."""
    return execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_score, away_score, home_xg, away_xg, "
        f"  home_shots, away_shots, home_shots_on_target, away_shots_on_target, "
        f"  home_total_passes, away_total_passes, "
        f"  home_completed_passes, away_completed_passes, "
        f"  home_progressive_passes, away_progressive_passes, "
        f"  home_pass_completion_pct, away_pass_completion_pct, "
        f"  home_possession_pct, home_ppda, away_ppda "
        f"FROM {t('fct_match_summary_synced')} WHERE match_id = %s",
        (int(match_id),),
    )


# ── Rendering — stat comparison bar charts ───────────────────────────────────

_HOME_COLOR = "#e63946"
_AWAY_COLOR = "#457b9d"


def _render_stat_bars(
    home_vals: list[float],
    away_vals: list[float],
    labels: list[str],
    home_name: str,
    away_name: str,
    title: str,
    file_name: str,
) -> str:
    """Render a grouped horizontal bar chart to temp PNG, return file path."""
    fig, ax = plt.subplots(figsize=(6, max(2.5, len(labels) * 0.8)), facecolor=PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    y = np.arange(len(labels))
    bar_h = 0.35
    ax.barh(y - bar_h / 2, home_vals, bar_h, label=home_name, color=_HOME_COLOR, alpha=0.85)
    ax.barh(y + bar_h / 2, away_vals, bar_h, label=away_name, color=_AWAY_COLOR, alpha=0.85, hatch="///")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, color=TEXT_COLOR, fontsize=10)
    ax.set_title(title, color=TEXT_COLOR, fontsize=12, pad=10)
    ax.tick_params(axis="x", colors=TEXT_COLOR)
    ax.legend(loc="lower right", fontsize=8, facecolor=PITCH_BG_COLOR, edgecolor="#444", labelcolor=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#444")
    ax.spines["left"].set_color("#444")

    return chart_to_file(fig, file_name)


# ── Refresh callback ─────────────────────────────────────────────────────────


def ms_refresh(state: Any) -> None:
    """Reload match summary data, compute scorecard, render 4 stat bar charts."""
    comp_id = get_comp_id(state.selected_competition)
    match_id = get_match_id(state.selected_match)
    if match_id is None:
        # Clear all state when no match selected
        state.ms_home_name = ""
        state.ms_away_name = ""
        state.ms_home_score = "--"
        state.ms_away_score = "--"
        state.ms_home_xg = "--"
        state.ms_away_xg = "--"
        state.ms_home_xg_delta = ""
        state.ms_away_xg_delta = ""
        state.ms_shooting_chart = ""
        state.ms_passing_chart = ""
        state.ms_possession_chart = ""
        state.ms_ppda_chart = ""
        state.ms_warning_text = ""
        state.ms_scope_label = ""
        state.ms_data_freshness = ""
        return

    # Scope label
    if comp_id is not None:
        state.ms_scope_label = fetch_scope_label(comp_id, None)
    else:
        state.ms_scope_label = ""

    match_data = _fetch_match_summary(match_id)
    if match_data.empty:
        state.ms_home_name = ""
        state.ms_away_name = ""
        state.ms_home_score = "--"
        state.ms_away_score = "--"
        state.ms_home_xg = "--"
        state.ms_away_xg = "--"
        state.ms_home_xg_delta = ""
        state.ms_away_xg_delta = ""
        state.ms_shooting_chart = ""
        state.ms_passing_chart = ""
        state.ms_possession_chart = ""
        state.ms_ppda_chart = ""
        state.ms_warning_text = "No match data for the selected filters."
        state.ms_data_freshness = ""
        return

    m = match_data.iloc[0]
    state.ms_warning_text = ""

    # --- Scorecard metrics ---
    home_name = str(m.get("home_team_name", "Home"))
    away_name = str(m.get("away_team_name", "Away"))
    home_score = int(m.get("home_score", 0) or 0)
    away_score = int(m.get("away_score", 0) or 0)
    home_xg = float(m.get("home_xg", 0) or 0)
    away_xg = float(m.get("away_xg", 0) or 0)

    state.ms_home_name = home_name
    state.ms_away_name = away_name
    state.ms_home_score = fmt_int(home_score)
    state.ms_away_score = fmt_int(away_score)
    state.ms_home_xg = f"{home_xg:.2f}"
    state.ms_away_xg = f"{away_xg:.2f}"
    state.ms_home_xg_delta = f"{home_score - home_xg:+.2f} vs actual"
    state.ms_away_xg_delta = f"{away_score - away_xg:+.2f} vs actual"

    # --- Shooting chart ---
    state.ms_shooting_chart = _render_stat_bars(
        home_vals=[float(m.get("home_shots", 0) or 0), float(m.get("home_shots_on_target", 0) or 0), home_xg],
        away_vals=[float(m.get("away_shots", 0) or 0), float(m.get("away_shots_on_target", 0) or 0), away_xg],
        labels=["Shots", "On Target", "xG"],
        home_name=home_name,
        away_name=away_name,
        title="Shooting",
        file_name="ms_bars_shooting",
    )

    # --- Passing chart ---
    state.ms_passing_chart = _render_stat_bars(
        home_vals=[
            float(m.get("home_total_passes", 0) or 0),
            float(m.get("home_completed_passes", 0) or 0),
            float(m.get("home_progressive_passes", 0) or 0),
        ],
        away_vals=[
            float(m.get("away_total_passes", 0) or 0),
            float(m.get("away_completed_passes", 0) or 0),
            float(m.get("away_progressive_passes", 0) or 0),
        ],
        labels=["Total", "Completed", "Progressive"],
        home_name=home_name,
        away_name=away_name,
        title="Passing",
        file_name="ms_bars_passing",
    )

    # --- Possession chart ---
    home_poss = float(m.get("home_possession_pct", 50) or 50)
    state.ms_possession_chart = _render_stat_bars(
        home_vals=[
            float(m.get("home_pass_completion_pct", 0) or 0),
            home_poss,
        ],
        away_vals=[
            float(m.get("away_pass_completion_pct", 0) or 0),
            100.0 - home_poss,
        ],
        labels=["Pass %", "Possession %"],
        home_name=home_name,
        away_name=away_name,
        title="Possession",
        file_name="ms_bars_possession",
    )

    # --- PPDA chart ---
    home_ppda = float(m.get("home_ppda", 0) or 0)
    away_ppda = float(m.get("away_ppda", 0) or 0)
    state.ms_ppda_chart = _render_stat_bars(
        home_vals=[home_ppda],
        away_vals=[away_ppda],
        labels=["PPDA"],
        home_name=home_name,
        away_name=away_name,
        title="Pressing (lower = more aggressive)",
        file_name="ms_bars_ppda",
    )

    # Data freshness
    state.ms_data_freshness = fetch_data_freshness()

    logger.info(
        "Match summary refreshed: %s %d-%d %s (xG: %.2f-%.2f)",
        home_name,
        home_score,
        away_score,
        away_name,
        home_xg,
        away_xg,
    )


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Match-Summary", ms_refresh)
