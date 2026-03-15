"""Match Summary page — scorecard, xG comparison, and stat bars."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_stat_group_bars
from streamlit_app.components.feedback import empty_result, empty_select
from streamlit_app.components.filters import render_competition_filter, render_match_filter, render_team_filter
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.db import execute_query, t


@st.cache_data(ttl=600, show_spinner="Loading match data...")
def _fetch_match_summary(tbl: str, m_id: int) -> Any:
    return execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_score, away_score, home_xg, away_xg, "
        f"  home_shots, away_shots, home_shots_on_target, away_shots_on_target, "
        f"  home_total_passes, away_total_passes, "
        f"  home_completed_passes, away_completed_passes, "
        f"  home_progressive_passes, away_progressive_passes, "
        f"  home_pass_completion_pct, away_pass_completion_pct, "
        f"  home_possession_pct, home_ppda, away_ppda "
        f"FROM {tbl} WHERE match_id = %s",
        (m_id,),
    )


def _load_match(match_id: int) -> Any:
    """Load match summary data for a single match."""
    # L-3: Explicit type assertion before query
    match_id = int(match_id)
    tbl = t("fct_match_summary_synced")
    return _fetch_match_summary(tbl, match_id)


def page() -> None:
    """Render the Match Summary page."""
    st.header(":material/scoreboard: Match Summary")
    st.caption(
        "Match scorecard with Expected Goals (xG) and key statistics. "
        "xG model by StatsBomb; custom models via "
        "[XGBoost](https://xgboost.readthedocs.io/)."
    )

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)

    if match_id is None:
        empty_select("a competition and match")
        return

    match_data = _load_match(match_id)
    if match_data.empty:
        empty_result("match data")
        return

    m = match_data.iloc[0]
    home_name = str(m.get("home_team_name", "Home"))
    away_name = str(m.get("away_team_name", "Away"))
    home_score = int(m.get("home_score", 0) or 0)
    away_score = int(m.get("away_score", 0) or 0)
    home_xg = float(m.get("home_xg", 0) or 0)
    away_xg = float(m.get("away_xg", 0) or 0)

    # Scorecard — st.metric instead of st.header (M3)
    # Score metrics use team name as label — universally understood, no help= needed
    col_h, col_dash, col_a, col_hxg, col_axg = st.columns([1, 0.3, 1, 1, 1])
    with col_h:
        st.metric(home_name, home_score, help="Match score.")
    with col_dash:
        st.markdown("## —")
    with col_a:
        st.metric(away_name, away_score, help="Match score.")
    with col_hxg:
        st.metric(
            "Home xG",
            f"{home_xg:.2f}",
            delta=f"{home_score - home_xg:+.2f} vs actual",
            delta_color="off",
            help=METRIC_HELP.get("Home xG") or None,
        )
    with col_axg:
        st.metric(
            "Away xG",
            f"{away_xg:.2f}",
            delta=f"{away_score - away_xg:+.2f} vs actual",
            delta_color="off",
            help=METRIC_HELP.get("Away xG") or None,
        )

    st.divider()

    # Small-multiples stat groups — per-group scales (H15, Cleveland & McGill fix)
    col_shooting, col_passing = st.columns(2)

    with col_shooting:
        fig_shoot = plot_stat_group_bars(
            [float(m.get("home_shots", 0) or 0), float(m.get("home_shots_on_target", 0) or 0), home_xg],
            [float(m.get("away_shots", 0) or 0), float(m.get("away_shots_on_target", 0) or 0), away_xg],
            ["Shots", "On Target", "xG"],
            home_name=home_name,
            away_name=away_name,
            title="Shooting",
        )
        st.pyplot(fig_shoot)

    with col_passing:
        fig_pass = plot_stat_group_bars(
            [
                float(m.get("home_total_passes", 0) or 0),
                float(m.get("home_completed_passes", 0) or 0),
                float(m.get("home_progressive_passes", 0) or 0),
            ],
            [
                float(m.get("away_total_passes", 0) or 0),
                float(m.get("away_completed_passes", 0) or 0),
                float(m.get("away_progressive_passes", 0) or 0),
            ],
            ["Total", "Completed", "Progressive"],
            home_name=home_name,
            away_name=away_name,
            title="Passing",
        )
        st.pyplot(fig_pass)

    col_possession, col_ppda = st.columns(2)
    with col_possession:
        fig_poss = plot_stat_group_bars(
            [
                float(m.get("home_pass_completion_pct", 0) or 0),
                float(m.get("home_possession_pct", 0) or 0),
            ],
            [
                float(m.get("away_pass_completion_pct", 0) or 0),
                100.0 - float(m.get("home_possession_pct", 50) or 50),
            ],
            ["Pass %", "Possession %"],
            home_name=home_name,
            away_name=away_name,
            title="Possession",
        )
        st.pyplot(fig_poss)

    with col_ppda:
        home_ppda = float(m.get("home_ppda", 0) or 0)
        away_ppda = float(m.get("away_ppda", 0) or 0)
        fig_ppda = plot_stat_group_bars(
            [home_ppda],
            [away_ppda],
            ["PPDA"],
            home_name=home_name,
            away_name=away_name,
            title="Pressing (lower = more aggressive)",
        )
        st.pyplot(fig_ppda)
        st.caption("PPDA: Passes Per Defensive Action. <10 = aggressive pressing, >15 = passive.")
