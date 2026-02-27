"""Match Summary page — scorecard, xG comparison, and stat bars."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_match_comparison_bars
from streamlit_app.components.filters import render_competition_filter, render_match_filter, render_team_filter
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_match(match_id: int) -> Any:
    """Load match summary data for a single match."""
    tbl = t("fct_match_summary_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading match data...")
    def _query(m_id: int) -> Any:
        return execute_query(
            f"SELECT * FROM {tbl} WHERE match_id = %s",  # noqa: S608
            (m_id,),
        )

    return _query(match_id)


def page() -> None:
    """Render the Match Summary page."""
    st.header(":material/scoreboard: Match Summary")

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)

    if match_id is None:
        st.info("Select a competition and match to view the summary.")
        return

    match_data = _load_match(match_id)
    if match_data.empty:
        st.warning("No data found for this match.")
        return

    m = match_data.iloc[0]

    # Scorecard header
    col_home, col_score, col_away = st.columns([2, 1, 2])
    with col_home:
        st.subheader(str(m.get("home_team_name", "Home")))
    with col_score:
        score = f"{int(m.get('home_score', 0) or 0)} — {int(m.get('away_score', 0) or 0)}"
        st.header(score)
    with col_away:
        st.subheader(str(m.get("away_team_name", "Away")))

    st.divider()

    # xG comparison
    col_hxg, col_axg = st.columns(2)
    with col_hxg:
        st.metric("Home xG", f"{m.get('home_xg', 0):.2f}")
    with col_axg:
        st.metric("Away xG", f"{m.get('away_xg', 0):.2f}")

    st.divider()

    # Bar chart comparison
    stat_labels = [
        "Shots",
        "Shots on Target",
        "xG",
        "Passes",
        "Completed Passes",
        "Progressive Passes",
        "Pass Completion %",
        "Possession %",
    ]
    home_vals = [
        float(m.get("home_shots", 0) or 0),
        float(m.get("home_shots_on_target", 0) or 0),
        float(m.get("home_xg", 0) or 0),
        float(m.get("home_total_passes", 0) or 0),
        float(m.get("home_completed_passes", 0) or 0),
        float(m.get("home_progressive_passes", 0) or 0),
        float(m.get("home_pass_completion_pct", 0) or 0),
        float(m.get("home_possession_pct", 0) or 0),
    ]
    away_vals = [
        float(m.get("away_shots", 0) or 0),
        float(m.get("away_shots_on_target", 0) or 0),
        float(m.get("away_xg", 0) or 0),
        float(m.get("away_total_passes", 0) or 0),
        float(m.get("away_completed_passes", 0) or 0),
        float(m.get("away_progressive_passes", 0) or 0),
        float(m.get("away_pass_completion_pct", 0) or 0),
        100.0 - float(m.get("home_possession_pct", 50) or 50),
    ]

    fig = plot_match_comparison_bars(
        home_vals,
        away_vals,
        stat_labels,
        home_name=str(m.get("home_team_name", "Home")),
        away_name=str(m.get("away_team_name", "Away")),
    )
    st.pyplot(fig)
