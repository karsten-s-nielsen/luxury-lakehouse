"""Pass Map page — visualize passes on a full pitch with progressive highlighting."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.filters import render_competition_filter, render_match_filter, render_team_filter
from streamlit_app.components.pitch import plot_pass_map
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_passes(competition_id: int, team_id: int, match_id: int) -> Any:
    """Load pass data for a specific team in a specific match."""
    tbl = t("fct_passes_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading passes...")
    def _query(comp_id: int, t_id: int, m_id: int) -> Any:
        return execute_query(
            f"SELECT start_x, start_y, end_x, end_y, is_complete, is_progressive, "  # noqa: S608
            f"  minute, second "
            f"FROM {tbl} "
            f"WHERE competition_id = %s AND team_id = %s AND match_id = %s "
            f"ORDER BY minute, second",
            (comp_id, t_id, m_id),
        )

    return _query(competition_id, team_id, match_id)


def page() -> None:
    """Render the Pass Map page."""
    st.header(":material/arrow_forward: Pass Map")

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)

    if competition_id is None or team_id is None or match_id is None:
        st.info("Select a competition, team, and match to view the pass map.")
        return

    passes = _load_passes(competition_id, team_id, match_id)

    if passes.empty:
        st.warning("No passes found for the selected filters.")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        highlight = st.checkbox("Highlight progressive passes", value=True)
        fig = plot_pass_map(passes, highlight_progressive=highlight)
        st.pyplot(fig)

    with col_stats:
        total = len(passes)
        completed = int(passes["is_complete"].sum()) if "is_complete" in passes.columns else 0
        progressive = int(passes["is_progressive"].sum()) if "is_progressive" in passes.columns else 0
        pct = (completed / total * 100) if total > 0 else 0.0

        st.metric("Total Passes", total)
        st.metric("Completed", completed)
        st.metric("Completion %", f"{pct:.1f}%")
        st.metric("Progressive", progressive)
