"""Pass Map page — visualize passes on a full pitch with progressive highlighting."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.feedback import empty_result, empty_select
from streamlit_app.components.filters import render_competition_filter, render_match_filter, render_team_filter
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.components.pitch import plot_pass_map
from streamlit_app.db import execute_query, t


@st.cache_data(ttl=600, show_spinner="Loading passes...")
def _fetch_passes(tbl: str, comp_id: int, t_id: int, m_id: int) -> Any:
    return execute_query(
        f"SELECT start_x, start_y, end_x, end_y, is_complete, is_progressive, "  # noqa: S608
        f"  is_line_breaking, minute, second "
        f"FROM {tbl} "
        f"WHERE competition_id = %s AND team_id = %s AND match_id = %s "
        f"ORDER BY minute, second LIMIT 2000",
        (comp_id, t_id, m_id),
    )


def _load_passes(competition_id: int, team_id: int, match_id: int) -> Any:
    """Load pass data for a specific team in a specific match."""
    # L-3: Explicit type assertion before query
    competition_id, team_id, match_id = int(competition_id), int(team_id), int(match_id)
    tbl = t("fct_passes_synced")
    return _fetch_passes(tbl, competition_id, team_id, match_id)


def page() -> None:
    """Render the Pass Map page."""
    st.header(":material/arrow_forward: Pass Map")
    st.caption(
        "Line-breaking detection adapted from "
        "[Parma Calcio 1913 line-breaking-passes](https://github.com/parmacalcio1913/line-breaking-passes) "
        "(Apache-2.0). Ward clustering on StatsBomb 360 freeze-frame positions."
    )

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)

    if competition_id is None or team_id is None or match_id is None:
        empty_select("a competition, team, and match")
        return

    passes = _load_passes(competition_id, team_id, match_id)

    if passes.empty:
        empty_result("passes")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        highlight = st.checkbox("Highlight progressive passes", value=True)
        highlight_lb = st.checkbox("Highlight line-breaking passes", value=True)
        fig = plot_pass_map(passes, highlight_progressive=highlight, highlight_line_breaking=highlight_lb)
        st.pyplot(fig)

    with col_stats:
        total = len(passes)
        completed = int(passes["is_complete"].sum()) if "is_complete" in passes.columns else 0
        complete_mask = passes["is_complete"] == 1 if "is_complete" in passes.columns else passes.index.notnull()
        progressive = (
            int(passes.loc[complete_mask, "is_progressive"].sum()) if "is_progressive" in passes.columns else 0
        )
        line_breaking = (
            int(passes.loc[complete_mask, "is_line_breaking"].sum()) if "is_line_breaking" in passes.columns else 0
        )
        pct = (completed / total * 100) if total > 0 else 0.0

        st.metric("Total Passes", total, help=METRIC_HELP.get("Total Passes"))
        st.metric("Completed", completed, help=METRIC_HELP.get("Completed"))
        st.metric("Completion %", f"{pct:.1f}%", help=METRIC_HELP.get("Completion %"))
        st.metric("Progressive", progressive, help=METRIC_HELP.get("Progressive") or None)
        st.metric(
            "Line-Breaking",
            line_breaking,
            help=METRIC_HELP.get("Line-Breaking") or None,
        )
