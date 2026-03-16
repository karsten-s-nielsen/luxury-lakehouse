"""Standardized user feedback helpers — distinguishes guidance from empty results.

Cognitive audit finding C2: st.info() was used for both "please select" (guidance)
and "no data exists" (empty result), making the two states visually identical.
These helpers enforce distinct visual treatment per Gestalt Similarity principle.
"""

from __future__ import annotations

import streamlit as st


def empty_select(item: str) -> None:
    """Guidance prompt — the user needs to make a selection to proceed.

    Uses st.info (blue) — signals "take action."
    """
    st.info(f"Select {item} to begin.")


def empty_result(item: str, scope_hint: str | None = None) -> None:
    """No-data state — the query returned zero rows for the current filters.

    Uses st.warning (yellow) — signals "nothing here, try different filters."
    Optionally includes a scope hint explaining data coverage constraints.
    """
    msg = f"No {item} for the selected filters."
    if scope_hint:
        msg += f" {scope_hint}"
    st.warning(msg)


def data_scope_note(text: str) -> None:
    """Data coverage context — explains constraints that limit available data.

    Uses st.caption (small gray text) — informational, not blocking.
    EID finding H17/H27: users need to understand why data is limited.
    """
    st.caption(text)


def render_scope_label(competition_id: int | None, team_id: int | None = None) -> None:
    """Show a persistent scope label in the main content area.

    Queries dim tables for human-readable names. Gergle state visibility:
    users should see what data subset they're viewing without checking the sidebar.
    """
    if competition_id is None:
        return
    from streamlit_app.db import execute_query, t

    try:
        comp_df = execute_query(
            f"SELECT country, competition_name FROM {t('dim_competitions_synced')} "  # noqa: S608
            f"WHERE competition_id = %s LIMIT 1",
            (int(competition_id),),
        )
        if comp_df.empty:
            return
        comp_label = f"{comp_df.iloc[0]['country']} — {comp_df.iloc[0]['competition_name']}"
        if team_id is not None:
            team_df = execute_query(
                f"SELECT team_name FROM {t('dim_teams_synced')} "  # noqa: S608
                f"WHERE team_id = %s LIMIT 1",
                (int(team_id),),
            )
            if not team_df.empty:
                comp_label += f" · {team_df.iloc[0]['team_name']}"
        st.caption(f"Showing: {comp_label}")
    except Exception:  # noqa: S110
        pass  # Non-critical — sidebar filters remain visible as fallback


def data_freshness(table: str = "fct_match_summary_synced") -> None:
    """Show a 'latest match' date at the bottom of the page.

    Queries MAX(match_date) from fct_match_summary_synced to show the latest
    match date in the dataset.  Gergle situation awareness: users need to know
    data currency.
    """
    from streamlit_app.db import execute_query, t

    try:
        df = execute_query(
            f"SELECT MAX(match_date) AS latest_match FROM {t(table)} LIMIT 1",  # noqa: S608
        )
        if not df.empty and df.iloc[0]["latest_match"] is not None:
            st.caption(f"Latest match data: {df.iloc[0]['latest_match']}")
    except Exception:
        st.caption("Data freshness unavailable.")
