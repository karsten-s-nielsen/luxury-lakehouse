"""Shot Map page — visualize shots on a half-pitch with xG sizing."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.filters import render_competition_filter, render_player_filter, render_team_filter
from streamlit_app.components.pitch import plot_shot_map
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_shots(
    competition_id: int,
    team_id: int | None = None,
    player_id: int | None = None,
) -> Any:
    """Load shot data from Lakebase with filters applied."""
    # L-3: Explicit type assertion before query — defense-in-depth beyond
    # Streamlit widget type enforcement and %s parameterized placeholders.
    competition_id = int(competition_id)
    conditions = ["s.competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        team_id = int(team_id)
        conditions.append("s.team_id = %s")
        params.append(team_id)
    if player_id is not None:
        player_id = int(player_id)
        conditions.append("s.player_id = %s")
        params.append(player_id)

    # SECURITY: `where` is built entirely from hardcoded conditions above
    # (never user input). All user-supplied values use %s parameterized placeholders.
    where = " AND ".join(conditions)

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading shots...")
    def _query(w: str, p: tuple[Any, ...]) -> Any:
        return execute_query(
            f"SELECT s.location_x, s.location_y, s.statsbomb_xg, s.is_goal, "  # noqa: S608
            f"  s.shot_outcome, s.shot_body_part, s.distance_to_goal, s.shot_angle, "
            f"  s.minute, p.player_display_name "
            f"FROM {t('fct_shots_synced')} s "
            f"JOIN {t('dim_players_synced')} p ON s.player_id = p.player_id "
            f"WHERE {w} "
            f"ORDER BY s.minute, s.second",
            p,
        )

    return _query(where, tuple(params))


def page() -> None:
    """Render the Shot Map page."""
    st.header(":material/target: Shot Map")

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        player_id = render_player_filter(competition_id, team_id)
        if isinstance(player_id, list):
            player_id = player_id[0] if player_id else None

    if competition_id is None:
        st.info("Select a competition to view shots.")
        return

    shots = _load_shots(competition_id, team_id, player_id)

    if shots.empty:
        st.warning("No shots found for the selected filters.")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        title_parts = ["Shot Map"]
        if player_id is not None and not shots.empty:
            title_parts.append(f"— {shots['player_display_name'].iloc[0]}")
        fig = plot_shot_map(shots, title=" ".join(title_parts))
        st.pyplot(fig)

    with col_stats:
        total = len(shots)
        goals = int(shots["is_goal"].sum())
        xg_sum = float(shots["statsbomb_xg"].sum())
        conversion = (goals / total * 100) if total > 0 else 0.0
        xg_per_shot = xg_sum / total if total > 0 else 0.0

        st.metric("Total Shots", total)
        st.metric("Goals", goals)
        st.metric("Total xG", f"{xg_sum:.2f}")
        st.metric("Conversion Rate", f"{conversion:.1f}%")
        st.metric("xG / Shot", f"{xg_per_shot:.3f}")
