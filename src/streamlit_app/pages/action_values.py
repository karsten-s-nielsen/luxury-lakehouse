"""Action Values page — VAEP player rankings, action breakdown, and match timelines."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_action_type_breakdown, plot_action_value_timeline
from streamlit_app.components.filters import (
    render_competition_filter,
    render_match_filter,
    render_minutes_filter,
    render_team_filter,
)
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def _load_vaep_rankings(competition_id: int, min_minutes: int) -> Any:
    """Load VAEP player rankings for a competition."""
    # L-3: Explicit type assertion before query
    competition_id = int(competition_id)
    min_minutes = int(min_minutes)

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading VAEP rankings...")
    def _query(comp_id: int, min_min: int) -> Any:
        return execute_query(
            f"SELECT ps.player_id, p.player_display_name, p.position_group, "  # noqa: S608
            f"  ps.minutes_played, ps.total_vaep, ps.vaep_per_90, "
            f"  ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
            f"  ps.total_actions "
            f"FROM {t('fct_player_stats_synced')} ps "
            f"JOIN {t('dim_players_synced')} p ON ps.player_id = p.player_id "
            f"WHERE ps.competition_id = %s "
            f"  AND ps.minutes_played >= %s "
            f"  AND ps.vaep_per_90 IS NOT NULL "
            f"ORDER BY ps.vaep_per_90 DESC",
            (comp_id, min_min),
        )

    return _query(competition_id, min_minutes)


def _load_action_type_breakdown(
    competition_id: int,
    team_id: int | None,
    player_id: int | None,
) -> Any:
    """Load VAEP breakdown by action type."""
    competition_id = int(competition_id)
    conditions = ["competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        team_id = int(team_id)
        conditions.append("team_id = %s")
        params.append(team_id)

    if player_id is not None:
        player_id = int(player_id)
        conditions.append("player_id = %s")
        params.append(player_id)

    where = " AND ".join(conditions)
    tbl = t("fct_action_values_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading action breakdown...")
    def _query(w: str, p: tuple[Any, ...]) -> Any:
        # SECURITY: WHERE clause built from hardcoded conditions only;
        # all user values use %s parameterized placeholders.
        return execute_query(
            f"SELECT action_type, "  # noqa: S608
            f"  sum(vaep_value) AS total_vaep, "
            f"  sum(offensive_value) AS total_offensive, "
            f"  sum(defensive_value) AS total_defensive, "
            f"  count(*) AS action_count "
            f"FROM {tbl} WHERE {w} "
            f"GROUP BY action_type "
            f"ORDER BY sum(vaep_value) DESC",
            p,
        )

    return _query(where, tuple(params))


def _load_match_timeline(match_id: int, team_id: int | None) -> Any:
    """Load action values for a specific match."""
    match_id = int(match_id)
    conditions = ["match_id = %s"]
    params: list[Any] = [match_id]

    if team_id is not None:
        team_id = int(team_id)
        conditions.append("team_id = %s")
        params.append(team_id)

    where = " AND ".join(conditions)
    tbl = t("fct_action_values_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading match timeline...")
    def _query(w: str, p: tuple[Any, ...]) -> Any:
        return execute_query(
            f"SELECT time_seconds, period, minute, second, "  # noqa: S608
            f"  action_type, action_result, vaep_value, "
            f"  offensive_value, defensive_value, player_id "
            f"FROM {tbl} WHERE {w} "
            f"ORDER BY period, time_seconds",
            p,
        )

    return _query(where, tuple(params))


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


def page() -> None:
    """Render the Action Values page."""
    st.header(":material/trending_up: Action Values (VAEP)")

    view = st.radio(
        "View",
        ["Player VAEP Rankings", "Action Type Breakdown", "Match Action Timeline"],
        horizontal=True,
    )

    if view == "Player VAEP Rankings":
        _render_rankings()
    elif view == "Action Type Breakdown":
        _render_breakdown()
    else:
        _render_timeline()


def _render_rankings() -> None:
    """Render the VAEP player rankings view."""
    with st.sidebar:
        competition_id = render_competition_filter()
        min_minutes = render_minutes_filter()

    if competition_id is None:
        st.info("Select a competition to view VAEP rankings.")
        return

    rankings = _load_vaep_rankings(competition_id, min_minutes)
    if rankings.empty:
        st.warning("No VAEP data available for the selected competition.")
        return

    st.dataframe(
        rankings.rename(
            columns={
                "player_display_name": "Player",
                "position_group": "Position",
                "minutes_played": "Minutes",
                "total_vaep": "Total VAEP",
                "vaep_per_90": "VAEP/90",
                "offensive_vaep_per_90": "Off. VAEP/90",
                "defensive_vaep_per_90": "Def. VAEP/90",
                "total_actions": "Actions",
            }
        ).drop(columns=["player_id"], errors="ignore"),
        use_container_width=True,
        hide_index=True,
    )


def _render_breakdown() -> None:
    """Render the action type breakdown view."""
    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        st.empty()  # Placeholder — player filter rendered below

    if competition_id is None:
        st.info("Select a competition to view action breakdown.")
        return

    # Simple player filter via team's players
    player_id: int | None = None
    if team_id is not None:
        tbl = t("fct_action_values_synced")
        players_tbl = t("dim_players_synced")

        @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
        def _player_options(comp: int, team: int) -> Any:
            return execute_query(
                f"SELECT DISTINCT a.player_id, p.player_display_name "  # noqa: S608
                f"FROM {tbl} a "
                f"JOIN {players_tbl} p ON a.player_id = p.player_id "
                f"WHERE a.competition_id = %s AND a.team_id = %s "
                f"ORDER BY p.player_display_name",
                (comp, team),
            )

        player_opts = _player_options(int(competition_id), int(team_id))
        if not player_opts.empty:
            with st.sidebar:
                selected = st.selectbox(
                    "Player (optional)",
                    options=[None, *player_opts["player_id"].tolist()],
                    format_func=lambda x: (
                        "All players"
                        if x is None
                        else str(player_opts.loc[player_opts["player_id"] == x, "player_display_name"].iloc[0])
                    ),
                )
                player_id = int(selected) if selected is not None else None

    breakdown = _load_action_type_breakdown(competition_id, team_id, player_id)
    if breakdown.empty:
        st.warning("No VAEP data available for the selected filters.")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        fig = plot_action_type_breakdown(breakdown, title="VAEP by Action Type")
        st.pyplot(fig)

    with col_stats:
        total_vaep = float(breakdown["total_vaep"].sum())
        total_actions = int(breakdown["action_count"].sum())
        top_type = str(breakdown.iloc[0]["action_type"]) if not breakdown.empty else "N/A"

        st.metric("Total VAEP", f"{total_vaep:.2f}")
        st.metric("Total Actions", total_actions)
        st.metric("Top Action Type", top_type)


def _render_timeline() -> None:
    """Render the match action timeline view."""
    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)

    if competition_id is None:
        st.info("Select a competition to view match timelines.")
        return

    if match_id is None:
        st.info("Select a match to view the action timeline.")
        return

    actions = _load_match_timeline(match_id, team_id)
    if actions.empty:
        st.warning("No VAEP data available for the selected match.")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        fig = plot_action_value_timeline(actions, title="Match Action Value Timeline")
        st.pyplot(fig)

    with col_stats:
        positive = int((actions["vaep_value"] > 0).sum())
        negative = int((actions["vaep_value"] < 0).sum())
        net_vaep = float(actions["vaep_value"].sum())

        st.metric("Positive Actions", positive)
        st.metric("Negative Actions", negative)
        st.metric("Net Match VAEP", f"{net_vaep:.3f}")

        # Most valuable action
        if not actions.empty:
            best_idx = actions["vaep_value"].idxmax()
            best = actions.loc[best_idx]
            st.metric(
                "Most Valuable Action",
                f"{best['action_type']} ({best['vaep_value']:.3f})",
            )

    with st.expander("Raw Data"):
        st.dataframe(actions, use_container_width=True)
