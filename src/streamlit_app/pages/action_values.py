"""Action Values page — VAEP player rankings, action breakdown, and match timelines."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_action_type_breakdown, plot_action_value_timeline
from streamlit_app.components.feedback import empty_result, empty_select
from streamlit_app.components.filters import (
    render_competition_filter,
    render_match_filter,
    render_minutes_filter,
    render_team_filter,
)
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.db import execute_query, t

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=600, show_spinner="Loading VAEP rankings...")
def _load_vaep_rankings(competition_id: int, min_minutes: int) -> Any:
    """Load VAEP player rankings for a competition."""
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
        f"ORDER BY ps.vaep_per_90 DESC "
        f"LIMIT 500",
        (competition_id, min_minutes),
    )


@st.cache_data(ttl=600, show_spinner="Loading action breakdown...")
def _load_action_type_breakdown_query(where: str, params: tuple[Any, ...], tbl: str) -> Any:
    """Execute the action type breakdown query with pre-built WHERE clause."""
    # SECURITY: WHERE clause built from hardcoded conditions only;
    # all user values use %s parameterized placeholders.
    return execute_query(
        f"SELECT action_type, "  # noqa: S608
        f"  sum(vaep_value) AS total_vaep, "
        f"  sum(offensive_value) AS total_offensive, "
        f"  sum(defensive_value) AS total_defensive, "
        f"  count(*) AS action_count "
        f"FROM {tbl} WHERE {where} "
        f"GROUP BY action_type "
        f"ORDER BY sum(vaep_value) DESC",
        params,
    )


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
    return _load_action_type_breakdown_query(where, tuple(params), tbl)


@st.cache_data(ttl=600, show_spinner="Loading match timeline...")
def _load_match_timeline_query(where: str, params: tuple[Any, ...], tbl: str) -> Any:
    """Execute the match timeline query with pre-built WHERE clause."""
    return execute_query(
        f"SELECT time_seconds, period, minute, second, "  # noqa: S608
        f"  action_type, action_result, vaep_value, "
        f"  offensive_value, defensive_value, player_id "
        f"FROM {tbl} WHERE {where} "
        f"ORDER BY period, time_seconds "
        f"LIMIT 2000",
        params,
    )


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
    return _load_match_timeline_query(where, tuple(params), tbl)


@st.cache_data(ttl=600, show_spinner="Loading players...")
def _load_player_options(comp: int, team: int, tbl: str, players_tbl: str) -> Any:
    """Load player options for a team within a competition.

    Uses a recursive CTE to gather distinct player_ids from the fact table
    (avoids the full sequential scan that SELECT DISTINCT forces), then
    joins to the dimension table for display names.
    """
    return execute_query(
        f"WITH RECURSIVE dp AS ("  # noqa: S608
        f"  SELECT MIN(player_id) AS player_id FROM {tbl}"
        f"  WHERE competition_id = %s AND team_id = %s"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(player_id) FROM {tbl}"
        f"          WHERE competition_id = %s AND team_id = %s AND player_id > dp.player_id)"
        f"  FROM dp WHERE dp.player_id IS NOT NULL"
        f") "
        f"SELECT dp.player_id, p.player_display_name "
        f"FROM dp "
        f"JOIN {players_tbl} p ON dp.player_id = p.player_id "
        f"WHERE dp.player_id IS NOT NULL "
        f"ORDER BY p.player_display_name",
        (comp, team, comp, team),
    )


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


def page() -> None:
    """Render the Action Values page."""
    st.header(":material/trending_up: Action Values (VAEP)")
    st.caption(
        "Valuing Actions by Estimating Probabilities — "
        "[Decroos et al. (2019)](https://doi.org/10.1007/s10994-021-05989-6). "
        "Implemented via [socceraction](https://github.com/ML-KULeuven/socceraction)."
    )

    with st.sidebar:
        view = st.radio(
            "View",
            ["Player VAEP Rankings", "Action Type Breakdown", "Match Action Timeline"],
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
        empty_select("a competition")
        return

    rankings = _load_vaep_rankings(int(competition_id), int(min_minutes))
    if rankings.empty:
        empty_result("VAEP data")
        return

    st.caption(
        "VAEP/90: higher = more impactful. Off. VAEP/90: offensive contribution. "
        "Def. VAEP/90: defensive contribution. Values typically range 0.01-1.0."
    )
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
        empty_select("a competition")
        return

    # Simple player filter via team's players
    player_id: int | None = None
    if team_id is not None:
        tbl = t("fct_action_values_synced")
        players_tbl = t("dim_players_synced")

        player_opts = _load_player_options(int(competition_id), int(team_id), tbl, players_tbl)
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
        empty_result("VAEP data")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        fig = plot_action_type_breakdown(breakdown, title="VAEP by Action Type")
        st.pyplot(fig)

    with col_stats:
        total_vaep = float(breakdown["total_vaep"].sum())
        total_actions = int(breakdown["action_count"].sum())
        top_type = str(breakdown.iloc[0]["action_type"]) if not breakdown.empty else "N/A"

        st.metric("Total VAEP", f"{total_vaep:.2f}", help=METRIC_HELP.get("Total VAEP") or None)
        st.metric("Total Actions", total_actions, help=METRIC_HELP.get("Total Actions"))
        st.metric("Top Action Type", top_type, help=METRIC_HELP.get("Top Action Type"))


def _render_timeline() -> None:
    """Render the match action timeline view."""
    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)

    if competition_id is None:
        empty_select("a competition")
        return

    if match_id is None:
        empty_select("a match")
        return

    actions = _load_match_timeline(match_id, team_id)
    if actions.empty:
        empty_result("VAEP data")
        return

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        fig = plot_action_value_timeline(actions, title="Match Action Value Timeline")
        st.pyplot(fig)

    with col_stats:
        positive = int((actions["vaep_value"] > 0).sum())
        negative = int((actions["vaep_value"] < 0).sum())
        net_vaep = float(actions["vaep_value"].sum())

        st.metric(
            "Positive Actions",
            positive,
            help=METRIC_HELP.get("Positive Actions") or None,
        )
        st.metric(
            "Negative Actions",
            negative,
            help=METRIC_HELP.get("Negative Actions") or None,
        )
        st.metric(
            "Net Match VAEP",
            f"{net_vaep:.3f}",
            help=METRIC_HELP.get("Net Match VAEP") or None,
        )

        # Most valuable action
        if not actions.empty:
            best_idx = actions["vaep_value"].idxmax()
            best = actions.loc[best_idx]
            st.metric(
                "Most Valuable Action",
                f"{best['action_type']} ({best['vaep_value']:.3f})",
                help=METRIC_HELP.get("Most Valuable Action"),
            )

    with st.expander("Action Details", icon=":material/table_chart:"):
        st.dataframe(actions.drop(columns=["player_id"], errors="ignore"), use_container_width=True)
