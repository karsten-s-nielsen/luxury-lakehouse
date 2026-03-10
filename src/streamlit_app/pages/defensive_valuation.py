"""Defensive Pressure page -- DEFCON-lite attacker pressure rankings and breakdown."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_rankings(competition_id: int, team_id: int | None) -> Any:
    """Load top attackers by total defensive pressure received for a competition."""
    tbl = t("fct_defcon_pressure_synced")
    dim_p = t("dim_players_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading rankings...")
    def _query(comp_id: int, t_id: int | None) -> Any:
        av_tbl = t("fct_action_values_synced")
        if t_id is not None:
            # Recursive CTE collects distinct player_ids from the action-values fact table
            # without a sequential scan, then filters pressure rows to those players.
            return execute_query(
                f"WITH RECURSIVE team_players AS ("  # noqa: S608
                f"  SELECT MIN(player_id) AS player_id FROM {av_tbl}"
                f"  WHERE competition_id = %s AND team_id = %s"
                f"  UNION ALL"
                f"  SELECT (SELECT MIN(player_id) FROM {av_tbl}"
                f"          WHERE competition_id = %s AND team_id = %s AND player_id > team_players.player_id)"
                f"  FROM team_players WHERE team_players.player_id IS NOT NULL"
                f") "
                f"SELECT dp.player_id, p.player_display_name, "
                f"  SUM(dp.total_pressure) as total_pressure, "
                f"  SUM(dp.total_defensive_actions) as total_actions, "
                f"  SUM(dp.intercept_count) as intercepts, "
                f"  SUM(dp.concede_count) as concedes, "
                f"  SUM(dp.disturb_count) as disturbs, "
                f"  SUM(dp.deter_count) as deters, "
                f"  COUNT(DISTINCT dp.match_id) as matches "
                f"FROM {tbl} dp "
                f"JOIN {dim_p} p ON dp.player_id = p.player_id "
                f"JOIN team_players tp ON tp.player_id = dp.player_id "
                f"WHERE dp.competition_id = %s "
                f"GROUP BY dp.player_id, p.player_display_name "
                f"ORDER BY total_pressure DESC "
                f"LIMIT 50",
                (comp_id, t_id, comp_id, t_id, comp_id),
            )
        return execute_query(
            f"SELECT dp.player_id, p.player_display_name, "  # noqa: S608
            f"  SUM(dp.total_pressure) as total_pressure, "
            f"  SUM(dp.total_defensive_actions) as total_actions, "
            f"  SUM(dp.intercept_count) as intercepts, "
            f"  SUM(dp.concede_count) as concedes, "
            f"  SUM(dp.disturb_count) as disturbs, "
            f"  SUM(dp.deter_count) as deters, "
            f"  COUNT(DISTINCT dp.match_id) as matches "
            f"FROM {tbl} dp "
            f"JOIN {dim_p} p ON dp.player_id = p.player_id "
            f"WHERE dp.competition_id = %s "
            f"GROUP BY dp.player_id, p.player_display_name "
            f"ORDER BY total_pressure DESC "
            f"LIMIT 50",
            (comp_id,),
        )

    return _query(int(competition_id), int(team_id) if team_id is not None else None)


def _load_pressure_breakdown(player_id: int, competition_id: int, team_id: int | None) -> Any:
    """Load per-match pressure breakdown for a specific attacker."""
    tbl = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading breakdown...")
    def _query(pid: int, comp_id: int, t_id: int | None) -> Any:
        conditions = ["dp.player_id = %s", "dp.competition_id = %s"]
        params: list[Any] = [pid, comp_id]

        if t_id is not None:
            conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
            params.extend([t_id, t_id])

        where = " AND ".join(conditions)
        return execute_query(
            f"SELECT dp.match_id, "  # noqa: S608
            f"  ms.home_team_name || ' v ' || ms.away_team_name as match_label, "
            f"  dp.intercept_pressure, dp.concede_pressure, "
            f"  dp.disturb_pressure, dp.deter_pressure, "
            f"  dp.total_pressure, dp.total_defensive_actions "
            f"FROM {tbl} dp "
            f"LEFT JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
            f"WHERE {where} "
            f"ORDER BY dp.match_id",
            tuple(params),
        )

    return _query(int(player_id), int(competition_id), int(team_id) if team_id is not None else None)


def _load_player_matches(player_id: int, competition_id: int, team_id: int | None) -> Any:
    """Load matches where an attacker has DEFCON pressure data, for the match dropdown."""
    tbl = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query(pid: int, comp_id: int, t_id: int | None) -> Any:
        conditions = ["dp.player_id = %s", "dp.competition_id = %s"]
        params: list[Any] = [pid, comp_id]

        if t_id is not None:
            conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
            params.extend([t_id, t_id])

        where = " AND ".join(conditions)
        return execute_query(
            f"SELECT dp.match_id, "  # noqa: S608
            f"  MAX(ms.match_date) as match_date, "
            f"  MAX(ms.home_team_name) as home_team_name, "
            f"  MAX(ms.away_team_name) as away_team_name, "
            f"  MAX(ms.home_score) as home_score, "
            f"  MAX(ms.away_score) as away_score "
            f"FROM {tbl} dp "
            f"LEFT JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
            f"WHERE {where} "
            f"GROUP BY dp.match_id "
            f"ORDER BY MAX(ms.match_date) DESC",
            tuple(params),
        )

    return _query(int(player_id), int(competition_id), int(team_id) if team_id is not None else None)


def _load_match_timeline(match_id: str, player_id: int) -> Any:
    """Load per-action credits for a specific player in a specific match."""
    tbl = t("fct_defcon_actions_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading timeline...")
    def _query(mid: str, pid: int) -> Any:
        return execute_query(
            f"SELECT da.event_id, da.player_id as opposing_player_id, "  # noqa: S608
            f"  da.credit_type, da.confidence, da.defcon_value, "
            f"  da.action_type, da.action_x, da.action_y, "
            f"  da.dist_to_ball "
            f"FROM {tbl} da "
            f"WHERE da.match_id = %s AND da.action_player_id = %s "
            f"ORDER BY da.event_id",
            (mid, pid),
        )

    return _query(str(match_id), int(player_id))


def _render_pressure_competition_filter() -> int | None:
    """Render competition selectbox filtered to those with pressure data."""
    dp = t("fct_defcon_pressure_synced")
    dc = t("dim_competitions_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query() -> Any:
        return execute_query(
            f"SELECT DISTINCT c.competition_id, c.competition_name, c.country "  # noqa: S608
            f"FROM {dc} c "
            f"JOIN {dp} dp ON dp.competition_id = c.competition_id "
            f"ORDER BY c.country, c.competition_name",
        )

    df = _query()
    if df is None or len(df) == 0:
        st.warning("No competitions with DEFCON-lite data found.")
        return None

    options = df.to_dict("records")
    labels = [f"{r['country']} — {r['competition_name']}" for r in options]

    idx = st.selectbox("Competition", range(len(labels)), format_func=lambda i: labels[i])
    if idx is None:
        return None
    return options[idx]["competition_id"]  # type: ignore[return-value]


def _render_optional_team_filter(competition_id: int | None) -> int | None:
    """Render team selectbox with 'All teams' default."""
    if competition_id is None:
        return None

    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    dim_t = t("dim_teams_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query(comp_id: int) -> Any:
        return execute_query(
            f"WITH RECURSIVE pressure_matches AS ("  # noqa: S608
            f"  SELECT MIN(match_id)::bigint AS match_id FROM {dp} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(match_id)::bigint FROM {dp}"
            f"          WHERE competition_id = %s AND match_id::bigint > pressure_matches.match_id)"
            f"  FROM pressure_matches WHERE pressure_matches.match_id IS NOT NULL"
            f") "
            f"SELECT DISTINCT dt.team_id, dt.team_name "
            f"FROM {dim_t} dt "
            f"JOIN {ms} ms"
            f"  ON ms.home_team_id = dt.team_id OR ms.away_team_id = dt.team_id "
            f"JOIN pressure_matches pm ON pm.match_id = ms.match_id "
            f"ORDER BY dt.team_name",
            (comp_id, comp_id),
        )

    teams = _query(int(competition_id))
    if teams is None or len(teams) == 0:
        return None

    options = teams.to_dict("records")
    labels = [r["team_name"] for r in options]

    idx = st.selectbox(
        "Team",
        [None, *range(len(labels))],
        format_func=lambda i: "All teams" if i is None else labels[i],  # type: ignore[arg-type]
    )
    if idx is None:
        return None
    return options[idx]["team_id"]  # type: ignore[return-value]


def _build_player_options(rankings: pd.DataFrame) -> dict[str, int]:
    """Build player name → id mapping from rankings DataFrame."""
    return {str(row["player_display_name"]): int(row["player_id"]) for _, row in rankings.iterrows()}


def _load_breakdown_player_ids(competition_id: int, team_id: int | None) -> set[int]:
    """Return player_ids that have pressure breakdown rows for the given filters.

    Without a team filter: recursive CTE avoids a full sequential scan on the fact table.
    With a team filter: GROUP BY on the join result (DISTINCT would force a seq scan after JOIN).
    """
    tbl = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query(comp_id: int, t_id: int | None) -> Any:
        if t_id is not None:
            return execute_query(
                f"SELECT dp.player_id "  # noqa: S608
                f"FROM {tbl} dp "
                f"JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
                f"WHERE dp.competition_id = %s "
                f"AND (ms.home_team_id = %s OR ms.away_team_id = %s) "
                f"GROUP BY dp.player_id",
                (comp_id, t_id, t_id),
            )
        return execute_query(
            f"WITH RECURSIVE dp_players AS ("  # noqa: S608
            f"  SELECT MIN(player_id) AS player_id FROM {tbl} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(player_id) FROM {tbl}"
            f"          WHERE competition_id = %s AND player_id > dp_players.player_id)"
            f"  FROM dp_players WHERE dp_players.player_id IS NOT NULL"
            f") SELECT player_id FROM dp_players WHERE player_id IS NOT NULL",
            (comp_id, comp_id),
        )

    result = _query(int(competition_id), int(team_id) if team_id is not None else None)
    if result is None or len(result) == 0:
        return set()
    return set(int(x) for x in result["player_id"])


def _load_timeline_player_ids(competition_id: int, team_id: int | None) -> set[int]:
    """Return action_player_ids that have DEFCON action rows for the given filters.

    Without a team filter: recursive CTE avoids a full sequential scan on the fact table.
    With a team filter: GROUP BY on the join result (DISTINCT would force a seq scan after JOIN).
    """
    tbl = t("fct_defcon_actions_synced")
    ms = t("fct_match_summary_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query(comp_id: int, t_id: int | None) -> Any:
        if t_id is not None:
            return execute_query(
                f"SELECT da.action_player_id as player_id "  # noqa: S608
                f"FROM {tbl} da "
                f"JOIN {ms} ms ON da.match_id::bigint = ms.match_id "
                f"WHERE da.competition_id = %s "
                f"AND (ms.home_team_id = %s OR ms.away_team_id = %s) "
                f"GROUP BY da.action_player_id",
                (comp_id, t_id, t_id),
            )
        return execute_query(
            f"WITH RECURSIVE da_players AS ("  # noqa: S608
            f"  SELECT MIN(action_player_id) AS player_id FROM {tbl} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(action_player_id) FROM {tbl}"
            f"          WHERE competition_id = %s AND action_player_id > da_players.player_id)"
            f"  FROM da_players WHERE da_players.player_id IS NOT NULL"
            f") SELECT player_id FROM da_players WHERE player_id IS NOT NULL",
            (comp_id, comp_id),
        )

    result = _query(int(competition_id), int(team_id) if team_id is not None else None)
    if result is None or len(result) == 0:
        return set()
    return set(int(x) for x in result["player_id"])


def page() -> None:
    """Render the Defensive Pressure page."""
    st.header(":material/shield: Defensive Pressure (DEFCON-lite)")
    st.caption(
        "How much defensive attention does each attacker attract? "
        "Tier 3 tabular approximation of Kim et al. (2025). "
        "Credits: Intercept, Concede, Disturb, Deter."
    )

    with st.sidebar:
        competition_id = _render_pressure_competition_filter()
        team_id = _render_optional_team_filter(competition_id)

    if competition_id is None:
        st.info("Select a competition to begin.")
        return

    rankings = _load_rankings(competition_id, team_id)
    if rankings is None or len(rankings) == 0:
        st.info("No DEFCON-lite data available for this competition.")
        return

    tab_rankings, tab_breakdown, tab_timeline = st.tabs(["Pressure Rankings", "Pressure Breakdown", "Match Timeline"])

    with tab_rankings:
        st.dataframe(rankings, use_container_width=True, hide_index=True)

    player_options = _build_player_options(rankings)

    with tab_breakdown:
        bd_pids = _load_breakdown_player_ids(competition_id, team_id)
        bd_options = {k: v for k, v in player_options.items() if v in bd_pids}
        if not bd_options:
            st.info("No pressure breakdown data available for the selected filters.")
        else:
            selected_name = st.selectbox("Select player", list(bd_options.keys()), key="breakdown_player")
            if selected_name:
                player_id = bd_options[selected_name]
                breakdown = _load_pressure_breakdown(player_id, competition_id, team_id)
                if breakdown is None or len(breakdown) == 0:
                    st.info("No breakdown data for this player in the selected team's matches.")
                else:
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Intercept", f"{breakdown['intercept_pressure'].sum():.2f}")
                    col2.metric("Concede", f"{breakdown['concede_pressure'].sum():.2f}")
                    col3.metric("Disturb", f"{breakdown['disturb_pressure'].sum():.2f}")
                    col4.metric("Deter", f"{breakdown['deter_pressure'].sum():.2f}")

                    label_col = "match_label" if breakdown["match_label"].notna().all() else "match_id"
                    fig = px.bar(
                        breakdown,
                        x=label_col,
                        y=["intercept_pressure", "concede_pressure", "disturb_pressure", "deter_pressure"],
                        title=f"Pressure Breakdown: {selected_name}",
                        barmode="group",
                        labels={
                            "value": "Pressure Value",
                            "variable": "Credit Type",
                            label_col: "Match",
                        },
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)

    with tab_timeline:
        tl_pids = _load_timeline_player_ids(competition_id, team_id)
        tl_options = {k: v for k, v in player_options.items() if v in tl_pids}
        if not tl_options:
            st.info("No match timeline data available for the selected filters.")
        else:
            selected_tl_name = st.selectbox("Select player", list(tl_options.keys()), key="timeline_player")
            if selected_tl_name:
                tl_player_id = tl_options[selected_tl_name]

                matches = _load_player_matches(tl_player_id, competition_id, team_id)
                if matches is None or len(matches) == 0:
                    st.info("No match-level data for this player in the selected team's matches.")
                else:
                    match_options = matches.to_dict("records")
                    match_labels = [
                        (
                            f"{r['match_date']} — {r['home_team_name']}"
                            f" {r['home_score']}-{r['away_score']} {r['away_team_name']}"
                        )
                        if r.get("match_date") is not None
                        else str(r["match_id"])
                        for r in match_options
                    ]
                    match_idx = st.selectbox("Match", range(len(match_labels)), format_func=lambda i: match_labels[i])
                    if match_idx is not None:
                        match_id = str(match_options[match_idx]["match_id"])
                        timeline = _load_match_timeline(match_id, tl_player_id)
                        if timeline is not None and len(timeline) > 0:
                            st.dataframe(timeline, use_container_width=True, hide_index=True)
                        else:
                            st.info("No DEFCON-lite actions for this match.")
