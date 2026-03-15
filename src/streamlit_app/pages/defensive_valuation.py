"""Defensive Pressure page -- DEFCON-lite attacker pressure rankings and breakdown."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from streamlit_app.components.feedback import data_scope_note, empty_result, empty_select
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.db import execute_query, t


@st.cache_data(ttl=600, show_spinner="Loading rankings...")
def _fetch_pressure_rankings(tbl: str, dim_p: str, av_tbl: str, comp_id: int, t_id: int | None) -> Any:
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


def _load_rankings(competition_id: int, team_id: int | None) -> Any:
    """Load top attackers by total defensive pressure received for a competition."""
    tbl = t("fct_defcon_pressure_synced")
    dim_p = t("dim_players_synced")
    av_tbl = t("fct_action_values_synced")
    return _fetch_pressure_rankings(
        tbl, dim_p, av_tbl, int(competition_id), int(team_id) if team_id is not None else None
    )


@st.cache_data(ttl=600, show_spinner="Loading breakdown...")
def _fetch_pressure_breakdown(tbl: str, ms: str, pid: int, comp_id: int, t_id: int | None) -> Any:
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
        f"ORDER BY dp.match_id "
        f"LIMIT 200",
        tuple(params),
    )


def _load_pressure_breakdown(player_id: int, competition_id: int, team_id: int | None) -> Any:
    """Load per-match pressure breakdown for a specific attacker."""
    tbl = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    return _fetch_pressure_breakdown(
        tbl, ms, int(player_id), int(competition_id), int(team_id) if team_id is not None else None
    )


@st.cache_data(ttl=600, show_spinner="Loading matches...")
def _fetch_player_defcon_matches(tbl: str, ms: str, pid: int, comp_id: int, t_id: int | None) -> Any:
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
        f"ORDER BY MAX(ms.match_date) DESC "
        f"LIMIT 200",
        tuple(params),
    )


def _load_player_matches(player_id: int, competition_id: int, team_id: int | None) -> Any:
    """Load matches where an attacker has DEFCON pressure data, for the match dropdown."""
    tbl = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    return _fetch_player_defcon_matches(
        tbl, ms, int(player_id), int(competition_id), int(team_id) if team_id is not None else None
    )


@st.cache_data(ttl=600, show_spinner="Loading timeline...")
def _fetch_match_timeline(tbl: str, mid: str, pid: int) -> Any:
    return execute_query(
        f"SELECT da.event_id, da.player_id as opposing_player_id, "  # noqa: S608
        f"  da.credit_type, da.confidence, da.defcon_value, "
        f"  da.action_type, da.action_x, da.action_y, "
        f"  da.dist_to_ball "
        f"FROM {tbl} da "
        f"WHERE da.match_id = %s AND da.action_player_id = %s "
        f"ORDER BY da.event_id "
        f"LIMIT 2000",
        (mid, pid),
    )


def _load_match_timeline(match_id: str, player_id: int) -> Any:
    """Load per-action credits for a specific player in a specific match."""
    tbl = t("fct_defcon_actions_synced")
    return _fetch_match_timeline(tbl, str(match_id), int(player_id))


@st.cache_data(ttl=600, show_spinner="Loading competitions...")
def _fetch_pressure_competitions(dp: str, dc: str) -> Any:
    return execute_query(
        f"SELECT DISTINCT c.competition_id, c.competition_name, c.country "  # noqa: S608
        f"FROM {dc} c "
        f"JOIN {dp} dp ON dp.competition_id = c.competition_id "
        f"ORDER BY c.country, c.competition_name",
    )


def _render_pressure_competition_filter() -> int | None:
    """Render competition selectbox filtered to those with pressure data."""
    dp = t("fct_defcon_pressure_synced")
    dc = t("dim_competitions_synced")

    df = _fetch_pressure_competitions(dp, dc)
    if df is None or len(df) == 0:
        empty_result("competitions with defensive pressure data")
        return None

    options = df.to_dict("records")
    labels = [f"{r['country']} — {r['competition_name']}" for r in options]

    idx = st.selectbox("Competition", range(len(labels)), format_func=lambda i: labels[i])
    if idx is None:
        return None
    return options[idx]["competition_id"]  # type: ignore[return-value]


@st.cache_data(ttl=600, show_spinner="Loading teams...")
def _fetch_pressure_teams(dp: str, ms: str, dim_t: str, comp_id: int) -> Any:
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


def _render_optional_team_filter(competition_id: int | None) -> int | None:
    """Render team selectbox with 'All teams' default."""
    if competition_id is None:
        return None

    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    dim_t = t("dim_teams_synced")

    teams = _fetch_pressure_teams(dp, ms, dim_t, int(competition_id))
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


@st.cache_data(ttl=600, show_spinner="Loading players...")
def _fetch_breakdown_player_ids(tbl: str, ms: str, comp_id: int, t_id: int | None) -> Any:
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


def _load_breakdown_player_ids(competition_id: int, team_id: int | None) -> set[int]:
    """Return player_ids that have pressure breakdown rows for the given filters.

    Without a team filter: recursive CTE avoids a full sequential scan on the fact table.
    With a team filter: GROUP BY on the join result (DISTINCT would force a seq scan after JOIN).
    """
    tbl = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    result = _fetch_breakdown_player_ids(tbl, ms, int(competition_id), int(team_id) if team_id is not None else None)
    if result is None or len(result) == 0:
        return set()
    return set(int(x) for x in result["player_id"])


@st.cache_data(ttl=600, show_spinner="Loading players...")
def _fetch_timeline_player_ids(tbl: str, ms: str, comp_id: int, t_id: int | None) -> Any:
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


def _load_timeline_player_ids(competition_id: int, team_id: int | None) -> set[int]:
    """Return action_player_ids that have DEFCON action rows for the given filters.

    Without a team filter: recursive CTE avoids a full sequential scan on the fact table.
    With a team filter: GROUP BY on the join result (DISTINCT would force a seq scan after JOIN).
    """
    tbl = t("fct_defcon_actions_synced")
    ms = t("fct_match_summary_synced")
    result = _fetch_timeline_player_ids(tbl, ms, int(competition_id), int(team_id) if team_id is not None else None)
    if result is None or len(result) == 0:
        return set()
    return set(int(x) for x in result["player_id"])


def page() -> None:
    """Render the Defensive Pressure page."""
    st.header(":material/shield: Defensive Impact")
    st.caption(
        "How much defensive attention does each attacker attract? "
        "Tier 3 (tabular heuristic, no GNN) approximation of "
        "[Kim et al. (2025)](https://github.com/hyunsungkim-ds/defcon) DEFCON framework. "
        "Credits: Intercept, Concede, Disturb, Deter. "
        "Tiers: 1 = full GNN, 2 = simplified GNN, 3 = tabular heuristic (this implementation)."
    )
    data_scope_note("Requires StatsBomb 360 freeze-frame data (323 of 380+ matches).")

    with st.sidebar:
        competition_id = _render_pressure_competition_filter()
        team_id = _render_optional_team_filter(competition_id)

    if competition_id is None:
        empty_select("a competition")
        return

    rankings = _load_rankings(competition_id, team_id)
    if rankings is None or len(rankings) == 0:
        empty_result(
            "defensive pressure data",
            scope_hint="Requires StatsBomb 360 freeze-frame data (323 of 380+ matches).",
        )
        return

    tab_rankings, tab_breakdown, tab_timeline = st.tabs(["Pressure Rankings", "Pressure Breakdown", "Match Timeline"])

    with tab_rankings:
        # Hide internal player_id column (H4); rename attacker-perspective columns (F7)
        display_cols = [c for c in rankings.columns if c != "player_id"]
        rename_map = {
            "player_display_name": "Player",
            "total_pressure": "Total Pressure",
            "total_actions": "Actions Faced",
            "intercepts": "Intercepted",
            "concedes": "Shots Conceded",
            "disturbs": "Disturbed",
            "deters": "Deterred",
            "matches": "Matches",
        }
        st.dataframe(
            rankings[display_cols].rename(columns=rename_map),
            use_container_width=True,
            hide_index=True,
        )

    player_options = _build_player_options(rankings)

    with tab_breakdown:
        bd_pids = _load_breakdown_player_ids(competition_id, team_id)
        bd_options = {k: v for k, v in player_options.items() if v in bd_pids}
        if not bd_options:
            empty_result("pressure breakdown data")
        else:
            selected_name = st.selectbox("Player", list(bd_options.keys()), key="breakdown_player")
            if selected_name:
                player_id = bd_options[selected_name]
                breakdown = _load_pressure_breakdown(player_id, competition_id, team_id)
                if breakdown is None or len(breakdown) == 0:
                    empty_result("breakdown data for this player")
                else:
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric(
                        "Intercept",
                        f"{breakdown['intercept_pressure'].sum():.2f}",
                        help=METRIC_HELP.get("Intercept") or None,
                    )
                    col2.metric(
                        "Concede",
                        f"{breakdown['concede_pressure'].sum():.2f}",
                        help=METRIC_HELP.get("Concede") or None,
                    )
                    col3.metric(
                        "Disturb",
                        f"{breakdown['disturb_pressure'].sum():.2f}",
                        help=METRIC_HELP.get("Disturb") or None,
                    )
                    col4.metric(
                        "Deter",
                        f"{breakdown['deter_pressure'].sum():.2f}",
                        help=METRIC_HELP.get("Deter") or None,
                    )

                    label_col = "match_label" if breakdown["match_label"].notna().all() else "match_id"
                    # Limit to 10 matches to prevent bar slivers (M13)
                    plot_data = breakdown.head(10) if len(breakdown) > 10 else breakdown
                    if len(breakdown) > 10:
                        st.caption(f"Showing top 10 of {len(breakdown)} matches.")
                    fig = px.bar(
                        plot_data,
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
            empty_result("match timeline data")
        else:
            selected_tl_name = st.selectbox("Player", list(tl_options.keys()), key="timeline_player")
            if selected_tl_name:
                tl_player_id = tl_options[selected_tl_name]

                matches = _load_player_matches(tl_player_id, competition_id, team_id)
                if matches is None or len(matches) == 0:
                    empty_result("match-level data for this player")
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
                            display_cols = [c for c in timeline.columns if c not in ("opposing_player_id", "event_id")]
                            tl_rename = {
                                "credit_type": "Credit Type",
                                "confidence": "Confidence (0-1)",
                                "defcon_value": "DEFCON Value",
                                "action_type": "Action",
                                "action_x": "Pitch X (m)",
                                "action_y": "Pitch Y (m)",
                                "dist_to_ball": "Dist to Ball (m)",
                            }
                            st.dataframe(
                                timeline[display_cols].rename(columns=tl_rename),
                                use_container_width=True,
                                hide_index=True,
                            )
                        else:
                            empty_result("defensive actions for this match")
