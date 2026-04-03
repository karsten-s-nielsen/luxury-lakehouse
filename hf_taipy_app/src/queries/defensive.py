"""Defensive queries — DEFCON/VAEP, extracted from state/defensive_valuation.py and state/action_values.py.

All functions return pd.DataFrame or typed containers. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Action Values (VAEP) queries (from state/action_values.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_vaep_rankings(comp_id: int, min_min: int) -> pd.DataFrame:
    """Fetch VAEP player rankings with percentile data when available.

    LEFT JOINs fct_player_percentiles_synced for vaep_per_90_pctile.
    Gracefully degrades if the percentile table doesn't exist yet.

    Expected columns: player_id, player_display_name, position_group,
    minutes_played, total_vaep, vaep_per_90, offensive_vaep_per_90,
    defensive_vaep_per_90, total_actions, vaep_per_90_pctile (optional).
    """
    ps_tbl = t("fct_player_stats_synced")
    dim_tbl = t("dim_players_synced")

    try:
        pctile_tbl = t("fct_player_percentiles_synced")
        return execute_query(
            f"SELECT ps.player_id, p.player_display_name, p.position_group, "  # noqa: S608
            f"  ps.minutes_played, ps.total_vaep, ps.vaep_per_90, "
            f"  ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
            f"  ps.total_actions, "
            f"  pct.vaep_per_90_pctile "
            f"FROM {ps_tbl} ps "
            f"JOIN {dim_tbl} p ON ps.player_id = p.player_id "
            f"LEFT JOIN {pctile_tbl} pct "
            f"  ON ps.player_id::text = pct.player_id "
            f"  AND ps.competition_id = pct.competition_id "
            f"  AND ps.season_id = pct.season_id "
            f"WHERE ps.competition_id = %s "
            f"  AND ps.minutes_played >= %s "
            f"  AND ps.vaep_per_90 IS NOT NULL "
            f"ORDER BY ps.vaep_per_90 DESC "
            f"LIMIT 500",
            (comp_id, min_min),
        )
    except Exception:
        logger.debug("Percentile join failed (table may not exist); falling back to base query")
        return execute_query(
            f"SELECT ps.player_id, p.player_display_name, p.position_group, "  # noqa: S608
            f"  ps.minutes_played, ps.total_vaep, ps.vaep_per_90, "
            f"  ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
            f"  ps.total_actions "
            f"FROM {ps_tbl} ps "
            f"JOIN {dim_tbl} p ON ps.player_id = p.player_id "
            f"WHERE ps.competition_id = %s "
            f"  AND ps.minutes_played >= %s "
            f"  AND ps.vaep_per_90 IS NOT NULL "
            f"ORDER BY ps.vaep_per_90 DESC "
            f"LIMIT 500",
            (comp_id, min_min),
        )


@ttl_cache()
def fetch_vaep_breakdown(
    comp_id: int,
    team_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch VAEP breakdown by action type with dynamic filters.

    Expected columns: action_type, total_vaep, total_offensive,
    total_defensive, action_count.
    """
    conditions = ["competition_id = %s"]
    params: list[Any] = [comp_id]

    if team_id is not None:
        conditions.append("team_id = %s")
        params.append(int(team_id))

    if player_id is not None:
        conditions.append("player_id = %s")
        params.append(int(player_id))

    where = " AND ".join(conditions)
    av_tbl = t("fct_action_values_synced")
    return execute_query(
        f"SELECT action_type, "  # noqa: S608
        f"  sum(vaep_value) AS total_vaep, "
        f"  sum(offensive_value) AS total_offensive, "
        f"  sum(defensive_value) AS total_defensive, "
        f"  count(*) AS action_count "
        f"FROM {av_tbl} WHERE {where} "
        f"GROUP BY action_type "
        f"ORDER BY sum(vaep_value) DESC "
        f"LIMIT 50",
        tuple(params),
    )


@ttl_cache()
def fetch_vaep_timeline(match_id: int, team_id: int | None) -> pd.DataFrame:
    """Fetch action values for a specific match.

    Expected columns: time_seconds, period, minute, second, action_type,
    action_result, vaep_value, offensive_value, defensive_value, player_id.
    """
    conditions = ["match_id = %s"]
    params: list[Any] = [match_id]

    if team_id is not None:
        conditions.append("team_id = %s")
        params.append(int(team_id))

    where = " AND ".join(conditions)
    av_tbl = t("fct_action_values_synced")
    return execute_query(
        f"SELECT time_seconds, period, minute, second, "  # noqa: S608
        f"  action_type, action_result, vaep_value, "
        f"  offensive_value, defensive_value, player_id "
        f"FROM {av_tbl} WHERE {where} "
        f"ORDER BY period, time_seconds "
        f"LIMIT 2000",
        tuple(params),
    )


# ---------------------------------------------------------------------------
# DEFCON queries (from state/defensive_valuation.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_pressure_competitions() -> pd.DataFrame:
    """Load competitions with DEFCON pressure data.

    Uses recursive CTE loose index scan to avoid SELECT DISTINCT sequential scan.

    Expected columns: competition_id, competition_name, country.
    """
    dp = t("fct_defcon_pressure_synced")
    dc = t("dim_competitions_synced")
    return execute_query(
        f"WITH RECURSIVE pc AS ("  # noqa: S608
        f"  SELECT MIN(competition_id) AS competition_id FROM {dp}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(competition_id) FROM {dp}"
        f"          WHERE competition_id > pc.competition_id)"
        f"  FROM pc WHERE pc.competition_id IS NOT NULL"
        f") SELECT pc.competition_id, c.competition_name, c.country "
        f"FROM pc "
        f"JOIN {dc} c ON pc.competition_id = c.competition_id "
        f"WHERE pc.competition_id IS NOT NULL "
        f"ORDER BY c.country, c.competition_name",
    )


@ttl_cache()
def fetch_pressure_teams(comp_id: int) -> pd.DataFrame:
    """Load teams with DEFCON pressure data in a competition.

    Recursive CTE for distinct match_ids, then join to match summary for teams.

    Expected columns: team_id, team_name.
    """
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")
    dim_t = t("dim_teams_synced")
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


@ttl_cache()
def fetch_pressure_rankings(comp_id: int, team_id: int | None) -> pd.DataFrame:
    """Ranked players by total defensive pressure received.

    With team filter: recursive CTE collects distinct player_ids from action_values
    for the team, then filters pressure rows to those players.
    Without team filter: direct aggregate on the pressure table.

    Expected columns: player_id, player_display_name, total_pressure,
    total_actions, intercepts, concedes, disturbs, deters, matches.
    """
    dp = t("fct_defcon_pressure_synced")
    dim_p = t("dim_players_synced")
    av_tbl = t("fct_action_values_synced")

    if team_id is not None:
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
            f"FROM {dp} dp "
            f"JOIN {dim_p} p ON dp.player_id = p.player_id "
            f"JOIN team_players tp ON tp.player_id = dp.player_id "
            f"WHERE dp.competition_id = %s "
            f"GROUP BY dp.player_id, p.player_display_name "
            f"ORDER BY total_pressure DESC "
            f"LIMIT 50",
            (comp_id, team_id, comp_id, team_id, comp_id),
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
        f"FROM {dp} dp "
        f"JOIN {dim_p} p ON dp.player_id = p.player_id "
        f"WHERE dp.competition_id = %s "
        f"GROUP BY dp.player_id, p.player_display_name "
        f"ORDER BY total_pressure DESC "
        f"LIMIT 50",
        (comp_id,),
    )


@ttl_cache()
def fetch_pressure_breakdown(pid: int, comp_id: int, team_id: int | None) -> pd.DataFrame:
    """Per-match pressure breakdown for a specific attacker.

    Expected columns: match_id, match_label, intercept_pressure,
    concede_pressure, disturb_pressure, deter_pressure, total_pressure,
    total_defensive_actions.
    """
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    conditions = ["dp.player_id = %s", "dp.competition_id = %s"]
    params: list[Any] = [pid, comp_id]

    if team_id is not None:
        conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
        params.extend([team_id, team_id])

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT dp.match_id, "  # noqa: S608
        f"  ms.home_team_name || ' v ' || ms.away_team_name as match_label, "
        f"  dp.intercept_pressure, dp.concede_pressure, "
        f"  dp.disturb_pressure, dp.deter_pressure, "
        f"  dp.total_pressure, dp.total_defensive_actions "
        f"FROM {dp} dp "
        f"LEFT JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
        f"WHERE {where} "
        f"ORDER BY dp.match_id "
        f"LIMIT 200",
        tuple(params),
    )


@ttl_cache()
def fetch_player_defcon_matches(pid: int, comp_id: int, team_id: int | None) -> pd.DataFrame:
    """Matches where an attacker has DEFCON pressure data (for match dropdown).

    Expected columns: match_id, match_date, home_team_name, away_team_name,
    home_score, away_score.
    """
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    conditions = ["dp.player_id = %s", "dp.competition_id = %s"]
    params: list[Any] = [pid, comp_id]

    if team_id is not None:
        conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
        params.extend([team_id, team_id])

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT dp.match_id, "  # noqa: S608
        f"  MAX(ms.match_date) as match_date, "
        f"  MAX(ms.home_team_name) as home_team_name, "
        f"  MAX(ms.away_team_name) as away_team_name, "
        f"  MAX(ms.home_score) as home_score, "
        f"  MAX(ms.away_score) as away_score "
        f"FROM {dp} dp "
        f"LEFT JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
        f"WHERE {where} "
        f"GROUP BY dp.match_id "
        f"ORDER BY MAX(ms.match_date) DESC "
        f"LIMIT 200",
        tuple(params),
    )


@ttl_cache()
def fetch_match_timeline(match_id: str, pid: int) -> pd.DataFrame:
    """Per-action DEFCON credits for a player in a specific match.

    Expected columns: event_id, opposing_player_id, credit_type,
    confidence, defcon_value, action_type, action_x, action_y, dist_to_ball.
    """
    da = t("fct_defcon_actions_synced")
    return execute_query(
        f"SELECT da.event_id, da.player_id as opposing_player_id, "  # noqa: S608
        f"  da.credit_type, da.confidence, da.defcon_value, "
        f"  da.action_type, da.action_x, da.action_y, "
        f"  da.dist_to_ball "
        f"FROM {da} da "
        f"WHERE da.match_id = %s AND da.action_player_id = %s "
        f"ORDER BY da.event_id "
        f"LIMIT 2000",
        (match_id, pid),
    )


@ttl_cache()
def fetch_breakdown_player_ids(comp_id: int, team_id: int | None) -> set[int]:
    """Player IDs that have pressure breakdown rows for the given filters."""
    dp = t("fct_defcon_pressure_synced")
    ms = t("fct_match_summary_synced")

    if team_id is not None:
        result = execute_query(
            f"SELECT dp.player_id "  # noqa: S608
            f"FROM {dp} dp "
            f"JOIN {ms} ms ON dp.match_id::bigint = ms.match_id "
            f"WHERE dp.competition_id = %s "
            f"AND (ms.home_team_id = %s OR ms.away_team_id = %s) "
            f"GROUP BY dp.player_id",
            (comp_id, team_id, team_id),
        )
    else:
        result = execute_query(
            f"WITH RECURSIVE dp_players AS ("  # noqa: S608
            f"  SELECT MIN(player_id) AS player_id FROM {dp} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(player_id) FROM {dp}"
            f"          WHERE competition_id = %s AND player_id > dp_players.player_id)"
            f"  FROM dp_players WHERE dp_players.player_id IS NOT NULL"
            f") SELECT player_id FROM dp_players WHERE player_id IS NOT NULL",
            (comp_id, comp_id),
        )

    if result.empty:
        return set()
    return {int(x) for x in result["player_id"]}


@ttl_cache(ttl=600)
def fetch_defcon_percentiles(comp_id: int, player_ids: tuple[int, ...]) -> dict[int, float]:
    """Fetch defcon_per_90_pctile for a batch of players.

    Returns {player_id: pctile_value} dict. Gracefully returns empty dict
    if the percentile table doesn't exist yet.
    """
    if not player_ids:
        return {}
    try:
        pctile_tbl = t("fct_player_percentiles_synced")
        placeholders = ", ".join(["%s"] * len(player_ids))
        # player_id in percentiles table is string type
        str_ids = tuple(str(pid) for pid in player_ids)
        df = execute_query(
            f"SELECT player_id, defcon_per_90_pctile "  # noqa: S608
            f"FROM {pctile_tbl} "
            f"WHERE competition_id = %s AND player_id IN ({placeholders}) "
            f"AND defcon_per_90_pctile IS NOT NULL",
            (comp_id, *str_ids),
        )
        if df.empty:
            return {}
        return {int(row["player_id"]): float(row["defcon_per_90_pctile"]) for _, row in df.iterrows()}
    except Exception:
        logger.debug("DEFCON percentile lookup failed (table may not exist)")
        return {}


@ttl_cache()
def fetch_timeline_player_ids(comp_id: int, team_id: int | None) -> set[int]:
    """action_player_ids that have DEFCON action rows for the given filters."""
    da = t("fct_defcon_actions_synced")
    ms = t("fct_match_summary_synced")

    if team_id is not None:
        result = execute_query(
            f"SELECT da.action_player_id as player_id "  # noqa: S608
            f"FROM {da} da "
            f"JOIN {ms} ms ON da.match_id::bigint = ms.match_id "
            f"WHERE da.competition_id = %s "
            f"AND (ms.home_team_id = %s OR ms.away_team_id = %s) "
            f"GROUP BY da.action_player_id",
            (comp_id, team_id, team_id),
        )
    else:
        result = execute_query(
            f"WITH RECURSIVE da_players AS ("  # noqa: S608
            f"  SELECT MIN(action_player_id) AS player_id FROM {da} WHERE competition_id = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(action_player_id) FROM {da}"
            f"          WHERE competition_id = %s AND action_player_id > da_players.player_id)"
            f"  FROM da_players WHERE da_players.player_id IS NOT NULL"
            f") SELECT player_id FROM da_players WHERE player_id IS NOT NULL",
            (comp_id, comp_id),
        )

    if result.empty:
        return set()
    return {int(x) for x in result["player_id"]}
