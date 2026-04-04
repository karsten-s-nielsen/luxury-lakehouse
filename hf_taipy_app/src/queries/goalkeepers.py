"""Goalkeeper analytics queries.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


@ttl_cache()
def fetch_gk_rankings(
    competition_id: int | None,
    team_id: int | None,
    min_minutes: int,
) -> pd.DataFrame:
    """Fetch GK rankings aggregated per player across all matches.

    Aggregates fct_goalkeeper_stats per player_id: sums counting stats
    (saves, goals_conceded, distribution_passes, punches), averages rate
    stats (save_pct, launch_rate, claim_success_rate, gk_xt_per_pass),
    and sums cumulative stats (minutes_played, psxg_faced, goals_prevented,
    gk_xt_delta_total).

    Expected columns: player_id, player_display_name, matches,
    minutes_played, saves, save_pct, gk_xt_per_pass, launch_rate,
    claim_success_rate, goals_prevented, psxg_faced, goals_conceded,
    avg_defensive_action_distance, actions_outside_box_per_90,
    distribution_passes, gk_xt_delta_total, punches, keeper_pick_ups.
    """
    # Build params in SQL clause order: WHERE first, then HAVING.
    # psycopg2 binds %s positionally, so param order must match clause order.
    where_parts: list[str] = []
    where_params: list[Any] = []
    if competition_id is not None:
        where_parts.append("gk.competition_id = %s")
        where_params.append(int(competition_id))

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    having_parts = ["sum(gk.minutes_played) >= %s"]
    having_params: list[Any] = [min_minutes]

    params = where_params + having_params

    sql = f"""
        SELECT
            gk.player_id,
            p.player_display_name,
            count(*)                                      AS matches,
            round(sum(gk.minutes_played)::numeric)        AS minutes_played,
            sum(gk.saves)                                 AS saves,
            round(avg(gk.save_pct)::numeric, 1)           AS save_pct,
            round(avg(gk.gk_xt_per_pass)::numeric, 4)    AS gk_xt_per_pass,
            round(avg(gk.launch_rate)::numeric, 1)        AS launch_rate,
            round(avg(gk.claim_success_rate)::numeric, 1) AS claim_success_rate,
            round(sum(gk.goals_prevented)::numeric, 2)    AS goals_prevented,
            round(sum(gk.psxg_faced)::numeric, 2)         AS psxg_faced,
            sum(gk.goals_conceded)                        AS goals_conceded,
            round(avg(gk.avg_defensive_action_distance)::numeric, 1)
                                                          AS avg_defensive_action_distance,
            round(avg(gk.actions_outside_box_per_90)::numeric, 1)
                                                          AS actions_outside_box_per_90,
            sum(gk.distribution_passes)                   AS distribution_passes,
            round(sum(gk.gk_xt_delta_total)::numeric, 3) AS gk_xt_delta_total,
            sum(gk.punches)                               AS punches,
            sum(gk.keeper_pick_ups)                       AS keeper_pick_ups
        FROM {t("fct_goalkeeper_stats_synced")} gk
        JOIN {t("dim_players_synced")} p ON gk.player_id = p.player_id
        {where}
        GROUP BY gk.player_id, p.player_display_name
        HAVING {" AND ".join(having_parts)}
        ORDER BY sum(gk.minutes_played) DESC
        LIMIT 500
    """  # noqa: S608

    return execute_query(sql, tuple(params))


@ttl_cache()
def fetch_gk_player_lov(competition_id: int, team_id: int | None = None) -> list[tuple[str, int]]:
    """Fetch GK-only player list for the sidebar dropdown.

    Returns (display_label, player_id) tuples sorted by minutes played DESC.
    Only includes players with position_group = 'Goalkeeper' who have GK stats.
    When team_id is provided, restricts to GKs whose PRIMARY team in this
    competition matches (determined by the team appearing most often across
    the GK's matches — the mode approach from resolve_gk_team_id).
    """
    gk = t("fct_goalkeeper_stats_synced")
    dp = t("dim_players_synced")
    ms = t("fct_match_summary_synced")

    if team_id is not None:
        # CTE: for each GK in this competition, determine their primary team
        # by finding which team_id appears most often across their matches.
        df = execute_query(
            f"WITH gk_teams AS ("  # noqa: S608
            f"  SELECT sub.player_id, sub.team_id, "
            f"    ROW_NUMBER() OVER (PARTITION BY sub.player_id ORDER BY count(*) DESC) AS rn "
            f"  FROM ("
            f"    SELECT g.player_id, m.home_team_id AS team_id "
            f"    FROM {gk} g JOIN {ms} m ON g.match_id = m.match_id "
            f"    WHERE g.competition_id = %s "
            f"    UNION ALL "
            f"    SELECT g.player_id, m.away_team_id AS team_id "
            f"    FROM {gk} g JOIN {ms} m ON g.match_id = m.match_id "
            f"    WHERE g.competition_id = %s "
            f"  ) sub GROUP BY sub.player_id, sub.team_id"
            f") "
            f"SELECT p.player_id, p.player_display_name, sum(gk.minutes_played) AS total_min "
            f"FROM {gk} gk "
            f"JOIN {dp} p ON gk.player_id = p.player_id "
            f"JOIN gk_teams gt ON gk.player_id = gt.player_id AND gt.rn = 1 AND gt.team_id = %s "
            f"WHERE gk.competition_id = %s "
            f"GROUP BY p.player_id, p.player_display_name "
            f"ORDER BY total_min DESC "
            f"LIMIT 200",
            (int(competition_id), int(competition_id), int(team_id), int(competition_id)),
        )
    else:
        df = execute_query(
            f"SELECT p.player_id, p.player_display_name, sum(gk.minutes_played) AS total_min "  # noqa: S608
            f"FROM {gk} gk "
            f"JOIN {dp} p ON gk.player_id = p.player_id "
            f"WHERE gk.competition_id = %s "
            f"GROUP BY p.player_id, p.player_display_name "
            f"ORDER BY total_min DESC "
            f"LIMIT 200",
            (int(competition_id),),
        )
    return [(row["player_display_name"], int(row["player_id"])) for _, row in df.iterrows()]


@ttl_cache()
def resolve_gk_team_id(player_id: int, competition_id: int | None = None) -> int | None:
    """Resolve a GK's team_id from match_summary within a competition.

    The GK's team is the team_id that appears in ALL their matches.
    For each GK match, both home_team_id and away_team_id are candidates.
    The GK's team appears in every match; opponents change per match.
    The mode (most frequent) team_id across all match sides is the GK's team.
    """
    gk_tbl = t("fct_goalkeeper_stats_synced")
    ms_tbl = t("fct_match_summary_synced")
    comp_filter = "AND g.competition_id = %s " if competition_id else ""
    params = [int(player_id), int(player_id)]
    if competition_id:
        params = [int(player_id), int(competition_id), int(player_id), int(competition_id)]
    df = execute_query(
        f"SELECT team_id, count(*) AS n FROM ("  # noqa: S608
        f"  SELECT m.home_team_id AS team_id "
        f"  FROM {gk_tbl} g JOIN {ms_tbl} m ON g.match_id = m.match_id "
        f"  WHERE g.player_id = %s {comp_filter}"
        f"  UNION ALL "
        f"  SELECT m.away_team_id AS team_id "
        f"  FROM {gk_tbl} g JOIN {ms_tbl} m ON g.match_id = m.match_id "
        f"  WHERE g.player_id = %s {comp_filter}"
        f") sub GROUP BY team_id ORDER BY n DESC LIMIT 1",
        tuple(params),
    )
    if df.empty:
        return None
    return int(df.iloc[0]["team_id"])


@ttl_cache()
def fetch_gk_shots(
    competition_id: int | None,
    player_id: int | None,
    gk_team_id: int | None = None,
) -> pd.DataFrame:
    """Fetch on-target shots faced by a GK for shot map scatter.

    Filters to on-target shots (Goal or Saved outcomes). When a GK is
    selected, restricts to matches where the GK played (from
    fct_goalkeeper_stats) and excludes shots by the GK's own team.
    Uses end_location_x/y (pitch coordinates) since goalmouth Z is not
    available in the synced table.

    Expected columns: event_id, match_id, end_x, end_y, shot_outcome,
    psxg, shooter_name.
    """
    where_parts = ["s.shot_outcome IN ('Goal', 'Saved')"]
    params: list[Any] = []

    if competition_id is not None:
        where_parts.append("s.competition_id = %s")
        params.append(int(competition_id))
    if player_id is not None:
        # Restrict to matches where this GK played.
        gk_tbl = t("fct_goalkeeper_stats_synced")
        where_parts.append(
            f"s.match_id IN (SELECT gk.match_id FROM {gk_tbl} gk WHERE gk.player_id = %s)"  # noqa: S608
        )
        params.append(int(player_id))
        # Exclude shots by the GK's own team — resolved via _resolve_gk_team_id().
        if gk_team_id is not None:
            where_parts.append("s.team_id != %s")
            params.append(int(gk_team_id))

    where = " AND ".join(where_parts)

    sql = (
        f"SELECT "  # noqa: S608
        f"  s.shot_id AS event_id, "
        f"  s.match_id, "
        f"  s.end_location_x AS end_x, "
        f"  s.end_location_y AS end_y, "
        f"  s.shot_outcome, "
        f"  s.statsbomb_xg AS psxg, "
        f"  shooter.player_display_name AS shooter_name "
        f"FROM {t('fct_shots_synced')} s "
        f"LEFT JOIN {t('dim_players_synced')} shooter "
        f"  ON s.player_id = shooter.player_id "
        f"WHERE {where} "
        f"ORDER BY s.match_id, s.period, s.minute "
        f"LIMIT 2000"
    )

    return execute_query(sql, tuple(params))


@ttl_cache()
def fetch_gk_passes(
    competition_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch GK distribution passes for pitch figure.

    Uses fct_action_values filtered to GK distribution action types
    (goalkick, pass) where the player is a Goalkeeper per dim_players.

    Expected columns: match_id, player_id, start_x, start_y, end_x, end_y,
    action_result, action_type.
    """
    where_parts = [
        "a.action_type IN ('goalkick', 'pass')",
        "dp.position_group = 'Goalkeeper'",
    ]
    params: list[Any] = []

    if competition_id is not None:
        where_parts.append("a.competition_id = %s")
        params.append(int(competition_id))
    if player_id is not None:
        where_parts.append("a.player_id = %s")
        params.append(int(player_id))

    where = " AND ".join(where_parts)

    sql = (
        f"SELECT "  # noqa: S608
        f"  a.match_id, "
        f"  a.player_id, "
        f"  a.start_x, "
        f"  a.start_y, "
        f"  a.end_x, "
        f"  a.end_y, "
        f"  a.action_result, "
        f"  a.action_type "
        f"FROM {t('fct_action_values_synced')} a "
        f"JOIN {t('dim_players_synced')} dp ON a.player_id = dp.player_id "
        f"WHERE {where} "
        f"ORDER BY a.match_id, a.period, a.time_seconds "
        f"LIMIT 5000"
    )

    return execute_query(sql, tuple(params))
