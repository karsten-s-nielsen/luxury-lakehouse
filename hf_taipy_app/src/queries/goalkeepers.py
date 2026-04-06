"""Goalkeeper analytics queries.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


@ttl_cache()
def fetch_gk_rankings(
    competition_id: int | None,
    team_id: int | None,
    min_minutes: int,
) -> pd.DataFrame:
    """Fetch GK rankings aggregated per player across all matches.

    Aggregates fct_goalkeeper_stats per player_id: sums counting stats
    (saves, goals_conceded, distribution_passes, punches), recomputes rate
    stats from summed counts (save_pct, launch_rate, claim_success_rate,
    gk_xt_per_pass), and normalizes per 90 min (psxg_per_90,
    goals_prevented_per_90).

    Expected columns: player_id, player_display_name, matches,
    minutes_played, saves, save_pct, gk_xt_per_pass, launch_rate,
    claim_success_rate, goals_prevented_per_90, psxg_per_90,
    goals_conceded, avg_defensive_action_distance, actions_outside_box_per_90,
    distribution_passes, gk_xt_delta_total, punches, keeper_pick_ups.
    """
    # Build params in SQL clause order: WHERE first, then HAVING.
    # psycopg2 binds %s positionally, so param order must match clause order.
    where_parts: list[str] = []
    where_params: list[Any] = []
    if competition_id is not None:
        where_parts.append("gk.competition_id = %s")
        where_params.append(int(competition_id))
    if team_id is not None:
        where_parts.append("gk.team_id = %s")
        where_params.append(int(team_id))

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
            round(
                CASE WHEN sum(gk.saves) + sum(gk.goals_conceded) > 0
                     THEN sum(gk.saves)::numeric
                          / (sum(gk.saves) + sum(gk.goals_conceded)) * 100
                     ELSE NULL END, 1)                    AS save_pct,
            round(
                CASE WHEN sum(gk.distribution_passes) > 0
                     THEN sum(gk.gk_xt_delta_total)::numeric
                          / sum(gk.distribution_passes)
                     ELSE NULL END, 4)                    AS gk_xt_per_pass,
            round(
                CASE WHEN sum(gk.distribution_passes) > 0
                     THEN sum(CASE WHEN gk.launch_rate IS NOT NULL
                                   THEN round(gk.launch_rate * gk.distribution_passes)
                                   ELSE 0 END)::numeric
                          / sum(gk.distribution_passes) * 100
                     ELSE NULL END, 1)                    AS launch_rate,
            round(
                CASE WHEN sum(gk.claims) > 0
                     THEN sum(CASE WHEN gk.claim_success_rate IS NOT NULL
                                   THEN round(gk.claim_success_rate * gk.claims)
                                   ELSE 0 END)::numeric
                          / sum(gk.claims) * 100
                     ELSE NULL END, 1)                    AS claim_success_rate,
            round((
                CASE WHEN sum(gk.minutes_played) > 0
                     THEN sum(gk.goals_prevented)::numeric * 90
                          / sum(gk.minutes_played)::numeric
                     ELSE NULL END)::numeric, 2)          AS goals_prevented_per_90,
            round((
                CASE WHEN sum(gk.minutes_played) > 0
                     THEN sum(gk.psxg_faced)::numeric * 90
                          / sum(gk.minutes_played)::numeric
                     ELSE NULL END)::numeric, 2)          AS psxg_per_90,
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

    return decode_unicode_columns(execute_query(sql, tuple(params)))


@ttl_cache()
def fetch_gk_player_lov(competition_id: int, team_id: int | None = None) -> list[tuple[str, int]]:
    """Fetch GK-only player list for the sidebar dropdown.

    Returns (display_label, player_id) tuples sorted by minutes played DESC.
    Only includes players with position_group = 'Goalkeeper' who have GK stats.
    When team_id is provided, filters directly on fct_goalkeeper_stats.team_id.
    """
    gk = t("fct_goalkeeper_stats_synced")
    dp = t("dim_players_synced")

    where_parts = ["gk.competition_id = %s"]
    params: list[Any] = [int(competition_id)]
    if team_id is not None:
        where_parts.append("gk.team_id = %s")
        params.append(int(team_id))

    where = " AND ".join(where_parts)
    df = decode_unicode_columns(
        execute_query(
            f"SELECT p.player_id, p.player_display_name, sum(gk.minutes_played) AS total_min "  # noqa: S608
            f"FROM {gk} gk "
            f"JOIN {dp} p ON gk.player_id = p.player_id "
            f"WHERE {where} "
            f"GROUP BY p.player_id, p.player_display_name "
            f"ORDER BY total_min DESC "
            f"LIMIT 200",
            tuple(params),
        )
    )
    return [(row["player_display_name"], int(row["player_id"])) for _, row in df.iterrows()]


@ttl_cache()
def fetch_gk_shots(
    competition_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch on-target shots faced by a GK for shot map scatter.

    When a GK is selected, joins fct_goalkeeper_stats to get per-match
    team_id for correct team exclusion (handles mid-season transfers).
    Uses end_location_x/y (pitch coordinates) since goalmouth Z is not
    available in the synced table.

    Expected columns: event_id, match_id, end_x, end_y, shot_outcome,
    xg, shooter_name.
    """
    where_parts = ["s.shot_outcome IN ('Goal', 'Saved', 'Saved Off Target', 'Saved to Post')"]
    join_params: list[Any] = []
    where_params: list[Any] = []
    join_clause = ""

    if competition_id is not None:
        where_parts.append("s.competition_id = %s")
        where_params.append(int(competition_id))
    if player_id is not None:
        # Join GK stats for per-match team_id — handles transfers correctly
        gk_tbl = t("fct_goalkeeper_stats_synced")
        join_clause = f"INNER JOIN {gk_tbl} gk ON gk.match_id = s.match_id AND gk.player_id = %s"
        join_params.append(int(player_id))
        # Exclude shots by the GK's own team (per-match correct)
        where_parts.append("s.team_id != gk.team_id")

    where = " AND ".join(where_parts)
    # Params must match SQL order: JOIN params before WHERE params
    params = join_params + where_params

    sql = (
        f"SELECT "  # noqa: S608
        f"  s.shot_id AS event_id, "
        f"  s.match_id, "
        f"  s.end_location_x AS end_x, "
        f"  s.end_location_y AS end_y, "
        f"  s.shot_outcome, "
        f"  s.statsbomb_xg AS xg, "
        f"  shooter.player_display_name AS shooter_name "
        f"FROM {t('fct_shots_synced')} s "
        f"{join_clause} "
        f"LEFT JOIN {t('dim_players_synced')} shooter "
        f"  ON s.player_id = shooter.player_id "
        f"WHERE {where} "
        f"ORDER BY s.match_id, s.period, s.minute "
        f"LIMIT 2000"
    )

    return decode_unicode_columns(execute_query(sql, tuple(params)))


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
