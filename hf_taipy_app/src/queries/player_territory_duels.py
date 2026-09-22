"""Player Territory & Duels queries — the player-match grain mart ``fct_player_match_metrics``.

Two per-player evaluative families (sk4118 P1 Phase C), both ranking-licensed:
  * Territory (TF-54/TF-54b) — territorial-dominance xT. The mart carries BOTH methods in one wide row,
    so the leaderboard shows them SIDE BY SIDE: v1 xT prevented (completed_failed) and the counterfactual
    xT prevented above expectation — the model-comparison affordance.
  * Duels (TF-55) — a stateful Glicko-2 ground-duel rating that carries forward across matches. The
    stabilised surface is the LATEST carried-forward rating per player (with its deviation), not a mean.

Both carry competition_key directly; team scoping joins dim_teams_synced on team_key. No raw ids shown.
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

_TERRITORY_MATCH_FLOOR = 3  # min matches for a player to enter the territory leaderboard
_DUEL_MATCH_FLOOR = 3  # min matches with a duel rating


def _team_join_and_filter(team_id: int | None) -> tuple[str, str, list[object]]:
    """Optional dim_teams join + WHERE clause fragment for a team filter (by legacy team_id)."""
    if team_id is None:
        return "", "", []
    join = f"JOIN {t('dim_teams_synced')} dt ON dt.team_key = pm.team_key "
    return join, "AND dt.team_id = %s ", [team_id]


def build_territory_leaderboard_sql(competition_key: int, team_id: int | None) -> tuple[str, tuple]:
    """Per-player territory aggregates for one competition, both methods side by side (ranked by xT net)."""
    join, team_where, team_params = _team_join_and_filter(team_id)
    sql = (
        f"SELECT p.player_display_name, "  # noqa: S608
        f"       COUNT(*) AS matches, "
        f"       SUM(pm.territory_xt_net) AS xt_net, "
        f"       SUM(pm.territory_xt_prevented) AS xt_prevented_v1, "
        f"       SUM(pm.territory_xt_prevented_above_expectation) AS xt_prevented_cf, "
        f"       SUM(pm.territory_passes_into_hull) AS passes_into_hull, "
        f"       AVG(pm.territory_hull_area_m2) AS hull_area_m2 "
        f"FROM {t('fct_player_match_metrics_synced')} pm "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = pm.player_key "
        f"{join}"
        f"WHERE pm.competition_key = %s AND pm.territory_xt_net IS NOT NULL {team_where}"
        f"GROUP BY pm.player_key, p.player_display_name "
        f"HAVING COUNT(*) >= {_TERRITORY_MATCH_FLOOR} "
        f"ORDER BY xt_net DESC NULLS LAST LIMIT 500"
    )
    return sql, (competition_key, *team_params)


def build_duels_leaderboard_sql(competition_key: int, team_id: int | None) -> tuple[str, tuple]:
    """Per-player duel leaderboard for one competition: latest carried-forward Glicko-2 rating + counts.

    Glicko-2 is stateful, so the stabilised surface is the LAST match's carried-forward rating, not a
    mean. ``latest`` picks it via DISTINCT ON over the (small, competition-filtered) player-match set,
    ordered by match_date; ``agg`` sums the per-match contested/won/lost. Ranked by rating desc."""
    join, team_where, team_params = _team_join_and_filter(team_id)
    sql = (
        f"WITH agg AS ("  # noqa: S608
        f"  SELECT pm.player_key, COUNT(*) AS matches, "
        f"         SUM(pm.duels_contested) AS contested, SUM(pm.duels_won) AS won, SUM(pm.duels_lost) AS lost "
        f"  FROM {t('fct_player_match_metrics_synced')} pm {join}"
        f"  WHERE pm.competition_key = %s AND pm.duel_rating IS NOT NULL {team_where}"
        f"  GROUP BY pm.player_key HAVING COUNT(*) >= {_DUEL_MATCH_FLOOR}"
        f"), latest AS ("
        f"  SELECT DISTINCT ON (pm.player_key) pm.player_key, "
        f"         pm.duel_rating, pm.duel_rating_deviation, pm.duel_volatility "
        f"  FROM {t('fct_player_match_metrics_synced')} pm "
        f"  JOIN {t('dim_matches_synced')} dm ON dm.match_key = pm.match_key "
        f"  WHERE pm.competition_key = %s AND pm.duel_rating IS NOT NULL "
        f"  ORDER BY pm.player_key, dm.match_date DESC NULLS LAST"
        f") "
        f"SELECT p.player_display_name, l.duel_rating, l.duel_rating_deviation, l.duel_volatility, "
        f"       a.matches, a.contested, a.won, a.lost "
        f"FROM agg a "
        f"JOIN latest l ON l.player_key = a.player_key "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = a.player_key "
        f"ORDER BY l.duel_rating DESC NULLS LAST LIMIT 500"
    )
    # agg params (competition + optional team) then latest params (competition only).
    return sql, (competition_key, *team_params, competition_key)


def build_ptd_freshness_sql() -> tuple[str, tuple]:
    """Page freshness from the player-match metrics mart."""
    sql = (
        f"SELECT COUNT(*) AS n_rows, COUNT(DISTINCT player_key) AS n_players "  # noqa: S608
        f"FROM {t('fct_player_match_metrics_synced')}"
    )
    return sql, ()


@ttl_cache()
def fetch_territory_leaderboard(competition_key: int, team_id: int | None) -> pd.DataFrame:
    sql, params = build_territory_leaderboard_sql(competition_key, team_id)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_duels_leaderboard(competition_key: int, team_id: int | None) -> pd.DataFrame:
    sql, params = build_duels_leaderboard_sql(competition_key, team_id)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_ptd_data_freshness() -> str:
    try:
        sql, params = build_ptd_freshness_sql()
        df = execute_query(sql, params)
        if not df.empty and int(df.iloc[0]["n_rows"]) > 0:
            return (
                f"Player territory + duels: {int(df.iloc[0]['n_rows'])} player-match rows "
                f"across {int(df.iloc[0]['n_players'])} players"
            )
    except Exception:  # noqa: BLE001 — freshness badge must never crash the page
        return "Player territory/duels freshness unavailable."
    return "Player territory/duels mart not yet populated."
