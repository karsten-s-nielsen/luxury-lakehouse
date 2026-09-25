"""VAEP Adjusted queries — the action-grain mart ``fct_action_values`` (sk4118 P1 Phase D).

Phase D added four columns to fct_action_values: ``xsuccess`` (pass-success probability) plus the
result-adjusted VAEP triple ``offensive_adjusted_value`` / ``defensive_adjusted_value`` /
``vaep_adjusted_value`` (silly-kicks ``VAEP.rate_adjusted`` — a surgical result-flip weighted by
xsuccess). This page aggregates actions -> player and shows RAW vs ADJUSTED VAEP side by side.

Filters on the Kimball ``competition_key`` + legacy ``team_id``; joins dim_players_synced on
``player_key`` for names (NULL for IDSSE/Metrica in the dual-column window, so those rows are excluded).
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

_ACTION_FLOOR = 50  # min actions for a player to enter the VAEP leaderboard (VAEP needs volume)


def build_vaep_adjusted_leaderboard_sql(competition_key: int, team_id: int | None) -> tuple[str, tuple]:
    """Per-player raw vs adjusted VAEP totals for one competition (ranked by adjusted VAEP)."""
    params: list[object] = [competition_key]
    where = ["av.competition_key = %s", "av.player_key IS NOT NULL"]
    if team_id is not None:
        where.append("av.team_id = %s")
        params.append(team_id)
    sql = (
        f"SELECT p.player_display_name, "  # noqa: S608
        f"       COUNT(*) AS actions, "
        f"       SUM(av.vaep_value) AS vaep_raw, "
        f"       SUM(av.vaep_adjusted_value) AS vaep_adj, "
        f"       SUM(av.offensive_value) AS off_raw, "
        f"       SUM(av.offensive_adjusted_value) AS off_adj, "
        f"       SUM(av.defensive_value) AS def_raw, "
        f"       SUM(av.defensive_adjusted_value) AS def_adj, "
        f"       AVG(av.xsuccess) AS mean_xsuccess "
        f"FROM {t('fct_action_values_synced')} av "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = av.player_key "
        f"WHERE {' AND '.join(where)} "
        f"GROUP BY av.player_key, p.player_display_name "
        f"HAVING COUNT(*) >= {_ACTION_FLOOR} "
        f"ORDER BY vaep_adj DESC NULLS LAST LIMIT 500"
    )
    return sql, tuple(params)


def build_vaep_adjusted_freshness_sql() -> tuple[str, tuple]:
    """Freshness — how many actions already carry the adjusted-VAEP columns."""
    sql = (
        f"SELECT COUNT(*) AS n_actions, "  # noqa: S608
        f"       COUNT(vaep_adjusted_value) AS n_adjusted "
        f"FROM {t('fct_action_values_synced')}"
    )
    return sql, ()


@ttl_cache()
def fetch_vaep_adjusted_leaderboard(competition_key: int, team_id: int | None) -> pd.DataFrame:
    sql, params = build_vaep_adjusted_leaderboard_sql(competition_key, team_id)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_vaep_adjusted_freshness() -> str:
    try:
        sql, params = build_vaep_adjusted_freshness_sql()
        df = execute_query(sql, params)
        if not df.empty and int(df.iloc[0]["n_adjusted"]) > 0:
            return (
                f"Adjusted VAEP: {int(df.iloc[0]['n_adjusted'])} of {int(df.iloc[0]['n_actions'])} "
                f"actions carry adjusted values"
            )
    except Exception:  # noqa: BLE001 — freshness badge must never crash the page
        return "Adjusted-VAEP freshness unavailable."
    return "Adjusted VAEP not yet populated (lands with the sk4118 recompute)."
