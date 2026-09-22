"""Goalkeeper Decisions & Shot-Stopping queries (sk4118 P1 Phase B/E).

Two evaluative GK families, per-COMPETITION cohorts (both carry competition_key directly):
  * Decision-Making — fct_gk_decision (per-decision; aggregated per keeper). NOT ranking-licensed
    (reconstruction-tier correlation only moderate), so we present COHORT POSITIONING, never a rank.
  * Shot-Stopping — fct_gk_shot_stopping_pooled (goals-prevented + Poisson-binomial CI band). The mart's
    ranking_enabled is FALSE everywhere (max ~14 shots faced), so the percentile leaderboard is DEFERRED
    upstream — we present goals-prevented with its CI + low_sample flag as cohort positioning, no rank.

Keeper + competition LOVs come from the pooled shot-stopping mart (every keeper faces shots); the
decision view shows an empty state when the selected keeper has no reconstructed decisions.
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

_DECISION_FLOOR = 10  # min reconstructed decisions for a keeper to enter the decision cohort


def build_gkd_competition_lov_sql() -> tuple[str, tuple]:
    """Competitions with pooled GK shot-stopping data, richest cohort first. (competition_key, name)."""
    sql = (
        f"SELECT p.competition_key, "  # noqa: S608
        f"       COALESCE(c.competition_name, 'Competition ' || CAST(p.competition_key AS text)) AS competition_name, "
        f"       COUNT(DISTINCT p.player_key) AS n_keepers "
        f"FROM {t('fct_gk_shot_stopping_pooled_synced')} p "
        f"LEFT JOIN {t('dim_competitions_synced')} c ON c.competition_key = p.competition_key "
        f"WHERE p.competition_key IS NOT NULL "
        f"GROUP BY p.competition_key, COALESCE(c.competition_name, 'Competition ' || CAST(p.competition_key AS text)) "
        f"ORDER BY n_keepers DESC, competition_name LIMIT 100"
    )
    return sql, ()


def build_gkd_keeper_lov_sql(competition_key: int) -> tuple[str, tuple]:
    """Keepers with shot-stopping rows in one competition, most shots first. (player_key, name)."""
    sql = (
        f"SELECT p.player_key, pl.player_display_name, "  # noqa: S608
        f"       SUM(COALESCE(p.shots_faced_total, 0)) AS n_shots "
        f"FROM {t('fct_gk_shot_stopping_pooled_synced')} p "
        f"JOIN {t('dim_players_synced')} pl ON pl.player_key = p.player_key "
        f"WHERE p.competition_key = %s "
        f"GROUP BY p.player_key, pl.player_display_name "
        f"ORDER BY n_shots DESC, pl.player_display_name LIMIT 500"
    )
    return sql, (competition_key,)


def build_decision_cohort_sql(competition_key: int) -> tuple[str, tuple]:
    """Per-keeper decision aggregates for one competition (cohort positioning, floored at n>=10).

    decision_value / sel_efficiency / decision_pct / chosen_ev / best_ev are the sk metric cols;
    (best_ev - chosen_ev) is the value left on the table — the comparison affordance."""
    sql = (
        f"SELECT g.player_key, pl.player_display_name, "  # noqa: S608
        f"       COUNT(*) AS n_decisions, "
        f"       AVG(g.decision_value) AS mean_decision_value, "
        f"       AVG(g.sel_efficiency) AS mean_sel_efficiency, "
        f"       AVG(g.decision_pct) AS mean_decision_pct, "
        f"       AVG(g.chosen_ev) AS mean_chosen_ev, "
        f"       AVG(g.best_ev) AS mean_best_ev, "
        f"       AVG(g.n_options) AS mean_n_options "
        f"FROM {t('fct_gk_decision_synced')} g "
        f"JOIN {t('dim_players_synced')} pl ON pl.player_key = g.player_key "
        f"WHERE g.competition_key = %s "
        f"GROUP BY g.player_key, pl.player_display_name "
        f"HAVING COUNT(*) >= {_DECISION_FLOOR} LIMIT 500"
    )
    return sql, (competition_key,)


def build_shot_stopping_cohort_sql(competition_key: int) -> tuple[str, tuple]:
    """Per-keeper pooled shot-stopping for one competition (goals prevented + CI + low_sample).

    Summed across a keeper's (competition, season) rows for the cohort scatter; the CI columns are
    carried through for the selected keeper's single-season display."""
    sql = (
        f"SELECT p.player_key, pl.player_display_name, "  # noqa: S608
        f"       SUM(p.goals_prevented) AS goals_prevented, "
        f"       SUM(p.psxg_faced) AS psxg_faced, "
        f"       SUM(p.goals_conceded_on_shots) AS goals_conceded, "
        f"       SUM(p.shots_faced_total) AS shots_faced_total, "
        f"       MIN(p.goals_prevented_ci_low) AS goals_prevented_ci_low, "
        f"       MAX(p.goals_prevented_ci_high) AS goals_prevented_ci_high, "
        f"       BOOL_OR(p.low_sample) AS low_sample "
        f"FROM {t('fct_gk_shot_stopping_pooled_synced')} p "
        f"JOIN {t('dim_players_synced')} pl ON pl.player_key = p.player_key "
        f"WHERE p.competition_key = %s "
        f"GROUP BY p.player_key, pl.player_display_name LIMIT 500"
    )
    return sql, (competition_key,)


def build_gkd_freshness_sql() -> tuple[str, tuple]:
    """Page freshness from the two GK evaluative marts."""
    sql = (
        f"SELECT "  # noqa: S608
        f"  (SELECT COUNT(*) FROM {t('fct_gk_decision_synced')}) AS n_decisions, "
        f"  (SELECT COUNT(DISTINCT player_key) FROM {t('fct_gk_shot_stopping_pooled_synced')}) AS n_keepers"
    )
    return sql, ()


@ttl_cache()
def fetch_gkd_competitions() -> pd.DataFrame:
    sql, params = build_gkd_competition_lov_sql()
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_gkd_keepers(competition_key: int) -> pd.DataFrame:
    sql, params = build_gkd_keeper_lov_sql(competition_key)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_decision_cohort(competition_key: int) -> pd.DataFrame:
    sql, params = build_decision_cohort_sql(competition_key)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_shot_stopping_cohort(competition_key: int) -> pd.DataFrame:
    sql, params = build_shot_stopping_cohort_sql(competition_key)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_gkd_data_freshness() -> str:
    try:
        sql, params = build_gkd_freshness_sql()
        df = execute_query(sql, params)
        if not df.empty and int(df.iloc[0]["n_decisions"]) > 0:
            return (
                f"GK decision + shot-stopping data: {int(df.iloc[0]['n_decisions'])} decisions "
                f"across {int(df.iloc[0]['n_keepers'])} keepers"
            )
    except Exception:  # noqa: BLE001 — freshness badge must never crash the page
        return "GK decision/shot-stopping freshness unavailable."
    return "GK decision/shot-stopping marts not yet populated."
