"""GK Analytics insight-views queries (per-COMPETITION; ADR-051 marts reused).

Read-side contract surface for the redesigned GK page. The user-facing filter is Competition
(restricted to tracking-provider competitions that have GK data); cohorts are within-competition
(finer + more comparable than per-provider, since each tracking competition belongs to one provider).
Goals-prevented reads the pre-aggregated fct_gk_shot_stopping_pooled (band/low_sample precomputed in
the mart; the NULL-safe season join preserves IDSSE). No in-app rollup.
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

GK_TRACKING_PROVIDERS: tuple[str, ...] = ("gradientsports", "idsse", "skillcorner")
_PROVIDER_SQL = ", ".join(["%s"] * len(GK_TRACKING_PROVIDERS))

_DIST_FLOOR = 20  # min distributions for a keeper to enter the distribution-profile cohort
_SWEEP_COLS = (
    "pc_share_mean",
    "reachable_area_mean_m2",
    "closing_min_six_yard_mean_s",
    "closing_min_near_post_mean_s",
    "closing_min_far_post_mean_s",
)


def build_gk_competition_lov_sql() -> tuple[str, tuple]:
    """Tracking-provider competitions that have GK tracking data — the page's Competition filter.
    Ordered by keeper count desc so the richest cohort is the default. Returns
    (competition_key, competition_name)."""
    sql = (
        f"SELECT dm.competition_key, "  # noqa: S608
        f"       COALESCE(c.competition_name, UPPER(s.data_source)) AS competition_name, "
        f"       COUNT(DISTINCT s.gk_player_key) AS n_keepers "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_matches_synced')} dm ON dm.match_key = s.match_key "
        f"LEFT JOIN {t('dim_competitions_synced')} c ON c.competition_key = dm.competition_key "
        f"WHERE s.data_source IN ({_PROVIDER_SQL}) AND dm.competition_key IS NOT NULL "
        f"GROUP BY dm.competition_key, COALESCE(c.competition_name, UPPER(s.data_source)) "
        f"ORDER BY n_keepers DESC, competition_name LIMIT 100"
    )
    return sql, GK_TRACKING_PROVIDERS


def build_gk_keeper_lov_sql(competition_key: int) -> tuple[str, tuple]:
    """Keepers with tracking stats in one competition, display names (no canonical dedup needed —
    within a competition each keeper is one row)."""
    sql = (
        f"SELECT s.gk_player_key, p.player_display_name, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions,0)) AS n_dist, "
        f"       SUM(COALESCE(s.n_defended_actions,0)) AS n_def "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_matches_synced')} dm ON dm.match_key = s.match_key "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = s.gk_player_key "
        f"WHERE dm.competition_key = %s "
        f"GROUP BY s.gk_player_key, p.player_display_name "
        f"ORDER BY n_def DESC, n_dist DESC LIMIT 500"
    )
    return sql, (competition_key,)


def build_distribution_profile_sql(competition_key: int) -> tuple[str, tuple]:
    """Per-GK distribution PROFILE for one competition, action grain (ADR-061 investigation 2026-06-22):
    the six game-model preset columns are ~0.99 collinear (one rescaled xT-GK formula) so the old
    "best-fit model" ladder was degenerate. Replaced by two real, ~independent axes:
      - share_adds_threat = fraction of distributions with xt_gk > 0 (threat headline; xT-GK is ~97%
        negative so the mean is a poor headline — the SHARE that adds threat varies, CV~0.46).
      - directness = mean forward progression (m); mean_completion is its inverse (r=-0.91) → tooltip.
    Floored at n>=20 distributions. xt_gk IS NOT NULL selects distribution actions."""
    sql = (
        f"SELECT a.player_key AS gk_player_key, p.player_display_name, "  # noqa: S608
        f"       COUNT(*) AS n_distributions, "
        f"       AVG(a.xt_gk) AS mean_xtgk, "
        f"       AVG(CASE WHEN a.xt_gk > 0 THEN 1.0 ELSE 0.0 END) AS share_adds_threat, "
        f"       AVG(a.gk_completion) AS mean_completion, "
        f"       AVG(a.end_x - a.start_x) AS mean_progress_m "
        f"FROM {t('fct_gk_tracking_actions_synced')} a "
        f"JOIN {t('dim_matches_synced')} m ON m.match_key = a.match_key "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = a.player_key "
        f"WHERE m.competition_key = %s AND a.xt_gk IS NOT NULL "
        f"GROUP BY a.player_key, p.player_display_name "
        f"HAVING COUNT(*) >= {_DIST_FLOOR} LIMIT 500"
    )
    return sql, (competition_key,)


def build_sweeper_stats_sql(competition_key: int) -> tuple[str, tuple]:
    """Per-GK volume-weighted sweeper means for one competition (the cohort)."""
    wsum = ", ".join(
        f"SUM(s.{c} * s.n_defended_actions) / NULLIF(SUM(s.n_defended_actions),0) AS {c}" for c in _SWEEP_COLS
    )
    sql = (
        f"SELECT s.gk_player_key, "  # noqa: S608
        f"       SUM(COALESCE(s.n_defended_actions,0)) AS n_defended_actions, "
        f"       SUM(COALESCE(s.shots_faced,0)) AS shots_faced, "
        f"       SUM(s.ghost_deviation_mean_m * s.shots_faced) / NULLIF(SUM(s.shots_faced),0) "
        f"           AS ghost_deviation_mean_m, "
        f"       {wsum} "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_matches_synced')} dm ON dm.match_key = s.match_key "
        f"WHERE dm.competition_key = %s "
        f"GROUP BY s.gk_player_key LIMIT 2000"
    )
    return sql, (competition_key,)


_LINE_FLOOR = 30  # min defended actions to enter the line cohort — low-sample keepers have wild
# per-keeper averages (the source of the earlier bimodal cohort).


def build_line_context_sql(competition_key: int) -> tuple[str, tuple]:
    """Per-(defending GK) line height + shape for one competition. avg_line_height_m = average
    DISTANCE FROM OWN GOAL (m; higher = higher line), normalised in the mart. Floored at
    n_actions >= 30. Reads the small precomputed mart (B2 defending-GK keyed; S3)."""
    sql = (
        f"SELECT gk_player_key, competition_key, "  # noqa: S608
        f"       avg_line_height_m, avg_width, avg_compactness, n_actions "
        f"FROM {t('fct_gk_defensive_line_synced')} "
        f"WHERE competition_key = %s AND n_actions >= {_LINE_FLOOR} LIMIT 2000"
    )
    return sql, (competition_key,)


def build_goals_prevented_sql(competition_key: int) -> tuple[str, tuple]:
    """Pre-aggregated goals-prevented rows (one per keeper x season) for one competition, read
    directly from fct_gk_shot_stopping_pooled — band/low_sample precomputed in the mart (NULL-safe
    season join keeps IDSSE). No in-app rollup, no LIMIT-on-SUM."""
    sql = (
        f"SELECT player_key, competition_key, season_id, data_source, "  # noqa: S608
        f"       goals_prevented, goals_prevented_ci_low, goals_prevented_ci_high, "
        f"       shots_faced_total, low_sample "
        f"FROM {t('fct_gk_shot_stopping_pooled_synced')} "
        f"WHERE competition_key = %s LIMIT 500"
    )
    return sql, (competition_key,)


def build_gk_freshness_sql() -> tuple[str, tuple]:
    """Page freshness from the page's OWN marts."""
    sql = (
        f"SELECT COUNT(*) AS n_rows, COUNT(DISTINCT gk_player_key) AS n_gks "  # noqa: S608
        f"FROM {t('fct_gk_tracking_stats_synced')} WHERE data_source IN ({_PROVIDER_SQL})"
    )
    return sql, GK_TRACKING_PROVIDERS


@ttl_cache()
def fetch_gk_competitions() -> pd.DataFrame:
    sql, params = build_gk_competition_lov_sql()
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_gk_data_freshness() -> str:
    try:
        sql, params = build_gk_freshness_sql()
        df = execute_query(sql, params)
        if not df.empty and int(df.iloc[0]["n_rows"]) > 0:
            return (
                f"GK tracking data: {int(df.iloc[0]['n_rows'])} GK-match rows "
                f"across {int(df.iloc[0]['n_gks'])} goalkeepers"
            )
    except Exception:  # noqa: BLE001 — freshness badge must never crash the page
        return "GK tracking data freshness unavailable."
    return "GK tracking marts not yet populated."


@ttl_cache()
def fetch_gk_keepers(competition_key: int) -> pd.DataFrame:
    sql, params = build_gk_keeper_lov_sql(competition_key)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_distribution_profile(competition_key: int) -> pd.DataFrame:
    sql, params = build_distribution_profile_sql(competition_key)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_sweeper_stats(competition_key: int) -> pd.DataFrame:
    sql, params = build_sweeper_stats_sql(competition_key)
    return execute_query(sql, params)


@ttl_cache()
def fetch_line_context(competition_key: int) -> pd.DataFrame:
    sql, params = build_line_context_sql(competition_key)
    return execute_query(sql, params)


@ttl_cache()
def fetch_goals_prevented(competition_key: int) -> pd.DataFrame:
    sql, params = build_goals_prevented_sql(competition_key)
    return execute_query(sql, params)
