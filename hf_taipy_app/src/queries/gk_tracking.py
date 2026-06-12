"""GK tracking-page queries (new page, side-by-side with queries/goalkeepers.py — ADR-051).

Provider gate: GK_TRACKING_PROVIDERS (spec section 3) — Metrica excluded (anonymized players),
SB360 deferred. The marts are provider-agnostic; the gate lives ONLY here.

Column constants (GK_ACTIONS_COLUMNS / GK_STATS_COLUMNS) are the read-side contract surface:
src/tests/test_gk_tracking_read_contract.py asserts they are a subset of the dbt contracts, so a
mart column rename fails CI here instead of silently in the Space (architecture-audit A3/A4).
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

GK_TRACKING_PROVIDERS: tuple[str, ...] = ("gradientsports", "idsse", "skillcorner")
# Placeholder string DERIVED from the constant (review M4): adding SB360 later is a
# one-tuple change, never a two-site edit.
_PROVIDER_SQL = f"data_source IN ({', '.join(['%s'] * len(GK_TRACKING_PROVIDERS))})"

GK_ACTIONS_COLUMNS: tuple[str, ...] = (
    "gk_action_id",
    "match_key",
    "team_key",
    "player_key",
    "defending_gk_player_key",
    "data_source",
    "action_id",
    "period_id",
    "time_seconds",
    "type_name",
    "game_state",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "frame_id",
    "action_result",
    "gk_was_distributing",
    "xt_gk",
    "xt_gk_possession",
    "xt_gk_counter",
    "xt_gk_direct",
    "xt_gk_high_press",
    "xt_gk_low_block",
    "xt_gk_base",
    "xt_gk_pev",
    "xt_gk_rav",
    "xt_gk_dzv",
    "xt_gk_pressure",
    "gk_completion",
    "pressure_on_actor__andrienko_oval",
    "ghost_gk_x",
    "ghost_gk_y",
    "ghost_gk_density_spread",
    "ghost_gk_method",
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    "gk_closing_time_mean_s__near_post",
    "gk_closing_time_min_s__near_post",
    "gk_closing_time_mean_s__far_post",
    "gk_closing_time_min_s__far_post",
    "defensive_line_x",
    "pitch_control_method",
    "pre_shot_gk_x",
    "pre_shot_gk_y",
    "pre_shot_gk_distance_to_goal",
    "pre_shot_gk_distance_to_shot",
    "pre_shot_gk_angle_to_shot_trajectory",
    "pre_shot_gk_angle_off_goal_line",
    "gk_frame_mirrored",
    "gk_actual_x",
    "gk_actual_y",
    "ghost_deviation_m",
    "line_height_m",
)

GK_STATS_COLUMNS: tuple[str, ...] = (
    "gk_match_stat_id",
    "gk_player_key",
    "match_key",
    "data_source",
    "n_distributions",
    "dist_xt_gk_mean",
    "dist_xt_gk_possession_mean",
    "dist_xt_gk_counter_mean",
    "dist_xt_gk_direct_mean",
    "dist_xt_gk_high_press_mean",
    "dist_xt_gk_low_block_mean",
    "dist_completion_mean",
    "dist_pressure_mean",
    "n_defended_actions",
    "shots_faced",
    "goals_conceded",
    "ghost_deviation_mean_m",
    "closing_min_six_yard_mean_s",
    "closing_min_near_post_mean_s",
    "closing_min_far_post_mean_s",
    "reachable_area_mean_m2",
    "pc_share_mean",
)

_PRESET_MEANS = (
    "dist_xt_gk_mean",
    "dist_xt_gk_possession_mean",
    "dist_xt_gk_counter_mean",
    "dist_xt_gk_direct_mean",
    "dist_xt_gk_high_press_mean",
    "dist_xt_gk_low_block_mean",
)

# Preset label -> stats-mart column (query-surface concern; state re-exports this).
# Covered by the read-side contract test alongside GK_STATS_COLUMNS.
PRESET_COLUMN: dict[str, str] = {
    "Default": "dist_xt_gk_mean",
    "Possession": "dist_xt_gk_possession_mean",
    "Counter": "dist_xt_gk_counter_mean",
    "Direct": "dist_xt_gk_direct_mean",
    "High Press": "dist_xt_gk_high_press_mean",
    "Low Block": "dist_xt_gk_low_block_mean",
}


def build_gk_lov_sql() -> tuple[str, tuple]:
    sql = (
        f"SELECT s.gk_player_key, p.player_display_name, s.data_source, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions, 0)) AS n_distributions, "
        f"       SUM(COALESCE(s.shots_faced, 0)) AS shots_faced "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = s.gk_player_key "
        f"WHERE s.{_PROVIDER_SQL} "
        f"GROUP BY s.gk_player_key, p.player_display_name, s.data_source "
        f"ORDER BY n_distributions DESC LIMIT 500"
    )
    return sql, GK_TRACKING_PROVIDERS


def build_gk_actions_sql(gk_player_key: str, family: str) -> tuple[str, tuple]:
    if family == "distribution":
        where = "gk_was_distributing AND xt_gk IS NOT NULL AND player_key = %s"
    elif family == "defense":
        where = "defending_gk_player_key = %s"
    elif family == "shots":
        where = "pre_shot_gk_x IS NOT NULL AND defending_gk_player_key = %s"
    else:
        raise ValueError(f"unknown family: {family}")
    cols = ", ".join(GK_ACTIONS_COLUMNS)  # explicit list (A4): no SELECT * under append_new_columns
    sql = (
        f"SELECT {cols} FROM {t('fct_gk_tracking_actions_synced')} "  # noqa: S608
        f"WHERE {_PROVIDER_SQL} AND {where} "
        f"ORDER BY match_key, period_id, time_seconds LIMIT 2000"
    )
    return sql, (*GK_TRACKING_PROVIDERS, gk_player_key)


def build_gk_stats_sql(gk_player_key: str) -> tuple[str, tuple]:
    cols = ", ".join(GK_STATS_COLUMNS)  # explicit list (review M3 — own A4 finding, applied)
    sql = (
        f"SELECT {cols} FROM {t('fct_gk_tracking_stats_synced')} "  # noqa: S608
        f"WHERE {_PROVIDER_SQL} AND gk_player_key = %s LIMIT 500"
    )
    return sql, (*GK_TRACKING_PROVIDERS, gk_player_key)


def build_gk_pool_stats_sql(min_distributions: int = 10) -> tuple[str, tuple]:
    """Pool-wide per-GK aggregates (review H2): feeds the Tab 1 bump chart (rank ALL GKs under
    every preset) and every 'vs sample' right-rail delta.

    Weighting (review N1): distribution metrics weighted by n_distributions; deviation by
    shots_faced (it only exists on shots); closing/reachable by n_defended_actions — a 1-shot
    match and a 10-shot match must not count equally.
    """
    wmeans = ", ".join(
        f"SUM(s.{c} * s.n_distributions) / NULLIF(SUM(s.n_distributions), 0) AS {c}" for c in _PRESET_MEANS
    )
    sql = (
        f"SELECT s.gk_player_key, p.player_display_name, s.data_source, "  # noqa: S608
        f"       SUM(COALESCE(s.n_distributions, 0)) AS n_distributions, {wmeans}, "
        f"       SUM(s.dist_completion_mean * s.n_distributions) "
        f"         / NULLIF(SUM(s.n_distributions), 0) AS dist_completion_mean, "
        f"       SUM(COALESCE(s.shots_faced, 0)) AS shots_faced, "
        f"       SUM(COALESCE(s.goals_conceded, 0)) AS goals_conceded, "
        f"       SUM(s.ghost_deviation_mean_m * s.shots_faced) "
        f"         / NULLIF(SUM(s.shots_faced), 0) AS ghost_deviation_mean_m, "
        f"       SUM(s.closing_min_six_yard_mean_s * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS closing_min_six_yard_mean_s, "
        f"       SUM(s.closing_min_near_post_mean_s * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS closing_min_near_post_mean_s, "
        f"       SUM(s.closing_min_far_post_mean_s * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS closing_min_far_post_mean_s, "
        f"       SUM(s.reachable_area_mean_m2 * s.n_defended_actions) "
        f"         / NULLIF(SUM(s.n_defended_actions), 0) AS reachable_area_mean_m2 "
        f"FROM {t('fct_gk_tracking_stats_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON p.player_key = s.gk_player_key "
        f"WHERE s.{_PROVIDER_SQL} "
        f"GROUP BY s.gk_player_key, p.player_display_name, s.data_source "
        f"HAVING SUM(COALESCE(s.n_distributions, 0)) >= %s OR SUM(COALESCE(s.shots_faced, 0)) > 0 "
        f"LIMIT 500"
    )
    return sql, (*GK_TRACKING_PROVIDERS, min_distributions)


def build_gk_freshness_sql() -> tuple[str, tuple]:
    """Page freshness from the page's OWN tables (observability O2 — the global
    fetch_data_freshness reads fct_match_summary, which says nothing about these marts)."""
    sql = (
        f"SELECT COUNT(*) AS n_rows, COUNT(DISTINCT match_key) AS n_matches "  # noqa: S608
        f"FROM {t('fct_gk_tracking_stats_synced')} WHERE {_PROVIDER_SQL}"
    )
    return sql, GK_TRACKING_PROVIDERS


def build_scene_frame_sql(match_key: int, period: int, frame: int) -> tuple[str, tuple]:
    sql = (
        f"SELECT player_id, team_id, x, y, ball_x, ball_y, is_goalkeeper "  # noqa: S608
        f"FROM {t('fct_tracking_frames_synced')} "
        f"WHERE match_key = %s AND period = %s AND frame = %s LIMIT 60"
    )
    return sql, (match_key, period, frame)


@ttl_cache()
def fetch_gk_lov() -> pd.DataFrame:
    sql, params = build_gk_lov_sql()
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_gk_actions(gk_player_key: str, family: str) -> pd.DataFrame:
    sql, params = build_gk_actions_sql(gk_player_key, family)
    return execute_query(sql, params)


@ttl_cache()
def fetch_gk_stats(gk_player_key: str) -> pd.DataFrame:
    sql, params = build_gk_stats_sql(gk_player_key)
    return execute_query(sql, params)


@ttl_cache()
def fetch_gk_pool_stats(min_distributions: int = 10) -> pd.DataFrame:
    sql, params = build_gk_pool_stats_sql(min_distributions)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_scene_frame(match_key: int, period: int, frame: int) -> pd.DataFrame:
    sql, params = build_scene_frame_sql(match_key, period, frame)
    return execute_query(sql, params)


@ttl_cache()
def fetch_gk_data_freshness() -> str:
    try:
        sql, params = build_gk_freshness_sql()
        df = execute_query(sql, params)
        if not df.empty and int(df.iloc[0]["n_rows"]) > 0:
            return (
                f"GK tracking data: {int(df.iloc[0]['n_rows'])} GK-match rows "
                f"across {int(df.iloc[0]['n_matches'])} matches"
            )
    except Exception:  # noqa: BLE001 — freshness badge must never crash the page
        return "GK tracking data freshness unavailable."
    return "GK tracking marts not yet populated."
