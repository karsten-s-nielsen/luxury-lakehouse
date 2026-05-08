"""Tracking queries — pitch control, movement, heat map. Extracted from state modules.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.

PR 6 (ADR-011): pitch-control queries use fct_tracking_frames_synced (PR 7
migration target, not PR 6). stg_pitch_control__values now carries match_key
forward-compat — annotation only here; no functional change in PR 6.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pitch Control queries (from state/pitch_control.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_pc_frame_range(match_id: str, period: int) -> tuple[int, int]:
    """Get min/max frame numbers for a match and period.

    Returns (min_frame, max_frame).
    """
    tbl = t("fct_tracking_frames_synced")
    df = execute_query(
        f"SELECT MIN(frame) as min_frame, MAX(frame) as max_frame "  # noqa: S608
        f"FROM {tbl} "
        f"WHERE match_id = %s AND period = %s",
        (str(match_id), int(period)),
    )
    if df.empty:
        return (0, 0)
    return (int(df.iloc[0]["min_frame"]), int(df.iloc[0]["max_frame"]))


@ttl_cache()
def fetch_pc_frame_rate(match_id: str) -> int:
    """Get the frame rate for a specific match."""
    tbl = t("fct_tracking_frames_synced")
    df = execute_query(
        f"SELECT frame_rate FROM {tbl} "  # noqa: S608
        f"WHERE match_id = %s LIMIT 1",
        (str(match_id),),
    )
    if df.empty:
        return 25
    return int(df.iloc[0]["frame_rate"])


@ttl_cache()
def fetch_pc_frame_data(match_id: str, frame: int) -> pd.DataFrame:
    """Load all player rows for a specific frame (pitch control).

    Expected columns: player_id, team, x, y, ball_x, ball_y,
    velocity_x, velocity_y, speed, distance_to_ball.
    """
    tbl = t("fct_tracking_frames_synced")
    return execute_query(
        f"SELECT player_id, team, x, y, ball_x, ball_y, "  # noqa: S608
        f"  velocity_x, velocity_y, speed, distance_to_ball "
        f"FROM {tbl} "
        f"WHERE match_id = %s AND frame = %s",
        (str(match_id), int(frame)),
    )


@ttl_cache()
def fetch_pc_match_label(match_id: str) -> str:
    """Resolve tracking match_id to human-readable label.

    Post-PR 2 (ADR-011): fct_match_summary_synced is keyed on match_key,
    so we route via dim_matches_synced (which carries native_match_id).
    The 'idsse_' prefix is stripped before matching because
    dim_matches.native_match_id is unprefixed for IDSSE.
    """
    match_tbl = t("fct_match_summary_synced")
    dim_tbl = t("dim_matches_synced")
    df = execute_query(
        f"SELECT ms.match_date, ms.home_team_name, ms.away_team_name "  # noqa: S608
        f"FROM {dim_tbl} dm "
        f"LEFT JOIN {match_tbl} ms ON dm.match_key = ms.match_key "
        f"WHERE dm.native_match_id = regexp_replace(%s, '^(idsse_|metrica_)', '') LIMIT 1",
        (str(match_id),),
    )
    if df.empty:
        return match_id
    r = df.iloc[0]
    return f"{r['match_date']} \u2014 {r['home_team_name']} v {r['away_team_name']}"


# ---------------------------------------------------------------------------
# Heat Map queries (from state/heat_map.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_heatmap_actions(
    comp_id: int,
    team_id: int | None,
    player_id: int | None,
    match_key: int | None,
) -> pd.DataFrame:
    """Fetch server-side aggregated heat data.

    Two access paths (split based on filter combo):

    1. **Aggregated mart** (fct_heatmap_agg_synced) — used when `player_id`
       and `match_id` are both None. The mart is pre-aggregated at
       (competition_id, team_id, action_type, x_bin, y_bin) grain by
       dbt, so a comp-only filter becomes a ~60K-row scan (measured
       <10 ms) instead of the 6,864 ms Parallel Seq Scan on the raw
       5.05M-row fct_passes table. comp+team is a direct composite-index
       lookup on (comp, team) in the mart.

    2. **Direct fall-through** — used when `player_id` or `match_id` is
       set, because those filters are not in the mart grain.  The
       fct_passes and fct_shots tables have `idx_passes_comp_player`,
       `idx_passes_comp_team_match`, `idx_shots_comp_team_player`
       etc. which already make these paths fast (<100 ms).

    Both paths return identical columns: x, y, action_type, cnt, n_matches.
    """
    # Path 1: aggregated mart — no player / match filter
    if player_id is None and match_key is None:
        heatmap_tbl = t("fct_heatmap_agg_synced")
        ms_tbl = t("fct_match_summary_synced")
        conditions = ["competition_id = %s"]
        params: list[Any] = [int(comp_id)]
        ms_conditions = ["competition_id = %s"]
        ms_params: list[Any] = [int(comp_id)]
        if team_id is not None:
            conditions.append("team_id = %s")
            params.append(int(team_id))
            ms_conditions.append("(home_team_id = %s OR away_team_id = %s)")
            ms_params.extend([int(team_id), int(team_id)])
        where = " AND ".join(conditions)
        ms_where = " AND ".join(ms_conditions)
        return execute_query(
            f"SELECT x_bin AS x, y_bin AS y, action_type, "  # noqa: S608
            f"       sum(event_count) AS cnt, "
            f"       (SELECT COUNT(*) FROM {ms_tbl} WHERE {ms_where}) AS n_matches "
            f"FROM {heatmap_tbl} "
            f"WHERE {where} "
            f"GROUP BY x_bin, y_bin, action_type",
            tuple(ms_params + params),
        )

    # Path 2: direct fall-through for player/match filters (not in mart grain)
    passes_tbl = t("fct_passes_synced")
    shots_tbl = t("fct_shots_synced")

    # Build match-count CTE conditions (no table alias — standalone subquery)
    mc_conditions = ["competition_id = %s"]
    mc_params: list[Any] = [int(comp_id)]

    pass_conditions = ["p.competition_id = %s"]
    shot_conditions = ["s.competition_id = %s"]
    pass_params: list[Any] = [int(comp_id)]
    shot_params: list[Any] = [int(comp_id)]

    if team_id is not None:
        mc_conditions.append("team_id = %s")
        mc_params.append(int(team_id))
        pass_conditions.append("p.team_id = %s")
        shot_conditions.append("s.team_id = %s")
        pass_params.append(int(team_id))
        shot_params.append(int(team_id))

    if player_id is not None:
        mc_conditions.append("player_id = %s")
        mc_params.append(int(player_id))
        pass_conditions.append("p.player_id = %s")
        shot_conditions.append("s.player_id = %s")
        pass_params.append(int(player_id))
        shot_params.append(int(player_id))

    if match_key is not None:
        mc_conditions.append("match_key = %s")
        mc_params.append(int(match_key))
        pass_conditions.append("p.match_key = %s")
        shot_conditions.append("s.match_key = %s")
        pass_params.append(int(match_key))
        shot_params.append(int(match_key))

    mc_where = " AND ".join(mc_conditions)
    pass_where = " AND ".join(pass_conditions)
    shot_where = " AND ".join(shot_conditions)
    all_params = tuple(mc_params + pass_params + shot_params)

    return execute_query(
        f"WITH mc AS ("  # noqa: S608
        f"  SELECT COUNT(DISTINCT match_key) AS n_matches "
        f"  FROM {passes_tbl} WHERE {mc_where}"
        f") "
        f"SELECT x, y, action_type, sum(cnt) AS cnt, "
        f"  (SELECT n_matches FROM mc) AS n_matches "
        f"FROM ("
        f"  SELECT round(p.start_x / 10) * 10 + 5 AS x,"
        f"    round(p.start_y / 10) * 10 + 5 AS y,"
        f"    'pass' AS action_type, count(*) AS cnt "
        f"  FROM {passes_tbl} p WHERE {pass_where} "
        f"  GROUP BY round(p.start_x / 10), round(p.start_y / 10) "
        f"  UNION ALL "
        f"  SELECT round(s.location_x / 10) * 10 + 5 AS x,"
        f"    round(s.location_y / 10) * 10 + 5 AS y,"
        f"    'shot' AS action_type, count(*) AS cnt "
        f"  FROM {shots_tbl} s WHERE {shot_where} "
        f"  GROUP BY round(s.location_x / 10), round(s.location_y / 10)"
        f") agg GROUP BY x, y, action_type",
        all_params,
    )


# ---------------------------------------------------------------------------
# Movement Analysis queries (from state/movement_analysis.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_physical_stats(match_id: str) -> pd.DataFrame:
    """Fetch physical stats for a tracking match, joined with player names.

    Expected columns: player_id, player_name, match_id, source_provider,
    minutes_played, total_distance_m, total_distance_km, hsr_distance_m,
    sprint_distance_m, sprint_frame_count, high_accel_count, high_decel_count,
    distance_per_minute_m, avg_speed_ms, max_speed_ms, total_off_ball_xt,
    avg_off_ball_xt.
    """
    # PR 5b note (ADR-011): this query joins dim_players_synced on
    # canonical_player_id already; player_key adoption here happens in PR 7
    # alongside the fct_physical_stats Kimball migration.
    tbl = t("fct_physical_stats_synced")
    dim = t("dim_players_synced")
    return execute_query(
        f"SELECT ps.player_id, COALESCE(dp.player_display_name, ps.player_id::text) AS player_name, "  # noqa: S608
        f"  ps.match_id, ps.source_provider, ps.minutes_played, "
        f"  ps.total_distance_m, ps.total_distance_km, ps.hsr_distance_m, ps.sprint_distance_m, "
        f"  ps.sprint_frame_count, ps.high_accel_count, ps.high_decel_count, "
        f"  ps.distance_per_minute_m, ps.avg_speed_ms, ps.max_speed_ms, "
        f"  ps.total_off_ball_xt, ps.avg_off_ball_xt "
        f"FROM {tbl} ps "
        f"LEFT JOIN {dim} dp ON ps.player_id::text = dp.canonical_player_id::text "
        f"WHERE ps.match_id = %s "
        f"ORDER BY ps.total_distance_m DESC",
        (str(match_id),),
    )


@ttl_cache()
def fetch_ppda_data(competition_id: int) -> pd.DataFrame:
    """Fetch PPDA data for a competition from match summary.

    Expected columns: match_id, match_date, home_team_name, away_team_name,
    home_ppda, away_ppda, home_possession_pct.
    """
    tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT match_key AS match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_ppda, away_ppda, home_possession_pct "
        f"FROM {tbl} "
        f"WHERE competition_id = %s AND home_ppda IS NOT NULL "
        f"ORDER BY match_date LIMIT 500",
        (int(competition_id),),
    )
