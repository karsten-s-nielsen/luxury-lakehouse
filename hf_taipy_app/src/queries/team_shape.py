"""Team shape + formation queries — extracted from state/team_shape.py.

All functions return pd.DataFrame or tuples. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import logging

import pandas as pd

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


@ttl_cache()
def fetch_ts_frame_data(match_id: str, frame: int) -> pd.DataFrame:
    """Load all player rows for a specific frame (~22 rows).

    Expected columns: player_id, team, x, y, speed.
    """
    tbl = t("fct_tracking_frames_synced")
    return execute_query(
        f"SELECT player_id, team, x, y, speed "  # noqa: S608
        f"FROM {tbl} "
        f"WHERE match_id = %s AND frame = %s "
        f"LIMIT 50",
        (str(match_id), int(frame)),
    )


@ttl_cache()
def fetch_phase_averages(match_id: str, period: int) -> pd.DataFrame:
    """Average positions per player for a full period (~22 rows).

    Reads from pre-aggregated fct_tracking_avg_positions_synced
    (one row per match/period/player) instead of scanning ~1M raw frames.

    Expected columns: player_id, team, x, y, speed.
    """
    tbl = t("fct_tracking_avg_positions_synced")
    return execute_query(
        f"SELECT player_id, team, avg_x AS x, avg_y AS y, avg_speed AS speed "  # noqa: S608
        f"FROM {tbl} "
        f"WHERE match_id = %s AND period = %s "
        f"ORDER BY team, player_id "
        f"LIMIT 50",
        (str(match_id), int(period)),
    )


@ttl_cache()
def fetch_full_match_averages(match_id: str) -> pd.DataFrame:
    """Average positions per player across the full match (~22 rows).

    Uses pre-aggregated fct_tracking_avg_positions_synced, computing a
    frame-weighted average across periods to get whole-match values.

    Expected columns: player_id, team, x, y, speed.
    """
    tbl = t("fct_tracking_avg_positions_synced")
    return execute_query(
        f"SELECT player_id, team, "  # noqa: S608
        f"  SUM(avg_x * frame_count) / SUM(frame_count) AS x, "
        f"  SUM(avg_y * frame_count) / SUM(frame_count) AS y, "
        f"  SUM(avg_speed * frame_count) / SUM(frame_count) AS speed "
        f"FROM {tbl} "
        f"WHERE match_id = %s "
        f"GROUP BY player_id, team "
        f"ORDER BY team, player_id "
        f"LIMIT 50",
        (str(match_id),),
    )


@ttl_cache()
def fetch_sampled_positions(match_id: str, period: int, sample_interval_s: int = 5) -> pd.DataFrame:
    """Fetch pre-bucketed positions from fct_tracking_shape_timeline_synced.

    The dbt model pre-computes 5-second time buckets, so this is a simple
    indexed read (~12K rows per half) instead of a GROUP BY over ~1M raw frames.

    Expected columns: player_id, team, period, time_bucket, x, y, speed.
    """
    tbl = t("fct_tracking_shape_timeline_synced")
    return execute_query(
        f"SELECT player_id, team, period, time_bucket, "  # noqa: S608
        f"  avg_x AS x, avg_y AS y, avg_speed AS speed "
        f"FROM {tbl} "
        f"WHERE match_id = %s AND period = %s "
        f"ORDER BY time_bucket, team, player_id "
        f"LIMIT 50000",
        (str(match_id), int(period)),
    )


@ttl_cache()
def fetch_ts_frame_range(match_id: str, period: int) -> tuple[int, int, int]:
    """Get min/max frame numbers and frame rate for a match + period.

    Uses pre-aggregated fct_tracking_avg_positions_synced which already
    stores min_frame, max_frame, and frame_rate per match/period.
    Returns (min_frame, max_frame, fps).
    """
    tbl = t("fct_tracking_avg_positions_synced")
    df = execute_query(
        f"SELECT MIN(min_frame) AS min_frame, MAX(max_frame) AS max_frame, "  # noqa: S608
        f"  MAX(frame_rate) AS fps "
        f"FROM {tbl} "
        f"WHERE match_id = %s AND period = %s "
        f"LIMIT 1",
        (str(match_id), int(period)),
    )
    if df.empty:
        return (0, 0, 25)
    row = df.iloc[0]
    return (
        int(row["min_frame"] or 0),
        int(row["max_frame"] or 0),
        int(row["fps"] or 25),
    )


@ttl_cache()
def fetch_formation_labels(match_id: str, team: str) -> pd.DataFrame:
    """Fetch formation labels for a match + team from fct_formation_labels_synced.

    Expected columns: period, window_start_s, window_end_s, formation_label, cost.
    Returns empty DataFrame if the synced table does not exist yet.
    """
    try:
        tbl = t("fct_formation_labels_synced")
        return execute_query(
            f"SELECT period, window_start_s, window_end_s, formation_label, cost "  # noqa: S608
            f"FROM {tbl} "
            f"WHERE match_id = %s AND team = %s "
            f"ORDER BY period, window_start_s "
            f"LIMIT 500",
            (str(match_id), str(team)),
        )
    except Exception:
        logger.warning("fct_formation_labels_synced not available — formation labels will be empty")
        return pd.DataFrame()


@ttl_cache()
def fetch_match_events(match_id: str) -> pd.DataFrame:
    """Fetch goals and substitutions from match summary for timeline annotations.

    Expected columns: match_id, home_team_name, away_team_name,
    home_score, away_score.
    Returns empty DataFrame on error.
    """
    try:
        tbl = t("fct_match_summary_synced")
        return execute_query(
            f"SELECT match_id, home_team_name, away_team_name, "  # noqa: S608
            f"  home_score, away_score "
            f"FROM {tbl} "
            f"WHERE match_id::text = %s "
            f"LIMIT 1",
            (str(match_id),),
        )
    except Exception:
        logger.warning("Could not fetch match events for timeline annotations")
        return pd.DataFrame()
