"""Tactical positions queries — shape graph position labels, formation labels, position maps.

Tracking-only scope (20 matches). All functions return pd.DataFrame.
SQL uses %s parameterized placeholders.

Player and team display names are resolved in the gold-layer dbt models
(fct_player_positions, fct_position_maps) via LEFT JOIN to
stg_tracking__player_metadata. No dim_players JOIN needed here.
"""

from __future__ import annotations

import logging

import pandas as pd

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


@ttl_cache()
def fetch_position_timeline(match_id: str, team: str) -> pd.DataFrame:
    """Per-frame position labels for a match + team.

    Uses player_display_name resolved in the gold table.

    Expected columns: frame_id, player_id, player_display_name,
    position_label, vertical_level, horizontal_level.
    """
    fp = t("fct_player_positions_synced")
    return execute_query(
        f"SELECT "  # noqa: S608
        f"  fp.frame_id, "
        f"  fp.player_id, "
        f"  fp.player_display_name, "
        f"  fp.position_label, "
        f"  fp.vertical_level, "
        f"  fp.horizontal_level "
        f"FROM {fp} fp "
        f"WHERE fp.match_id = %s AND fp.team = %s "
        f"ORDER BY fp.frame_id, fp.player_display_name "
        f"LIMIT 50000",
        (str(match_id), str(team)),
    )


@ttl_cache()
def fetch_formation_labels_dual(match_id: str, team: str) -> pd.DataFrame:
    """Formation labels for BOTH detectors (EFPI and shape_graph).

    Expected columns: period, window_start_s, window_end_s, formation_label,
    cost, detector.
    Returns empty DataFrame if the synced table does not exist yet.
    """
    try:
        tbl = t("fct_formation_labels_synced")
        return execute_query(
            f"SELECT period, window_start_s, window_end_s, "  # noqa: S608
            f"  formation_label, cost, detector "
            f"FROM {tbl} "
            f"WHERE match_id = %s AND team = %s "
            f"ORDER BY detector, period, window_start_s "
            f"LIMIT 500",
            (str(match_id), str(team)),
        )
    except RuntimeError:
        logger.warning("fct_formation_labels_synced not available — formation labels will be empty")
        return pd.DataFrame()


@ttl_cache()
def fetch_position_maps(match_id: str, team: str, player_id: str | None = None) -> pd.DataFrame:
    """Aggregated position maps (pct_time per position) for a match + team.

    Uses player_display_name resolved in the gold table.
    Optional player_id filter for single-player view.

    Expected columns: player_id, player_display_name, position_label,
    vertical_level, horizontal_level, pct_time, phase.
    """
    pm = t("fct_position_maps_synced")

    where_parts = ["pm.match_id = %s", "pm.team = %s"]
    params: list[str] = [str(match_id), str(team)]

    if player_id is not None:
        where_parts.append("pm.player_id = %s")
        params.append(str(player_id))

    where = " AND ".join(where_parts)

    return execute_query(
        f"SELECT "  # noqa: S608
        f"  pm.player_id, "
        f"  pm.player_display_name, "
        f"  pm.position_label, "
        f"  pm.vertical_level, "
        f"  pm.horizontal_level, "
        f"  pm.pct_time, "
        f"  pm.phase "
        f"FROM {pm} pm "
        f"WHERE {where} "
        f"ORDER BY pm.player_display_name, pm.pct_time DESC "
        f"LIMIT 2000",
        tuple(params),
    )


@ttl_cache()
def fetch_tp_players(match_id: str, team: str) -> pd.DataFrame:
    """Distinct player list from fct_position_maps for the player selector.

    Uses player_display_name resolved in the gold table.

    Expected columns: player_id, player_display_name.
    """
    pm = t("fct_position_maps_synced")
    return execute_query(
        f"SELECT "  # noqa: S608
        f"  pm.player_id, "
        f"  pm.player_display_name "
        f"FROM {pm} pm "
        f"WHERE pm.match_id = %s AND pm.team = %s "
        f"GROUP BY pm.player_id, pm.player_display_name "
        f"ORDER BY "
        f"  CASE WHEN pm.player_id ~ '^[0-9]+$' THEN pm.player_id::int ELSE 999 END, "
        f"  pm.player_display_name "
        f"LIMIT 50",
        (str(match_id), str(team)),
    )


@ttl_cache()
def fetch_tracking_teams(match_id: str) -> pd.DataFrame:
    """Team display names from position maps for a match.

    Returns one row per team with team_side and team_display_name.
    Used as fallback when fetch_match_events returns empty (tracking-only matches).

    Expected columns: team, team_display_name.
    """
    pm = t("fct_position_maps_synced")
    return execute_query(
        f"SELECT DISTINCT pm.team, pm.team_display_name "  # noqa: S608
        f"FROM {pm} pm "
        f"WHERE pm.match_id = %s "
        f"LIMIT 10",
        (str(match_id),),
    )
