"""Tactical positions queries — shape graph position labels, formation labels, position maps.

Tracking-only scope (20 matches). All functions return pd.DataFrame.
SQL uses %s parameterized placeholders.

Note: fct_player_positions and fct_position_maps use string player_id from
tracking providers (e.g., "Home_1"). dim_players uses int player_id from
StatsBomb. The LEFT JOIN + COALESCE falls back to the raw tracking ID when
no dimension match exists, which is the expected case for tracking-only data.
"""

from __future__ import annotations

import logging

import pandas as pd

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


@ttl_cache()
def fetch_position_timeline(match_id: str, team: str) -> pd.DataFrame:
    """Per-frame position labels for a match + team.

    Joins fct_player_positions with dim_players for human-readable names.
    Falls back to raw player_id for tracking-only data (no dim_players match).

    Expected columns: frame_id, player_id, player_display_name,
    position_label, vertical_level, horizontal_level.
    """
    fp = t("fct_player_positions_synced")
    dp = t("dim_players_synced")
    return execute_query(
        f"SELECT "  # noqa: S608
        f"  fp.frame_id, "
        f"  fp.player_id, "
        f"  COALESCE(p.player_display_name, fp.player_id) AS player_display_name, "
        f"  fp.position_label, "
        f"  fp.vertical_level, "
        f"  fp.horizontal_level "
        f"FROM {fp} fp "
        f"LEFT JOIN {dp} p ON fp.player_id = p.player_id::text "
        f"WHERE fp.match_id = %s AND fp.team = %s "
        f"ORDER BY fp.frame_id, player_display_name "
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
    except Exception:
        logger.warning("fct_formation_labels_synced not available — formation labels will be empty")
        return pd.DataFrame()


@ttl_cache()
def fetch_position_maps(match_id: str, team: str, player_id: str | None = None) -> pd.DataFrame:
    """Aggregated position maps (pct_time per position) for a match + team.

    Joins fct_position_maps with dim_players for human-readable names.
    Falls back to raw player_id for tracking-only data.
    Optional player_id filter for single-player view.

    Expected columns: player_id, player_display_name, position_label,
    vertical_level, horizontal_level, pct_time, phase.
    """
    pm = t("fct_position_maps_synced")
    dp = t("dim_players_synced")

    where_parts = ["pm.match_id = %s", "pm.team = %s"]
    params: list[str] = [str(match_id), str(team)]

    if player_id is not None:
        where_parts.append("pm.player_id = %s")
        params.append(str(player_id))

    where = " AND ".join(where_parts)

    return execute_query(
        f"SELECT "  # noqa: S608
        f"  pm.player_id, "
        f"  COALESCE(p.player_display_name, pm.player_id) AS player_display_name, "
        f"  pm.position_label, "
        f"  pm.vertical_level, "
        f"  pm.horizontal_level, "
        f"  pm.pct_time, "
        f"  pm.phase "
        f"FROM {pm} pm "
        f"LEFT JOIN {dp} p ON pm.player_id = p.player_id::text "
        f"WHERE {where} "
        f"ORDER BY player_display_name, pm.pct_time DESC "
        f"LIMIT 2000",
        tuple(params),
    )


@ttl_cache()
def fetch_tp_players(match_id: str, team: str) -> pd.DataFrame:
    """Distinct player list from fct_position_maps for the player selector.

    Falls back to raw player_id as display name for tracking-only data.

    Expected columns: player_id, player_display_name.
    """
    pm = t("fct_position_maps_synced")
    dp = t("dim_players_synced")
    return execute_query(
        f"SELECT "  # noqa: S608
        f"  pm.player_id, "
        f"  COALESCE(p.player_display_name, pm.player_id) AS player_display_name "
        f"FROM {pm} pm "
        f"LEFT JOIN {dp} p ON pm.player_id = p.player_id::text "
        f"WHERE pm.match_id = %s AND pm.team = %s "
        f"GROUP BY pm.player_id, player_display_name "
        f"ORDER BY "
        f"  CASE WHEN pm.player_id ~ '^[0-9]+$' THEN pm.player_id::int ELSE 999 END, "
        f"  player_display_name "
        f"LIMIT 50",
        (str(match_id), str(team)),
    )
