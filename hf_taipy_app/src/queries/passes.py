"""Pass-related queries — extracted from state/pass_map.py, state/pass_network.py, state/pass_timing.py.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache

# ---------------------------------------------------------------------------
# Pass Map queries (from state/pass_map.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_passes(
    comp_id: int,
    team_id: int,
    match_id: int,
) -> pd.DataFrame:
    """Fetch passes for a specific team in a specific match.

    Expected columns: start_x, start_y, end_x, end_y, is_complete,
    is_progressive, is_line_breaking, minute, second.
    """
    tbl = t("fct_passes_synced")
    conditions = ["competition_id = %s", "team_id = %s", "match_id = %s"]
    params: list[Any] = [int(comp_id), int(team_id), int(match_id)]

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT start_x, start_y, end_x, end_y, is_complete, is_progressive, "  # noqa: S608
        f"  is_line_breaking, minute, second "
        f"FROM {tbl} "
        f"WHERE {where} "
        f"ORDER BY minute, second LIMIT 2000",
        tuple(params),
    )


# ---------------------------------------------------------------------------
# Pass Network queries (from state/pass_network.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_network_passes(comp_id: int, team_id: int, match_id: int) -> pd.DataFrame:
    """Fetch completed passes with passer/receiver names for network construction.

    Expected columns: player_id, pass_recipient_id, start_x, start_y,
    end_x, end_y, is_complete, passer_name, receiver_name.
    """
    passes_tbl = t("fct_passes_synced")
    players_tbl = t("dim_players_synced")
    return execute_query(
        f"SELECT p.player_id, p.pass_recipient_id, "  # noqa: S608
        f"  p.start_x, p.start_y, p.end_x, p.end_y, p.is_complete, "
        f"  passer.player_display_name AS passer_name, "
        f"  receiver.player_display_name AS receiver_name "
        f"FROM {passes_tbl} p "
        f"JOIN {players_tbl} passer ON p.player_id = passer.player_id "
        f"LEFT JOIN {players_tbl} receiver ON p.pass_recipient_id = receiver.player_id "
        f"WHERE p.competition_id = %s AND p.team_id = %s AND p.match_id = %s "
        f"  AND p.is_complete = true AND p.pass_recipient_id IS NOT NULL "
        f"ORDER BY p.minute, p.second LIMIT 2000",
        (comp_id, team_id, match_id),
    )


# ---------------------------------------------------------------------------
# Pass Timing (PAUSA) queries (from state/pass_timing.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_pausa_summary(match_id: str, team: str | None, player_id: str | None) -> pd.DataFrame:
    """Load aggregate PAUSA metrics for current filter selection.

    Expected columns: avg_pausa, avg_temporal, avg_spatial, pass_count.
    """
    pausa_tbl = t("fct_pausa_values_synced")
    conditions = ["match_id = %s"]
    params: list[Any] = [match_id]

    if team:
        conditions.append("team = %s")
        params.append(team)
    if player_id:
        conditions.append("player_id = %s")
        params.append(player_id)

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT "  # noqa: S608
        f"  AVG(pausa_score) AS avg_pausa, "
        f"  AVG(temporal_judgment) AS avg_temporal, "
        f"  AVG(spatial_selection) AS avg_spatial, "
        f"  COUNT(*) AS pass_count "
        f"FROM {pausa_tbl} WHERE {where}",
        tuple(params),
    )


@ttl_cache()
def fetch_pausa_passes(match_id: str, team: str | None, player_id: str | None) -> pd.DataFrame:
    """Load individual pass PAUSA scores for scatter/heatmap (bounded).

    Expected columns: pass_id, player_id, player_display_name, team,
    temporal_judgment, spatial_selection, pausa_score, actual_obso,
    peak_obso, optimal_obso, receiver_x, receiver_y.
    """
    pausa_tbl = t("fct_pausa_values_synced")
    dim_tbl = t("dim_players_synced")
    conditions = ["pv.match_id = %s"]
    params: list[Any] = [match_id]

    if team:
        conditions.append("pv.team = %s")
        params.append(team)
    if player_id:
        conditions.append("pv.player_id = %s")
        params.append(player_id)

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT pv.pass_id, pv.player_id, dp.player_display_name, pv.team, "  # noqa: S608
        f"  pv.temporal_judgment, pv.spatial_selection, pv.pausa_score, "
        f"  pv.actual_obso, pv.peak_obso, pv.optimal_obso, "
        f"  pv.receiver_x, pv.receiver_y "
        f"FROM {pausa_tbl} pv "
        f"LEFT JOIN {dim_tbl} dp ON pv.player_id::text = dp.player_id::text "
        f"WHERE {where} "
        f"LIMIT 2000",
        tuple(params),
    )


@ttl_cache()
def fetch_pass_timing_rankings() -> pd.DataFrame:
    """Load fct_pass_timing rankings (bounded).

    Expected columns: player_display_name, match_label, pass_count,
    avg_pausa, avg_temporal_judgment, avg_spatial_selection,
    median_pausa, passes_above_median_pausa.
    """
    timing_tbl = t("fct_pass_timing_synced")
    match_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT COALESCE(pt.player_display_name, pt.player_id) AS player_display_name, "  # noqa: S608
        f"  COALESCE(ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    'Match ' || pt.match_id) AS match_label, "
        f"  pt.pass_count, "
        f"  pt.avg_pausa, pt.avg_temporal_judgment, pt.avg_spatial_selection, "
        f"  pt.median_pausa, pt.passes_above_median_pausa "
        f"FROM {timing_tbl} pt "
        f"LEFT JOIN {match_tbl} ms ON pt.match_id::text = ms.match_id::text "
        f"ORDER BY pt.avg_pausa DESC "
        f"LIMIT 500",
    )


@ttl_cache(ttl=600)
def fetch_pausa_aggregate_rankings() -> pd.DataFrame:
    """Load fct_pausa_rankings (player-level aggregate, bounded).

    Expected columns: player_display_name, total_matches, total_passes,
    passes_with_value, avg_pausa, avg_temporal_judgment,
    avg_spatial_selection, median_pausa, total_minutes.
    """
    rankings_tbl = t("fct_pausa_rankings_synced")
    return execute_query(
        f"SELECT player_display_name, total_matches, total_passes, "  # noqa: S608
        f"  passes_with_value, avg_pausa, avg_temporal_judgment, "
        f"  avg_spatial_selection, median_pausa, total_minutes "
        f"FROM {rankings_tbl} "
        f"ORDER BY avg_pausa DESC "
        f"LIMIT 500",
    )
