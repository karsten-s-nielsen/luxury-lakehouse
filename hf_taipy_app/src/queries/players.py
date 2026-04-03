"""Player queries — stats, similarity, radar. Extracted from state/player_radar.py, state/player_similarity.py.

All functions return pd.DataFrame or typed containers. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache, validate_param_id

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Security allowlists for column-name interpolation
# ---------------------------------------------------------------------------
_ALLOWED_VECTOR_COLUMNS: frozenset[str] = frozenset({"behavioral_vector", "stat_vector"})
_ALLOWED_COUNT_COLUMNS: frozenset[str] = frozenset({"total_matches", "matches_in_sample"})


# ---------------------------------------------------------------------------
# Player Radar queries (from state/player_radar.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_player_radar_stats(
    comp_id: int,
    player_ids: list[int],
) -> pd.DataFrame:
    """Fetch per-90 stats for selected players, picking best season per player.

    Uses ROW_NUMBER() to select the season with most minutes, avoiding
    duplicates when a competition spans multiple seasons. LEFT JOINs
    physical stats averaged across tracking matches.

    Expected columns: player_id, player_display_name, minutes_played,
    goals_per_90, xg_per_90, passes_per_90, progressive_passes_per_90,
    pass_completion_pct, xg_overperformance, line_breaking_per_90,
    vaep_per_90, offensive_vaep_per_90, defensive_vaep_per_90,
    defcon_per_90, avg_distance_per_min, avg_max_speed_ms.
    """
    placeholders = ", ".join(["%s"] * len(player_ids))
    stats_tbl = t("fct_player_stats_synced")
    players_tbl = t("dim_players_synced")
    phys_tbl = t("fct_physical_stats_synced")

    return execute_query(
        f"SELECT sub.player_id, sub.player_display_name, "  # noqa: S608
        f"  sub.minutes_played, sub.goals_per_90, sub.xg_per_90, "
        f"  sub.passes_per_90, sub.progressive_passes_per_90, "
        f"  sub.pass_completion_pct, sub.xg_overperformance, "
        f"  sub.line_breaking_per_90, "
        f"  sub.vaep_per_90, sub.offensive_vaep_per_90, sub.defensive_vaep_per_90, "
        f"  sub.defcon_per_90, "
        f"  phys.avg_distance_per_min, phys.avg_max_speed_ms "
        f"FROM ("
        f"  SELECT ps.player_id, p.player_display_name, "
        f"    ps.minutes_played, ps.goals_per_90, ps.xg_per_90, "
        f"    ps.passes_per_90, ps.progressive_passes_per_90, "
        f"    ps.pass_completion_pct, ps.xg_overperformance, "
        f"    ps.line_breaking_per_90, "
        f"    ps.vaep_per_90, ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
        f"    ps.defcon_per_90, "
        f"    ROW_NUMBER() OVER (PARTITION BY ps.player_id ORDER BY ps.minutes_played DESC) AS rn "
        f"  FROM {stats_tbl} ps "
        f"  JOIN {players_tbl} p ON ps.player_id = p.player_id "
        f"  WHERE ps.competition_id = %s AND ps.player_id IN ({placeholders})"
        f") sub "
        f"LEFT JOIN ("
        f"  SELECT player_id, "
        f"    AVG(distance_per_minute_m) AS avg_distance_per_min, "
        f"    AVG(max_speed_ms) AS avg_max_speed_ms "
        f"  FROM {phys_tbl} "
        f"  GROUP BY player_id"
        f") phys ON sub.player_id::text = phys.player_id "
        f"WHERE sub.rn = 1",
        (comp_id, *player_ids),
    )


def fetch_player_percentiles_batch(
    player_ids: list[int],
    comp_id: int,
) -> dict[int, pd.DataFrame]:
    """Fetch percentile ranks for multiple players in one query.

    Returns a dict mapping player_id to a single-row DataFrame.
    Empty dict if the synced table doesn't exist or no data found.
    """
    if not player_ids:
        return {}
    try:
        pctile_tbl = t("fct_player_percentiles_synced")
        placeholders = ", ".join(["%s"] * len(player_ids))
        df = execute_query(
            f"SELECT * FROM {pctile_tbl} "  # noqa: S608
            f"WHERE player_id IN ({placeholders}) AND competition_id = %s",
            (*[str(pid) for pid in player_ids], comp_id),
        )
        if df.empty:
            return {}
        result: dict[int, pd.DataFrame] = {}
        for pid in player_ids:
            mask = df["player_id"].astype(str) == str(pid)
            player_df = df[mask]
            if not player_df.empty:
                result[pid] = player_df.head(1)
        return result
    except Exception:
        logger.debug("Percentile data unavailable (table may not be synced yet)")
        return {}


# ---------------------------------------------------------------------------
# Player Similarity queries (from state/player_similarity.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_player_embedding_vector(
    table: str,
    player_id: str,
    competition_id: int | None,
) -> pd.DataFrame:
    """Fetch the target player's embedding vectors.

    Expected columns: behavioral_vector, stat_vector.
    """
    validate_param_id(player_id)
    tbl = t(table)
    if competition_id is not None:
        return execute_query(
            f"SELECT behavioral_vector, stat_vector "  # noqa: S608
            f"FROM {tbl} WHERE canonical_player_id = %s "
            f"AND competition_id = %s",
            (player_id, competition_id),
        )
    return execute_query(
        f"SELECT behavioral_vector, stat_vector "  # noqa: S608
        f"FROM {tbl} WHERE canonical_player_id = %s",
        (player_id,),
    )


def search_similar_players(
    table: str,
    vector_str: str,
    vector_col: str,
    vector_dim: int,
    total_col: str,
    player_id: str,
    min_matches: int,
    limit: int,
    competition_id: int | None,
) -> pd.DataFrame:
    """Run pgvector cosine distance query to find similar players.

    Expected columns: canonical_player_id, player_display_name,
    data_sources, <total_col>, distance.
    """
    if vector_col not in _ALLOWED_VECTOR_COLUMNS:
        msg = f"Invalid vector column: {vector_col}"
        raise ValueError(msg)
    if total_col not in _ALLOWED_COUNT_COLUMNS:
        msg = f"Invalid count column: {total_col}"
        raise ValueError(msg)

    tbl = t(table)
    dim_players_tbl = t("dim_players_synced")

    comp_filter = ""
    params: list[Any] = [vector_str, min_matches, player_id, limit]
    if competition_id is not None:
        comp_filter = "AND e.competition_id = %s "
        params = [vector_str, min_matches, competition_id, player_id, limit]

    return execute_query(
        f"SELECT e.canonical_player_id, p.player_display_name, "  # noqa: S608
        f"  p.data_sources, "
        f"  e.{total_col}, "
        f"  e.{vector_col}::text::vector({vector_dim}) <=> %s::vector({vector_dim}) AS distance "
        f"FROM {tbl} e "
        f"JOIN {dim_players_tbl} p "
        f"  ON e.canonical_player_id = p.canonical_player_id "
        f"WHERE e.{total_col} >= %s " + comp_filter + "  AND e.canonical_player_id != %s "
        "ORDER BY distance LIMIT %s",
        tuple(params),
    )


@ttl_cache()
def fetch_similarity_radar_stats(
    canonical_player_ids: list[str],
    competition_id: int | None,
) -> pd.DataFrame:
    """Load per-90 stats for radar comparison of two players.

    Expected columns: canonical_player_id, player_display_name,
    minutes_played, goals_per_90, xg_per_90, passes_per_90,
    progressive_passes_per_90, pass_completion_pct, xg_overperformance,
    line_breaking_per_90, vaep_per_90, offensive_vaep_per_90,
    defensive_vaep_per_90, defcon_per_90.
    """
    pids = tuple(str(pid) for pid in canonical_player_ids)
    placeholders = ", ".join(["%s"] * len(pids))
    stats_tbl = t("fct_player_stats_synced")
    players_tbl = t("dim_players_synced")

    comp_clause = ""
    params: list[Any] = list(pids)
    if competition_id is not None:
        comp_clause = "AND ps.competition_id = %s "
        params.append(competition_id)

    return execute_query(
        f"SELECT sub.canonical_player_id, sub.player_display_name, "  # noqa: S608
        f"  sub.minutes_played, sub.goals_per_90, sub.xg_per_90, "
        f"  sub.passes_per_90, sub.progressive_passes_per_90, "
        f"  sub.pass_completion_pct, sub.xg_overperformance, "
        f"  sub.line_breaking_per_90, "
        f"  sub.vaep_per_90, sub.offensive_vaep_per_90, sub.defensive_vaep_per_90, "
        f"  sub.defcon_per_90 "
        f"FROM ("
        f"  SELECT p.canonical_player_id, p.player_display_name, "
        f"    ps.minutes_played, ps.goals_per_90, ps.xg_per_90, "
        f"    ps.passes_per_90, ps.progressive_passes_per_90, "
        f"    ps.pass_completion_pct, ps.xg_overperformance, "
        f"    ps.line_breaking_per_90, "
        f"    ps.vaep_per_90, ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
        f"    ps.defcon_per_90, "
        f"    ROW_NUMBER() OVER (PARTITION BY p.canonical_player_id ORDER BY ps.minutes_played DESC) AS rn "
        f"  FROM {stats_tbl} ps "
        f"  JOIN {players_tbl} p ON ps.player_id = p.player_id "
        f"  WHERE p.canonical_player_id IN ({placeholders}) " + comp_clause + ") sub "
        "WHERE sub.rn = 1",
        tuple(params),
    )
