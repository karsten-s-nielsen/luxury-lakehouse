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
    player_ids: tuple[int, ...],
) -> pd.DataFrame:
    """Fetch per-90 stats for selected players, picking best season per player.

    Uses ROW_NUMBER() to select the season with most minutes, avoiding
    duplicates when a competition spans multiple seasons. LEFT JOINs
    physical stats averaged across tracking matches.

    ``player_ids`` MUST be a tuple so the cache key is deterministic — a
    mutable list argument would cache ``[1, 2]`` and ``[2, 1]`` as different
    entries despite producing the same SQL.

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


@ttl_cache()
def fetch_player_percentiles_batch(
    player_ids: tuple[int, ...],
    comp_id: int,
) -> dict[int, pd.DataFrame] | None:
    """Fetch percentile ranks for multiple players in one query.

    ``player_ids`` MUST be a tuple so the cache key is deterministic.

    Returns one of three disambiguated results:
      - ``dict[int, pd.DataFrame]`` (non-empty) — percentile rows keyed by
        player_id.
      - ``{}`` (empty dict) — query succeeded but no matching rows for the
        requested (comp_id, player_ids) combination.
      - ``None`` — the percentile feature is UNAVAILABLE for this app run:
        either no player_ids were passed, or the upstream
        ``fct_player_percentiles_synced`` table is missing / inaccessible.

    Callers can use ``if result:`` (truthy check) or
    ``bool(result) and len(result) == len(player_ids)`` for the "all
    percentiles present" check — both work identically for ``None`` and
    ``{}``. The ``None`` sentinel gives future callers the ability to
    distinguish "feature off-line" from "no matching rows".

    Caching note: an error return (``None`` from the RuntimeError branch)
    is cached for the TTL window.  This is intentional — it prevents
    hammering a broken ``fct_player_percentiles_synced`` endpoint on every
    radar render; the warning log still fires once per TTL window.
    """
    if not player_ids:
        return None
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
            player_df = df.loc[mask]
            if not player_df.empty:
                # Use `.iloc[[0]]` (double brackets) so the slice is always
                # a DataFrame, not a Series. `.head(1)` can return Series
                # under some pandas typings, breaking the dict[int, DataFrame]
                # invariant.
                result[pid] = player_df.iloc[[0]]
        return result
    except RuntimeError:
        logger.warning(
            "Player percentile data unavailable — feature unavailable for this request. "
            "If this persists, check that fct_player_percentiles_synced is up-to-date.",
            exc_info=True,
        )
        return None


# ---------------------------------------------------------------------------
# Player Similarity queries (from state/player_similarity.py)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_player_embedding_vector(
    table: str,
    player_id: str,
    competition_id: int | None,
    player_key: int | None = None,
) -> pd.DataFrame:
    """Fetch the target player's embedding vectors.

    PR 5b dual-read (ADR-011): when ``player_key`` is provided, filter on
    the Kimball BIGINT surrogate; otherwise fall back to
    ``canonical_player_id`` (the legacy path preserved through the
    2026-07-22 dual-column window). Default None preserves existing
    behaviour.

    Expected columns: behavioral_vector, stat_vector.
    """
    validate_param_id(player_id)
    tbl = t(table)
    if player_key is not None:
        if competition_id is not None:
            return execute_query(
                f"SELECT behavioral_vector, stat_vector "  # noqa: S608
                f"FROM {tbl} WHERE player_key = %s "
                f"AND competition_id = %s",
                (int(player_key), competition_id),
            )
        return execute_query(
            f"SELECT behavioral_vector, stat_vector "  # noqa: S608
            f"FROM {tbl} WHERE player_key = %s",
            (int(player_key),),
        )
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


@ttl_cache()
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
    player_key: int | None = None,
) -> pd.DataFrame:
    """Run pgvector cosine distance query to find similar players.

    All arguments are primitives (str / int / int | None), so the cache
    key is deterministic without any normalization.  Caching avoids
    re-running the cosine-distance scan for the same target player and
    filter combination on every re-render.

    PR 5b dual-read (ADR-011): when ``player_key`` is provided, the
    self-exclusion clause uses ``e.player_key != %s``; otherwise falls
    back to ``e.canonical_player_id != %s`` (legacy path). Default None
    preserves existing behaviour.

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
    excl_clause = "AND e.player_key != %s " if player_key is not None else "AND e.canonical_player_id != %s "
    excl_value: Any = int(player_key) if player_key is not None else player_id

    params: list[Any] = [vector_str, min_matches, excl_value, limit]
    if competition_id is not None:
        comp_filter = "AND e.competition_id = %s "
        params = [vector_str, min_matches, competition_id, excl_value, limit]

    return execute_query(
        f"SELECT e.canonical_player_id, p.player_display_name, "  # noqa: S608
        f"  p.data_sources, "
        f"  e.{total_col}, "
        f"  e.{vector_col}::text::vector({vector_dim}) <=> %s::vector({vector_dim}) AS distance "
        f"FROM {tbl} e "
        f"JOIN {dim_players_tbl} p "
        f"  ON e.canonical_player_id = p.canonical_player_id "
        f"WHERE e.{total_col} >= %s " + comp_filter + excl_clause + "ORDER BY distance LIMIT %s",
        tuple(params),
    )


@ttl_cache()
def fetch_similarity_radar_stats(
    canonical_player_ids: tuple[str, ...],
    competition_id: int | None,
) -> pd.DataFrame:
    """Load per-90 stats for radar comparison of two players.

    ``canonical_player_ids`` MUST be a tuple so the cache key is
    deterministic — a mutable list would cache ``["A", "B"]`` and
    ``["B", "A"]`` separately despite producing the same result.

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
