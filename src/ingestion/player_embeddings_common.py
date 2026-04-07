"""Shared helpers for player embedding pipelines (v1 Doc2Vec and v2 transformer).

Contains constants, z-score normalization, stat vector computation, event
loading, and bronze DataFrame assembly used by both embedding paths.

Bronze table produced:
  - player_embeddings_raw
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd

from shared.constants import DEFAULT_GOLD_SCHEMA

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

_TABLE_NAME = "player_embeddings_raw"
_PLAYERS_PER_BATCH = 100

STAT_FEATURES_BY_GROUP: dict[str, tuple[str, ...]] = {
    "Goalkeeper": (
        "save_pct",
        "gk_xt_per_pass",
        "launch_rate",
        "claim_success_rate",
        "goals_prevented_per_90",
        "psxg_per_shot_faced",
        "avg_defensive_action_distance",
        "actions_outside_box_per_90",
        "clean_sheet_pct",
        "saves_per_90",
        "distribution_passes_per_90",
        "gk_xt_delta_total_per_90",
        "punches_per_90",
    ),
    "Defender": (
        "goals_per_90",
        "xg_per_90",
        "passes_per_90",
        "pass_completion_pct",
        "progressive_passes_per_90",
        "line_breaking_per_90",
        "vaep_per_90",
        "offensive_vaep_per_90",
        "defensive_vaep_per_90",
        "defcon_per_90",
        "intercept_per_90",
        "deter_per_90",
        "xg_overperformance",
    ),
    "Midfielder": (
        "goals_per_90",
        "xg_per_90",
        "passes_per_90",
        "pass_completion_pct",
        "progressive_passes_per_90",
        "line_breaking_per_90",
        "vaep_per_90",
        "offensive_vaep_per_90",
        "defensive_vaep_per_90",
        "defcon_per_90",
        "intercept_per_90",
        "deter_per_90",
        "xg_overperformance",
    ),
    "Forward": (
        "goals_per_90",
        "xg_per_90",
        "passes_per_90",
        "pass_completion_pct",
        "progressive_passes_per_90",
        "line_breaking_per_90",
        "vaep_per_90",
        "offensive_vaep_per_90",
        "defensive_vaep_per_90",
        "defcon_per_90",
        "intercept_per_90",
        "deter_per_90",
        "xg_overperformance",
    ),
}

# Backwards compatibility — outfield features as an immutable tuple.
STAT_FEATURES: tuple[str, ...] = STAT_FEATURES_BY_GROUP["Defender"]

# SQL expressions for GK features that need per-90 or ratio derivation.
# Features not listed here fall back to simple ``AVG(gk.{f})``.
GK_FEATURE_SQL: dict[str, str] = {
    "goals_prevented_per_90": (
        "AVG(CASE WHEN gk.minutes_played > 0 THEN gk.goals_prevented / gk.minutes_played * 90 ELSE NULL END)"
    ),
    "psxg_per_shot_faced": (
        "AVG(CASE WHEN (gk.saves + gk.goals_conceded) > 0"
        " THEN gk.psxg_faced / (gk.saves + gk.goals_conceded) ELSE NULL END)"
    ),
    "clean_sheet_pct": "AVG(CASE WHEN gk.goals_conceded = 0 THEN 1.0 ELSE 0.0 END)",
    "saves_per_90": ("AVG(CASE WHEN gk.minutes_played > 0 THEN gk.saves / gk.minutes_played * 90 ELSE NULL END)"),
    "distribution_passes_per_90": (
        "AVG(CASE WHEN gk.minutes_played > 0 THEN gk.distribution_passes / gk.minutes_played * 90 ELSE NULL END)"
    ),
    "gk_xt_delta_total_per_90": (
        "AVG(CASE WHEN gk.minutes_played > 0 THEN gk.gk_xt_delta_total / gk.minutes_played * 90 ELSE NULL END)"
    ),
    "punches_per_90": ("AVG(CASE WHEN gk.minutes_played > 0 THEN gk.punches / gk.minutes_played * 90 ELSE NULL END)"),
}


# ---------------------------------------------------------------------------
# Z-score normalization
# ---------------------------------------------------------------------------


def _zscore_normalize(
    df: pd.DataFrame,
    features: Sequence[str],
) -> tuple[pd.DataFrame, dict[str, dict[str, float]]]:
    """Z-score normalize feature columns in-place.

    For each feature: ``(value - mean) / std``.  If std == 0, all values
    become 0.  NULL values remain NULL.

    Args:
        df: DataFrame with the feature columns.
        features: Column names to normalize.

    Returns:
        Tuple of (normalized DataFrame copy, params dict mapping
        feature name to {"mean": float, "std": float}).
    """
    result = df.copy()
    params: dict[str, dict[str, float]] = {}

    for feat in features:
        col = result[feat].astype(float)
        mean_val = float(col.mean(skipna=True))
        std_val = float(col.std(ddof=0, skipna=True))

        if np.isnan(mean_val):
            mean_val = 0.0
        if np.isnan(std_val) or std_val == 0.0:
            std_val = 0.0

        params[feat] = {"mean": mean_val, "std": std_val}

        if std_val == 0.0:
            # All values identical or all null — set non-null to 0
            result[feat] = col.where(col.isna(), 0.0)
        else:
            result[feat] = (col - mean_val) / std_val

    return result, params


def _save_norm_params(
    catalog: str,
    params: dict[str, dict[str, dict[str, float]]],
    logger: logging.Logger,
) -> None:
    """Save per-position-group normalization parameters to UC Volumes as JSON.

    Args:
        catalog: Unity Catalog name.
        params: Normalization parameters keyed by position group, then
            feature name, then mean/std.
        logger: Logger instance.
    """
    import os

    model_dir = f"/Volumes/{catalog}/dev_gold/model_weights/football2vec"
    params_path = os.path.join(model_dir, "zscore_params.json")

    try:
        with open(params_path, "w") as f:
            json.dump(params, f, indent=2)
        logger.info("Saved normalization params to %s", params_path)
    except OSError:
        logger.warning("Could not save normalization params to %s", params_path)


# ---------------------------------------------------------------------------
# Event loading — SPADL actions (Spark-native — no .toPandas())
# ---------------------------------------------------------------------------


def _load_events_sdf(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    match_ids: set[str] | None = None,
) -> SparkDataFrame:
    """Load SPADL actions joined to dim_players as a Spark DF.

    Reads from ``fct_action_values`` (23-type SPADL vocabulary, 105x68m
    coordinate system) instead of raw provider events.  Source-agnostic:
    StatsBomb and Wyscout events are already unified by the SPADL converter.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name (unused — queries gold directly).
        match_ids: If provided, only load actions for these match IDs.

    Returns:
        Spark DataFrame with columns: canonical_player_id, match_id,
        action_type, start_x, start_y, event_index, data_source,
        competition_id, season_id.

    Note:
        Competition and season metadata comes from ``fct_action_values``
        directly (not ``fct_match_summary``), because SPADL match IDs
        use a different namespace (Wyscout IDs) than ``fct_match_summary``
        (StatsBomb IDs).
    """
    _ = schema
    gold = DEFAULT_GOLD_SCHEMA

    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(av.match_id AS STRING) AS match_id,
            av.action_type,
            CAST(av.start_x AS DOUBLE) AS start_x,
            CAST(av.start_y AS DOUBLE) AS start_y,
            CAST(av.time_seconds * 1000 AS INT) AS event_index,
            av.data_source,
            CAST(av.competition_id AS STRING) AS competition_id,
            CAST(av.season_id AS STRING) AS season_id
        FROM {catalog}.{gold}.fct_action_values av
        INNER JOIN {catalog}.{gold}.dim_players dp
            ON av.player_id = dp.player_id
        WHERE av.player_id IS NOT NULL
          AND dp.canonical_player_id IS NOT NULL
    """  # noqa: S608
    events_sdf = spark.sql(query)

    if match_ids:
        from pyspark.sql import functions as spark_fn

        events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(list(match_ids)))

    return events_sdf


# ---------------------------------------------------------------------------
# Backward-compatible wrapper (used by tests)
# ---------------------------------------------------------------------------


def _load_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    match_ids: set[str] | None = None,
) -> pd.DataFrame:
    """Load SPADL actions and collect to pandas (backward-compatible wrapper).

    .. deprecated::
        Prefer ``_load_events_sdf`` for distributed processing.  This wrapper
        exists for test compatibility and small ad-hoc queries only.
    """
    return _load_events_sdf(spark, catalog, schema, match_ids=match_ids).toPandas()


# ---------------------------------------------------------------------------
# Stat vector computation
# ---------------------------------------------------------------------------


def _load_outfield_stats(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    group_name: str,
    features: Sequence[str],
    player_ids: set[int] | None,
) -> pd.DataFrame:
    """Load per-player-competition-season outfield stats from ``fct_player_stats``.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        gold_schema: Gold schema name.
        group_name: Position group to filter (Defender, Midfielder, Forward).
        features: Stat feature column names to select.
        player_ids: Optional set of canonical player IDs to restrict the query.

    Returns:
        Pandas DataFrame with canonical_player_id, competition_id, season_id,
        position_group, and the requested feature columns.
    """
    feature_cols = ", ".join(f"ps.{f}" for f in features)
    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(ps.competition_id AS STRING) AS competition_id,
            CAST(ps.season_id AS STRING) AS season_id,
            dp.position_group,
            {feature_cols}
        FROM {catalog}.{gold_schema}.fct_player_stats ps
        INNER JOIN {catalog}.{gold_schema}.dim_players dp
            ON ps.player_id = dp.player_id
        WHERE dp.canonical_player_id IS NOT NULL
            AND dp.position_group = '{group_name}'
    """  # noqa: S608
    sdf = spark.sql(query)
    if player_ids:
        id_list = [str(pid) for pid in player_ids]
        sdf = sdf.filter(sdf["canonical_player_id"].isin(id_list))
    return sdf.limit(50_000).toPandas()


def _load_goalkeeper_stats(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    features: Sequence[str],
    player_ids: set[int] | None,
) -> pd.DataFrame:
    """Load per-player-competition-season goalkeeper stats from ``fct_goalkeeper_stats``.

    ``fct_goalkeeper_stats`` is per-match grain, so we aggregate to
    per-(player, competition, season) with ``AVG`` for rate metrics.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        gold_schema: Gold schema name.
        features: Stat feature column names to aggregate (e.g. save_pct).
        player_ids: Optional set of canonical player IDs to restrict the query.

    Returns:
        Pandas DataFrame with canonical_player_id, competition_id, season_id,
        position_group='Goalkeeper', and the requested feature columns.
    """
    select_parts: list[str] = []
    for f in features:
        sql_expr = GK_FEATURE_SQL.get(f, f"AVG(gk.{f})")
        select_parts.append(f"{sql_expr} AS {f}")
    agg_cols = ", ".join(select_parts)
    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(gk.competition_id AS STRING) AS competition_id,
            CAST(gk.season_id AS STRING) AS season_id,
            'Goalkeeper' AS position_group,
            {agg_cols}
        FROM {catalog}.{gold_schema}.fct_goalkeeper_stats gk
        INNER JOIN {catalog}.{gold_schema}.dim_players dp
            ON gk.player_id = dp.player_id
        WHERE dp.canonical_player_id IS NOT NULL
            AND dp.position_group = 'Goalkeeper'
        GROUP BY dp.canonical_player_id, gk.competition_id, gk.season_id
    """  # noqa: S608
    sdf = spark.sql(query)
    if player_ids:
        id_list = [str(pid) for pid in player_ids]
        sdf = sdf.filter(sdf["canonical_player_id"].isin(id_list))
    return sdf.limit(50_000).toPandas()


def _compute_stat_vectors(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    player_ids: set[int] | None = None,
) -> tuple[pd.DataFrame, dict[str, dict[str, dict[str, float]]]]:
    """Load stats per position group, z-score normalize, and return stat vectors.

    Each position group has its own feature set (see ``STAT_FEATURES_BY_GROUP``):
    goalkeeper stats come from ``fct_goalkeeper_stats`` (aggregated to
    player-competition-season), outfield stats from ``fct_player_stats``.
    Z-score normalization is applied within each group independently.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        gold_schema: Gold schema name (e.g. ``dev_gold``).
        player_ids: If provided, only load stats for these canonical player IDs.
            Prevents unbounded ``.toPandas()`` on full tables.

    Returns:
        Tuple of (DataFrame with canonical_player_id, competition_id,
        season_id, stat_vector columns; normalization params dict keyed
        by position group, then feature name, then mean/std).
    """
    empty_result = pd.DataFrame(
        {
            "canonical_player_id": pd.Series(dtype="str"),
            "competition_id": pd.Series(dtype="str"),
            "season_id": pd.Series(dtype="str"),
            "stat_vector": pd.Series(dtype="object"),
        }
    )

    normalized_groups: list[pd.DataFrame] = []
    all_params: dict[str, dict[str, dict[str, float]]] = {}

    for group_name, features in STAT_FEATURES_BY_GROUP.items():
        # Load group-specific stats from the appropriate source table
        if group_name == "Goalkeeper":
            group_df = _load_goalkeeper_stats(spark, catalog, gold_schema, features, player_ids)
        else:
            group_df = _load_outfield_stats(spark, catalog, gold_schema, group_name, features, player_ids)

        if group_df.empty:
            continue

        # Z-score normalize within this position group
        norm_group, group_params = _zscore_normalize(group_df, features)
        all_params[group_name] = group_params

        # Build stat_vector column from the group-specific features
        stat_arr = norm_group[list(features)].values
        norm_group["stat_vector"] = [[None if pd.isna(v) else float(v) for v in row] for row in stat_arr]
        normalized_groups.append(
            cast(pd.DataFrame, norm_group[["canonical_player_id", "competition_id", "season_id", "stat_vector"]])
        )

    if not normalized_groups:
        return empty_result, {}

    result_df = cast(pd.DataFrame, pd.concat(normalized_groups, ignore_index=True))
    return result_df, all_params


# ---------------------------------------------------------------------------
# Merge vectors
# ---------------------------------------------------------------------------


def _merge_vectors(
    behavioral_keys: list[tuple[str, str]],
    stat_df: pd.DataFrame,
    match_competition_map: dict[str, tuple[str, str]],
) -> dict[tuple[str, str], list[float | None] | None]:
    """Join stat vectors to behavioral keys via player + competition + season.

    Args:
        behavioral_keys: List of (canonical_player_id, match_id) tuples.
        stat_df: DataFrame with canonical_player_id, competition_id,
            season_id, stat_vector columns.
        match_competition_map: Maps match_id to (competition_id, season_id).

    Returns:
        Dict mapping (player_id, match_id) to stat_vector or None.
    """
    # Build lookup: (player_id, comp_id, season_id) -> stat_vector
    lookup: dict[tuple[str, str, str], list[float | None]] = {}
    for pid, comp, season, vec in zip(
        stat_df["canonical_player_id"].astype(str),
        stat_df["competition_id"].astype(str),
        stat_df["season_id"].astype(str),
        stat_df["stat_vector"],
        strict=True,
    ):
        lookup[(pid, comp, season)] = cast(list[float | None], vec)

    result: dict[tuple[str, str], list[float | None] | None] = {}
    for player_id, match_id in behavioral_keys:
        comp_season = match_competition_map.get(match_id)
        if comp_season is None:
            result[(player_id, match_id)] = None
            continue

        stat_key = (player_id, comp_season[0], comp_season[1])
        result[(player_id, match_id)] = lookup.get(stat_key)

    return result


# ---------------------------------------------------------------------------
# Build bronze DataFrame
# ---------------------------------------------------------------------------


def _build_bronze_dataframe(
    behavioral_vectors: dict[tuple[str, str], list[float]],
    stat_vectors: Mapping[tuple[str, str], Sequence[float | None] | None],
    source_map: Mapping[str, str],
) -> pd.DataFrame:
    """Assemble the final bronze DataFrame from behavioral and stat vectors.

    Args:
        behavioral_vectors: (player_id, match_id) -> behavioral vector (128-dim v2 or 32-dim v1).
        stat_vectors: (player_id, match_id) -> 13-dim stat vector or None.
        source_map: match_id -> data_source.

    Returns:
        DataFrame with columns: canonical_player_id, match_id, data_source,
        behavioral_vector, stat_vector.
    """
    rows: list[dict[str, Any]] = []
    for (player_id, match_id), bvec in behavioral_vectors.items():
        rows.append(
            {
                "canonical_player_id": player_id,
                "match_id": match_id,
                "data_source": source_map.get(match_id, "unknown"),
                "behavioral_vector": bvec,
                "stat_vector": stat_vectors.get((player_id, match_id)),
            }
        )

    return pd.DataFrame(rows)
