"""Player embedding ingestion pipeline.

Loads StatsBomb + Wyscout bronze events, joins to ``dim_players`` for
canonical_player_id, runs Football2Vec inference to produce 32-dim behavioral
embeddings per (player, match), computes 13-dim z-score normalized stat
vectors from ``fct_player_stats``, and writes merged results to
``player_embeddings_raw`` bronze table.

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
from gensim.models.doc2vec import Doc2Vec

from analytics.football2vec import TokenizerConfig, infer_vectors, tokenize_match_events
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "player_embeddings_raw"
_GOLD_SCHEMA = "dev_gold"

STAT_FEATURES: list[str] = [
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
]


# ---------------------------------------------------------------------------
# Z-score normalization
# ---------------------------------------------------------------------------


def _zscore_normalize(
    df: pd.DataFrame,
    features: list[str],
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


# ---------------------------------------------------------------------------
# Event loading
# ---------------------------------------------------------------------------


def _load_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    match_ids: set[str] | None = None,
) -> pd.DataFrame:
    """Load StatsBomb + Wyscout events joined to dim_players.

    Reads events from dbt staging views (which parse JSON columns and
    normalize coordinates) and joins to ``dim_players`` for
    ``canonical_player_id``.  Competition and season metadata comes from
    ``fct_match_summary``.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name (e.g. ``soccer_analytics``).
        schema: Bronze schema name (unused — queries staging views directly).
        match_ids: If provided, only load events for these match IDs.
            Prevents unbounded ``.toPandas()`` on large event tables.

    Returns:
        pandas DataFrame with columns: canonical_player_id, match_id,
        event_type, x, y, event_index, data_source, play_pattern,
        pass_cross, sub_event_type, competition_id, season_id.
    """
    _ = schema  # bronze schema unused; staging views are in dev_silver
    silver = "dev_silver"
    gold = _GOLD_SCHEMA

    # Join dim_players within each source CTE using source-specific ID columns
    # (statsbomb_player_id / wyscout_player_id) to avoid cross-source ID collisions.
    query = f"""
        WITH sb_events AS (
            SELECT
                dp.canonical_player_id,
                CAST(e.match_id AS STRING) AS match_id,
                e.event_type,
                CAST(e.location_x AS DOUBLE) AS x,
                CAST(e.location_y AS DOUBLE) AS y,
                CAST(e.`index` AS INT) AS event_index,
                'statsbomb' AS data_source,
                e.play_pattern,
                CASE WHEN e.pass_cross IS NOT NULL THEN TRUE ELSE FALSE END AS pass_cross,
                CAST(NULL AS STRING) AS sub_event_type
            FROM {catalog}.{silver}.stg_statsbomb__events e
            INNER JOIN {catalog}.{gold}.dim_players dp
                ON e.player_id = dp.statsbomb_player_id
            WHERE e.player_id IS NOT NULL
        ),
        ws_events AS (
            SELECT
                dp.canonical_player_id,
                CAST(e.match_id AS STRING) AS match_id,
                e.event_type,
                CAST(e.start_x AS DOUBLE) AS x,
                CAST(e.start_y AS DOUBLE) AS y,
                CAST(e.event_sec AS INT) AS event_index,
                'wyscout' AS data_source,
                CAST(NULL AS STRING) AS play_pattern,
                FALSE AS pass_cross,
                e.sub_event_type
            FROM {catalog}.{silver}.stg_wyscout__events e
            INNER JOIN {catalog}.{gold}.dim_players dp
                ON e.player_id = dp.wyscout_player_id
            WHERE e.player_id IS NOT NULL
        ),
        all_events AS (
            SELECT * FROM sb_events
            UNION ALL
            SELECT * FROM ws_events
        )
        SELECT
            ae.canonical_player_id,
            ae.match_id,
            ae.event_type,
            ae.x,
            ae.y,
            ae.event_index,
            ae.data_source,
            ae.play_pattern,
            ae.pass_cross,
            ae.sub_event_type,
            CAST(m.competition_id AS STRING) AS competition_id,
            CAST(m.season_id AS STRING) AS season_id
        FROM all_events ae
        INNER JOIN {catalog}.{gold}.fct_match_summary m
            ON CAST(ae.match_id AS STRING) = CAST(m.match_id AS STRING)
    """  # noqa: S608
    events_sdf = spark.sql(query)

    # Filter to only new match IDs to prevent unbounded .toPandas()
    if match_ids:
        from pyspark.sql import functions as spark_fn

        events_sdf = events_sdf.filter(spark_fn.col("match_id").isin(list(match_ids)))

    return events_sdf.toPandas()


# ---------------------------------------------------------------------------
# Stat vector computation
# ---------------------------------------------------------------------------


def _compute_stat_vectors(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    player_ids: set[int] | None = None,
) -> tuple[pd.DataFrame, dict[str, dict[str, float]]]:
    """Load fct_player_stats, z-score normalize, and return stat vectors.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        gold_schema: Gold schema name (e.g. ``dev_gold``).
        player_ids: If provided, only load stats for these canonical player IDs.
            Prevents unbounded ``.toPandas()`` on full fct_player_stats table.

    Returns:
        Tuple of (DataFrame with canonical_player_id, competition_id,
        season_id, stat_vector columns; normalization params dict).
    """
    feature_cols = ", ".join(f"ps.{f}" for f in STAT_FEATURES)
    player_filter = ""
    if player_ids:
        ids_csv = ", ".join(str(pid) for pid in player_ids)
        player_filter = f"AND dp.canonical_player_id IN ({ids_csv})"
    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(ps.competition_id AS STRING) AS competition_id,
            CAST(ps.season_id AS STRING) AS season_id,
            {feature_cols}
        FROM {catalog}.{gold_schema}.fct_player_stats ps
        INNER JOIN {catalog}.{gold_schema}.dim_players dp
            ON ps.player_id = dp.player_id
        WHERE dp.canonical_player_id IS NOT NULL
        {player_filter}
    """  # noqa: S608
    df = spark.sql(query).toPandas()

    if df.empty:
        return (
            pd.DataFrame(
                {
                    "canonical_player_id": pd.Series(dtype="str"),
                    "competition_id": pd.Series(dtype="str"),
                    "season_id": pd.Series(dtype="str"),
                    "stat_vector": pd.Series(dtype="object"),
                }
            ),
            {},
        )

    normalized, params = _zscore_normalize(df, STAT_FEATURES)

    # Build stat_vector column as list[float | None] (vectorized via NumPy array access)
    stat_arr = normalized[STAT_FEATURES].values
    normalized["stat_vector"] = [[None if pd.isna(v) else float(v) for v in row] for row in stat_arr]

    result_df = cast(pd.DataFrame, normalized[["canonical_player_id", "competition_id", "season_id", "stat_vector"]])
    return result_df, params


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
    # Build lookup: (player_id, comp_id, season_id) → stat_vector
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
        behavioral_vectors: (player_id, match_id) → 32-dim behavioral vector.
        stat_vectors: (player_id, match_id) → 13-dim stat vector or None.
        source_map: match_id → data_source.

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


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------


def _load_model(catalog: str) -> Doc2Vec:
    """Load trained Doc2Vec model from UC Volumes.

    Args:
        catalog: Unity Catalog name for volume path resolution.

    Returns:
        Trained Doc2Vec model.
    """
    import os

    model_dir = f"/Volumes/{catalog}/dev_gold/model_weights/football2vec"
    model_path = os.path.join(model_dir, "player2vec.model")
    return cast(Doc2Vec, Doc2Vec.load(model_path))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for player embedding computation."""
    args = parse_ingestion_args("Compute player embeddings from event data")
    catalog, schema = args.catalog, args.schema
    logger = configure_logging("player_embeddings")
    spark = get_spark_session()

    logger.info("Starting player embedding pipeline for %s.%s", catalog, schema)

    # 0. Incremental check — skip if all source matches already have embeddings
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
    try:
        existing_matches = {
            str(row["match_id"]) for row in spark.table(results_table).select("match_id").distinct().collect()
        }
    except Exception:
        existing_matches = set()  # table doesn't exist yet

    # Count source matches from fct_match_summary (all sources — embeddings
    # cover every match that has events joined to dim_players)
    gold = _GOLD_SCHEMA
    try:
        source_match_query = (
            f"SELECT DISTINCT CAST(match_id AS STRING) AS match_id "  # noqa: S608
            f"FROM {catalog}.{gold}.fct_match_summary"
        )
        source_matches = {str(row["match_id"]) for row in spark.sql(source_match_query).collect()}
    except Exception:
        source_matches = set()

    # Defensive fallback: if source query returned nothing but embeddings
    # already exist, the query may have a transient issue — skip rather
    # than recomputing everything and risking OOM.
    if not source_matches and existing_matches:
        logger.warning(
            "Source match query returned 0 rows but %d existing embeddings found — "
            "skipping to avoid unnecessary full recompute",
            len(existing_matches),
        )
        return

    new_matches = source_matches - existing_matches
    if source_matches and not new_matches:
        logger.info(
            "All %d matches already have embeddings — skipping full recompute",
            len(existing_matches),
        )
        return

    logger.info(
        "%d source matches, %d existing, %d new — running full pipeline",
        len(source_matches),
        len(existing_matches),
        len(new_matches),
    )

    # 1. Load events (filtered to new matches only to avoid unbounded toPandas)
    events_df = _load_events(spark, catalog, schema, match_ids=new_matches)
    if events_df.empty:
        logger.warning("No events found — exiting")
        return

    logger.info("Loaded %d events", len(events_df))

    # 2. Build match → (competition, season) map and match → source map
    match_meta_cols = cast(pd.DataFrame, events_df[["match_id", "competition_id", "season_id", "data_source"]])
    match_meta = match_meta_cols.drop_duplicates(subset="match_id")
    match_competition_map: dict[str, tuple[str, str]] = {}
    source_map: dict[str, str] = {}
    for mid, comp, season, source in zip(
        match_meta["match_id"].astype(str),
        match_meta["competition_id"].astype(str),
        match_meta["season_id"].astype(str),
        match_meta["data_source"].astype(str),
        strict=True,
    ):
        match_competition_map[mid] = (comp, season)
        source_map[mid] = source

    # 3. Load trained model
    model = _load_model(catalog)
    logger.info("Loaded Doc2Vec model")

    # 4. Tokenize and infer behavioral vectors
    config = TokenizerConfig()
    sequences = tokenize_match_events(events_df, config)
    if not sequences:
        logger.warning("No valid token sequences produced — exiting")
        return

    behavioral_vectors = infer_vectors(model, sequences)
    logger.info("Inferred %d behavioral vectors", len(behavioral_vectors))

    # 5. Compute stat vectors (filtered to players present in events)
    raw_ids = events_df["canonical_player_id"].dropna().unique()
    event_player_ids: set[int] = set()
    for pid in raw_ids:
        try:
            event_player_ids.add(int(pid))
        except (ValueError, TypeError):
            pass  # non-numeric IDs are skipped — stat filter remains broad
    stat_df, norm_params = _compute_stat_vectors(spark, catalog, _GOLD_SCHEMA, player_ids=event_player_ids or None)
    logger.info("Computed stat vectors for %d player-comp-season entries", len(stat_df))

    # Save normalization params alongside model artifacts
    if norm_params:
        _save_norm_params(catalog, norm_params, logger)

    # 6. Merge stat vectors to behavioral keys
    behavioral_keys = list(behavioral_vectors.keys())
    merged_stats = _merge_vectors(behavioral_keys, stat_df, match_competition_map)

    # 7. Build bronze DataFrame
    bronze_df = _build_bronze_dataframe(behavioral_vectors, merged_stats, source_map)
    logger.info("Built bronze DataFrame: %d rows", len(bronze_df))

    # 8. Write per data source with replaceWhere for idempotency
    for source in bronze_df["data_source"].unique():
        source_str = str(source)
        source_slice = bronze_df[bronze_df["data_source"] == source_str]

        sdf = spark.createDataFrame(source_slice)
        row_count = validate_dataframe(
            sdf,
            ["canonical_player_id", "match_id", "data_source", "behavioral_vector"],
            _TABLE_NAME,
            logger,
        )
        write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"data_source = '{source_str}'",
            logger=logger,
            row_count=row_count,
        )

    logger.info("Player embedding pipeline complete")


def _save_norm_params(
    catalog: str,
    params: dict[str, dict[str, float]],
    logger: logging.Logger,
) -> None:
    """Save normalization parameters to UC Volumes as JSON.

    Args:
        catalog: Unity Catalog name.
        params: Feature normalization parameters.
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


if __name__ == "__main__":
    main()
