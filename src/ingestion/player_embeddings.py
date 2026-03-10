"""Player embedding ingestion pipeline.

Loads StatsBomb + Wyscout bronze events, joins to ``dim_players`` for
canonical_player_id, runs Football2Vec inference to produce 32-dim behavioral
embeddings per (player, match), computes 13-dim z-score normalized stat
vectors from ``fct_player_stats``, and writes merged results to
``player_embeddings_raw`` bronze table.

Behavioral inference uses ``applyInPandas`` with flat partitioning by
``batch_id`` to distribute tokenisation and Doc2Vec inference across Spark
executors.  The Doc2Vec model is loaded from UC Volumes on each executor
and cached in a module-level dict so it is only loaded once per JVM lifetime.

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

from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

_TABLE_NAME = "player_embeddings_raw"
_GOLD_SCHEMA = "dev_gold"
_PLAYERS_PER_BATCH = 100

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
# Event loading (Spark-native — no .toPandas())
# ---------------------------------------------------------------------------


def _load_events_sdf(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    match_ids: set[str] | None = None,
) -> SparkDataFrame:
    """Load StatsBomb + Wyscout events joined to dim_players as a Spark DF.

    Reads events from dbt staging views (which parse JSON columns and
    normalize coordinates) and joins to ``dim_players`` for
    ``canonical_player_id``.  Competition and season metadata comes from
    ``fct_match_summary``.

    Unlike the previous ``_load_events`` which called ``.toPandas()`` on the
    driver, this returns a distributed Spark DataFrame for downstream
    ``applyInPandas`` processing on executors.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name (e.g. ``soccer_analytics``).
        schema: Bronze schema name (unused — queries staging views directly).
        match_ids: If provided, only load events for these match IDs.

    Returns:
        Spark DataFrame with columns: canonical_player_id, match_id,
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

    # Filter to only new match IDs to prevent unbounded processing
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
    """Load events and collect to pandas (backward-compatible wrapper).

    .. deprecated::
        Prefer ``_load_events_sdf`` for distributed processing.  This wrapper
        exists for test compatibility and small ad-hoc queries only.
    """
    return _load_events_sdf(spark, catalog, schema, match_ids=match_ids).toPandas()


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
        behavioral_vectors: (player_id, match_id) -> 32-dim behavioral vector.
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


# ---------------------------------------------------------------------------
# applyInPandas UDF for behavioral inference
# ---------------------------------------------------------------------------


def _make_behavioral_udf(model_path: str) -> object:
    """Build the ``applyInPandas`` UDF closure for Doc2Vec behavioral inference.

    The Doc2Vec model is loaded from a UC Volume path on each executor and
    cached in a module-level dict so it is loaded only once per JVM lifetime.
    All library imports happen inside the closure so they are available on
    Spark executors without requiring module-level serialisation.

    Args:
        model_path: UC Volume path to the Doc2Vec model file
            (e.g. ``/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/player2vec.model``).

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Tokenize events and infer Doc2Vec embeddings for a batch of players."""
        import json as _json
        import math as _math
        from typing import Any as _Any

        import pandas as _pd

        _output_cols = _pd.Index(
            [
                "canonical_player_id",
                "match_id",
                "data_source",
                "behavioral_vector",
                "competition_id",
                "season_id",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_output_cols)

        # ---- Model loading with executor-level cache ----
        if not hasattr(_udf, "_model_cache"):
            _udf._model_cache = {}  # type: ignore[attr-defined]

        cache: dict = _udf._model_cache  # type: ignore[attr-defined]
        if "model" not in cache:
            from typing import cast as _cast

            from gensim.models.doc2vec import Doc2Vec as _Doc2Vec

            cache["model"] = _cast(_Doc2Vec, _Doc2Vec.load(model_path))

        model: _Any = cache["model"]

        # ---- Tokenize events per (player, match) ----
        # Inline tokenization to avoid importing analytics.football2vec on executors
        # (keeps the same logic as tokenize_match_events + tokenize_event)
        sb_map: dict[str, str] = {
            "Pass": "pass",
            "Shot": "shot",
            "Carry": "carry",
            "Duel": "duel",
            "Interception": "interception",
            "Foul Committed": "foul",
            "Clearance": "clearance",
            "Dribble": "take_on",
            "Goalkeeper": "goalkeeper",
        }
        ws_map: dict[str, str] = {
            "Pass": "pass",
            "Shot": "shot",
            "Duel": "duel",
            "Foul": "foul",
            "Goalkeeper leaving line": "goalkeeper",
        }
        ws_others: dict[str, str] = {
            "Interception": "interception",
            "Acceleration": "take_on",
            "Touch": "throw_in",
        }
        grid_cols, grid_rows = 12, 8
        pitch_length, pitch_width = 120.0, 80.0
        cell_w = pitch_length / grid_cols
        cell_h = pitch_width / grid_rows

        sorted_pdf = pdf.sort_values("event_index")
        sequences: dict[tuple[str, str], list[str]] = {}
        match_meta: dict[str, tuple[str, str, str]] = {}  # match_id -> (data_source, comp, season)

        for rec in sorted_pdf.to_dict("records"):
            rec_dict: dict[str, _Any] = rec
            x_val = rec_dict.get("x")
            y_val = rec_dict.get("y")
            if x_val is None or y_val is None:
                continue
            if isinstance(x_val, float) and _math.isnan(x_val):
                continue
            if isinstance(y_val, float) and _math.isnan(y_val):
                continue

            gx = min(int(x_val / cell_w), grid_cols - 1)
            gy = min(int(y_val / cell_h), grid_rows - 1)

            event_type = rec_dict.get("event_type", "")
            data_source = rec_dict.get("data_source", "")

            if data_source == "statsbomb":
                if event_type == "Pass":
                    if rec_dict.get("pass_cross"):
                        action = "cross"
                    else:
                        pp = rec_dict.get("play_pattern")
                        if pp == "From Corner":
                            action = "corner"
                        elif pp == "From Throw In":
                            action = "throw_in"
                        else:
                            action = "pass"
                else:
                    action = sb_map.get(event_type, "other")
            elif data_source == "wyscout":
                sub_event = rec_dict.get("sub_event_type") or ""
                if event_type == "Pass":
                    action = "cross" if sub_event == "Cross" else "pass"
                elif event_type == "Free Kick":
                    action = "clearance" if "clearance" in sub_event.lower() else "free_kick"
                elif event_type == "Others":
                    action = ws_others.get(sub_event, "other")
                else:
                    action = ws_map.get(event_type, "other")
            else:
                action = "other"

            token = f"{action}_{gx}_{gy}"
            key = (str(rec_dict["canonical_player_id"]), str(rec_dict["match_id"]))
            if key not in sequences:
                sequences[key] = []
            sequences[key].append(token)

            mid = str(rec_dict["match_id"])
            if mid not in match_meta:
                match_meta[mid] = (
                    str(data_source),
                    str(rec_dict.get("competition_id", "")),
                    str(rec_dict.get("season_id", "")),
                )

        if not sequences:
            return _pd.DataFrame(columns=_output_cols)

        # ---- Infer Doc2Vec vectors ----
        rows: list[dict[str, _Any]] = []
        for (player_id, match_id), tokens in sequences.items():
            vec = model.infer_vector(tokens, epochs=20)
            bvec_list = [float(v) for v in vec]
            meta = match_meta.get(match_id, ("unknown", "", ""))
            rows.append(
                {
                    "canonical_player_id": player_id,
                    "match_id": match_id,
                    "data_source": meta[0],
                    "behavioral_vector": _json.dumps(bvec_list),
                    "competition_id": meta[1],
                    "season_id": meta[2],
                }
            )

        return _pd.DataFrame(_pd.DataFrame(rows)[_output_cols])

    return _udf


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

    # 1. Load events as distributed Spark DataFrame (no .toPandas())
    events_sdf = _load_events_sdf(spark, catalog, schema, match_ids=new_matches)

    # Quick emptiness check via limit(1) — avoids full DAG recomputation
    if events_sdf.limit(1).count() == 0:
        logger.warning("No events found — exiting")
        return

    logger.info("Events DataFrame loaded (distributed — no driver collection)")

    # 2. Assign flat batch_id for balanced distribution across executors.
    #    We partition by canonical_player_id so all events for a player end up
    #    in the same batch (required for per-player-match tokenization).
    from pyspark.sql import functions as spark_fn
    from pyspark.sql.types import StringType, StructField, StructType

    # Get distinct player IDs and assign batch_id
    player_sdf = events_sdf.select("canonical_player_id").distinct()
    player_count = player_sdf.count()
    num_batches = max(1, player_count // _PLAYERS_PER_BATCH)

    player_batched = player_sdf.withColumn(
        "batch_id",
        (spark_fn.monotonically_increasing_id() % num_batches).cast("int"),
    )

    # Join batch_id back to events
    events_batched = events_sdf.join(player_batched, on="canonical_player_id", how="inner")

    logger.info(
        "Assigned %d players to %d batches (~%d players/batch)",
        player_count,
        num_batches,
        _PLAYERS_PER_BATCH,
    )

    # 3. Build UDF and run applyInPandas for behavioral vector inference
    model_path = f"/Volumes/{catalog}/dev_gold/model_weights/football2vec/player2vec.model"

    behavioral_schema = StructType(
        [
            StructField("canonical_player_id", StringType()),
            StructField("match_id", StringType()),
            StructField("data_source", StringType()),
            StructField("behavioral_vector", StringType()),  # JSON-encoded list[float]
            StructField("competition_id", StringType()),
            StructField("season_id", StringType()),
        ]
    )

    udf_fn = _make_behavioral_udf(model_path)
    behavioral_sdf = events_batched.groupBy("batch_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=behavioral_schema,
    )

    # 4. Collect behavioral results to driver for stat vector merging.
    #    Result size is O(players * matches_per_player) — typically ~90K rows,
    #    each with a short JSON string. Well within driver memory.
    behavioral_pdf = behavioral_sdf.toPandas()

    if behavioral_pdf.empty:
        logger.warning("No behavioral vectors produced — exiting")
        return

    # Deserialize behavioral vectors from JSON strings
    behavioral_pdf["behavioral_vector"] = behavioral_pdf["behavioral_vector"].apply(json.loads)

    # Build behavioral_vectors dict and metadata maps
    behavioral_vectors: dict[tuple[str, str], list[float]] = {}
    match_competition_map: dict[str, tuple[str, str]] = {}
    source_map: dict[str, str] = {}

    for _, row in behavioral_pdf.iterrows():
        pid = str(row["canonical_player_id"])
        mid = str(row["match_id"])
        behavioral_vectors[(pid, mid)] = cast(list[float], row["behavioral_vector"])
        if mid not in match_competition_map:
            match_competition_map[mid] = (str(row["competition_id"]), str(row["season_id"]))
        if mid not in source_map:
            source_map[mid] = str(row["data_source"])

    logger.info("Inferred %d behavioral vectors via applyInPandas", len(behavioral_vectors))

    # 5. Compute stat vectors (filtered to players present in events)
    #    fct_player_stats is small (~20K rows) — driver-side is fine.
    event_player_ids: set[int] = set()
    for pid_str in behavioral_pdf["canonical_player_id"].unique():
        try:
            event_player_ids.add(int(pid_str))
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
