"""v2 transformer player embedding pipeline + combined orchestrator.

Imports pre-computed 128-dim transformer embeddings from HF Hub dataset
``luxury-lakehouse/football2vec-statsbomb-wyscout``.  Also provides the
combined ``run_pipeline()`` that tries v2 first then falls back to v1.

Bronze table produced:
  - player_embeddings_raw
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
from huggingface_hub import hf_hub_download, repo_exists

from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
from ingestion.player_embeddings_common import (
    _TABLE_NAME,
    _compute_stat_vectors,
    _merge_vectors,
    _save_norm_params,
)
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# v2 import — pre-computed transformer embeddings from HF Hub
# ---------------------------------------------------------------------------

_HF_V2_DATASET = "luxury-lakehouse/football2vec-statsbomb-wyscout"
_HF_360_DATASET = "luxury-lakehouse/football2vec-360-embeddings"  # pragma: allowlist secret


class _Football2VecV2Guard:
    workflow_id = "wf-football2vec-v2"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, _HF_V2_DATASET)


skip_guard = _Football2VecV2Guard()


class _Football2Vec360Guard:
    workflow_id = "wf-football2vec-360"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, _HF_360_DATASET)


_football2vec_360_guard = _Football2Vec360Guard()

_V2_BEHAVIORAL_DIM = 128
_V360_BEHAVIORAL_DIM = 144
_FOOTBALL2VEC_360_DATA_SOURCE = "football2vec_360"


def _import_v2_embeddings(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> bool:
    """Import pre-computed 128-dim transformer embeddings from HF Hub.

    Downloads the Parquet file from the ``luxury-lakehouse/football2vec-statsbomb-wyscout``
    dataset on HF Hub, converts to a Spark DataFrame, merges with stat vectors,
    and writes to ``player_embeddings_raw`` in Delta with ``replaceWhere`` per
    data_source for idempotency.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name.
        logger: Logger instance.

    Returns:
        True if v2 embeddings were successfully imported, False if the
        dataset is not available (falls back to Doc2Vec v1 path).
    """
    # hf_hub_download and repo_exists are imported at module level so the
    # patch seam is consistent with _import_embeddings_360().

    if not repo_exists(_HF_V2_DATASET, repo_type="dataset"):
        logger.info("HF dataset %s not found — falling back to Doc2Vec v1", _HF_V2_DATASET)
        return False

    logger.info("Importing v2 transformer embeddings from %s", _HF_V2_DATASET)

    # Download the Parquet file to local cache
    parquet_path = hf_hub_download(
        repo_id=_HF_V2_DATASET,
        filename="data/embeddings_v2.parquet",
        repo_type="dataset",
    )

    # Read into pandas
    v2_pdf = pd.read_parquet(parquet_path)
    logger.info("Downloaded %d v2 embeddings from HF Hub", len(v2_pdf))

    if v2_pdf.empty:
        logger.warning("v2 embeddings Parquet is empty — falling back to Doc2Vec v1")
        return False

    # Validate expected columns
    required_cols = {"canonical_player_id", "match_id", "behavioral_vector"}
    if not required_cols.issubset(v2_pdf.columns):
        missing = required_cols - set(v2_pdf.columns)
        logger.warning("v2 Parquet missing columns %s — falling back to Doc2Vec v1", missing)
        return False

    # Ensure string types for key columns
    for col in ("canonical_player_id", "match_id"):
        v2_pdf[col] = v2_pdf[col].astype(str)

    # Derive data_source + competition/season metadata from a single query
    # (OPT-AUDIT: combined two SELECT DISTINCT queries into one to halve shuffle)
    gold = DEFAULT_GOLD_SCHEMA
    needs_data_source = "data_source" not in v2_pdf.columns
    try:
        meta_query = (
            f"SELECT DISTINCT "  # noqa: S608
            f"  CAST(match_id AS STRING) AS match_id, "
            f"  CAST(data_source AS STRING) AS data_source, "
            f"  CAST(competition_id AS STRING) AS competition_id, "
            f"  CAST(season_id AS STRING) AS season_id "
            f"FROM {catalog}.{gold}.fct_action_values"
        )
        meta_pdf = spark.sql(meta_query).toPandas()
        match_competition_map: dict[str, tuple[str, str]] = dict(
            zip(
                meta_pdf["match_id"].astype(str),
                zip(meta_pdf["competition_id"].astype(str), meta_pdf["season_id"].astype(str), strict=True),
                strict=True,
            )
        )
        if needs_data_source:
            ds_map = dict(zip(meta_pdf["match_id"].astype(str), meta_pdf["data_source"].astype(str), strict=True))
            v2_pdf["data_source"] = v2_pdf["match_id"].map(lambda mid: ds_map.get(mid, "unknown"))
            logger.info("Derived data_source for %d matches from fct_action_values", len(ds_map))
    except Exception:
        logger.error(
            "Could not load match metadata from fct_action_values — "
            "stat vectors will be None and data_source will be 'unknown'",
            exc_info=True,
        )
        match_competition_map = {}
        if needs_data_source:
            v2_pdf["data_source"] = "unknown"
    v2_pdf["data_source"] = v2_pdf["data_source"].astype(str)

    # Behavioral vectors may be stored as numpy arrays or lists — normalize to list[float]
    v2_pdf["behavioral_vector"] = v2_pdf["behavioral_vector"].apply(
        lambda v: [float(x) for x in v] if not isinstance(v, list) else v
    )

    # Build metadata maps from v2 embeddings for stat vector join
    data_sources = v2_pdf["data_source"].unique().tolist()
    logger.info("v2 embeddings cover data sources: %s", data_sources)

    # Compute stat vectors for matched players
    event_player_ids: set[int] = set()
    for pid_str in v2_pdf["canonical_player_id"].unique():
        try:
            event_player_ids.add(int(pid_str))
        except (ValueError, TypeError):
            pass

    stat_df, norm_params = _compute_stat_vectors(
        spark, catalog, DEFAULT_GOLD_SCHEMA, player_ids=event_player_ids or None
    )
    logger.info("Computed stat vectors for %d player-comp-season entries", len(stat_df))

    if norm_params:
        _save_norm_params(catalog, norm_params, logger)

    # Merge stat vectors
    behavioral_keys = list(zip(v2_pdf["canonical_player_id"].astype(str), v2_pdf["match_id"].astype(str), strict=True))
    merged_stats = _merge_vectors(behavioral_keys, stat_df, match_competition_map)

    # Attach stat vectors via vectorized key lookup
    # (OPT-AUDIT: replaced .iterrows() on ~87K rows with tuple-keyed map)
    v2_pdf["stat_vector"] = [
        merged_stats.get(k)
        for k in zip(v2_pdf["canonical_player_id"].astype(str), v2_pdf["match_id"].astype(str), strict=True)
    ]

    # Write per data source with replaceWhere for idempotency
    for source in data_sources:
        source_str = str(source)
        source_slice = v2_pdf[v2_pdf["data_source"] == source_str][
            ["canonical_player_id", "match_id", "data_source", "behavioral_vector", "stat_vector"]
        ]

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

    logger.info("Successfully imported %d v2 embeddings from HF Hub", len(v2_pdf))
    return True


# ---------------------------------------------------------------------------
# v2 pipeline entry
# ---------------------------------------------------------------------------


@workflow("wf-football2vec-v2", phase="training")
def run_pipeline_v2(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Import pre-computed v2 transformer embeddings from HF Hub.

    Decorated with ``wf-football2vec-v2`` for independent cost/runtime tracking.
    Wraps ``_import_v2_embeddings`` and logs the outcome.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new v2 embedding work")

    logger.info("Starting v2 transformer embedding import for %s.%s", catalog, schema)

    success = _import_v2_embeddings(spark, catalog, schema, logger)
    if success:
        record_import_sha(
            spark, catalog, "wf-football2vec-v2", _HF_V2_DATASET, filter_result.metadata.get("commit_sha")
        )
        logger.info("v2 transformer embedding import complete")
    else:
        logger.info("v2 transformer embeddings not available — no action taken")
    return 0


# ---------------------------------------------------------------------------
# 360 import — pre-computed 144-dim 360-enriched transformer embeddings from HF Hub
# ---------------------------------------------------------------------------


def _import_embeddings_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> int:
    """Import pre-computed 144-dim 360-enriched embeddings from HF Hub.

    Downloads the Parquet file from ``luxury-lakehouse/football2vec-360-embeddings``,
    labels every row with ``data_source='football2vec_360'`` (overriding any
    provider info in the parquet), validates the 144-dim vector contract, and
    writes to ``player_embeddings_raw`` with ``replace_where=data_source='football2vec_360'``
    so the 360 partition is isolated from v2's provider-keyed partitions.

    The 360 model has its own embedding space (144-dim = 128-dim transformer +
    16-dim Deep Sets context) and is NOT directly comparable to the v2 128-dim
    embeddings. Downstream dbt models (``fct_player_embeddings_season_360``,
    ``fct_player_embeddings_career_360``) filter on ``data_source='football2vec_360'``
    to aggregate it separately from the v1/v2 paths.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name.
        logger: Logger instance.

    Returns:
        Row count of imported embeddings (>0 on success), or 0 if the
        HF Hub dataset does not exist or is empty (early exit, non-fatal).

    Raises:
        RuntimeError: If the downloaded parquet has vectors of the wrong
            dimension (expected 144 per row). Silent dimension drift is
            forbidden — ADR-002 applies.
    """
    if not repo_exists(_HF_360_DATASET, repo_type="dataset"):
        logger.info("HF dataset %s not found — skipping 360 import", _HF_360_DATASET)
        return 0

    logger.info("Importing 360-enriched transformer embeddings from %s", _HF_360_DATASET)

    parquet_path = hf_hub_download(
        repo_id=_HF_360_DATASET,
        filename="data/embeddings_360.parquet",
        repo_type="dataset",
    )

    pdf = pd.read_parquet(parquet_path)
    logger.info("Downloaded %d 360 embeddings from HF Hub", len(pdf))

    if pdf.empty:
        logger.warning("360 embeddings Parquet is empty — nothing to import")
        return 0

    required_cols = {"canonical_player_id", "match_id", "behavioral_vector"}
    if not required_cols.issubset(pdf.columns):
        missing = required_cols - set(pdf.columns)
        msg = f"360 Parquet missing required columns {missing}"
        raise RuntimeError(msg)

    # Validate the 144-dim contract. Dimension drift would silently corrupt
    # downstream cosine similarity — fail loudly. Check the first 10 rows
    # (cheap, catches drift without full-column iteration).
    for i, vec in enumerate(pdf["behavioral_vector"].iloc[:10]):
        vec_list = list(vec) if not isinstance(vec, list) else vec
        if len(vec_list) != _V360_BEHAVIORAL_DIM:
            msg = (
                f"360 Parquet vector at row {i} has length {len(vec_list)}, "
                f"expected {_V360_BEHAVIORAL_DIM} (144-dim 360-enriched). "
                f"This means the HF dataset schema drifted — do NOT import "
                f"until the training run is re-verified."
            )
            raise RuntimeError(msg)

    # Ensure string types for key columns.
    for col in ("canonical_player_id", "match_id"):
        pdf[col] = pdf[col].astype(str)

    # Normalize behavioral_vector entries to list[float].
    pdf["behavioral_vector"] = pdf["behavioral_vector"].apply(
        lambda v: [float(x) for x in v] if not isinstance(v, list) else v
    )

    # ALL 360 rows get data_source='football2vec_360' — this is the
    # dbt discriminator, NOT the provider. Overrides any provider
    # column in the source parquet.
    pdf["data_source"] = _FOOTBALL2VEC_360_DATA_SOURCE

    # Stat vectors — 360 embeddings are aggregated with the same stat
    # features as v2. Reuse the existing helper.
    event_player_ids: set[int] = set()
    for pid_str in pdf["canonical_player_id"].unique():
        try:
            event_player_ids.add(int(pid_str))
        except (ValueError, TypeError):
            pass

    stat_df, norm_params = _compute_stat_vectors(
        spark, catalog, DEFAULT_GOLD_SCHEMA, player_ids=event_player_ids or None
    )
    logger.info("Computed stat vectors for %d player-comp-season entries", len(stat_df))

    if norm_params:
        _save_norm_params(catalog, norm_params, logger)

    # 360 uses the same match_competition_map as v2 — derive from fct_action_values.
    gold = DEFAULT_GOLD_SCHEMA
    try:
        meta_query = (
            f"SELECT DISTINCT "  # noqa: S608
            f"  CAST(match_id AS STRING) AS match_id, "
            f"  CAST(competition_id AS STRING) AS competition_id, "
            f"  CAST(season_id AS STRING) AS season_id "
            f"FROM {catalog}.{gold}.fct_action_values"
        )
        meta_pdf = spark.sql(meta_query).toPandas()
        match_competition_map: dict[str, tuple[str, str]] = dict(
            zip(
                meta_pdf["match_id"].astype(str),
                zip(meta_pdf["competition_id"].astype(str), meta_pdf["season_id"].astype(str), strict=True),
                strict=True,
            )
        )
    except Exception:  # best-effort enrichment: 360 import continues without match metadata
        logger.error(
            "Could not load match metadata from fct_action_values — 360 stat vectors will be None for all rows",
            exc_info=True,
        )
        match_competition_map = {}

    behavioral_keys = list(zip(pdf["canonical_player_id"].astype(str), pdf["match_id"].astype(str), strict=True))
    merged_stats = _merge_vectors(behavioral_keys, stat_df, match_competition_map)

    pdf["stat_vector"] = [
        merged_stats.get(k)
        for k in zip(pdf["canonical_player_id"].astype(str), pdf["match_id"].astype(str), strict=True)
    ]

    # Write to bronze with a SINGLE replace_where on the 360 partition.
    sdf = spark.createDataFrame(
        pdf[["canonical_player_id", "match_id", "data_source", "behavioral_vector", "stat_vector"]]
    )
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
        replace_where=f"data_source = '{_FOOTBALL2VEC_360_DATA_SOURCE}'",
        logger=logger,
        row_count=row_count,
    )

    count = len(pdf)
    logger.info("Successfully imported %d 360 embeddings from HF Hub", count)
    return count


@workflow("wf-football2vec-360", phase="inference")
def run_pipeline_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Import pre-computed 360-enriched embeddings from HF Hub.

    Decorated with ``wf-football2vec-360`` for independent cost/runtime tracking.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new 360 embedding work")

    logger.info("Starting 360 transformer embedding import for %s.%s", catalog, schema)

    row_count = _import_embeddings_360(spark, catalog, schema, logger)
    if row_count:
        record_import_sha(
            spark, catalog, "wf-football2vec-360", _HF_360_DATASET, filter_result.metadata.get("commit_sha")
        )
        logger.info("360 transformer embedding import complete (%d rows)", row_count)
    else:
        logger.info("360 transformer embeddings not available — no action taken")
    return row_count


# ---------------------------------------------------------------------------
# Combined orchestrator — tries v2 then falls back to v1
# ---------------------------------------------------------------------------


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Execute the player embedding computation pipeline (convenience wrapper).

    Tries v2 (transformer) embeddings from HF Hub first.  If the v2 dataset
    is not available, falls back to v1 Doc2Vec inference via applyInPandas.

    This function is NOT decorated with ``@workflow`` — it calls
    ``_import_v2_embeddings`` and ``run_pipeline_v1`` directly.  Use
    ``run_pipeline_v2`` or ``run_pipeline_v1`` for independent Databricks
    task execution with separate cost/runtime tracking.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new player embedding work")

    from ingestion.player_embeddings_v1 import run_pipeline_v1

    logger.info("Starting player embedding pipeline for %s.%s", catalog, schema)

    # 0a. Try v2 import path (pre-computed transformer embeddings from HF Hub)
    try:
        if _import_v2_embeddings(spark, catalog, schema, logger):
            logger.info("Player embedding pipeline complete (v2 path)")
            return 0
    except Exception:
        logger.error(
            "v2 import failed — falling back to Doc2Vec v1. "
            "The v1 fallback masks the v2 failure — investigate the ERROR trace "
            "above to fix the root cause (e.g. HF Hub auth, dataset schema drift).",
            exc_info=True,
        )

    logger.info("Proceeding with v1 Doc2Vec inference path")
    run_pipeline_v1(spark, catalog, schema, logger, filter_result=filter_result)
    return 0


# ---------------------------------------------------------------------------
# CLI entry points
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for player embedding computation."""
    args = parse_ingestion_args("Compute player embeddings from event data")
    logger = configure_logging("player_embeddings")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


def main_v2() -> None:
    """CLI entry point for v2 transformer embedding import from HF Hub."""
    args = parse_ingestion_args("Import v2 transformer embeddings from HF Hub")
    logger = configure_logging("player_embeddings_v2")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline_v2(spark, args.catalog, args.schema, logger, filter_result=filter_result)


def main_360() -> None:
    """CLI entry point for 360 transformer embedding import from HF Hub."""
    args = parse_ingestion_args("Import 360 transformer embeddings from HF Hub")
    logger = configure_logging("player_embeddings_360")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(_football2vec_360_guard, spark, args.catalog, args.schema)

    run_pipeline_360(spark, args.catalog, args.schema, logger, filter_result=filter_result)
