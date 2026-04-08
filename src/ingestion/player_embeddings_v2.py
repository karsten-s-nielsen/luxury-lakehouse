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

from ingestion.guards import FilterResult
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


class _Football2VecV2Guard:
    workflow_id = "wf-football2vec-v2"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _Football2VecV2Guard()


# ---------------------------------------------------------------------------
# v2 import — pre-computed transformer embeddings from HF Hub
# ---------------------------------------------------------------------------

_HF_V2_DATASET = "luxury-lakehouse/football2vec-statsbomb-wyscout"


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
    try:
        from huggingface_hub import hf_hub_download, repo_exists
    except ImportError:
        logger.info("huggingface_hub not available — falling back to Doc2Vec v1")
        return False

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
        logger.warning("Could not load match metadata — stat vectors will be None")
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
) -> None:
    """Import pre-computed v2 transformer embeddings from HF Hub.

    Decorated with ``wf-football2vec-v2`` for independent cost/runtime tracking.
    Wraps ``_import_v2_embeddings`` and logs the outcome.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new v2 embedding work")

    logger.info("Starting v2 transformer embedding import for %s.%s", catalog, schema)

    success = _import_v2_embeddings(spark, catalog, schema, logger)
    if success:
        logger.info("v2 transformer embedding import complete")
    else:
        logger.info("v2 transformer embeddings not available — no action taken")


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
) -> None:
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
            return
    except Exception:
        logger.warning("v2 import failed — falling back to Doc2Vec v1", exc_info=True)

    logger.info("Proceeding with v1 Doc2Vec inference path")
    run_pipeline_v1(spark, catalog, schema, logger, filter_result=filter_result)


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

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-football2vec-v2")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


def main_v2() -> None:
    """CLI entry point for v2 transformer embedding import from HF Hub."""
    args = parse_ingestion_args("Import v2 transformer embeddings from HF Hub")
    logger = configure_logging("player_embeddings_v2")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-football2vec-v2")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    run_pipeline_v2(spark, args.catalog, args.schema, logger, filter_result=filter_result)
