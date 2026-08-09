"""Export SPADL action sequences for Football2Vec v2 transformer training.

Reads per-player-match action sequences from ``fct_action_values``, joins
``dim_players`` for ``canonical_player_id`` and ``position_group``, serializes
ordered action tuples as a struct array in Parquet, writes to UC Volume,
and publishes to HF Hub as ``luxury-lakehouse/football2vec-training-data``.

Output schema (one row per player-match):
    canonical_player_id  (string)
    match_id             (string)
    competition_id       (int)
    season_id            (int)
    position_group       (string, nullable)
    actions              (array of struct: action_type int, x float, y float, result int)

Action types use the standard SPADL 23-type vocabulary (0-22), coordinates
are normalized to [0, 1], and result is binary (1 = success, 0 = otherwise).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
_guard_logger = logging.getLogger(f"{__name__}.guard")


class _Football2VecV2ExportGuard:
    """SkipGuard adapter for Football2Vec v2 training data export."""

    workflow_id = "wf-football2vec-v2-export"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if upstream data has grown since last export."""
        from ingestion.utils import tolerate_missing_table

        gold = DEFAULT_GOLD_SCHEMA
        output_path = _UC_VOLUME_PATH.format(catalog=catalog)

        upstream_count: int | None = None
        with tolerate_missing_table(_guard_logger, f"Upstream fct_action_values missing in {catalog}.{gold}"):
            upstream_count = (
                spark.table(f"{catalog}.{gold}.fct_action_values").select("player_id", "match_id").distinct().count()
            )

        if upstream_count is None:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        existing_count = 0
        with tolerate_missing_table(_guard_logger, f"No existing export at {output_path}"):
            existing_count = spark.read.parquet(output_path).count()

        if existing_count >= upstream_count and existing_count > 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=upstream_count - existing_count,
        )


skip_guard = _Football2VecV2ExportGuard()
_HF_DATASET_REPO = "luxury-lakehouse/football2vec-training-data"
_UC_VOLUME_PATH = "/Volumes/{catalog}/dev_gold/training_data/football2vec_v2"

# SPADL 23-type action vocabulary mapping (string → int).
# Canonical ordering matches silly_kicks SPADL and defcon_lite._ACTION_TYPE_IDS.
_ACTION_TYPE_IDS: dict[str, int] = {
    "pass": 0,
    "cross": 1,
    "throw_in": 2,
    "freekick_crossed": 3,
    "freekick_short": 4,
    "corner_crossed": 5,
    "corner_short": 6,
    "take_on": 7,
    "foul": 8,
    "tackle": 9,
    "interception": 10,
    "shot": 11,
    "shot_penalty": 12,
    "shot_freekick": 13,
    "keeper_save": 14,
    "keeper_claim": 15,
    "keeper_punch": 16,
    "keeper_pick_up": 17,
    "clearance": 18,
    "bad_touch": 19,
    "non_action": 20,
    "dribble": 21,
    "goalkick": 22,
}

# SPADL pitch dimensions (meters) for coordinate normalization.
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0


# ---------------------------------------------------------------------------
# Export logic
# ---------------------------------------------------------------------------


def _export_training_sequences(
    spark: SparkSession,
    catalog: str,
    schema: str,
    export_logger: logging.Logger,
) -> int:
    """Read SPADL actions, group by player-match, and write training Parquet.

    Steps:
        1. Query ``fct_action_values`` joined to ``dim_players`` for
           canonical_player_id, position_group, and action metadata.
        2. Collect struct arrays of (action_type, x, y, result) per
           player-match using Spark-native aggregation.
        3. Write Parquet to UC Volume.
        4. Upload to HF Hub.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name (unused — reads from gold).
        export_logger: Logger instance.

    Returns:
        Number of player-match rows exported.
    """
    _ = schema  # reads from DEFAULT_GOLD_SCHEMA, not the pipeline schema
    gold = DEFAULT_GOLD_SCHEMA
    output_path = _UC_VOLUME_PATH.format(catalog=catalog)

    # ------------------------------------------------------------------
    # 1. Load actions joined to dim_players (Spark-native, no .toPandas())
    # ------------------------------------------------------------------
    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(av.match_id AS STRING)            AS match_id,
            CAST(av.competition_id AS INT)         AS competition_id,
            CAST(av.season_id AS INT)              AS season_id,
            dp.position_group,
            av.action_type,
            av.start_x,
            av.start_y,
            av.action_result,
            av.period,
            av.time_seconds
        FROM {catalog}.{gold}.fct_action_values av
        INNER JOIN {catalog}.{gold}.dim_players dp
            ON av.player_id = dp.player_id
        WHERE av.player_id IS NOT NULL
          AND dp.canonical_player_id IS NOT NULL
          AND av.action_type IS NOT NULL
          AND av.start_x IS NOT NULL
          AND av.start_y IS NOT NULL
          -- ADR-064 / SEC6: fct_action_values carries RESTRICTED providers (skillcorner,
          -- gradientsports). This export lands in a PUBLIC HF dataset via
          -- upload_volume_to_hf_hub, which bypasses the pandas seam entirely, so the
          -- tier decision has to happen HERE. ADR-064 calls the football2vec chain
          -- "rebuilt public-only upstream" and publish_football2vec_embeddings_hf
          -- (registry mode "derived") asserts "the materialized source had ZERO
          -- access_tier != 'public' rows" — this filter is what makes that true.
          AND av.access_tier = 'public'
    """  # noqa: S608

    raw_sdf = spark.sql(query)

    # Quick emptiness check — limit(1) avoids full DAG computation.
    if raw_sdf.limit(1).count() == 0:
        export_logger.warning("No actions found in fct_action_values — exiting")
        return 0

    # ------------------------------------------------------------------
    # 2. Map action_type (string) → int, normalize coordinates, binarize result
    # ------------------------------------------------------------------
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        ArrayType,
        FloatType,
        IntegerType,
        StructField,
        StructType,
    )

    # Build a Spark mapping column for action_type -> int.
    map_args = [item for pair in _ACTION_TYPE_IDS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    action_map_expr = F.create_map(*map_args)

    actions_sdf = (
        raw_sdf.withColumn(
            "action_type_id",
            F.coalesce(action_map_expr[F.col("action_type")], F.lit(_ACTION_TYPE_IDS["non_action"])),
        )
        .withColumn(
            "x_norm",
            (F.col("start_x") / F.lit(_PITCH_LENGTH)).cast("float"),
        )
        .withColumn(
            "y_norm",
            (F.col("start_y") / F.lit(_PITCH_WIDTH)).cast("float"),
        )
        .withColumn(
            "result_binary",
            F.when(F.col("action_result") == "success", F.lit(1)).otherwise(F.lit(0)).cast("int"),
        )
    )

    # ------------------------------------------------------------------
    # 3. Group by (canonical_player_id, match_id) and collect ordered arrays
    # ------------------------------------------------------------------
    # Sort key: (period ASC, time_seconds ASC) for temporal ordering.
    # Spark's sort_array + collect_list approach:
    #   - Create a struct with sort key and action fields
    #   - collect_list, then sort_array by the sort key fields
    #   - Extract just the action struct fields

    # Build sort+action struct
    actions_sdf = actions_sdf.withColumn(
        "sort_action",
        F.struct(
            F.col("period").cast("int").alias("period"),
            F.col("time_seconds").cast("double").alias("time_seconds"),
            F.col("action_type_id").cast("int").alias("action_type"),
            F.col("x_norm").alias("x"),
            F.col("y_norm").alias("y"),
            F.col("result_binary").alias("result"),
        ),
    )

    # Aggregate: group by (player, match) → collect_list + sort_array
    grouped_sdf = actions_sdf.groupBy("canonical_player_id", "match_id").agg(
        F.first("competition_id").alias("competition_id"),
        F.first("season_id").alias("season_id"),
        F.first("position_group").alias("position_group"),
        F.sort_array(F.collect_list("sort_action"), asc=True).alias("sorted_actions"),
    )

    # Extract just the action fields (drop period/time_seconds sort keys).
    action_struct = StructType(
        [
            StructField("action_type", IntegerType(), False),
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("result", IntegerType(), False),
        ]
    )

    grouped_sdf = grouped_sdf.withColumn(
        "actions",
        F.transform(
            F.col("sorted_actions"),
            lambda s: F.struct(
                s["action_type"].alias("action_type"),
                s["x"].alias("x"),
                s["y"].alias("y"),
                s["result"].alias("result"),
            ),
        ),
    ).select(
        "canonical_player_id",
        "match_id",
        "competition_id",
        "season_id",
        "position_group",
        F.col("actions").cast(ArrayType(action_struct)).alias("actions"),
    )

    # ------------------------------------------------------------------
    # 4. Write Parquet to UC Volume
    # ------------------------------------------------------------------
    export_logger.info("Writing training data Parquet to %s", output_path)
    grouped_sdf.write.mode("overwrite").parquet(output_path)

    # Read row count from the written Parquet to avoid double DAG evaluation.
    row_count = spark.read.parquet(output_path).count()
    export_logger.info("Wrote %d player-match sequences to UC Volume", row_count)

    # ------------------------------------------------------------------
    # 5. Upload to HF Hub
    # ------------------------------------------------------------------
    _upload_to_hf_hub(spark, output_path, export_logger)

    return row_count


def _upload_to_hf_hub(
    spark: SparkSession,
    volume_path: str,
    upload_logger: logging.Logger,
) -> None:
    """Upload Parquet from UC Volume to HF Hub dataset."""
    _ = spark  # Volume reads use the FUSE mount, not Spark

    from ingestion.utils import upload_volume_to_hf_hub

    url = upload_volume_to_hf_hub(
        volume_path,
        _HF_DATASET_REPO,
        logger=upload_logger,
    )
    if url.startswith("file://"):
        upload_logger.warning("HF Hub upload skipped — data at UC Volume only")
        return
    upload_logger.info("Published training dataset to %s", url)

    # Upload README alongside data (PR 4c).
    from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
    from ingestion.utils import resolve_hf_token

    hf_token = resolve_hf_token()
    if hf_token:
        readme_result = upload_hf_readme(
            repo_id=_HF_DATASET_REPO,
            readme_path=get_hf_card_path("football2vec-training-data.md", kind="dataset"),
            hf_token=hf_token,
        )
        upload_logger.info(
            "Uploaded README: %s (sha256=%s)",
            readme_result["commit_url"],
            readme_result["sha256"][:8],
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@workflow("wf-football2vec-v2-export", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object | None = None,
) -> int:
    """Execute the Football2Vec v2 training data export pipeline."""
    _ = ctx
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new training data export work")
    pipeline_logger.info("Starting Football2Vec v2 training data export for %s.%s", catalog, schema)
    row_count = _export_training_sequences(spark, catalog, schema, pipeline_logger)
    pipeline_logger.info("Exported %d player-match training sequences", row_count)
    return row_count


def main() -> None:
    """CLI entry point for Football2Vec v2 training data export."""
    args = parse_ingestion_args("Export SPADL action sequences for Football2Vec v2 training")
    export_logger = configure_logging("export_embeddings_training_data")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, export_logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
