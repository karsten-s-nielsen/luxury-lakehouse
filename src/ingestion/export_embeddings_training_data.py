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
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_GOLD_SCHEMA = "dev_gold"
_HF_DATASET_REPO = "luxury-lakehouse/football2vec-training-data"
_UC_VOLUME_PATH = "/Volumes/{catalog}/dev_gold/training_data/football2vec_v2"

# SPADL 23-type action vocabulary mapping (string → int).
# Canonical ordering matches socceraction and defcon_lite._ACTION_TYPE_IDS.
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
    _ = schema  # reads from _GOLD_SCHEMA, not the pipeline schema
    gold = _GOLD_SCHEMA

    # ------------------------------------------------------------------
    # 0. Skip guard — compare upstream freshness against last export
    # ------------------------------------------------------------------
    output_path = _UC_VOLUME_PATH.format(catalog=catalog)
    try:
        upstream_max_ts = (
            spark.table(f"{catalog}.{gold}.fct_action_values")
            .selectExpr("MAX(_ingested_at) AS max_ts")
            .collect()[0]["max_ts"]
        )
    except Exception:
        upstream_max_ts = None

    if upstream_max_ts is not None:
        try:
            existing_count = spark.read.parquet(output_path).count()
            if existing_count > 0:
                export_logger.info(
                    "Training data already exists at %s (%d rows) — checking upstream freshness",
                    output_path,
                    existing_count,
                )
                # Compare against a simple marker: if the Parquet already has
                # the same row count as the grouped action values, skip.
                upstream_count = (
                    spark.table(f"{catalog}.{gold}.fct_action_values")
                    .select("player_id", "match_id")
                    .distinct()
                    .count()
                )
                if existing_count >= upstream_count:
                    export_logger.info(
                        "Existing export (%d rows) covers all %d upstream player-match pairs — skipping re-export",
                        existing_count,
                        upstream_count,
                    )
                    return existing_count
                export_logger.info(
                    "Upstream has %d player-match pairs vs %d exported — re-exporting",
                    upstream_count,
                    existing_count,
                )
        except Exception:
            export_logger.info("No existing export at %s — full export", output_path)

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
    """Download Parquet from UC Volume to local temp dir and upload to HF Hub.

    On Databricks serverless, Spark cannot write to local filesystem. So
    we read the Volume path on the driver and re-write locally for the
    HF Hub upload_folder API.

    Args:
        spark: Active Spark session (unused — reads via dbutils/os).
        volume_path: UC Volume path containing Parquet part files.
        upload_logger: Logger instance.
    """
    _ = spark  # Volume reads use the FUSE mount, not Spark

    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""

    if not hf_token:
        upload_logger.warning(
            "No HF_TOKEN found — skipping HF Hub upload. Data is available at UC Volume: %s",
            volume_path,
        )
        return

    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)

    # Ensure the dataset repo exists.
    api.create_repo(
        _HF_DATASET_REPO,
        exist_ok=True,
        repo_type="dataset",
        token=hf_token,
    )
    upload_logger.info("Ensured HF dataset repo exists: %s", _HF_DATASET_REPO)

    # Copy Parquet files from UC Volume to local temp dir for upload.
    # UC Volumes are accessible via FUSE at the same path on Databricks.
    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / "data"
        staging_dir.mkdir(parents=True, exist_ok=True)

        import shutil

        volume_dir = Path(volume_path)
        if not volume_dir.exists():
            upload_logger.error("UC Volume path does not exist: %s", volume_path)
            return

        # Copy all parquet part files to staging
        part_count = 0
        for part_file in volume_dir.glob("*.parquet"):
            shutil.copy2(str(part_file), str(staging_dir / part_file.name))
            part_count += 1

        # Also copy _SUCCESS and any other metadata files
        for meta_file in volume_dir.glob("_*"):
            if meta_file.is_file():
                shutil.copy2(str(meta_file), str(staging_dir / meta_file.name))

        upload_logger.info("Staged %d Parquet files for HF Hub upload", part_count)

        if part_count == 0:
            upload_logger.warning("No Parquet files found at %s — skipping upload", volume_path)
            return

        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo="data",
            repo_id=_HF_DATASET_REPO,
            repo_type="dataset",
            token=hf_token,
        )

    dataset_url = f"https://huggingface.co/datasets/{_HF_DATASET_REPO}"
    upload_logger.info("Published training dataset to %s", dataset_url)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@workflow("wf-football2vec-v2", phase="training")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    ctx: object | None = None,
) -> None:
    """Execute the Football2Vec v2 training data export pipeline."""
    _ = ctx
    pipeline_logger.info("Starting Football2Vec v2 training data export for %s.%s", catalog, schema)
    row_count = _export_training_sequences(spark, catalog, schema, pipeline_logger)
    pipeline_logger.info("Exported %d player-match training sequences", row_count)


def main() -> None:
    """CLI entry point for Football2Vec v2 training data export."""
    args = parse_ingestion_args("Export SPADL action sequences for Football2Vec v2 training")
    export_logger = configure_logging("export_embeddings_training_data")
    spark = get_spark_session()

    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook

    register_hook(CostEstimateHook(spark, args.catalog, args.schema))

    run_pipeline(spark, args.catalog, args.schema, export_logger)


if __name__ == "__main__":
    main()
