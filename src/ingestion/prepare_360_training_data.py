"""Prepare 360-enriched training data for Football2Vec 360 model.

Joins fct_action_values with stg_statsbomb__360 to build per-action
freeze frame player matrices. Exports to HF Hub.

Runs on Databricks (requires Spark for the 15.58M row join).

Output schema (one row per player-match):
    canonical_player_id  (string)
    match_id             (string)
    competition_id       (int)
    season_id            (int)
    position_group       (string, nullable)
    actions              (array of struct: action_type int, x float, y float, result int)
    freeze_frames        (array of struct: action_idx int, players array of struct [x float, y float,
                          is_keeper bool, is_teammate bool])

360 freeze frame coordinates use the StatsBomb 120x80 system.
SPADL action coordinates use the 105x68 meter system.
Both are normalized to [0, 1] within their own coordinate systems.

Usage (Databricks):
    prepare_360_training_data --catalog soccer_analytics --schema dev_gold
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from ingestion.utils import resolve_hf_token
from shared.constants import DEFAULT_GOLD_SCHEMA, DEFAULT_SILVER_SCHEMA, IDENTIFIER_RE
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)
_guard_logger = logging.getLogger(f"{__name__}.guard")

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HF_ORG = "luxury-lakehouse"
DATASET_REPO = f"{HF_ORG}/football2vec-360-training-data"

# StatsBomb 360 coordinate system (yards)
_SB_PITCH_LENGTH = 120.0
_SB_PITCH_WIDTH = 80.0

# SPADL coordinate system (meters)
_SPADL_PITCH_LENGTH = 105.0
_SPADL_PITCH_WIDTH = 68.0

# SPADL 23-type action vocabulary mapping (string → int).
# Canonical ordering matches silly_kicks SPADL and football2vec v2.
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

# UC Volume staging path for Parquet output
_DEFAULT_VOLUME_PATH = "/Volumes/soccer_analytics/dev_gold/training_data/football2vec_360"

# Silver schema where stg_statsbomb__360 lives
_SILVER_SCHEMA = DEFAULT_SILVER_SCHEMA


class _Prepare360Guard:
    """SkipGuard adapter for 360 training data preparation."""

    workflow_id = "wf-prepare-360-data"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if upstream data has grown since last export."""
        from ingestion.utils import tolerate_missing_table

        volume_path = _DEFAULT_VOLUME_PATH

        # fct_action_values is a GOLD mart; hf_sync passes --schema bronze. Under the
        # caller-passed schema this read raised TABLE_OR_VIEW_NOT_FOUND, which
        # tolerate_missing_table absorbed at INFO and turned into "nothing to do" —
        # so this export silently did nothing on every run (2026-08-07). ADR-073.
        _ = schema  # reads from DEFAULT_GOLD_SCHEMA, not the pipeline schema
        upstream_count: int | None = None
        with tolerate_missing_table(
            _guard_logger, f"Upstream fct_action_values missing in {catalog}.{DEFAULT_GOLD_SCHEMA}"
        ):
            upstream_count = (
                spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values")
                .filter("data_source = 'statsbomb'")
                .select("player_id", "match_id")
                .distinct()
                .count()
            )

        if upstream_count is None:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        existing_count = 0
        with tolerate_missing_table(_guard_logger, f"No existing export at {volume_path}"):
            existing_count = spark.read.parquet(volume_path).count()

        if existing_count >= upstream_count and existing_count > 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=upstream_count - existing_count,
        )


skip_guard = _Prepare360Guard()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_identifier(field_name: str, value: str) -> None:
    """Validate a SQL identifier against the safe-name pattern.

    Args:
        field_name: Human-readable name for error messages (e.g. ``catalog``).
        value: The identifier string to validate.

    Raises:
        SystemExit: If the value does not match the safe identifier pattern.
    """
    if not IDENTIFIER_RE.match(value):
        msg = f"Invalid {field_name} '{value}': must match {IDENTIFIER_RE.pattern}"
        raise SystemExit(msg)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def _build_query(catalog: str, gold: str) -> str:
    """Build the SQL query joining SPADL actions with 360 freeze frames.

    Uses explicit catalog-level reference for the silver schema
    (``{catalog}.dev_silver.stg_statsbomb__360``), since 360 data always
    lives in the fixed silver schema regardless of the ``--schema`` arg.

    Args:
        catalog: Unity Catalog name (already validated).
        gold: Gold-layer schema name (DEFAULT_GOLD_SCHEMA -- a trusted constant).

    Returns:
        SQL string.
    """
    # catalog is validated by _validate_identifier before this call; `gold` is the
    # module-level DEFAULT_GOLD_SCHEMA constant (ADR-073) -- a trusted literal, never
    # user input, which is a stronger guarantee than runtime validation.
    return f"""\
SELECT
    CAST(dp.canonical_player_id AS STRING)    AS canonical_player_id,
    CAST(av.match_id AS STRING)               AS match_id,
    CAST(av.competition_id AS INT)            AS competition_id,
    CAST(av.season_id AS INT)                 AS season_id,
    dp.position_group,
    av.action_type,
    CAST(av.start_x / {_SPADL_PITCH_LENGTH}   AS FLOAT) AS x,
    CAST(av.start_y / {_SPADL_PITCH_WIDTH}    AS FLOAT) AS y,
    av.action_result,
    av.period,
    av.time_seconds,
    av.original_event_id,
    CAST(ff.location_x / {_SB_PITCH_LENGTH}   AS FLOAT) AS ff_x_norm,
    CAST(ff.location_y / {_SB_PITCH_WIDTH}    AS FLOAT) AS ff_y_norm,
    CAST(ff.is_keeper                          AS BOOLEAN) AS ff_is_keeper,
    CAST(ff.is_teammate                        AS BOOLEAN) AS ff_is_teammate
FROM {catalog}.{gold}.fct_action_values av
INNER JOIN {catalog}.{_SILVER_SCHEMA}.stg_statsbomb__360 ff
    ON av.original_event_id = ff.event_uuid
INNER JOIN {catalog}.{gold}.dim_players dp
    ON av.player_id = dp.player_id
WHERE av.data_source = 'statsbomb'
  AND av.player_id IS NOT NULL
  AND dp.canonical_player_id IS NOT NULL
  AND av.action_type IS NOT NULL
  AND av.start_x IS NOT NULL
  AND av.start_y IS NOT NULL
  AND ff.location_x IS NOT NULL
  AND ff.location_y IS NOT NULL
"""  # noqa: S608


# ---------------------------------------------------------------------------
# Spark aggregation
# ---------------------------------------------------------------------------


def _build_training_dataset(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> tuple[object, int]:
    """Join actions with 360 freeze frames and aggregate into per-player-match sequences.

    Each output row represents one player's full match sequence, with:
    - ``actions``: temporally ordered array of SPADL action structs
      ``(action_type int, x float, y float, result int)``
    - ``freeze_frames``: parallel array, one entry per action, each containing
      the N freeze-frame players at that moment as a sub-array of
      ``(x float, y float, is_keeper bool, is_teammate bool)``

    The two arrays are positionally aligned: ``actions[i]`` and ``freeze_frames[i]``
    describe the same on-ball action.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Gold schema name.

    Returns:
        Tuple of (Spark DataFrame, row_count). The DataFrame has one row per
        player-match and is ready for Parquet write.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        FloatType,
        IntegerType,
        StructField,
        StructType,
    )

    # _build_query's `schema` contract is "gold schema" — hf_sync passes bronze (ADR-073).
    _ = schema  # reads from DEFAULT_GOLD_SCHEMA, not the pipeline schema
    sql = _build_query(catalog, DEFAULT_GOLD_SCHEMA)
    logger.info("Querying fct_action_values + stg_statsbomb__360 join from %s.%s", catalog, DEFAULT_GOLD_SCHEMA)

    start = time.time()
    raw_sdf = spark.sql(sql)

    # Quick emptiness check — limit(1) avoids full DAG computation.
    if raw_sdf.limit(1).count() == 0:
        raise RuntimeError(
            f"No rows returned from {catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values joined with "
            f"{catalog}.{_SILVER_SCHEMA}.stg_statsbomb__360. "
            "Check that fct_action_values has been built by dbt and that StatsBomb 360 "
            "data has been ingested (data_source = 'statsbomb', original_event_id not null)."
        )

    elapsed = time.time() - start
    logger.info("Join query returned data in %.2fs — beginning Spark aggregation", elapsed)

    # ------------------------------------------------------------------
    # 1. Map action_type (string) → int, binarize result
    # ------------------------------------------------------------------
    map_args = [item for pair in _ACTION_TYPE_IDS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    action_map_expr = F.create_map(*map_args)

    enriched_sdf = raw_sdf.withColumn(
        "action_type_id",
        F.coalesce(action_map_expr[F.col("action_type")], F.lit(_ACTION_TYPE_IDS["non_action"])),
    ).withColumn(
        "result_binary",
        F.when(F.col("action_result") == "success", F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )

    # ------------------------------------------------------------------
    # 2. Build sort+action struct and per-player freeze frame struct
    #
    # Sort key: (period ASC, time_seconds ASC, original_event_id ASC) for
    # temporal ordering. event_id as tiebreak ensures stable sort within
    # the same timestamp (multiple freeze-frame players share same event).
    # ------------------------------------------------------------------
    enriched_sdf = enriched_sdf.withColumn(
        "sort_action_ff",
        F.struct(
            F.col("period").cast("int").alias("period"),
            F.col("time_seconds").cast("double").alias("time_seconds"),
            F.col("original_event_id").alias("event_id"),
            F.col("action_type_id").cast("int").alias("action_type"),
            F.col("x").alias("x"),
            F.col("y").alias("y"),
            F.col("result_binary").alias("result"),
            # Freeze frame player fields nested inside the same sort struct so
            # collect_list keeps them aligned with their action.
            F.col("ff_x_norm").alias("ff_x"),
            F.col("ff_y_norm").alias("ff_y"),
            F.col("ff_is_keeper").alias("ff_is_keeper"),
            F.col("ff_is_teammate").alias("ff_is_teammate"),
        ),
    )

    # ------------------------------------------------------------------
    # 3. Group by (canonical_player_id, match_id, original_event_id) first
    #    to collect freeze-frame players per action, then group by
    #    (canonical_player_id, match_id) to build action sequences.
    #
    # Two-pass aggregation:
    #   Pass 1: per (player, match, event) → collect freeze-frame players
    #           into an array, extract single action fields via first()
    #   Pass 2: per (player, match) → sort_array + build final sequences
    # ------------------------------------------------------------------

    # Pass 1: Collect freeze-frame players per action
    # Spark's collect_list/transform always infers nullable fields regardless
    # of input nullability. All struct fields must be nullable=True to avoid
    # DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION on the downstream .cast() calls.
    player_struct = StructType(
        [
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("is_keeper", BooleanType(), True),
            StructField("is_teammate", BooleanType(), True),
        ]
    )

    per_action_sdf = enriched_sdf.groupBy("canonical_player_id", "match_id", "original_event_id").agg(
        F.first("competition_id").alias("competition_id"),
        F.first("season_id").alias("season_id"),
        F.first("position_group").alias("position_group"),
        F.first("period").alias("period"),
        F.first("time_seconds").alias("time_seconds"),
        F.first("action_type_id").alias("action_type_id"),
        F.first("x").alias("x"),
        F.first("y").alias("y"),
        F.first("result_binary").alias("result_binary"),
        # Collect all freeze-frame players for this action into an array
        F.collect_list(
            F.struct(
                F.col("ff_x_norm").cast("float").alias("x"),
                F.col("ff_y_norm").cast("float").alias("y"),
                F.coalesce(F.col("ff_is_keeper"), F.lit(False)).alias("is_keeper"),
                F.coalesce(F.col("ff_is_teammate"), F.lit(False)).alias("is_teammate"),
            )
        )
        .cast(ArrayType(player_struct))
        .alias("players"),
    )

    # Build the per-action sort struct (with players array embedded for aligned sorting)
    per_action_sdf = per_action_sdf.withColumn(
        "sort_entry",
        F.struct(
            F.col("period").cast("int").alias("period"),
            F.col("time_seconds").cast("double").alias("time_seconds"),
            F.col("original_event_id").alias("event_id"),
            F.col("action_type_id").cast("int").alias("action_type"),
            F.col("x").alias("x"),
            F.col("y").alias("y"),
            F.col("result_binary").alias("result"),
            F.col("players").alias("players"),
        ),
    )

    # Pass 2: Group by (player, match) → collect_list + sort_array
    grouped_sdf = per_action_sdf.groupBy("canonical_player_id", "match_id").agg(
        F.first("competition_id").alias("competition_id"),
        F.first("season_id").alias("season_id"),
        F.first("position_group").alias("position_group"),
        F.sort_array(F.collect_list("sort_entry"), asc=True).alias("sorted_entries"),
    )

    # ------------------------------------------------------------------
    # 4. Extract actions and freeze_frames as parallel aligned arrays
    # ------------------------------------------------------------------
    action_struct = StructType(
        [
            StructField("action_type", IntegerType(), True),
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("result", IntegerType(), True),
        ]
    )

    ff_entry_struct = StructType(
        [
            StructField("players", ArrayType(player_struct), True),
        ]
    )

    grouped_sdf = (
        grouped_sdf.withColumn(
            "actions",
            F.transform(
                F.col("sorted_entries"),
                lambda s: F.struct(
                    s["action_type"].alias("action_type"),
                    s["x"].alias("x"),
                    s["y"].alias("y"),
                    s["result"].alias("result"),
                ),
            ).cast(ArrayType(action_struct)),
        )
        .withColumn(
            "freeze_frames",
            F.transform(
                F.col("sorted_entries"),
                lambda s: F.struct(
                    s["players"].alias("players"),
                ),
            ).cast(ArrayType(ff_entry_struct)),
        )
        .select(
            "canonical_player_id",
            "match_id",
            "competition_id",
            "season_id",
            "position_group",
            "actions",
            "freeze_frames",
        )
    )

    # Materialize row count after aggregation (single DAG pass)
    start = time.time()
    row_count = grouped_sdf.count()
    elapsed = time.time() - start
    logger.info("Aggregated %d player-match rows in %.2fs", row_count, elapsed)

    return grouped_sdf, row_count


# ---------------------------------------------------------------------------
# HF Hub upload
# ---------------------------------------------------------------------------


def _upload_to_hf_hub(volume_path: str, spark: object) -> str:
    """Upload Parquet from UC Volume to HF Hub dataset."""
    _ = spark  # Volume reads via FUSE, not Spark

    from ingestion.utils import upload_volume_to_hf_hub

    # delete_patterns intentionally omitted: upload_volume_to_hf_hub now sweeps ["**"] by default
    # (ADR-072 amendment). This call previously passed ["data/*.parquet", "data/_*"], the
    # path_in_repo-prefixed form copied from the helper's old docstring, which matched NOTHING.
    return upload_volume_to_hf_hub(volume_path, DATASET_REPO, logger=logger)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@workflow("wf-prepare-360-data", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_path: str,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Join 360 freeze frames with SPADL actions, stage to UC Volume, upload to HF Hub."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new 360 training data work")

    # ------------------------------------------------------------------
    # 1. Build 360-enriched training dataset via Spark aggregation
    # ------------------------------------------------------------------
    grouped_sdf, row_count = _build_training_dataset(spark, catalog, schema)

    if row_count == 0:
        raise RuntimeError(
            "Aggregation produced zero player-match rows. "
            "Check that fct_action_values has StatsBomb actions with original_event_id "
            "and that stg_statsbomb__360 has been built."
        )

    # ------------------------------------------------------------------
    # 3. Write Parquet to UC Volume
    # ------------------------------------------------------------------
    logger.info("Writing %d player-match rows to %s", row_count, volume_path)

    start = time.time()
    grouped_sdf.write.mode("overwrite").parquet(volume_path)  # type: ignore[union-attr]
    elapsed = time.time() - start
    logger.info("Parquet write complete in %.2fs", elapsed)

    # ------------------------------------------------------------------
    # 4. Upload to HF Hub
    # ------------------------------------------------------------------
    logger.info("Uploading training data to HF Hub dataset: %s", DATASET_REPO)
    dataset_url = _upload_to_hf_hub(volume_path, spark)

    # ------------------------------------------------------------------
    # 5. Upload README alongside data (PR 4c).
    # ------------------------------------------------------------------
    if not dataset_url.startswith("file://"):
        hf_token = resolve_hf_token()
        if hf_token:
            readme_result = upload_hf_readme(
                repo_id=DATASET_REPO,
                readme_path=get_hf_card_path("football2vec-360-training-data.md", kind="dataset"),
                hf_token=hf_token,
            )
            logger.info(
                "Uploaded README: %s (sha256=%s)",
                readme_result["commit_url"],
                readme_result["sha256"][:8],
            )

    logger.info("Pipeline complete. Dataset: %s", dataset_url)
    logger.info(
        "Final stats: %d player-match sequences with 360 context exported",
        row_count,
    )
    return row_count


def main() -> None:
    """CLI entry point for 360 training data preparation."""
    # Late import — PySpark only available in Databricks runtime
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    logger.info("Starting Football2Vec 360 training data preparation pipeline")

    parser = argparse.ArgumentParser(description="Prepare 360-enriched training data for Football2Vec 360 model")
    parser.add_argument(
        "--catalog",
        default="soccer_analytics",
        help="Unity Catalog name (default: soccer_analytics)",
    )
    parser.add_argument(
        "--schema",
        default="dev_gold",
        help="Gold schema name (default: dev_gold)",
    )
    parser.add_argument(
        "--volume-path",
        default=_DEFAULT_VOLUME_PATH,
        help=(f"UC Volume staging path for Parquet output (default: {_DEFAULT_VOLUME_PATH})"),
    )
    args = parser.parse_args()

    catalog: str = args.catalog
    schema: str = args.schema
    volume_path: str = args.volume_path.rstrip("/")

    # Validate SQL identifiers before interpolating into queries
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)

    spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, catalog, schema)

    filter_result = timed_check(skip_guard, spark, catalog, schema)

    run_pipeline(spark, catalog, schema, volume_path, filter_result=filter_result)


if __name__ == "__main__":
    main()
