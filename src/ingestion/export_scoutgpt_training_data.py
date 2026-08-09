"""Export SPADL possession episodes for ScoutGPT decoder training.

Reads per-action SPADL sequences from ``fct_action_values``, joins
``dim_players`` for ``canonical_player_id``, segments actions into
possession episodes, and exports to HF Hub.

Possession segmentation rules — a new episode starts when any of:
    1. ``team_id`` changes from the previous action
    2. ``period`` changes
    3. Set piece restart (goalkick, throw_in, freekick_short/crossed,
       corner_short/crossed)
    4. Time gap > 10 seconds since the previous action

Minimum episode length: 3 actions. Shorter episodes are discarded.

Output schema (one row per possession episode):
    episode_id       (string)  — match_id + period + episode_seq
    match_id         (string)
    competition_id   (int)
    season_id        (int)
    team_id          (int)
    data_source      (string)
    actions          (array of struct: action_type int, start_x float,
                      start_y float, end_x float, end_y float,
                      result int, vaep_value float, time_delta float,
                      player_idx int)

A ``player_id_map.json`` mapping canonical_player_id -> contiguous int
is written alongside the Parquet and uploaded to HF Hub.

Reference: Hong et al. (2025). ScoutGPT: Player-conditioned Football
Language Model for Counterfactual Evaluation. arXiv:2512.17266.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)
_HF_DATASET_REPO = "luxury-lakehouse/scoutgpt-training-data"
_UC_VOLUME_PATH = "/Volumes/{catalog}/dev_gold/training_data/scoutgpt"

# SPADL 23-type action vocabulary mapping (string -> int).
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

# Action types that signal a set piece restart (episode boundary).
_SET_PIECE_TYPES: frozenset[str] = frozenset(
    {
        "goalkick",
        "throw_in",
        "freekick_short",
        "freekick_crossed",
        "corner_short",
        "corner_crossed",
    }
)

# SPADL pitch dimensions (meters) for coordinate normalization.
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0

# Maximum time gap (seconds) before a new episode is started.
_TIME_GAP_THRESHOLD = 10.0

# Minimum number of actions for an episode to be retained.
_MIN_EPISODE_LENGTH = 3


# ---------------------------------------------------------------------------
# Player ID mapping
# ---------------------------------------------------------------------------


def _build_player_id_map(
    actions_sdf: DataFrame,
) -> dict[str, int]:
    """Build a contiguous mapping from canonical_player_id to int index.

    Collects all distinct canonical_player_id values from the actions
    DataFrame, sorts them, and assigns contiguous indices starting at 0.

    Args:
        actions_sdf: Spark DataFrame containing a ``canonical_player_id``
            column (string).

    Returns:
        Dict mapping canonical_player_id string to contiguous int.
    """
    player_rows = actions_sdf.select("canonical_player_id").distinct().orderBy("canonical_player_id").collect()
    return {str(row["canonical_player_id"]): idx for idx, row in enumerate(player_rows)}


# ---------------------------------------------------------------------------
# Export logic
# ---------------------------------------------------------------------------


def _export_possession_episodes(
    spark: SparkSession,
    catalog: str,
    schema: str,
    export_logger: logging.Logger,
) -> int:
    """Read SPADL actions, segment into possession episodes, and write Parquet.

    Steps:
        1. Query ``fct_action_values`` joined to ``dim_players`` for
           canonical_player_id and action metadata.
        2. Compute time_delta and apply possession segmentation rules.
        3. Assign episode_id via cumulative sum of boundary markers.
        4. Filter episodes with < 3 actions.
        5. Build contiguous player_id_map and save as JSON.
        6. Normalize coordinates, map action_type -> int, binarize result.
        7. Group by episode, collect actions as sorted struct array.
        8. Write Parquet to UC Volume and upload to HF Hub.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name (unused — reads from gold).
        export_logger: Logger instance.

    Returns:
        Number of possession episode rows exported.
    """
    from ingestion.utils import tolerate_missing_table

    _ = schema  # reads from DEFAULT_GOLD_SCHEMA, not the pipeline schema
    gold = DEFAULT_GOLD_SCHEMA

    # ------------------------------------------------------------------
    # 0. Skip guard — compare upstream action count vs existing episodes
    # ------------------------------------------------------------------
    output_path = _UC_VOLUME_PATH.format(catalog=catalog)
    upstream_count: int | None = None
    with tolerate_missing_table(export_logger, f"Upstream fct_action_values missing in {catalog}.{gold}"):
        upstream_count = spark.table(f"{catalog}.{gold}.fct_action_values").count()

    if upstream_count is not None:
        existing_count = 0
        with tolerate_missing_table(export_logger, f"No existing export at {output_path} — full export"):
            existing_count = spark.read.parquet(output_path).count()

        if existing_count > 0:
            export_logger.info(
                "ScoutGPT training data already exists at %s (%d episodes) — checking upstream",
                output_path,
                existing_count,
            )
            # Simple freshness heuristic: if action count hasn't changed,
            # the segmented episodes won't change either.
            export_logger.info(
                "Upstream fct_action_values has %d actions",
                upstream_count,
            )
            # Store upstream count as a metadata marker to detect changes.
            # If no change in action count, skip re-export.
            stored_count: int | None = None
            with tolerate_missing_table(export_logger, "No upstream count marker found — re-exporting"):
                meta_df = spark.read.text(f"{output_path}/_upstream_count")
                stored_count = int(meta_df.collect()[0]["value"])

            if stored_count is not None:
                if stored_count == upstream_count:
                    export_logger.info(
                        "Upstream action count unchanged (%d) — skipping re-export",
                        upstream_count,
                    )
                    return existing_count
                export_logger.info(
                    "Upstream action count changed (%d -> %d) — re-exporting",
                    stored_count,
                    upstream_count,
                )

    # ------------------------------------------------------------------
    # 1. Load actions joined to dim_players
    # ------------------------------------------------------------------
    query = f"""
        SELECT
            CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
            CAST(av.match_id AS STRING)            AS match_id,
            CAST(av.team_id AS INT)                AS team_id,
            CAST(av.competition_id AS INT)         AS competition_id,
            CAST(av.season_id AS INT)              AS season_id,
            av.data_source,
            av.action_type,
            av.action_result,
            av.period,
            av.time_seconds,
            av.start_x,
            av.start_y,
            av.end_x,
            av.end_y,
            av.vaep_value
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
    # 2. Build player_id_map before any transformations
    # ------------------------------------------------------------------
    player_id_map = _build_player_id_map(raw_sdf)
    export_logger.info("Built player_id_map with %d unique players", len(player_id_map))

    # ------------------------------------------------------------------
    # 3. Possession segmentation via window functions
    # ------------------------------------------------------------------
    from pyspark.sql import Window
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        ArrayType,
        FloatType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    # Order actions within each match by (period, time_seconds).
    match_window = Window.partitionBy("match_id").orderBy("period", "time_seconds")

    # Compute lagged values for boundary detection.
    seg_sdf = (
        raw_sdf.withColumn("prev_team_id", F.lag("team_id").over(match_window))
        .withColumn("prev_period", F.lag("period").over(match_window))
        .withColumn("prev_time_seconds", F.lag("time_seconds").over(match_window))
    )

    # Mark episode boundaries:
    # 1. team_id changes
    # 2. period changes
    # 3. set piece restart
    # 4. time gap > 10s
    set_piece_types = list(_SET_PIECE_TYPES)
    seg_sdf = seg_sdf.withColumn(
        "is_boundary",
        F.when(F.col("prev_team_id").isNull(), F.lit(1))  # first action in match
        .when(F.col("team_id") != F.col("prev_team_id"), F.lit(1))
        .when(F.col("period") != F.col("prev_period"), F.lit(1))
        .when(F.col("action_type").isin(set_piece_types), F.lit(1))
        .when(
            (F.col("period") == F.col("prev_period"))
            & (F.col("time_seconds") - F.col("prev_time_seconds") > F.lit(_TIME_GAP_THRESHOLD)),
            F.lit(1),
        )
        .otherwise(F.lit(0)),
    )

    # Assign episode_seq as cumulative sum of boundaries within each match.
    seg_sdf = seg_sdf.withColumn(
        "episode_seq",
        F.sum("is_boundary").over(match_window),
    )

    # Compute time_delta: seconds since previous action within the same
    # match and period. 0.0 at episode boundaries.
    seg_sdf = seg_sdf.withColumn(
        "time_delta",
        F.when(
            F.col("is_boundary") == 1,
            F.lit(0.0),
        )
        .otherwise(
            F.coalesce(
                F.col("time_seconds") - F.col("prev_time_seconds"),
                F.lit(0.0),
            )
        )
        .cast("float"),
    )

    # Build episode_id as match_id + period + episode_seq.
    seg_sdf = seg_sdf.withColumn(
        "episode_id",
        F.concat_ws(
            "_",
            F.col("match_id"),
            F.col("period").cast("string"),
            F.col("episode_seq").cast("string"),
        ),
    )

    # ------------------------------------------------------------------
    # 4. Filter episodes with fewer than MIN_EPISODE_LENGTH actions
    # ------------------------------------------------------------------
    episode_counts = seg_sdf.groupBy("episode_id").agg(
        F.count("*").alias("action_count"),
    )
    valid_episodes = episode_counts.filter(F.col("action_count") >= F.lit(_MIN_EPISODE_LENGTH)).select("episode_id")

    seg_sdf = seg_sdf.join(valid_episodes, on="episode_id", how="inner")

    # ------------------------------------------------------------------
    # 5. Map action_type -> int, normalize coordinates, binarize result
    # ------------------------------------------------------------------
    map_args = [item for pair in _ACTION_TYPE_IDS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    action_map_expr = F.create_map(*map_args)

    seg_sdf = (
        seg_sdf.withColumn(
            "action_type_id",
            F.coalesce(
                action_map_expr[F.col("action_type")],
                F.lit(_ACTION_TYPE_IDS["non_action"]),
            ),
        )
        .withColumn("start_x_norm", (F.col("start_x") / F.lit(_PITCH_LENGTH)).cast("float"))
        .withColumn("start_y_norm", (F.col("start_y") / F.lit(_PITCH_WIDTH)).cast("float"))
        .withColumn(
            "end_x_norm",
            (F.coalesce(F.col("end_x"), F.col("start_x")) / F.lit(_PITCH_LENGTH)).cast("float"),
        )
        .withColumn(
            "end_y_norm",
            (F.coalesce(F.col("end_y"), F.col("start_y")) / F.lit(_PITCH_WIDTH)).cast("float"),
        )
        .withColumn(
            "result_binary",
            F.when(F.col("action_result") == "success", F.lit(1)).otherwise(F.lit(0)).cast("int"),
        )
        .withColumn(
            "vaep_val",
            F.coalesce(F.col("vaep_value"), F.lit(0.0)).cast("float"),
        )
    )

    # ------------------------------------------------------------------
    # 6. Map player IDs to contiguous indices via join
    # ------------------------------------------------------------------
    # Build a small lookup DataFrame from the player_id_map (no broadcast
    # variables — Databricks serverless constraint).
    player_map_rows = [(pid, idx) for pid, idx in player_id_map.items()]
    player_map_sdf = spark.createDataFrame(
        player_map_rows,
        StructType(
            [
                StructField("_pid", StringType(), False),
                StructField("player_idx", IntegerType(), False),
            ]
        ),
    )

    seg_sdf = seg_sdf.join(
        player_map_sdf,
        seg_sdf["canonical_player_id"] == player_map_sdf["_pid"],
        how="left",
    ).drop("_pid")

    # ------------------------------------------------------------------
    # 7. Group by episode, collect sorted action struct arrays.
    # ------------------------------------------------------------------
    seg_sdf = seg_sdf.withColumn(
        "sort_action",
        F.struct(
            F.col("period").cast("int").alias("period"),
            F.col("time_seconds").cast("double").alias("time_seconds"),
            F.col("action_type_id").cast("int").alias("action_type"),
            F.col("start_x_norm").alias("start_x"),
            F.col("start_y_norm").alias("start_y"),
            F.col("end_x_norm").alias("end_x"),
            F.col("end_y_norm").alias("end_y"),
            F.col("result_binary").alias("result"),
            F.col("vaep_val").alias("vaep_value"),
            F.col("time_delta").alias("time_delta"),
            F.coalesce(F.col("player_idx"), F.lit(0)).alias("player_idx"),
        ),
    )

    grouped_sdf = seg_sdf.groupBy("episode_id").agg(
        F.first("match_id").alias("match_id"),
        F.first("competition_id").alias("competition_id"),
        F.first("season_id").alias("season_id"),
        F.first("team_id").alias("team_id"),
        F.first("data_source").alias("data_source"),
        F.sort_array(F.collect_list("sort_action"), asc=True).alias("sorted_actions"),
    )

    # Extract action fields (drop period/time_seconds sort keys).
    action_struct = StructType(
        [
            StructField("action_type", IntegerType(), False),
            StructField("start_x", FloatType(), True),
            StructField("start_y", FloatType(), True),
            StructField("end_x", FloatType(), True),
            StructField("end_y", FloatType(), True),
            StructField("result", IntegerType(), False),
            StructField("vaep_value", FloatType(), True),
            StructField("time_delta", FloatType(), True),
            StructField("player_idx", IntegerType(), True),
        ]
    )

    grouped_sdf = grouped_sdf.withColumn(
        "actions",
        F.transform(
            F.col("sorted_actions"),
            lambda s: F.struct(
                s["action_type"].alias("action_type"),
                s["start_x"].alias("start_x"),
                s["start_y"].alias("start_y"),
                s["end_x"].alias("end_x"),
                s["end_y"].alias("end_y"),
                s["result"].alias("result"),
                s["vaep_value"].alias("vaep_value"),
                s["time_delta"].alias("time_delta"),
                s["player_idx"].alias("player_idx"),
            ),
        ),
    ).select(
        "episode_id",
        "match_id",
        "competition_id",
        "season_id",
        "team_id",
        "data_source",
        F.col("actions").cast(ArrayType(action_struct)).alias("actions"),
    )

    # ------------------------------------------------------------------
    # 8. Log episode length statistics
    # ------------------------------------------------------------------
    episode_lengths = grouped_sdf.withColumn("ep_len", F.size("actions"))
    stats_row = episode_lengths.agg(
        F.count("*").alias("count"),
        F.mean("ep_len").alias("mean"),
        F.expr("percentile_approx(ep_len, 0.5)").alias("median"),
        F.expr("percentile_approx(ep_len, 0.05)").alias("p5"),
        F.expr("percentile_approx(ep_len, 0.25)").alias("p25"),
        F.expr("percentile_approx(ep_len, 0.75)").alias("p75"),
        F.expr("percentile_approx(ep_len, 0.95)").alias("p95"),
        F.max("ep_len").alias("max"),
    ).collect()[0]

    export_logger.info(
        "Episode length stats — count=%d, mean=%.1f, median=%s, p5=%s, p25=%s, p75=%s, p95=%s, max=%s",
        stats_row["count"],
        float(stats_row["mean"]),
        stats_row["median"],
        stats_row["p5"],
        stats_row["p25"],
        stats_row["p75"],
        stats_row["p95"],
        stats_row["max"],
    )

    # ------------------------------------------------------------------
    # 9. Write Parquet to UC Volume
    # ------------------------------------------------------------------
    from ingestion.utils import ensure_volume_directory

    ensure_volume_directory(output_path)
    export_logger.info("Writing ScoutGPT training data Parquet to %s", output_path)
    grouped_sdf.write.mode("overwrite").parquet(output_path)

    # Read row count from the written Parquet to avoid double DAG evaluation.
    row_count = spark.read.parquet(output_path).count()
    export_logger.info("Wrote %d possession episodes to UC Volume", row_count)

    # Write upstream count marker for skip guard.
    if upstream_count is not None:
        spark.createDataFrame(
            [(str(upstream_count),)],
            StructType([StructField("value", StringType(), False)]),
        ).write.mode("overwrite").text(f"{output_path}/_upstream_count")

    # ------------------------------------------------------------------
    # 10. Save player_id_map.json to UC Volume
    # ------------------------------------------------------------------
    player_map_json = json.dumps(player_id_map, indent=2)
    spark.createDataFrame(
        [(player_map_json,)],
        StructType([StructField("value", StringType(), False)]),
    ).write.mode("overwrite").text(f"{output_path}/_player_id_map")
    export_logger.info("Wrote player_id_map.json (%d players) to UC Volume", len(player_id_map))

    # ------------------------------------------------------------------
    # 11. Upload Parquet + player_id_map to HF Hub
    # ------------------------------------------------------------------
    _upload_to_hf_hub(spark, output_path, player_id_map, export_logger)

    return row_count


def _upload_to_hf_hub(
    spark: SparkSession,
    volume_path: str,
    player_id_map: dict[str, int],
    upload_logger: logging.Logger,
) -> None:
    """Upload Parquet and player_id_map.json from UC Volume to HF Hub."""
    _ = spark  # Volume reads use the FUSE mount, not Spark

    from ingestion.utils import upload_volume_to_hf_hub

    # Upload Parquet data.
    url = upload_volume_to_hf_hub(volume_path, _HF_DATASET_REPO, logger=upload_logger)
    if url.startswith("file://"):
        upload_logger.warning("HF Hub upload skipped — data at UC Volume only")
        return
    upload_logger.info("Published training dataset Parquet to %s", url)

    # Upload player_id_map.json as a separate file.
    from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
    from ingestion.utils import resolve_hf_token

    hf_token = resolve_hf_token()
    if not hf_token:
        return

    from huggingface_hub import HfApi  # type: ignore[import-not-found]

    api = HfApi(token=hf_token)
    map_bytes = json.dumps(player_id_map, indent=2).encode("utf-8")
    api.upload_file(
        path_or_fileobj=map_bytes,
        path_in_repo="player_id_map.json",
        repo_id=_HF_DATASET_REPO,
        repo_type="dataset",
        token=hf_token,
    )
    upload_logger.info("Uploaded player_id_map.json to %s", url)

    # Upload README alongside data (PR 4c).
    readme_result = upload_hf_readme(
        repo_id=_HF_DATASET_REPO,
        readme_path=get_hf_card_path("scoutgpt-training-data.md", kind="dataset"),
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


@workflow("wf-scoutgpt-export", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    ctx: object | None = None,
) -> int:
    """Execute the ScoutGPT training data export pipeline."""
    _ = ctx
    pipeline_logger.info("Starting ScoutGPT training data export for %s.%s", catalog, schema)
    row_count = _export_possession_episodes(spark, catalog, schema, pipeline_logger)
    pipeline_logger.info("Exported %d possession episode training sequences", row_count)
    return row_count


def main() -> None:
    """CLI entry point for ScoutGPT training data export."""
    args = parse_ingestion_args("Export SPADL possession episodes for ScoutGPT training")
    export_logger = configure_logging("export_scoutgpt_training_data")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, export_logger)


if __name__ == "__main__":
    main()
