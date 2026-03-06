"""DEFCON-lite batch computation pipeline.

Reads action values from ``fct_action_values`` and freeze frame data from
``statsbomb_360`` in the bronze layer, computes per-defender defensive credits
using the DEFCON-lite analytics module, and writes results to a new
``defcon_results`` bronze table.

Design: "Read from gold + bronze, compute, write to bronze." Actions come from
the gold mart (SPADL 105x68m coordinates). Freeze frames come from bronze
(StatsBomb 360 data).
"""

from __future__ import annotations

import gc
import logging
from typing import TYPE_CHECKING

import pandas as pd

from analytics.defcon_lite import DefconLiteParams, compute_defcon_match
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "defcon_results"
_GOLD_SCHEMA = "dev_gold"


def _process_360_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: DefconLiteParams,
) -> int:
    """Process StatsBomb 360 matches.

    Returns number of rows written.
    """
    action_table = f"{catalog}.{_GOLD_SCHEMA}.fct_action_values"
    ff_table = f"{catalog}.bronze.statsbomb_360"

    try:
        match_ids_df = spark.table(ff_table).select("match_id").distinct().toPandas()
    except Exception:
        logger.warning("Cannot read table %s", ff_table)
        return 0

    if match_ids_df.empty:
        logger.info("No matches in %s", ff_table)
        return 0

    match_ids = match_ids_df["match_id"].unique()
    logger.info("%d 360 matches to process", len(match_ids))
    total_written = 0

    for match_id in match_ids:
        try:
            actions = (
                spark.table(action_table)
                .filter(f"match_id = '{match_id}'")
                .filter("original_event_id IS NOT NULL AND original_event_id != 'None'")
                .select(
                    "original_event_id",
                    "match_id",
                    "competition_id",
                    "season_id",
                    "player_id",
                    "team_id",
                    "action_type",
                    "start_x",
                    "start_y",
                    "offensive_value",
                )
                .toPandas()
            )
        except Exception:
            logger.warning("Cannot read actions for match %s — skipping", match_id)
            continue

        if actions.empty:
            continue

        actions = actions.rename(columns={"original_event_id": "event_id"})

        try:
            ff = (
                spark.table(ff_table)
                .filter(f"match_id = '{match_id}'")
                .selectExpr(
                    "id as event_id",
                    "teammate",
                    "from_json(location, 'ARRAY<DOUBLE>') as loc",
                )
                .selectExpr(
                    "event_id",
                    "teammate",
                    "loc[0] * 105.0 / 120.0 as x",
                    "loc[1] * 68.0 / 80.0 as y",
                )
                .toPandas()
            )
        except Exception:
            logger.warning("Cannot read freeze frames for match %s — skipping", match_id)
            continue

        if ff.empty:
            continue

        # 360 freeze frames are anonymous — generate synthetic IDs per row
        ff["player_id"] = range(len(ff))
        ff["team_id"] = 0
        ff["velocity_x"] = 0.0
        ff["velocity_y"] = 0.0

        logger.info(
            "Processing 360 match %s: %d actions, %d freeze frame rows",
            match_id,
            len(actions),
            len(ff),
        )

        try:
            result = compute_defcon_match(actions, ff, params, data_source="statsbomb_360")
        except Exception:
            logger.exception("Error computing DEFCON-lite for match %s", match_id)
            continue

        if result.empty:
            logger.info("Match %s: no DEFCON-lite credits", match_id)
            continue

        result["_ingested_at"] = pd.Timestamp.utcnow()

        sdf = spark.createDataFrame(result)
        written = write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"data_source = 'statsbomb_360' AND match_id = '{match_id}'",
            logger=logger,
        )
        total_written += written
        logger.info("Match %s: %d DEFCON-lite rows written", match_id, written)

        del actions, ff, result
        gc.collect()

    return total_written


def _process_tracking_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: DefconLiteParams,
) -> int:
    """Process Metrica tracking matches (3 matches).

    For tracking data, we build pseudo-freeze-frames by sampling the nearest
    frame to each action's time window. This is an approximation — full
    temporal alignment would require event-tracking timestamp matching.

    Returns number of rows written.
    """
    action_table = f"{catalog}.{_GOLD_SCHEMA}.fct_action_values"
    tracking_table = f"{catalog}.{_GOLD_SCHEMA}.fct_tracking_frames"

    try:
        match_ids_df = (
            spark.table(tracking_table).filter("source_provider = 'metrica'").select("match_id").distinct().toPandas()
        )
    except Exception:
        logger.warning("Cannot read table %s", tracking_table)
        return 0

    if match_ids_df.empty:
        return 0

    match_ids = match_ids_df["match_id"].unique()
    logger.info("%d tracking matches to process", len(match_ids))
    total_written = 0

    for match_id in match_ids:
        try:
            actions = (
                spark.table(action_table)
                .filter(f"match_id = '{match_id}'")
                .select(
                    "original_event_id",
                    "match_id",
                    "competition_id",
                    "season_id",
                    "player_id",
                    "team_id",
                    "action_type",
                    "start_x",
                    "start_y",
                    "offensive_value",
                )
                .toPandas()
            )
        except Exception:
            logger.warning("Cannot read actions for tracking match %s", match_id)
            continue

        if actions.empty:
            continue

        actions = actions.rename(columns={"original_event_id": "event_id"})

        try:
            tracking = (
                spark.table(tracking_table)
                .filter(f"match_id = '{match_id}'")
                .select(
                    "match_id",
                    "player_id",
                    "team",
                    "x",
                    "y",
                    "velocity_x",
                    "velocity_y",
                    "frame",
                    "period",
                )
                .toPandas()
            )
        except Exception:
            logger.warning("Cannot read tracking for match %s", match_id)
            continue

        if tracking.empty:
            continue

        # Build pseudo-freeze-frames: for each action, use first frame as
        # representative snapshot. A proper implementation would match action
        # timestamps to tracking frames.
        unique_frames = tracking[["period", "frame"]].drop_duplicates().sort_values(["period", "frame"])
        if unique_frames.empty:
            continue

        first_period = int(unique_frames.iloc[0]["period"])
        first_frame = int(unique_frames.iloc[0]["frame"])
        snapshot = tracking[(tracking["period"] == first_period) & (tracking["frame"] == first_frame)]

        ff_rows: list[dict[str, object]] = []
        for _, act in actions.iterrows():
            for _, player in snapshot.iterrows():
                ff_rows.append(
                    {
                        "event_id": act["event_id"],
                        "player_id": player["player_id"],
                        "team_id": 0,
                        "teammate": str(player["team"]) == "home",
                        "x": float(player["x"]),
                        "y": float(player["y"]),
                        "velocity_x": float(player.get("velocity_x", 0.0) or 0.0),
                        "velocity_y": float(player.get("velocity_y", 0.0) or 0.0),
                    }
                )

        if not ff_rows:
            continue

        ff = pd.DataFrame(ff_rows)

        logger.info("Processing tracking match %s: %d actions", match_id, len(actions))

        try:
            result = compute_defcon_match(actions, ff, params, data_source="metrica_tracking")
        except Exception:
            logger.exception("Error computing DEFCON-lite for tracking match %s", match_id)
            continue

        if result.empty:
            continue

        result["_ingested_at"] = pd.Timestamp.utcnow()

        sdf = spark.createDataFrame(result)
        written = write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"data_source = 'metrica_tracking' AND match_id = '{match_id}'",
            logger=logger,
        )
        total_written += written

        del actions, tracking, ff, result
        gc.collect()

    return total_written


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Execute the DEFCON-lite computation pipeline."""
    params = DefconLiteParams()

    total_360 = _process_360_matches(spark, catalog, schema, logger, params)
    logger.info("360 processing complete: %d rows", total_360)

    total_tracking = _process_tracking_matches(spark, catalog, schema, logger, params)
    logger.info("Tracking processing complete: %d rows", total_tracking)

    logger.info("DEFCON-lite pipeline complete — %d total rows written", total_360 + total_tracking)


def main() -> None:
    """CLI entry point for DEFCON-lite computation."""
    args = parse_ingestion_args("Compute DEFCON-lite defensive valuations")
    logger = configure_logging("defcon_lite")
    spark = get_spark_session()

    logger.info("Starting DEFCON-lite pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
