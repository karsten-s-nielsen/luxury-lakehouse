"""Off-Ball xT batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer (which
already has standardised column names, velocity, and speed), computes
per-player Off-Ball xT using pitch control x expected threat, and writes
results to a new ``off_ball_xt_results`` bronze table.

Design: "Read from gold, compute, write to bronze." The gold mart provides
the standardised schema (x, y, velocity_x, velocity_y, etc.) that raw bronze
tables lack.  The xT grid is loaded from the dbt seed table at runtime.
"""

from __future__ import annotations

import gc
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from analytics.off_ball_xt import OffBallXtParams, compute_off_ball_xt_match
from analytics.pitch_control import PitchControlParams
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "off_ball_xt_results"
_GOLD_SCHEMA = "dev_gold"


def _load_xt_grid() -> np.ndarray:
    """Load the 12x8 xT grid from the seed CSV.

    Returns:
        numpy array of shape (12, 8) indexed by [zone_x, zone_y].
    """
    # Try local path relative to this module
    local_path = Path(__file__).resolve().parent.parent.parent / "dbt_project" / "seeds" / "expected_threat_grid.csv"
    try:
        df = pd.read_csv(str(local_path))
    except FileNotFoundError:
        # On Databricks, the seed file is in the workspace
        df = pd.read_csv("/Workspace/dbt_project/seeds/expected_threat_grid.csv")

    grid = np.zeros((12, 8), dtype=np.float64)
    for _, row in df.iterrows():
        grid[int(row["zone_x"]), int(row["zone_y"])] = float(row["xt_value"])
    return grid


def _load_xt_grid_from_spark(spark: SparkSession, catalog: str) -> np.ndarray:
    """Load xT grid from the dbt seed table (preferred on Databricks).

    Falls back to CSV if the seed table doesn't exist.
    """
    try:
        seed_table = f"{catalog}.dev_silver.expected_threat_grid"
        df = spark.table(seed_table).toPandas()
        grid = np.zeros((12, 8), dtype=np.float64)
        for _, row in df.iterrows():
            grid[int(row["zone_x"]), int(row["zone_y"])] = float(row["xt_value"])
        return grid
    except Exception:
        return _load_xt_grid()


def _process_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt_grid: np.ndarray,
    params: OffBallXtParams,
    pc_params: PitchControlParams,
) -> int:
    """Process all matches from fct_tracking_frames.

    Returns number of rows written.
    """
    gold_table = f"{catalog}.{_GOLD_SCHEMA}.fct_tracking_frames"

    try:
        match_ids_df = spark.table(gold_table).select("match_id").distinct().toPandas()
    except Exception:
        logger.warning("Cannot read table %s", gold_table)
        return 0

    if match_ids_df.empty:
        logger.info("No matches in %s", gold_table)
        return 0

    match_ids = match_ids_df["match_id"].unique()
    logger.info("%d matches to process from %s", len(match_ids), gold_table)
    total_written = 0

    for match_id in match_ids:
        try:
            match_df = (
                spark.table(gold_table)
                .filter(f"match_id = '{match_id}'")
                .select(
                    "match_id",
                    "player_id",
                    "team",
                    "source_provider",
                    "x",
                    "y",
                    "velocity_x",
                    "velocity_y",
                    "frame",
                    "period",
                    "frame_rate",
                )
                .toPandas()
            )
        except Exception:
            logger.warning("Cannot read tracking for match %s — skipping", match_id)
            continue

        if match_df.empty:
            continue

        # Filter out ball rows (player_id is null for ball)
        match_df = pd.DataFrame(match_df[match_df["player_id"].notna()])
        match_df["match_id"] = str(match_id)

        provider = match_df["source_provider"].iloc[0] if not match_df.empty else "unknown"

        logger.info(
            "Processing match %s (%s): %d player frames",
            match_id,
            provider,
            len(match_df),
        )

        try:
            result = compute_off_ball_xt_match(match_df, xt_grid, params, pc_params)
        except Exception:
            logger.exception("Error computing Off-Ball xT for match %s", match_id)
            continue

        if result.empty:
            logger.info("Match %s: no Off-Ball xT results", match_id)
            continue

        # Write to bronze
        sdf = spark.createDataFrame(result)
        written = write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total_written += written
        logger.info("Match %s: %d Off-Ball xT rows written", match_id, written)

        del match_df, result
        gc.collect()

    logger.info("Off-Ball xT processing complete: %d rows written", total_written)
    return total_written


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Execute the Off-Ball xT computation pipeline."""
    params = OffBallXtParams()
    pc_params = PitchControlParams()

    xt_grid = _load_xt_grid_from_spark(spark, catalog)
    logger.info("xT grid loaded: shape %s, max %.5f", xt_grid.shape, float(xt_grid.max()))

    total = _process_matches(spark, catalog, schema, logger, xt_grid, params, pc_params)
    logger.info("Off-Ball xT pipeline complete — %d total rows written", total)


def main() -> None:
    """CLI entry point for Off-Ball xT computation."""
    args = parse_ingestion_args("Compute Off-Ball xT from tracking data")
    logger = configure_logging("off_ball_xt")
    spark = get_spark_session()

    logger.info("Starting Off-Ball xT pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
