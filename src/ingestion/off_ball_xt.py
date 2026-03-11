"""Off-Ball xT batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer (which
already has standardised column names, velocity, and speed), computes
per-player Off-Ball xT using pitch control x expected threat, and writes
results to a new ``off_ball_xt_results`` bronze table.

Design: "Read from gold, compute, write to bronze." The gold mart provides
the standardised schema (x, y, velocity_x, velocity_y, etc.) that raw bronze
tables lack.  The xT grid is loaded from the dbt seed table at runtime.

Architecture: Uses ``applyInPandas`` to distribute frame-batch computation
across Spark executors instead of a sequential per-match driver loop.
Pass 1 computes per-player xT per frame-batch on executors; Pass 2
aggregates across batches via Spark-native ``groupBy.agg``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from analytics.off_ball_xt import OffBallXtParams
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

# Default number of source frames per batch group.  Each batch is processed
# as a single ``applyInPandas`` partition on an executor.  A value of 500
# at 25 fps ≈ 20 seconds of play — large enough to amortise per-group
# overhead, small enough to stay within the 1 GB executor memory budget.
_DEFAULT_BATCH_SIZE = 500


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
        # On Databricks serverless, try UC Volume first, then workspace path
        volume_path = "/Volumes/soccer_analytics/bronze/libs/expected_threat_grid.csv"
        workspace_path = "/Workspace/dbt_project/seeds/expected_threat_grid.csv"
        try:
            df = pd.read_csv(volume_path)
        except FileNotFoundError:
            df = pd.read_csv(workspace_path)

    grid = np.zeros((12, 8), dtype=np.float64)
    grid[df["zone_x"].astype(int).values, df["zone_y"].astype(int).values] = df["xt_value"].values
    return grid


def _load_xt_grid_from_spark(spark: SparkSession, catalog: str) -> np.ndarray:
    """Load xT grid from the dbt seed table (preferred on Databricks).

    Falls back to CSV if the seed table doesn't exist.
    """
    try:
        seed_table = f"{catalog}.dev_silver.expected_threat_grid"
        df = spark.table(seed_table).toPandas()
        grid = np.zeros((12, 8), dtype=np.float64)
        grid[df["zone_x"].astype(int).values, df["zone_y"].astype(int).values] = df["xt_value"].values
        return grid
    except Exception:
        return _load_xt_grid()


def _make_batch_udf(
    xt_grid: np.ndarray,
    sample_fps: float,
    pc_grid_cells_x: int,
    pc_grid_cells_y: int,
) -> object:
    """Build the ``applyInPandas`` UDF closure.

    The xT grid (96 floats) and scalar params are captured by the closure
    so they are serialised with the UDF and available on executors without
    network access.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    # Serialise grid as list-of-lists so pickle has no ndarray dependency issues
    grid_data: list[list[float]] = xt_grid.tolist()

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Compute per-player off-ball xT for one (match_id, frame_batch_id) group."""
        # Lazy imports — executors have the wheel installed but no internet
        import numpy as _np
        import pandas as _pd

        from analytics.off_ball_xt import compute_off_ball_xt_frame
        from analytics.pitch_control import PitchControlParams as _PCParams

        grid = _np.array(grid_data, dtype=_np.float64)
        pc_params = _PCParams(grid_cells_x=pc_grid_cells_x, grid_cells_y=pc_grid_cells_y)

        _empty = _pd.DataFrame(columns=_pd.Index(["match_id", "player_id", "off_ball_xt_sum", "frame_count"]))

        if pdf.empty:
            return _empty

        match_id = str(pdf["match_id"].iloc[0])

        # Filter out ball rows (player_id is null for ball)
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna()])
        if pdf.empty:
            return _empty

        # Sample frames at desired fps
        frame_rate = int(pdf["frame_rate"].iloc[0]) if "frame_rate" in pdf.columns else 25
        sample_every = max(1, int(frame_rate / sample_fps))

        # Get unique (period, frame) combinations, then sample
        period_frames = _pd.DataFrame(pdf[["period", "frame"]].drop_duplicates()).sort_values(by=["period", "frame"])
        sampled_pf = period_frames.iloc[::sample_every]

        all_frame_results: list[_pd.DataFrame] = []

        for _, pf_row in sampled_pf.iterrows():
            period = pf_row["period"]
            frame = pf_row["frame"]
            frame_df = _pd.DataFrame(pdf[(pdf["period"] == period) & (pdf["frame"] == frame)])

            if frame_df.empty:
                continue

            # Only process frames with players from both teams
            teams_present = list(frame_df["team"].unique())
            if len(teams_present) < 2:
                continue

            frame_results = compute_off_ball_xt_frame(frame_df, grid, pc_params)
            all_frame_results.append(frame_results)

        if not all_frame_results:
            return _empty

        combined = _pd.concat(all_frame_results, ignore_index=True)
        combined = combined[combined["off_ball_xt"].notna()]
        combined["player_id"] = combined["player_id"].astype(str)

        # Per-player aggregation within this batch
        agg = combined.groupby("player_id")["off_ball_xt"].agg(["sum", "count"]).reset_index()
        agg.columns = _pd.Index(["player_id", "off_ball_xt_sum", "frame_count"])
        agg["match_id"] = match_id

        return _pd.DataFrame(agg[["match_id", "player_id", "off_ball_xt_sum", "frame_count"]])

    return _udf


def _process_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt_grid: np.ndarray,
    params: OffBallXtParams,
    pc_params: PitchControlParams,
) -> int:
    """Process all matches from fct_tracking_frames via applyInPandas.

    Pass 1: ``groupBy(match_id, frame_batch_id).applyInPandas`` computes
    per-player xT per batch on executors.
    Pass 2: Spark-native ``groupBy(match_id, player_id).agg`` aggregates
    across batches to produce the final per-player per-match output.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    gold_table = f"{catalog}.{_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    try:
        match_id_rows = spark.table(gold_table).select("match_id").distinct().collect()
    except Exception:
        logger.warning("Cannot read table %s", gold_table)
        return 0

    if not match_id_rows:
        logger.info("No matches in %s", gold_table)
        return 0

    all_match_ids = [row["match_id"] for row in match_id_rows]

    # Check which matches already have off-ball xT results (incremental)
    existing_ids: set[str] = set()
    try:
        existing_rows = spark.table(results_table).select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in all_match_ids if str(mid) not in existing_ids]
    logger.info(
        "%d matches total, %d already processed, %d to process",
        len(all_match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # Build filter predicate for all new matches at once
    new_ids_str = [str(mid) for mid in new_match_ids]
    tracking_df = (
        spark.table(gold_table)
        .filter(F.col("match_id").isin(new_ids_str))
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
            "frame_rate",
        )
    )

    # Add synthetic partition key: frame_batch_id groups frames into
    # batches of _DEFAULT_BATCH_SIZE for uniform executor distribution.
    tracking_df = tracking_df.withColumn(
        "frame_batch_id",
        (F.col("frame") / F.lit(_DEFAULT_BATCH_SIZE)).cast("int"),
    )

    # Build UDF closure with captured grid and params
    udf_fn = _make_batch_udf(
        xt_grid=xt_grid,
        sample_fps=params.sample_fps,
        pc_grid_cells_x=pc_params.grid_cells_x,
        pc_grid_cells_y=pc_params.grid_cells_y,
    )

    # Pass 1: per-batch computation on executors
    batch_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=False),
            StructField("player_id", StringType(), nullable=False),
            StructField("off_ball_xt_sum", DoubleType(), nullable=False),
            StructField("frame_count", IntegerType(), nullable=False),
        ]
    )

    batch_results = tracking_df.groupBy("match_id", "frame_batch_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=batch_schema,
    )

    # Pass 2: aggregate across batches per (match_id, player_id)
    final_df = (
        batch_results.groupBy("match_id", "player_id")
        .agg(
            F.sum("off_ball_xt_sum").alias("total_off_ball_xt"),
            F.sum("frame_count").alias("frames_sampled"),
        )
        .withColumn(
            "avg_off_ball_xt",
            F.when(F.col("frames_sampled") > 0, F.col("total_off_ball_xt") / F.col("frames_sampled")).otherwise(0.0),
        )
        .select("player_id", "match_id", "total_off_ball_xt", "avg_off_ball_xt", "frames_sampled")
    )

    # Write all results in one pass
    written = write_delta_table(
        final_df,
        catalog,
        schema,
        _TABLE_NAME,
        logger=logger,
    )

    logger.info("Off-Ball xT processing complete: %d rows written", written)
    return written


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
