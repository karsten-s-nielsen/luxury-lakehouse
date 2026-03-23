"""Pitch control value batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer, computes
per-player pitch control at each player's position using the Spearman 2017
physics-based model, and writes results to a ``pitch_control_values`` bronze
table.

Design: "Read from gold, compute, write to bronze." The gold mart provides
the standardised schema (x, y, velocity_x, velocity_y, etc.) that raw bronze
tables lack.

Architecture: Uses ``applyInPandas`` to distribute frame-batch computation
across Spark executors instead of a sequential per-match driver loop.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "pitch_control_values"
_GOLD_SCHEMA = "dev_gold"

# Default number of source frames per batch group.  Each batch is processed
# as a single ``applyInPandas`` partition on an executor.  A value of 500
# at 25 fps ~ 20 seconds of play -- large enough to amortise per-group
# overhead, small enough to stay within the 1 GB executor memory budget.
_DEFAULT_BATCH_SIZE = 500


def _make_batch_udf(
    reaction_time: float,
    max_acceleration: float,
    sigma: float,
    pitch_length_m: float,
    pitch_width_m: float,
    sb_length: float,
    sb_width: float,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure.

    Scalar params are captured by the closure so they are serialised with
    the UDF and available on executors without network access.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    # Capture serialisable scalars (no dataclass -- pickle compatibility)
    _rt = reaction_time
    _ma = max_acceleration
    _sig = sigma
    _pl = pitch_length_m
    _pw = pitch_width_m
    _sbl = sb_length
    _sbw = sb_width

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Compute per-player pitch control for one (match_id, frame_batch_id) group."""
        import numpy as _np
        import pandas as _pd

        from analytics.pitch_control import PitchControlParams as _PCParams
        from analytics.pitch_control import compute_pitch_control_at_points as _pc_at_points

        _empty = _pd.DataFrame(columns=_pd.Index(["tracking_id", "match_id", "pitch_control_value"]))

        if pdf.empty:
            return _empty

        pc_params = _PCParams(
            reaction_time=_rt,
            max_acceleration=_ma,
            sigma=_sig,
            pitch_length_m=_pl,
            pitch_width_m=_pw,
            sb_length=_sbl,
            sb_width=_sbw,
        )

        # Filter out ball rows (player_id is null for ball)
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna()])
        if pdf.empty:
            return _empty

        results: list[dict[str, object]] = []

        for _key, frame_df in pdf.groupby(["period", "frame"]):
            # Need players from both teams for meaningful pitch control
            if frame_df["team"].nunique() < 2:
                continue

            # Handle NaN velocities -- replace with 0 (stationary player)
            frame_clean = _pd.DataFrame(frame_df).copy()
            frame_clean["velocity_x"] = _pd.Series(_pd.to_numeric(frame_clean["velocity_x"], errors="coerce")).fillna(
                0.0
            )
            frame_clean["velocity_y"] = _pd.Series(_pd.to_numeric(frame_clean["velocity_y"], errors="coerce")).fillna(
                0.0
            )

            # Target points are the player positions themselves
            target_points = _np.column_stack(
                [frame_clean["x"].values.astype(_np.float64), frame_clean["y"].values.astype(_np.float64)]
            )

            # Compute pitch control at each player's position
            pc_values = _pc_at_points(frame_clean, target_points, pc_params)

            for tid, mid, pcv in zip(
                frame_clean["tracking_id"],
                frame_clean["match_id"],
                pc_values,
                strict=False,
            ):
                results.append(
                    {
                        "tracking_id": str(tid),
                        "match_id": str(mid),
                        "pitch_control_value": float(pcv),
                    }
                )

        if not results:
            return _empty

        return _pd.DataFrame(results)

    return _udf


def _process_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> int:
    """Process all new matches from fct_tracking_frames via applyInPandas.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    from analytics.pitch_control import PitchControlParams

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

    # Incremental skip guard
    existing_ids: set[str] = set()
    try:
        existing_rows = spark.table(results_table).select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table -- processing all matches", results_table)

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
            "tracking_id",
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

    # Build UDF closure with captured scalar params
    params = PitchControlParams()
    udf_fn = _make_batch_udf(
        reaction_time=params.reaction_time,
        max_acceleration=params.max_acceleration,
        sigma=params.sigma,
        pitch_length_m=params.pitch_length_m,
        pitch_width_m=params.pitch_width_m,
        sb_length=params.sb_length,
        sb_width=params.sb_width,
    )

    output_schema = StructType(
        [
            StructField("tracking_id", StringType(), nullable=False),
            StructField("match_id", StringType(), nullable=False),
            StructField("pitch_control_value", DoubleType(), nullable=False),
        ]
    )

    result_df = tracking_df.groupBy("match_id", "frame_batch_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # Write results with replaceWhere for idempotent incremental writes.
    ids_sql = ", ".join(f"'{mid}'" for mid in new_ids_str)
    written = write_delta_table(
        result_df,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("Pitch control processing complete: %d rows written", written)
    return written


@workflow("wf-pitch-control", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx=None,
) -> None:
    """Execute the pitch control value computation pipeline."""
    total = _process_matches(spark, catalog, schema, logger)
    logger.info("Pitch control pipeline complete -- %d total rows written", total)


def main() -> None:
    """CLI entry point for pitch control batch computation."""
    args = parse_ingestion_args("Compute pitch control values from tracking data")
    logger = configure_logging("pitch_control_batch")
    spark = get_spark_session()

    logger.info("Starting pitch control batch pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
