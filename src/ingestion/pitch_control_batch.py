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

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "pitch_control_values"
# PR 7 (ADR-011 close-out): widened with data_source + match_key. Collapses
# the prefix-CASE bridge in stg_pitch_control__values introduced by PR 6
# (§4.7) to a passthrough — the writer now emits Kimball-conformed columns
# natively from the gold tracking source.
_RESULTS_SCHEMA = (
    "tracking_id STRING, match_id STRING, data_source STRING, match_key BIGINT, "
    "pitch_control_value DOUBLE, _ingested_at TIMESTAMP"
)

# Bronze contract — column names emitted by the writer. Single source of
# truth for test_pitch_control_bronze_coverage.py +
# test_bronze_live_schema.py (PR 6, ADR-011 first-class promotion of
# stg_pitch_control__values; PR 7 widening adds data_source + match_key).
_PITCH_CONTROL_BRONZE_COLS: tuple[str, ...] = (
    "tracking_id",
    "match_id",
    "data_source",
    "match_key",
    "pitch_control_value",
    "_ingested_at",
)
_guard_logger = logging.getLogger(f"{__name__}.guard")

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

        # PR 7 (ADR-011): writer emits data_source + match_key natively so
        # stg_pitch_control__values can collapse its prefix-CASE bridge to
        # a passthrough.
        _empty = _pd.DataFrame(
            columns=_pd.Index(["tracking_id", "match_id", "data_source", "match_key", "pitch_control_value"])
        )

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
                [frame_clean["x"].to_numpy(dtype=_np.float64), frame_clean["y"].to_numpy(dtype=_np.float64)]
            )

            # Compute pitch control at each player's position
            pc_values = _pc_at_points(frame_clean, target_points, pc_params)

            for tid, mid, ds, mk, pcv in zip(
                frame_clean["tracking_id"],
                frame_clean["match_id"],
                frame_clean["data_source"],
                frame_clean["match_key"],
                pc_values,
                strict=False,
            ):
                results.append(
                    {
                        "tracking_id": str(tid),
                        "match_id": str(mid),
                        "data_source": str(ds) if ds is not None else None,
                        "match_key": int(mk) if mk is not None else None,
                        "pitch_control_value": float(pcv),
                    }
                )

        if not results:
            return _empty

        return _pd.DataFrame(results)

    return _udf


# Maximum matches per fan-out chunk.  Each match produces ~200-400 MB
# of tracking data in the applyInPandas group — at 2 per chunk we stay
# safely under the 800 MB UDF executor memory budget.
_MATCHES_PER_CHUNK = 2


class _PitchControlGuard:
    """SkipGuard adapter for pitch control batch pipeline."""

    workflow_id = "wf-pitch-control"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which tracking matches need pitch control computation."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        new_match_ids = find_new_ids(
            spark,
            source_table=f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames",
            results_table=results_table,
        )

        if not new_match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        # Pre-compute fan-out chunks at _MATCHES_PER_CHUNK per chunk
        chunks = [new_match_ids[i : i + _MATCHES_PER_CHUNK] for i in range(0, len(new_match_ids), _MATCHES_PER_CHUNK)]

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            chunks=chunks if len(chunks) > 1 else None,
            metadata={"new_match_ids": new_match_ids},
        )


skip_guard = _PitchControlGuard()


def _process_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
) -> int:
    """Process all new matches from fct_tracking_frames via applyInPandas.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    from analytics.pitch_control import PitchControlParams

    gold_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames"

    new_ids_str = filter_result.metadata["new_match_ids"]
    logger.info("%d matches to process", len(new_ids_str))

    if not new_ids_str:
        return 0
    # PR 7 (ADR-011): pull data_source + match_key from fct_tracking_frames
    # (added in PR 7's tracking-subsystem migration). Eliminates the prefix-CASE
    # bridge in stg_pitch_control__values.
    tracking_df = (
        spark.table(gold_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            "tracking_id",
            "match_id",
            "data_source",
            "match_key",
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
            StructField("data_source", StringType(), nullable=True),
            StructField("match_key", LongType(), nullable=True),
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
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Execute the pitch control value computation pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    total = _process_matches(spark, catalog, schema, logger, filter_result=filter_result)
    logger.info("Pitch control pipeline complete -- %d total rows written", total)
    return total


def main() -> None:
    """CLI entry point for pitch control batch computation."""
    args = parse_ingestion_args("Compute pitch control values from tracking data")
    logger = configure_logging("pitch_control_batch")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting pitch control batch pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
