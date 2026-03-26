"""Formation detection batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer, detects
team formations using the EFPI algorithm (elastic template matching via the
Hungarian method), and writes formation labels to a ``formation_labels`` bronze
table.

Design: "Read from gold, compute, write to bronze." The gold mart provides
the standardised schema (x, y, timestamp_seconds, team, player_id, etc.)

Architecture: Uses ``applyInPandas`` grouped by ``(match_id, period, team)``
to distribute formation detection across Spark executors.  Each group is one
team in one half (~7K rows), keeping executor memory well under the 1 GB
serverless limit.  Formation templates are pre-built on the driver and
serialized into the UDF closure (no mplsoccer import on executors).

Reference: Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team strategy
in professional football."
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

_TABLE_NAME = "formation_labels"
_GOLD_SCHEMA = "dev_gold"

_RESULT_COLUMNS = [
    "match_id",
    "period",
    "team",
    "window_start_s",
    "window_end_s",
    "formation_label",
    "cost",
]


# ---------------------------------------------------------------------------
# applyInPandas UDF closure
# ---------------------------------------------------------------------------


def _make_formation_udf(
    window_seconds: int,
    min_outfield_players: int,
    serialized_templates: dict[int, dict[str, dict[str, object]]],
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for formation detection.

    Scalar params and pre-serialized templates are captured by the closure so
    they are serialised with the UDF and available on executors without network
    access.  The UDF does NOT import ``mplsoccer`` — templates are
    reconstructed from plain dicts + numpy arrays on the executor.

    Parameters
    ----------
    window_seconds : Time window length in seconds.
    min_outfield_players : Minimum outfield players for detection.
    serialized_templates : Output of ``templates_to_serializable()`` — plain
        dicts with numpy arrays and string lists (pickle-safe).

    Returns
    -------
    A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
    ``applyInPandas`` grouped by ``("match_id", "period", "team")``.
    """
    _window_seconds = window_seconds
    _min_outfield = min_outfield_players
    _ser_templates = serialized_templates

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Detect formations for one (match_id, period, team) group."""
        import pandas as _pd

        from analytics.formation_detection import (
            FormationParams,
            process_group_formations,
            templates_from_serializable,
        )

        _empty = _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        if pdf.empty:
            return _empty

        match_id = str(pdf["match_id"].iloc[0])
        period = int(pdf["period"].iloc[0])
        team = str(pdf["team"].iloc[0])

        params = FormationParams(
            window_seconds=_window_seconds,
            min_outfield_players=_min_outfield,
        )

        # Reconstruct templates from serialized data (no mplsoccer import)
        templates = templates_from_serializable(_ser_templates)

        # Filter to outfield players:
        # - player_id must be non-null (excludes ball rows)
        # - team must be non-null (excludes unassigned rows)
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna() & pdf["team"].notna()])
        if pdf.empty:
            return _empty

        result = process_group_formations(pdf, match_id, period, team, templates, params)

        return _pd.DataFrame(result[_RESULT_COLUMNS]) if len(result) > 0 else _empty

    return _udf


# ---------------------------------------------------------------------------
# Pipeline processing
# ---------------------------------------------------------------------------


def _process_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> int:
    """Process all new matches from fct_tracking_frames via applyInPandas.

    Returns number of rows written.
    """
    from analytics.formation_detection import (
        FormationParams,
        build_formation_templates,
        templates_to_serializable,
    )

    gold_table = f"{catalog}.{_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # Get all distinct match_ids from tracking data
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

    # --- pyspark imports deferred past early-exit guards ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    params = FormationParams()

    # --- Build templates on the DRIVER (imports mplsoccer here, not on executors) ---
    driver_templates = build_formation_templates()
    serialized_templates = templates_to_serializable(driver_templates)
    logger.info("Formation templates serialized for UDF closure (%d player counts)", len(serialized_templates))

    # Build filter predicate for all new matches at once
    new_ids_str = [str(mid) for mid in new_match_ids]

    tracking_df = (
        spark.table(gold_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            "match_id",
            "period",
            "team",
            "player_id",
            "timestamp_seconds",
            "x",
            "y",
        )
    )

    # Build UDF closure with pre-serialized templates (no mplsoccer on executors)
    udf_fn = _make_formation_udf(
        window_seconds=params.window_seconds,
        min_outfield_players=params.min_outfield_players,
        serialized_templates=serialized_templates,
    )

    output_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=False),
            StructField("period", IntegerType(), nullable=False),
            StructField("team", StringType(), nullable=False),
            StructField("window_start_s", DoubleType(), nullable=False),
            StructField("window_end_s", DoubleType(), nullable=False),
            StructField("formation_label", StringType(), nullable=False),
            StructField("cost", DoubleType(), nullable=False),
        ]
    )

    # Group by (match_id, period, team) — each group is ~7K rows (one team, one half)
    # instead of ~150K-300K rows per match_id. Keeps executor memory well under 1 GB.
    result_df = tracking_df.groupBy("match_id", "period", "team").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # Write results with replaceWhere for idempotent incremental writes
    ids_sql = ", ".join(f"'{mid}'" for mid in new_ids_str)
    written = write_delta_table(
        result_df,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("Formation detection complete: %d rows written", written)
    return written


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-formations", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx: object = None,
) -> None:
    """Execute the formation detection pipeline."""
    total = _process_matches(spark, catalog, schema, logger)
    logger.info("Formation detection pipeline complete -- %d total rows written", total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for formation detection."""
    args = parse_ingestion_args("Detect team formations from tracking data")
    logger = configure_logging("formations")
    spark = get_spark_session()

    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook

    register_hook(CostEstimateHook(spark, args.catalog, args.schema))

    logger.info("Starting formation detection pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
