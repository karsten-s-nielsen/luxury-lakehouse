"""Formation detection batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer, detects
team formations using two algorithms:

1. **EFPI** — elastic template matching via the Hungarian method (Shaw &
   Glickman 2019).  Templates are pre-built on the driver and serialised into
   the UDF closure (no mplsoccer import on executors).
2. **Shape graph** — Delaunay-based stable subgraph with face-center position
   decomposition (Sotudeh 2026).  Produces both formation labels and per-window
   player position assignments.

Results are written to two bronze tables:

* ``formation_labels`` — window-level formation labels from both detectors
  (distinguished by the ``detector`` column).
* ``player_positions`` — per-window player position labels from the shape graph
  detector, one row per player per window midpoint frame.

Design: "Read from gold, compute, write to bronze." The gold mart provides
the standardised schema (x, y, timestamp_seconds, team, player_id, etc.)

Architecture: Uses ``applyInPandas`` grouped by ``(match_id, period, team)``
to distribute formation detection across Spark executors.  Each group is one
team in one half (~7K rows), keeping executor memory well under the 1 GB
serverless limit.

References:
  Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team strategy
  in professional football."
  Sotudeh, H. (2026). "Identification of Team Tactical Formations and Player
  Positions in Association Football." PhD thesis, ETH Zurich.
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
_POSITIONS_TABLE_NAME = "player_positions"
_GOLD_SCHEMA = "dev_gold"

_RESULT_COLUMNS = [
    "match_id",
    "period",
    "team",
    "window_start_s",
    "window_end_s",
    "formation_label",
    "cost",
    "detector",
]

_POSITION_COLUMNS = [
    "match_id",
    "frame_id",
    "player_id",
    "team",
    "position_label",
    "vertical_level",
    "horizontal_level",
    "detector",
]

# Vertical level ordering for formation label derivation (back → front).
# Levels with zero players are skipped.
_VERTICAL_LEVEL_ORDER: tuple[str, ...] = ("B", "DM", "M", "AM", "F")


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
        # - is_goalkeeper must be False (templates only cover 8-10 outfield players)
        # fillna(False) handles NULL values; the column-presence guard handles
        # DataFrames that pre-date the is_goalkeeper column (e.g. unit tests).
        if "is_goalkeeper" not in pdf.columns:
            pdf["is_goalkeeper"] = False
        gk_flag: pd.Series = pdf["is_goalkeeper"].fillna(False)  # type: ignore[assignment]
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna() & pdf["team"].notna() & ~gk_flag])
        if pdf.empty:
            return _empty

        result = process_group_formations(pdf, match_id, period, team, templates, params)

        if len(result) > 0:
            result = result.copy()
            result["detector"] = "efpi"
            return _pd.DataFrame(result[_RESULT_COLUMNS])
        return _empty

    return _udf


# ---------------------------------------------------------------------------
# Shape graph UDF closure
# ---------------------------------------------------------------------------


def _attacking_direction(team: str, period: int) -> float:
    """Return attacking direction for *team* in *period*.

    Convention (StatsBomb / Metrica coordinate system):
    * Home attacks left-to-right (+1.0) in period 1, right-to-left (-1.0) in period 2.
    * Away is the opposite.
    """
    home_p1: float = 1.0
    if team == "away":
        home_p1 = -home_p1
    if period == 2:
        home_p1 = -home_p1
    return home_p1


def _derive_formation_label(vertical_levels: list[str]) -> str:
    """Derive a formation label string from vertical level assignments.

    Counts players per vertical level, orders by the standard back-to-front
    sequence (B → DM → M → AM → F), skips levels with zero players, and
    joins with hyphens.  E.g. {B:4, M:4, F:2} → ``"4-4-2"``.
    """
    from collections import Counter

    counts = Counter(vertical_levels)
    parts = [str(counts[lv]) for lv in _VERTICAL_LEVEL_ORDER if counts.get(lv, 0) > 0]
    return "-".join(parts) if parts else "unknown"


def _make_shape_graph_udf(
    window_seconds: int = 300,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for shape graph detection.

    Returns a tuple-style output: a *formation rows* DataFrame and a *position
    rows* DataFrame concatenated row-wise with a ``_row_type`` discriminator
    column.  The caller splits them back apart after ``applyInPandas``.

    We use a single UDF (rather than two separate passes) so that each
    (match_id, period, team) group is only materialised once on an executor.

    The combined schema is the *union* of formation columns and position columns
    (missing columns filled with NULL).  ``_row_type`` is ``"formation"`` or
    ``"position"``.
    """
    _window_seconds = window_seconds

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Detect formations + positions for one (match_id, period, team) group."""
        import numpy as np
        import pandas as _pd

        from analytics.shape_graph import compute_shape_graph, infer_positions

        # Combined schema columns (union of formation + position columns + _row_type)
        _combined_columns = [
            "match_id",
            "period",
            "team",
            "window_start_s",
            "window_end_s",
            "formation_label",
            "cost",
            "detector",
            "frame_id",
            "player_id",
            "position_label",
            "vertical_level",
            "horizontal_level",
            "_row_type",
        ]
        _empty = _pd.DataFrame(columns=_pd.Index(_combined_columns))

        if pdf.empty:
            return _empty

        match_id = str(pdf["match_id"].iloc[0])
        period = int(pdf["period"].iloc[0])
        team = str(pdf["team"].iloc[0])
        direction = _attacking_direction(team, period)

        # Filter to outfield players (same guards as EFPI UDF)
        if "is_goalkeeper" not in pdf.columns:
            pdf["is_goalkeeper"] = False
        gk_flag: _pd.Series = pdf["is_goalkeeper"].fillna(False)  # type: ignore[assignment]
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna() & pdf["team"].notna() & ~gk_flag])
        if pdf.empty:
            return _empty

        ts = pdf["timestamp_seconds"].values.astype(np.float64)
        ts_min = float(ts.min())
        ts_max = float(ts.max())

        formation_rows: list[dict[str, object]] = []
        position_rows: list[dict[str, object]] = []

        # Build frame column lookup (may be absent in unit tests)
        has_frame = "frame" in pdf.columns

        window_start = ts_min
        while window_start < ts_max:
            window_end = window_start + _window_seconds

            mask = (ts >= window_start) & (ts < window_end)
            window_df = pdf[mask]
            if len(window_df) == 0:
                window_start = window_end
                continue

            # Compute mean position per player within the window
            player_means = window_df.groupby("player_id")[["x", "y"]].mean()
            if len(player_means) < 3:
                window_start = window_end
                continue

            player_ids = list(player_means.index)
            positions = player_means[["x", "y"]].values.astype(np.float64)

            sg = compute_shape_graph(positions)
            pos_labels = infer_positions(sg, positions, direction)

            if not pos_labels:
                window_start = window_end
                continue

            # Derive formation label from vertical level counts
            verticals = [pl.vertical for pl in pos_labels]
            formation_label = _derive_formation_label(verticals)

            # Formation row (cost is NaN — shape graph has no template matching cost)
            formation_rows.append(
                {
                    "match_id": match_id,
                    "period": period,
                    "team": team,
                    "window_start_s": window_start,
                    "window_end_s": min(window_end, ts_max),
                    "formation_label": formation_label,
                    "cost": float("nan"),
                    "detector": "shape_graph",
                    "frame_id": None,
                    "player_id": None,
                    "position_label": None,
                    "vertical_level": None,
                    "horizontal_level": None,
                    "_row_type": "formation",
                }
            )

            # Position rows — one per player, frame_id = window midpoint frame.
            # If the tracking data has a 'frame' column, pick the frame closest
            # to the window midpoint timestamp.  Otherwise use a synthetic integer
            # derived from the midpoint timestamp.
            mid_ts = window_start + _window_seconds / 2.0
            if has_frame:
                frame_col = window_df["frame"].values
                ts_col = window_df["timestamp_seconds"].values.astype(np.float64)
                mid_idx = int(np.argmin(np.abs(ts_col - mid_ts)))
                mid_frame = int(frame_col[mid_idx])
            else:
                mid_frame = int(mid_ts)

            for pid, pl in zip(player_ids, pos_labels, strict=True):
                position_rows.append(
                    {
                        "match_id": match_id,
                        "frame_id": mid_frame,
                        "player_id": str(pid),
                        "team": team,
                        "position_label": pl.label,
                        "vertical_level": pl.vertical,
                        "horizontal_level": pl.horizontal,
                        "detector": "shape_graph",
                        "period": period,
                        "window_start_s": None,
                        "window_end_s": None,
                        "formation_label": None,
                        "cost": None,
                        "_row_type": "position",
                    }
                )

            window_start = window_end

        all_rows = formation_rows + position_rows
        if not all_rows:
            return _empty
        return _pd.DataFrame(all_rows, columns=_pd.Index(_combined_columns))

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

    Runs both the EFPI and shape graph detectors on every new match.  EFPI
    results and shape graph formation results are written to ``formation_labels``
    (distinguished by the ``detector`` column).  Shape graph position results
    are written to ``player_positions``.

    Returns total number of rows written across both tables.
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

    # Incremental skip guard — only skip matches that have results from BOTH
    # detectors.  A match with only EFPI results (pre-Cycle 2) still needs
    # shape graph processing.
    fully_processed: set[str] = set()
    try:
        from pyspark.sql import functions as _F  # noqa: N812

        detector_counts = (
            spark.table(results_table)
            .groupBy("match_id")
            .agg(_F.countDistinct("detector").alias("n_detectors"))
            .filter(_F.col("n_detectors") >= 2)
            .select("match_id")
            .collect()
        )
        fully_processed = {str(row["match_id"]) for row in detector_counts}
    except Exception:
        logger.info("No existing %s table -- processing all matches", results_table)

    new_match_ids = [mid for mid in all_match_ids if str(mid) not in fully_processed]
    logger.info(
        "%d matches total, %d fully processed (both detectors), %d to process",
        len(all_match_ids),
        len(fully_processed),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # --- pyspark imports deferred past early-exit guards ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

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
            "is_goalkeeper",
            "frame",
        )
    )

    total_written = 0

    # ── Pass 1: EFPI detector ──────────────────────────────────────────────
    efpi_udf_fn = _make_formation_udf(
        window_seconds=params.window_seconds,
        min_outfield_players=params.min_outfield_players,
        serialized_templates=serialized_templates,
    )

    efpi_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=False),
            StructField("period", IntegerType(), nullable=False),
            StructField("team", StringType(), nullable=False),
            StructField("window_start_s", DoubleType(), nullable=False),
            StructField("window_end_s", DoubleType(), nullable=False),
            StructField("formation_label", StringType(), nullable=False),
            StructField("cost", DoubleType(), nullable=True),
            StructField("detector", StringType(), nullable=False),
        ]
    )

    efpi_df = tracking_df.groupBy("match_id", "period", "team").applyInPandas(
        efpi_udf_fn,  # type: ignore[arg-type]
        schema=efpi_schema,
    )

    # ── Pass 2: Shape graph detector ───────────────────────────────────────
    sg_udf_fn = _make_shape_graph_udf(window_seconds=params.window_seconds)

    # Combined output schema (union of formation + position columns + _row_type).
    # The UDF returns both formation and position rows in a single DataFrame.
    sg_combined_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=False),
            StructField("period", IntegerType(), nullable=True),
            StructField("team", StringType(), nullable=False),
            StructField("window_start_s", DoubleType(), nullable=True),
            StructField("window_end_s", DoubleType(), nullable=True),
            StructField("formation_label", StringType(), nullable=True),
            StructField("cost", DoubleType(), nullable=True),
            StructField("detector", StringType(), nullable=False),
            StructField("frame_id", LongType(), nullable=True),
            StructField("player_id", StringType(), nullable=True),
            StructField("position_label", StringType(), nullable=True),
            StructField("vertical_level", StringType(), nullable=True),
            StructField("horizontal_level", StringType(), nullable=True),
            StructField("_row_type", StringType(), nullable=False),
        ]
    )

    sg_combined_df = tracking_df.groupBy("match_id", "period", "team").applyInPandas(
        sg_udf_fn,  # type: ignore[arg-type]
        schema=sg_combined_schema,
    )

    # Split shape graph results into formation rows and position rows
    sg_formation_df = sg_combined_df.filter(F.col("_row_type") == "formation").select(
        "match_id", "period", "team", "window_start_s", "window_end_s", "formation_label", "cost", "detector"
    )
    sg_position_df = sg_combined_df.filter(F.col("_row_type") == "position").select(
        "match_id",
        "frame_id",
        "player_id",
        "team",
        "position_label",
        "vertical_level",
        "horizontal_level",
        "detector",
    )

    # ── Write formation_labels (both detectors) ───────────────────────────
    combined_formations = efpi_df.unionByName(sg_formation_df)

    ids_sql = ", ".join(f"'{mid}'" for mid in new_ids_str)
    written_formations = write_delta_table(
        combined_formations,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )
    total_written += written_formations
    logger.info("Formation labels written: %d rows (EFPI + shape graph)", written_formations)

    # ── Write player_positions (shape graph only) ─────────────────────────
    written_positions = write_delta_table(
        sg_position_df,
        catalog,
        schema,
        _POSITIONS_TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )
    total_written += written_positions
    logger.info("Player positions written: %d rows (shape graph)", written_positions)

    return total_written


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
