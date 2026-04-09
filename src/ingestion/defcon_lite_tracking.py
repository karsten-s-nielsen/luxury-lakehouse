"""DEFCON-lite Metrica tracking processing path.

Reads action values from ``fct_action_values`` and tracking frame data from
``fct_tracking_frames``, builds pseudo-freeze-frames from tracking snapshots,
and computes per-defender defensive credits via two-pass ``applyInPandas``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult
from ingestion.utils import write_delta_table
from shared.constants import DEFAULT_GOLD_SCHEMA

_TABLE_NAME = "defcon_results"
_RESULTS_SCHEMA = (
    "event_id STRING, match_id STRING, competition_id BIGINT, season_id BIGINT, "
    "defender_player_id BIGINT, defender_team_id BIGINT, defender_x DOUBLE, defender_y DOUBLE, "
    "action_player_id BIGINT, action_type STRING, action_x DOUBLE, action_y DOUBLE, "
    "credit_type STRING, confidence STRING, dist_to_ball DOUBLE, pitch_control_at_action DOUBLE, "
    "defcon_value FLOAT, data_source STRING, _ingested_at TIMESTAMP"
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.defcon_lite import DefconLiteParams

_guard_logger = logging.getLogger(f"{__name__}.guard")


class _DefconTrackingGuard:
    """SkipGuard adapter for DEFCON-lite tracking data path."""

    workflow_id = "wf-defcon-tracking"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which tracking matches need DEFCON computation."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)

        new_match_ids = find_new_ids(
            spark,
            source_table=f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames",
            results_table=f"{catalog}.{schema}.{_TABLE_NAME}",
            source_filter="source_provider = 'metrica'",
            results_filter="data_source = 'metrica_tracking'",
        )

        if not new_match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            metadata={"new_match_ids": new_match_ids},
        )


skip_guard = _DefconTrackingGuard()


def _make_credits_udf_tracking(
    disturb_radius_m: float,
    deter_cone_angle_deg: float,
    pitch_length: float,
    pitch_width: float,
) -> object:
    """Build the Pass 1 ``applyInPandas`` UDF closure for tracking data.

    The tracking path builds pseudo-freeze-frames from the first frame
    snapshot within each match, then assigns credits.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    _disturb_r = disturb_radius_m
    _deter_angle = deter_cone_angle_deg
    _pitch_l = pitch_length
    _pitch_w = pitch_width

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Assign defensive credits for one tracking match."""
        import pandas as _pd

        from analytics.defcon_lite import DefconLiteParams as _Params
        from analytics.defcon_lite import assign_credits_for_period as _assign

        _empty_cols = _pd.Index(
            [
                "event_id",
                "match_id",
                "competition_id",
                "season_id",
                "defender_player_id",
                "defender_team_id",
                "defender_x",
                "defender_y",
                "action_player_id",
                "action_type",
                "action_x",
                "action_y",
                "credit_type",
                "confidence",
                "dist_to_ball",
                "pitch_control_at_action",
                "offensive_value",
                "vaep_target",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_empty_cols)

        params = _Params(
            disturb_radius_m=_disturb_r,
            deter_cone_angle_deg=_deter_angle,
            pitch_length=_pitch_l,
            pitch_width=_pitch_w,
        )

        # Split joined columns back into actions and tracking data
        actions = _pd.DataFrame(
            {
                "event_id": pdf["act_event_id"],
                "match_id": pdf["act_match_id"],
                "competition_id": pdf["act_competition_id"],
                "season_id": pdf["act_season_id"],
                "player_id": pdf["act_player_id"],
                "team_id": pdf["act_team_id"],
                "action_type": pdf["act_action_type"],
                "start_x": pdf["act_start_x"],
                "start_y": pdf["act_start_y"],
                "offensive_value": pdf["act_offensive_value"],
            }
        ).drop_duplicates(subset=["event_id"])

        if actions.empty:
            return _pd.DataFrame(columns=_empty_cols)

        # Extract tracking snapshot columns
        tracking = _pd.DataFrame(
            {
                "match_id": pdf["act_match_id"],
                "player_id": pdf["trk_player_id"],
                "team": pdf["trk_team"],
                "x": pdf["trk_x"],
                "y": pdf["trk_y"],
                "velocity_x": pdf["trk_velocity_x"],
                "velocity_y": pdf["trk_velocity_y"],
                "frame": pdf["trk_frame"],
                "period": pdf["trk_period"],
            }
        ).drop_duplicates()

        if tracking.empty:
            return _pd.DataFrame(columns=_empty_cols)

        # Build pseudo-freeze-frames: use first frame as representative snapshot
        unique_frames = _pd.DataFrame(tracking[["period", "frame"]].drop_duplicates()).sort_values(
            by=["period", "frame"]
        )
        if unique_frames.empty:
            return _pd.DataFrame(columns=_empty_cols)

        first_period = int(unique_frames.iloc[0]["period"])
        first_frame = int(unique_frames.iloc[0]["frame"])
        snapshot = tracking[(tracking["period"] == first_period) & (tracking["frame"] == first_frame)]

        # Cross-join actions x snapshot players
        event_ids = list(actions["event_id"])
        snap_player_ids = list(snapshot["player_id"])
        snap_teams = [str(t) == "home" for t in snapshot["team"]]
        snap_xs = [float(v) for v in snapshot["x"]]
        snap_ys = [float(v) for v in snapshot["y"]]
        vx_col = snapshot["velocity_x"] if "velocity_x" in snapshot.columns else _pd.Series([0.0] * len(snapshot))
        vy_col = snapshot["velocity_y"] if "velocity_y" in snapshot.columns else _pd.Series([0.0] * len(snapshot))
        snap_vxs = [float(v or 0.0) for v in vx_col]
        snap_vys = [float(v or 0.0) for v in vy_col]

        ff_rows: list[dict[str, object]] = [
            {
                "event_id": eid,
                "player_id": pid,
                "team_id": 0,
                "teammate": teammate,
                "x": x,
                "y": y,
                "velocity_x": vx,
                "velocity_y": vy,
            }
            for eid in event_ids
            for pid, teammate, x, y, vx, vy in zip(
                snap_player_ids, snap_teams, snap_xs, snap_ys, snap_vxs, snap_vys, strict=True
            )
        ]

        if not ff_rows:
            return _pd.DataFrame(columns=_empty_cols)

        ff = _pd.DataFrame(ff_rows)
        result = _assign(actions, ff, params)
        if result.empty:
            return _pd.DataFrame(columns=_empty_cols)

        return _pd.DataFrame(result)

    return _udf


def process_tracking_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: DefconLiteParams,
    champion_model_bytes: bytes | None = None,
    *,
    filter_result: FilterResult | None = None,
) -> int:
    """Process Metrica tracking matches via two-pass applyInPandas.

    For tracking data, pseudo-freeze-frames are built inside the UDF
    by sampling the nearest frame to each action's time window.

    Pass 1: ``groupBy("act_match_id").applyInPandas`` assigns credits.
    Pass 2: ``groupBy("match_id").applyInPandas`` estimates values
    (using @Champion model if available).

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    action_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values"
    tracking_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # Use guard metadata if available, otherwise fall back to inline guard
    if filter_result and filter_result.metadata.get("new_match_ids"):
        new_ids_str = filter_result.metadata["new_match_ids"]
    else:
        from ingestion.guards import find_new_ids

        new_ids_str = find_new_ids(
            spark,
            source_table=tracking_table,
            results_table=results_table,
            source_filter="source_provider = 'metrica'",
            results_filter="data_source = 'metrica_tracking'",
        )
    logger.info("%d tracking matches to process", len(new_ids_str))

    if not new_ids_str:
        return 0

    # Early-out: check if fct_action_values has ANY matching entries.
    # Metrica tracking has no SPADL action values (SPADL only covers StatsBomb/Wyscout).
    # Cast to string avoids BIGINT mismatch when match_ids are strings like "Sample_Game_2".
    matching_action_count = (
        spark.table(action_table).filter(F.col("match_id").cast("string").isin(new_ids_str)).limit(1).count()
    )
    if matching_action_count == 0:
        logger.info("No matching action values for %d tracking matches — skipping DEFCON tracking", len(new_ids_str))
        return 0

    actions_df = (
        spark.table(action_table)
        .filter(F.col("match_id").cast("string").isin(new_ids_str))
        .select(
            F.col("original_event_id").alias("act_event_id"),
            F.col("match_id").cast("string").alias("act_match_id"),
            F.col("competition_id").alias("act_competition_id"),
            F.col("season_id").alias("act_season_id"),
            F.col("player_id").alias("act_player_id"),
            F.col("team_id").alias("act_team_id"),
            F.col("action_type").alias("act_action_type"),
            F.col("start_x").alias("act_start_x"),
            F.col("start_y").alias("act_start_y"),
            F.col("offensive_value").alias("act_offensive_value"),
        )
    )

    tracking_df = (
        spark.table(tracking_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("match_id").alias("trk_match_id"),
            F.col("player_id").alias("trk_player_id"),
            F.col("team").alias("trk_team"),
            F.col("x").alias("trk_x"),
            F.col("y").alias("trk_y"),
            F.col("velocity_x").alias("trk_velocity_x"),
            F.col("velocity_y").alias("trk_velocity_y"),
            F.col("frame").alias("trk_frame"),
            F.col("period").alias("trk_period"),
        )
    )

    # Join actions x tracking on match_id (cross-join within each match).
    # The UDF will build pseudo-freeze-frames from the tracking snapshot.
    joined = actions_df.join(tracking_df, actions_df["act_match_id"] == tracking_df["trk_match_id"], "inner").drop(
        "trk_match_id"
    )

    # Pass 1: assign credits per match on executors
    credits_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("competition_id", LongType(), nullable=True),
            StructField("season_id", LongType(), nullable=True),
            StructField("defender_player_id", LongType(), nullable=True),
            StructField("defender_team_id", LongType(), nullable=True),
            StructField("defender_x", DoubleType(), nullable=True),
            StructField("defender_y", DoubleType(), nullable=True),
            StructField("action_player_id", StringType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("action_x", DoubleType(), nullable=True),
            StructField("action_y", DoubleType(), nullable=True),
            StructField("credit_type", StringType(), nullable=True),
            StructField("confidence", StringType(), nullable=True),
            StructField("dist_to_ball", DoubleType(), nullable=True),
            StructField("pitch_control_at_action", DoubleType(), nullable=True),
            StructField("offensive_value", DoubleType(), nullable=True),
            StructField("vaep_target", DoubleType(), nullable=True),
        ]
    )

    credits_udf = _make_credits_udf_tracking(
        disturb_radius_m=params.disturb_radius_m,
        deter_cone_angle_deg=params.deter_cone_angle_deg,
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
    )

    credits_sdf = joined.groupBy("act_match_id").applyInPandas(
        credits_udf,  # type: ignore[arg-type]
        schema=credits_schema,
    )

    # Pass 2: estimate DEFCON values per match on executors
    valued_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("competition_id", LongType(), nullable=True),
            StructField("season_id", LongType(), nullable=True),
            StructField("defender_player_id", LongType(), nullable=True),
            StructField("defender_team_id", LongType(), nullable=True),
            StructField("defender_x", DoubleType(), nullable=True),
            StructField("defender_y", DoubleType(), nullable=True),
            StructField("action_player_id", StringType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("action_x", DoubleType(), nullable=True),
            StructField("action_y", DoubleType(), nullable=True),
            StructField("credit_type", StringType(), nullable=True),
            StructField("confidence", StringType(), nullable=True),
            StructField("defcon_value", DoubleType(), nullable=True),
            StructField("dist_to_ball", DoubleType(), nullable=True),
            StructField("pitch_control_at_action", DoubleType(), nullable=True),
            StructField("data_source", StringType(), nullable=True),
        ]
    )

    from ingestion.defcon_lite_common import _make_values_udf

    values_udf = _make_values_udf(
        disturb_radius_m=params.disturb_radius_m,
        deter_cone_angle_deg=params.deter_cone_angle_deg,
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
        data_source="metrica_tracking",
        champion_model_bytes=champion_model_bytes,
    )

    valued_sdf = credits_sdf.groupBy("match_id").applyInPandas(
        values_udf,  # type: ignore[arg-type]
        schema=valued_schema,
    )

    # Build replaceWhere predicate for all new matches
    escaped_ids = ", ".join(f"'{mid}'" for mid in new_ids_str)
    replace_predicate = f"data_source = 'metrica_tracking' AND match_id IN ({escaped_ids})"

    written = write_delta_table(
        valued_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=replace_predicate,
        logger=logger,
    )

    logger.info("Tracking matches: %d DEFCON-lite rows written across %d matches", written, len(new_ids_str))
    return written
