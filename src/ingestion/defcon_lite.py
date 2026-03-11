"""DEFCON-lite batch computation pipeline.

Reads action values from ``fct_action_values`` and freeze frame data from
``statsbomb_360`` in the bronze layer, computes per-defender defensive credits
using the DEFCON-lite analytics module, and writes results to a new
``defcon_results`` bronze table.

Design: "Read from gold + bronze, compute, write to bronze." Actions come from
the gold mart (SPADL 105x68m coordinates). Freeze frames come from bronze
(StatsBomb 360 data).

Architecture: Uses two-pass ``applyInPandas`` to distribute DEFCON computation
across Spark executors instead of sequential per-match driver loops.
Pass 1 assigns defensive credits per match (Stage 1), Pass 2 estimates
DEFCON values via XGBoost per match (Stage 2).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from analytics.defcon_lite import DefconLiteParams
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

# ---------------------------------------------------------------------------
# Column name prefixes used to distinguish actions vs freeze-frame columns
# after joining in Spark.  The UDF splits them back apart.
# ---------------------------------------------------------------------------
_ACTION_PREFIX = "act_"
_FF_PREFIX = "ff_"


def _make_credits_udf_360(
    disturb_radius_m: float,
    deter_cone_angle_deg: float,
    pitch_length: float,
    pitch_width: float,
) -> object:
    """Build the Pass 1 ``applyInPandas`` UDF closure for 360 data.

    Scalar params are captured by the closure so they are serialised with
    the UDF and available on executors without network access.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    # Capture serialisable scalars (no dataclass — pickle compatibility)
    _disturb_r = disturb_radius_m
    _deter_angle = deter_cone_angle_deg
    _pitch_l = pitch_length
    _pitch_w = pitch_width

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Assign defensive credits for one match group (360 data)."""
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

        # Split joined columns back into actions and freeze frames
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

        ff = _pd.DataFrame(
            {
                "event_id": pdf["act_event_id"],
                "player_id": pdf["ff_player_id"],
                "team_id": pdf["ff_team_id"],
                "teammate": pdf["ff_teammate"],
                "x": pdf["ff_x"],
                "y": pdf["ff_y"],
                "velocity_x": pdf["ff_velocity_x"],
                "velocity_y": pdf["ff_velocity_y"],
            }
        )

        result = _assign(actions, ff, params)
        if result.empty:
            return _pd.DataFrame(columns=_empty_cols)

        return _pd.DataFrame(result)

    return _udf


def _make_values_udf(
    disturb_radius_m: float,
    deter_cone_angle_deg: float,
    pitch_length: float,
    pitch_width: float,
    data_source: str,
) -> object:
    """Build the Pass 2 ``applyInPandas`` UDF closure for DEFCON value estimation.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    _disturb_r = disturb_radius_m
    _deter_angle = deter_cone_angle_deg
    _pitch_l = pitch_length
    _pitch_w = pitch_width
    _data_source = data_source

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Estimate DEFCON values for one match's credits."""
        import pandas as _pd

        from analytics.defcon_lite import DefconLiteParams as _Params
        from analytics.defcon_lite import estimate_values_for_match as _estimate

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
                "defcon_value",
                "dist_to_ball",
                "pitch_control_at_action",
                "data_source",
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

        result = _estimate(pdf, params)
        result["data_source"] = _data_source

        return _pd.DataFrame(result)

    return _udf


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


def _process_360_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: DefconLiteParams,
) -> int:
    """Process StatsBomb 360 matches via two-pass applyInPandas.

    Pass 1: ``groupBy("match_id").applyInPandas`` assigns defensive credits
    on executors.
    Pass 2: ``groupBy("match_id").applyInPandas`` runs XGBoost value
    estimation on executors.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    action_table = f"{catalog}.{_GOLD_SCHEMA}.fct_action_values"
    ff_table = f"{catalog}.bronze.statsbomb_360"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    try:
        match_id_rows = spark.table(ff_table).select("match_id").distinct().collect()
    except Exception:
        logger.warning("Cannot read table %s", ff_table)
        return 0

    if not match_id_rows:
        logger.info("No matches in %s", ff_table)
        return 0

    all_match_ids = [row["match_id"] for row in match_id_rows]

    # Check which matches already have DEFCON results (incremental)
    # Normalize to str for comparison — source tables may store match_id as int or str
    existing_ids: set[str] = set()
    try:
        existing_rows = (
            spark.table(results_table).filter("data_source = 'statsbomb_360'").select("match_id").distinct().collect()
        )
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in all_match_ids if str(mid) not in existing_ids]
    logger.info(
        "%d 360 matches total, %d already processed, %d to process",
        len(all_match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # Build Spark DataFrames for all new matches at once
    new_ids_str = [str(mid) for mid in new_match_ids]

    actions_df = (
        spark.table(action_table)
        .filter(F.col("match_id").cast("string").isin(new_ids_str))
        .filter("original_event_id IS NOT NULL AND original_event_id != 'None'")
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

    ff_df = (
        spark.table(ff_table)
        .filter(F.col("match_id").cast("string").isin(new_ids_str))
        .selectExpr(
            "id as ff_event_id",
            "teammate as ff_teammate",
            "from_json(location, 'ARRAY<DOUBLE>') as loc",
        )
        .selectExpr(
            "ff_event_id",
            "ff_teammate",
            "loc[0] * 105.0 / 120.0 as ff_x",
            "loc[1] * 68.0 / 80.0 as ff_y",
        )
    )

    # 360 freeze frames are anonymous — add synthetic IDs
    ff_df = (
        ff_df.withColumn("ff_player_id", F.monotonically_increasing_id().cast("int"))
        .withColumn("ff_team_id", F.lit(0).cast("int"))
        .withColumn("ff_velocity_x", F.lit(0.0))
        .withColumn("ff_velocity_y", F.lit(0.0))
    )

    # Join actions x freeze frames on event_id
    joined = actions_df.join(ff_df, actions_df["act_event_id"] == ff_df["ff_event_id"], "inner").drop("ff_event_id")

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

    credits_udf = _make_credits_udf_360(
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

    values_udf = _make_values_udf(
        disturb_radius_m=params.disturb_radius_m,
        deter_cone_angle_deg=params.deter_cone_angle_deg,
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
        data_source="statsbomb_360",
    )

    valued_sdf = credits_sdf.groupBy("match_id").applyInPandas(
        values_udf,  # type: ignore[arg-type]
        schema=valued_schema,
    )

    # Build replaceWhere predicate for all new matches
    escaped_ids = ", ".join(f"'{mid}'" for mid in new_ids_str)
    replace_predicate = f"data_source = 'statsbomb_360' AND match_id IN ({escaped_ids})"

    written = write_delta_table(
        valued_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=replace_predicate,
        logger=logger,
    )

    logger.info("360 matches: %d DEFCON-lite rows written across %d matches", written, len(new_match_ids))
    return written


def _process_tracking_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: DefconLiteParams,
) -> int:
    """Process Metrica tracking matches via two-pass applyInPandas.

    For tracking data, pseudo-freeze-frames are built inside the UDF
    by sampling the nearest frame to each action's time window.

    Pass 1: ``groupBy("act_match_id").applyInPandas`` assigns credits.
    Pass 2: ``groupBy("match_id").applyInPandas`` estimates values.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    action_table = f"{catalog}.{_GOLD_SCHEMA}.fct_action_values"
    tracking_table = f"{catalog}.{_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    try:
        match_id_rows = (
            spark.table(tracking_table).filter("source_provider = 'metrica'").select("match_id").distinct().collect()
        )
    except Exception:
        logger.warning("Cannot read table %s", tracking_table)
        return 0

    if not match_id_rows:
        return 0

    all_match_ids = [row["match_id"] for row in match_id_rows]

    # Check which matches already have DEFCON results (incremental)
    existing_ids: set[str] = set()
    try:
        existing_rows = (
            spark.table(results_table)
            .filter("data_source = 'metrica_tracking'")
            .select("match_id")
            .distinct()
            .collect()
        )
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in all_match_ids if str(mid) not in existing_ids]
    logger.info(
        "%d tracking matches total, %d already processed, %d to process",
        len(all_match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # Build Spark DataFrames for all new matches at once
    new_ids_str = [str(mid) for mid in new_match_ids]

    # Early-out: check if fct_action_values has ANY matching entries.
    # Metrica tracking has no SPADL action values (SPADL only covers StatsBomb/Wyscout).
    # Cast to string avoids BIGINT mismatch when match_ids are strings like "Sample_Game_2".
    matching_action_count = (
        spark.table(action_table).filter(F.col("match_id").cast("string").isin(new_ids_str)).limit(1).count()
    )
    if matching_action_count == 0:
        logger.info("No matching action values for %d tracking matches — skipping DEFCON tracking", len(new_match_ids))
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

    values_udf = _make_values_udf(
        disturb_radius_m=params.disturb_radius_m,
        deter_cone_angle_deg=params.deter_cone_angle_deg,
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
        data_source="metrica_tracking",
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

    logger.info("Tracking matches: %d DEFCON-lite rows written across %d matches", written, len(new_match_ids))
    return written


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
