"""AC-1 — Unified action context pipeline.

Reads SPADL actions + tracking data from bronze, runs the full silly-kicks
enrichment chain in a single applyInPandas pass per match, writes results to
bronze.spadl_action_context.

Providers: ALL (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, GradientSports).
Event-only providers get game_state + GK resolution; tracking providers get ~102 cols.
Architecture: "Read from bronze, compute, write to bronze."
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

import numpy as np

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    from silly_kicks.xthreat import ExpectedThreat

_TABLE_NAME = "spadl_action_context"

# ── Frame batching ────────────────────────────────────────────────────
# IDSSE (match_id, period) groups are 1.5M-1.7M rows -- exceeds the 1 GB
# Databricks serverless UDF group cap. Sub-batch by frame number.
_FRAME_BATCH_SIZE = 250

# Tolerance (seconds) for buffering actions at batch edges.
_ACTION_TIME_BUFFER_SECONDS = 0.5

# Metrica player ID jersey regex — compiled at module level per convention.
_JERSEY_RE = re.compile(r"Player\s*(\d+)")


# ── Provider classification ──────────────────────────────────────────

_TRACKING_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner", "gradientsports"})
_EVENT_ONLY_PROVIDERS: frozenset[str] = frozenset({"statsbomb", "wyscout"})
_ALL_PROVIDERS: frozenset[str] = _TRACKING_PROVIDERS | _EVENT_ONLY_PROVIDERS


def _is_tracking_provider(provider: str) -> bool:
    return provider in _TRACKING_PROVIDERS


def _is_event_only_provider(provider: str) -> bool:
    return provider in _EVENT_ONLY_PROVIDERS


# ── Schema constants ─────────────────────────────────────────────────
# Identity (12) + game_state (1) + linkage (4) + GK (10) + features (77) + audit (1) = 105

_RESULT_COLUMNS: list[str] = [
    # Identity (12)
    "data_source",
    "match_id",
    "action_id",
    "period_id",
    "time_seconds",
    "team_id",
    "player_id",
    "type_name",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    # Game state (1)
    "game_state",
    # Frame linkage (4)
    "frame_id",
    "time_offset_seconds",
    "link_quality_score",
    "n_candidate_frames",
    # GK resolution (4)
    "defending_gk_player_id_native",
    "gk_was_distributing",
    "gk_was_engaged",
    "gk_actions_in_possession",
    # GK spatial (6)
    "pre_shot_gk_x",
    "pre_shot_gk_y",
    "pre_shot_gk_distance_to_goal",
    "pre_shot_gk_distance_to_shot",
    "pre_shot_gk_angle_to_shot_trajectory",
    "pre_shot_gk_angle_off_goal_line",
    # Action context (4)
    "nearest_defender_distance",
    "actor_speed",
    "receiver_zone_density",
    "defenders_in_triangle_to_goal",
    # Actor pre-window (2)
    "actor_arc_length_pre_window",
    "actor_displacement_pre_window",
    # Pressure (3)
    "pressure_on_actor__andrienko_oval",
    "pressure_on_actor__link_zones",
    "pressure_on_actor__bekkers_pi",
    # Pitch control (3)
    "pitch_control_at_ball__spearman",
    "pitch_control_at_ball__fernandez_bornn",
    "pitch_control_at_ball__voronoi",
    # Defensive line (6)
    "defensive_line_x",
    "back_line_high_x",
    "compactness_x",
    "lateral_width",
    "max_lateral_gap",
    "back_n_count",
    # Off-ball context (6)
    "line_break",
    "n_attackers_behind_line",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    # Ward line-breaking (3)
    "line_break__ward",
    "lines_broken__ward",
    "line_breaking_type__ward",
    # Team shape (14)
    "team_shape_centroid_x_attacking",
    "team_shape_centroid_y_attacking",
    "team_shape_convex_hull_area_attacking",
    "team_shape_team_length_attacking",
    "team_shape_team_width_attacking",
    "team_shape_stretch_index_attacking",
    "team_shape_n_outfield_players_attacking",
    "team_shape_centroid_x_defending",
    "team_shape_centroid_y_defending",
    "team_shape_convex_hull_area_defending",
    "team_shape_team_length_defending",
    "team_shape_team_width_defending",
    "team_shape_stretch_index_defending",
    "team_shape_n_outfield_players_defending",
    # DAS (3)
    "das_team",
    "das_opponent",
    "das_diff",
    # GK influence (4)
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    # Cover shadows (5)
    "n_blocked_receivers",
    "n_potential_receivers",
    "blocking_score",
    "blocked_threat_fraction",
    "max_single_defender_blocking_score",
    # Sync score (3)
    "sync_score_min",
    "sync_score_mean",
    "sync_score_high_quality_frac",
    # OBSO (3)
    "obso_actual",
    "obso_peak",
    "obso_optimal",
    # PAUSA (3)
    "pausa_temporal",
    "pausa_spatial",
    "pausa_composite",
    # Space creation (2)
    "space_created_m2_team",
    "space_created_m2_opponent",
    # ELASTIC sync (3)
    "elastic_frame_id",
    "elastic_confidence",
    "elastic_error_seconds",
    # Shape graph (6)
    "shape_graph_density_attacking",
    "shape_graph_n_edges_attacking",
    "shape_graph_mean_stability_attacking",
    "shape_graph_density_defending",
    "shape_graph_n_edges_defending",
    "shape_graph_mean_stability_defending",
    # Audit (1)
    "_ingested_at",
]

_ACTION_CONTEXT_DDL = (
    "data_source STRING, match_id STRING, action_id BIGINT, period_id BIGINT, "
    "time_seconds DOUBLE, team_id STRING, player_id STRING, type_name STRING, "
    "start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "game_state STRING, "
    "frame_id BIGINT, time_offset_seconds DOUBLE, link_quality_score DOUBLE, "
    "n_candidate_frames BIGINT, "
    "defending_gk_player_id_native STRING, gk_was_distributing BOOLEAN, "
    "gk_was_engaged BOOLEAN, gk_actions_in_possession BIGINT, "
    "pre_shot_gk_x DOUBLE, pre_shot_gk_y DOUBLE, "
    "pre_shot_gk_distance_to_goal DOUBLE, pre_shot_gk_distance_to_shot DOUBLE, "
    "pre_shot_gk_angle_to_shot_trajectory DOUBLE, pre_shot_gk_angle_off_goal_line DOUBLE, "
    "nearest_defender_distance DOUBLE, actor_speed DOUBLE, "
    "receiver_zone_density BIGINT, defenders_in_triangle_to_goal BIGINT, "
    "actor_arc_length_pre_window DOUBLE, actor_displacement_pre_window DOUBLE, "
    "pressure_on_actor__andrienko_oval DOUBLE, pressure_on_actor__link_zones DOUBLE, "
    "pressure_on_actor__bekkers_pi DOUBLE, "
    "pitch_control_at_ball__spearman DOUBLE, pitch_control_at_ball__fernandez_bornn DOUBLE, "
    "pitch_control_at_ball__voronoi DOUBLE, "
    "defensive_line_x DOUBLE, back_line_high_x DOUBLE, compactness_x DOUBLE, "
    "lateral_width DOUBLE, max_lateral_gap DOUBLE, back_n_count BIGINT, "
    "line_break BOOLEAN, n_attackers_behind_line BIGINT, "
    "n_off_ball_runners_pre_window BIGINT, "
    "max_off_ball_run_displacement_pre_window DOUBLE, "
    "mean_off_ball_run_speed_pre_window DOUBLE, "
    "n_off_ball_runners_toward_goal_pre_window BIGINT, "
    "line_break__ward BOOLEAN, lines_broken__ward BIGINT, "
    "line_breaking_type__ward STRING, "
    "team_shape_centroid_x_attacking DOUBLE, team_shape_centroid_y_attacking DOUBLE, "
    "team_shape_convex_hull_area_attacking DOUBLE, team_shape_team_length_attacking DOUBLE, "
    "team_shape_team_width_attacking DOUBLE, team_shape_stretch_index_attacking DOUBLE, "
    "team_shape_n_outfield_players_attacking BIGINT, "
    "team_shape_centroid_x_defending DOUBLE, team_shape_centroid_y_defending DOUBLE, "
    "team_shape_convex_hull_area_defending DOUBLE, team_shape_team_length_defending DOUBLE, "
    "team_shape_team_width_defending DOUBLE, team_shape_stretch_index_defending DOUBLE, "
    "team_shape_n_outfield_players_defending BIGINT, "
    "das_team DOUBLE, das_opponent DOUBLE, das_diff DOUBLE, "
    "gk_pitch_control_share_weighted DOUBLE, gk_reachable_area_m2 DOUBLE, "
    "gk_closing_time_mean_s__six_yard_box DOUBLE, gk_closing_time_min_s__six_yard_box DOUBLE, "
    "n_blocked_receivers BIGINT, n_potential_receivers BIGINT, "
    "blocking_score DOUBLE, blocked_threat_fraction DOUBLE, "
    "max_single_defender_blocking_score DOUBLE, "
    "sync_score_min DOUBLE, sync_score_mean DOUBLE, sync_score_high_quality_frac DOUBLE, "
    "obso_actual DOUBLE, obso_peak DOUBLE, obso_optimal DOUBLE, "
    "pausa_temporal DOUBLE, pausa_spatial DOUBLE, pausa_composite DOUBLE, "
    "space_created_m2_team DOUBLE, space_created_m2_opponent DOUBLE, "
    "elastic_frame_id BIGINT, elastic_confidence DOUBLE, elastic_error_seconds DOUBLE, "
    "shape_graph_density_attacking DOUBLE, shape_graph_n_edges_attacking BIGINT, "
    "shape_graph_mean_stability_attacking DOUBLE, "
    "shape_graph_density_defending DOUBLE, shape_graph_n_edges_defending BIGINT, "
    "shape_graph_mean_stability_defending DOUBLE, "
    "_ingested_at TIMESTAMP"
)


# ── DDL parser ────────────────────────────────────────────────────────


def _parse_ddl_to_struct_type(ddl: str) -> StructType:
    """Parse a Spark DDL column-list string into a StructType.

    Handles: STRING, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP.
    Excludes _ingested_at (added by write_delta_table, not by the UDF).
    """
    from pyspark.sql.types import (
        BooleanType,
        DataType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map: dict[str, DataType] = {
        "STRING": StringType(),
        "BIGINT": LongType(),
        "DOUBLE": DoubleType(),
        "BOOLEAN": BooleanType(),
        "TIMESTAMP": TimestampType(),
    }
    fields: list[StructField] = []
    for token in ddl.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split()
        if len(parts) != 2:
            continue
        col_name, col_type = parts[0], parts[1].upper()
        if col_name == "_ingested_at":
            continue
        spark_type = type_map.get(col_type)
        if spark_type is None:
            msg = f"Unknown Spark type {col_type!r} for column {col_name!r}"
            raise ValueError(msg)
        fields.append(StructField(col_name, spark_type, nullable=True))
    return StructType(fields)


_RESULT_SCHEMA_CACHE: StructType | None = None


def _get_result_schema() -> StructType:
    """Lazy accessor for the applyInPandas StructType schema."""
    global _RESULT_SCHEMA_CACHE
    if _RESULT_SCHEMA_CACHE is None:
        _RESULT_SCHEMA_CACHE = _parse_ddl_to_struct_type(_ACTION_CONTEXT_DDL)
    return _RESULT_SCHEMA_CACHE


# ── xT serialization ─────────────────────────────────────────────────
# Re-uses helpers from tracking_context. Import at call site to avoid
# circular imports at module load time.


def _serialize_xt_grid(xt_array: np.ndarray, *, grid_l: int, grid_w: int) -> dict[str, object]:
    """Serialize an ExpectedThreat grid as JSON-safe scalar primitives."""
    return {"xt_grid": xt_array.tolist(), "l": grid_l, "w": grid_w}


# ── Column projection constants ──────────────────────────────────────
# GradientSports is new to AC-1 (not in tracking_context.py).
# IDSSE/Metrica/SkillCorner column lists imported from tracking_context
# inside the UDF (lazy import to avoid import-time pyspark dependency).

_GRADIENTSPORTS_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "period",
    "frame_num",
    "period_elapsed_time",
    "team_side",
    "is_ball",
    "jersey_num",
    "x",
    "y",
    "z",
)


# ── Identity resolution ──────────────────────────────────────────────


def _resolve_enrichment_identity(
    actions: pd.DataFrame,
    *,
    provider: str,
    match_id_native: str,
) -> pd.DataFrame:
    """Replace team_id/player_id with silly-kicks-compatible values.

    MUTATE-THEN-RESTORE contract: this overwrites team_id/player_id before
    enrichment. _restore_native_identity() restores native IDs after enrichment.
    """
    non_null_mask = actions["team_id_native"].notna()
    if not non_null_mask.any():
        msg = f"team_id_native is entirely null for provider={provider}"
        raise ValueError(msg)

    actions["team_id"] = actions["team_id"].astype("object")
    actions["player_id"] = actions["player_id"].astype("object")

    if provider == "idsse":
        # DFL CLU/OBJ strings match both frames and home_team_id directly.
        actions.loc[non_null_mask, "team_id"] = actions.loc[non_null_mask, "team_id_native"]
        actions.loc[non_null_mask, "player_id"] = actions.loc[non_null_mask, "player_id_native"]

    elif provider == "metrica":
        from shared.identifiers import metrica_native_team_id

        fwd = {
            metrica_native_team_id(match_id_native, "home"): "Home",
            metrica_native_team_id(match_id_native, "away"): "Away",
        }
        actions.loc[non_null_mask, "team_id"] = actions.loc[non_null_mask, "team_id_native"].map(fwd)
        actions.loc[non_null_mask, "player_id"] = actions.loc[non_null_mask, "player_id_native"]

    elif provider == "skillcorner":
        # SkillCorner native IDs are stringified integers.
        actions.loc[non_null_mask, "team_id"] = actions.loc[non_null_mask, "team_id_native"]
        actions.loc[non_null_mask, "player_id"] = actions.loc[non_null_mask, "player_id_native"]

    elif provider == "gradientsports":
        # GradientSports native IDs are stringified integers (same pattern as SkillCorner).
        # Frames from convert_to_frames use string team_id matching native format.
        actions.loc[non_null_mask, "team_id"] = actions.loc[non_null_mask, "team_id_native"]
        actions.loc[non_null_mask, "player_id"] = actions.loc[non_null_mask, "player_id_native"]

    return actions


def _restore_native_identity(actions: pd.DataFrame) -> pd.DataFrame:
    """Restore native IDs for output (dim table joins via staging layer)."""
    actions["team_id"] = actions["team_id_native"]
    actions["player_id"] = actions["player_id_native"]
    return actions


# ── Enrichment chains ─────────────────────────────────────────────────


def _enrich_tracking_match(
    actions_df: pd.DataFrame,
    tracking_df: pd.DataFrame,
    xt: ExpectedThreat,
    home_team_id: str,
) -> pd.DataFrame:
    """Full enrichment chain for tracking providers.

    See spec section 4.2 for the complete call graph and ordering rationale.
    """
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_das,
        add_defensive_line,
        add_elastic_sync,
        add_gk_influence,
        add_line_break,
        add_obso,
        add_off_ball_context,
        add_pausa,
        add_pre_shot_gk_angle,
        add_pre_shot_gk_position,
        add_pressure_on_actor,
        add_shape_graph,
        add_space_creation,
        add_sync_score,
        add_team_shape,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    # Step 0: Actions-only enrichments (no tracking needed)
    out = add_game_state(actions_df)

    # Step 1: Frame linkage — computed ONCE; links passed to every add_* call.
    links, _report = link_actions_to_frames(out, tracking_df)

    # Step 2: GK resolution (pure SPADL + tracking; no links kwarg).
    out = add_pre_shot_gk_context(out, frames=tracking_df)

    # Step 3: Action context
    out = add_action_context(out, tracking_df, links=links)

    # Step 4: Actor pre-window
    out = add_actor_pre_window(out, tracking_df, links=links)

    # Step 5a: Pressure — andrienko_oval + link_zones
    out = add_pressure_on_actor(
        out,
        tracking_df,
        links=links,
        methods=("andrienko_oval", "link_zones"),
    )

    # Step 5b: Pressure — bekkers_pi (needs is_ball=True rows)
    try:
        out = add_pressure_on_actor(
            out,
            tracking_df,
            links=links,
            methods=("bekkers_pi",),
        )
    except ValueError as exc:
        if "is_ball=True" in str(exc):
            logger.error("bekkers_pi degraded to NaN: %s", exc)
            out["pressure_on_actor__bekkers_pi"] = np.nan
        else:
            raise

    # Step 6: Pitch control — 3 methods via Series API
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(out, tracking_df, links=links, method=method)
        out[s.name] = s.values

    # Step 7: Defensive line
    out = add_defensive_line(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 8: Off-ball context (umbrella — includes off-ball-run columns)
    out = add_off_ball_context(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 9: Ward line-breaking
    out = add_line_break(out, tracking_df, links=links, method="ward", home_team_id=home_team_id)

    # Step 10: Team shape
    out = add_team_shape(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 11: DAS (chunk_size=10 prevents OOM under 1 GB group cap)
    out = add_das(out, tracking_df, links=links, chunk_size=10)

    # Step 12: GK spatial (requires defending_gk_player_id from Step 2)
    out = add_pre_shot_gk_position(out, tracking_df, links=links)
    out = add_pre_shot_gk_angle(out, frames=tracking_df, links=links)

    # Step 13: GK influence (xt positional)
    out = add_gk_influence(out, tracking_df, xt, links=links, home_team_id=home_team_id)

    # Step 14: Cover shadows (xt positional)
    out = add_cover_shadows(out, tracking_df, xt, links=links, home_team_id=home_team_id)

    # Step 15: Shape graph
    out = add_shape_graph(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 16: OBSO — MUST precede add_pausa
    out = add_obso(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 17: PAUSA (depends on OBSO columns from Step 16)
    out = add_pausa(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 18: Space creation
    out = add_space_creation(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 19: ELASTIC sync
    out = add_elastic_sync(out, tracking_df)

    # Step 20: Sync score
    out = add_sync_score(out, links)

    return out


def _enrich_sb360_match(
    actions_df: pd.DataFrame,
    freeze_frames: pd.DataFrame,
    home_team_id: str,
) -> pd.DataFrame:
    """Enrichment chain for StatsBomb 360 matches.

    Uses snapshot_to_tracking_frames to convert per-event freeze-frame
    snapshots into synthetic tracking frames, then runs single-frame
    add_* features. Velocity/temporal features remain NULL.
    """
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_defensive_line,
        add_line_break,
        add_team_shape,
        snapshot_to_tracking_frames,
    )

    # Step 0: Actions-only enrichments
    out = add_game_state(actions_df)
    # GK resolution — SPADL-only (no frames=). Snapshot frames lack temporal
    # continuity for GK tracking fallback; positional features run post-conversion.
    out = add_pre_shot_gk_context(out)

    # Step 1: Convert freeze-frames to synthetic tracking frames + links.
    frames, links = snapshot_to_tracking_frames(freeze_frames, out)

    if len(frames) == 0:
        return out  # No freeze-frame data — event-only fallback

    # Step 2: Single-frame positional features
    out = add_action_context(out, frames, links=links)

    # Step 3: Defensive line
    out = add_defensive_line(out, frames, links=links, home_team_id=home_team_id)

    # Step 4: Ward line-breaking — primary SB360 value-add
    out = add_line_break(out, frames, links=links, method="ward", home_team_id=home_team_id)

    # Step 5: Team shape
    out = add_team_shape(out, frames, links=links, home_team_id=home_team_id)

    return out


def _enrich_event_only_match(actions_df: pd.DataFrame) -> pd.DataFrame:
    """Minimal enrichment for event-only providers (StatsBomb, Wyscout)."""
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context

    out = add_game_state(actions_df)
    out = add_pre_shot_gk_context(out)
    return out


# ── Post-enrichment output handler ────────────────────────────────────


def _build_output(
    actions: pd.DataFrame,
    match_id_native: str,
    data_source: str,
) -> pd.DataFrame:
    """Post-enrichment: renames + column selection for bronze write.

    1. game_id -> match_id (silly-kicks uses game_id, we use match_id)
    2. defending_gk_player_id -> defending_gk_player_id_native (ADR-018)
    3. type_id -> type_name via silly-kicks add_names
    4. Column selection to _RESULT_COLUMNS with NaN fill for missing cols.
    """
    out = actions.copy()
    out["match_id"] = match_id_native
    out["data_source"] = data_source

    if "type_name" not in out.columns and "type_id" in out.columns:
        from silly_kicks.spadl.utils import add_names

        out = add_names(out)

    # Restore native IDs for dim table joins via staging layer.
    if "team_id_native" in out.columns:
        out = _restore_native_identity(out)

    if "defending_gk_player_id" in out.columns:
        out = out.rename(columns={"defending_gk_player_id": "defending_gk_player_id_native"})

    output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    for col in output_cols:
        if col not in out.columns:
            out[col] = np.nan
    return out[output_cols].copy()


# ── GradientSports bronze -> converter input mapper ───────────────────

_GS_FRAME_RATE = 30  # GradientSports default frame rate


def _bronze_gradientsports_to_converter_input(
    trk_pdf: pd.DataFrame,
    *,
    team_side_to_id: dict[str, str],
    jersey_to_player_id: dict[tuple[str, str], str],
    gk_player_ids: frozenset[str],
) -> pd.DataFrame:
    """Map bronze ``gradientsports_tracking`` columns to silly-kicks converter input.

    Args:
        trk_pdf: Bronze tracking rows (columns per _GRADIENTSPORTS_TRACKING_SELECT_COLS).
        team_side_to_id: Maps team_side ("home"/"away") -> native team_id string.
        jersey_to_player_id: Maps (team_side, jersey_num) -> native player_id string.
        gk_player_ids: Set of player_id strings who are goalkeepers.

    Returns:
        DataFrame with columns matching silly_kicks.tracking.gradientsports.EXPECTED_INPUT_COLUMNS.
    """
    import pandas as _pd

    result = _pd.DataFrame()
    result["game_id"] = trk_pdf["match_id"]
    result["period_id"] = trk_pdf["period"].astype("Int64")
    result["frame_id"] = trk_pdf["frame_num"].astype("Int64")
    result["time_seconds"] = trk_pdf["period_elapsed_time"].astype("float64")
    result["frame_rate"] = _GS_FRAME_RATE
    result["is_ball"] = trk_pdf["is_ball"].fillna(False)
    result["x_centered"] = trk_pdf["x"].astype("float64")
    result["y_centered"] = trk_pdf["y"].astype("float64")
    result["z"] = trk_pdf["z"].astype("float64")
    result["speed_native"] = np.nan  # Derived by converter/post-processing
    result["ball_state"] = "alive"  # GS does not provide per-frame ball state

    # Map team_side -> team_id; ball rows get NaN team_id
    result["team_id"] = trk_pdf["team_side"].map(team_side_to_id)

    # Map (team_side, jersey_num) -> player_id; ball rows get NaN
    _side = trk_pdf["team_side"].fillna("")
    _jersey = trk_pdf["jersey_num"].fillna("")
    result["player_id"] = [jersey_to_player_id.get((s, j)) for s, j in zip(_side, _jersey, strict=False)]

    # is_goalkeeper from roster
    result["is_goalkeeper"] = result["player_id"].isin(gk_player_ids)
    # Ball rows: explicit False for is_goalkeeper
    result.loc[result["is_ball"] == True, "is_goalkeeper"] = False  # noqa: E712

    return result.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


# ── UDF factory ───────────────────────────────────────────────────────


def _make_action_context_udf(
    provider: str,
    home_team_id: str,
    home_start_left: bool,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    actions_records: list[dict[str, Any]],
    native_match_id: str,
    *,
    gs_team_side_to_id: dict[str, str] | None = None,
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None,
    gs_gk_player_ids: list[str] | None = None,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the applyInPandas UDF closure for action context enrichment.

    All arguments are Python scalar primitives or small serializable structures.
    GradientSports-specific args (gs_*) are only needed for that provider.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import gc as _gc
        import logging as _logging

        import numpy as _np
        import pandas as _pd
        from silly_kicks.xthreat import ExpectedThreat as _ExpectedThreat

        from ingestion.action_context import (
            _ACTION_TIME_BUFFER_SECONDS,
            _RESULT_COLUMNS,
            _build_output,
            _enrich_tracking_match,
            _resolve_enrichment_identity,
        )

        _logger = _logging.getLogger("action_context_udf")

        if pdf.empty:
            output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
            return _pd.DataFrame(columns=_pd.Index(output_cols))

        match_id_val = pdf["match_id"].iloc[0]
        period_val = pdf["period"].iloc[0]
        batch_id_val = pdf["frame_batch_id"].iloc[0] if "frame_batch_id" in pdf.columns else None

        if len(pdf) > 2_000_000:
            _logger.warning(
                "Large UDF group: match_id=%s, period=%s, rows=%d (>2M)",
                match_id_val,
                period_val,
                len(pdf),
            )

        try:
            # Reconstruct xT from scalar primitives
            xt = _ExpectedThreat(l=xt_l, w=xt_w)
            xt.xT = _np.array(xt_grid_data, dtype=_np.float64)

            # Reconstruct actions, filter to this period
            all_actions = _pd.DataFrame(actions_records)
            actions = all_actions[all_actions["period_id"] == int(period_val)].copy()
            del all_actions

            # Further filter actions to this batch's time window
            if "time_seconds" in actions.columns and "timestamp" in pdf.columns:
                t_min = float(pdf["timestamp"].min()) - _ACTION_TIME_BUFFER_SECONDS
                t_max = float(pdf["timestamp"].max()) + _ACTION_TIME_BUFFER_SECONDS
                actions = actions[(actions["time_seconds"] >= t_min) & (actions["time_seconds"] <= t_max)].copy()

            if actions.empty:
                output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
                return _pd.DataFrame(columns=_pd.Index(output_cols))

            # Drop synthetic frame_batch_id before passing to converters
            if "frame_batch_id" in pdf.columns:
                pdf = pdf.drop(columns=["frame_batch_id"])

            # ── Provider-specific conversion ──
            if provider == "idsse":
                from silly_kicks.tracking import PreprocessConfig as _PreprocessConfig
                from silly_kicks.tracking.sportec import convert_to_frames as _convert_to_frames

                from ingestion.tracking_context import _bronze_idsse_to_sportec_input

                sportec_input = _bronze_idsse_to_sportec_input(pdf)
                del pdf
                _gc.collect()

                frames, _report = _convert_to_frames(
                    sportec_input,
                    home_team_id=home_team_id,
                    home_team_start_left=home_start_left,
                    output_convention="ltr",
                    preprocess=_PreprocessConfig(derive_velocity=True),
                )
                del sportec_input
                _gc.collect()

            elif provider == "metrica":
                from ingestion.tracking_context import _bronze_metrica_to_frames

                game_id = int(actions["game_id"].iloc[0])
                _pid_col = "player_id_native"
                _unique_pids = actions[_pid_col].dropna().unique()
                _has_space = any(" " in str(p) for p in _unique_pids)
                _fallback_fmt = "Player {}" if _has_space else "Player{}"
                _jersey_to_pid: dict[str, str] = {}
                for _p in _unique_pids:
                    _m = _JERSEY_RE.match(str(_p))
                    if _m:
                        _jersey_to_pid[_m.group(1)] = str(_p)
                frames = _bronze_metrica_to_frames(
                    pdf,
                    game_id=game_id,
                    jersey_to_pid=_jersey_to_pid,
                    fallback_fmt=_fallback_fmt,
                )
                del pdf
                _gc.collect()

            elif provider == "skillcorner":
                from ingestion.tracking_context import _bronze_skillcorner_to_frames

                game_id = int(actions["game_id"].iloc[0])
                frames = _bronze_skillcorner_to_frames(pdf, game_id=game_id)
                del pdf
                _gc.collect()

            elif provider == "gradientsports":
                from silly_kicks.tracking import PreprocessConfig as _PreprocessConfig
                from silly_kicks.tracking.gradientsports import (
                    convert_to_frames as _gs_convert_to_frames,
                )

                from ingestion.action_context import _bronze_gradientsports_to_converter_input

                _gs_j2p: dict[tuple[str, str], str] = {
                    (str(k[0]), str(k[1])): v for k, v in (gs_jersey_to_player_id or {}).items()
                }
                converter_input = _bronze_gradientsports_to_converter_input(
                    pdf,
                    team_side_to_id=gs_team_side_to_id or {},
                    jersey_to_player_id=_gs_j2p,
                    gk_player_ids=frozenset(gs_gk_player_ids or []),
                )
                del pdf
                _gc.collect()

                frames, _report = _gs_convert_to_frames(
                    converter_input,
                    home_team_id=int(home_team_id),
                    home_team_start_left=home_start_left,
                    output_convention="ltr",
                    preprocess=_PreprocessConfig(derive_velocity=True),
                )
                del converter_input
                _gc.collect()

            else:
                raise ValueError(f"Unknown provider: {provider}")

            # Align game_id
            frames["game_id"] = int(actions["game_id"].iloc[0])

            # Resolve enrichment identity
            actions = _resolve_enrichment_identity(
                actions,
                provider=provider,
                match_id_native=native_match_id,
            )

            # Run full enrichment chain
            result = _enrich_tracking_match(
                actions_df=actions,
                tracking_df=frames,
                xt=xt,
                home_team_id=home_team_id,
            )
            del frames, actions
            _gc.collect()

            return _build_output(result, native_match_id, provider)

        except Exception as exc:  # ADR-002 §5 hard-fail-first UDF: re-raise with group key context
            import traceback as _tb

            inner_tb = _tb.format_exc()
            _logger.error(
                "UDF failed for match_id=%s, period=%s, batch=%s:\n%s",
                match_id_val,
                period_val,
                batch_id_val,
                inner_tb,
            )
            raise RuntimeError(
                f"action_context UDF failed for match_id={match_id_val}, "
                f"period={period_val}, frame_batch_id={batch_id_val}:\n{inner_tb}"
            ) from exc

    return _udf


# ── Guard ─────────────────────────────────────────────────────────────


def _spadl_match_ids_by_provider(spark: SparkSession, catalog: str) -> dict[str, set[str]]:
    """Return {provider: {match_id_native, ...}} for all providers with SPADL actions."""
    from pyspark.sql import functions as F  # noqa: N812

    rows = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter(F.col("data_source").isin(*_ALL_PROVIDERS))
        .select(
            F.col("data_source").alias("provider"),
            F.col("match_id_native").alias("match_id"),
        )
        .distinct()
        .collect()
    )
    result: dict[str, set[str]] = {}
    for row in rows:
        result.setdefault(row["provider"], set()).add(row["match_id"])
    return result


class _ActionContextGuard:
    """SkipGuard adapter for action context pipeline.

    Discovers unprocessed matches across all 6 providers.
    IDSSE = 1 half per iteration (period-level).
    Other tracking + event-only = 2-4 matches/iteration.
    """

    workflow_id = "wf-action-context"
    chunk_sizes: ClassVar[dict[str, int]] = {
        "metrica": 2,
        "skillcorner": 2,
        "gradientsports": 2,
        "statsbomb": 4,
        "wyscout": 4,
    }

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check for unprocessed matches across all 6 providers."""
        from pyspark.sql import functions as F  # noqa: N812

        from ingestion.guards import ensure_table, find_new_ids
        from ingestion.utils import tolerate_missing_table

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _ACTION_CONTEXT_DDL)

        spadl_ids_by_provider = _spadl_match_ids_by_provider(spark, catalog)

        # ── IDSSE: period-level discovery (same as tracking_context) ──
        _raw_pairs = (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .select(
                F.col("match_id").cast("string").alias("match_id"),
                F.col("period").cast("bigint").alias("period"),
            )
            .distinct()
            .collect()
        )
        idsse_source_pairs = [(str(r["match_id"]), int(r["period"])) for r in _raw_pairs]

        idsse_done_pairs: set[tuple[str, int]] = set()
        with tolerate_missing_table(logger, "results table empty/missing"):
            done_rows = (
                spark.table(results_table)
                .filter(F.col("data_source") == "idsse")
                .select(
                    F.col("match_id").cast("string"),
                    F.col("period_id").cast("bigint").alias("period"),
                )
                .distinct()
                .collect()
            )
            idsse_done_pairs = {(str(r["match_id"]), int(r["period"])) for r in done_rows}

        idsse_spadl = spadl_ids_by_provider.get("idsse", set())
        idsse_half_chunks: list[str] = [
            f"idsse:{mid}:{period}"
            for mid, period in idsse_source_pairs
            if mid in idsse_spadl and (mid, period) not in idsse_done_pairs
        ]

        # ── Other tracking providers: match-level discovery ──
        metrica_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.metrica_tracking",
            results_table,
            results_filter="data_source = 'metrica'",
        )
        skillcorner_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.skillcorner_tracking",
            results_table,
            results_filter="data_source = 'skillcorner'",
        )
        gradientsports_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.gradientsports_tracking",
            results_table,
            results_filter="data_source = 'gradientsports'",
        )

        metrica_ids = [m for m in metrica_ids if m in spadl_ids_by_provider.get("metrica", set())]
        skillcorner_ids = [m for m in skillcorner_ids if m in spadl_ids_by_provider.get("skillcorner", set())]
        gradientsports_ids = [m for m in gradientsports_ids if m in spadl_ids_by_provider.get("gradientsports", set())]

        # ── Event-only providers: match-level from spadl_actions ──
        statsbomb_ids = self._find_event_only_new_ids(
            spark,
            catalog,
            schema,
            "statsbomb",
            spadl_ids_by_provider,
        )
        wyscout_ids = self._find_event_only_new_ids(
            spark,
            catalog,
            schema,
            "wyscout",
            spadl_ids_by_provider,
        )

        total = (
            len(idsse_half_chunks)
            + len(metrica_ids)
            + len(skillcorner_ids)
            + len(gradientsports_ids)
            + len(statsbomb_ids)
            + len(wyscout_ids)
        )
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        # Build chunks
        chunks: list[list[str]] = []
        for chunk_str in idsse_half_chunks:
            chunks.append([chunk_str])

        for prov, ids in [
            ("metrica", metrica_ids),
            ("skillcorner", skillcorner_ids),
            ("gradientsports", gradientsports_ids),
            ("statsbomb", statsbomb_ids),
            ("wyscout", wyscout_ids),
        ]:
            cs = self.chunk_sizes.get(prov, 2)
            for i in range(0, len(ids), cs):
                batch = ids[i : i + cs]
                chunks.append([f"{prov}:{','.join(batch)}"])

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            chunks=chunks,
        )

    def _find_event_only_new_ids(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        provider: str,
        spadl_ids_by_provider: dict[str, set[str]],
    ) -> list[str]:
        """Find match_ids in spadl_actions not yet in action_context for an event-only provider."""
        from ingestion.utils import tolerate_missing_table

        spadl_ids = spadl_ids_by_provider.get(provider, set())
        if not spadl_ids:
            return []

        done_ids: set[str] = set()
        from pyspark.sql import functions as F  # noqa: N812

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        with tolerate_missing_table(logger, f"results table missing for {provider}"):
            done_rows = (
                spark.table(results_table)
                .filter(F.col("data_source") == provider)
                .select(F.col("match_id").cast("string"))
                .distinct()
                .collect()
            )
            done_ids = {str(r["match_id"]) for r in done_rows}

        return sorted(spadl_ids - done_ids)


skip_guard = _ActionContextGuard()


# ── CLI arg parser ────────────────────────────────────────────────────


def _parse_action_match_ids_arg(raw: str | None) -> tuple[str, list[str], int | None] | None:
    """Parse ``--match-ids`` CLI value.

    Formats:
        ``"provider:id1,id2"`` — multiple matches, no period filter.
        ``"provider:id:period"`` — single match + period (IDSSE half-game chunks).
    """
    if raw is None or raw == "":
        return None
    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2' or 'provider:id:period', got {raw!r}. "
            f"Valid providers: {sorted(_ALL_PROVIDERS)}"
        )
    parts = raw.split(":")
    provider = parts[0]
    if provider not in _ALL_PROVIDERS:
        raise SystemExit(f"Unknown provider {provider!r}. Valid: {sorted(_ALL_PROVIDERS)}")

    # Detect "provider:match_id:period" format (3 parts, last is numeric)
    if len(parts) == 3 and parts[2].strip().isdigit():
        match_id = parts[1].strip()
        period = int(parts[2].strip())
        if not match_id:
            return None
        return (provider, [match_id], period)

    # Standard "provider:id1,id2" format
    id_str = ":".join(parts[1:])
    ids = [i.strip() for i in id_str.split(",") if i.strip()]
    if not ids:
        return None
    return (provider, ids, None)


def _write_action_chunks_task_value(
    chunks_for_inputs: list[str],
    task_logger: logging.Logger,
) -> None:
    """Write discovered chunks as a Databricks task value."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        spark = get_spark_session()
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="action_context_chunks", value=chunks_for_inputs)
        task_logger.info("Wrote task value 'action_context_chunks' (%d chunks)", len(chunks_for_inputs))
    except (ImportError, AttributeError, RuntimeError) as exc:
        task_logger.warning("Task values not available (standalone mode) -- %s", exc)


# ── Entry points ──────────────────────────────────────────────────────


def main_preflight() -> None:
    """CLI entry point for action context preflight.

    Runs the skip guard, partitions discovered matches into fan-out chunks,
    fits xT once, writes both as Databricks task values.
    """
    args = parse_ingestion_args("Preflight: discover unprocessed action context matches and emit chunks")
    task_logger = configure_logging("action_context_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    task_logger.info(
        "Action context preflight: %d missing matches across %d chunks",
        fr.count,
        len(chunks_for_inputs),
    )

    _write_action_chunks_task_value(chunks_for_inputs, task_logger)

    # Fit xT model once and serialize for all iterations
    if fr.count > 0:
        from pyspark.sql import functions as F  # noqa: N812
        from silly_kicks.xthreat import ExpectedThreat

        spadl_pdf = (
            spark.table(f"{args.catalog}.bronze.spadl_actions")
            .filter(F.col("data_source").isin(*_ALL_PROVIDERS))
            .select(
                "game_id",
                "action_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "type_id",
                "result_id",
                "bodypart_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "original_event_id",
            )
            .toPandas()
        )
        xt = ExpectedThreat().fit(spadl_pdf)
        del spadl_pdf
        task_logger.info("xT model fitted (grid shape %s)", xt.xT.shape)

        xt_data = _serialize_xt_grid(xt.xT, grid_l=xt.l, grid_w=xt.w)

        try:
            from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

            dbutils = DBUtils(spark)
            dbutils.jobs.taskValues.set(key="action_context_xt", value=xt_data)
            task_logger.info("Wrote task value 'action_context_xt'")
        except (ImportError, AttributeError, RuntimeError) as exc:
            task_logger.warning("Task values not available -- %s", exc)


def main() -> None:
    """CLI entry point for action context enrichment (for_each_task iteration).

    Reads ``--match-ids "provider:id1,id2"`` from the for_each_task input.
    Dispatches to the correct enrichment tier:
    - Tracking providers (IDSSE, Metrica, SkillCorner, GradientSports): applyInPandas
    - StatsBomb: SB360 tier (with freeze-frames) or event-only
    - Wyscout: event-only (driver-side, no tracking)
    """
    import json

    args = parse_ingestion_args(
        "Compute action context features",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2"})],
    )
    task_logger = configure_logging("action_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids_parsed = _parse_action_match_ids_arg(getattr(args, "match_ids", None))
    if match_ids_parsed is None:
        raise SystemExit("--match-ids is required")

    provider, ids, period_filter = match_ids_parsed
    task_logger.info("Iteration: provider=%s, match_ids=%s, period=%s", provider, ids, period_filter)

    # Deserialize preflight xT from task value
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(spark)
        xt_raw = dbutils.jobs.taskValues.get(
            taskKey="preflight_action_context",
            key="action_context_xt",
        )
        if isinstance(xt_raw, str):
            xt_data = json.loads(xt_raw)
        else:
            xt_data = xt_raw
        xt_grid_data: list[list[float]] = xt_data["xt_grid"]
        xt_l: int = int(xt_data["l"])
        xt_w: int = int(xt_data["w"])
        task_logger.info("Deserialized preflight xT grid (%dx%d)", xt_w, xt_l)
    except (ImportError, AttributeError, RuntimeError):
        task_logger.warning("Task values not available — fitting xT locally")
        from pyspark.sql import functions as F  # noqa: N812
        from silly_kicks.xthreat import ExpectedThreat

        spadl_pdf = (
            spark.table(f"{args.catalog}.bronze.spadl_actions")
            .filter(F.col("data_source").isin(*_ALL_PROVIDERS))
            .select(
                "game_id",
                "action_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "type_id",
                "result_id",
                "bodypart_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "original_event_id",
            )
            .toPandas()
        )
        xt = ExpectedThreat().fit(spadl_pdf)
        del spadl_pdf
        xt_serialized = _serialize_xt_grid(xt.xT, grid_l=xt.l, grid_w=xt.w)
        xt_grid_data = list(xt_serialized["xt_grid"])  # type: ignore[arg-type]
        xt_l = int(xt_serialized["l"])  # type: ignore[arg-type]
        xt_w = int(xt_serialized["w"])  # type: ignore[arg-type]

    from pyspark.sql import functions as F  # noqa: N812

    catalog, schema = args.catalog, args.schema
    total_written = 0

    for match_id in ids:
        period_str = f" period {period_filter}" if period_filter else ""
        task_logger.info("Processing %s match %s%s", provider, match_id, period_str)

        if _is_tracking_provider(provider):
            total_written += _process_tracking_match(
                spark,
                catalog,
                schema,
                provider,
                match_id,
                period_filter,
                xt_grid_data,
                xt_l,
                xt_w,
                task_logger,
            )
        elif provider == "statsbomb":
            total_written += _process_statsbomb_match(
                spark,
                catalog,
                schema,
                match_id,
                task_logger,
            )
        elif provider == "wyscout":
            total_written += _process_event_only_match(
                spark,
                catalog,
                schema,
                "wyscout",
                match_id,
                task_logger,
            )
        else:
            raise SystemExit(f"Unknown provider: {provider}")

    task_logger.info("Iteration complete -- %d rows written for %s", total_written, provider)


# ── Provider-specific processing ──────────────────────────────────────


def _process_tracking_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    provider: str,
    match_id: str,
    period_filter: int | None,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    task_logger: logging.Logger,
) -> int:
    """Process a single tracking-provider match via applyInPandas."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.tracking_context import (
        _IDSSE_TRACKING_SELECT_COLS,
        _METRICA_TRACKING_SELECT_COLS,
        _SKILLCORNER_TRACKING_SELECT_COLS,
    )
    from ingestion.utils import write_delta_table

    # ── Read tracking (Spark DataFrame — no .toPandas()) ──
    if provider == "idsse":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_IDSSE_TRACKING_SELECT_COLS)
        )
    elif provider == "metrica":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.metrica_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_METRICA_TRACKING_SELECT_COLS)
        )
    elif provider == "skillcorner":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.skillcorner_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_SKILLCORNER_TRACKING_SELECT_COLS)
        )
        matches_meta = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(F.col("match_id") == match_id)
            .select(
                F.col("player_id"),
                F.col("team_id").cast("string").alias("team"),
                (F.col("position_acronym") == "GK").alias("is_goalkeeper"),
            )
        )
        trk_sdf = trk_sdf.join(F.broadcast(matches_meta), on="player_id", how="left")
    elif provider == "gradientsports":
        trk_sdf = (
            spark.table(f"{catalog}.bronze.gradientsports_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_GRADIENTSPORTS_TRACKING_SELECT_COLS)
        )
    else:
        raise ValueError(f"Unknown tracking provider: {provider}")

    if period_filter is not None:
        trk_sdf = trk_sdf.filter(F.col("period") == period_filter)

    if trk_sdf.limit(1).count() == 0:
        task_logger.warning("No tracking data for %s match %s", provider, match_id)
        return 0

    # ── Read SPADL actions ──
    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for %s match %s", provider, match_id)
        return 0
    actions_records: list[dict[str, Any]] = actions_pdf.to_dict("records")  # type: ignore[assignment]

    # ── Resolve match-level metadata (driver scalars) ──
    home_start_left = True
    gs_team_side_to_id: dict[str, str] | None = None
    gs_jersey_to_player_id: dict[tuple[str, str], str] | None = None
    gs_gk_player_ids: list[str] | None = None

    if provider == "idsse":
        from ingestion.spadl_adapter import (
            adapt_idsse_events_for_silly_kicks,
            derive_idsse_home_team_start_left,
        )

        events_pdf = spark.table(f"{catalog}.bronze.idsse_events").filter(F.col("match_id") == match_id).toPandas()
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = adapt_idsse_events_for_silly_kicks(events_pdf)
        home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)
        del events_pdf, adapted_events
    elif provider == "metrica":
        home_team_id = "Home"
    elif provider == "skillcorner":
        row = (
            spark.table(f"{catalog}.bronze.skillcorner_matches")
            .filter(F.col("match_id") == match_id)
            .select("home_team_id")
            .limit(1)
            .collect()[0]
        )
        home_team_id = str(row["home_team_id"])
    elif provider == "gradientsports":
        from ingestion.spadl_adapter import extract_gradientsports_match_metadata

        gs_events_tbl = f"{catalog}.bronze.gradientsports_events"
        events_pdf = spark.table(gs_events_tbl).filter(F.col("match_id") == match_id).toPandas()
        gs_meta = extract_gradientsports_match_metadata(events_pdf)
        home_team_id = str(gs_meta["home_team_id"])
        home_start_left = gs_meta["home_team_start_left"]
        del events_pdf

        # Build team_side -> team_id mapping and jersey -> player_id from roster
        gs_roster_tbl = f"{catalog}.bronze.gradientsports_roster"
        roster_pdf = spark.table(gs_roster_tbl).filter(F.col("match_id") == match_id).toPandas()
        if not roster_pdf.empty:
            # Derive away_team_id from roster (the team that is not home)
            all_team_ids = roster_pdf["team_id"].dropna().unique()
            home_tid = str(gs_meta["home_team_id"])
            away_tids = [str(t) for t in all_team_ids if str(t) != home_tid]
            away_team_id = away_tids[0] if away_tids else home_tid

            gs_team_side_to_id = {"home": home_tid, "away": away_team_id}

            # Build (team_side, jersey_num) -> player_id mapping
            gs_jersey_to_player_id = {}
            for _, row in roster_pdf.iterrows():
                tid = str(row.get("team_id", ""))
                side = "home" if tid == home_tid else "away"
                jersey = str(row.get("jersey_number", ""))
                pid = str(row.get("player_id", ""))
                if jersey and pid:
                    gs_jersey_to_player_id[(side, jersey)] = pid

            # GK player IDs
            if "position" in roster_pdf.columns:
                gk_rows = roster_pdf[roster_pdf["position"].str.upper() == "GK"]
            else:
                gk_rows = roster_pdf.iloc[0:0]
            gs_gk_player_ids = [str(r["player_id"]) for _, r in gk_rows.iterrows()]

        del roster_pdf

    # ── Frame batching + UDF dispatch ──
    # Use "frame" for most providers; GradientSports uses "frame_num"
    frame_col = "frame_num" if provider == "gradientsports" else "frame"
    trk_sdf = trk_sdf.withColumn(
        "frame_batch_id",
        F.floor(F.col(frame_col) / F.lit(_FRAME_BATCH_SIZE)),
    )

    # GradientSports uses "period_elapsed_time" as timestamp, rename for consistency
    if provider == "gradientsports":
        trk_sdf = trk_sdf.withColumnRenamed("period_elapsed_time", "timestamp")

    udf_fn = _make_action_context_udf(
        provider=provider,
        home_team_id=home_team_id,
        home_start_left=home_start_left,
        xt_grid_data=xt_grid_data,
        xt_l=xt_l,
        xt_w=xt_w,
        actions_records=actions_records,
        native_match_id=match_id,
        gs_team_side_to_id=gs_team_side_to_id,
        gs_jersey_to_player_id=gs_jersey_to_player_id,
        gs_gk_player_ids=gs_gk_player_ids,
    )

    # GradientSports uses "period" (not "period_id") in bronze
    result_sdf = trk_sdf.groupBy("match_id", "period", "frame_batch_id").applyInPandas(
        udf_fn,
        schema=_get_result_schema(),
    )

    if period_filter is not None:
        rw = f"match_id = '{match_id}' AND period_id = {period_filter}"
    else:
        rw = f"match_id = '{match_id}'"

    written = write_delta_table(
        result_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=rw,
        logger=task_logger,
    )
    del actions_pdf, actions_records
    return written


def _process_statsbomb_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    match_id: str,
    task_logger: logging.Logger,
) -> int:
    """Process a StatsBomb match — SB360 tier (with freeze-frames) or event-only."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    # Read SPADL actions
    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "statsbomb"))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for statsbomb match %s", match_id)
        return 0

    # Check for SB360 freeze-frame data
    from ingestion.utils import tolerate_missing_table

    has_360 = False
    with tolerate_missing_table(task_logger, "statsbomb_360 table missing"):
        count_360 = (
            spark.table(f"{catalog}.bronze.statsbomb_360").filter(F.col("match_id") == match_id).limit(1).count()
        )
        has_360 = count_360 > 0

    if has_360:
        task_logger.info("StatsBomb match %s has 360 data — using SB360 tier", match_id)
        result_pdf = _run_sb360_enrichment(
            spark,
            catalog,
            actions_pdf,
            match_id,
            task_logger,
        )
    else:
        task_logger.info("StatsBomb match %s — event-only tier", match_id)
        result_pdf = _enrich_event_only_match(actions_pdf)

    out_pdf = _build_output(result_pdf, match_id_native=match_id, data_source="statsbomb")

    # Convert to Spark DataFrame and write
    out_sdf = spark.createDataFrame(out_pdf)
    written = write_delta_table(
        out_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id = '{match_id}'",
        logger=task_logger,
        row_count=len(out_pdf),
    )
    return written


def _run_sb360_enrichment(
    spark: SparkSession,
    catalog: str,
    actions_pdf: pd.DataFrame,
    match_id: str,
    task_logger: logging.Logger,
) -> pd.DataFrame:
    """Run SB360 enrichment — converts freeze-frames to synthetic tracking then enriches."""
    import pandas as pd
    from pyspark.sql import functions as F  # noqa: N812

    # Read SB360 freeze-frame data
    sb360_pdf = spark.table(f"{catalog}.bronze.statsbomb_360").filter(F.col("match_id") == match_id).toPandas()

    if sb360_pdf.empty:
        task_logger.warning("SB360 data empty for match %s — falling back to event-only", match_id)
        return _enrich_event_only_match(actions_pdf)

    # Map event_uuid → action_id via original_event_id in SPADL actions
    # SB360.id = event_uuid; spadl_actions.original_event_id = event_uuid
    _event_ids = actions_pdf["original_event_id"].dropna()
    _action_ids = actions_pdf.loc[_event_ids.index, "action_id"]
    event_to_action = dict(zip(_event_ids, _action_ids, strict=True))

    # Pre-build indexed lookups — avoids O(n*m) boolean mask filtering in loop.
    action_to_team: dict[Any, str] = dict(
        zip(actions_pdf["action_id"], actions_pdf["team_id"].astype(str), strict=False)
    )
    all_teams = [str(t) for t in actions_pdf["team_id"].dropna().unique()]

    # Build snapshot format: action_id, team_id, is_goalkeeper, x, y
    # SB360 has: id (event_uuid), teammate (bool), actor (bool), keeper (bool), location (JSON [x,y])
    import json

    snapshots: list[dict[str, Any]] = []
    for _, row in sb360_pdf.iterrows():
        event_uuid = str(row.get("id", ""))
        action_id = event_to_action.get(event_uuid)
        if action_id is None:
            continue

        # Resolve team_id from teammate flag + acting team (dict lookup, O(1))
        acting_team_id = action_to_team.get(action_id)
        if acting_team_id is None:
            continue

        opponent_teams = [t for t in all_teams if t != acting_team_id]
        opponent_team_id = opponent_teams[0] if opponent_teams else acting_team_id

        is_teammate = bool(row.get("teammate", False))
        team_id = acting_team_id if is_teammate else opponent_team_id
        is_gk = bool(row.get("keeper", False))

        # Parse location
        loc = row.get("location")
        if loc is None:
            continue
        if isinstance(loc, str):
            try:
                loc = json.loads(loc)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(loc, (list, tuple)) or len(loc) < 2:
            continue

        snapshots.append(
            {
                "action_id": int(action_id),
                "team_id": team_id,
                "is_goalkeeper": is_gk,
                "x": float(loc[0]),
                "y": float(loc[1]),
            }
        )

    if not snapshots:
        task_logger.warning("No valid snapshots for match %s — falling back to event-only", match_id)
        return _enrich_event_only_match(actions_pdf)

    freeze_frames = pd.DataFrame(snapshots)

    # Derive home_team_id for SB360 (use first team in actions as home approximation)
    unique_teams = actions_pdf["team_id"].dropna().unique()
    home_team_id = str(unique_teams[0]) if len(unique_teams) > 0 else "unknown"

    return _enrich_sb360_match(actions_pdf, freeze_frames, home_team_id)


def _process_event_only_match(
    spark: SparkSession,
    catalog: str,
    schema: str,
    provider: str,
    match_id: str,
    task_logger: logging.Logger,
) -> int:
    """Process a pure event-only match (no tracking data)."""
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    actions_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
        .toPandas()
    )
    if actions_pdf.empty:
        task_logger.warning("No SPADL actions for %s match %s", provider, match_id)
        return 0

    result_pdf = _enrich_event_only_match(actions_pdf)
    out_pdf = _build_output(result_pdf, match_id_native=match_id, data_source=provider)

    out_sdf = spark.createDataFrame(out_pdf)
    written = write_delta_table(
        out_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"match_id = '{match_id}'",
        logger=task_logger,
        row_count=len(out_pdf),
    )
    return written


if __name__ == "__main__":
    main()
