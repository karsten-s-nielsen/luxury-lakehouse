"""TC-1 — Unified action-coupled tracking features pipeline.

Reads tracking data + SPADL actions from bronze, runs all silly-kicks
enrichments in a single applyInPandas pass per match, writes results to
bronze.spadl_tracking_context.

Providers: IDSSE (Sportec), Metrica, SkillCorner.
Architecture: "Read from bronze, compute, write to bronze."
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

import numpy as np

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession
    from silly_kicks.xthreat import ExpectedThreat

_TABLE_NAME = "spadl_tracking_context"

# ── Column projection constants ───────────────────────────────────────
# Minimum Spark .select() set per provider. Each tuple matches the
# corresponding _*_CONSUMED_COLS frozenset defined next to the converter
# function. Parity enforced by test_tracking_context_column_projection.py.
#
# NOTE: Spark filter columns (match_id for all providers) do NOT need to be
# in the select — Catalyst pushes predicates below projections.

_IDSSE_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "match_id",
    "period",
    "frame",
    "timestamp",
    "x",
    "y",
    "s",
    "ball_status",
    "frame_rate",
    "player_id",
    "team_id",
    "is_goalkeeper",
    "ball_x",
    "ball_y",
    "ball_z",
    "ball_s",
)

_METRICA_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "period",
    "frame",
    "timestamp",
    "frame_rate",
    "gk_jersey_numbers",
    "home_players",
    "away_players",
    "ball_x",
    "ball_y",
)

_SKILLCORNER_TRACKING_SELECT_COLS: tuple[str, ...] = (
    "frame",
    "period",
    "timestamp",
    "player_id",
    "team",
    "x",
    "y",
    "is_goalkeeper",
    "frame_rate",
    "home_team_id",
    "ball_x",
    "ball_y",
)

# ── Column ordering ────────────────────────────────────────────────────
# Identity (12) + linkage (4) + features (66) + audit (1) = 83 columns.

_RESULT_COLUMNS: list[str] = [
    # Identity
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
    # Linkage provenance
    "frame_id",
    "time_offset_seconds",
    "link_quality_score",
    "n_candidate_frames",
    # GK resolution (event-based)
    "defending_gk_player_id",
    "gk_was_distributing",
    "gk_was_engaged",
    "gk_actions_in_possession",
    # GK spatial (shot-only, from add_pre_shot_gk_context with frames)
    "pre_shot_gk_x",
    "pre_shot_gk_y",
    "pre_shot_gk_distance_to_goal",
    "pre_shot_gk_distance_to_shot",
    "pre_shot_gk_angle_to_shot_trajectory",
    "pre_shot_gk_angle_off_goal_line",
    # Action context
    "nearest_defender_distance",
    "actor_speed",
    "receiver_zone_density",
    "defenders_in_triangle_to_goal",
    # Actor pre-window (TF-3)
    "actor_arc_length_pre_window",
    "actor_displacement_pre_window",
    # Pressure (TF-2, all 3 methods)
    "pressure_on_actor__andrienko_oval",
    "pressure_on_actor__link_zones",
    "pressure_on_actor__bekkers_pi",
    # Pitch control (3 methods)
    "pitch_control_at_ball__spearman",
    "pitch_control_at_ball__fernandez_bornn",
    "pitch_control_at_ball__voronoi",
    # Defensive line
    "defensive_line_x",
    "back_line_high_x",
    "compactness_x",
    "lateral_width",
    "max_lateral_gap",
    "back_n_count",
    # Off-ball context (threshold line-break + runs)
    "line_break",
    "n_attackers_behind_line",
    "n_off_ball_runners_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    # Ward line-breaking
    "line_break__ward",
    "lines_broken__ward",
    "line_breaking_type__ward",
    # Team shape (14: 7 metrics x 2 teams)
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
    # DAS (nullable)
    "das_team",
    "das_opponent",
    "das_diff",
    # GK influence
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    # Cover shadows
    "n_blocked_receivers",
    "n_potential_receivers",
    "blocking_score",
    "blocked_threat_fraction",
    "max_single_defender_blocking_score",
    # Sync score
    "sync_score_min",
    "sync_score_mean",
    "sync_score_high_quality_frac",
    # Audit
    "_ingested_at",
]

_TRACKING_CONTEXT_DDL = (
    "data_source STRING, match_id STRING, action_id BIGINT, period_id BIGINT, "
    "time_seconds DOUBLE, team_id STRING, player_id STRING, type_name STRING, "
    "start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    "frame_id BIGINT, time_offset_seconds DOUBLE, link_quality_score DOUBLE, "
    "n_candidate_frames BIGINT, "
    "defending_gk_player_id DOUBLE, gk_was_distributing BOOLEAN, "
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
    "_ingested_at TIMESTAMP"
)


# ── Enrichment chain ──────────────────────────────────────────────────


def _enrich_match(
    *,
    actions: pd.DataFrame,
    frames: pd.DataFrame,
    xt: ExpectedThreat,
    home_team_id: int | str,
    match_id_native: str,
    data_source: str,
) -> pd.DataFrame:
    """Run the full silly-kicks enrichment chain for one match.

    Args:
        actions: SPADL actions with game_id column (silly-kicks convention).
        frames: Tracking frames in TRACKING_FRAMES_COLUMNS schema (105x68 LTR).
        xt: Fitted ExpectedThreat model.
        home_team_id: Home team identifier for directional features.
        match_id_native: Native match ID string for the output.
        data_source: Provider name (idsse, metrica, skillcorner).

    Returns:
        DataFrame with all _RESULT_COLUMNS except _ingested_at.
    """

    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_actor_pre_window,
        add_cover_shadows,
        add_das,
        add_defensive_line,
        add_gk_influence,
        add_line_break,
        add_off_ball_context,
        add_pressure_on_actor,
        add_sync_score,
        add_team_shape,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    # Step 0: Link actions to frames (keep links aside for sync_score)
    links, _report = link_actions_to_frames(actions, frames)

    # Step 1: GK resolution (events + tracking)
    actions = add_pre_shot_gk_context(actions, frames=frames)

    # Step 2: Action context (provenance skip guard in 3.11.2+)
    actions = add_action_context(actions, frames)

    # Step 3: Actor pre-window (TF-3)
    actions = add_actor_pre_window(actions, frames)

    # Step 4: Pressure (TF-2, all 3 methods)
    actions = add_pressure_on_actor(
        actions,
        frames,
        methods=("andrienko_oval", "link_zones", "bekkers_pi"),
    )

    # Steps 5-7: Pitch control (3 methods, using Series API to avoid 3x copies)
    # TODO: TC-2 — pre-link once, pass linked frames to avoid ~14x redundant
    # link_actions_to_frames calls. Every aggregator (steps 1-14) re-links
    # internally (~2-5s each on 3000 actions x 150k frames = 30-70s/match).
    # At 20 matches, that's 10-20 min of pure overhead. Accepted for v1;
    # silly-kicks upstream optimization to expose a pointers kwarg is tracked.
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_action(actions, frames, method=method)
        actions[s.name] = s.values

    # Step 8: Defensive line
    actions = add_defensive_line(actions, frames, home_team_id=home_team_id)

    # Step 9: Off-ball context (threshold line-break + 4 off-ball-run columns)
    # NOTE (M1): add_off_ball_context is an umbrella that ALSO adds the threshold
    # line_break + n_attackers_behind_line columns. Step 10 (add_line_break with
    # method="ward") is separate and adds the Ward-specific columns.
    actions = add_off_ball_context(actions, frames, home_team_id=home_team_id)

    # Step 10: Ward line-breaking
    actions = add_line_break(actions, frames, method="ward", home_team_id=home_team_id)

    # Step 11: Team shape
    actions = add_team_shape(actions, frames, home_team_id=home_team_id)

    # Step 12: DAS (defensive wrapper — accessible-space can IndexError)
    try:
        actions = add_das(actions, frames)
    except Exception:  # noqa: BLE001 — accessible-space IndexError on edge-case frames
        actions["das_team"] = actions["das_opponent"] = actions["das_diff"] = np.nan

    # Step 13: GK influence
    actions = add_gk_influence(actions, frames, xt, home_team_id=home_team_id)

    # Step 14: Cover shadows
    actions = add_cover_shadows(actions, frames, xt, home_team_id=home_team_id)

    # Step 15: Sync score
    actions = add_sync_score(actions, links)

    # ── Build output ───────────────────────────────────────────────────
    # Rename game_id → match_id (silly-kicks uses game_id, we use match_id)
    out = actions.copy()
    out["match_id"] = match_id_native
    out["data_source"] = data_source

    # Map silly-kicks type_id → type_name
    # NOTE: ACTION_TYPES does not exist in silly_kicks.spadl.schema.
    # The correct import is actiontypes from silly_kicks.spadl.config.
    if "type_name" not in out.columns and "type_id" in out.columns:
        from silly_kicks.spadl.config import actiontypes

        type_map = {i: name for i, name in enumerate(actiontypes)}
        out["type_name"] = out["type_id"].map(type_map)

    # Cast team_id and player_id to string (output schema is STRING)
    out["team_id"] = out["team_id"].astype(str)
    out["player_id"] = out["player_id"].astype(str)

    # Select and order output columns (excluding _ingested_at — added by write_delta_table)
    output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    for col in output_cols:
        if col not in out.columns:
            out[col] = np.nan
    return out[output_cols].copy()


# ── Provider processing ────────────────────────────────────────────────


def _process_idsse(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: ExpectedThreat,
    new_ids: list[str],
) -> int:
    """Process IDSSE matches via sportec.convert_to_frames from bronze."""
    import gc

    from pyspark.sql import functions as F  # noqa: N812
    from silly_kicks.tracking import PreprocessConfig
    from silly_kicks.tracking.sportec import convert_to_frames

    from ingestion.spadl_adapter import (
        adapt_idsse_events_for_silly_kicks,
        derive_idsse_home_team_start_left,
    )
    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing IDSSE match %s", match_id)

        # Load tracking frames — project only consumed columns
        trk_pdf = (
            spark.table(f"{catalog}.bronze.idsse_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_IDSSE_TRACKING_SELECT_COLS)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for IDSSE match %s", match_id)
            continue

        # Load SPADL actions from bronze
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "idsse"))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for IDSSE match %s", match_id)
            continue

        # Derive home team info from bronze events
        events_pdf = spark.table(f"{catalog}.bronze.idsse_events").filter(F.col("match_id") == match_id).toPandas()
        home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
        adapted_events = adapt_idsse_events_for_silly_kicks(events_pdf)
        home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)
        del events_pdf, adapted_events

        # Map bronze columns to sportec EXPECTED_INPUT_COLUMNS schema
        sportec_input = _bronze_idsse_to_sportec_input(trk_pdf)
        del trk_pdf

        # Convert tracking to silly-kicks frames (105x68 LTR)
        frames, _report = convert_to_frames(
            sportec_input,
            home_team_id=home_team_id,
            home_team_start_left=home_start_left,
            output_convention="ltr",
            preprocess=PreprocessConfig(derive_velocity=True),
        )
        del sportec_input

        # Align game_id: sportec converter uses DFL string ID, but SPADL
        # actions carry a BIGINT hash.
        frames["game_id"] = int(actions_pdf["game_id"].iloc[0])

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="idsse",
        )
        del frames, actions_pdf

        result_sdf = spark.createDataFrame(result)
        del result
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written
        gc.collect()

    return total


# ── Bronze → silly-kicks frames helpers ──────────────────────────────


def _derive_velocities_savgol(
    frames: pd.DataFrame,
    provider: str,
    frame_rate: int,
) -> None:
    """Derive vx/vy/speed via Savitzky-Golay smoothed differentiation (in-place).

    NOTE: silly-kicks uses a two-pass pipeline (smooth_frames → derive_velocities
    on smoothed positions). This helper applies a single SG derivative pass on raw
    positions — numerically slightly noisier but practically equivalent for
    well-formed data. Acceptable for v1; align with two-pass if velocity quality
    proves insufficient on SkillCorner 10fps data.

    Uses silly-kicks per-provider defaults from _provider_defaults_generated.py:
    - Metrica:     sg_window_seconds=0.4, sg_poly_order=3 → window=11 at 25fps
    - SkillCorner: sg_window_seconds=1.0, sg_poly_order=3 → window=11 at 10fps
    - Sportec:     sg_window_seconds=0.4, sg_poly_order=3 → window=11 at 25fps

    Ball velocity IS derived (silly-kicks groups by [period_id, is_ball, player_id]).

    Args:
        frames: Must have columns [player_id, is_ball, x, y] sorted by time
                within each player/ball group.
        provider: "metrica" or "skillcorner" — selects SG parameters.
        frame_rate: Tracking data frame rate (Hz).
    """
    from scipy.signal import savgol_filter

    # Per-provider SG defaults matching silly-kicks _provider_defaults_generated.py
    _sg_defaults: dict[str, tuple[float, int]] = {
        "metrica": (0.4, 3),  # sg_window_seconds, sg_poly_order
        "skillcorner": (1.0, 3),
        "sportec": (0.4, 3),  # IDSSE uses convert_to_frames, but fallback
    }
    sg_window_s, polyorder = _sg_defaults.get(provider, (0.4, 3))

    dt = 1.0 / frame_rate
    window = max(round(sg_window_s * frame_rate) | 1, polyorder + 2)
    if window % 2 == 0:
        window += 1

    # Initialize with NaN (not 0.0 — 0.0 implies stationary, NaN implies unknown)
    frames["vx"] = np.nan
    frames["vy"] = np.nan

    # Group by (period_id, is_ball, player_id) — matching silly-kicks pipeline.
    # Ball rows ARE processed (pid=None, is_ball=True).
    for _key, idx in frames.groupby(["period_id", "is_ball", "player_id"]).groups.items():
        group = frames.loc[idx]
        x_raw = group["x"].values.astype(float)
        y_raw = group["y"].values.astype(float)
        nan_mask = np.isnan(x_raw) | np.isnan(y_raw)

        if nan_mask.all():
            continue

        # Short groups: np.gradient fallback (matches silly-kicks _velocity.py)
        if len(group) < window:
            x_safe = np.where(nan_mask, 0.0, x_raw)
            y_safe = np.where(nan_mask, 0.0, y_raw)
            vx_g = np.gradient(x_safe, dt)
            vy_g = np.gradient(y_safe, dt)
            vx_g[nan_mask] = np.nan
            vy_g[nan_mask] = np.nan
            frames.loc[idx, "vx"] = vx_g
            frames.loc[idx, "vy"] = vy_g
            continue

        # Interpolate NaN positions before SG filtering (linear interp across gaps),
        # then re-mask original NaN positions back to NaN in the output.
        # Matches silly-kicks derive_velocities (_velocity.py:84-124).
        valid_idx = np.where(~nan_mask)[0]
        x_filled = np.interp(np.arange(len(group)), valid_idx, x_raw[~nan_mask])
        y_filled = np.interp(np.arange(len(group)), valid_idx, y_raw[~nan_mask])

        vx_g = savgol_filter(x_filled, window, polyorder, deriv=1, delta=dt)
        vy_g = savgol_filter(y_filled, window, polyorder, deriv=1, delta=dt)
        vx_g[nan_mask] = np.nan
        vy_g[nan_mask] = np.nan

        frames.loc[idx, "vx"] = vx_g
        frames.loc[idx, "vy"] = vy_g

    # Compute speed from velocity (matches silly-kicks derive_velocities output)
    frames["speed"] = np.sqrt(frames["vx"] ** 2 + frames["vy"] ** 2)


_IDSSE_CONSUMED_COLS: frozenset[str] = frozenset(
    {
        "match_id",
        "period",
        "frame",
        "timestamp",
        "x",
        "y",
        "s",
        "ball_status",
        "frame_rate",
        "player_id",
        "team_id",
        "is_goalkeeper",
        "ball_x",
        "ball_y",
        "ball_z",
        "ball_s",
    }
)
"""Columns consumed by _bronze_idsse_to_sportec_input from bronze.idsse_tracking."""


def _bronze_idsse_to_sportec_input(trk_pdf: pd.DataFrame) -> pd.DataFrame:
    """Map bronze ``idsse_tracking`` columns to silly-kicks sportec input schema.

    Bronze ``idsse_tracking`` stores one row per player per frame with ball
    data denormalized as ``ball_x``/``ball_y``/``ball_z``/``ball_status``
    columns on every player row.  ``convert_to_frames`` expects the sportec
    ``EXPECTED_INPUT_COLUMNS`` schema which includes separate ball rows
    (``is_ball=True``, ``player_id=NaN``, ``team_id=NaN``).

    Column mapping (bronze → sportec input):

    +--------------+--------------+--------------------------------------+
    | Bronze       | Sportec      | Notes                                |
    +--------------+--------------+--------------------------------------+
    | match_id     | game_id      | rename                               |
    | period       | period_id    | rename                               |
    | frame        | frame_id     | rename                               |
    | timestamp    | time_seconds | rename                               |
    | x            | x_centered   | already DFL-centered (±52.5)         |
    | y            | y_centered   | already DFL-centered (±34.0)         |
    | s            | speed_native | rename                               |
    | ball_status  | ball_state   | lowercase (``Alive`` → ``alive``)    |
    | frame_rate   | frame_rate   | identity                             |
    | player_id    | player_id    | identity                             |
    | team_id      | team_id      | identity                             |
    | is_goalkeeper| is_goalkeeper| identity                             |
    +--------------+--------------+--------------------------------------+

    Synthetic ball rows are created by deduplicating
    ``(frame, period)`` and pivoting ``ball_x``/``ball_y``/``ball_z``
    into ``x_centered``/``y_centered``/``z``.  Player rows get
    ``z=NaN`` (DFL does not provide z for non-ball objects).
    """
    import pandas as pd

    # Filter to consumed columns — runtime assertion against drift.
    trk_pdf = trk_pdf[list(_IDSSE_CONSUMED_COLS)].copy()

    # ── Player rows ──────────────────────────────────────────────
    players = trk_pdf.rename(
        columns={
            "match_id": "game_id",
            "period": "period_id",
            "frame": "frame_id",
            "timestamp": "time_seconds",
            "x": "x_centered",
            "y": "y_centered",
            "s": "speed_native",
            "ball_status": "ball_state",
        },
    ).copy()
    players["is_ball"] = False
    players["z"] = np.nan

    # ball_state: DFL uses capitalized "Alive"/"Dead"; sportec expects lowercase
    bs = players["ball_state"]
    players["ball_state"] = bs.str.lower().where(bs.notna(), other=None)

    # ── Synthetic ball rows (one per frame) ──────────────────────
    ball_src = trk_pdf[
        [
            "frame",
            "period",
            "timestamp",
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_s",
            "ball_status",
            "match_id",
            "frame_rate",
        ]
    ].copy()
    ball_src = ball_src.drop_duplicates(subset=["frame", "period"])
    ball_src.rename(
        columns={
            "match_id": "game_id",
            "frame": "frame_id",
            "period": "period_id",
            "timestamp": "time_seconds",
            "ball_x": "x_centered",
            "ball_y": "y_centered",
            "ball_z": "z",
            "ball_s": "speed_native",
            "ball_status": "ball_state",
        },
        inplace=True,
    )
    bs_ball = ball_src["ball_state"]
    ball_src["ball_state"] = bs_ball.str.lower().where(bs_ball.notna(), other=None)
    ball_src["player_id"] = None
    ball_src["team_id"] = None
    ball_src["is_ball"] = True
    ball_src["is_goalkeeper"] = False

    # ── Combine and select only EXPECTED_INPUT_COLUMNS ───────────
    expected_cols = [
        "game_id",
        "period_id",
        "frame_id",
        "time_seconds",
        "frame_rate",
        "player_id",
        "team_id",
        "is_ball",
        "is_goalkeeper",
        "x_centered",
        "y_centered",
        "z",
        "speed_native",
        "ball_state",
    ]
    result = pd.concat(
        [players[expected_cols], ball_src[expected_cols]],
        ignore_index=True,
    )
    return result.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


_METRICA_CONSUMED_COLS: frozenset[str] = frozenset(
    {
        "period",
        "frame",
        "timestamp",
        "frame_rate",
        "gk_jersey_numbers",
        "home_players",
        "away_players",
        "ball_x",
        "ball_y",
    }
)
"""Columns consumed by _bronze_metrica_to_frames from bronze.metrica_tracking."""


def _bronze_metrica_to_frames(trk_pdf: pd.DataFrame, game_id: int) -> pd.DataFrame:
    """Convert Metrica bronze tracking (frame-level JSON) to silly-kicks frames.

    Bronze schema: period, frame, timestamp, ball_x, ball_y,
    home_players (JSON), away_players (JSON), gk_jersey_numbers (JSON),
    pitch_length_m, pitch_width_m, frame_rate.

    COORDINATE CONVERSION: Metrica 0-1 normalized → SPADL 105x68 meters.
    - x_spadl = x_01 * 105.0
    - y_spadl = (1 - y_01) * 68.0  (Metrica y-axis is flipped: 0=top, 1=bottom)

    Do NOT use metrica_to_statsbomb() — that produces 120x80 StatsBomb yards,
    not 105x68 SPADL meters. silly-kicks TRACKING_CONSTRAINTS require (0,105)x(0,68).
    """
    import json

    import pandas as pd

    # Filter to consumed columns — runtime assertion against drift.
    trk_pdf = trk_pdf[list(_METRICA_CONSUMED_COLS)].copy()

    # Parse GK jersey numbers (match-level constant)
    gk_jerseys: set[str] = set()
    if "gk_jersey_numbers" in trk_pdf.columns:
        gk_raw = trk_pdf["gk_jersey_numbers"].dropna()
        if not gk_raw.empty:
            parsed = json.loads(gk_raw.iloc[0]) if isinstance(gk_raw.iloc[0], str) else gk_raw.iloc[0]
            gk_jerseys = {str(j) for j in parsed} if parsed else set()

    frame_rate = int(trk_pdf["frame_rate"].iloc[0]) if "frame_rate" in trk_pdf.columns else 25

    rows: list[dict] = []
    for _, row in trk_pdf.iterrows():
        # Skip rows with NaN period (e.g. pre-match warmup data)
        if pd.isna(row["period"]):
            continue
        fid = int(row["frame"])
        pid = int(row["period"])
        t = float(row["timestamp"])

        # Home and away player rows from JSON
        for team_label, json_col in [("Home", "home_players"), ("Away", "away_players")]:
            raw = row.get(json_col)
            if pd.isna(raw) or raw is None:
                continue
            players = json.loads(raw) if isinstance(raw, str) else raw
            for jersey, coords in players.items():
                if isinstance(coords, dict) and "x" in coords and "y" in coords:
                    # Direct Metrica 0-1 → SPADL 105x68 (NOT StatsBomb 120x80)
                    x_spadl = float(coords["x"]) * 105.0
                    y_spadl = (1.0 - float(coords["y"])) * 68.0
                    rows.append(
                        {
                            "game_id": game_id,
                            "frame_id": fid,
                            "period_id": pid,
                            "time_seconds": t,
                            "player_id": f"{team_label}_{jersey}",
                            "team_id": team_label,
                            "x": x_spadl,
                            "y": y_spadl,
                            "is_goalkeeper": str(jersey) in gk_jerseys,
                            "is_ball": False,
                        }
                    )

        # Ball row
        bx, by = row.get("ball_x"), row.get("ball_y")
        if not pd.isna(bx) and not pd.isna(by):
            rows.append(
                {
                    "game_id": game_id,
                    "frame_id": fid,
                    "period_id": pid,
                    "time_seconds": t,
                    "player_id": None,
                    "team_id": None,
                    "x": float(bx) * 105.0,
                    "y": (1.0 - float(by)) * 68.0,
                    "is_goalkeeper": False,
                    "is_ball": True,
                }
            )

    frames = pd.DataFrame(rows)

    # ── Add all required TRACKING_FRAMES_COLUMNS ────────────────────
    # link_actions_to_frames hard-selects source_provider → KeyError without it.
    frames["source_provider"] = "metrica"
    frames["is_goalkeeper_source"] = "native"
    frames["frame_rate"] = float(frame_rate)
    frames["z"] = np.nan
    frames["speed_source"] = "derived"
    frames["ball_state"] = None  # Metrica bronze doesn't carry ball state
    frames["team_attacking_direction"] = None
    frames["confidence"] = None
    frames["visibility"] = None

    # Sort by player then frame for velocity derivation
    frames = frames.sort_values(["player_id", "frame_id"]).reset_index(drop=True)
    # Savitzky-Golay velocity + speed (matches silly-kicks PreprocessConfig)
    _derive_velocities_savgol(frames, provider="metrica", frame_rate=frame_rate)
    return frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


_SKILLCORNER_CONSUMED_COLS: frozenset[str] = frozenset(
    {
        "frame",
        "period",
        "timestamp",
        "player_id",
        "team",
        "x",
        "y",
        "is_goalkeeper",
        "frame_rate",
        "ball_x",
        "ball_y",
    }
)
"""Columns consumed by _bronze_skillcorner_to_frames from bronze.skillcorner_tracking.

NOTE: ``home_team_id`` is consumed by ``_process_skillcorner`` (not the converter),
so it appears in the projection constant but NOT here.
"""


def _bronze_skillcorner_to_frames(trk_pdf: pd.DataFrame, game_id: int) -> pd.DataFrame:
    """Convert SkillCorner bronze tracking (narrow) to silly-kicks frames.

    Bronze schema (narrow, one row per player per frame):
    period, frame, timestamp, player_id, team, x, y, ball_x, ball_y,
    ball_z, is_goalkeeper, home_team_id, away_team_id, frame_rate.

    COORDINATE CONVERSION: center-origin meters → SPADL 105x68 meters.
    - x_spadl = x_center + 52.5
    - y_spadl = y_center + 34.0

    Do NOT use center_m_to_statsbomb() — that produces 120x80 StatsBomb yards,
    not 105x68 SPADL meters. silly-kicks TRACKING_CONSTRAINTS require (0,105)x(0,68).
    """
    import pandas as pd

    # Filter to consumed columns — runtime assertion against drift.
    trk_pdf = trk_pdf[list(_SKILLCORNER_CONSUMED_COLS)].copy()

    frame_rate = int(trk_pdf["frame_rate"].iloc[0]) if "frame_rate" in trk_pdf.columns else 10

    # Player rows — rename to match TRACKING_FRAMES_COLUMNS
    players = trk_pdf[["frame", "period", "timestamp", "player_id", "team", "x", "y", "is_goalkeeper"]].copy()
    players.rename(
        columns={
            "frame": "frame_id",
            "period": "period_id",
            "timestamp": "time_seconds",
            "team": "team_id",
        },
        inplace=True,
    )
    # Direct center-origin meters → SPADL 105x68 (NOT StatsBomb 120x80)
    players["x"] = players["x"] + 52.5
    players["y"] = players["y"] + 34.0
    players["is_ball"] = False
    players["game_id"] = game_id

    # Ball rows — deduplicate (ball_x/ball_y are on every player row)
    ball_src = trk_pdf[["frame", "period", "timestamp", "ball_x", "ball_y"]].copy()
    ball_src = ball_src.drop_duplicates(subset=["frame", "period"])
    ball_src.rename(
        columns={
            "frame": "frame_id",
            "period": "period_id",
            "timestamp": "time_seconds",
            "ball_x": "x",
            "ball_y": "y",
        },
        inplace=True,
    )
    ball_src["x"] = ball_src["x"] + 52.5
    ball_src["y"] = ball_src["y"] + 34.0
    ball_src["player_id"] = None
    ball_src["team_id"] = None
    ball_src["is_goalkeeper"] = False
    ball_src["is_ball"] = True
    ball_src["game_id"] = game_id

    frames = pd.concat([players, ball_src], ignore_index=True)

    # ── Add all required TRACKING_FRAMES_COLUMNS ────────────────────
    # link_actions_to_frames hard-selects source_provider → KeyError without it.
    frames["source_provider"] = "skillcorner"
    frames["is_goalkeeper_source"] = "native"
    frames["frame_rate"] = float(frame_rate)
    frames["z"] = np.nan
    frames["speed_source"] = "derived"
    frames["ball_state"] = None
    frames["team_attacking_direction"] = None
    frames["confidence"] = None
    frames["visibility"] = None

    # Sort by player then frame for velocity derivation
    frames = frames.sort_values(["player_id", "frame_id"]).reset_index(drop=True)
    # Savitzky-Golay velocity + speed (matches silly-kicks PreprocessConfig)
    _derive_velocities_savgol(frames, provider="skillcorner", frame_rate=frame_rate)
    return frames.sort_values(["frame_id", "is_ball"]).reset_index(drop=True)


def _process_metrica(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: ExpectedThreat,
    new_ids: list[str],
) -> int:
    """Process Metrica matches from bronze tables (NOT from internet)."""
    import gc

    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing Metrica match %s", match_id)

        trk_pdf = (
            spark.table(f"{catalog}.bronze.metrica_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_METRICA_TRACKING_SELECT_COLS)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for Metrica match %s", match_id)
            continue

        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "metrica"))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for Metrica match %s", match_id)
            continue

        game_id = int(actions_pdf["game_id"].iloc[0])
        frames = _bronze_metrica_to_frames(trk_pdf, game_id=game_id)
        del trk_pdf

        home_team_id = "Home"

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="metrica",
        )
        del frames, actions_pdf

        result_sdf = spark.createDataFrame(result)
        del result
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written
        gc.collect()

    return total


def _process_skillcorner(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    xt: ExpectedThreat,
    new_ids: list[str],
) -> int:
    """Process SkillCorner matches from bronze tables (NOT from internet)."""
    import gc

    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    total = 0
    for match_id in new_ids:
        logger.info("Processing SkillCorner match %s", match_id)

        trk_pdf = (
            spark.table(f"{catalog}.bronze.skillcorner_tracking")
            .filter(F.col("match_id") == match_id)
            .select(*_SKILLCORNER_TRACKING_SELECT_COLS)
            .toPandas()
        )
        if trk_pdf.empty:
            logger.warning("No tracking data for SkillCorner match %s", match_id)
            continue

        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == "skillcorner"))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for SkillCorner match %s", match_id)
            continue

        # Derive home_team_id from projected bronze data BEFORE converter discards it
        home_team_id = str(trk_pdf["home_team_id"].dropna().iloc[0])

        game_id = int(actions_pdf["game_id"].iloc[0])
        frames = _bronze_skillcorner_to_frames(trk_pdf, game_id=game_id)
        del trk_pdf

        result = _enrich_match(
            actions=actions_pdf,
            frames=frames,
            xt=xt,
            home_team_id=home_team_id,
            match_id_native=match_id,
            data_source="skillcorner",
        )
        del frames, actions_pdf

        result_sdf = spark.createDataFrame(result)
        del result
        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total += written
        gc.collect()

    return total


# ── Skip Guard ─────────────────────────────────────────────────────────

_VALID_PROVIDERS: frozenset[str] = frozenset({"idsse", "metrica", "skillcorner"})


class _TrackingContextGuard:
    """SkipGuard adapter for tracking context pipeline.

    chunk_sizes: per-provider match count per for_each_task iteration.
    IDSSE = 1 match/iteration (~2.9 GB peak per match on 16 GB driver).
    Metrica/SkillCorner = 2 matches/iteration (lighter data).
    """

    workflow_id = "wf-tracking-context"
    chunk_sizes: ClassVar[dict[str, int]] = {"idsse": 1, "metrica": 2, "skillcorner": 2}

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check each provider's tracking table for unprocessed matches."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _TRACKING_CONTEXT_DDL)

        idsse_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.idsse_tracking",
            results_table,
            results_filter="data_source = 'idsse'",
        )
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

        total = len(idsse_ids) + len(metrica_ids) + len(skillcorner_ids)
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        # Build chunks: each inner list has one element "provider:id1,id2".
        # main_preflight joins with "," -> same string (single element).
        # for_each_task spawns one iteration per chunk string.
        chunks: list[list[str]] = []
        for provider, ids in [("idsse", idsse_ids), ("metrica", metrica_ids), ("skillcorner", skillcorner_ids)]:
            chunk_size = self.chunk_sizes[provider]
            for i in range(0, len(ids), chunk_size):
                batch = ids[i : i + chunk_size]
                chunks.append([f"{provider}:{','.join(batch)}"])

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            chunks=chunks,
            metadata={
                "idsse_ids": idsse_ids,
                "metrica_ids": metrica_ids,
                "skillcorner_ids": skillcorner_ids,
            },
        )


skip_guard = _TrackingContextGuard()


def _parse_tracking_match_ids_arg(raw: str | None) -> tuple[str, list[str]] | None:
    """Parse ``--match-ids`` CLI value for tracking context iterations.

    Format: ``"provider:id1,id2"`` (e.g. ``"idsse:J03WMX,J03WN1"``).
    The provider prefix routes to the correct ``_process_*`` function.

    Returns:
        ``(provider, [id1, id2])`` tuple, or ``None`` when ``raw`` is empty.

    Raises:
        SystemExit: On missing provider prefix or unknown provider.
    """
    if raw is None or raw == "":
        return None
    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2' format, got {raw!r}. Valid providers: {sorted(_VALID_PROVIDERS)}"
        )
    provider, ids_str = raw.split(":", 1)
    if provider not in _VALID_PROVIDERS:
        raise SystemExit(f"Unknown provider {provider!r} in --match-ids. Valid providers: {sorted(_VALID_PROVIDERS)}")
    ids = [mid.strip() for mid in ids_str.split(",") if mid.strip()]
    if not ids:
        return None
    return (provider, ids)


# ── Pipeline orchestration ─────────────────────────────────────────────


@workflow("wf-tracking-context", phase="enrichment")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Execute the tracking context enrichment pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from pyspark.sql import functions as F  # noqa: N812
    from silly_kicks.xthreat import ExpectedThreat

    # ── Driver-side setup ──────────────────────────────────────────────
    # Fit xT model on SPADL actions from tracking providers only (M2).
    # The xT grid converges quickly; restricting to tracking providers
    # keeps driver memory bounded as the lakehouse grows.
    spadl_pdf = (
        spark.table(f"{catalog}.bronze.spadl_actions")
        .filter(F.col("data_source").isin("idsse", "metrica", "skillcorner"))
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
    logger.info("xT model fitted on %d actions (grid shape %s)", len(spadl_pdf), xt.xT.shape)

    # Home team lookups for IDSSE (Metrica/SkillCorner resolve from bronze)
    idsse_ids = filter_result.metadata.get("idsse_ids", [])
    metrica_ids = filter_result.metadata.get("metrica_ids", [])
    skillcorner_ids = filter_result.metadata.get("skillcorner_ids", [])

    total_written = 0

    # Process each provider separately (different converter paths)
    if idsse_ids:
        rows = _process_idsse(spark, catalog, schema, logger, xt, idsse_ids)
        total_written += rows

    if metrica_ids:
        rows = _process_metrica(spark, catalog, schema, logger, xt, metrica_ids)
        total_written += rows

    if skillcorner_ids:
        rows = _process_skillcorner(spark, catalog, schema, logger, xt, skillcorner_ids)
        total_written += rows

    logger.info("Tracking context pipeline complete — %d total rows written", total_written)
    return total_written


# ── Entry points ──────────────────────────────────────────────────────


def _write_tracking_chunks_task_value(
    chunks_for_inputs: list[str],
    logger: logging.Logger,
) -> None:
    """Write discovered chunks as a Databricks task value.

    The downstream ``compute_tracking_context`` for_each_task reads this
    via ``"{{tasks.preflight_tracking_context.values.tracking_context_chunks}}"``.
    Empty list -> 0 iterations spawned.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            logger.warning("No active SparkSession -- task value not written")
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="tracking_context_chunks", value=chunks_for_inputs)
        logger.info(
            "Wrote task value 'tracking_context_chunks' (%d chunks)",
            len(chunks_for_inputs),
        )
    except (ImportError, AttributeError, RuntimeError) as exc:
        logger.warning("Task values not available (likely standalone mode) -- %s", exc)


def main_preflight() -> None:
    """CLI entry point for the tracking context preflight task.

    Runs the skip guard, partitions discovered matches into fan-out chunks
    (``provider:id1,id2`` format), and writes them as a Databricks task value
    for the downstream ``compute_tracking_context`` ``for_each_task``.
    """
    args = parse_ingestion_args(
        "Preflight: discover unprocessed tracking matches and emit chunks "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    logger = configure_logging("tracking_context_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    # Serialize each chunk as a single string (inner list is always length 1)
    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    logger.info(
        "Tracking context preflight: %d missing matches across %d chunks",
        fr.count,
        len(chunks_for_inputs),
    )

    _write_tracking_chunks_task_value(chunks_for_inputs, logger)


def main() -> None:
    """CLI entry point for tracking context enrichment.

    Without ``--match-ids``: runs all providers (legacy / standalone mode).
    With ``--match-ids "provider:id1,id2"``: processes only the specified
    provider and match IDs (for_each_task iteration mode).
    """
    args = parse_ingestion_args(
        "Compute action-coupled tracking features",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2 from for_each_task"})],
    )
    logger = configure_logging("tracking_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids_parsed = _parse_tracking_match_ids_arg(getattr(args, "match_ids", None))

    if match_ids_parsed is not None:
        # for_each_task iteration mode: process one provider's chunk
        provider, ids = match_ids_parsed
        logger.info("Iteration mode: provider=%s, match_ids=%s", provider, ids)

        # Fit xT per iteration (deterministic, ~2s, no accuracy impact).
        # Value iteration on zone transition counts — row-order-independent.
        from pyspark.sql import functions as F  # noqa: N812
        from silly_kicks.xthreat import ExpectedThreat

        spadl_pdf = (
            spark.table(f"{args.catalog}.bronze.spadl_actions")
            .filter(F.col("data_source").isin("idsse", "metrica", "skillcorner"))
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
        logger.info("xT model fitted (grid shape %s)", xt.xT.shape)

        if provider == "idsse":
            total = _process_idsse(spark, args.catalog, args.schema, logger, xt, ids)
        elif provider == "metrica":
            total = _process_metrica(spark, args.catalog, args.schema, logger, xt, ids)
        elif provider == "skillcorner":
            total = _process_skillcorner(spark, args.catalog, args.schema, logger, xt, ids)
        else:
            raise SystemExit(f"Unknown provider: {provider}")

        logger.info("Iteration complete -- %d rows written for %s", total, provider)
    else:
        # Legacy / standalone mode: run full pipeline
        filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)
        logger.info("Starting tracking context pipeline into %s.%s", args.catalog, args.schema)
        run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
