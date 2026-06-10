"""Result schema + output projection for action-context (pure pandas/numpy/silly_kicks).

Moved verbatim from ``ingestion.action_context`` (behavior-preserving). The
Spark ``StructType`` builder stays in the ingestion adapter layer (it imports
pyspark); this module is pure so it can run locally and on executors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd

# Identity (12) + game_state (1) + linkage (4) + GK (14) + features (76)
# + xShotOccurrence (1) + xT-GK (16) + gk_completion (1) + provenance (2) + audit (1) = 128
RESULT_COLUMNS: list[str] = [
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
    # GK influence (8)
    "gk_pitch_control_share_weighted",
    "gk_reachable_area_m2",
    "gk_closing_time_mean_s__six_yard_box",
    "gk_closing_time_min_s__six_yard_box",
    "gk_closing_time_mean_s__near_post",
    "gk_closing_time_min_s__near_post",
    "gk_closing_time_mean_s__far_post",
    "gk_closing_time_min_s__far_post",
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
    # Ghost-GK (3) — spread renamed to density_spread (silly-kicks 4.14.0; served value = boosted mean)
    "ghost_gk_x",
    "ghost_gk_y",
    "ghost_gk_density_spread",
    # Structural pass (TF-45; Karakus & Arkadas 2026, arXiv:2603.28916) (3)
    "structural_lbs",
    "structural_sgm",
    "structural_sdi",
    # Player influence (silly-kicks add_player_influence) (7)
    "actor_reachable_area_m2",
    "off_ball_xt_team",
    "off_ball_xt_opponent",
    "off_ball_xt_diff",
    "reachable_area_team",
    "reachable_area_opponent",
    "reachable_area_diff",
    # xCrossAttempt (silly-kicks 4.18.0 bundled public model) (1)
    "xcross_attempt",
    # xShotOccurrence (Pipping-Gamón, Feng & Sabin 2026; arXiv:2512.00203) (1)
    "xshot_occurrence",
    # xT-GK (Eyestone; silly-kicks 4.21.0/4.22.0, ADR-024 upstream) — GK-distribution
    # valuation. Composite stored per philosophy preset (default = `xt_gk`; the deck's named
    # presets as suffixed columns — δ/η enter the rav/temporal terms, so other presets are
    # NOT client-side derivable from the stored components). Components + provenance stored
    # once from the default-params run. (16)
    "xt_gk",
    "xt_gk_possession",
    "xt_gk_counter",
    "xt_gk_direct",
    "xt_gk_high_press",
    "xt_gk_low_block",
    "xt_gk_base",
    "xt_gk_pev",
    "xt_gk_rav",
    "xt_gk_dzv",
    "xt_gk_pressure",
    "xt_gk_origin_source",
    "xt_gk_dest_source",
    "xt_gk_origin_confidence",
    "xt_gk_completion_variant",
    "xt_gk_completion_source",
    # GK-distribution completion probability — the exact P(success) RAV consumes (1)
    "gk_completion",
    # Pitch-control provenance for the persisted pitch-control-derived metrics (1)
    "pitch_control_method",
    # Ghost-GK backend provenance — the resolved kde_backend per row (scopes to ghost_gk_* only) (1)
    "ghost_gk_method",
    # Audit (1)
    "_ingested_at",
]

ACTION_CONTEXT_DDL = (
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
    "gk_closing_time_mean_s__near_post DOUBLE, gk_closing_time_min_s__near_post DOUBLE, "
    "gk_closing_time_mean_s__far_post DOUBLE, gk_closing_time_min_s__far_post DOUBLE, "
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
    "ghost_gk_x DOUBLE, ghost_gk_y DOUBLE, ghost_gk_density_spread DOUBLE, "
    "structural_lbs BIGINT, structural_sgm DOUBLE, structural_sdi DOUBLE, "
    "actor_reachable_area_m2 DOUBLE, off_ball_xt_team DOUBLE, off_ball_xt_opponent DOUBLE, "
    "off_ball_xt_diff DOUBLE, reachable_area_team DOUBLE, reachable_area_opponent DOUBLE, "
    "reachable_area_diff DOUBLE, xcross_attempt DOUBLE, "
    "xshot_occurrence DOUBLE, "
    "xt_gk DOUBLE, xt_gk_possession DOUBLE, xt_gk_counter DOUBLE, xt_gk_direct DOUBLE, "
    "xt_gk_high_press DOUBLE, xt_gk_low_block DOUBLE, "
    "xt_gk_base DOUBLE, xt_gk_pev DOUBLE, xt_gk_rav DOUBLE, xt_gk_dzv DOUBLE, "
    "xt_gk_pressure DOUBLE, "
    "xt_gk_origin_source STRING, xt_gk_dest_source STRING, xt_gk_origin_confidence DOUBLE, "
    "xt_gk_completion_variant STRING, xt_gk_completion_source STRING, "
    "gk_completion DOUBLE, "
    "pitch_control_method STRING, ghost_gk_method STRING, "
    "_ingested_at TIMESTAMP"
)


def _restore_native_identity(actions: pd.DataFrame) -> pd.DataFrame:
    """Restore native IDs for output (dim table joins via staging layer)."""
    actions["team_id"] = actions["team_id_native"]
    actions["player_id"] = actions["player_id_native"]
    return actions


def _to_native_string(v: object) -> str | None:
    """Coerce a STRING-output cell to ``str`` (or ``None`` for null) — Arrow StringType-safe.

    A STRING-typed column can arrive holding non-string values: NaN where unresolved, OR numeric
    ids (e.g. statsbomb's integer player ids, which pandas stores as float64 once NaN-mixed) — so
    `spark.createDataFrame(schema=STRING)` raises BOTH `got float64` (all-numeric series) and
    `Expected bytes, got a 'float' object` (object series with a surviving float). Mapping nulls to
    None is NOT enough; real values must be stringified. Integral floats render without the ``.0``
    (5522.0 -> "5522") so the native id matches its integer-string form. The ``isinstance(v, float)``
    guards avoid pd.NA boolean-ambiguity on nullable dtypes.
    """
    if v is None or (isinstance(v, float) and v != v):  # None or NaN
        return None
    if isinstance(v, float) and v.is_integer():  # 5522.0 -> "5522", not "5522.0"
        return str(int(v))
    return str(v)


def _ddl_string_columns(ddl: str) -> frozenset[str]:
    """Column names declared ``STRING`` in ``ACTION_CONTEXT_DDL`` (the single source of truth).

    Drift-safe: any STRING column added to the DDL is automatically covered by build_output's
    object/None coercion below — no second list to keep in sync.
    """
    cols: set[str] = set()
    for field in ddl.split(","):
        parts = field.split()
        if len(parts) >= 2 and parts[1].upper() == "STRING":
            cols.add(parts[0])
    return frozenset(cols)


# STRING-typed output columns. Spark Connect's Arrow serializer cannot serialize a STRING column
# whose pandas values are non-string: an all-NULL float64 (`out[col] = np.nan`) raises
# `got float64`, and an object column holding numeric ids (statsbomb's integer player ids stored as
# float64 once NaN-mixed) raises `Expected bytes, got a 'float' object` — even WITH an explicit
# `schema=` (ADR-033). build_output therefore STRINGIFIES STRING columns (null -> None, numeric ->
# str via _to_native_string), the same single-source-of-truth coercion discipline as the
# GradientSports id fix (ADR-034). Bit statsbomb + wyscout event-only writes (ADR-033 amendment).
_STRING_OUTPUT_COLUMNS = _ddl_string_columns(ACTION_CONTEXT_DDL)


def build_output(actions: pd.DataFrame, match_id_native: str, data_source: str) -> pd.DataFrame:
    """Post-enrichment: renames + column selection for bronze write.

    1. game_id -> match_id (silly-kicks uses game_id, we use match_id)
    2. defending_gk_player_id -> defending_gk_player_id_native (ADR-018)
    3. type_id -> type_name via silly-kicks add_names
    4. Column selection to RESULT_COLUMNS, dtype-correct fill for missing cols (numeric -> NaN,
       STRING -> object/None so the explicit-schema Arrow write never sees float64-vs-StringType).
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

    output_cols = [c for c in RESULT_COLUMNS if c != "_ingested_at"]
    missing_cols = [c for c in output_cols if c not in out.columns]
    if missing_cols:
        import pandas as _pd

        # Single concat instead of up-to-~100 sequential per-column inserts — each insert
        # copies the block manager (pandas "highly fragmented" PerformanceWarning, visible
        # in the event-only driver path). Dtypes preserved EXACTLY as the old loop produced:
        # STRING columns get object/None (NOT np.nan -> float64, which Arrow cannot cast to
        # StringType under an explicit schema — see _STRING_OUTPUT_COLUMNS note + ADR-033
        # §amend); everything else float64/NaN.
        filler = _pd.DataFrame(
            {
                col: _pd.Series(None, index=out.index, dtype=object)
                if col in _STRING_OUTPUT_COLUMNS
                else _pd.Series(np.nan, index=out.index, dtype="float64")
                for col in missing_cols
            },
            index=out.index,
        )
        out = _pd.concat([out, filler], axis=1)

    # STRINGIFY every STRING column (null -> None, numeric id -> str) so the explicit-schema Arrow
    # write always sees str/None, never a float (all-NULL float64 OR an object column with numeric
    # ids). Idempotent for already-string columns. See _to_native_string + ADR-033 amendment.
    for col in _STRING_OUTPUT_COLUMNS:
        if col in out.columns:
            out[col] = np.array([_to_native_string(v) for v in out[col].tolist()], dtype=object)

    return out[output_cols].copy()
