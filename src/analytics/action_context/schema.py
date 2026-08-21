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

# 165 columns total (164 output + _ingested_at). The authoritative source is this list itself;
# ACTION_CONTEXT_DDL must declare the SAME columns in the SAME order — enforced by
# test_action_context_schema_parity.py (do not hand-maintain a category breakdown here; it drifted
# to a stale total before and the parity test is the real guard).
# (spec §7.4 / ADR xtgk-v2: the 16 v1 xt_gk METRIC columns — xt_gk, the 5 philosophy presets,
#  base/pev/rav/dzv/pressure, and the 5 origin/dest/completion provenance columns — are RETIRED from
#  the drain; xt_gk_v2 replaces them as a MART-JOIN column set fed by ingestion.xt_gk_v2_writer, NOT a
#  drain column. The 4 xt_gk_origin_x/_y + xt_gk_dest_x/_y RESOLVED-coordinate columns are KEPT — they
#  are the v2 writer's geometry bridge (silly-kicks xtgk.apply_resolved_gk_geometry reads them by
#  default). gk_completion is KEPT — a distinct add_gk_completion call, unaffected by v2.)
# (ADR-056: game_state + GK action-sequence flags removed — actions-level, served
#  by fct_action_values; defending_gk_player_id_native kept for the key resolution.)
# (F1 2026-07-11: is_gk_distribution added — GK-distribution domain marker, silly-kicks 4.43.0.)
RESULT_COLUMNS: list[str] = [
    # Identity (13)
    "data_source",
    # Per-match HF redistribution tier (spec 2026-06-29). Provider-native passthrough →
    # canonical name (ADR-016). Stamped DIRECTLY per (provider, match) in build_output —
    # never a dim_matches join (unmatched→NULL→fail-safe-restricted silently drops public
    # data, spec D3/M1). "public" | "restricted".
    "access_tier",
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
    # NOTE (ADR-056): game_state + the GK action-sequence flags
    # (gk_was_distributing/gk_was_engaged/gk_actions_in_possession) were REMOVED
    # here — they are actions-level (frame-independent; add_game_state / the
    # add_pre_shot_gk_context lookback window) and already served by
    # fct_action_values. Consumers join on (match_id, action_id). KEPT:
    # defending_gk_player_id_native (load-bearing — resolves the AC-specific
    # defending_gk_player_key via dim_players, which action_values does not), the
    # tracking-derived pre_shot_gk_* POSITION columns, and frame-linkage provenance.
    # Frame linkage (4)
    "frame_id",
    "time_offset_seconds",
    "link_quality_score",
    "n_candidate_frames",
    # GK resolution — native id resolves defending_gk_player_key in the mart (1)
    "defending_gk_player_id_native",
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
    "pitch_control_at_target__spearman",
    "pitch_control_at_target__fernandez_bornn",
    "pitch_control_at_target__voronoi",
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
    # Space creation (2) — silly-kicks 4.24.0 lean contract: `space_created_m2` (attacking LOO,
    # >=0) + `space_denied_m2_opponent` (rest-defense LOO on the mirrored opponent surface, >=0).
    # Renamed from `space_created_m2_team`; the structurally-zero `space_created_m2_opponent` was
    # retired upstream (removal-based LOO makes opponent-created mathematically 0). See ADR-026.
    "space_created_m2",
    "space_denied_m2_opponent",
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
    # Ghost-GK (2) — silly-kicks 4.87.0 retired ghost_gk_density_spread (ghost_gk_xfns 9→6-col)
    "ghost_gk_x",
    "ghost_gk_y",
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
    # Shot goalmouth crossing (TF-48; Anzer & Bauer 2021) — post-shot ball-trajectory geometry.
    # Tracking-derived (lives only here, not the actions-level fct_action_values). NOT a VAEP
    # feature (post-contact outcome leakage; upstream ADR-030 guard). NaN/NA off-scope. (11)
    "shot_crossing_y",
    "shot_crossing_z",
    "shot_speed",
    "shot_time_to_goal_line",
    "shot_on_target_derived",
    "shot_crossing_source",
    "shot_crossing_confidence",
    "shot_fit_n_frames",
    "shot_fit_rmse",
    "shot_fit_end_reason",
    "shot_z_profile",
    # xT-GK v1 METRIC columns RETIRED (spec §7.4 / ADR xtgk-v2 replaces v1): xt_gk, the 5 philosophy
    # presets (possession/counter/direct/high_press/low_block — NO v2 successor, a known capability
    # regression), base/pev/rav/dzv/pressure, and origin_source/dest_source/origin_confidence/
    # completion_variant/completion_source. xt_gk_v2 replaces them as a MART-JOIN column set
    # (ingestion.xt_gk_v2_writer → bronze.xt_gk_v2_predictions → stg_xt_gk_v2 → fct_action_context LEFT
    # JOIN), NOT a drain column. See the header note.
    #
    # Resolved-coordinate columns (silly-kicks 4.36.0 `_COORD_COLS`) — KEPT as the v2 writer's geometry
    # bridge: silly-kicks `xtgk.apply_resolved_gk_geometry` reads exactly these column names by default
    # to override the GK-distribution start/end coords with gold's resolved keeper geometry (the writer
    # has no tracking frames of its own). The drain still produces them via `resolve_gk_geometry`.
    # LTR SPADL meters (x∈[0,105], y∈[0,68]); NaN off-scope. (4)
    "xt_gk_origin_x",
    "xt_gk_origin_y",
    "xt_gk_dest_x",
    "xt_gk_dest_y",
    # GK-distribution completion probability — the exact P(success) RAV consumes (1)
    "gk_completion",
    # GK-distribution domain marker (silly-kicks 4.43.0 gk_distribution_mask) — True for any
    # goal-kick OR an open-play pass/throw-in whose actor is the acting-team GK. Non-nullable at
    # the PRODUCER: both AC arms always compute it and the mask never emits NULL — tracking arm =
    # full domain (resolve_gk="robust"), SB360 arm = goal-kicks-only (frames=None). Left nullable
    # through bronze/mart to tolerate the phased Phase-5 recompute (pre-F1 rows stay NULL until
    # re-materialized); silly-kicks' rho retention loader reads it with COALESCE(...,FALSE). (1)
    "is_gk_distribution",
    # Pitch-control provenance for the persisted pitch-control-derived metrics (1)
    "pitch_control_method",
    # === silly-kicks 4.87.0 DRAIN-NATIVE columns (spec §7.1 / §7.3) — 23 total ===
    # Real-xT OBSO provenance (4.52; xt= on obso/pausa/space): "xt"/"synthetic"/"injected", NA off-domain (1)
    "obso_epv_source",
    # Off-ball run values (TF-35, 4.52; add_off_ball_run_values) (5)
    "run_value_target",
    "run_value_disruptive_sum",
    "run_value_enabled_pass",
    "n_disruptive_runs",
    "n_valued_disruptive_runs",
    # Press commitment (TF-51, 4.61; add_press_commitment) (3)
    "press_commitment",
    "press_commitment_closing_speed",
    "press_commitment_source",
    # Packing (TF-49, 4.50; add_packing) — receiver player id is a native-id passthrough (STRING) (5)
    "packing_made",
    "packing_goal_threat",
    "packing_net",
    "packing_receiver_player_id",
    "packing_secured",
    # Provenance free-ride (add_das / add_ghost_gk already called) (2)
    "das_source",
    "ghost_gk_source",
    # Cover-shadow single-defender id free-ride (add_cover_shadows(detailed=True) already called) (1)
    "max_single_defender_player_id",
    # team_shape gap columns free-ride (add_team_shape emits 20; we carried 14, now carry 20) (6)
    "team_shape_defensive_line_height_attacking",
    "team_shape_defensive_line_height_defending",
    "team_shape_inter_line_gap_1_attacking",
    "team_shape_inter_line_gap_1_defending",
    "team_shape_inter_line_gap_2_attacking",
    "team_shape_inter_line_gap_2_defending",
    # Visibility coverage (silly-kicks 4.87.0; spec §7.1/§7.5) — SB360-only, empty until SB360 AC is
    # enabled (ADR-058) but shipped in the schema/contract now. add_visible_area_coverage emits the 2
    # base cols (observed pitch fraction + provenance); add_action_context(visible_area=) emits the 6
    # *_observed_* companions (fraction DOUBLE + source STRING per count feature). (8)
    "visible_area_fraction",
    "visible_area_source",
    "nearest_defender_distance_observed_fraction",
    "nearest_defender_distance_observed_source",
    "receiver_zone_density_observed_fraction",
    "receiver_zone_density_observed_source",
    "defenders_in_triangle_to_goal_observed_fraction",
    "defenders_in_triangle_to_goal_observed_source",
    # Audit (1)
    "_ingested_at",
]

ACTION_CONTEXT_DDL = (
    "data_source STRING, access_tier STRING, match_id STRING, action_id BIGINT, period_id BIGINT, "
    "time_seconds DOUBLE, team_id STRING, player_id STRING, type_name STRING, "
    "start_x DOUBLE, start_y DOUBLE, end_x DOUBLE, end_y DOUBLE, "
    # game_state + GK action-sequence flags removed (ADR-056) — actions-level,
    # served by fct_action_values; consumers join on (match_id, action_id).
    # defending_gk_player_id_native KEPT (resolves defending_gk_player_key).
    "frame_id BIGINT, time_offset_seconds DOUBLE, link_quality_score DOUBLE, "
    "n_candidate_frames BIGINT, "
    "defending_gk_player_id_native STRING, "
    "pre_shot_gk_x DOUBLE, pre_shot_gk_y DOUBLE, "
    "pre_shot_gk_distance_to_goal DOUBLE, pre_shot_gk_distance_to_shot DOUBLE, "
    "pre_shot_gk_angle_to_shot_trajectory DOUBLE, pre_shot_gk_angle_off_goal_line DOUBLE, "
    "nearest_defender_distance DOUBLE, actor_speed DOUBLE, "
    "receiver_zone_density BIGINT, defenders_in_triangle_to_goal BIGINT, "
    "actor_arc_length_pre_window DOUBLE, actor_displacement_pre_window DOUBLE, "
    "pressure_on_actor__andrienko_oval DOUBLE, pressure_on_actor__link_zones DOUBLE, "
    "pressure_on_actor__bekkers_pi DOUBLE, "
    "pitch_control_at_target__spearman DOUBLE, pitch_control_at_target__fernandez_bornn DOUBLE, "
    "pitch_control_at_target__voronoi DOUBLE, "
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
    "space_created_m2 DOUBLE, space_denied_m2_opponent DOUBLE, "
    "elastic_frame_id BIGINT, elastic_confidence DOUBLE, elastic_error_seconds DOUBLE, "
    "shape_graph_density_attacking DOUBLE, shape_graph_n_edges_attacking BIGINT, "
    "shape_graph_mean_stability_attacking DOUBLE, "
    "shape_graph_density_defending DOUBLE, shape_graph_n_edges_defending BIGINT, "
    "shape_graph_mean_stability_defending DOUBLE, "
    "ghost_gk_x DOUBLE, ghost_gk_y DOUBLE, "
    "structural_lbs BIGINT, structural_sgm DOUBLE, structural_sdi DOUBLE, "
    "actor_reachable_area_m2 DOUBLE, off_ball_xt_team DOUBLE, off_ball_xt_opponent DOUBLE, "
    "off_ball_xt_diff DOUBLE, reachable_area_team DOUBLE, reachable_area_opponent DOUBLE, "
    "reachable_area_diff DOUBLE, xcross_attempt DOUBLE, "
    "xshot_occurrence DOUBLE, "
    "shot_crossing_y DOUBLE, shot_crossing_z DOUBLE, shot_speed DOUBLE, "
    "shot_time_to_goal_line DOUBLE, shot_on_target_derived BOOLEAN, "
    "shot_crossing_source STRING, shot_crossing_confidence DOUBLE, "
    "shot_fit_n_frames DOUBLE, shot_fit_rmse DOUBLE, shot_fit_end_reason STRING, "
    "shot_z_profile STRING, "
    # xT-GK v1 metric columns RETIRED (spec §7.4 — xt_gk_v2 replaces them as a mart-join). The 4
    # resolved-coordinate columns below are KEPT as the v2 writer's geometry bridge.
    "xt_gk_origin_x DOUBLE, xt_gk_origin_y DOUBLE, xt_gk_dest_x DOUBLE, xt_gk_dest_y DOUBLE, "
    "gk_completion DOUBLE, "
    "is_gk_distribution BOOLEAN, "
    "pitch_control_method STRING, "
    # silly-kicks 4.87.0 DRAIN-NATIVE columns (spec §7.1) — MUST stay in exact name+order
    # parity with RESULT_COLUMNS (test_action_context_schema_parity).
    "obso_epv_source STRING, "
    "run_value_target DOUBLE, run_value_disruptive_sum DOUBLE, run_value_enabled_pass DOUBLE, "
    "n_disruptive_runs BIGINT, n_valued_disruptive_runs BIGINT, "
    "press_commitment DOUBLE, press_commitment_closing_speed DOUBLE, press_commitment_source STRING, "
    "packing_made BIGINT, packing_goal_threat BIGINT, packing_net DOUBLE, "
    "packing_receiver_player_id STRING, packing_secured BOOLEAN, "
    "das_source STRING, ghost_gk_source STRING, max_single_defender_player_id STRING, "
    "team_shape_defensive_line_height_attacking DOUBLE, team_shape_defensive_line_height_defending DOUBLE, "
    "team_shape_inter_line_gap_1_attacking DOUBLE, team_shape_inter_line_gap_1_defending DOUBLE, "
    "team_shape_inter_line_gap_2_attacking DOUBLE, team_shape_inter_line_gap_2_defending DOUBLE, "
    # Visibility coverage (silly-kicks 4.87.0; spec §7.1/§7.5) — SB360-only. 2 base + 6 *_observed_*
    # companions. Must stay in exact name+order parity with RESULT_COLUMNS (test_action_context_schema_parity).
    "visible_area_fraction DOUBLE, visible_area_source STRING, "
    "nearest_defender_distance_observed_fraction DOUBLE, nearest_defender_distance_observed_source STRING, "
    "receiver_zone_density_observed_fraction DOUBLE, receiver_zone_density_observed_source STRING, "
    "defenders_in_triangle_to_goal_observed_fraction DOUBLE, defenders_in_triangle_to_goal_observed_source STRING, "
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


def build_output(
    actions: pd.DataFrame,
    match_id_native: str,
    data_source: str,
    access_tier: str | None = None,
) -> pd.DataFrame:
    """Post-enrichment: renames + column selection for bronze write.

    1. game_id -> match_id (silly-kicks uses game_id, we use match_id)
    2. defending_gk_player_id -> defending_gk_player_id_native (ADR-018)
    3. type_id -> type_name via silly-kicks add_names
    4. access_tier stamp (per-match HF redistribution tier, spec 2026-06-29) — DIRECT, never a
       dim_matches join. Resolution order: explicit ``access_tier`` arg (caller derived it from the
       per-match SPADL actions) > carry-through of a non-NULL ``access_tier`` already on ``actions``
       (constant per match) > provider default via ``classify_access_tier(provider, visibility=None)``.
       An unmatched join would yield NULL → fail-safe-restricted → silently drop public data (spec
       D3/M1), so we never join: the provider default is the floor and is correct for the four
       no-feed providers (statsbomb/wyscout/idsse/metrica → public; gradientsports → restricted).
    5. Column selection to RESULT_COLUMNS, dtype-correct fill for missing cols (numeric -> NaN,
       STRING -> object/None so the explicit-schema Arrow write never sees float64-vs-StringType).
    """
    out = actions.copy()
    out["match_id"] = match_id_native
    out["data_source"] = data_source

    if access_tier is None:
        if "access_tier" in actions.columns and actions["access_tier"].notna().any():
            access_tier = str(actions["access_tier"].dropna().iloc[0])
        else:
            from shared.access_tier import classify_access_tier

            access_tier = classify_access_tier(provider=data_source, visibility=None).value
    out["access_tier"] = access_tier

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
