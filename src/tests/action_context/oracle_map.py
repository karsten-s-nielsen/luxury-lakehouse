"""Verified AC-1-column -> legacy-oracle map (spec §9.1).

Three oracle tables, each scoped to the match by the extract tool, with its own
action-level join key:

  - ``tracking_context`` (``fct_tracking_context``): same-name feature columns;
    action-join on ``action_id``; providers idsse/skillcorner/metrica (NOT
    gradientsports — 0 rows). This pipeline batched per 250 frames; AC-1 batched
    identically until ADR-047 raised ``_FRAME_BATCH_SIZE`` to 2500, so
    geometric/at-action features still match tightly but WINDOW-DEPENDENT
    features (``_WINDOW_DEPENDENT_COLS``) legitimately diverge — their windows
    are no longer truncated at 250-frame edges.
    FROZEN (PR-1, TC-1 retirement): the TC-1 pipeline (``fct_tracking_context``)
    is DELETED. This is now a frozen historical CROSS-PIPELINE snapshot (AC-1
    validated against the values of the independent TC-1 pipeline) and is
    NON-REGENERABLE BY DESIGN. It must never be re-sourced from
    ``fct_action_context`` (that would make the golden suite AC-1-vs-AC-1,
    permanently green and blind). The extract tool no longer regenerates it.
  - ``pausa`` (``fct_pausa_values``): OBSO + PAUSA, renamed; action-join on
    ``pass_id``; IDSSE only.
  - ``elastic`` (``elastic_sync_results``): NOT a usable oracle. The legacy
    ``analytics.elastic_sync`` that produced it has an IDSSE frame-origin bug: it
    aligns events to ``frame ~= 25*ts`` (0-based) instead of ``10000 + 25*ts``
    (IDSSE period-1 frames start at 10000). Verified on J03WMX: oracle
    ``frame_id = 25.000*ts - 0.9`` (intercept ~0, should be 10000), so it yields NO
    results for the first ~400s (25*ts below the 10000 frame floor) and
    ~400s-misaligned results after. silly-kicks 3.25.0 fixes exactly this; AC-1's
    elastic is correct. Validating the fix against the buggy oracle is meaningless,
    so elastic is INVARIANT_ONLY (range-checked).

Columns with no usable action-grain oracle (``game_state``, ``shape_graph_*``,
``space_created_m2``, ``space_denied_m2_opponent``, ``ghost_gk_*``, ``elastic_*``) are
INVARIANT_ONLY: range-checked only.
"""

from __future__ import annotations

from dataclasses import dataclass

PROVIDERS_TRACKING_CONTEXT = frozenset({"idsse", "skillcorner", "metrica"})
PROVIDERS_IDSSE_ONLY = frozenset({"idsse"})


@dataclass(frozen=True)
class OracleSpec:
    """How to check one AC-1 result column against a legacy oracle (or as an invariant)."""

    ac_col: str
    oracle: str | None  # "tracking_context" | "pausa" | None (invariant-only)
    oracle_col: str | None
    providers: frozenset[str]
    kind: str = "float"  # "float" | "int" | "bool" | "categorical" | "invariant"
    atol: float = 1e-3
    rtol: float = 1e-2
    known_divergence: bool = False
    note: str = ""


# Action-level join: (AC-1-side column, oracle-side column) per oracle table.
# tracking_context joins on the integer action_id; fct_pausa_values' ``pass_id`` is a 32-char
# event-UUID hash (NOT the SPADL action_id), so PAUSA/OBSO join on the linked ``frame_id``.
# (elastic has no entry: its oracle is frame-origin-buggy, so elastic is INVARIANT_ONLY.)
ORACLE_JOIN: dict[str, tuple[str, str]] = {
    "tracking_context": ("action_id", "action_id"),
    "pausa": ("frame_id", "frame_id"),
}

# OBSO + PAUSA renames: AC-1 column -> fct_pausa_values column.
PAUSA_RENAME: dict[str, str] = {
    "obso_actual": "actual_obso",
    "obso_peak": "peak_obso",
    "obso_optimal": "optimal_obso",
    "pausa_temporal": "temporal_judgment",
    "pausa_spatial": "spatial_selection",
    "pausa_composite": "pausa_score",
}

# Columns with NO action-grain oracle — range-checked only. (kind, lo, hi) where None = unbounded.
# NOTE: shape_graph_mean_stability is NOT a [0,1] metric — empirically ~88-134 on real IDSSE
# data (it is a stability magnitude, not a normalized fraction), so it is bounded only >= 0.
INVARIANT_ONLY: dict[str, tuple[str, float | None, float | None]] = {
    "game_state": ("categorical", None, None),
    "shape_graph_density_attacking": ("float", 0.0, 1.0),
    "shape_graph_density_defending": ("float", 0.0, 1.0),
    "shape_graph_n_edges_attacking": ("int", 0.0, None),
    "shape_graph_n_edges_defending": ("int", 0.0, None),
    "shape_graph_mean_stability_attacking": ("float", 0.0, None),
    "shape_graph_mean_stability_defending": ("float", 0.0, None),
    # Space creation (silly-kicks 4.24.0 lean contract): `space_created_m2` (attacking LOO on own
    # team's OBSO surface, >=0) renamed from `space_created_m2_team`; `space_denied_m2_opponent`
    # (rest-defense LOO on the mirrored opponent surface, >=0) replaces the retired structurally-
    # zero `space_created_m2_opponent`. Both range-checked only (no legacy oracle). The 4.24.0
    # mirrored-multiplier fix means the opponent column now carries real, non-constant values.
    "space_created_m2": ("float", 0.0, None),
    "space_denied_m2_opponent": ("float", 0.0, None),
    # Ghost-GK (silly-kicks 3.24.0+): no legacy oracle exists (new column). Range-check only.
    # x/y are LTR-normalized pitch metres (105x68); spread is a positive dispersion magnitude.
    "ghost_gk_x": ("float", 0.0, 105.0),
    "ghost_gk_y": ("float", 0.0, 68.0),
    "ghost_gk_density_spread": ("float", 0.0, None),
    # Structural pass (TF-45; Karakus & Arkadas 2026): no legacy oracle. Range-check only.
    # structural_lbs is Int64/bigint and NaN on non-pass/cross rows — the differential range-check
    # dropna()s first (test_differential.py), so the "int" invariant tolerates <NA> (cf. elastic_frame_id).
    "structural_lbs": ("int", 0.0, None),
    # SGM (space gain) and SDI (structural disruption) are SIGNED — a pass can lose space / reduce
    # disruption, so both go negative (observed on J03WMX_p1: sgm min -0.56, sdi min -14.36). Unbounded.
    "structural_sgm": ("float", None, None),
    "structural_sdi": ("float", None, None),
    # Player influence (silly-kicks add_player_influence): areas non-negative; diffs may be negative.
    "actor_reachable_area_m2": ("float", 0.0, None),
    "off_ball_xt_team": ("float", 0.0, None),
    "off_ball_xt_opponent": ("float", 0.0, None),
    "off_ball_xt_diff": ("float", None, None),
    "reachable_area_team": ("float", 0.0, None),
    "reachable_area_opponent": ("float", 0.0, None),
    "reachable_area_diff": ("float", None, None),
    # xCrossAttempt (silly-kicks 4.18.0): probability in [0,1]. Range-check only.
    "xcross_attempt": ("float", 0.0, 1.0),
    # ELASTIC sync (silly-kicks 3.25.0): legacy elastic_sync_results oracle has an IDSSE
    # frame-origin bug (see module docstring), so the correct AC-1 values are range-checked,
    # not oracle-compared. frame_id is a non-negative frame number; confidence is a [0,1]
    # score; error_seconds is a non-negative alignment residual.
    "elastic_frame_id": ("int", 0.0, None),
    "elastic_confidence": ("float", 0.0, 1.0),
    "elastic_error_seconds": ("float", 0.0, None),
    # GK-influence near/far-post closing-time zones (silly-kicks gk_influence zone_names; ADR-039):
    # non-negative time-to-reach magnitudes, no legacy oracle. Range-check only.
    "gk_closing_time_mean_s__near_post": ("float", 0.0, None),
    "gk_closing_time_min_s__near_post": ("float", 0.0, None),
    "gk_closing_time_mean_s__far_post": ("float", 0.0, None),
    "gk_closing_time_min_s__far_post": ("float", 0.0, None),
    # xShotOccurrence (Pipping-Gamón, Feng & Sabin 2026): a probability in [0,1]. Range-check only.
    "xshot_occurrence": ("float", 0.0, 1.0),
    # xT-GK (Eyestone; silly-kicks 4.21.0/4.22.0): no legacy oracle (new feature family).
    # Composites and rav are SIGNED (rav = p*xT_dest - delta*(1-p)*xT_counter can go negative;
    # the composite inherits it) — unbounded. Confidence + completion are probabilities.
    "xt_gk": ("float", None, None),
    "xt_gk_possession": ("float", None, None),
    "xt_gk_counter": ("float", None, None),
    "xt_gk_direct": ("float", None, None),
    "xt_gk_high_press": ("float", None, None),
    "xt_gk_low_block": ("float", None, None),
    "xt_gk_base": ("float", None, None),
    "xt_gk_pev": ("float", None, None),
    "xt_gk_rav": ("float", None, None),
    "xt_gk_dzv": ("float", None, None),
    "xt_gk_pressure": ("float", None, None),
    "xt_gk_origin_source": ("categorical", None, None),
    "xt_gk_dest_source": ("categorical", None, None),
    "xt_gk_origin_confidence": ("float", 0.0, 1.0),
    "xt_gk_completion_variant": ("categorical", None, None),
    "xt_gk_completion_source": ("categorical", None, None),
    # Resolved-coordinate audit (silly-kicks 4.36.0): LTR SPADL meters, NaN off-scope.
    "xt_gk_origin_x": ("float", 0.0, 105.0),
    "xt_gk_origin_y": ("float", 0.0, 68.0),
    "xt_gk_dest_x": ("float", 0.0, 105.0),
    "xt_gk_dest_y": ("float", 0.0, 68.0),
    "gk_completion": ("float", 0.0, 1.0),
    # Pitch-control provenance (ADR-039): categorical {spearman, voronoi}; NULL on event-only rows.
    "pitch_control_method": ("categorical", None, None),
    # Ghost-GK backend provenance (ADR-035 amendment): categorical {scipy,vectorized,cpu-numba,fft,fft-cic};
    # NULL on event-only rows.
    "ghost_gk_method": ("categorical", None, None),
}

# Identity + linkage passthrough columns — not differential features (skip).
IDENTITY_PASSTHROUGH: frozenset[str] = frozenset({
    "data_source", "match_id", "action_id", "period_id", "time_seconds",
    "team_id", "player_id", "type_name", "start_x", "start_y", "end_x", "end_y",
    "defending_gk_player_id_native", "access_tier",
})  # fmt: skip

# DAS columns: oracle infers ball-carrier on the WHOLE match (contiguous hysteresis);
# AC-1's fix infers per-batch, so boundary actions can diverge -> looser tolerance.
_DAS_COLS = frozenset({"das_team", "das_opponent", "das_diff"})

# Window-dependent columns (ADR-047 + amendment 2): the legacy oracle batched at 250
# frames; AC-1's batch size is now PER-PROVIDER (resolve_frame_batch_size). For a
# provider batching at 250 the oracle is a valid target again (windows truncate at the
# SAME edges — coverage restored by amendment 2); for a provider batching larger
# (metrica/skillcorner at 2500), pre_window features see windows the oracle truncated
# (NaN<->value flips + integer runner-count deltas up to 8 measured in the 2026-06-10
# batch-size A/B), pressure_on_actor__bekkers_pi is batch-window-sensitive (max delta
# 0.21), and n_candidate_frames is the elastic candidate window, batch-edge-clipped at
# 250 — known_divergence for those providers. build_oracle_specs splits the spec by
# resolved size, so coverage tracks the per-provider defaults automatically.
_ORACLE_BATCH_SIZE = 250  # what the legacy fct_tracking_context pipeline batched at
_WINDOW_DEPENDENT_COLS = frozenset({
    "n_off_ball_runners_pre_window",
    "n_off_ball_runners_toward_goal_pre_window",
    "max_off_ball_run_displacement_pre_window",
    "mean_off_ball_run_speed_pre_window",
    "actor_arc_length_pre_window",
    "actor_displacement_pre_window",
    "pressure_on_actor__bekkers_pi",
    "n_candidate_frames",
})  # fmt: skip

# Threat-weighted columns: AC-1 uses the PERSISTED GLOBAL xT grid (expected_threat_grids,
# competition_id='global', 12x8 — ADR-013 design), while the legacy tracking_context oracle
# FITS its own ExpectedThreat per match at runtime (default dims). Different threat surface ->
# these four diverge by design, while every geometric feature matches. Verified root cause
# (silly-kicks 3.15.3 oracle vs 3.23.0 AC-1 has NO cover-shadow/gk-influence algo change in
# its CHANGELOG, so the divergence is the xT input, not a library bump). Reported, not asserted.
_XT_DEPENDENT_COLS = frozenset({
    "gk_pitch_control_share_weighted",
    "blocking_score",
    "blocked_threat_fraction",
    "max_single_defender_blocking_score",
})  # fmt: skip

# Integer / boolean tracking-context columns (exact match, no float tolerance).
_INT_COLS = frozenset({
    "n_candidate_frames", "back_n_count", "n_attackers_behind_line",
    "n_off_ball_runners_pre_window", "n_off_ball_runners_toward_goal_pre_window",
    "lines_broken__ward", "n_blocked_receivers", "n_potential_receivers",
    "team_shape_n_outfield_players_attacking", "team_shape_n_outfield_players_defending",
})  # fmt: skip
_BOOL_COLS = frozenset({"gk_was_distributing", "gk_was_engaged", "line_break", "line_break__ward"})
_CATEGORICAL_COLS = frozenset({"line_breaking_type__ward"})


def build_oracle_specs(ac_columns: list[str], tracking_oracle_columns: list[str]) -> list[OracleSpec]:
    """Build the per-column oracle spec for the columns AC-1 actually produced.

    ``tracking_oracle_columns`` is the column list of the pulled ``fct_tracking_context``
    fixture; same-name AC-1 columns map to it.
    """
    tc_cols = set(tracking_oracle_columns)
    specs: list[OracleSpec] = []
    for col in ac_columns:
        if col in IDENTITY_PASSTHROUGH:
            continue
        if col in PAUSA_RENAME:
            # OBSO/PAUSA are xT/threat metrics (OBSO = off-ball scoring opportunity), so they
            # carry the same global-vs-fit-per-match xT divergence as the threat-weighted tracking
            # cols; and on the 30-batch anchor only ~1 pass overlaps fct_pausa_values, far too few
            # to differential. Report-only; a full-half fixture is needed to validate these.
            specs.append(
                OracleSpec(
                    col,
                    "pausa",
                    PAUSA_RENAME[col],
                    PROVIDERS_IDSSE_ONLY,
                    kind="float",
                    known_divergence=True,
                    note="xT-dependent + insufficient anchor overlap",
                )
            )
        elif col in INVARIANT_ONLY:
            kind, _lo, _hi = INVARIANT_ONLY[col]
            specs.append(OracleSpec(col, None, None, frozenset(), kind="invariant", note=f"range {kind}"))
        elif col in tc_cols:
            kind = (
                "int"
                if col in _INT_COLS
                else "bool"
                if col in _BOOL_COLS
                else "categorical"
                if col in _CATEGORICAL_COLS
                else "float"
            )
            if col in _XT_DEPENDENT_COLS:
                specs.append(
                    OracleSpec(
                        col,
                        "tracking_context",
                        col,
                        PROVIDERS_TRACKING_CONTEXT,
                        kind=kind,
                        atol=1e-3,
                        rtol=1e-2,
                        known_divergence=True,
                        note="threat-weighted: AC-1 global xT grid vs oracle fit-per-match xT",
                    )
                )
            elif col in _WINDOW_DEPENDENT_COLS:
                # Split by the provider's RESOLVED batch size (ADR-047 amendment 2):
                # providers batching at the oracle's 250 get asserted coverage back;
                # providers batching larger keep known_divergence. Tracks the
                # per-provider defaults automatically.
                from analytics.action_context.batching import resolve_frame_batch_size

                at_oracle_size = frozenset(
                    p for p in PROVIDERS_TRACKING_CONTEXT if resolve_frame_batch_size(p) == _ORACLE_BATCH_SIZE
                )
                if at_oracle_size:
                    specs.append(
                        OracleSpec(
                            col,
                            "tracking_context",
                            col,
                            at_oracle_size,
                            kind=kind,
                            note="window-dependent; provider batches at the oracle's 250 -> valid target",
                        )
                    )
                diverging = PROVIDERS_TRACKING_CONTEXT - at_oracle_size
                if diverging:
                    specs.append(
                        OracleSpec(
                            col,
                            "tracking_context",
                            col,
                            diverging,
                            kind=kind,
                            known_divergence=True,
                            note="window-dependent: oracle batched at 250 frames, provider batches larger (ADR-047)",
                        )
                    )
            elif col in _DAS_COLS:
                specs.append(
                    OracleSpec(
                        col,
                        "tracking_context",
                        col,
                        PROVIDERS_TRACKING_CONTEXT,
                        kind=kind,
                        atol=5e-2,
                        rtol=5e-2,
                        known_divergence=True,
                        note="DAS: whole-match vs per-batch ball-carrier",
                    )
                )
            else:
                specs.append(OracleSpec(col, "tracking_context", col, PROVIDERS_TRACKING_CONTEXT, kind=kind))
        # else: AC-1 column with no oracle and not invariant-listed -> silently unmapped
    return specs


def invariant_range(col: str) -> tuple[float | None, float | None]:
    """(lo, hi) bounds for an INVARIANT_ONLY column."""
    _kind, lo, hi = INVARIANT_ONLY[col]
    return lo, hi
