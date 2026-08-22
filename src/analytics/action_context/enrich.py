"""Action-context enrichment tiers (pure pandas/numpy/silly_kicks).

Moved verbatim from ``ingestion.action_context`` (behavior-preserving). Action-context
is FRAMES-REQUIRED (ADR-057): two tiers — tracking (full ~21-step chain) and SB360
(synthetic freeze-frames) — plus the mutate-then-restore identity resolver. Event-only
matches are out of scope (no row). No pyspark; runs identically on a Spark executor
(inside the per-group UDF, mapInPandas dispatch per ADR-045) and locally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    from silly_kicks.xthreat import ExpectedThreat

_GHOST_GK_MODEL_CACHE: dict[str, Any] = {}
"""Process-local Ghost-GK model singleton (ADR-045). Passing ``model="default"`` (a
string) to silly-kicks ``add_ghost_gk`` makes it re-resolve + re-load the ~12 MB weights
from disk on EVERY call — once per frame batch, ~8% of measured per-half wall (plus
one sklearn-provenance warning per batch). This is the databricks-serverless.md "Model
loading on executors" convention: Spark reuses Python worker processes across groups,
so the instance loads once per executor process and is reused by every batch."""


def _ghost_gk_model_cached(variant: str = "default") -> Any:
    """Lazy-load + cache the Ghost-GK model instance for this process."""
    model = _GHOST_GK_MODEL_CACHE.get(variant)
    if model is None:
        from silly_kicks.tracking import GhostGkModel

        model = GhostGkModel.from_variant(variant)  # type: ignore[arg-type]  # "default"/"full" literals
        _GHOST_GK_MODEL_CACHE[variant] = model
    return model


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


# SPADL restart action types where ``action.team_id`` unambiguously establishes
# possession on the linked frame, regardless of whether infer_ball_carrier finds
# a carrier in the surrounding dead-ball window. Excludes open-play action types
# (pass / dribble / shot / tackle / etc.) where the ball-carrier inference is
# authoritative. Per the PR-S67 ADR, silly-kicks stays a pure pass-through; the
# lakehouse synthesizes possession ONLY for the narrow subset where SPADL
# semantics make it unambiguous.
_SET_PIECE_RESTART_TYPE_NAMES: frozenset[str] = frozenset(
    {
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "shot_freekick",
        "corner_crossed",
        "corner_short",
        "goalkick",
        "shot_penalty",
    }
)


def _set_piece_restart_type_ids() -> frozenset[int]:
    """Derive the type_id frozenset at first call from silly-kicks's authoritative list.

    Drift-safe: if silly-kicks reorders or adds a restart type, this auto-tracks
    (the names are the source of truth; ids follow the canonical actiontypes list).
    Lazy because silly-kicks import is heavyweight; helper callers pay the cost
    once per worker, not at module import.
    """
    from silly_kicks.spadl.config import actiontypes

    return frozenset(i for i, name in enumerate(actiontypes) if name in _SET_PIECE_RESTART_TYPE_NAMES)


def _fill_possession_from_set_piece_actions(
    frames_tip: pd.DataFrame,
    *,
    actions: pd.DataFrame,
    links: pd.DataFrame,
) -> pd.DataFrame:
    """Fill ``team_in_possession`` on action-linked frames using set-piece team_id.

    SPADL set-piece restart actions (throw-in, free-kick, corner, goal-kick,
    penalty) have unambiguous possession from ``action.team_id`` even when the
    surrounding frame-batch window is entirely dead-ball and ``infer_ball_carrier``
    returns no carrier. Without this fill, silly-kicks' ``add_das`` (3.30.0+)
    correctly returns NaN for dead-ball-linked actions — honest but
    information-poor. This helper supplies the missing possession signal so
    silly-kicks computes FINITE DAS on the actions whose possession is
    SPADL-determinable.

    Reads ``type_id`` (canonical SPADL int, present from add_game_state onward),
    NOT ``type_name`` (which build_output adds only at the bronze-output stage).

    Fill semantics: only writes ``team_in_possession`` where currently NaN on
    the linked (period_id, frame_id). Never overwrites a carrier-derived value
    (carrier inference remains authoritative for open-play). Non-set-piece
    dead-ball actions are NOT touched and still get NaN DAS — the metric is
    undefined where no team has possession.

    See [[project_sk330_dead_ball_robustness_handoff]] for the architectural
    split between silly-kicks (no-crash + honest NaN) and lakehouse (modeling
    decision: synthesize possession for the SPADL-unambiguous subset).
    """
    type_ids = _set_piece_restart_type_ids()
    sp_actions = actions[actions["type_id"].isin(type_ids) & actions["team_id"].notna()]
    if sp_actions.empty:
        return frames_tip

    # silly-kicks' link_actions_to_frames returns only (action_id, frame_id, ...) — no
    # period_id. Pull period_id + team_id from sp_actions via the action_id join.
    sp_links = links.merge(
        sp_actions[["action_id", "team_id", "period_id"]],
        on="action_id",
        how="inner",
    )
    if sp_links.empty:
        return frames_tip

    # If two set-piece actions land on the exact same (period_id, frame_id),
    # take the first deterministically — silly-kicks needs ONE value per frame.
    fill_df = (
        sp_links[["period_id", "frame_id", "team_id"]]
        .drop_duplicates(["period_id", "frame_id"])
        .rename(columns={"team_id": "_fill_team_id"})
    )

    out = frames_tip.merge(fill_df, on=["period_id", "frame_id"], how="left")
    mask = out["team_in_possession"].isna() & out["_fill_team_id"].notna()
    out.loc[mask, "team_in_possession"] = out.loc[mask, "_fill_team_id"]
    return out.drop(columns=["_fill_team_id"])


def _override_goalkick_actor_from_frames(out: pd.DataFrame, frames: pd.DataFrame) -> pd.DataFrame:
    """Credit goal-kicks to the acting keeper (silly-kicks 4.39.0 ``acting_gk_from_frames``).

    Goal-kicks carry a NULL SPADL taker for all four tracking providers, so the AC chain fills
    the actor from the ball-carrier at the linked frame — which for a goal-kick is the DOWNFIELD
    event location (the 4.37.0 origin scatter), crediting whichever outfielder is near the ball
    14-20 m upfield rather than the keeper (the actor-analog of the origin scatter: 4.37.0 fixed
    the origin, 4.38.0 the identity, this the taker). A goal-kick's taker is unambiguously the
    acting team's keeper, so override the carrier-derived ``player_id`` with the frames-resolved
    acting GK. The 4.39.0 resolver has a roster-identity fallback, so it fires even on the ~40% of
    goal-kicks where the keeper is undetected at the event frame.

    Scope is deliberately narrow (mirrors the analysis-side handoff):
    - **Goal-kicks only.** Other set-piece restarts (throw-in/corner/free-kick) have outfielder
      takers whose event ball sits *at* the taker — the carrier fill is correct there; do not touch.
    - **Only where the resolver is non-NaN.** Event-only providers carry a real SPADL taker and have
      no frames → NaN → never blanked (this path is tracking-only anyway; the gate is defence-in-depth).

    Sets BOTH ``player_id`` and ``player_id_native`` so ``build_output``'s ``_restore_native_identity``
    (player_id ← player_id_native) preserves the correction. Called AFTER the whole xt_gk family so it
    relabels the actor ONLY — ``xt_gk`` values + origins are unchanged (the analysis-side invariant;
    they resolve from frame geometry, not the action's player_id). Lakehouse-owned modeling decision,
    the actor-analog of ``_fill_possession_from_set_piece_actions`` (PR-S67 boundary: silly-kicks stays
    a pure resolver; the lakehouse decides WHEN to apply it).
    """
    from silly_kicks.spadl.config import actiontypes
    from silly_kicks.tracking import acting_gk_from_frames

    goalkick_id = actiontypes.index("goalkick")
    gk_mask = out["type_id"] == goalkick_id
    if not gk_mask.any():
        return out

    resolved = acting_gk_from_frames(out, frames)  # index-aligned; dtype = frames' player_id dtype
    apply_mask = gk_mask & resolved.notna()
    if not apply_mask.any():
        return out

    out.loc[apply_mask, "player_id"] = resolved[apply_mask]
    if "player_id_native" in out.columns:
        out.loc[apply_mask, "player_id_native"] = resolved[apply_mask]
    return out


def _enrich_tracking_match(
    actions_df: pd.DataFrame,
    tracking_df: pd.DataFrame,
    xt: ExpectedThreat,
    home_team_id: str,
    kde_backend: str = "fft-cic",
) -> pd.DataFrame:
    """Full enrichment chain for tracking providers.

    ``kde_backend`` is retained for WorkUnit/signature compatibility but is no longer persisted:
    silly-kicks 4.87.0 resolves the ghost-GK KDE backend upstream (predict_density default) and the
    ``ghost_gk_method`` provenance column was retired. See spec section 4.2 for the complete call graph.
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
        add_gk_completion,
        add_gk_influence,
        add_line_break,
        add_obso,
        add_off_ball_context,
        add_off_ball_run_values,
        add_packing,
        add_pausa,
        add_pre_shot_gk_angle,
        add_pre_shot_gk_position,
        add_press_commitment,
        add_pressure_on_actor,
        add_shape_graph,
        add_shot_goalmouth,
        add_space_creation,
        add_structural_pass,
        add_sync_score,
        add_team_shape,
        add_xcross_attempt,
        add_xshot_occurrence,
        derive_team_in_possession,
        gk_distribution_mask,
        infer_ball_carrier,
        link_actions_to_frames,
        pitch_control_at_target,
        resolve_defended_goals,
        resolve_gk_geometry,
    )

    # add_ghost_gk + add_player_influence + PitchControlCache are not re-exported from the
    # silly_kicks.tracking namespace (3.25.0) — import from their defining modules.
    from silly_kicks.tracking.features import add_ghost_gk, add_player_influence
    from silly_kicks.tracking.pitch_control import PitchControlCache

    # Step 0: Actions-only enrichments (no tracking needed)
    out = add_game_state(actions_df)

    # Step 1: Frame linkage — computed ONCE; links passed to every add_* call.
    # on_low_coverage="ignore": silly-kicks 4.12.0 (ADR-017) added a warn-by-default per-period
    # low-coverage guard, but THIS call is per-frame-batch — enrich_batch already pre-filters
    # actions to the batch's frame time-window and M13 drops cross-batch actions, so a sub-1.0
    # link rate here is expected and benign. The real time-base guard runs ONCE per work unit at
    # the driver entry (assert_work_unit_time_base, ADR-040); keep this bit-identical to pre-4.12.0
    # and avoid spurious per-batch UserWarnings inside the UDF (which land in executor logs).
    links, _report = link_actions_to_frames(out, tracking_df, on_low_coverage="ignore")

    # One shared per-frame pitch-control surface cache for this batch (silly-kicks 3.25.0
    # TF-7): obso / cover_shadows / gk_influence / space_creation / pitch_control_at_target
    # otherwise each recompute the same canonical surfaces. Caller-supplied cache extends
    # reuse ACROSS those families within this pass. Bit-identical output (only canonical,
    # not counterfactual, surfaces are cached).
    pc_cache = PitchControlCache()

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

    # Step 5b: Pressure — bekkers_pi
    # silly-kicks 3.30.0 falls back per-action to the base TTI model
    # (use_ball_carrier_max=False semantics) when an action's linked frame has no
    # ball row, so we no longer need a whole-batch try/except wrapper. Loud
    # failure on missing vx/vy is preserved (that's a real data shape error).
    out = add_pressure_on_actor(
        out,
        tracking_df,
        links=links,
        methods=("bekkers_pi",),
    )

    # Step 6: Pitch control — 3 methods via Series API
    for method in ("spearman", "fernandez_bornn", "voronoi"):
        s = pitch_control_at_target(out, tracking_df, links=links, method=method, pitch_control_cache=pc_cache)
        out[s.name] = s.values

    # Direction map (silly-kicks 4.53+): resolve defended goals ONCE per work-unit on the full
    # frames and pass goal_map= to the aggregators that accept it (perf — avoids each re-resolving).
    # The other direction-aware aggregators self-resolve on our home-LTR frames (ADR-053), so the
    # 4.87.0 migration is a straight drop of the retired home_team_id= kwarg on those.
    goal_map = resolve_defended_goals(tracking_df)

    # Step 7: Defensive line
    out = add_defensive_line(out, tracking_df, links=links, goal_map=goal_map)

    # Step 8: Off-ball context (umbrella — includes off-ball-run columns)
    out = add_off_ball_context(out, tracking_df, links=links, goal_map=goal_map)

    # Step 9: Ward line-breaking
    out = add_line_break(out, tracking_df, links=links, method="ward")

    # Step 10: Team shape
    out = add_team_shape(out, tracking_df, links=links)

    # Step 11: DAS (chunk_size=10 prevents OOM under 1 GB group cap).
    # add_das -> _precompute_das_lookup requires a `team_in_possession` column; derive it
    # first (ball-carrier inferred on ALL frames for correct contiguous hysteresis). Then
    # fill possession for SPADL set-piece restart actions where the carrier-derived value
    # is NaN but action.team_id makes possession unambiguous (PR-S67-respecting modeling
    # decision in the lakehouse domain). silly-kicks 3.30.0 honestly returns NaN DAS for
    # any action whose linked frame still has NaN possession after this fill.
    # PERF: silly-kicks 3.25.0 restricts the expensive per-frame DAS computation to the
    # action-linked frames internally when `links` is supplied (pinning attacking direction on
    # the full frames first → bit-identical). No lakehouse-side frame filtering needed.
    _carrier = infer_ball_carrier(tracking_df)
    _frames_tip = derive_team_in_possession(tracking_df, _carrier)
    _frames_tip = _fill_possession_from_set_piece_actions(_frames_tip, actions=out, links=links)
    out = add_das(out, _frames_tip, links=links, chunk_size=10)
    del _carrier, _frames_tip

    # Step 12: GK spatial (requires defending_gk_player_id from Step 2)
    out = add_pre_shot_gk_position(out, tracking_df, links=links)
    out = add_pre_shot_gk_angle(out, frames=tracking_df, links=links)

    # Step 12b: Ghost-GK (silly-kicks 3.24.0+) — defending GK's model-predicted "ghost"
    # position + spread at the linked frame. Uses the bundled "default" model (~9 MB, ships
    # in the wheel → no network, safe in the no-internet Databricks UDF). actions_for_context
    # supplies score_diff / phase context.
    #
    # kde_backend="fft-cic": ghost-GK is the AC-1 bottleneck (~74% of the tracking chain — the
    # weighted KDE over the full ~36k-point training cloud per action). The "fft-cic" backend
    # (silly-kicks 4.9.0+) is the binned-convolution KDE with CIC (bilinear) binning — ~2000x the
    # cpu-numba @njit kernel on large clouds, which is what makes a full metrica tracking game
    # finish inside the per-game watchdog (cpu-numba cannot). It approximates the scipy-oracle
    # argmax: on J03WMX_p1 it is 95% mode-exact (92/97), mean Δ 97mm, with 2 multi-metre flips on
    # genuinely bimodal near-tie grids (argmax inherently unstable there); entropy/spread err
    # <0.3%. CIC chosen over plain "fft" (NGP): 95% vs 78% mode-exact at the same cost. NOT
    # value-equivalent to cpu-numba within bit tolerance — BOTH goldens were re-baselined to
    # fft-cic. See ADR-035 (amendment) + project memory next-session-cic-ghost-gk-testing.
    # silly-kicks 4.87.0 dropped the ``kde_backend`` selector from add_ghost_gk / compute_ghost_gk;
    # the KDE backend is now resolved upstream (predict_density default). The ``ghost_gk_method``
    # provenance column was retired with it (drain-native schema change), so ``kde_backend`` is
    # accepted for WorkUnit/signature compatibility but no longer persisted.
    out = add_ghost_gk(
        out,
        tracking_df,
        model=_ghost_gk_model_cached(),  # process-cached instance — NOT the "default" string (ADR-045)
        links=links,
        home_team_id=home_team_id,
        actions_for_context=actions_df,
    )

    # Step 13: GK influence (xt positional). Explicit method="spearman" (velocity-aware; full
    # tracking has velocity) keeps the pitch_control_method provenance label honest if the
    # silly-kicks default ever changes. zone_names persists near/far-post closing-time too.
    out = add_gk_influence(
        out,
        tracking_df,
        xt,
        links=links,
        goal_map=goal_map,
        pitch_control_cache=pc_cache,
        method="spearman",
        zone_names=["six_yard_box", "near_post", "far_post"],
    )

    # Step 14: Cover shadows (xt positional). detailed=True: the cheap fixed-cast default
    # only affects max_single_defender_blocking_score, where it diverges from the accurate
    # per-defender counterfactual by more than that column's own observed range. ~1.5x the
    # cover_shadows cost (scales with action count, not frames; not on the critical path).
    out = add_cover_shadows(
        out, tracking_df, xt, links=links, goal_map=goal_map, detailed=True, pitch_control_cache=pc_cache
    )

    # Step 15: Shape graph — silly-kicks 3.25.0 restricts the per-frame snapshot computation
    # to action-linked frames internally when `links` is supplied (bit-identical).
    out = add_shape_graph(out, tracking_df, links=links)

    # Step 16: OBSO — MUST precede add_pausa. Explicit spearman (provenance honesty). xt= is the
    # fitted GLOBAL xT grid: MANDATORY from silly-kicks 4.52 (omitting it fires a non-fatal
    # SyntheticEPVWarning and falls back to SYNTHETIC EPV) and switches OBSO to real fitted xT (stamps
    # obso_epv_source="xt"). The warning is NOT escalated to an error (no filterwarnings config); the
    # mini-golden obso_epv_source value test is what guards against a regression to synthetic EPV.
    out = add_obso(
        out,
        tracking_df,
        links=links,
        xt=xt,
        pitch_control_cache=pc_cache,
        pitch_control_method="spearman",
    )

    # Step 17: PAUSA (depends on OBSO columns from Step 16)
    out = add_pausa(out, tracking_df, links=links, pitch_control_method="spearman")

    # Step 18: Space creation
    out = add_space_creation(out, tracking_df, links=links, pitch_control_cache=pc_cache, xt=xt)

    # Step 19: ELASTIC sync
    out = add_elastic_sync(out, tracking_df)

    # Step 20: Sync score
    out = add_sync_score(out, links)

    # Step 21: xShotOccurrence (xS) — P(shot attempted); Pipping-Gamón, Feng & Sabin (2026),
    # arXiv:2512.00203. Bundled "default" XGBoost (model=None; no network, serverless-safe).
    # Reuse the shared pitch-control cache. ADR-039.
    out = add_xshot_occurrence(out, tracking_df, model=None, links=links, pitch_control_cache=pc_cache)

    # Step 21b: Shot goalmouth crossing (TF-48; Anzer & Bauer 2021). Pure ball-trajectory geometry
    # over the post-shot frames — post-contact outcome, so NOT a VAEP feature (upstream ADR-030
    # leakage guard). NaN/NA for non-shot / unresolved rows. Tracking-derived → lives only in
    # fct_action_context (the Kimball tracking-context fact), never the actions-level lineage.
    out = add_shot_goalmouth(out, tracking_df, links=links)

    # Step 22: Structural-pass primitives (TF-45; Karakus & Arkadas 2026, arXiv:2603.28916).
    # No xt / no pitch control. NaN for non-pass/non-cross + non-possessing-team actions.
    out = add_structural_pass(out, tracking_df, links=links)

    # Step 23: Player influence (xt positional; shared pitch-control cache; spearman = velocity-aware).
    out = add_player_influence(
        out,
        tracking_df,
        xt,
        links=links,
        method="spearman",
        pitch_control_cache=pc_cache,
    )

    # Step 24: xCrossAttempt (bundled "default" public model; no network; shared cache).
    # NaN for non-possessing-team action at the linked frame. actions_for_context supplies score_diff.
    out = add_xcross_attempt(
        out,
        tracking_df,
        model=None,
        links=links,
        home_team_id=home_team_id,
        actions_for_context=actions_df,
        pitch_control_cache=pc_cache,
    )

    # Step 25: GK-distribution GEOMETRY RESOLUTION (spec §7.4 — v1 xt_gk metric RETIRED, replaced by
    # the xt_gk_v2 mart-join). The v1 `add_xt_gk` metric + the 5-preset `compute_xt_gk` loop are gone;
    # what stays is the geometry resolution the v2 writer depends on. `apply_resolved_gk_geometry`
    # (silly-kicks xtgk) reads `xt_gk_origin_x/_y` + `xt_gk_dest_x/_y` by default to override the
    # GK-distribution start/end coords with the resolved keeper geometry — but that resolution needs
    # tracking frames, which the writer lacks, so the drain persists it here. This mirrors what
    # `compute_xt_gk` did internally: distrust a broadcast provider's native origin, resolve, and
    # write the 4 `_COORD_COLS` for in-scope rows only (NaN off-scope). native_origin_is_trusted +
    # _resolve_single_provider + _gk_distribution_mask are imported from the same sanctioned private
    # submodules the guard already lists (exec_visibility._SK_GUARD_SUBMODULES).
    from silly_kicks.tracking._gk_geometry import native_origin_is_trusted
    from silly_kicks.tracking._xt_gk import (
        _gk_distribution_mask,
        _resolve_completion_for_frames,
        _resolve_single_provider,
    )

    _distrust = not native_origin_is_trusted(_resolve_single_provider(tracking_df))
    _geom = resolve_gk_geometry(out, frames=tracking_df, links=links, distrust_native_origin=_distrust)
    _in_scope = _gk_distribution_mask(out, tracking_df)
    for _dst, _src in (
        ("xt_gk_origin_x", "origin_x"),
        ("xt_gk_origin_y", "origin_y"),
        ("xt_gk_dest_x", "dest_x"),
        ("xt_gk_dest_y", "dest_y"),
    ):
        out[_dst] = float("nan")
        out.loc[_in_scope, _dst] = _geom.loc[_in_scope, _src].to_numpy()

    # Step 26: GK-distribution completion probability — the exact P(success) the keeper-completion model
    # scores (shared geometry + scoring path, masked to in-scope GK distributions; NaN out-of-scope).
    # The provider-aware completion variant is resolved via the SAME private resolver the retired
    # `add_xt_gk` used: the mapper returns key "gs" but the bundled weights dir is "default"
    # (from_variant("gs") → FileNotFoundError; the resolver owns that fallback).
    _completion, _variant_key = _resolve_completion_for_frames(tracking_df, None)
    out = add_gk_completion(out, tracking_df, model=_completion, links=links)

    # Step 26b: goal-kick actor override (silly-kicks 4.39.0 acting_gk_from_frames) — credit
    # goal-kicks to the acting keeper, overriding the carrier-derived NULL-taker fill. Placed
    # AFTER the xt_gk family so it relabels the actor only (values/origins invariant). See the
    # helper docstring for scope (goal-kicks only, non-NaN resolver only).
    out = _override_goalkick_actor_from_frames(out, tracking_df)

    # Step 26c: GK-distribution domain marker (silly-kicks 4.43.0 gk_distribution_mask) — True for
    # any goal-kick OR an open-play pass/throw-in whose actor is the acting-team GK. FULL domain
    # here (frames present). resolve_gk="robust" pins the SAME acting_gk_from_frames resolver as the
    # Step-26b goal-kick actor override, so the domain marker and the taker override agree (robust ⊆
    # native — it tightens stale/substituted keepers, never broadens). The actor-independent
    # goal-kick term is unaffected by 26b's relabel and open-play actors are untouched by it, so
    # placement after 26b is value-invariant. Never NULL: the mask always returns a bool per action.
    # silly-kicks' rho retention loader consumes this column on fct_action_context.
    out["is_gk_distribution"] = gk_distribution_mask(out, tracking_df, resolve_gk="robust")

    # Step 27: Off-ball run values (TF-35, silly-kicks 4.52; ADR-042). Values the receiver's own run
    # plus disruptive teammate runs against the fitted GLOBAL xT grid; reuses the shared pitch-control
    # cache. NA/<NA> off-domain (domain = completed pass/cross with a resolved receiver). Emits
    # run_value_target/_disruptive_sum/_enabled_pass (DOUBLE) + n_disruptive_runs/n_valued_disruptive_runs
    # (Int64). NOT a VAEP feature here — persisted to fct_action_context only.
    out = add_off_ball_run_values(out, tracking_df, xt, links=links, pitch_control_cache=pc_cache)

    # Step 28: Press commitment (TF-51, silly-kicks 4.61). Per-action pressing-defender cue
    # (+ committing / - containing), closing speed (m/s) and provenance. Direction-agnostic (relative
    # defender->actor axis) — no home_team_id. Honest-NaN off-domain / on velocity-less frames.
    out = add_press_commitment(out, tracking_df, links=links)

    # Step 29: Packing (TF-49, silly-kicks 4.50; Impect-faithful bypass counts). Emits packing_made/
    # packing_goal_threat (Int64), packing_net (DOUBLE), packing_receiver_player_id (native-id
    # passthrough) and packing_secured (boolean). NaN/<NA> off-domain (domain = successful pass/cross).
    out = add_packing(out, tracking_df, links=links)

    # Provenance: the persisted pitch-control-derived metrics on the tracking path use spearman.
    # (silly-kicks 4.87.0 retired the ghost_gk_method KDE-backend provenance column — the KDE backend
    # is resolved upstream on the default predict_density path; kde_backend is no longer persisted.)
    out["pitch_control_method"] = "spearman"

    return out


def _enrich_sb360_match(
    actions_df: pd.DataFrame,
    freeze_frames: pd.DataFrame,
    home_team_id: str,
    xt: ExpectedThreat,
    kde_backend: str = "fft-cic",
    sb360_raw_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Enrichment chain for StatsBomb 360 matches.

    Uses snapshot_to_tracking_frames to convert per-event freeze-frame snapshots into synthetic
    tracking frames, then runs every single-frame-supportable enrichment (ADR-039). Velocity- and
    temporal-dependent features (DAS, cover_shadows, pre_shot_gk, off-ball, space-creation, elastic,
    xcross_attempt, ghost_gk) remain NULL. Pitch-control-dependent metrics use voronoi (position-only —
    freeze-frames have no velocity); pitch_control_method='voronoi' records the provenance. The
    standalone pitch_control_at_target__voronoi column IS emitted (ADR-056); the spearman/fernandez
    variants stay NULL (velocity-dependent). Ghost-GK is excluded (ADR-058 — velocity-dependent model,
    degenerate on freeze-frames). All partial/sparse.

    ``kde_backend`` is retained for caller/signature compatibility but is no longer used (it only fed
    ghost-GK, which no longer runs on this path).

    ``sb360_raw_df`` is the RAW ``bronze.statsbomb_360`` slice (``id`` + ``visible_area`` STRING) for
    this match — threaded ONLY from the production cogroup UDF (``ingestion.action_context``). When
    supplied, the silly-kicks 4.87.0 visibility-coverage columns populate (spec §7.1/§7.5): the
    ``add_action_context`` companions (6) + ``add_visible_area_coverage`` (2). ``None`` on the local
    hexagon path (the raw freeze-frame df is not threaded there) leaves those 8 columns NaN/None via
    ``build_output``. SB360-only; moot for live data (SB360 AC held/empty, ADR-058), correct for tests
    and future enable.
    """
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context
    from silly_kicks.tracking import (
        add_action_context,
        add_defensive_line,
        add_line_break,
        add_obso,
        add_pausa,
        add_pressure_on_actor,
        add_shape_graph,
        add_structural_pass,
        add_team_shape,
        add_visible_area_coverage,
        add_xshot_occurrence,
        gk_distribution_mask,
        pitch_control_at_target,
        resolve_defended_goals,
        snapshot_to_tracking_frames,
    )
    from silly_kicks.tracking.features import add_gk_influence, add_player_influence

    from analytics.action_context.visible_area import build_visible_area

    # Step 0: Actions-only enrichments
    out = add_game_state(actions_df)
    # GK resolution — SPADL-only (no frames=). Snapshot frames lack temporal
    # continuity for GK tracking fallback; positional features run post-conversion.
    out = add_pre_shot_gk_context(out)

    # GK-distribution domain marker (silly-kicks 4.43.0) — SB360 gets GOAL-KICKS-ONLY via
    # frames=None. SB360 synthetic frames (snapshot_to_tracking_frames below) are shot-centric —
    # freeze-frames exist at shot/event moments, not at goal-kick/GK-pass moments — so the acting-GK
    # open-play-pass term is undetectable on this arm; frames=None is the honest, documented
    # coverage (never a hand-rolled type_id==22 OR — the mask owns the goal-kick term). Computed
    # BEFORE the empty-frames early return so the returned slice always carries the column, and
    # BEFORE frame conversion since goal-kicks-only needs actions only. Never NULL.
    out["is_gk_distribution"] = gk_distribution_mask(out, frames=None)

    # Step 1: Convert freeze-frames to synthetic tracking frames + links.
    frames, links = snapshot_to_tracking_frames(freeze_frames, out)

    # Frames-required (ADR-057): a sb360 match whose freeze-frames convert to ZERO synthetic
    # frames produces NO rows. The production edge (the cogroup UDF / _process_statsbomb_matches,
    # ADR-058) handles this via build_output of the empty result; the pure core just returns empty.
    if len(frames) == 0:
        return out.iloc[0:0]

    # Direction map (silly-kicks 4.53+): resolve defended goals once on the synthetic frames for the
    # aggregators that accept goal_map= (defensive_line, gk_influence). The rest self-resolve, so the
    # 4.87.0 migration drops the retired home_team_id= kwarg. (SB360 AC is held/empty — code-correct.)
    goal_map = resolve_defended_goals(frames)

    # Visibility coverage (silly-kicks 4.87.0; spec §7.1/§7.5) — SB360-only. Build the action_id ->
    # polygon frame from the RAW 360 visible_area STRING; None on the local hexagon (raw df not
    # threaded there) → the 8 visibility columns fill NaN/None via build_output. The polygon is keyed
    # canonically (ADR-019) and coordinate-consistent (SPADL) with `out`'s SPADL actions.
    visible_area = build_visible_area(actions_df, sb360_raw_df) if sb360_raw_df is not None else None

    # Step 2: Single-frame positional features. Passing visible_area= appends the 6 *_observed_*
    # companions ({nearest_defender_distance,receiver_zone_density,defenders_in_triangle_to_goal} x
    # {fraction,source}); the 4 primary columns are byte-identical with/without it (opt-in, additive).
    out = add_action_context(out, frames, links=links, visible_area=visible_area)

    # Observed pitch fraction + provenance (visible_area_fraction/source) — the 2 base visibility
    # columns. links lets it tag unlinked actions distinctly from no_polygon.
    if visible_area is not None:
        out = add_visible_area_coverage(out, visible_area=visible_area, links=links)

    # Step 3: Defensive line
    out = add_defensive_line(out, frames, links=links, goal_map=goal_map)

    # Step 4: Ward line-breaking — primary SB360 value-add
    out = add_line_break(out, frames, links=links, method="ward")

    # Step 5: Team shape
    out = add_team_shape(out, frames, links=links)

    # SB360 coverage (ADR-039): the remaining single-frame-supportable metrics. Pitch-control-
    # dependent ones use voronoi (no velocity on freeze-frames; spearman returns all-NaN). All
    # partial/sparse — honest NULL where the freeze-frame lacks the needed players.
    out = add_pressure_on_actor(out, frames, links=links)
    out = add_shape_graph(out, frames, links=links)

    # Ghost-GK is deliberately NOT run on SB360 (ADR-058). It is a velocity-aware tracking model:
    # 5 of its 26 features are velocity-derived (ball_vx/vy, ball_speed, defensive_line_speed,
    # defending_centroid_vx) and it is fit on full continuous tracking. Freeze-frames have no
    # velocity, so those features are NaN, which degenerates the tree-ensemble leaf-matching the
    # KDE depends on → ~7% action coverage with ~85% of those clamped off-pitch (measured locally
    # on match 3788746). Honest NULL beats clamped garbage; ghost_gk_x/y/density_spread/method stay
    # NULL (build_output fills). Ghost-GK runs only on the full-tracking path (_enrich_tracking_match).

    # Pitch control at the action target (ADR-056): voronoi ONLY — it is position-only, so it works
    # on freeze-frames. spearman/fernandez need velocity and return all-NaN, so those two columns
    # stay NULL (build_output fills). This is the headline at_target feature, previously absent on
    # SB360 (the path used voronoi internally for gk_influence/obso/pausa but never emitted the
    # standalone pitch_control_at_target column).
    _pc_voronoi = pitch_control_at_target(out, frames, links=links, method="voronoi")
    out[_pc_voronoi.name] = _pc_voronoi.values

    out = add_gk_influence(
        out,
        frames,
        xt,
        links=links,
        goal_map=goal_map,
        method="voronoi",
        zone_names=["six_yard_box", "near_post", "far_post"],
    )
    out = add_obso(out, frames, links=links, xt=xt, pitch_control_method="voronoi")
    out = add_pausa(out, frames, links=links, pitch_control_method="voronoi")
    out = add_xshot_occurrence(out, frames, model=None, links=links)

    # Structural-pass (single-frame supportable; no pitch control / no velocity).
    out = add_structural_pass(out, frames, links=links)
    # Player influence — voronoi (freeze-frames have no velocity; spearman returns all-NaN).
    out = add_player_influence(out, frames, xt, links=links, method="voronoi")
    # NOTE: add_xcross_attempt is NOT run on SB360 — its extract_xcross_features hard-requires ball
    # velocity (`vx`), which freeze-frames lack (raises KeyError, not honest-NaN). xcross is therefore
    # velocity-dependent like DAS / cover_shadows / pre_shot_gk and stays NULL on SB360 (build_output
    # fills it). It runs only on the full-tracking path.

    # Provenance: the persisted pitch-control-derived metrics on SB360 use voronoi (ADR-039).
    # (Ghost-GK is not run on SB360 anyway — ADR-058; and its provenance column was retired at
    # silly-kicks 4.87.0.)
    out["pitch_control_method"] = "voronoi"

    return out
