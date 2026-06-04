"""Action-context enrichment tiers (pure pandas/numpy/silly_kicks).

Moved verbatim from ``ingestion.action_context`` (behavior-preserving). Three
tiers — tracking (full ~21-step chain), SB360 (synthetic freeze-frames), and
event-only — plus the mutate-then-restore identity resolver. No pyspark; runs
identically on a Spark executor (inside the applyInPandas UDF) and locally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from silly_kicks.xthreat import ExpectedThreat


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
    surrounding 250-frame window is entirely dead-ball and ``infer_ball_carrier``
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


def _enrich_tracking_match(
    actions_df: pd.DataFrame,
    tracking_df: pd.DataFrame,
    xt: ExpectedThreat,
    home_team_id: str,
    kde_backend: str = "fft-cic",
) -> pd.DataFrame:
    """Full enrichment chain for tracking providers.

    ``kde_backend`` selects the ghost-GK KDE backend (resolved upstream; default ``fft-cic``) and is
    recorded per-row in ``ghost_gk_method``. See spec section 4.2 for the complete call graph.
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
        add_xshot_occurrence,
        derive_team_in_possession,
        infer_ball_carrier,
        link_actions_to_frames,
        pitch_control_at_action,
    )

    # add_ghost_gk + PitchControlCache are not re-exported from the silly_kicks.tracking
    # namespace (3.25.0) — import from their defining modules.
    from silly_kicks.tracking.features import add_ghost_gk
    from silly_kicks.tracking.pitch_control import PitchControlCache

    # Step 0: Actions-only enrichments (no tracking needed)
    out = add_game_state(actions_df)

    # Step 1: Frame linkage — computed ONCE; links passed to every add_* call.
    links, _report = link_actions_to_frames(out, tracking_df)

    # One shared per-frame pitch-control surface cache for this batch (silly-kicks 3.25.0
    # TF-7): obso / cover_shadows / gk_influence / space_creation / pitch_control_at_action
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
        s = pitch_control_at_action(out, tracking_df, links=links, method=method, pitch_control_cache=pc_cache)
        out[s.name] = s.values

    # Step 7: Defensive line
    out = add_defensive_line(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 8: Off-ball context (umbrella — includes off-ball-run columns)
    out = add_off_ball_context(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 9: Ward line-breaking
    out = add_line_break(out, tracking_df, links=links, method="ward", home_team_id=home_team_id)

    # Step 10: Team shape
    out = add_team_shape(out, tracking_df, links=links, home_team_id=home_team_id)

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
    out = add_ghost_gk(
        out,
        tracking_df,
        model="default",
        links=links,
        home_team_id=home_team_id,
        actions_for_context=actions_df,
        kde_backend=kde_backend,
    )

    # Step 13: GK influence (xt positional). Explicit method="spearman" (velocity-aware; full
    # tracking has velocity) keeps the pitch_control_method provenance label honest if the
    # silly-kicks default ever changes. zone_names persists near/far-post closing-time too.
    out = add_gk_influence(
        out,
        tracking_df,
        xt,
        links=links,
        home_team_id=home_team_id,
        pitch_control_cache=pc_cache,
        method="spearman",
        zone_names=["six_yard_box", "near_post", "far_post"],
    )

    # Step 14: Cover shadows (xt positional). detailed=True: the cheap fixed-cast default
    # only affects max_single_defender_blocking_score, where it diverges from the accurate
    # per-defender counterfactual by more than that column's own observed range. ~1.5x the
    # cover_shadows cost (scales with action count, not frames; not on the critical path).
    out = add_cover_shadows(
        out, tracking_df, xt, links=links, home_team_id=home_team_id, detailed=True, pitch_control_cache=pc_cache
    )

    # Step 15: Shape graph — silly-kicks 3.25.0 restricts the per-frame snapshot computation
    # to action-linked frames internally when `links` is supplied (bit-identical).
    out = add_shape_graph(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 16: OBSO — MUST precede add_pausa. Explicit spearman (provenance honesty).
    out = add_obso(
        out,
        tracking_df,
        links=links,
        home_team_id=home_team_id,
        pitch_control_cache=pc_cache,
        pitch_control_method="spearman",
    )

    # Step 17: PAUSA (depends on OBSO columns from Step 16)
    out = add_pausa(out, tracking_df, links=links, home_team_id=home_team_id, pitch_control_method="spearman")

    # Step 18: Space creation
    out = add_space_creation(out, tracking_df, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache)

    # Step 19: ELASTIC sync
    out = add_elastic_sync(out, tracking_df)

    # Step 20: Sync score
    out = add_sync_score(out, links)

    # Step 21: xShotOccurrence (xS) — P(shot attempted); Pipping-Gamón, Feng & Sabin (2026),
    # arXiv:2512.00203. Bundled "default" XGBoost (model=None; no network, serverless-safe).
    # Reuse the shared pitch-control cache. ADR-039.
    out = add_xshot_occurrence(
        out, tracking_df, model=None, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache
    )

    # Provenance: the persisted pitch-control-derived metrics on the tracking path use spearman;
    # ghost_gk_method records which KDE backend produced ghost_gk_* (scopes to ghost_gk_* only).
    out["pitch_control_method"] = "spearman"
    out["ghost_gk_method"] = kde_backend

    return out


def _enrich_sb360_match(
    actions_df: pd.DataFrame,
    freeze_frames: pd.DataFrame,
    home_team_id: str,
    xt: ExpectedThreat,
    kde_backend: str = "fft-cic",
) -> pd.DataFrame:
    """Enrichment chain for StatsBomb 360 matches.

    Uses snapshot_to_tracking_frames to convert per-event freeze-frame snapshots into synthetic
    tracking frames, then runs every single-frame-supportable enrichment (ADR-039). Velocity- and
    temporal-dependent features (DAS, cover_shadows, pre_shot_gk, off-ball, space-creation, elastic)
    remain NULL. Pitch-control-dependent metrics use voronoi (position-only — freeze-frames have no
    velocity); pitch_control_method='voronoi' records the provenance. All partial/sparse.
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
        add_team_shape,
        add_xshot_occurrence,
        snapshot_to_tracking_frames,
    )
    from silly_kicks.tracking.features import add_ghost_gk, add_gk_influence

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

    # SB360 coverage (ADR-039): the remaining single-frame-supportable metrics. Pitch-control-
    # dependent ones use voronoi (no velocity on freeze-frames; spearman returns all-NaN). All
    # partial/sparse — honest NULL where the freeze-frame lacks the needed players.
    out = add_pressure_on_actor(out, frames, links=links)
    out = add_shape_graph(out, frames, links=links, home_team_id=home_team_id)
    out = add_ghost_gk(
        out,
        frames,
        model="default",
        links=links,
        home_team_id=home_team_id,
        actions_for_context=out,
        kde_backend=kde_backend,
    )
    out = add_gk_influence(
        out,
        frames,
        xt,
        links=links,
        home_team_id=home_team_id,
        method="voronoi",
        zone_names=["six_yard_box", "near_post", "far_post"],
    )
    out = add_obso(out, frames, links=links, home_team_id=home_team_id, pitch_control_method="voronoi")
    out = add_pausa(out, frames, links=links, home_team_id=home_team_id, pitch_control_method="voronoi")
    out = add_xshot_occurrence(out, frames, model=None, links=links, home_team_id=home_team_id)

    # Provenance: the persisted pitch-control-derived metrics on SB360 use voronoi (ADR-039);
    # ghost_gk_method records the ghost-GK KDE backend (scopes to ghost_gk_* only).
    out["pitch_control_method"] = "voronoi"
    out["ghost_gk_method"] = kde_backend

    return out


def _enrich_event_only_match(actions_df: pd.DataFrame) -> pd.DataFrame:
    """Minimal enrichment for event-only providers (StatsBomb, Wyscout)."""
    from silly_kicks.spadl import add_game_state
    from silly_kicks.spadl.utils import add_pre_shot_gk_context

    out = add_game_state(actions_df)
    out = add_pre_shot_gk_context(out)
    return out
