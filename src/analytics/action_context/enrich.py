"""Action-context enrichment tiers (pure pandas/numpy/silly_kicks).

Moved verbatim from ``ingestion.action_context`` (behavior-preserving). Three
tiers — tracking (full ~20-step chain), SB360 (synthetic freeze-frames), and
event-only — plus the mutate-then-restore identity resolver. No pyspark; runs
identically on a Spark executor (inside the applyInPandas UDF) and locally.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd
    from silly_kicks.xthreat import ExpectedThreat

logger = logging.getLogger(__name__)


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
    # first (ball-carrier inferred on ALL frames for correct contiguous hysteresis). Without
    # it, DAS is structurally all-NaN.
    # PERF: silly-kicks 3.25.0 restricts the expensive per-frame DAS computation to the
    # action-linked frames internally when `links` is supplied (pinning attacking direction on
    # the full frames first → bit-identical). No lakehouse-side frame filtering needed.
    _carrier = infer_ball_carrier(tracking_df)
    _frames_tip = derive_team_in_possession(tracking_df, _carrier)
    out = add_das(out, _frames_tip, links=links, chunk_size=10)
    del _carrier, _frames_tip

    # Step 12: GK spatial (requires defending_gk_player_id from Step 2)
    out = add_pre_shot_gk_position(out, tracking_df, links=links)
    out = add_pre_shot_gk_angle(out, frames=tracking_df, links=links)

    # Step 12b: Ghost-GK (silly-kicks 3.24.0+) — defending GK's model-predicted "ghost"
    # position + spread at the linked frame. Uses the bundled "default" model (~9 MB, ships
    # in the wheel → no network, safe in the no-internet Databricks UDF). actions_for_context
    # supplies score_diff / phase context.
    out = add_ghost_gk(
        out, tracking_df, model="default", links=links, home_team_id=home_team_id, actions_for_context=actions_df
    )

    # Step 13: GK influence (xt positional)
    out = add_gk_influence(out, tracking_df, xt, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache)

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

    # Step 16: OBSO — MUST precede add_pausa
    out = add_obso(out, tracking_df, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache)

    # Step 17: PAUSA (depends on OBSO columns from Step 16)
    out = add_pausa(out, tracking_df, links=links, home_team_id=home_team_id)

    # Step 18: Space creation
    out = add_space_creation(out, tracking_df, links=links, home_team_id=home_team_id, pitch_control_cache=pc_cache)

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
