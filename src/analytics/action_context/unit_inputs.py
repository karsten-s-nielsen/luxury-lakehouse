"""Per-work-unit oriented ``(actions, frames, xt)`` construction — the SHARED input seam.

The AC drain (``pipeline.run_work_unit`` / ``ingestion.action_context._process_tracking_match``)
converts each work unit's raw bronze tracking rows into **home-LTR oriented long-form frames** and
resolves the SPADL actions' identity, then runs the enrichment chain. The Rev-6 grain marts
(``fct_off_ball_runs``, ``fct_action_defensive``, ``fct_defensive_credit_attributions``) need the
IDENTICAL oriented ``(actions, frames, xt)`` — but call a DIFFERENT silly-kicks function
(``detect_off_ball_runs`` / ``add_defensive_credit`` / ``compute_defensive_credits``) instead of the
per-frame-batch enrich chain.

This module factors out that construction so the writers do not duplicate the conversion. It runs the
WHOLE unit at once (no frame-batching): the run-detection / defensive-credit functions are per-action /
per-run whole-unit computations (they link every action to its nearest frame across the unit), not the
window-dependent per-batch metrics (OBSO peak, elastic_sync) that made frame-batch size part of the AC
metric contract (ADR-047). Driver-mode, 16 GB — a per-``(match, period)`` unit's frames fit.

**Time-base rebase (lockstep).** ``rebase_frames_time_base`` mirrors the per-provider timestamp rebase
inlined in ``pipeline.run_work_unit`` and ``ingestion.action_context._process_tracking_match`` (both
carry the same block; ADR-040). The three are a lockstep set: SkillCorner's absolute broadcast clock and
Metrica's absolute match clock must be re-based to period-relative BEFORE conversion, or
``link_actions_to_frames`` finds the action window near-disjoint from the frames and silently returns
all-NaN. The writer fixture tests (``test_off_ball_runs_writer`` / ``test_defensive_credit_writer``) are
the acceptance oracle — they build a unit through this seam and assert the produced frames actually link
to the actions (non-empty runs / credits on the SkillCorner fixture), which a mis-rebase would break.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

    from analytics.action_context.work_unit import FrameBundle, MatchMeta, WorkUnit


@dataclass(frozen=True)
class UnitInputs:
    """Oriented, identity-resolved inputs for a single tracking work unit."""

    actions: pd.DataFrame
    frames: pd.DataFrame
    xt: Any  # silly_kicks.xthreat.ExpectedThreat (runtime-only import)


def rebase_frames_time_base(provider: str, frames: pd.DataFrame) -> pd.DataFrame:
    """Re-base a raw bronze frame's ``timestamp`` to PERIOD-RELATIVE seconds (ADR-040).

    Returns a COPY. Mirrors the per-provider rebase in ``pipeline.run_work_unit`` and
    ``ingestion.action_context._process_tracking_match`` (lockstep — see module docstring):

    * gradientsports: alias ``period_elapsed_time`` -> ``timestamp`` (already period-relative).
    * metrica: ``timestamp = (frame - period_min_frame) / frame_rate`` (bronze clock is absolute and
      Sample_Game_3 resets P2 to 0; frame-number based so the reset is irrelevant).
    * skillcorner: subtract the silly-kicks per-period nominal offset (bronze is the absolute
      broadcast clock, P2 = 2700 s+).
    * idsse: already period-relative — unchanged.
    """
    f = frames.copy()
    if provider == "gradientsports" and "period_elapsed_time" in f.columns:
        f["timestamp"] = f["period_elapsed_time"]
    elif provider == "metrica":
        fr = f["frame_rate"].astype("float64").fillna(25.0) if "frame_rate" in f.columns else 25.0
        period_min = f.groupby("period")["frame"].transform("min").astype("float64")
        f["timestamp"] = (f["frame"].astype("float64") - period_min) / fr
    elif provider == "skillcorner":
        # Single-source the SC period offset from silly-kicks (the builder's own constant, TF-23).
        from silly_kicks.spadl.skillcorner import _PERIOD_START_SECONDS as _SC_PERIOD_START

        f["timestamp"] = f["timestamp"].astype("float64") - f["period"].map(_SC_PERIOD_START).fillna(0.0)
    return f


def build_unit_inputs(
    wu: WorkUnit,
    *,
    frame_bundle: FrameBundle,
    actions_df: pd.DataFrame,
    meta: MatchMeta,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
) -> UnitInputs:
    """Build oriented, identity-resolved ``(actions, frames, xt)`` for ONE tracking work unit.

    Reuses the AC pipeline's conversion so the frames are byte-for-byte what the drain would build:
    rebase timestamps -> ``_convert_tracking_batch`` (home-LTR, velocity-derived) -> stamp ``game_id``
    -> ``_resolve_enrichment_identity`` (team_id/player_id -> silly-kicks-compatible native values that
    match the frames). ``xt`` is reconstructed from the global grid.

    Only the tracking tier is supported (the Rev-6 grain marts are tracking-only — they need real
    tracking frames). A statsbomb/sb360 bundle raises.
    """
    from analytics.action_context.enrich import _resolve_enrichment_identity
    from analytics.action_context.pipeline import _convert_tracking_batch, _reconstruct_xt

    if frame_bundle.tier != "tracking":
        raise ValueError(
            f"build_unit_inputs supports only the tracking tier (got {frame_bundle.tier!r}); "
            "the Rev-6 grain marts are tracking-only (frames-required)."
        )

    frames_raw = rebase_frames_time_base(wu.provider, frame_bundle.frames)

    actions = (
        actions_df[actions_df["period_id"] == int(wu.period)].copy() if wu.period is not None else actions_df.copy()
    )
    if actions.empty:
        raise ValueError(f"No actions for unit {wu.provider}:{wu.match_id}:{wu.period}")

    frames = _convert_tracking_batch(wu.provider, frames_raw, actions, meta)
    frames["game_id"] = int(actions["game_id"].iloc[0])

    actions = _resolve_enrichment_identity(actions, provider=wu.provider, match_id_native=wu.match_id)

    xt = _reconstruct_xt(xt_grid_data, xt_l, xt_w)
    return UnitInputs(actions=actions, frames=frames, xt=xt)
