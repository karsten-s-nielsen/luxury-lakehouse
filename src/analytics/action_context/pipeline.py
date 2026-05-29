"""Action-context orchestration: the per-250-frame-batch contract + work-unit loop.

H3 (load-bearing): the shared unit of compute is ``enrich_batch`` over ONE
250-frame batch — NOT the whole work unit. Production runs ``enrich_batch`` once
per Spark ``groupBy(match_id, period, frame_batch_id)`` group; the local
``run_work_unit`` runs the IDENTICAL ``enrich_batch`` in a ``floor(frame/250)``
loop and concatenates. Window-dependent features (elastic_sync, OBSO peak,
sync_score) differ between a 250-frame batch and a whole slice, so the batching
is part of the domain contract, not a Spark dispatch detail.

M6/M11: tier dispatch (tracking / sb360 / event_only) is by the explicit ``tier``
argument, since ``provider == "statsbomb"`` resolves to sb360 vs event_only only
at runtime via the FrameBundle.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from analytics.action_context.enrich import (
    _enrich_event_only_match,
    _enrich_sb360_match,
    _enrich_tracking_match,
    _resolve_enrichment_identity,
)
from analytics.action_context.schema import RESULT_COLUMNS, build_output

if TYPE_CHECKING:
    import pandas as pd

    from analytics.action_context.ports import (
        ActionsSource,
        FrameSource,
        MatchMetadataSource,
        ResultSink,
        XtSource,
    )
    from analytics.action_context.work_unit import MatchMeta, WorkUnit

# Frame batch size — IDSSE (match_id, period) groups are 1.5M+ rows, exceeding the
# 1 GB serverless UDF group cap; sub-batch by frame number. Identical to the value
# used by the Spark dispatch so prod and local batch the same way (H3).
_FRAME_BATCH_SIZE = 250

# Tolerance (seconds) for buffering actions at batch edges.
_ACTION_TIME_BUFFER_SECONDS = 0.5

# Metrica player ID jersey regex — compiled at module level per convention.
_JERSEY_RE = re.compile(r"Player\s*(\d+)")


def _empty_result() -> pd.DataFrame:
    import pandas as pd

    output_cols = [c for c in RESULT_COLUMNS if c != "_ingested_at"]
    return pd.DataFrame(columns=pd.Index(output_cols))


def _reconstruct_xt(xt_grid_data: list[list[float]], xt_l: int, xt_w: int) -> Any:
    import numpy as np
    from silly_kicks.xthreat import ExpectedThreat

    xt = ExpectedThreat(l=xt_l, w=xt_w)
    xt.xT = np.array(xt_grid_data, dtype=np.float64)
    return xt


def _convert_tracking_batch(
    provider: str,
    pdf: pd.DataFrame,
    actions: pd.DataFrame,
    meta: MatchMeta,
) -> pd.DataFrame:
    """Provider-specific bronze->frames conversion for ONE batch (pure domain copies)."""
    import gc as _gc

    from analytics.action_context import convert as _cv

    if provider == "idsse":
        from silly_kicks.tracking import PreprocessConfig as _PreprocessConfig
        from silly_kicks.tracking.sportec import convert_to_frames as _convert_to_frames

        sportec_input = _cv._bronze_idsse_to_sportec_input(pdf)
        _gc.collect()
        frames, _report = _convert_to_frames(
            sportec_input,
            home_team_id=meta.home_team_id,
            home_team_start_left=meta.home_start_left,
            output_convention="ltr",
            preprocess=_PreprocessConfig(derive_velocity=True),
        )
        return frames

    if provider == "metrica":
        game_id = int(actions["game_id"].iloc[0])
        _unique_pids = actions["player_id_native"].dropna().unique()
        _has_space = any(" " in str(p) for p in _unique_pids)
        _fallback_fmt = "Player {}" if _has_space else "Player{}"
        _jersey_to_pid: dict[str, str] = {}
        for _p in _unique_pids:
            _m = _JERSEY_RE.match(str(_p))
            if _m:
                _jersey_to_pid[_m.group(1)] = str(_p)
        return _cv._bronze_metrica_to_frames(
            pdf, game_id=game_id, jersey_to_pid=_jersey_to_pid, fallback_fmt=_fallback_fmt
        )

    if provider == "skillcorner":
        game_id = int(actions["game_id"].iloc[0])
        return _cv._bronze_skillcorner_to_frames(pdf, game_id=game_id)

    if provider == "gradientsports":
        from silly_kicks.tracking import PreprocessConfig as _PreprocessConfig
        from silly_kicks.tracking.gradientsports import convert_to_frames as _gs_convert_to_frames

        _gs_j2p: dict[tuple[str, str], str] = {
            (str(k[0]), str(k[1])): v for k, v in (meta.gs_jersey_to_player_id or {}).items()
        }
        converter_input = _cv._bronze_gradientsports_to_converter_input(
            pdf,
            team_side_to_id=meta.gs_team_side_to_id or {},
            jersey_to_player_id=_gs_j2p,
            gk_player_ids=frozenset(meta.gs_gk_player_ids or []),
        )
        _gc.collect()
        frames, _report = _gs_convert_to_frames(
            converter_input,
            home_team_id=int(meta.home_team_id),
            home_team_start_left=meta.home_start_left,
            output_convention="ltr",
            preprocess=_PreprocessConfig(derive_velocity=True),
        )
        return frames

    msg = f"Unknown tracking provider: {provider}"
    raise ValueError(msg)


def _owned_action_ids(provider: str, frames_pdf: pd.DataFrame, actions: pd.DataFrame) -> set[Any] | None:
    """Action ids owned by THIS batch (M13 single-owner de-dup).

    Owner = batch whose 250-frame window contains the action's frame. The frame for an
    action time ``t`` is recovered from this batch's exact linear ``frame = slope·t + c``
    relationship (tracking fps is constant), so every batch computes the same global frame
    for a given ``t`` and exactly one batch claims each action. Returns ``None`` (no de-dup)
    when the batch lacks the columns to fit the map.
    """
    frame_col = "frame_num" if provider == "gradientsports" else "frame"
    if not {"frame_batch_id", "timestamp"}.issubset(frames_pdf.columns) or frame_col not in frames_pdf.columns:
        return None
    if "time_seconds" not in actions.columns or "action_id" not in actions.columns:
        return None
    import numpy as np

    this_batch_id = int(frames_pdf["frame_batch_id"].iloc[0])
    fr = frames_pdf[[frame_col, "timestamp"]].dropna()
    if len(fr) < 2:
        return None
    ts_frames = fr["timestamp"].to_numpy(dtype=float)
    frame_nums = fr[frame_col].to_numpy(dtype=float)
    lo, hi = int(ts_frames.argmin()), int(ts_frames.argmax())
    t0, f0, t1, f1 = ts_frames[lo], frame_nums[lo], ts_frames[hi], frame_nums[hi]
    if t1 == t0:
        return None
    slope = (f1 - f0) / (t1 - t0)
    est_frame = f0 + (actions["time_seconds"].to_numpy(dtype=float) - t0) * slope
    owning_batch = np.floor(est_frame / _FRAME_BATCH_SIZE).astype("int64")
    action_ids = actions["action_id"].to_numpy()
    return set(action_ids[owning_batch == this_batch_id].tolist())


def enrich_batch(
    *,
    provider: str,
    tier: str,
    frames_pdf: pd.DataFrame,
    actions_records: list[dict[str, Any]],
    period: int | None,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    meta: MatchMeta,
    native_match_id: str,
) -> pd.DataFrame:
    """Enrich ONE unit of work — the shared contract called identically by prod + local.

    tracking: ``frames_pdf`` is ONE 250-frame batch; filter actions to the batch's
    time window (±_ACTION_TIME_BUFFER_SECONDS), convert, resolve identity, run the
    full chain. sb360: ``frames_pdf`` is the synthetic freeze-frames for the match.
    event_only: ``frames_pdf`` is ignored.
    """
    import pandas as pd

    if tier == "event_only":
        actions = pd.DataFrame(actions_records)
        if actions.empty:
            return _empty_result()
        result = _enrich_event_only_match(actions)
        return build_output(result, native_match_id, provider)

    if tier == "sb360":
        actions = pd.DataFrame(actions_records)
        if actions.empty:
            return _empty_result()
        result = _enrich_sb360_match(actions, frames_pdf, meta.home_team_id)
        return build_output(result, native_match_id, provider)

    # ── tracking tier (per-250-frame batch) ──
    xt = _reconstruct_xt(xt_grid_data, xt_l, xt_w)

    all_actions = pd.DataFrame(actions_records)
    actions = all_actions[all_actions["period_id"] == int(period)].copy() if period is not None else all_actions.copy()
    del all_actions

    # Filter actions to this batch's time window (±buffer for frame lookup at edges).
    if "time_seconds" in actions.columns and "timestamp" in frames_pdf.columns:
        t_min = float(frames_pdf["timestamp"].min()) - _ACTION_TIME_BUFFER_SECONDS
        t_max = float(frames_pdf["timestamp"].max()) + _ACTION_TIME_BUFFER_SECONDS
        actions = actions[(actions["time_seconds"] >= t_min) & (actions["time_seconds"] <= t_max)].copy()

    if actions.empty:
        return _empty_result()

    # M13 single-owner de-dup: the ±buffer window above pulls boundary actions into BOTH
    # adjacent batches (they would each emit the action → duplicate (match_id, action_id)).
    # Assign each action to exactly ONE batch — the one whose 250-frame window contains the
    # action's frame — via the global linear frame↔timestamp map (constant fps), computed
    # from this batch's frames. Identical in Spark + local since both call enrich_batch.
    owned_action_ids = _owned_action_ids(provider, frames_pdf, actions)

    pdf = frames_pdf.drop(columns=["frame_batch_id"]) if "frame_batch_id" in frames_pdf.columns else frames_pdf
    frames = _convert_tracking_batch(provider, pdf, actions, meta)
    frames["game_id"] = int(actions["game_id"].iloc[0])

    actions = _resolve_enrichment_identity(actions, provider=provider, match_id_native=native_match_id)
    result = _enrich_tracking_match(actions_df=actions, tracking_df=frames, xt=xt, home_team_id=meta.home_team_id)
    out = build_output(result, native_match_id, provider)
    if owned_action_ids is not None and "action_id" in out.columns:
        out = out[out["action_id"].isin(owned_action_ids)].copy()
    return out


def run_work_unit(
    wu: WorkUnit,
    *,
    frames: FrameSource,
    actions: ActionsSource,
    xt: XtSource,
    meta: MatchMetadataSource,
    sink: ResultSink,
) -> int:
    """Pull a work unit's inputs via ports, run the tier-appropriate enrichment, write.

    Tracking tier loops ``floor(frame/250)`` batches calling ``enrich_batch`` per
    batch (replicating production's Spark groupBy dispatch — H3) and concatenates.
    sb360/event_only run a single ``enrich_batch`` (no frame batching).
    """
    import pandas as pd

    bundle = frames.frames(wu)
    actions_df = actions.actions(wu)
    actions_records: list[dict[str, Any]] = actions_df.to_dict("records")  # type: ignore[assignment]
    xt_grid_data, xt_l, xt_w = xt.grid()
    m = meta.metadata(wu)

    common = {
        "provider": wu.provider,
        "actions_records": actions_records,
        "xt_grid_data": xt_grid_data,
        "xt_l": xt_l,
        "xt_w": xt_w,
        "meta": m,
        "native_match_id": wu.match_id,
    }

    if bundle.tier != "tracking":
        result = enrich_batch(tier=bundle.tier, frames_pdf=bundle.frames, period=wu.period, **common)
        return sink.write(wu, result)

    # tracking tier: batch by floor(frame/250) and concat — identical to Spark dispatch.
    f = bundle.frames.copy()
    frame_col = "frame_num" if wu.provider == "gradientsports" else "frame"
    if wu.provider == "gradientsports" and "period_elapsed_time" in f.columns:
        f = f.rename(columns={"period_elapsed_time": "timestamp"})
    f["frame_batch_id"] = (f[frame_col] // _FRAME_BATCH_SIZE).astype("int64")

    parts: list[pd.DataFrame] = []
    for (period_val, _batch_id), group in f.groupby(["period", "frame_batch_id"], sort=True):
        parts.append(enrich_batch(tier="tracking", frames_pdf=group, period=int(period_val), **common))  # type: ignore[arg-type]  # period_val is the int `period` groupby key; pandas types it as Hashable

    result = pd.concat(parts, ignore_index=True) if parts else _empty_result()
    return sink.write(wu, result)
