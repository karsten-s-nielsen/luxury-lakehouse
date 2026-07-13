"""Action-context orchestration: the per-frame-batch contract + work-unit loop.

H3 (load-bearing): the shared unit of compute is ``enrich_batch`` over ONE
frame batch — NOT the whole work unit. Production runs ``enrich_batch`` once
per Spark ``groupBy(match_id, period, frame_batch_id)`` group; the local
``run_work_unit`` runs the IDENTICAL ``enrich_batch`` in a
``floor(frame/size)`` loop and concatenates. Window-dependent features
(elastic_sync, OBSO peak, sync_score) differ between a batch and a whole
slice, so the batch size is part of the domain contract, not a Spark dispatch
detail (250→2500 was a metric-definition change — ADR-047; per-provider sizes
+ run override after the 2500 IDSSE OOM — ADR-047 amendment 2, resolved via
``analytics.action_context.batching.resolve_frame_batch_size``).

M6/M11: tier dispatch (tracking / sb360 / event_only) is by the explicit ``tier``
argument, since ``provider == "statsbomb"`` resolves to sb360 vs event_only only
at runtime via the FrameBundle.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from analytics.action_context.batching import resolve_frame_batch_size
from analytics.action_context.completeness import assert_unit_action_completeness
from analytics.action_context.enrich import (
    _enrich_sb360_match,
    _enrich_tracking_match,
    _resolve_enrichment_identity,
)
from analytics.action_context.schema import RESULT_COLUMNS, build_output
from analytics.action_context.time_base_guard import assert_frames_time_base, assert_work_unit_time_base


def _resolve_match_access_tier(actions: pd.DataFrame, provider: str) -> str:
    """Per-match HF redistribution tier for the AC output (spec 2026-06-29).

    DIRECT resolution — never a dim_matches join (unmatched→NULL→fail-safe-restricted would
    silently drop public data, spec D3/M1). The per-match SPADL actions carry ``access_tier``
    (constant per match, stamped at SPADL conversion); read it. Falls back to the provider
    default ``classify_access_tier(provider, visibility=None)`` when the column is absent
    (e.g. pre-migration bronze), which is correct for the four no-feed providers.
    """
    if "access_tier" in actions.columns:
        vals = actions["access_tier"].dropna()
        if len(vals):
            return str(vals.iloc[0])
    from shared.access_tier import classify_access_tier

    return classify_access_tier(provider=provider, visibility=None).value


if TYPE_CHECKING:
    import pandas as pd

    from analytics.action_context.ports import (
        ActionsSource,
        FrameSource,
        MatchMetadataSource,
        ResultSink,
        XtSource,
    )
    from analytics.action_context.work_unit import FrameTier, MatchMeta, WorkUnit

# Frame batch sizing — IDSSE (match_id, period) groups are 1.5M+ rows, exceeding
# the 1 GB serverless UDF group cap; sub-batch by frame number. The size is
# PER-PROVIDER + run-overridable (ADR-047 amendment 2: 2500 OOMed the densest
# provider in prod once the 4.22 column families landed) and MUST resolve
# identically on the Spark driver, inside the executor UDF, and in the local
# loop (H3) — all three import resolve_frame_batch_size from
# analytics.action_context.batching (imported above).

# Only run the explicit per-batch gc.collect() when the input group is actually large
# (ADR-045). The collect exists to protect the 1 GB serverless UDF cap when a converter
# briefly holds two copies of a BIG group — but a 2500-frame batch is ~57.5K rows
# (~10-30 MB), and an unconditional full collection per batch measured 9.5% of local
# per-half wall. Gate keeps the protection where it matters, skips it where it can't.
_GC_COLLECT_MIN_ROWS = 100_000

# Tolerance (seconds) for buffering actions at batch edges.
_ACTION_TIME_BUFFER_SECONDS = 0.5

# Metrica player ID jersey regex — compiled at module level per convention.
_JERSEY_RE = re.compile(r"Player\s*(\d+)")

logger = logging.getLogger(__name__)


def _log_xt_gk_provenance(result: pd.DataFrame, native_match_id: str) -> None:
    """M2 observability (silly-kicks 4.37.0): surface the xT-GK origin-resolution provenance + the S4
    native-goalkick-out-of-region count, BEFORE build_output drops the per-row flag. The origin-source
    counts at INFO calibrate the goal-kick fix (tracking_gk vs goalkick_prior split — both land in-box);
    n_native_goalkick_out_of_region at ERROR (CLAUDE.md telemetry rule) flags a data-quality signal.
    """
    if "xt_gk_origin_source" not in result.columns:
        return  # event-only / no xt_gk in this batch — nothing to report
    from silly_kicks.tracking._xt_gk import XtGkReport

    rep = XtGkReport.from_frame(result)
    logger.info(
        "AC observability: xt_gk match=%s scored=%d origin_sources=%s",
        native_match_id,
        rep.n_scored,
        rep.origin_source_counts,
    )
    if rep.n_native_goalkick_out_of_region:
        logger.error(
            "AC observability: %d native goal-kick origin(s) out-of-region (S4) for match %s — "
            "a native goal-kick coordinate sits implausibly far from goal",
            rep.n_native_goalkick_out_of_region,
            native_match_id,
        )


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
        from silly_kicks.providers.sportec import shape_tracking_to_native
        from silly_kicks.tracking import PreprocessConfig as _PreprocessConfig
        from silly_kicks.tracking.sportec import convert_to_frames as _convert_to_frames

        sportec_input = shape_tracking_to_native(pdf)
        if len(pdf) > _GC_COLLECT_MIN_ROWS:
            _gc.collect()
        frames, _report = _convert_to_frames(
            sportec_input,
            home_team_id=meta.home_team_id,
            home_team_start_left=meta.home_start_left,
            home_team_start_left_extratime=meta.home_team_start_left_extratime,
            output_convention="ltr",
            preprocess=_PreprocessConfig(derive_velocity=True),
        )
        result_frames = frames

    elif provider == "metrica":
        from analytics.action_context.sk_frame_adapters import convert_metrica_bronze_to_frames

        game_id = int(actions["game_id"].iloc[0])
        # Per-team roster {"Home"|"Away": {jersey: pid}} from actions (we own identity, O4/ADR-016).
        # team_id_native is "metrica_<game>_<home|away>" (NOT the literal "Home"/"Away"); split on
        # the match-level home_team_id_native. Per-team (not flat) so home/away jersey collisions
        # resolve correctly. Builder maps home_players/away_players jerseys -> these pids; an
        # unmapped jersey -> synthetic "Home_<j>" id, surfaced by the adapter (D5 Hyrum guard).
        home_native = actions["home_team_id_native"].iloc[0]
        roster: dict[str, dict[str, str]] = {"Home": {}, "Away": {}}
        _seen = actions[["player_id_native", "team_id_native"]].dropna().drop_duplicates()
        for _pid_native, _team_native in zip(_seen["player_id_native"], _seen["team_id_native"], strict=False):
            _m = _JERSEY_RE.match(str(_pid_native))
            if _m:
                roster["Home" if _team_native == home_native else "Away"][_m.group(1)] = str(_pid_native)
        _prt = pdf[["frame", "period", "timestamp"]].drop_duplicates()
        _prt = _prt.rename(columns={"frame": "frame_id", "period": "period_id", "timestamp": "time_seconds"})
        result_frames, _ = convert_metrica_bronze_to_frames(
            pdf, game_id=game_id, jersey_to_player_id=roster, period_relative_time=_prt
        )

    elif provider == "skillcorner":
        from analytics.action_context.sk_frame_adapters import convert_skillcorner_bronze_to_frames

        game_id = int(actions["game_id"].iloc[0])
        _prt = pdf[["frame", "period", "timestamp"]].drop_duplicates()
        _prt = _prt.rename(columns={"frame": "frame_id", "period": "period_id", "timestamp": "time_seconds"})
        result_frames, _sc_report = convert_skillcorner_bronze_to_frames(
            pdf, game_id=game_id, home_team_id=meta.home_team_id, period_relative_time=_prt
        )
        # M2 observability (silly-kicks 4.37.0 S1): surface the within-pitch gross-off-pitch count at ERROR
        # (CLAUDE.md telemetry rule — alerts are ERROR, never warning) so a transform regression is visible.
        if getattr(_sc_report, "n_gross_off_pitch", 0):
            logger.error(
                "AC observability: skillcorner convert_to_frames flagged %d gross-off-pitch row(s) (S1 "
                "within-pitch) for game %s — investigate the native->SPADL transform upstream",
                _sc_report.n_gross_off_pitch,
                game_id,
            )
        # S2 observability (silly-kicks 4.38.0): surface implausible per-(game,team) GK resolutions at ERROR
        # (a resolved GK count >2 or 0 per team). 4.38.0 trusts the native roster is_goalkeeper flag, so this
        # is expected 0 on SkillCorner (clean 1/team); non-zero signals whole-squad contamination (the pre-4.38.0
        # per-batch positional re-derivation that flagged both full squads) — a data-quality issue to investigate.
        if getattr(_sc_report, "n_implausible_gk_teams", 0):
            logger.error(
                "AC observability: skillcorner convert_to_frames flagged %d implausible GK team(s) (S2 "
                "resolved per-(game,team) GK count >2 or 0) for game %s — investigate roster is_goalkeeper upstream",
                _sc_report.n_implausible_gk_teams,
                game_id,
            )

    elif provider == "gradientsports":
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
        if len(pdf) > _GC_COLLECT_MIN_ROWS:
            _gc.collect()
        frames, _report = _gs_convert_to_frames(
            converter_input,
            # home_team_id MUST be the native STRING (matching converter_input.team_id, which
            # gs_team_side_to_id maps to native-string ids). Passing int() here makes
            # convert_to_frames' play_left_to_right `is_home` match ZERO players, so the
            # per-period LTR flip is silently skipped — GS frames stay mis-oriented in
            # switched-end periods (P2/P4), and structural_pass's away mirror then amplifies
            # that into a ~1e8 SGM blow-up. (IDSSE/Sportec already passes the string and is
            # correct.) meta.home_team_id is declared str; do NOT re-cast to int.
            # silly-kicks' GS convert_to_frames annotates home_team_id: int (inconsistent with
            # its own sportec converter, which takes str), but the RUNTIME contract requires the
            # native-string id matching converter_input.team_id — passing the annotated int is
            # exactly the orientation bug. Ignore the (wrong) upstream annotation.
            home_team_id=meta.home_team_id,  # type: ignore[arg-type]  # see note above: upstream int annotation is wrong
            home_team_start_left=meta.home_start_left,
            home_team_start_left_extratime=meta.home_team_start_left_extratime,
            output_convention="ltr",
            preprocess=_PreprocessConfig(derive_velocity=True),
        )
        # convert_to_frames forces GS player_id/team_id to Int64; downstream compares
        # against native-STRING action ids, so realign (Int64(366) == "366" is False).
        result_frames = _cv._coerce_gradientsports_frame_ids_to_native_str(frames)

    else:
        msg = f"Unknown tracking provider: {provider}"
        raise ValueError(msg)

    # TF-23/TF-23b (ADR-035): every provider is now oriented to home-LTR UPSTREAM —
    # SkillCorner/Metrica by the silly-kicks builders' geometric net (flags omitted), idsse/GS by
    # the native-adapter geometric backstop in silly-kicks 4.34.0. The in-repo
    # correct_frames_to_home_ltr tail net is therefore redundant and DELETED; orientation
    # correctness is the cross-provider golden test_frame_orientation_golden's acceptance oracle
    # (idsse / skillcorner / gradientsports-ET 10517_p3, the exact GS-ET flip the net used to fix).
    return result_frames


def compute_ownership_anchors(
    frames: pd.DataFrame, frame_col: str, period_col: str = "period"
) -> dict[int, tuple[float, float, float]]:
    """Per-period GLOBAL frame↔time anchors ``{period: (t0, f0, slope)}`` for M13 ownership.

    Computed ONCE over the whole unit by the dispatcher (post-rebase) so every batch
    evaluates the IDENTICAL ``est_frame`` line. The per-batch fit it replaces derived
    (t0, f0, t1, f1) from each batch's own rows — with gappy tracking (SkillCorner
    broadcast: ~30% of frames missing) adjacent batches fit slightly different lines,
    and an action within ~1 frame of a batch boundary was claimed by BOTH (duplicate
    action rows 346/365 on 1899585 P1, runs v2+v3) — or potentially by neither.
    Periods with <2 distinct timestamps are omitted (ownership falls back to None →
    no de-dup, matching the legacy degenerate case).
    """
    anchors: dict[int, tuple[float, float, float]] = {}
    fr = frames.dropna(subset=[frame_col, "timestamp"]) if "timestamp" in frames.columns else frames.iloc[0:0]
    for period, g in fr.groupby(period_col):
        ts = g["timestamp"].to_numpy(dtype=float)
        fn = g[frame_col].to_numpy(dtype=float)
        lo, hi = int(ts.argmin()), int(ts.argmax())
        t0, f0, t1, f1 = ts[lo], fn[lo], ts[hi], fn[hi]
        if t1 == t0:
            continue
        anchors[int(period)] = (float(t0), float(f0), float((f1 - f0) / (t1 - t0)))  # type: ignore[arg-type]  # period is the int groupby key
    return anchors


def _owned_action_ids(
    provider: str,
    frames_pdf: pd.DataFrame,
    actions: pd.DataFrame,
    frame_batch_size: int,
    anchor: tuple[float, float, float] | None = None,
) -> set[Any] | None:
    """Action ids owned by THIS batch (M13 single-owner de-dup).

    Owner = batch whose ``frame_batch_size``-frame window contains the action's frame. The frame
    for an action time ``t`` is recovered from the linear ``frame = f0 + (t - t0)·slope``
    relationship (tracking fps is constant). ``anchor`` is the dispatcher's GLOBAL per-period
    ``(t0, f0, slope)`` (see ``compute_ownership_anchors``) — every batch then computes the
    IDENTICAL global frame for a given ``t`` and exactly one batch claims each action even on
    gappy tracking. Without an anchor, falls back to the legacy per-batch fit (direct callers
    only — the dispatchers always pass one; lockstep-tested). ``frame_batch_size`` MUST be the
    same size the dispatcher used to assign ``frame_batch_id`` (H3). Returns ``None`` (no
    de-dup) when the batch lacks the columns to evaluate the map.
    """
    frame_col = "frame_num" if provider == "gradientsports" else "frame"
    if not {"frame_batch_id", "timestamp"}.issubset(frames_pdf.columns) or frame_col not in frames_pdf.columns:
        return None
    if "time_seconds" not in actions.columns or "action_id" not in actions.columns:
        return None
    import numpy as np

    this_batch_id = int(frames_pdf["frame_batch_id"].iloc[0])
    if anchor is not None:
        t0, f0, slope = anchor
    else:
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
    owning_batch = np.floor(est_frame / frame_batch_size).astype("int64")
    action_ids = actions["action_id"].to_numpy()
    return set(action_ids[owning_batch == this_batch_id].tolist())


def enrich_batch(
    *,
    provider: str,
    tier: FrameTier,
    frames_pdf: pd.DataFrame,
    actions_records: list[dict[str, Any]],
    period: int | None,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    meta: MatchMeta,
    native_match_id: str,
    kde_backend: str = "fft-cic",
    frame_batch_size: int | None = None,
    ownership_anchors: dict[int, tuple[float, float, float]] | None = None,
) -> pd.DataFrame:
    """Enrich ONE unit of work — the shared contract called identically by prod + local.

    tracking: ``frames_pdf`` is ONE frame batch; filter actions to the batch's
    time window (±_ACTION_TIME_BUFFER_SECONDS), convert, resolve identity, run the
    full chain. ``frame_batch_size`` MUST equal the size the dispatcher used to
    assign ``frame_batch_id`` (H3 — ``None`` resolves the provider default, which
    is only correct when the dispatcher used the default too; explicit callers
    pass it through). ``ownership_anchors`` is the dispatcher's GLOBAL per-period
    M13 map (``compute_ownership_anchors``) — without it, boundary actions on gappy
    tracking can be double-claimed by adjacent batches. sb360: ``frames_pdf`` is
    the synthetic freeze-frames for the match. event_only: ``frames_pdf`` is ignored.
    """
    import pandas as pd

    if tier == "sb360":
        # ADR-058 lockstep (home + determinism): resolve home via the shared core resolver (NOT
        # meta.home_team_id) and sort actions for the dup-event tie-break, matching production.
        # NOTE: ``frames_pdf`` is still the PRE-BUILT snapshot frame (the committed fixture). The full
        # raw-freeze-frame contract (build snapshots in-core, so the hexagon also exercises
        # build_sb360_snapshots) lands with the fixture regen from full data (plan Task 7,
        # compute-gated). The production paths (_run_sb360_enrichment + the cogroup UDF) already build
        # snapshots from raw via the shared helper.
        from analytics.action_context.sb360_snapshots import resolve_home_team_id

        actions = pd.DataFrame(actions_records)
        if actions.empty:
            return _empty_result()
        actions = actions.sort_values("action_id").reset_index(drop=True)
        # xt for the SB360 voronoi pitch-control metrics. The grid params are already enrich_batch
        # arguments; the tracking branch reconstructs the same below (ADR-039).
        xt = _reconstruct_xt(xt_grid_data, xt_l, xt_w)
        result = _enrich_sb360_match(actions, frames_pdf, resolve_home_team_id(actions), xt, kde_backend=kde_backend)
        return build_output(result, native_match_id, provider, _resolve_match_access_tier(actions, provider))

    if tier != "tracking":
        raise ValueError(f"unknown action-context tier {tier!r} (frames-required; ADR-057)")

    # ── tracking tier (per-frame-batch) ──
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
    # Assign each action to exactly ONE batch — the one whose frame-batch window contains the
    # action's frame — via the global linear frame↔timestamp map (constant fps), computed
    # from this batch's frames. Identical in Spark + local since both call enrich_batch.
    resolved_batch_size = frame_batch_size if frame_batch_size is not None else resolve_frame_batch_size(provider)
    _anchor = ownership_anchors.get(int(period)) if ownership_anchors is not None and period is not None else None
    owned_action_ids = _owned_action_ids(provider, frames_pdf, actions, resolved_batch_size, anchor=_anchor)

    # M13 EARLY-RETURN: if this batch owns zero actions (all buffer-windowed actions belong
    # to adjacent batches), short-circuit before the expensive 20-step enrich chain.
    # Bit-identical to running enrich + filtering at the end — the filter would drop
    # everything anyway. Eliminates ~10s of wasted compute per zero-owned batch.
    # ``owned_action_ids is None`` means classification was skipped (no frame_batch_id /
    # timestamp), in which case we proceed unchanged (no filter applied).
    if owned_action_ids is not None and not owned_action_ids:
        return _empty_result()

    pdf = frames_pdf.drop(columns=["frame_batch_id"]) if "frame_batch_id" in frames_pdf.columns else frames_pdf
    frames = _convert_tracking_batch(provider, pdf, actions, meta)
    frames["game_id"] = int(actions["game_id"].iloc[0])

    actions = _resolve_enrichment_identity(actions, provider=provider, match_id_native=native_match_id)

    # ADR-019 dtype-contract pre-flight (4.15.0+): loud guard that the post-identity-resolve action
    # ids + the converted frame ids share comparable dtypes before the enrich chain. With the GS
    # Int64->native-str coercion in _convert_tracking_batch kept (its drop is unproven — the GS enrich
    # fixture's absolute-clock time-base guard blocks an end-to-end seam-coverage test, so per
    # Chesterton's fence the coercion is retained), every provider reaches here object/string-on-both
    # so this passes; it fails LOUD if a future change drifts an id dtype (the silent-miss class).
    from silly_kicks.tracking import validate_id_dtypes

    validate_id_dtypes(actions, frames, home_team_id=meta.home_team_id, on_mismatch="raise")

    result = _enrich_tracking_match(
        actions_df=actions, tracking_df=frames, xt=xt, home_team_id=meta.home_team_id, kde_backend=kde_backend
    )
    _log_xt_gk_provenance(result, native_match_id)
    out = build_output(result, native_match_id, provider, _resolve_match_access_tier(actions, provider))
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
    is_slice: bool = False,
) -> int:
    """Pull a work unit's inputs via ports, run the tier-appropriate enrichment, write.

    Tracking tier loops ``floor(frame/size)`` batches — size resolved per provider
    via ``resolve_frame_batch_size`` (env-overridable) — calling ``enrich_batch``
    per batch (replicating production's Spark groupBy dispatch — H3) and
    concatenates. sb360/event_only run a single ``enrich_batch`` (no frame batching).

    ``is_slice`` (ADR-067) exempts a test/golden FIXTURE from the per-unit completeness invariant.
    Fixtures carry a WINDOWED frame slice but the WHOLE match's actions
    (``extract_action_context_fixture._pull_actions`` applies no time filter), so their
    ``bronze_expected`` legitimately dwarfs ``emitted``. The Spark production driver never sets it.
    """
    import pandas as pd

    bundle = frames.frames(wu)
    actions_df = actions.actions(wu)
    actions_records: list[dict[str, Any]] = actions_df.to_dict("records")  # type: ignore[assignment]

    # Work-unit time-base guard (ADR-040): assert the work unit's actions are period-relative
    # (silly-kicks' canonical convention), not on an absolute match clock — the GradientSports
    # period-2 class. Frame-independent (action min per period); mirrors the Spark driver
    # (_process_tracking_match). Kept in lockstep by the sentinel test.
    if "time_seconds" in actions_df.columns:
        assert_work_unit_time_base(
            {
                int(p): float(s.min())  # type: ignore[arg-type]  # p is the int period groupby key
                for p, s in actions_df.dropna(subset=["time_seconds"]).groupby("period_id")["time_seconds"]
            }
        )

    xt_grid_data, xt_l, xt_w = xt.grid()
    m = meta.metadata(wu)

    frame_batch_size = resolve_frame_batch_size(wu.provider)

    common = {
        "provider": wu.provider,
        "actions_records": actions_records,
        "xt_grid_data": xt_grid_data,
        "xt_l": xt_l,
        "xt_w": xt_w,
        "meta": m,
        "native_match_id": wu.match_id,
        "kde_backend": wu.kde_backend,
        "frame_batch_size": frame_batch_size,
    }

    if bundle.tier != "tracking":
        result = enrich_batch(tier=bundle.tier, frames_pdf=bundle.frames, period=wu.period, **common)
        return sink.write(wu, result)

    # tracking tier: batch by floor(frame/size) and concat — identical to Spark dispatch.
    f = bundle.frames.copy()
    frame_col = "frame_num" if wu.provider == "gradientsports" else "frame"
    # ADD a "timestamp" alias (batch/link/owned-action logic needs it) but KEEP
    # "period_elapsed_time" — the GS converter reads the latter; a destructive rename drops it
    # and the converter KeyErrors. Mirrors the driver (action_context._process_tracking_match).
    if wu.provider == "gradientsports" and "period_elapsed_time" in f.columns:
        f["timestamp"] = f["period_elapsed_time"]
    # Metrica (ADR-040): re-base bronze "timestamp" to PERIOD-RELATIVE via the CONTINUOUS
    # frame number, keyed on each period's FIRST frame — aligns with the SPADL action
    # time_seconds (which _convert_metrica_from_bronze re-bases off the SAME min(frame) per
    # (match,period)) and is immune to Sample_Game_3's hand-curated P2 timestamp reset.
    # Mirrors action_context._process_tracking_match; lockstep via test_metrica_period_relative_time.
    if wu.provider == "metrica":
        _fr = f["frame_rate"].astype("float64").fillna(25.0) if "frame_rate" in f.columns else 25.0
        _period_min = f.groupby("period")["frame"].transform("min").astype("float64")
        f["timestamp"] = (f["frame"].astype("float64") - _period_min) / _fr
    # SkillCorner (ADR-040 amendment): bronze "timestamp" is the ABSOLUTE broadcast clock
    # (P2 = 2700s+), while SPADL action time_seconds is period-relative. The DISPATCH layer
    # (this batch window filter + M13 ownership) reads the bronze column directly and silently
    # dropped ~90% of P2 actions (2026-06-11 scoped-run census) unless re-based here. Since TF-23
    # the SC frames are produced by the silly-kicks builder (via sk_frame_adapters), whose clock
    # the adapter OVERWRITES with this same period-relative timestamp (B' map-join) — so this
    # dispatch rebase is the single owner of the SC re-base. Subtract the SAME nominal offsets the
    # builder uses (single imported constant). Lockstep via test_skillcorner_dispatch_time_base.
    if wu.provider == "skillcorner":
        # B' (TF-23): single-source the SC period offset from silly-kicks (the builder's own
        # constant); the lakehouse copy in convert.py is deleted in this PR. Values are identical
        # (guard: test_skillcorner_dispatch_time_base value-check).
        from silly_kicks.spadl.skillcorner import _PERIOD_START_SECONDS as _SKILLCORNER_PERIOD_START_SECONDS

        f["timestamp"] = f["timestamp"].astype("float64") - f["period"].map(_SKILLCORNER_PERIOD_START_SECONDS).fillna(
            0.0
        )
    # Frames-side time-base guard (ADR-040 amendment, two-sided): after ALL provider
    # re-bases, every period's earliest frame time must be near its own kickoff. A
    # frames-side absolute clock silently empties the per-batch action window — the
    # exact SkillCorner P2 class the actions-side guard above cannot see. Min-based,
    # so sparse/partial frame coverage never false-fires (unlike the rejected
    # overlap-metric draft — see time_base_guard docstring).
    if "timestamp" in f.columns:
        assert_frames_time_base(
            {
                int(p): float(s.min())  # type: ignore[arg-type]  # p is the int period groupby key
                for p, s in f.dropna(subset=["timestamp"]).groupby("period")["timestamp"]
            }
        )
    f["frame_batch_id"] = (f[frame_col] // frame_batch_size).astype("int64")

    # M13 GLOBAL ownership anchors (ADR-040 amendment 2 follow-up): one per-period
    # frame↔time line for the WHOLE unit, so every batch claims actions identically —
    # per-batch fits drift on gappy tracking and double-claim boundary actions.
    # Mirrors the Spark driver; lockstep via test_m13_global_anchor.
    common["ownership_anchors"] = compute_ownership_anchors(f, frame_col)

    parts: list[pd.DataFrame] = []
    for (period_val, _batch_id), group in f.groupby(["period", "frame_batch_id"], sort=True):
        parts.append(enrich_batch(tier="tracking", frames_pdf=group, period=int(period_val), **common))  # type: ignore[arg-type]  # period_val is the int `period` groupby key; pandas types it as Hashable

    result = pd.concat(parts, ignore_index=True) if parts else _empty_result()

    # M13 single-owner invariant: each action is owned by exactly one batch, so the
    # concatenated unit output must be action-unique. Duplicates mean the ownership
    # map de-aligned (e.g. a frames-side clock skew) — fail loud, never write dupes.
    if "action_id" in result.columns and not result.empty:
        _dupes = result["action_id"][result["action_id"].duplicated()].unique()
        if len(_dupes) > 0:
            msg = (
                f"M13 single-owner violated for {wu.provider}:{wu.match_id}:{wu.period} — "
                f"duplicate action_ids {sorted(_dupes.tolist())[:10]}"
            )
            raise RuntimeError(msg)

    written = sink.write(wu, result)

    # Per-unit completeness invariant (ADR-040 amendment): emitted rows vs the actions
    # the frames COVER (per-period frame window, post-rebase — same clock). Converts
    # silent data loss (a mis-based clock, an over-eager filter) into a loud unit
    # failure instead of a "processed" unit with 12% of rows. Window-relative so slice
    # fixtures and partial broadcast coverage stay valid. Mirrors the Spark driver.
    if "time_seconds" in actions_df.columns and "timestamp" in f.columns:
        _act = actions_df.dropna(subset=["time_seconds"])
        if wu.period is not None and "period_id" in _act.columns:
            _act = _act[_act["period_id"] == int(wu.period)]
        _windows = {
            int(p): (float(s.min()), float(s.max()))  # type: ignore[arg-type]  # p is the int period groupby key
            for p, s in f.dropna(subset=["timestamp"]).groupby("period")["timestamp"]
        }
        _times = {
            int(p): s.tolist()  # type: ignore[arg-type]  # p is the int period_id groupby key
            for p, s in _act.groupby("period_id")["time_seconds"]
        }
        assert_unit_action_completeness(
            emitted=written,
            bronze_expected=sum(len(t) for t in _times.values()),
            action_times_by_period=_times,
            frame_window_by_period=_windows,
            unit_desc=f"{wu.provider}:{wu.match_id}:{wu.period}",
            buffer_s=_ACTION_TIME_BUFFER_SECONDS,
            is_slice=is_slice,
        )
    return written
