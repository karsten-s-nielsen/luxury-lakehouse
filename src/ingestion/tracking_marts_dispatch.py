"""Two-stage executor-distributed dispatch for the tracking-marts drain (rev3, ADR-045 lineage).

``TrackingMartsProcessor.process(unit)`` stays the per-unit unit-of-work boundary that
``analytics.action_context.drain.drain_worker`` wraps (events / watchdog / circuit-breaker / rollforward
UNCHANGED, TM-PLAN-10). Its INTERNALS move here: instead of ``toPandas``-ing a whole ``(match, period)``
half onto the 16 GB driver and scoring there (the OOM), it runs TWO Spark stages per unit, so the frames
live on executors:

* **Stage 1 — build once + spill.** The unit's RAW tracking rows stay a Spark DataFrame; a
  ``repartition(N, keys).sortWithinPartitions(keys, frame).mapInPandas`` pass (ADR-045 — exempt from AQE
  coalescing) runs ``build_unit_inputs`` on the group and emits the BUILT oriented frames (one homogeneous
  schema). The driver writes that output to a per-unit UC-Volume Parquet (a normal Spark write, NOT an
  in-UDF FS write). Peak is the built frame alone (~0.64 GB float32 on sk 4.128), on an executor.

* **Stage 2a — driver precompute (ADR-090).** The whole-unit quantities every scoring window needs are
  computed ONCE on the driver from a bounded spill read (``precompute_stage2a``): the coverage-restricted
  actions, the GoalMap (+ gkdv ball-carrier) invariants, the gkdv possession-stride survivor set, and the
  action→window routing. Positions domain only (no grids), so it stays well under the fit-profile budget.

* **Stage 2b — six per-mart scoring passes.** The six marts are different-cardinality (per-run / per-action /
  per-keeper-frame / per-decision / per-sample) so they cannot share one ``mapInPandas`` output schema, and
  rebuilding per mart would pay the BUILD cost 6x. Each mart reads the spilled built frames back and scores
  that ONE mart (``build_stage2_mart_sdf``). A SEAM provider (the dense halves that OOM) is scored WINDOWED
  (``windowed_score_sdf``) — re-window the spill with the per-mart scoring halo, score each window injecting
  the Stage-2a invariants, keep only this window's core outputs, union == whole-unit; the resident built
  slice is bounded by ``_TM_SCORE_WINDOW_FRAMES``, not unit size (the OOM fix). metrica (non-seam, does not
  OOM) keeps the whole-unit ``(game_id, period_id)`` regroup, letting silly-kicks' internal ``batch_size``
  bound the scorer. Each returns its own ``StructType``; the driver writes it with the per-unit ``replaceWhere``.

Per-unit small data (actions / meta / xg predictions / comp-season / the bundled completion model /
access_tier) is loaded on the DRIVER and captured into the UDF closures — the scorers' former in-``process``
Spark reads (``_read_xg_preds`` / ``resolve_unit_meta``) cannot run on an executor (TM-PLAN-04). Everything
captured is small + serializable (lazy closure capture, serverless constraint).

Pure cores (``build_unit_inputs``, the scorers, the spill round-trip) are unit-tested on fixtures
(``tests/tracking_marts``); THIS module's Spark plumbing is validated by the ``_make_streaming_group_mapper``
carry/flush test (``action_context/test_adr045_perf.py``) + the live Part-B recompute, matching the posture
of ``tracking_marts_driver`` / ``xg_shot_scorer.run_pipeline``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from analytics.action_context.frame_windows import SEAM_PROVIDERS

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

    from analytics.action_context.unit_inputs import UnitInputs
    from analytics.action_context.work_unit import MatchMeta, WorkUnit

logger = logging.getLogger(__name__)

#: UC-Volume base for the Stage-1 built-frame spill. One subdir per ``(provider, match_id, period)``; the
#: volume is operator-provisioned (Terraform) — see the re-materialize runbook. Overridable for tests/ops.
SPILL_VOLUME_BASE = "/Volumes/{catalog}/{schema}/tracking_marts_build_spill"


def spill_path(catalog: str, schema: str, unit: WorkUnit) -> str:
    """Per-unit UC-Volume Parquet directory for the Stage-1 spill (overwritten per run — idempotent)."""
    base = SPILL_VOLUME_BASE.format(catalog=catalog, schema=schema)
    return f"{base}/{unit.provider}/{unit.match_id}_{int(unit.period or 0)}"


@dataclass(frozen=True)
class MartSpec:
    """One Stage-2 scoring pass: a mart key + bronze table + its StructType + a pure scorer.

    ``key`` is the ``MART_KEYS`` identity (``off_ball_runs`` / … / ``gkdv_observations``) — distinct from
    ``table`` (the bronze sink). The windowed Stage-2 dispatch (``windowed_score_sdf``) routes on it: it
    selects the per-mart scoring halo (``assign_score_windows``) and the core-output filter
    (``_keep_core_outputs`` — the gkdv frame-trim vs the action-grain anchor). ``table`` is only the write sink.
    """

    key: str
    table: str
    schema: Any  # pyspark StructType (runtime import)
    #: pure scorer: (built_frames) -> output pandas DataFrame. Closes over the unit's driver-side inputs
    #: (incl. the injected whole-unit invariants for the windowed path — ADR-090).
    score: Callable[[pd.DataFrame], pd.DataFrame]


def _frame_col(provider: str) -> str:
    """The bronze frame-ordering column (gradientsports uses ``frame_num``; others ``frame``)."""
    return "frame_num" if provider == "gradientsports" else "frame"


# ── Pure closure factories (no Spark) — the per-unit UDF bodies ────────────────────────────────────
# These capture the unit's small driver-side data (actions / meta / xt grid / xg preds / access_tier) and
# return pure-pandas functions. The Spark mapInPandas dispatch only wraps them, so they are unit-tested
# directly on fixtures (tests/tracking_marts) == the baseline, with NO Spark needed; Docker/live validates
# only the wrapping. All captured values are small + serializable (lazy closure capture).


def make_build_udf(
    provider: str,
    match_id: str,
    period: int,
    raw_actions: pd.DataFrame,
    meta: MatchMeta,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Stage-1 build closure: a group of RAW tracking rows -> the BUILT oriented frames (``inputs.frames``).

    ``raw_actions`` is captured as a DataFrame (dtype-faithful through Spark's closure pickle; a match's
    actions are small) rather than records, which would coerce Int64/category dtypes the builder depends on.
    """

    def _build(raw_group: pd.DataFrame) -> pd.DataFrame:
        from analytics.action_context.unit_inputs import build_unit_inputs
        from analytics.action_context.work_unit import FrameBundle, WorkUnit

        wu = WorkUnit(provider=provider, match_id=match_id, period=period)
        return build_unit_inputs(
            wu,
            frame_bundle=FrameBundle(tier="tracking", frames=raw_group),
            actions_df=raw_actions,
            meta=meta,
            xt_grid_data=xt_grid_data,
            xt_l=xt_l,
            xt_w=xt_w,
        ).frames

    return _build


def make_mart_scorers(
    provider: str,
    match_id: str,
    resolved_actions: pd.DataFrame,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    xg_preds: pd.DataFrame,
    meta: MatchMeta,
    competition_id: str | None,
    season_id: str | None,
    access_tier: str | None,
    *,
    gkdv_enabled: bool,
    goal_map: object | None = None,
    carrier: object | None = None,
    scored_frame_keys: object | None = None,
) -> dict[str, Callable[[pd.DataFrame], pd.DataFrame]]:
    """Stage-2 per-mart scorers: ``built_frames -> mart output``. Keys match ``_oracle.MART_KEYS``.

    Takes the driver-resolved actions (identity resolution is frames-free, done once on the driver —
    byte-identical to ``build_unit_inputs``' internal resolve) + the built frames from the spill + a
    locally-reconstructed xT. ``resolved_actions`` / ``xg_preds`` captured as DataFrames (dtype-faithful).
    gkdv is included only when enabled.

    ``goal_map`` / ``carrier`` / ``scored_frame_keys`` (ADR-090/ADR-119): the whole-unit invariants captured
    into the invariant-consuming closures (gk_decision + rest_defense take ``goal_map``; gkdv takes all
    three) for windowed Stage-2 scoring. All ``None`` (the whole-unit caller) → the scorers resolve them
    internally, byte-identical to the pre-seam path. Built ONCE on the driver; the window dispatch calls the
    SAME closures per window and filters the output to this window's routed rows (``_keep_core_outputs``).
    """

    def _actions() -> pd.DataFrame:
        return resolved_actions

    def _xt() -> Any:
        from analytics.action_context.pipeline import _reconstruct_xt

        return _reconstruct_xt(xt_grid_data, xt_l, xt_w)

    def _actions_xg() -> pd.DataFrame:
        from ingestion.defensive_credit_writer import attach_xg

        return attach_xg(resolved_actions, xg_preds)

    def _off_ball(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.off_ball_runs_writer import compute_off_ball_runs

        return compute_off_ball_runs(_actions(), built, _xt())

    def _agg(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.defensive_credit_writer import compute_action_defensive_credit

        return compute_action_defensive_credit(_actions_xg(), built, _xt())

    def _long(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.defensive_credit_writer import compute_defensive_credit_long

        return compute_defensive_credit_long(_actions_xg(), built, _xt())

    def _gk_decision(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.gk_decision_writer import _bundled_completion_model, score_gk_decision_unit

        return score_gk_decision_unit(
            _actions(),
            built,
            _bundled_completion_model(),
            data_source=provider,
            match_id=match_id,
            access_tier=access_tier,
            goal_map=goal_map,
        )

    def _rest_defense(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.restdefense_writer import compute_rest_defense_samples

        return compute_rest_defense_samples(_actions(), built, _xt(), access_tier=access_tier, goal_map=goal_map)

    def _gkdv(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.gkdv_writer import score_gkdv_unit

        obs = score_gkdv_unit(
            built,
            meta.home_team_id,
            _xt(),
            data_source=provider,
            match_id=match_id,
            competition_id=competition_id,
            season_id=season_id,
            goal_map=goal_map,
            carrier=carrier,
            scored_frame_keys=scored_frame_keys,
        )
        obs = obs.copy()
        obs["match_id"] = match_id  # per-unit replaceWhere key (game_id carries the same value)
        return obs

    scorers: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
        "off_ball_runs": _off_ball,
        "action_defensive": _agg,
        "defensive_credit_attributions": _long,
        "gk_decision": _gk_decision,
        "rest_defense": _rest_defense,
    }
    if gkdv_enabled:
        scorers["gkdv_observations"] = _gkdv
    return scorers


# ── Stage-2a whole-unit invariants (ADR-090) — precompute once, inject into every window ───────────
# The defended-goal GoalMap (resolve_defended_goals = mean GK x per game/period/team) and the ball
# carrier (infer_ball_carrier, stateful hysteresis) are WHOLE-UNIT estimators: resolved per window they
# are smaller-sample / state-reset estimators that change the metric (skillcorner ~7.1% GoalMap shift).
# They are CHEAP (a groupby-mean; a per-frame carrier column) and read only position columns — not the
# pitch-control/DAS grids that OOM. So compute them ONCE over the whole unit and inject the SAME value
# into every window's scoring (spec §4.3), keeping windowed scoring byte-identical to whole-unit.


@dataclass(frozen=True)
class UnitInvariants:
    """The whole-unit quantities injected into Stage-2 windowed scoring (ADR-090 §4.3).

    ``goal_map`` is sk ``resolve_defended_goals`` output (the GoalMap); ``carrier`` is sk
    ``infer_ball_carrier`` output (one row per frame) or ``None`` when no gkdv is scored. Both are small
    + serializable (lazy closure capture, serverless constraint).
    """

    goal_map: Any  # sk GoalMap (runtime import)
    carrier: pd.DataFrame | None


def compute_unit_invariants(built_frames: pd.DataFrame, *, need_carrier: bool) -> UnitInvariants:
    """Compute the whole-unit invariants from a unit's BUILT frames (pure; sk only, no Spark).

    ``goal_map = resolve_defended_goals(built_frames)`` always; ``carrier = infer_ball_carrier(built_frames)``
    only when ``need_carrier`` (gkdv). Byte-identical to what the pre-seam scorers computed internally over
    the whole unit — the injection equivalence + guard-for-the-guard tests pin this (spec §6).
    """
    from silly_kicks.tracking import infer_ball_carrier, resolve_defended_goals

    goal_map = resolve_defended_goals(built_frames)
    carrier = infer_ball_carrier(built_frames) if need_carrier else None
    return UnitInvariants(goal_map=goal_map, carrier=carrier)


#: Columns the invariant precompute needs — positions / GK / ball / identity only (NOT the velocity,
#: smoothed or derived columns, and NO grids). The pruned read keeps the whole-unit invariant pass well
#: under the UDF cap (spec §4.6 fit-profile gate); a missing column falls back to the full read.
_INVARIANT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "period_id",
    "frame_id",
    "team_id",
    "player_id",
    "x",
    "y",
    "is_ball",
    "is_goalkeeper",
    "time_seconds",
    "ball_x",
    "ball_y",
)


def _read_spill_table(spill_dir: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Serverless-safe read of a spilled built-frame Parquet directory to pandas (ADR-087 follow-up 3).

    Reads via ``pyarrow.parquet.read_table().to_pandas()`` — serverless pins pyarrow 8 (NOT uv.lock), where
    ``pd.read_parquet`` is the banned class (``test_no_pd_read_parquet_on_serverless``). ``columns`` is a
    best-effort projection: names absent from the spill schema are dropped from the request (``None`` reads
    all) so sk resolves from whatever is present. ``spark`` is not needed (pyarrow reads the UC-Volume path
    directly); callers pass it only for signature symmetry with the Spark dispatch.
    """
    import pyarrow.dataset as pds
    import pyarrow.parquet as pq

    if columns is not None:
        present = set(pds.dataset(spill_dir).schema.names)
        columns = [c for c in columns if c in present] or None
    return pq.read_table(spill_dir, columns=columns).to_pandas()


def precompute_unit_invariants(spark: SparkSession, spill_dir: str, *, need_carrier: bool) -> UnitInvariants:
    """Read the spilled built frames (serverless-safe) and compute the whole-unit invariants.

    A single cheap whole-unit pass (positions domain, no grids) — bounded by the §4.6 fit-profile. The
    GoalMap (``resolve_defended_goals``) needs only positions/GK, so a GoalMap-only read prunes to
    ``_INVARIANT_COLUMNS``. The ball carrier (``infer_ball_carrier``), however, ALSO needs the velocity
    (``vx``/``vy``) + ``ball_state`` columns, which are NOT in ``_INVARIANT_COLUMNS`` — so a ``need_carrier``
    read must be FULL (still positions domain, no grids). A pruned carrier read KeyErrors on ``ball_state``
    or silently degrades to distance-only inference (caught by ``test_precompute_stage2a_equals_whole_unit``).
    ``spark`` is accepted for signature symmetry only (the read is pyarrow, UC-Volume-direct).
    """
    cols = None if need_carrier else list(_INVARIANT_COLUMNS)
    built = _read_spill_table(spill_dir, columns=cols)
    return compute_unit_invariants(built, need_carrier=need_carrier)


# ── Stage-2 windowed scoring (ADR-090) — the pure per-window core + the whole-unit driver ──────────
# A window's (core+halo) built frames are scored like the whole unit, keeping only the CORE entities:
# actions are routed to the window whose CORE frames their asof-link lands on (the scorer re-links within
# core+halo → the same frame → byte-identical, since the halo >= each mart's reach); gkdv (frame-local,
# no actions) scores (core+halo) then trims the halo-origin frame rows. The union of windows == the
# whole-unit mart output (neutrality, spec §6). This pure core is shared by the Spark dispatch
# (windowed_score_sdf, one call per (game_id, period_id, _window_id) group) and the neutrality tests.


def route_actions_to_windows(actions: pd.DataFrame, tagged: pd.DataFrame) -> dict:
    """Map each ``action_id`` → its CORE ``_window_id`` via its NEAREST core frame by time (unambiguous).

    Each action is routed to the window whose CORE frames contain its nearest-in-time frame — exactly the
    frame the whole-unit scorer resolves it to (the scorers link by NEAREST frame, NOT a 0.2 s drop: a late
    action 1.4 s past the last frame is still scored whole-unit against that frame). The window scorer then
    re-links the action within (core+halo) to the SAME frame (present as core) → byte-identical. Every
    action gets exactly one window (no drops, no dupes). A ``merge_asof(direction='nearest')`` on
    ``time_seconds`` — NOT ``link_actions_to_frames`` (whose default 0.2 s tolerance dropped action 54, the
    43/44 bug). In the Spark path the driver computes this from a tiny ``(frame_id, time_seconds)`` whole-unit
    projection (bounded) and broadcasts it; each window UDF filters by ``mapping[action_id] == window_id``.
    """
    import pandas as pd

    from analytics.action_context.frame_windows import _HALO_COL, _WINDOW_COL

    core = tagged.loc[~tagged[_HALO_COL]]
    if actions.empty or core.empty or "time_seconds" not in actions.columns:
        return {}
    frames = (
        core[["frame_id", "time_seconds", _WINDOW_COL]]
        .drop_duplicates("frame_id")
        .sort_values("time_seconds", kind="mergesort")
        .reset_index(drop=True)
    )
    acts = actions[["action_id", "time_seconds"]].sort_values("time_seconds", kind="mergesort").reset_index(drop=True)
    merged = pd.merge_asof(acts, frames[["time_seconds", _WINDOW_COL]], on="time_seconds", direction="nearest")
    return {
        aid: int(win)
        for aid, win in zip(merged["action_id"].to_numpy(), merged[_WINDOW_COL].to_numpy(), strict=True)
        if pd.notna(win)
    }


#: The identity column each action-grain mart anchors an output row to (used to keep only the outputs of
#: actions routed to THIS window). gk_decision keys ``decision_id`` (== the SPADL action_id); the rest key
#: ``action_id``. gkdv is frame-grain (trimmed by ``frame_id`` instead).
_MART_ACTION_ANCHOR: dict[str, str] = {
    "off_ball_runs": "action_id",
    "action_defensive": "action_id",
    "defensive_credit_attributions": "action_id",
    "rest_defense": "action_id",
    "gk_decision": "decision_id",
}


def restrict_actions_to_frame_coverage(actions: pd.DataFrame, frames: pd.DataFrame) -> pd.DataFrame:
    """Drop actions that link to NO frame — out-of-contract for windowed Stage-2 scoring (ADR-090).

    An action beyond the unit's frame coverage (e.g. an event logged after tracking stops) does not link
    to any frame; the whole-unit scorer still emits a DEGENERATE, frame-INDEPENDENT fallback row for it
    (zero-value credit, ``n=1``) that a single window cannot reproduce — so windowed ≠ whole-unit for it
    (owner decision 2026-10-12: exclude, do not special-case). Applied ONCE at the unit level in BOTH the
    windowed driver AND the whole-unit baseline/processor so the two agree and production cannot silently
    differ. Uses the scorers' own ``link_actions_to_frames`` so "covered" == "linkable" exactly.
    """
    from silly_kicks.tracking import link_actions_to_frames

    if actions.empty or frames.empty or "action_id" not in actions.columns:
        return actions
    linked, _report = link_actions_to_frames(actions, frames, on_low_coverage="ignore")
    covered = set(linked.loc[linked["frame_id"].notna(), "action_id"])
    return actions[actions["action_id"].isin(covered)].reset_index(drop=True)


def _score_one_window(
    mart: str,
    provider: str,
    inp: UnitInputs,
    window_tagged: pd.DataFrame,
    *,
    action_window: dict,
    goal_map: object | None = None,
    carrier: object | None = None,
    scored_frame_keys: object | None = None,
    match_id: str,
    meta: MatchMeta,
) -> pd.DataFrame:
    """Score ONE scoring-window's ``(core+halo)`` tagged built frames for ``mart`` → CORE outputs only.

    ``window_tagged`` carries ``_window_id`` + ``_is_halo``. The scorer runs on the window's (core+halo)
    frames with the WHOLE unit's actions — the action-grain scorers chain the full action sequence
    (``with_possessions``), so filtering actions per window would break possession context (the 43/44
    + ``n_defensive_credits`` bug). Instead keep only the OUTPUT rows anchored to actions ROUTED to THIS
    window (``action_window``): each action is kept in exactly one window (its nearest frame's), scored with
    full possession + its frame (core) present. ``goal_map`` / ``carrier`` are the whole-unit invariants
    injected so the defended-end flip + possession do not re-estimate per window. gkdv (frame-grain, no
    actions) trims halo-frame rows. The heavy per-frame work stays bounded by the window's frames —
    out-of-window actions drop (no in-window frame) cheaply. Shared by the Spark UDF + the neutrality driver.
    """
    from analytics.action_context.frame_windows import _HALO_COL, _WINDOW_COL
    from ingestion import tracking_marts_canary as _canary

    core_frame_ids = set(window_tagged.loc[~window_tagged[_HALO_COL], "frame_id"].unique())
    window_id = int(window_tagged[_WINDOW_COL].iloc[0])
    window_built = window_tagged.drop(columns=[_WINDOW_COL, _HALO_COL]).reset_index(drop=True)

    out = _canary.score_one_mart(
        mart,
        provider,
        inp,
        window_built,
        match_id=match_id,
        meta=meta,
        goal_map=goal_map,
        carrier=carrier,
        scored_frame_keys=scored_frame_keys,
    )
    return _keep_core_outputs(
        mart, out, core_frame_ids=core_frame_ids, window_id=window_id, action_window=action_window
    )


def _keep_core_outputs(
    mart: str, out: pd.DataFrame, *, core_frame_ids: set, window_id: int, action_window: dict
) -> pd.DataFrame:
    """Keep only the rows of a window's scored output that belong to THIS window's cores.

    Shared by the pure neutrality driver (``_score_one_window``) and the Spark ``windowed_score_sdf`` UDF
    so both discard identically: gkdv (frame-grain) keeps rows whose ``frame_id`` is a CORE frame; the
    action-grain marts keep rows whose anchor id (``_MART_ACTION_ANCHOR``) was ROUTED to this window. Each
    scored entity survives in exactly one window → the union is the whole-unit output (neutrality §6).
    """
    if mart == "gkdv_observations":
        return out[out["frame_id"].isin(core_frame_ids)].reset_index(drop=True)
    anchor = _MART_ACTION_ANCHOR[mart]
    core_action_ids = {aid for aid, win in action_window.items() if win == window_id}
    return out[out[anchor].isin(core_action_ids)].reset_index(drop=True)


def score_unit_windowed(
    mart: str,
    provider: str,
    inp: UnitInputs,
    *,
    match_id: str,
    meta: MatchMeta,
    target_window_frames: int,
) -> pd.DataFrame:
    """Score ONE mart over a unit WINDOWED (pure pandas) — the Stage-2 neutrality driver.

    Tags ``inp.frames`` with the mart's scoring windows (``assign_score_window_ids``), scores each window
    via ``_score_one_window`` (the same per-window core the Spark ``windowed_score_sdf`` UDF runs), and
    unions. The result MUST equal ``score_one_mart(..., inp.frames)`` (whole unit) — the §6 neutrality gate.
    """
    import dataclasses

    import pandas as pd

    from analytics.action_context.frame_windows import _WINDOW_COL, assign_score_window_ids
    from ingestion import tracking_marts_canary as _canary

    # Out-of-contract guard (unit level, both paths): drop actions beyond frame coverage (§ADR-090 owner
    # decision) so windowed scoring cannot silently differ from the whole-unit baseline on a degenerate row.
    inp = dataclasses.replace(inp, actions=restrict_actions_to_frame_coverage(inp.actions, inp.frames))

    # Whole-unit invariants (ADR-090 §4.3): computed ONCE over the full unit + injected into every window so
    # the defended-end GoalMap + gkdv ball-carrier do not re-estimate per window (byte-identity). Only the
    # three invariant-dependent marts need them; the frame-local marts pass None.
    need_goal_map = mart in {"rest_defense", "gk_decision", "gkdv_observations"}
    inv = compute_unit_invariants(inp.frames, need_carrier=mart == "gkdv_observations") if need_goal_map else None
    goal_map = inv.goal_map if inv is not None else None
    carrier = inv.carrier if inv is not None else None
    # gkdv's possession-stride survivor set is ALSO a whole-unit rank (ADR-119): compute it once over the
    # full unit (cheap — the §4.1 domain has no grids) + inject it into every window's build_ghost_frames,
    # else the per-window stride re-ranks and keeps different frames. sk 4.133 seam.
    scored_frame_keys = None
    if mart == "gkdv_observations":
        from silly_kicks.gkdv import select_gkdv_scored_frames

        scored_frame_keys = select_gkdv_scored_frames(inp.frames, carrier=carrier, goal_map=goal_map)

    tagged = assign_score_window_ids(inp.frames, mart, provider, target_window_frames=target_window_frames)
    action_window = {} if mart == "gkdv_observations" else route_actions_to_windows(inp.actions, tagged)
    outs = [
        _score_one_window(
            mart,
            provider,
            inp,
            grp,
            action_window=action_window,
            goal_map=goal_map,
            carrier=carrier,
            scored_frame_keys=scored_frame_keys,
            match_id=match_id,
            meta=meta,
        )
        for _wid, grp in tagged.groupby(_WINDOW_COL, sort=True)
    ]
    if not outs:  # no frames → score the (empty) whole unit for the canonical empty schema
        return _canary.score_one_mart(mart, provider, inp, inp.frames, match_id=match_id, meta=meta)
    return pd.concat(outs, ignore_index=True)


# ── Stage-2a driver precompute (ADR-090) — the PRODUCTION counterpart to score_unit_windowed's setup ──
# score_unit_windowed is the pure neutrality driver (synthetic-xG canary scorers, in-memory frames). The
# production processor instead reads the SPILL and dispatches each window to executors (windowed_score_sdf).
# The whole-unit quantities every window needs — coverage-restricted actions, the GoalMap/carrier
# invariants, the gkdv stride-survivor set, and the action→window routing — are the SAME whole-unit values;
# this computes them ONCE on the driver from a bounded spill read (§4.6 fit-profile: positions domain, no
# grids), mirroring score_unit_windowed's lines but sourced from the spill instead of inp.

#: Columns route_actions_to_windows + restrict_actions_to_frame_coverage need — a tiny projection,
#: independent of the heavy position/grid columns. ``source_provider`` + ``period_id`` + ``frame_id`` +
#: ``time_seconds`` are what sk ``link_actions_to_frames`` projects (``frames[["period_id","frame_id",
#: "time_seconds","source_provider"]]``); ``game_id`` carries the unit id for the window tagging.
_ROUTING_COLUMNS: tuple[str, ...] = ("game_id", "period_id", "frame_id", "time_seconds", "source_provider")


@dataclass(frozen=True)
class Stage2aInputs:
    """The whole-unit driver-side quantities injected into the windowed Stage-2 dispatch (ADR-090).

    ``restricted_actions`` is the unit's actions with the beyond-coverage degenerate rows dropped (§4.6a,
    applied here so the production windowed path AND the whole-unit metrica path agree with the neutrality
    baseline). ``invariants`` is the GoalMap (+ carrier for gkdv). ``scored_frame_keys`` is the gkdv
    whole-unit possession-stride survivor set (ADR-119) or ``None``. ``action_window`` maps each action_id →
    its core ``_window_id`` (mart-independent — the core ordinal) for ``_keep_core_outputs``.
    """

    restricted_actions: pd.DataFrame
    invariants: UnitInvariants
    scored_frame_keys: pd.DataFrame | None
    action_window: dict


def precompute_stage2a(
    spark: SparkSession,
    spill_dir: str,
    resolved_actions: pd.DataFrame,
    provider: str,
    *,
    gkdv_enabled: bool,
    target_window_frames: int,
) -> Stage2aInputs:
    """Compute the whole-unit Stage-2a quantities once on the driver from the spill (ADR-090 §4.3).

    Bounded reads only: the invariants use the pruned ``_INVARIANT_COLUMNS`` projection; routing +
    coverage-restriction use the tiny ``_ROUTING_COLUMNS`` projection; the gkdv stride-survivor set needs
    the full built frame (positions domain, no grids — the §4.1 cheap pass) and is read ONLY when gkdv is
    enabled. The action→window map is computed from the CORE window ordinal (mart-independent; the halo
    never moves a core frame), so one map serves every action-grain mart. Byte-identical to the values
    ``score_unit_windowed`` computes from ``inp`` — pinned by ``test_precompute_stage2a_equals_whole_unit``.
    """
    from analytics.action_context.frame_windows import assign_score_window_ids

    # gkdv needs the carrier (vx/vy/ball_state) AND the stride-survivor set, both from the full frame — read
    # it ONCE and derive both. No gkdv → the pruned GoalMap-only read suffices (cheaper).
    if gkdv_enabled:
        from silly_kicks.gkdv import select_gkdv_scored_frames

        full = _read_spill_table(spill_dir)
        inv = compute_unit_invariants(full, need_carrier=True)
        scored_frame_keys = select_gkdv_scored_frames(full, carrier=inv.carrier, goal_map=inv.goal_map)
    else:
        inv = precompute_unit_invariants(spark, spill_dir, need_carrier=False)
        scored_frame_keys = None

    link_frames = _read_spill_table(spill_dir, columns=list(_ROUTING_COLUMNS))
    restricted_actions = restrict_actions_to_frame_coverage(resolved_actions, link_frames)
    # Route on the CORE window ordinal: gkdv's halo-0 tagging yields cores only (no halo rows to filter),
    # and the core _window_id is the mart-independent ordinal every action-grain mart shares.
    tagged_core = assign_score_window_ids(
        link_frames, "gkdv_observations", provider, target_window_frames=target_window_frames
    )
    action_window = route_actions_to_windows(restricted_actions, tagged_core)

    return Stage2aInputs(
        restricted_actions=restricted_actions,
        invariants=inv,
        scored_frame_keys=scored_frame_keys,
        action_window=action_window,
    )


# Providers whose dense halves OOM the whole-unit Stage-1 build → routed through the haloed frame-window
# seam (ADR-089). metrica is EXCLUDED (owner: no value; convert window-unsafe; does not OOM) → whole-unit.
# Single-sourced from the pure seam core (imported at top) so the AC drain + this site agree (no drift).
_SEAM_PROVIDERS = SEAM_PROVIDERS
#: Target CORE frames per window (fit-profile: GS built ~583 MB / 83 950 frames → ~21 k keeps built
#: ~146 MB + raw + transients well under the 1 GB UDF-worker cap). Halo (≤39 frames) is negligible on top.
_TM_WINDOW_FRAMES = 20000

#: Target CORE frames per Stage-2 SCORING window (ADR-090) — DISTINCT from the Stage-1 build knob above.
#: Stage-2 OOMs because the whole-unit regroup makes the unit's entire built frame (~0.64 GB on a dense
#: skillcorner half) resident in ONE scoring UDF group, on top of the scorer's working set (off_ball /
#: defensive-credit per-action grids; the doubled actual+ghost ghost-GK DAS). Windowing bounds the resident
#: built slice to this many core frames (+ the mart's scoring halo), leaving headroom under the 800 MB UDF
#: working target for the scorer transients. PROVISIONAL from the offline §4.6 fit-profile; the exact lock
#: is the plan's Task 0.2/D.1 fit-profile (a scoped dense-unit canary measurement, separate approval)
#: BEFORE the 8-unit re-run — not yet executed. 8000 keeps the resident built slice ~0.06 GB (sk-4.133
#: float32) so even the doubled DAS transients stay well under cap; the neutrality tests prove any size is
#: byte-identical, so a later downward lock is safe.
_TM_SCORE_WINDOW_FRAMES = 8000


def run_stage1_build_and_spill(
    spark: SparkSession,
    unit: WorkUnit,
    raw_trk_sdf: SparkDataFrame,
    build_udf: Callable[[pd.DataFrame], pd.DataFrame],
    built_schema: Any,
    spill_dir: str,
) -> str:
    """Stage 1: distribute the per-unit build over executors, spill the built frames to ``spill_dir``.

    ``raw_trk_sdf`` is the unit's raw tracking rows (already projected to the builder's input columns, NO
    driver ``toPandas``). For a dense seam provider the build is windowed (haloed frame-windows, ADR-089)
    so the executor peak is bounded by ``_TM_WINDOW_FRAMES``, not unit size (the 16 GB-driver OOM fix);
    for an excluded provider (metrica) the whole unit is ONE ``(match, period)`` group. The union of either
    is the whole-unit built frame. ``spill_dir`` is injected (``spill_path(...)`` in prod; a local temp dir
    in the container test).
    """
    if unit.provider in _SEAM_PROVIDERS:
        from ingestion.frame_window_dispatch import windowed_build_sdf

        built_sdf = windowed_build_sdf(
            spark, raw_trk_sdf, unit.provider, build_udf, built_schema, target_window_frames=_TM_WINDOW_FRAMES
        )
    else:
        from ingestion.action_context import _UDF_SHUFFLE_PARTITIONS, _make_streaming_group_mapper

        keys = ["match_id", "period"]
        frame_col = _frame_col(unit.provider)
        sort_cols = [*keys, frame_col] if frame_col in raw_trk_sdf.columns else keys
        built_sdf = (
            raw_trk_sdf.repartition(_UDF_SHUFFLE_PARTITIONS, *keys)
            .sortWithinPartitions(*sort_cols)
            .mapInPandas(_make_streaming_group_mapper(build_udf, keys), schema=built_schema)
        )
    # Driver-orchestrated Spark write (executors write their partitions) — NOT an in-UDF FS write.
    built_sdf.write.mode("overwrite").parquet(spill_dir)
    return spill_dir


def windowed_score_sdf(
    spark: SparkSession,
    spill_dir: str,
    mart: MartSpec,
    provider: str,
    *,
    action_window: dict,
    target_window_frames: int,
) -> SparkDataFrame:
    """Stage 2 (one mart, WINDOWED): a LAZY Spark DF that scores the spilled built frames window-by-window.

    The ADR-090 Stage-2 analogue of ``windowed_build_sdf``: ``assign_score_windows`` tags the spilled built
    frames with ``(_window_id, _is_halo)`` on the per-mart SCORING halo, then a per-``(game_id, period_id,
    _window_id)`` ``mapInPandas`` scores each window's ``(core+halo)`` frames and keeps only THIS window's
    core outputs (``_keep_core_outputs``). The scorer (``mart.score``) already closes over the whole-unit
    invariants (Stage-2a) + the coverage-restricted whole-unit actions — so each window reproduces the
    whole-unit metric for its core entities and the union == the whole-unit mart output (neutrality §6). The
    resident built slice is bounded by ``target_window_frames`` (+ halo), not unit size — the OOM fix. The
    per-window core core == the pure ``_score_one_window``; THIS Spark plumbing is Docker/live-validated.
    """
    from analytics.action_context.frame_windows import _HALO_COL, _WINDOW_COL
    from ingestion.action_context import _UDF_SHUFFLE_PARTITIONS, _make_streaming_group_mapper
    from ingestion.frame_window_dispatch import assign_score_windows

    built_sdf = spark.read.parquet(spill_dir)
    tagged = assign_score_windows(built_sdf, mart.key, provider, target_window_frames=target_window_frames)
    keys = ["game_id", "period_id", _WINDOW_COL]

    def _udf(window_tagged: pd.DataFrame) -> pd.DataFrame:
        core_frame_ids = set(window_tagged.loc[~window_tagged[_HALO_COL], "frame_id"].unique())
        window_id = int(window_tagged[_WINDOW_COL].iloc[0])
        window_built = window_tagged.drop(columns=[_WINDOW_COL, _HALO_COL]).reset_index(drop=True)
        out = mart.score(window_built)
        return _keep_core_outputs(
            mart.key, out, core_frame_ids=core_frame_ids, window_id=window_id, action_window=action_window
        )

    sort_cols = [*keys, "frame_id"]
    return (
        tagged.repartition(_UDF_SHUFFLE_PARTITIONS, *keys)
        .sortWithinPartitions(*sort_cols)
        .mapInPandas(_make_streaming_group_mapper(_udf, keys), schema=mart.schema)
    )


def build_stage2_mart_sdf(
    spark: SparkSession,
    unit: WorkUnit,
    spill_dir: str,
    mart: MartSpec,
    *,
    action_window: dict,
    target_window_frames: int = _TM_SCORE_WINDOW_FRAMES,
) -> SparkDataFrame:
    """Stage 2 (one mart): a LAZY Spark DataFrame that reads the spilled built frames and scores this mart.

    Branches on the provider exactly like ``run_stage1_build_and_spill`` (Stage 1): a SEAM provider
    (skillcorner/idsse/gradientsports — the dense halves that OOM) is scored WINDOWED (``windowed_score_sdf``,
    ADR-090), bounding the resident built slice to ``target_window_frames``; the EXCLUDED provider (metrica —
    does not OOM, converter window-unsafe) keeps the whole-unit regroup (one ``(game_id, period_id)`` group ==
    the whole unit, sk's internal ``batch_size`` bounding the scorer). Both scorers carry the SAME Stage-2a
    coverage-restricted actions + injected invariants, so the two paths and the neutrality baseline agree.
    The caller writes the result directly (``write_delta_table``) with the per-unit ``replaceWhere`` — no
    driver ``toPandas`` of the frames or outputs; or materializes it with ``.count()`` for the dry-run canary.
    """
    if unit.provider in _SEAM_PROVIDERS:
        return windowed_score_sdf(
            spark,
            spill_dir,
            mart,
            unit.provider,
            action_window=action_window,
            target_window_frames=target_window_frames,
        )

    from ingestion.action_context import _UDF_SHUFFLE_PARTITIONS, _make_streaming_group_mapper

    # The spilled BUILT frame is keyed by game_id/period_id (build_unit_inputs stamps game_id + period_id;
    # it carries NO match_id/period — those are raw-bronze columns). One unit == one (game_id, period_id).
    keys = ["game_id", "period_id"]

    def _udf(built: pd.DataFrame) -> pd.DataFrame:
        return mart.score(built)

    built_sdf = spark.read.parquet(spill_dir)
    return (
        built_sdf.repartition(_UDF_SHUFFLE_PARTITIONS, *keys)
        .sortWithinPartitions(*keys)
        .mapInPandas(_make_streaming_group_mapper(_udf, keys), schema=mart.schema)
    )


def cleanup_spill(spark: SparkSession, spill_dir: str) -> None:
    """Best-effort removal of a unit's spill directory (idempotent; a re-run also overwrites it)."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        DBUtils(spark).fs.rm(spill_dir, recurse=True)
    except Exception as exc:  # noqa: BLE001 - cleanup is best-effort; the next run overwrites the path
        logger.warning("tracking-marts spill cleanup skipped for %s: %s", spill_dir, exc)


__all__ = [
    "SPILL_VOLUME_BASE",
    "MartSpec",
    "Stage2aInputs",
    "UnitInvariants",
    "build_stage2_mart_sdf",
    "cleanup_spill",
    "compute_unit_invariants",
    "precompute_stage2a",
    "precompute_unit_invariants",
    "run_stage1_build_and_spill",
    "spill_path",
    "windowed_score_sdf",
]
