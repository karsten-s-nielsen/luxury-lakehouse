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

* **Stage 2 — six per-mart scoring passes.** The six marts are different-cardinality (per-run / per-action /
  per-keeper-frame / per-decision / per-sample) so they cannot share one ``mapInPandas`` output schema, and
  rebuilding per mart would pay the BUILD cost 6x. Each mart instead reads the spilled built frames back
  (``spark.read.parquet`` of the unit path, re-grouped by ``(match, period)`` — the always-feasible path;
  the in-UDF UC-Volume direct-read is a post-spike optimization, spec §12.1a) and scores that ONE mart,
  letting silly-kicks' internal ``batch_size`` bound the scorer working set. Each returns its own
  ``StructType``; the driver writes it with the per-unit ``replaceWhere``.

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
    """One Stage-2 scoring pass: a bronze table + its StructType + a pure scorer over the built frames."""

    table: str
    schema: Any  # pyspark StructType (runtime import)
    #: pure scorer: (built_frames) -> output pandas DataFrame. Closes over the unit's driver-side inputs.
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
) -> dict[str, Callable[[pd.DataFrame], pd.DataFrame]]:
    """Stage-2 per-mart scorers: ``built_frames -> mart output``. Keys match ``_oracle.MART_KEYS``.

    Takes the driver-resolved actions (identity resolution is frames-free, done once on the driver —
    byte-identical to ``build_unit_inputs``' internal resolve) + the built frames from the spill + a
    locally-reconstructed xT. ``resolved_actions`` / ``xg_preds`` captured as DataFrames (dtype-faithful).
    gkdv is included only when enabled.
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
        )

    def _rest_defense(built: pd.DataFrame) -> pd.DataFrame:
        from ingestion.restdefense_writer import compute_rest_defense_samples

        return compute_rest_defense_samples(_actions(), built, _xt(), access_tier=access_tier)

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


# Providers whose dense halves OOM the whole-unit Stage-1 build → routed through the haloed frame-window
# seam (ADR-089). metrica is EXCLUDED (owner: no value; convert window-unsafe; does not OOM) → whole-unit.
# Single-sourced from the pure seam core (imported at top) so the AC drain + this site agree (no drift).
_SEAM_PROVIDERS = SEAM_PROVIDERS
#: Target CORE frames per window (fit-profile: GS built ~583 MB / 83 950 frames → ~21 k keeps built
#: ~146 MB + raw + transients well under the 1 GB UDF-worker cap). Halo (≤39 frames) is negligible on top.
_TM_WINDOW_FRAMES = 20000


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


def build_stage2_mart_sdf(
    spark: SparkSession,
    unit: WorkUnit,
    spill_dir: str,
    mart: MartSpec,
) -> SparkDataFrame:
    """Stage 2 (one mart): a LAZY Spark DataFrame that reads the spilled built frames and scores this mart.

    One ``(match, period)`` group == the whole unit; the scorer runs on an executor and sk's internal
    ``batch_size`` bounds its working set. The caller writes the result directly (``write_delta_table``) with
    the per-unit ``replaceWhere`` — no driver ``toPandas`` of the (large) frames OR the outputs; or materializes
    it with ``.count()`` for the dry-run canary.
    """
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
    "build_stage2_mart_sdf",
    "cleanup_spill",
    "run_stage1_build_and_spill",
    "spill_path",
]
