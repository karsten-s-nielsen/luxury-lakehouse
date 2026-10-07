"""Consolidated per-unit processor for the Rev-6 tracking-grain marts (ADR-037 drain fan-out).

The three driver-sequential writers (``off_ball_runs_writer`` / ``defensive_credit_writer`` /
``gkdv_writer``) each rebuilt the SAME oriented ``(actions, frames, xt)`` per unit and looped the whole
corpus on the driver. This processor builds those inputs ONCE per unit and runs the enabled scorers, so a
single ``tracking_marts`` worker-drain (mirroring ``analytics.action_context.drain``) replaces the three
sequential jobs. It satisfies ``analytics.action_context.drain.GameProcessorPort`` (``process(unit)->int``).

**Orchestration only — the scoring math is unchanged.** The pure per-mart cores are imported verbatim
from the writer modules (``compute_off_ball_runs`` / ``compute_action_defensive_credit`` /
``compute_defensive_credit_long`` / ``score_gkdv_unit``); this module only fans them out per unit and
writes each result idempotently (per-unit ``replaceWhere``). Each scorer runs in its OWN try/except that
attributes the failure and re-raises a combined unit-level error, so one broken scorer fails the WHOLE
unit (which the drain rolls forward) rather than silently dropping one of its outputs.

**gkdv is GATED OFF by default (``GKDV_ENABLED``) pending its perf project (ADR-082 amendment).** The
default run scores off_ball_runs + defensive_credit only; ``gkdv_enabled=True`` re-adds the gkdv arm.

**gkdv scoring/pooling split.** ``score_gkdv_unit`` writes per-frame keeper observations to the
``bronze.gkdv_observations`` intermediate; the whole-corpus ``pool_keepers`` reduce runs later in a
separate single-driver ``gkdv_pool`` task (there is no per-unit pooling — pooling is cross-game).

**Validation boundary (spec Part B).** ``__init__``'s xT-grid + comp/season loads and ``_write``'s Spark
write are validated by the live Part-B recompute, same posture as ``xg_shot_scorer.run_pipeline``; the
per-unit orchestration (which scorers, which tables, error attribution) is unit-tested with fakes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ingestion.defensive_credit_writer import (
    _AGG_TYPES,
    _LONG_TYPES,
    AGG_OUTPUT_COLUMNS,
    AGG_TABLE,
    LONG_OUTPUT_COLUMNS,
    LONG_TABLE,
    _read_xg_preds,
)
from ingestion.defensive_credit_writer import (
    _assert_silly_kicks_min as _dc_assert_sk,
)
from ingestion.defensive_credit_writer import (
    _struct_type as _dc_struct_type,
)
from ingestion.gk_decision_writer import (
    BRONZE_TABLE as GK_DECISION_TABLE,
)
from ingestion.gk_decision_writer import (
    _assert_silly_kicks_min as _gk_decision_assert_sk,
)
from ingestion.gk_decision_writer import (
    _bundled_completion_model,
)
from ingestion.gk_decision_writer import (
    _struct_type as _gk_decision_struct_type,
)
from ingestion.gkdv_writer import (
    _assert_silly_kicks_min as _gkdv_assert_sk,
)
from ingestion.gkdv_writer import _build_comp_season_lookup
from ingestion.off_ball_runs_writer import (
    BRONZE_TABLE as OFF_BALL_TABLE,
)
from ingestion.off_ball_runs_writer import (
    _assert_silly_kicks_min as _obr_assert_sk,
)
from ingestion.off_ball_runs_writer import (
    _struct_type as _off_ball_struct_type,
)
from ingestion.restdefense_writer import (
    BRONZE_TABLE as REST_DEFENSE_TABLE,
)
from ingestion.restdefense_writer import (
    _assert_silly_kicks_min as _rd_assert_sk,
)
from ingestion.restdefense_writer import (
    _struct_type as _rd_struct_type,
)
from ingestion.tracking_marts_driver import (
    _TRACKING_PROVIDERS,
    ac_xt_grid,
    resolve_unit_meta,
)
from shared.constants import DEFAULT_BRONZE_SCHEMA

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

    from analytics.action_context.work_unit import WorkUnit

logger = logging.getLogger(__name__)

GKDV_OBS_TABLE = "gkdv_observations"

#: gkdv scoring is RE-ENABLED (ADR-082 amendment, sk 4.128 adoption). The perf project it was gated on
#: shipped: silly-kicks 4.128 native DAS (ADR-107, @njit kernels) + the executor-distributed two-stage
#: refactor (build-once → spill → per-mart scoring) move the per-scored-frame ghost-GK DAS (doubled: actual
#: + ghost) off the 16 GB driver onto executors, so it no longer exceeds the per-unit watchdog. The gkdv
#: arm scores as the sixth Stage-2 mart (``gkdv_observations``); the cross-game ``pool_keepers`` reduce runs
#: in the separate ``tracking_marts_drain.main_gkdv_pool`` task. SINGLE SOURCE OF TRUTH for the flag: the
#: gate (``tracking_marts_gate._OUTPUT_TABLES``) now includes ``gkdv_observations`` and the pool runs.
GKDV_ENABLED = True

# ── gkdv_observations intermediate schema ──
# Derived from ``gkdv_writer.build_keeper_observations`` (the per-scored-keeper-frame grain, want_threat
# =True): ``player_id, period_id, frame_id, delta_das, delta_threat_suppression``. Plus the four identity
# columns ``score_gkdv_unit`` stamps (``data_source, game_id, competition_id, season_id``) and ``match_id``
# (stamped by the processor for the per-unit ``replaceWhere``; ``period_id`` is already present). The
# reduce (``pool_keepers``) needs ``player_id, game_id, data_source, competition_id, season_id, delta_das,
# delta_threat_suppression``; ``match_id, period_id`` exist only for the idempotent per-unit write.
_GKDV_OBS_COLUMNS: tuple[str, ...] = (
    "data_source",
    "match_id",
    "game_id",
    "competition_id",
    "season_id",
    "player_id",
    "period_id",
    "frame_id",
    "delta_das",
    "delta_threat_suppression",
)
_GKDV_OBS_TYPES: dict[str, str] = {
    "data_source": "string",
    "match_id": "string",
    "game_id": "string",
    "competition_id": "string",
    "season_id": "string",
    "player_id": "string",
    "period_id": "long",
    "frame_id": "long",
    "delta_das": "double",
    "delta_threat_suppression": "double",
}


def _gkdv_obs_struct_type() -> Any:
    """Explicit StructType for ``bronze.gkdv_observations`` (ADR-033 — never infer)."""
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    type_map = {"long": LongType(), "double": DoubleType(), "string": StringType()}
    return StructType([StructField(c, type_map[_GKDV_OBS_TYPES[c]], True) for c in _GKDV_OBS_COLUMNS])


class TrackingMartsProcessor:
    """Build one unit's inputs ONCE, run the enabled tracking-grain scorers (up to four), write per-unit.

    ``GameProcessorPort``. gkdv is gated off by default (``GKDV_ENABLED``) pending its perf project, so the
    default run scores three surfaces (off_ball_runs + defensive_credit); ``gkdv_enabled=True`` adds gkdv.

    Loads the xT grid + the ``(provider, match_id) -> (competition, season)`` lookup ONCE at construction
    (mirrors ``drain_adapters.SparkGameProcessor`` loading the xT grid once), not per unit.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str, *, gkdv_enabled: bool = GKDV_ENABLED) -> None:
        # sk-version guard: the retired writer ``run_pipeline``s each asserted this at start; keep it live
        # here (the drain's single scoring entry) so a stale silly-kicks cannot silently score any of the
        # four surfaces once Task 12 deletes those call sites ([silly_kicks-bump-version-sentinels]).
        _obr_assert_sk()
        _dc_assert_sk()
        _gkdv_assert_sk()
        _gk_decision_assert_sk()
        _rd_assert_sk()
        self._spark = spark
        self._catalog = catalog
        self._schema = schema
        # gkdv is gated off by default (GKDV_ENABLED) pending its perf project; the arg keeps the scoring
        # path testable and lets the perf project re-enable it from a tuned worker without a code fork.
        self._gkdv_enabled = gkdv_enabled
        self._logger = logging.getLogger("tracking_marts_drain")
        self._xt_grid, self._xt_l, self._xt_w = ac_xt_grid(spark, catalog, schema)
        # The per-unit (comp, season) lookup feeds ONLY the gkdv arm — skip the warehouse query when gated off.
        self._comp_season = _build_comp_season_lookup(spark, catalog, _TRACKING_PROVIDERS) if gkdv_enabled else {}
        # Struct schemas built ONCE (import pyspark.sql.types lazily inside the factories).
        self._off_ball_schema = _off_ball_struct_type()
        self._agg_schema = _dc_struct_type(AGG_OUTPUT_COLUMNS, _AGG_TYPES)
        self._long_schema = _dc_struct_type(LONG_OUTPUT_COLUMNS, _LONG_TYPES)
        self._gkdv_obs_schema = _gkdv_obs_struct_type()
        # sk4118 Phase E — gk_decision (per-decision, period-native) + rest_defense (per-action, period-native)
        # folded onto this drain.
        self._gk_decision_schema = _gk_decision_struct_type()
        self._rd_schema = _rd_struct_type()
        # The bundled PassCompletionModel is loop-invariant — build ONCE per worker (like the xT grid), not
        # per unit (gk_decision's option-set scoring needs it for every GK-distribution decision).
        self._completion_model = _bundled_completion_model()

    def _write(self, pdf: pd.DataFrame, schema: Any, table: str, where: str) -> int:
        """Write one scored slice to ``bronze.{table}`` with the per-unit ``replaceWhere`` (idempotent)."""
        from ingestion.utils import write_delta_table

        sdf = self._spark.createDataFrame(pdf, schema)
        return write_delta_table(
            sdf,
            self._catalog,
            DEFAULT_BRONZE_SCHEMA,
            table,
            replace_where=where,
            row_count=len(pdf),
            logger=self._logger,
        )

    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
        """Score the enabled tracking-grain outputs for one unit (two-stage, executor-distributed; rev3).

        ``process(unit)`` stays the per-unit unit-of-work boundary ``drain_worker`` wraps (events / watchdog /
        circuit-breaker / rollforward UNCHANGED, TM-PLAN-10). Internally it runs Stage 1 (build the unit's
        oriented frames ONCE on executors + spill to a UC-Volume Parquet) then Stage 2 (six per-mart scoring
        passes that read the spill) — frames never hit the 16 GB driver (the OOM fix). Each mart's
        dispatch+write runs in its own try/except; if ANY failed the whole unit fails (combined
        ``RuntimeError``) so the drain rolls it forward. The spill is cleaned up in a ``finally``.

        ``dry_run=True`` (the in-preflight canary, ADR-087): both stages still RUN so a compute-path defect
        surfaces, but nothing is persisted and 0 is returned.
        """
        from analytics.action_context.enrich import _resolve_enrichment_identity
        from analytics.action_context.tracking_frame_spill import built_frame_struct_type
        from ingestion.tracking_marts_dispatch import (
            MartSpec,
            build_stage2_mart_sdf,
            cleanup_spill,
            make_build_udf,
            make_mart_scorers,
            run_stage1_build_and_spill,
            spill_path,
        )
        from ingestion.tracking_marts_driver import read_raw_trk_sdf, read_unit_actions
        from ingestion.utils import write_delta_table

        if unit.period is None:
            return 0  # match-grain unit has no frames (mirrors the old read_and_build_unit_inputs no-op)
        period = int(unit.period)

        # ── Driver-side small data (actions / meta / xg / resolved-actions) → the UDF closures ──────────
        raw_actions = read_unit_actions(self._spark, self._catalog, unit.provider, unit.match_id)
        period_actions = raw_actions[raw_actions["period_id"] == period].copy()
        if raw_actions.empty or period_actions.empty:
            return 0
        meta = resolve_unit_meta(self._spark, self._catalog, unit.provider, unit.match_id)
        # Identity resolution is frames-free (byte-identical to build_unit_inputs' internal resolve, TM-PLAN-04).
        resolved_actions = _resolve_enrichment_identity(
            period_actions, provider=unit.provider, match_id_native=unit.match_id
        )
        xg_preds = _read_xg_preds(self._spark, self._catalog, unit.provider, unit.match_id)
        comp, season = self._comp_season.get((unit.provider, unit.match_id), (None, None))
        # Per-match HF redistribution tier rides per-row on the actions (ADR-064); constant per match.
        _at = resolved_actions["access_tier"].iloc[0] if "access_tier" in resolved_actions.columns else None
        access_tier = None if _at is None or (isinstance(_at, float) and _at != _at) else str(_at)

        # ── Stage 1: build + spill (executor-distributed) ──────────────────────────────────────────────
        raw_sdf = read_raw_trk_sdf(self._spark, self._catalog, unit.provider, unit.match_id, period)
        build_udf = make_build_udf(
            unit.provider, unit.match_id, period, raw_actions, meta, self._xt_grid, self._xt_l, self._xt_w
        )
        sdir = spill_path(self._catalog, self._schema, unit)
        where = f"data_source = '{unit.provider}' AND match_id = '{unit.match_id}' AND period_id = {period}"
        scorers = make_mart_scorers(
            unit.provider,
            unit.match_id,
            resolved_actions,
            self._xt_grid,
            self._xt_l,
            self._xt_w,
            xg_preds,
            meta,
            comp,
            season,
            access_tier,
            gkdv_enabled=self._gkdv_enabled,
        )
        specs: list[MartSpec] = [
            MartSpec(OFF_BALL_TABLE, self._off_ball_schema, scorers["off_ball_runs"]),
            MartSpec(AGG_TABLE, self._agg_schema, scorers["action_defensive"]),
            MartSpec(LONG_TABLE, self._long_schema, scorers["defensive_credit_attributions"]),
            MartSpec(GK_DECISION_TABLE, self._gk_decision_schema, scorers["gk_decision"]),
            MartSpec(REST_DEFENSE_TABLE, self._rd_schema, scorers["rest_defense"]),
        ]
        if self._gkdv_enabled:
            specs.append(MartSpec(GKDV_OBS_TABLE, self._gkdv_obs_schema, scorers["gkdv_observations"]))

        total = 0
        errors: list[str] = []
        try:
            run_stage1_build_and_spill(
                self._spark, unit, raw_sdf, build_udf, built_frame_struct_type(unit.provider), sdir
            )
            # ── Stage 2: six per-mart scoring passes (read the spill) ──────────────────────────────────
            for spec in specs:
                try:
                    scored_sdf = build_stage2_mart_sdf(self._spark, unit, sdir, spec)
                    if dry_run:
                        int(scored_sdf.count())  # canary: force the UDF to run; persist nothing
                    else:
                        total += write_delta_table(
                            scored_sdf,
                            self._catalog,
                            DEFAULT_BRONZE_SCHEMA,
                            spec.table,
                            replace_where=where,
                            logger=self._logger,
                        )
                except Exception as exc:  # noqa: BLE001 — attributed + re-raised as a combined unit failure
                    errors.append(f"{spec.table}: {exc}")
        finally:
            cleanup_spill(self._spark, sdir)

        if errors:
            raise RuntimeError(
                f"tracking-marts unit {unit.provider}:{unit.match_id}:{unit.period} failed: " + "; ".join(errors)
            )
        return 0 if dry_run else total
