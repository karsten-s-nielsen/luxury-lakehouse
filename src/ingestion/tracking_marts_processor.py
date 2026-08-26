"""Consolidated per-unit processor for the Rev-6 tracking-grain marts (ADR-037 drain fan-out).

The three driver-sequential writers (``off_ball_runs_writer`` / ``defensive_credit_writer`` /
``gkdv_writer``) each rebuilt the SAME oriented ``(actions, frames, xt)`` per unit and looped the whole
corpus on the driver. This processor builds those inputs ONCE per unit and runs all three scorers, so a
single ``tracking_marts`` worker-drain (mirroring ``analytics.action_context.drain``) replaces the three
sequential jobs. It satisfies ``analytics.action_context.drain.GameProcessorPort`` (``process(unit)->int``).

**Orchestration only — the scoring math is unchanged.** The pure per-mart cores are imported verbatim
from the writer modules (``compute_off_ball_runs`` / ``compute_action_defensive_credit`` /
``compute_defensive_credit_long`` / ``score_gkdv_unit``); this module only fans them out per unit and
writes each result idempotently (per-unit ``replaceWhere``). Each scorer runs in its OWN try/except that
attributes the failure and re-raises a combined unit-level error, so one broken scorer fails the WHOLE
unit (which the drain rolls forward) rather than silently dropping one of the four outputs.

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
    attach_xg,
    compute_action_defensive_credit,
    compute_defensive_credit_long,
)
from ingestion.defensive_credit_writer import (
    _assert_silly_kicks_min as _dc_assert_sk,
)
from ingestion.defensive_credit_writer import (
    _struct_type as _dc_struct_type,
)
from ingestion.gkdv_writer import (
    _assert_silly_kicks_min as _gkdv_assert_sk,
)
from ingestion.gkdv_writer import _build_comp_season_lookup, score_gkdv_unit
from ingestion.off_ball_runs_writer import (
    BRONZE_TABLE as OFF_BALL_TABLE,
)
from ingestion.off_ball_runs_writer import (
    _assert_silly_kicks_min as _obr_assert_sk,
)
from ingestion.off_ball_runs_writer import (
    _struct_type as _off_ball_struct_type,
)
from ingestion.off_ball_runs_writer import (
    compute_off_ball_runs,
)
from ingestion.tracking_marts_driver import (
    _TRACKING_PROVIDERS,
    ac_xt_grid,
    read_and_build_unit_inputs,
    resolve_unit_meta,
)
from shared.constants import DEFAULT_BRONZE_SCHEMA

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
    from pyspark.sql import SparkSession

    from analytics.action_context.work_unit import WorkUnit

logger = logging.getLogger(__name__)

GKDV_OBS_TABLE = "gkdv_observations"

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
    """Build one unit's inputs ONCE, run all four tracking-grain scorers, write per-unit (``GameProcessorPort``).

    Loads the xT grid + the ``(provider, match_id) -> (competition, season)`` lookup ONCE at construction
    (mirrors ``drain_adapters.SparkGameProcessor`` loading the xT grid once), not per unit.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        # sk-version guard: the retired writer ``run_pipeline``s each asserted this at start; keep it live
        # here (the drain's single scoring entry) so a stale silly-kicks cannot silently score any of the
        # four surfaces once Task 12 deletes those call sites ([silly_kicks-bump-version-sentinels]).
        _obr_assert_sk()
        _dc_assert_sk()
        _gkdv_assert_sk()
        self._spark = spark
        self._catalog = catalog
        self._schema = schema
        self._logger = logging.getLogger("tracking_marts_drain")
        self._xt_grid, self._xt_l, self._xt_w = ac_xt_grid(spark, catalog, schema)
        self._comp_season = _build_comp_season_lookup(spark, catalog, _TRACKING_PROVIDERS)
        # Struct schemas built ONCE (import pyspark.sql.types lazily inside the factories).
        self._off_ball_schema = _off_ball_struct_type()
        self._agg_schema = _dc_struct_type(AGG_OUTPUT_COLUMNS, _AGG_TYPES)
        self._long_schema = _dc_struct_type(LONG_OUTPUT_COLUMNS, _LONG_TYPES)
        self._gkdv_obs_schema = _gkdv_obs_struct_type()

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

    def process(self, unit: WorkUnit) -> int:
        """Score all four tracking-grain outputs for one unit; return the summed rows written.

        Each scorer runs in isolation and attributes its own failure; if ANY failed, the unit fails as a
        whole (combined ``RuntimeError``) so the drain rolls it forward rather than shipping a partial unit.
        """
        inputs = read_and_build_unit_inputs(
            self._spark, self._catalog, unit, xt_grid_data=self._xt_grid, xt_l=self._xt_l, xt_w=self._xt_w
        )
        if inputs is None:
            return 0

        where = (
            f"data_source = '{unit.provider}' AND match_id = '{unit.match_id}' AND period_id = {int(unit.period or 0)}"
        )
        total = 0
        errors: list[str] = []

        # off_ball_runs (fct_off_ball_runs).
        try:
            obr = compute_off_ball_runs(inputs.actions, inputs.frames, inputs.xt)
            total += self._write(obr, self._off_ball_schema, OFF_BALL_TABLE, where)
        except Exception as exc:  # noqa: BLE001 — attributed + re-raised as a combined unit failure below
            errors.append(f"off_ball_runs: {exc}")

        # defensive_credit (fct_action_defensive + fct_defensive_credit_attributions) — needs per-unit xG.
        try:
            xg_preds = _read_xg_preds(self._spark, self._catalog, unit.provider, unit.match_id)
            actions = attach_xg(inputs.actions, xg_preds)
            total += self._write(
                compute_action_defensive_credit(actions, inputs.frames, inputs.xt), self._agg_schema, AGG_TABLE, where
            )
            total += self._write(
                compute_defensive_credit_long(actions, inputs.frames, inputs.xt), self._long_schema, LONG_TABLE, where
            )
        except Exception as exc:  # noqa: BLE001 — attributed + re-raised as a combined unit failure below
            errors.append(f"defensive_credit: {exc}")

        # gkdv scoring -> bronze.gkdv_observations (pooled later in the separate gkdv_pool reduce).
        try:
            meta = resolve_unit_meta(self._spark, self._catalog, unit.provider, unit.match_id)
            comp, season = self._comp_season.get((unit.provider, unit.match_id), (None, None))
            obs = score_gkdv_unit(
                inputs.frames,
                meta.home_team_id,
                inputs.xt,
                data_source=unit.provider,
                match_id=unit.match_id,
                competition_id=comp,
                season_id=season,
            )
            obs = obs.copy()
            obs["match_id"] = unit.match_id  # for the per-unit replaceWhere (game_id carries the same value)
            total += self._write(obs[list(_GKDV_OBS_COLUMNS)], self._gkdv_obs_schema, GKDV_OBS_TABLE, where)
        except Exception as exc:  # noqa: BLE001 — attributed + re-raised as a combined unit failure below
            errors.append(f"gkdv: {exc}")

        if errors:
            raise RuntimeError(
                f"tracking-marts unit {unit.provider}:{unit.match_id}:{unit.period} failed: " + "; ".join(errors)
            )
        return total
