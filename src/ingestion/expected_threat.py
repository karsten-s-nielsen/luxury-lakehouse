"""Expected Threat batch pipeline — computes xT grids from SPADL action data.

Reads SPADL actions from the gold mart (fct_action_values), computes per-competition
xT grids via Markov chain value iteration, and writes results to Delta.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "expected_threat_grids"
_RESULTS_SCHEMA = "zone_x BIGINT, zone_y BIGINT, xt_value DOUBLE, competition_id STRING, _ingested_at TIMESTAMP"
_GOLD_TABLE = "fct_action_values"

# SPADL action types relevant to xT
_RELEVANT_TYPES = (
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "dribble",
    "goalkick",
    "clearance",
    "shot",
    "shot_penalty",
    "shot_freekick",
)

logger = logging.getLogger(__name__)
_guard_logger = logging.getLogger(f"{__name__}.guard")

_WORKFLOW_ID = "wf-xt-grids"


def _decide_rebuild(
    find_new: list[str],
    all_comps: list[str],
    *,
    upstream_changed: bool,
    global_exists: bool,
) -> tuple[list[str], bool]:
    """Pure guard decision (ADR-063, review L10 — testable without Spark).

    Returns ``(competitions_to_build, need_global)``.

    - When the upstream mart (``fct_action_values``) was re-derived (watermark changed), rebuild **all**
      competition grids + global — ``find_new_ids`` only catches genuinely-new competitions and would otherwise
      leave every existing per-comp grid stale (the build-if-absent bug this ADR fixes).
    - Otherwise build only genuinely-new competitions; rebuild global only if it is absent.
    """
    need_global = upstream_changed or not global_exists
    comps = sorted(set(find_new) | set(all_comps)) if upstream_changed else sorted(set(find_new))
    return comps, need_global


# Per-comp grids at/above this action count get the directionality assert; smaller ones are exempt
# (ADR-063 M5/M6 — small/noisy competitions must not false-fail).
_MIN_ACTIONS_DIRECTIONAL = 5000
# Write-if-changed materiality (ADR-063 R4). Cells below the floor are noise; a grid is written +
# propagated only if the max relative change among above-floor cells (vs the last-PROPAGATED grid =
# the current table contents) reaches the threshold. PROVISIONAL — tune after observing the drift
# logged each run; gating vs the current table (only ever holds propagated grids) bounds cumulative drift.
_MATERIALITY_VALUE_FLOOR = 0.005
_MATERIALITY_REL_THRESHOLD = 0.10


def _grid_drift(new_values: np.ndarray, previous_values: np.ndarray | None) -> float | None:
    """Max relative per-cell change vs the last-propagated grid, among cells above the value floor.

    Returns ``None`` when there is no comparable baseline (treat as material → write). ADR-063 R4(iv):
    the baseline is the CURRENT table grid which — because we only write on material change — IS the
    last-propagated grid, so slow sub-threshold drift cannot accumulate unbounded.
    """
    if previous_values is None or previous_values.shape != new_values.shape:
        return None
    mask = previous_values >= _MATERIALITY_VALUE_FLOOR
    if not bool(mask.any()):
        return None
    rel = np.abs(new_values[mask] - previous_values[mask]) / previous_values[mask]
    return float(rel.max())


def _write_grid_if_material(
    spark: SparkSession,
    grid: Any,
    *,
    catalog: str,
    schema: str,
    comp_id: str,
    logger: logging.Logger,
) -> bool:
    """WARN-only differential + write-only-on-material-change (ADR-063 R4/H3). Returns True if written.

    ``grid`` is an ``analytics.expected_threat.XTGrid`` (duck-typed here to keep this guard-adjacent
    module free of module-level analytics imports).
    """
    previous = _load_previous_grid(spark, catalog, schema, comp_id, logger)
    # Differential is advisory only now (ADR-063 H3): never raise — the directionality assert is the
    # hard gate, and a hard differential would deadlock the auto-rebuild on a legitimate large shift.
    try:
        grid.validate_differential(previous)
    except ValueError as exc:
        logger.warning("xT grid '%s' differential WARN (not blocking, ADR-063 H3): %s", comp_id, exc)
    prev_values = previous.values if previous is not None else None
    drift = _grid_drift(grid.values, prev_values)
    logger.info(
        "xT grid '%s' drift vs last-propagated: %s",
        comp_id,
        "n/a (no baseline)" if drift is None else f"{drift:.4f}",
    )
    if drift is not None and drift < _MATERIALITY_REL_THRESHOLD:
        logger.info(
            "xT grid '%s' immaterial (drift %.4f < %.2f) — skip write, no version bump (ADR-063 R4)",
            comp_id,
            drift,
            _MATERIALITY_REL_THRESHOLD,
        )
        return False
    spark_df = spark.createDataFrame(grid.to_dataframe())  # type: ignore[union-attr]
    write_delta_table(
        spark_df,
        catalog=catalog,
        schema=schema,
        table_name=_TABLE_NAME,
        replace_where=f"competition_id = '{comp_id}'",
        logger=logger,
    )
    return True


class _ExpectedThreatGuard:
    """SkipGuard adapter for expected threat grid computation."""

    workflow_id = _WORKFLOW_ID

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which xT grids need (re)computation.

        Watermark-aware (ADR-063): if the upstream gold mart ``fct_action_values`` was re-derived
        (e.g. the SPADL->LTR migration), rebuild ALL grids — not just absent ones — because the
        build-if-absent pattern silently froze the grid for ~2 months. ``find_new_ids`` still catches
        genuinely-new competitions; ``check_upstream_freshness`` catches in-place re-derivation.
        """
        from ingestion.guards import (
            check_upstream_freshness,
            ensure_table,
            find_new_ids,
            resolve_upstream_tables_from_card,
        )

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        types_sql = ", ".join(f"'{t}'" for t in _RELEVANT_TYPES)
        new_comps = find_new_ids(
            spark,
            source_table=f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}",
            results_table=results_table,
            id_column="competition_id",
            source_filter=f"action_type IN ({types_sql}) AND competition_id IS NOT NULL",
        )

        try:
            existing = {
                str(row["competition_id"])
                for row in spark.table(results_table).select("competition_id").distinct().collect()
            }
            global_exists = "global" in existing
        except Exception:  # noqa: BLE001 — first-run fallback: any table-read failure means rebuild global grid
            global_exists = False

        # Watermark on the upstream mart (the card pins `{catalog}.dev_gold.fct_action_values`).
        upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        upstream_changed = check_upstream_freshness(spark, catalog, self.workflow_id, upstream).count > 0

        all_comps = _list_relevant_competition_ids(spark, catalog) if upstream_changed else []
        comps, need_global = _decide_rebuild(
            new_comps, all_comps, upstream_changed=upstream_changed, global_exists=global_exists
        )

        total = len(comps) + (1 if need_global else 0)
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            metadata={"new_competition_ids": comps, "need_global": need_global},
        )


skip_guard = _ExpectedThreatGuard()


def _load_previous_grid(
    spark: SparkSession,
    catalog: str,
    schema: str,
    competition_id: str,
    logger: logging.Logger,
):
    """Load the previous run's xT grid for the given competition_id.

    Returns ``None`` if no prior grid exists (first run for this
    competition_id, or the bronze table is empty / missing). All grids
    written by this pipeline are SPADL 105x68 — ``coord_system`` is
    hardcoded as that's the established convention for this bronze table.
    """
    import numpy as np

    from analytics.expected_threat import XTGrid
    from ingestion.utils import tolerate_missing_table

    table = f"{catalog}.{schema}.{_TABLE_NAME}"
    rows: list = []
    with tolerate_missing_table(
        logger,
        f"first run on {table} — no previous grid for differential check",
    ):
        rows = list(
            spark.sql(
                f"SELECT zone_x, zone_y, xt_value FROM {table} "  # noqa: S608
                f"WHERE competition_id = '{competition_id}'"
            ).collect()
        )

    if not rows:
        return None

    n_x = max(int(r.zone_x) for r in rows) + 1
    n_y = max(int(r.zone_y) for r in rows) + 1
    values = np.zeros((n_x, n_y))
    for row in rows:
        values[int(row.zone_x), int(row.zone_y)] = float(row.xt_value)

    return XTGrid(
        values=values,
        pitch_length=105.0,
        pitch_width=68.0,
        coord_system="spadl",
        competition_id=competition_id,
    )


def _list_relevant_competition_ids(spark: SparkSession, catalog: str) -> list[str]:
    """Return distinct competition_ids in fct_action_values restricted to
    xT-relevant action types.

    Bounded — the column has ~22 distinct values across all current
    sources. Returns a list of strings (we never join numerically here).
    Driver memory: O(n_competitions x ~30 bytes) = trivial.
    """
    from pyspark.sql.functions import col

    rows = (
        spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}")
        .filter(col("action_type").isin(list(_RELEVANT_TYPES)))
        .filter(col("competition_id").isNotNull())
        .select("competition_id")
        .distinct()
        .collect()
    )
    return [str(row["competition_id"]) for row in rows]


def _load_actions_for_competition(
    spark: SparkSession,
    catalog: str,
    competition_id: str,
) -> pd.DataFrame:
    """Pull a single competition's xT-relevant actions to driver memory.

    Bounded by per-competition row count — largest competition (a full
    league season's SPADL events) is ~500K rows x 6 cols ≈ 24 MB,
    well below the 16 GB driver budget. Replaces the pre-OPT-1
    full-fact-table pull (~9.5M x 6 cols ≈ 456 MB) that used to run
    when the global grid needed rebuilding.

    Returns the same column shape as the legacy ``_load_actions``:
    ``competition_id, type_name, result_name, start_x/y, end_x/y``.
    """
    from pyspark.sql.functions import col

    return (
        spark.table(f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_GOLD_TABLE}")
        .filter(col("action_type").isin(list(_RELEVANT_TYPES)))
        .filter(col("competition_id") == competition_id)
        .selectExpr(
            "CAST(competition_id AS STRING) AS competition_id",
            "action_type AS type_name",
            "action_result AS result_name",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
        )
        .toPandas()  # type: ignore[union-attr]
    )


@workflow("wf-xt-grids", phase="grid_computation")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Compute per-competition and global xT grids, write to Delta.

    Streams per-competition action slices (~24 MB each) instead of
    pulling the full ``fct_action_values`` table to driver memory at
    once (~456 MB peak under the legacy implementation). Exploits the
    additivity of ``ZoneCounters`` to build the global grid by
    accumulating per-comp counters across iterations — see
    ``analytics.expected_threat.ZoneCounters`` docstring for the
    primitive's contract. Refactored OPT-1 (2026-05-03).
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from analytics.expected_threat import (
        ExpectedThreatParams,
        ZoneCounters,
        bucket_actions_into_counters,
        xt_grid_from_counters,
    )

    params = ExpectedThreatParams()

    # Use guard-provided metadata instead of inline re-computation
    new_comps = filter_result.metadata["new_competition_ids"]
    need_global = filter_result.metadata.get("need_global", False)

    if not new_comps and not need_global:
        logger.info("All xT grids already computed (including global) — skipping")
        return 0

    logger.info(
        "Need to compute %d new competition grids%s",
        len(new_comps),
        " + global" if need_global else "",
    )

    # ── Determine which competitions need to be visited this run ─────
    # Per-comp grids: just `new_comps`. Global grid: every competition
    # with relevant actions (so its counters can be folded into the
    # global accumulator). Visiting each comp once and reusing its
    # counters for both purposes is the streaming optimisation.
    new_comp_set = {str(c) for c in new_comps}
    if need_global:
        all_comp_ids = _list_relevant_competition_ids(spark, catalog)
        comps_to_visit = sorted(set(all_comp_ids) | new_comp_set)
    else:
        comps_to_visit = sorted(new_comp_set)

    global_counters = ZoneCounters.zero(params)
    competitions_written = 0
    total_actions_accumulated = 0

    for comp_id in comps_to_visit:
        comp_actions = _load_actions_for_competition(spark, catalog, comp_id)
        if comp_actions.empty:
            continue
        n_events = len(comp_actions)
        total_actions_accumulated += n_events

        comp_counters = bucket_actions_into_counters(comp_actions, params)
        if need_global:
            global_counters = global_counters + comp_counters

        # Per-comp grid only for competitions the guard flagged as new.
        if comp_id in new_comp_set:
            if n_events < 100:
                logger.warning(
                    "Competition %s has only %d events — skipping per-comp grid",
                    comp_id,
                    n_events,
                )
                continue

            xt_grid = xt_grid_from_counters(comp_counters, params, competition_id=comp_id)

            # Directionality gate for substantial competitions only (ADR-063 M5/M6): small/noisy
            # per-comp grids are exempt to avoid false-fails; large ones must not be silently inverted.
            if n_events >= _MIN_ACTIONS_DIRECTIONAL:
                xt_grid.assert_directional()

            # WARN-only differential + write-only-on-material-change (ADR-063 R4/H3).
            if _write_grid_if_material(spark, xt_grid, catalog=catalog, schema=schema, comp_id=comp_id, logger=logger):
                competitions_written += 1
                logger.info("Competition %s: %d events, max xT=%.5f", comp_id, n_events, float(xt_grid.values.max()))

    # ── Global grid (built from accumulated per-comp counters) ────────
    if need_global:
        if global_counters.total_actions == 0:
            logger.warning("No relevant actions found across any competition — skipping global xT grid")
        else:
            global_xt_grid = xt_grid_from_counters(global_counters, params, competition_id="global")

            # HARD gate (ADR-063 R1): a non-directional global grid is a build FAILURE — raises before
            # any watermark is recorded, so a stale/broken grid forces a re-run rather than silently
            # propagating (the negative-DZV root cause). max_value=0.50 is the legacy v1 ceiling.
            global_xt_grid.validate_structural(max_value=0.50, require_directional=True)

            # WARN-only differential + write-only-on-material-change (ADR-063 R4/H3).
            if _write_grid_if_material(
                spark, global_xt_grid, catalog=catalog, schema=schema, comp_id="global", logger=logger
            ):
                logger.info(
                    "Global grid: %d events accumulated across %d competitions, max xT=%.5f",
                    global_counters.total_actions,
                    len(comps_to_visit),
                    float(global_xt_grid.values.max()),
                )

    logger.info(
        "Done — wrote %d competition grids%s (streamed %d total actions across %d competitions)",
        competitions_written,
        " + global" if need_global else "",
        total_actions_accumulated,
        len(comps_to_visit),
    )

    # Record the upstream watermark ONLY after a validated, successful run (ADR-063 H3). If the
    # directionality assert raised above, we never reach here → the guard re-fires next run rather
    # than recording "fresh" on an un-rebuilt grid (the silent-staleness failure this ADR targets).
    from ingestion.guards import record_watermarks, resolve_upstream_tables_from_card

    upstream = resolve_upstream_tables_from_card(_WORKFLOW_ID, catalog, schema)
    record_watermarks(spark, catalog, _WORKFLOW_ID, upstream)
    return 0


def main() -> None:
    """CLI entry point."""
    args = parse_ingestion_args("Compute Expected Threat grids from SPADL actions")
    logger = configure_logging("expected_threat")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
