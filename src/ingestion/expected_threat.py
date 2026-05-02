"""Expected Threat batch pipeline — computes xT grids from SPADL action data.

Reads SPADL actions from the gold mart (fct_action_values), computes per-competition
xT grids via Markov chain value iteration, and writes results to Delta.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

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


class _ExpectedThreatGuard:
    """SkipGuard adapter for expected threat grid computation."""

    workflow_id = "wf-xt-grids"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which competitions need xT grid computation."""
        from ingestion.guards import ensure_table, find_new_ids

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

        # Check global sentinel separately — find_new_ids only handles
        # real competition IDs from the source table.
        need_global = False
        try:
            existing = {
                str(row["competition_id"])
                for row in spark.table(f"{catalog}.{schema}.{_TABLE_NAME}")
                .select("competition_id")
                .distinct()
                .collect()
            }
            need_global = "global" not in existing
        except Exception:  # noqa: BLE001 — first-run fallback: any table-read failure means rebuild global grid
            need_global = True

        total = len(new_comps) + (1 if need_global else 0)
        if total == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id,
            count=total,
            metadata={"new_competition_ids": sorted(new_comps), "need_global": need_global},
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

            # Differential validation against the previous run's grid for this competition.
            previous = _load_previous_grid(spark, catalog, schema, comp_id, logger)
            xt_grid.validate_differential(previous)

            grid_df = xt_grid.to_dataframe()
            spark_df = spark.createDataFrame(grid_df)  # type: ignore[union-attr]
            write_delta_table(
                spark_df,
                catalog=catalog,
                schema=schema,
                table_name=_TABLE_NAME,
                replace_where=f"competition_id = '{comp_id}'",
                logger=logger,
            )
            competitions_written += 1
            logger.info(
                "Competition %s: %d events, max xT=%.5f",
                comp_id,
                n_events,
                float(xt_grid.values.max()),
            )

    # ── Global grid (built from accumulated per-comp counters) ────────
    if need_global:
        if global_counters.total_actions == 0:
            logger.warning("No relevant actions found across any competition — skipping global xT grid")
        else:
            global_xt_grid = xt_grid_from_counters(global_counters, params, competition_id="global")

            # Structural validation (legacy v1 max_value=0.50 preserved here;
            # ExT v2 conditional grids will pass max_value=None or a higher ceiling).
            global_xt_grid.validate_structural(max_value=0.50)

            # Differential validation against the previous global grid.
            previous_global = _load_previous_grid(spark, catalog, schema, "global", logger)
            global_xt_grid.validate_differential(previous_global)

            global_df = global_xt_grid.to_dataframe()
            spark_df = spark.createDataFrame(global_df)  # type: ignore[union-attr]
            write_delta_table(
                spark_df,
                catalog=catalog,
                schema=schema,
                table_name=_TABLE_NAME,
                replace_where="competition_id = 'global'",
                logger=logger,
            )
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
