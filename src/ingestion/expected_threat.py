"""Expected Threat batch pipeline — computes xT grids from SPADL action data.

Reads SPADL actions from the gold mart (fct_action_values), computes per-competition
xT grids via Markov chain value iteration, and writes results to Delta.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from analytics.expected_threat import (
    ExpectedThreatParams,
    compute_expected_threat_grid,
    grid_to_dataframe,
    validate_xt_grid,
)
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "expected_threat_grids"
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


def _load_actions(spark: SparkSession, catalog: str) -> pd.DataFrame:
    """Load SPADL actions from gold mart, filtered to xT-relevant types."""
    types_sql = ", ".join(f"'{t}'" for t in _RELEVANT_TYPES)
    query = f"""
        SELECT
            competition_id,
            action_type AS type_name,
            action_result AS result_name,
            start_x,
            start_y,
            end_x,
            end_y
        FROM {catalog}.dev_gold.{_GOLD_TABLE}
        WHERE action_type IN ({types_sql})
    """  # noqa: S608
    return spark.sql(query).toPandas()  # type: ignore[union-attr]


@workflow("wf-xt-grids", phase="grid_computation")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx=None,
) -> None:
    """Compute per-competition and global xT grids, write to Delta."""
    params = ExpectedThreatParams()
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # ── Incremental skip guard on competition_id ──────────────────────
    existing: set[str] = set()
    try:
        existing = {
            str(row["competition_id"])
            for row in spark.table(results_table).select("competition_id").distinct().collect()
        }
    except Exception:
        logger.info("No existing %s table — will process all competitions", _TABLE_NAME)

    # Determine available competition IDs from the gold mart (cheap Spark query)
    types_sql = ", ".join(f"'{t}'" for t in _RELEVANT_TYPES)
    available_comps = {
        str(row["competition_id"])
        for row in spark.sql(
            f"SELECT DISTINCT competition_id FROM {catalog}.dev_gold.{_GOLD_TABLE}"  # noqa: S608
            f" WHERE action_type IN ({types_sql}) AND competition_id IS NOT NULL"
        ).collect()
    }

    # Compute what's missing: per-competition grids + global grid
    new_comps = sorted(available_comps - existing)
    need_global = "global" not in existing

    if not new_comps and not need_global:
        logger.info(
            "All %d xT grids already computed (including global) — skipping",
            len(existing),
        )
        return

    logger.info(
        "Need to compute %d new competition grids%s (existing: %d)",
        len(new_comps),
        " + global" if need_global else "",
        len(existing),
    )

    # ── Load actions (only for missing competitions + global) ─────────
    # Global grid needs all actions; per-comp grids only need their slice.
    # When global is needed, load everything; otherwise load only new comps.
    if need_global:
        logger.info("Loading all SPADL actions from %s.dev_gold.%s (global grid needed)", catalog, _GOLD_TABLE)
        actions_df = _load_actions(spark, catalog)
    else:
        comp_filter = ", ".join(f"'{c}'" for c in new_comps)
        query = f"""
            SELECT
                competition_id,
                action_type AS type_name,
                action_result AS result_name,
                start_x, start_y, end_x, end_y
            FROM {catalog}.dev_gold.{_GOLD_TABLE}
            WHERE action_type IN ({types_sql})
              AND CAST(competition_id AS STRING) IN ({comp_filter})
        """  # noqa: S608
        actions_df = spark.sql(query).toPandas()  # type: ignore[union-attr]
    logger.info("Loaded %d relevant actions", len(actions_df))

    if actions_df.empty:
        logger.warning("No actions found — skipping xT computation")
        return

    # ── Per-competition grids (only missing ones) ─────────────────────
    # Pre-build indexed lookup to avoid O(n*m) boolean mask (F-02 OPT-AUDIT-200)
    actions_by_comp = dict(iter(actions_df.groupby("competition_id")))
    competitions_written = 0
    for comp_id in new_comps:
        comp_actions = actions_by_comp.get(comp_id)
        if comp_actions is None:
            continue
        n_events = len(comp_actions)
        if n_events < 100:
            logger.warning("Competition %s has only %d events — skipping", comp_id, n_events)
            continue

        grid = compute_expected_threat_grid(comp_actions, params)
        grid_df = grid_to_dataframe(grid, competition_id=str(comp_id))
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
        logger.info("Competition %s: %d events, max xT=%.5f", comp_id, n_events, float(grid.max()))

    # ── Global grid (all competitions combined) ───────────────────────
    if need_global:
        global_grid = compute_expected_threat_grid(actions_df, params)
        validate_xt_grid(global_grid)
        global_df = grid_to_dataframe(global_grid, competition_id="global")
        spark_df = spark.createDataFrame(global_df)  # type: ignore[union-attr]
        write_delta_table(
            spark_df,
            catalog=catalog,
            schema=schema,
            table_name=_TABLE_NAME,
            replace_where="competition_id = 'global'",
            logger=logger,
        )
        logger.info("Global grid: %d events, max xT=%.5f", len(actions_df), float(global_grid.max()))

    logger.info(
        "Done — wrote %d competition grids%s",
        competitions_written,
        " + global" if need_global else "",
    )


def main() -> None:
    """CLI entry point."""
    args = parse_ingestion_args("Compute Expected Threat grids from SPADL actions")
    logger = configure_logging("expected_threat")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, logger)
