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


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    log: logging.Logger,
) -> None:
    """Compute per-competition and global xT grids, write to Delta."""
    params = ExpectedThreatParams()

    log.info("Loading SPADL actions from %s.dev_gold.%s", catalog, _GOLD_TABLE)
    actions_df = _load_actions(spark, catalog)
    log.info("Loaded %d relevant actions", len(actions_df))

    if actions_df.empty:
        log.warning("No actions found — skipping xT computation")
        return

    # Per-competition grids
    competitions = sorted(actions_df["competition_id"].dropna().unique())
    log.info("Computing xT grids for %d competitions", len(competitions))

    for comp_id in competitions:
        comp_actions = pd.DataFrame(actions_df[actions_df["competition_id"] == comp_id])
        n_events = len(comp_actions)
        if n_events < 100:
            log.warning("Competition %s has only %d events — skipping", comp_id, n_events)
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
            logger=log,
        )
        log.info("Competition %s: %d events, max xT=%.5f", comp_id, n_events, float(grid.max()))

    # Global grid (all competitions combined)
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
        logger=log,
    )
    log.info("Global grid: %d events, max xT=%.5f", len(actions_df), float(global_grid.max()))
    log.info("Done — wrote grids for %d competitions + global", len(competitions))


def main() -> None:
    """CLI entry point."""
    args = parse_ingestion_args("Compute Expected Threat grids from SPADL actions")
    log = configure_logging("expected_threat")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, log)
