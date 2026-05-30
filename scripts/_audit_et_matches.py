"""One-shot ET-match audit + fixture-candidate identification (throwaway).

Counts matches with period_id in {3,4} per per-period-absolute provider
(idsse, metrica, gradientsports) across bronze tracking + spadl_actions. Lists
candidate matches for silly-kicks PR-S70 Task 8 fixture extraction +
luxury-lakehouse §8 historical mis-orientation audit.

Read-only via Databricks SDK Statement Execution. No writes anywhere.
"""

from __future__ import annotations

import logging
import os
import sys
import time

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG = "soccer_analytics"
BRONZE = "bronze"

_PROVIDERS = ("idsse", "metrica", "gradientsports")
_FRAME_COL = {"idsse": "frame", "metrica": "frame", "gradientsports": "frame_num"}


def _exec(sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute a Databricks SQL statement and return result as a pandas DataFrame."""
    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    statement_id = resp.statement_id
    state = resp.status.state if resp.status else None
    while state and state.value in ("PENDING", "RUNNING"):
        time.sleep(2)
        resp = w.statement_execution.get_statement(statement_id)
        state = resp.status.state if resp.status else None
    if not resp.status or resp.status.state.value != "SUCCEEDED":
        err_msg = resp.status.error.message if resp.status and resp.status.error else "?"
        raise RuntimeError(f"SQL {state}: {err_msg}")
    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return pd.DataFrame()
    cols = [c.name for c in resp.manifest.schema.columns]
    if not resp.result or not resp.result.data_array:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(resp.result.data_array, columns=cols)


def audit_provider(provider: str, warehouse_id: str) -> None:
    """Count ET matches per provider via spadl_actions + tracking tables."""
    logger.info("=== %s ===", provider)

    # Actions side
    sql = f"""
        SELECT match_id_native, COUNT(*) AS et_actions, COUNT(DISTINCT period_id) AS et_periods,
               MIN(period_id) AS min_period, MAX(period_id) AS max_period
        FROM {CATALOG}.{BRONZE}.spadl_actions
        WHERE data_source = '{provider}' AND period_id IN (3, 4)
        GROUP BY match_id_native
        ORDER BY et_actions DESC
        LIMIT 20
    """  # noqa: S608
    actions_df = _exec(sql, warehouse_id)
    logger.info("  ET matches (spadl_actions): %d", len(actions_df))
    if not actions_df.empty:
        print(f"\n  Top {provider} ET matches (by ET-action count):")
        print(actions_df.to_string(index=False))

    # Tracking side
    table = f"{CATALOG}.{BRONZE}.{provider}_tracking"
    fcol = _FRAME_COL[provider]
    try:
        sql = f"""
            SELECT match_id, COUNT(*) AS et_frame_rows, COUNT(DISTINCT {fcol}) AS et_frames,
                   MIN({fcol}) AS min_frame, MAX({fcol}) AS max_frame,
                   MIN(period) AS min_period, MAX(period) AS max_period
            FROM {table}
            WHERE period IN (3, 4)
            GROUP BY match_id
            ORDER BY et_frame_rows DESC
            LIMIT 20
        """  # noqa: S608
        track_df = _exec(sql, warehouse_id)
        logger.info("  ET matches (%s_tracking): %d", provider, len(track_df))
        if not track_df.empty:
            print(f"\n  Top {provider} ET tracking (by ET-frame-row count):")
            print(track_df.to_string(index=False))
    except RuntimeError as exc:
        logger.warning("  tracking audit skipped: %s", exc)

    print()


def main() -> int:
    warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
    for provider in _PROVIDERS:
        audit_provider(provider, warehouse_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
