"""Workflow-related queries — extracted from state/workflows.py.

Only the 2 SQL queries (cold costs + warm costs). Non-SQL data fetching
(Jobs API, HF Hub) remains in state/workflows.py as those are API calls,
not database queries.
"""

from __future__ import annotations

import logging

import pandas as pd
from config import get_settings

from queries.common import execute_query, t, ttl_cache

logger = logging.getLogger(__name__)


@ttl_cache(ttl=3600)
def fetch_cold_costs() -> pd.DataFrame:
    """30-day aggregated costs from fct_workflow_costs_synced (cold tier).

    Expected columns: workflow_id, task_key, total_cost_usd, total_dbu, run_count.
    Grouped by workflow_id (falls back to task_key when workflow_id is NULL).
    """
    _empty = pd.DataFrame(columns=pd.Index(["workflow_id", "task_key", "total_cost_usd", "total_dbu", "run_count"]))
    try:
        tbl = t("fct_workflow_costs_synced")
        return execute_query(
            f"SELECT COALESCE(workflow_id, task_key) AS workflow_id, "  # noqa: S608
            f"  task_key, "
            f"  SUM(attributed_cost_usd) AS total_cost_usd, "
            f"  SUM(attributed_dbu) AS total_dbu, "
            f"  COUNT(DISTINCT job_run_id) AS run_count "
            f"FROM {tbl} "
            f"WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days' "
            f"GROUP BY COALESCE(workflow_id, task_key), task_key "
            f"ORDER BY total_cost_usd DESC "
            f"LIMIT 100",
        )
    except Exception:
        logger.warning("Cold cost query failed — costs unavailable", exc_info=True)
        return _empty


@ttl_cache(ttl=120)
def fetch_warm_costs() -> pd.DataFrame:
    """Recent cost estimates from workflow_cost_live_synced (warm tier).

    Expected columns: workflow_id, phase, state, task_key,
    duration_seconds, estimated_cost_usd, started_at, ended_at,
    rate_usd_per_hour.
    """
    try:
        settings = get_settings()
        tbl = t("workflow_cost_live_synced", schema=settings.observability_schema)
        return execute_query(
            f"SELECT workflow_id, phase, state, task_key, "  # noqa: S608
            f"  duration_seconds, estimated_cost_usd, "
            f"  started_at, ended_at, rate_usd_per_hour "
            f"FROM {tbl} "
            f"WHERE started_at >= NOW() - INTERVAL '30 days' "
            f"ORDER BY started_at DESC "
            f"LIMIT 500",
        )
    except Exception:
        logger.warning("Warm cost query failed — costs unavailable", exc_info=True)
        return pd.DataFrame()
