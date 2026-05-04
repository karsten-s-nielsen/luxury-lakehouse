# ruff: noqa: S608 — SQL built from gold_schema fixture; no user input.
"""PAUSA post-retrain smoke gate. Spec §3 — Lee et al. 2026 PAUSA definition.

Phase 0.8 findings applied:
- Column is `pausa_score`, not `pausa_value` (plan-write-time error caught).
- Empirical baseline 0.558 (n=1627, sigma=0.382), not the plan's hardcoded 0.45.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.tests.sk3_mig_b.conftest import execute_sql

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


# Empirical baseline from Phase 0.8 probe on 2026-05-03:
# AVG(pausa_score) = 0.558, n = 1627, sigma = 0.382
_EMPIRICAL_BASELINE_MEAN = 0.558
# Wide bounds to absorb retrain variance; tighten post-runtime calibration.
_LOWER_BOUND = 0.40
_UPPER_BOUND = 0.70


def test_pausa_value_within_bounds(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN pausa_score < 0 OR pausa_score > 1 THEN 1 ELSE 0 END) AS n_out,
      SUM(CASE WHEN pausa_score IS NULL THEN 1 ELSE 0 END) AS n_null,
      AVG(pausa_score) AS mean_value
    FROM {gold_schema}.fct_pausa_values
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows
    n_total = int(rows[0][0])
    n_out = int(rows[0][1])
    n_null = int(rows[0][2])
    mean_v = float(rows[0][3])

    assert n_total > 0
    assert n_out == 0, f"{n_out}/{n_total} pausa_score outside [0, 1]"
    assert n_null == 0, f"{n_null}/{n_total} pausa_score NULL"
    assert _LOWER_BOUND <= mean_v <= _UPPER_BOUND, (
        f"PAUSA mean = {mean_v:.4f}, expected [{_LOWER_BOUND}, {_UPPER_BOUND}] "
        f"(empirical baseline {_EMPIRICAL_BASELINE_MEAN})"
    )
