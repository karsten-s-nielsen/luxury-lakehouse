# ruff: noqa: S608 — SQL built from gold_schema fixture + module constants; no user input.
"""Post-retrain smoke gate for VAEP.

Spec §3 acceptance:
- per-action vaep_value distribution mean within +/-50% of Singh-2018 ballpark
- 0% NaN
- 100% rows within [-1, 1] bounds
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.tests.sk3_mig_b.conftest import execute_sql

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


# Singh 2018 + Decroos 2019 published per-action mean ballpark.
_VAEP_PER_ACTION_MEAN_BALLPARK = 0.0035
_TOLERANCE = 0.5  # +/-50% per spec §3
_LOWER = _VAEP_PER_ACTION_MEAN_BALLPARK * (1 - _TOLERANCE)
_UPPER = _VAEP_PER_ACTION_MEAN_BALLPARK * (1 + _TOLERANCE)

# Eval fold: 1k actions from a deterministic StatsBomb slice.
_EVAL_FOLD_SIZE = 1000


def test_vaep_value_within_bounds(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN vaep_value < -1 OR vaep_value > 1 THEN 1 ELSE 0 END) AS n_out,
      SUM(CASE WHEN vaep_value IS NULL THEN 1 ELSE 0 END) AS n_null,
      AVG(vaep_value) AS mean_value
    FROM (
      SELECT vaep_value
      FROM {gold_schema}.fct_action_values
      WHERE data_source = 'statsbomb'
      ORDER BY action_value_id
      LIMIT {_EVAL_FOLD_SIZE}
    )
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows
    n_total = int(rows[0][0])
    n_out = int(rows[0][1])
    n_null = int(rows[0][2])
    mean_v = float(rows[0][3])

    assert n_total > 0
    assert n_out == 0, f"{n_out}/{n_total} vaep_value outside [-1, 1]"
    assert n_null == 0, f"{n_null}/{n_total} vaep_value NULL"
    assert _LOWER <= mean_v <= _UPPER, (
        f"vaep_value mean = {mean_v:.6f}, expected within "
        f"[{_LOWER:.6f}, {_UPPER:.6f}] "
        f"({_TOLERANCE * 100:.0f}% of Singh-2018 ballpark "
        f"{_VAEP_PER_ACTION_MEAN_BALLPARK})"
    )
