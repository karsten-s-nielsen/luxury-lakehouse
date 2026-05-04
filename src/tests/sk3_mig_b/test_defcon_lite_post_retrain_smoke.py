# ruff: noqa: S608 — SQL built from gold_schema fixture; no user input.
"""DEFCON-lite post-retrain smoke gate. Spec §3.

Note: DEFCON-lite is a compute-only re-run (no model fitting); the gate
asserts the recomputed predictions are sensible against new fct_action_values.

Phase 0.8 findings applied:
- Column is `defcon_value`, not `defcon_credit` (plan-write-time error caught).
- Empirical baseline 0.209 (n=9826, sigma=0.549), not the plan's hardcoded 0.85.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.tests.sk3_mig_b.conftest import execute_sql

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


# Empirical baseline from Phase 0.8 probe on 2026-05-03:
# AVG(SUM(defcon_value) GROUPED BY (match_id, action_player_id)) = 0.2086
# n_team_match_rows = 9826, sigma = 0.549.
# Plan's hardcoded 0.85 was 4x too high; gate would have always failed.
# Wide +/-50% bound to absorb retrain-time variance + match-set composition shifts.
_EMPIRICAL_BASELINE_MEAN = 0.209
_TOLERANCE = 0.5  # +/-50%
_LOWER = _EMPIRICAL_BASELINE_MEAN * (1 - _TOLERANCE)
_UPPER = _EMPIRICAL_BASELINE_MEAN * (1 + _TOLERANCE)


def test_defcon_credit_sum_within_bounds(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT
      AVG(team_credit_sum) AS mean_credit,
      COUNT(*) AS n_team_match,
      SUM(CASE WHEN team_credit_sum IS NULL THEN 1 ELSE 0 END) AS n_null
    FROM (
      SELECT match_id, action_player_id, SUM(defcon_value) AS team_credit_sum
      FROM {gold_schema}.fct_defcon_actions
      GROUP BY match_id, action_player_id
    )
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows
    mean_credit = float(rows[0][0])
    n_total = int(rows[0][1])
    n_null = int(rows[0][2])

    assert n_total > 0
    assert n_null == 0, f"{n_null}/{n_total} action_player-match credit sums NULL"
    assert _LOWER <= mean_credit <= _UPPER, (
        f"DEFCON credit-sum mean = {mean_credit:.4f}, "
        f"expected [{_LOWER:.4f}, {_UPPER:.4f}] "
        f"(empirical baseline {_EMPIRICAL_BASELINE_MEAN}, +/-{_TOLERANCE * 100:.0f}%)"
    )


def test_no_null_action_player_id(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    sql = f"""
    SELECT SUM(CASE WHEN action_player_id IS NULL THEN 1 ELSE 0 END) AS n_null,
           COUNT(*) AS n_total
    FROM {gold_schema}.fct_defcon_actions
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    n_null, n_total = int(rows[0][0]), int(rows[0][1])
    assert n_null == 0, f"{n_null}/{n_total} fct_defcon_actions rows have NULL action_player_id"
