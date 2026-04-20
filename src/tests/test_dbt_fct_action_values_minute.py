"""Integration test: fct_action_values.minute is match-absolute.

Regression sentinel for the 2026-04-19 stg_spadl__action_values migration that
changed SPADL period-local minutes to match-absolute minutes.

Anchor event: Anthony Martial's only goal in 2016-04-03 Manchester United vs
Everton (match_id=3754299). Bronze `statsbomb_events` records the shot at
period=2, minute=53, second=2. A correctly-converted fct_action_values row
MUST show minute=53 (match-absolute), NOT minute=8 (period-local, 53-45=8).

Skipped when Databricks env not configured — this is an integration test, not
a unit test.
"""

from __future__ import annotations

import os
import time

import pytest


@pytest.mark.skipif(
    not os.environ.get("DATABRICKS_HOST") or not os.environ.get("DATABRICKS_HTTP_PATH"),
    reason="Integration test requires DATABRICKS_HOST + DATABRICKS_HTTP_PATH",
)
def test_fct_action_values_minute_is_match_absolute() -> None:
    """Martial's 2016-04-03 goal must read as minute=53 in fct_action_values.

    Bronze anchor: period=2, minute=53, second=2, player_id=20039, team_id=39
    (Manchester United), match_id=3754299, shot_outcome='Goal'.
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    warehouse_id = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").split("/")[-1]

    query = """
SELECT minute, second, period, vaep_value
FROM soccer_analytics.dev_gold.fct_action_values
WHERE match_id = 3754299
  AND player_id = 20039
  AND period = 2
  AND action_type = 'shot'
  AND action_result = 'success'
  AND vaep_value > 0.9
"""
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="30s",
    )
    for _ in range(15):
        status = response.status
        assert status is not None and status.state is not None
        if status.state.value not in ("PENDING", "RUNNING"):
            break
        time.sleep(2)
        assert response.statement_id is not None
        response = client.statement_execution.get_statement(response.statement_id)
    status = response.status
    assert status is not None and status.state is not None
    assert status.state.value == "SUCCEEDED", f"query failed: {status}"

    result = response.result
    assert result is not None
    rows = result.data_array or []
    assert rows, "No Martial goal row in fct_action_values for match 3754299 — data missing"
    minute = int(rows[0][0])
    assert minute == 53, (
        f"Martial's 2016-04-03 goal should be match-absolute minute 53 "
        f"(period 2, time-of-day 53rd minute). Got minute={minute}. "
        "If this is 8, stg_spadl__action_values has regressed to period-local."
    )
