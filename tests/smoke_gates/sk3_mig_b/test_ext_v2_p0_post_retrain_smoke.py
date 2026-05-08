# ruff: noqa: S608 — SQL built from gold_schema fixture + module constants; no user input.
"""ExT v2 Phase 0 (Singh baseline) post-retrain smoke gate. Spec §3.

Refits the Singh producer end-to-end on the current `fct_action_values` and
asserts held-out NLL ≤ pre-registered baseline (3.7892, PR #206) + 1%
tolerance. Origin: `project_session58_phase_0_singh_baseline.md`.

Module-level import of `run_phase0_harness` is the API-drift sentinel — it
fails loudly at pytest collection if the harness symbol is renamed or moved.
The orchestrator's `_dispatch_trained_model` for ext_v2_p0 imports the same
symbols at runtime; this gate is the post-retrain validator.

Skips when DATABRICKS_HOST/TOKEN/warehouse env are absent (CI without
secrets); the orchestrator's `_run_smoke_gate` invocation always has them.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from analytics.ext_v2.harness import run_phase0_harness
from tests.smoke_gates.sk3_mig_b.conftest import chunked_sql_to_pandas

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_PHASE_0_BASELINE_NLL = 3.7892
_TOLERANCE_PCT = 0.01
_THRESHOLD = _PHASE_0_BASELINE_NLL * (1 + _TOLERANCE_PCT)


def test_phase_0_nll_within_threshold(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    """Refit Singh on current fct_action_values; assert NLL ≤ baseline + 1%."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]

    # fct_action_values stores SPADL action / result names directly in
    # `action_type` and `action_result` (verified to match silly-kicks'
    # actiontypes_df / results_df vocab on 2026-05-04). The harness expects
    # `type_name` + `result_name` + `action_type` — alias once, keep once.
    sql = f"""
    SELECT
      competition_id,
      match_key,
      action_type AS type_name,
      action_result AS result_name,
      action_type,
      start_x,
      start_y,
      end_x,
      end_y
    FROM {gold_schema}.fct_action_values
    WHERE start_x IS NOT NULL
      AND start_y IS NOT NULL
      AND end_x IS NOT NULL
      AND end_y IS NOT NULL
    """
    actions = chunked_sql_to_pandas(host, token, sql, warehouse_id)

    result = run_phase0_harness(actions)
    nll = result.best_nll

    assert nll <= _THRESHOLD, (
        f"Phase 0 NLL = {nll:.6f} > threshold {_THRESHOLD:.6f} "
        f"(baseline {_PHASE_0_BASELINE_NLL} + {_TOLERANCE_PCT * 100:.0f}%). "
        f"Halt + investigate before Phase 1 dispatch."
    )
