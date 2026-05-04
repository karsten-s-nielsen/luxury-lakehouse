# ruff: noqa: S608 — SQL built from gold_schema fixture + module constants; no user input.
"""ExT v2 Phase 1 (KDE-smoothed Singh) post-retrain smoke gate. Spec §3.

Bypasses `run_phase1_harness`'s 500-trial Optuna search (too slow for a smoke
gate) — directly fits `KDESmoothedProducer` with the production champion
params per `project_session60_phase_1_kde_smoothed.md`:
kernel=gaussian, bandwidth=1.99998 (saturated upper edge of [0.01, 2.0]),
adaptive=True. Asserts held-out NLL ≤ baseline (3.7482, PR #213) + 1%.

Module-level import of `run_phase1_harness` catches API drift; the actual
gate uses harness primitives (`KDESmoothedProducer`, `holdout_split`,
`compute_holdout_nll`) — running optuna in a smoke gate is ~135 min on full
data, vs ~2 min for direct champion-params evaluation.

Skips when DATABRICKS_HOST/TOKEN/warehouse env are absent.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from analytics.ext_v2.fitness import compute_holdout_nll
from analytics.ext_v2.harness import run_phase1_harness  # noqa: F401 — API-drift sentinel
from analytics.ext_v2.holdout import holdout_split
from analytics.ext_v2.producer import KDESmoothedProducer
from analytics.ext_v2.transition import GridSpec
from src.tests.sk3_mig_b.conftest import chunked_sql_to_pandas

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_PHASE_1_BASELINE_NLL = 3.7482
_TOLERANCE_PCT = 0.01
_THRESHOLD = _PHASE_1_BASELINE_NLL * (1 + _TOLERANCE_PCT)

# Production champion params per project_session60_phase_1_kde_smoothed.md.
_CHAMPION_KERNEL = "gaussian"
_CHAMPION_BANDWIDTH = 1.99998
_CHAMPION_ADAPTIVE = True
_HOLDOUT_FRACTION = 0.15
_NLL_FLOOR_EPS = 1e-10


def test_phase_1_nll_within_threshold(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    gold_schema: str,
) -> None:
    """Refit KDE-smoothed Singh with champion params; assert NLL ≤ baseline + 1%."""
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

    grid = GridSpec()
    train_actions, holdout_actions = holdout_split(actions, holdout_fraction=_HOLDOUT_FRACTION)
    holdout_passes = holdout_actions[holdout_actions["action_type"] == "pass"].copy()

    producer = KDESmoothedProducer(
        grid=grid,
        kernel=_CHAMPION_KERNEL,
        bandwidth=_CHAMPION_BANDWIDTH,
        adaptive=_CHAMPION_ADAPTIVE,
    ).fit(train_actions)

    nll = compute_holdout_nll(producer, holdout_passes, grid=grid, eps=_NLL_FLOOR_EPS)

    assert nll <= _THRESHOLD, (
        f"Phase 1 NLL = {nll:.6f} > threshold {_THRESHOLD:.6f} "
        f"(baseline {_PHASE_1_BASELINE_NLL} + {_TOLERANCE_PCT * 100:.0f}%). Halt."
    )
