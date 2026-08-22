"""Read-side contract reconciliation (A3/A4) — operator/live-run gate, NOT a CI gate.

conftest.py autouse-sets LAKEBASE_HOST="test-host", so the `if not LAKEBASE_HOST` guard is
defeated in CI; this skips unless a REAL host is present. The CI regression guard for the IDSSE
fix is the dbt singular test (assert_psxg_pooled_keeps_idsse) + the pure text guard
(src/tests/test_gk_pooled_join_null_safe.py). This test asserts the app column constants are a
subset of the live mart columns, so a producer rename fails here, not silently in the Space.
"""

import os

import pytest

pytest.importorskip("plotly")

_HOST = os.environ.get("LAKEBASE_HOST", "")
if not _HOST or _HOST == "test-host" or "example" in _HOST:
    pytest.skip("needs a real Lakebase host (operator/live run only)", allow_module_level=True)

from queries.common import execute_query, t  # noqa: E402
from queries.gk_analytics import _SWEEP_COLS  # noqa: E402


def _cols(table: str) -> set[str]:
    # LIMIT 1, not 0: execute_query builds the DataFrame from rows, so a zero-row result yields zero
    # columns (pandas can't infer columns without a row). The synced marts are non-empty.
    return set(execute_query(f"SELECT * FROM {t(table)} LIMIT 1", ()).columns)  # noqa: S608


def test_sweeper_cols_subset_of_stats_mart():
    live = _cols("fct_gk_tracking_stats_synced")
    assert set(_SWEEP_COLS) <= live, set(_SWEEP_COLS) - live


def test_distribution_profile_cols_subset_of_actions_mart():
    # ADR-061: the offensive profile is action-grain off fct_gk_tracking_actions (re-homed onto xt_gk_v2).
    live = _cols("fct_gk_tracking_actions_synced")
    assert {"player_key", "match_key", "xt_gk_v2", "gk_completion", "start_x", "end_x"} <= live


def test_goals_prevented_cols_subset_of_pooled_mart():
    live = _cols("fct_gk_shot_stopping_pooled_synced")
    assert {
        "player_key",
        "competition_key",
        "season_id",
        "data_source",
        "goals_prevented",
        "goals_prevented_ci_low",
        "goals_prevented_ci_high",
        "shots_faced_total",
        "low_sample",
    } <= live


def test_line_cols_subset_of_defensive_line_mart():
    live = _cols("fct_gk_defensive_line_synced")
    assert {
        "gk_player_key",
        "competition_key",
        "data_source",
        "avg_line_height_m",
        "avg_width",
        "avg_compactness",
        "n_actions",
    } <= live
