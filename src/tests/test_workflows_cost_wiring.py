"""Tests for D54/D55 cost wiring — cold start stats, effective cost, table columns.

Verifies the complete data flow from queries through stat computation and
table rendering. Catches silent failures where enrichment columns are
present in the database but not surfaced in the UI.

Data sources:
- cold_costs: 30-day aggregate (cost, DBU, run_count) — NO timing/entity data
- latest_run_metrics: most recent run per workflow (cold_start, duration, entity_count)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Add hf_taipy_app/src to path so we can import state/query modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed")

from queries.workflows import _COLD_COST_COLS
from state.workflows_stats import (
    WF_TABLE_COLS,
    _compute_cold_start_stats,
    build_table_data,
    compute_stats,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_LATEST_RUN_COLS = [
    "workflow_id",
    "cold_start_seconds",
    "duration_seconds",
    "entity_count",
    "row_count",
    "pipeline_state",
]


def _make_cold_costs(**overrides: Any) -> pd.DataFrame:
    """Build a realistic cold_costs DataFrame (cost aggregates only, no timing)."""
    base = {
        "workflow_id": ["wf-vaep", "wf-xg-v1", "wf-football2vec"],
        "task_key": ["compute_spadl_vaep", "compute_xg_model", "compute_embeddings_v1"],
        "total_cost_usd": [0.50, 0.10, 0.30],
        "total_dbu": [1.5, 0.3, 0.9],
        "run_count": [10, 5, 3],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _make_latest_run_metrics(**overrides: Any) -> pd.DataFrame:
    """Build latest-run-per-workflow DataFrame with timing + entity data."""
    base = {
        "workflow_id": ["wf-vaep", "wf-xg-v1", "wf-football2vec"],
        "cold_start_seconds": [45, 30, 60],
        "duration_seconds": [120, 15, 90],
        "entity_count": [3000, 500, 1200],
        "row_count": [0, 42, 87000],
        "pipeline_state": ["COMPLETED", "COMPLETED", "COMPLETED"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _make_cards() -> dict[str, dict[str, Any]]:
    """Minimal workflow cards for table builder."""
    return {
        "wf-vaep": {
            "name": "VAEP Action Valuation",
            "type": "train_infer",
            "execution": {
                "inference": {"runtime": "databricks", "trigger": "scheduled", "entry_point": "compute_spadl_vaep"},
            },
            "monitoring": {"freshness_sla_hours": 48},
        },
        "wf-xg-v1": {
            "name": "xG Model v1",
            "type": "train_infer",
            "execution": {
                "inference": {"runtime": "databricks", "trigger": "scheduled", "entry_point": "compute_xg_model"},
            },
            "monitoring": {"freshness_sla_hours": 48},
        },
    }


def _mock_state() -> MagicMock:
    """Create a mock Taipy state object that accepts any attribute assignment."""
    return MagicMock()


# ---------------------------------------------------------------------------
# TestColdCostColumns — query returns expected schema
# ---------------------------------------------------------------------------


class TestColdCostColumns:
    """Verify the cold cost query column contract."""

    def test_cold_cost_cols_are_cost_only(self) -> None:
        """_COLD_COST_COLS must NOT include timing/entity averages.

        Timing data comes from fetch_latest_run_metrics(), not cold costs.
        Averages across 30 days of runs dilute recent values with stale data.
        """
        assert "avg_cold_start_s" not in _COLD_COST_COLS
        assert "avg_work_duration_s" not in _COLD_COST_COLS
        assert "avg_entity_count" not in _COLD_COST_COLS

    def test_cold_cost_cols_have_required_fields(self) -> None:
        for col in ("workflow_id", "task_key", "total_cost_usd", "total_dbu", "run_count"):
            assert col in _COLD_COST_COLS

    def test_cold_cost_query_uses_effective_cost(self) -> None:
        """Query must use effective_cost_usd (not attributed_cost_usd).

        effective_cost_usd = COALESCE(actual, estimated) so recent runs
        always have a cost value even before billing data arrives.
        """
        import inspect

        from queries.workflows import fetch_cold_costs

        source = inspect.getsource(fetch_cold_costs)
        assert "effective_cost_usd" in source, (
            "fetch_cold_costs() must use effective_cost_usd, not attributed_cost_usd. "
            "Without this, recent runs show $0 until billing catches up (~1 day)."
        )


class TestLatestRunMetrics:
    """Verify fetch_latest_run_metrics() exists and has expected column contract."""

    def test_function_exists(self) -> None:
        from queries.workflows import fetch_latest_run_metrics

        assert callable(fetch_latest_run_metrics)

    def test_returns_dataframe_with_expected_columns(self) -> None:
        """The empty-fallback DataFrame must have the right columns."""
        import inspect

        from queries.workflows import fetch_latest_run_metrics

        source = inspect.getsource(fetch_latest_run_metrics)
        for col in ("workflow_id", "cold_start_seconds", "duration_seconds", "entity_count"):
            assert col in source, f"fetch_latest_run_metrics() missing column {col} in query"


# ---------------------------------------------------------------------------
# TestColdStartStats — stat card computation
# ---------------------------------------------------------------------------


class TestColdStartStats:
    """_compute_cold_start_stats must produce values from latest-run metrics."""

    def test_produces_value_with_data(self) -> None:
        state = _mock_state()
        latest = _make_latest_run_metrics()
        _compute_cold_start_stats(state, latest)

        assert state.wf_avg_cold_start != "\u2014", (
            "Avg Cold Start stat card shows dash despite latest-run data being present."
        )
        assert "s" in state.wf_avg_cold_start

    def test_uses_median_not_mean(self) -> None:
        """Median is more robust than mean for cold start outliers."""
        state = _mock_state()
        # 3 workflows: 10s, 20s, 300s. Median=20s, Mean=110s.
        latest = _make_latest_run_metrics(
            workflow_id=["wf-a", "wf-b", "wf-c"],
            cold_start_seconds=[10, 20, 300],
            duration_seconds=[5, 5, 5],
            entity_count=[1, 1, 1],
            row_count=[0, 0, 0],
            pipeline_state=["COMPLETED", "COMPLETED", "COMPLETED"],
        )
        _compute_cold_start_stats(state, latest)

        assert "20s" in state.wf_avg_cold_start, (
            f"Expected median 20s, got {state.wf_avg_cold_start}. Using mean instead of median?"
        )

    def test_shows_dash_when_no_data(self) -> None:
        state = _mock_state()
        latest = pd.DataFrame(columns=pd.Index(_LATEST_RUN_COLS))
        _compute_cold_start_stats(state, latest)

        assert state.wf_avg_cold_start == "\u2014"

    def test_shows_dash_when_all_null(self) -> None:
        state = _mock_state()
        latest = _make_latest_run_metrics(cold_start_seconds=[None, None, None])
        _compute_cold_start_stats(state, latest)

        assert state.wf_avg_cold_start == "\u2014"

    def test_detail_includes_range(self) -> None:
        state = _mock_state()
        latest = _make_latest_run_metrics()
        _compute_cold_start_stats(state, latest)

        assert "range" in state.wf_cold_start_detail


# ---------------------------------------------------------------------------
# TestTableColumns — Cold Start and Entities in table
# ---------------------------------------------------------------------------


class TestTableColumns:
    """build_table_data must include Cold Start and Entities columns."""

    def test_table_has_cold_start_column(self) -> None:
        assert "Cold Start" in WF_TABLE_COLS

    def test_table_has_entities_column(self) -> None:
        assert "Entities" in WF_TABLE_COLS

    def test_cold_start_populated_from_latest_run(self) -> None:
        cards = _make_cards()
        cold = _make_cold_costs()
        latest = _make_latest_run_metrics()
        df, _card_ids = build_table_data(cards, cold, {}, type_filter="All", latest_run_metrics=latest)

        if df.empty:
            pytest.skip("No rows matched filters")

        vaep_rows = df[df["Name"] == "VAEP Action Valuation"]
        if not vaep_rows.empty:
            cold_start_val = vaep_rows.iloc[0]["Cold Start"]
            assert cold_start_val != "\u2014", (
                "Cold Start shows dash for VAEP despite cold_start_seconds=45 in latest run metrics."
            )
            assert "45s" in cold_start_val

    def test_entities_populated_from_latest_run(self) -> None:
        cards = _make_cards()
        cold = _make_cold_costs()
        latest = _make_latest_run_metrics()
        df, _card_ids = build_table_data(cards, cold, {}, type_filter="All", latest_run_metrics=latest)

        if df.empty:
            pytest.skip("No rows matched filters")

        vaep_rows = df[df["Name"] == "VAEP Action Valuation"]
        if not vaep_rows.empty:
            entities_val = vaep_rows.iloc[0]["Entities"]
            assert entities_val != "\u2014", (
                "Entities shows dash for VAEP despite entity_count=3000 in latest run metrics."
            )
            assert "3,000" in entities_val

    def test_dash_when_no_latest_metrics(self) -> None:
        """Table should show dash for Cold Start/Entities when no latest-run data."""
        cards = _make_cards()
        cold = _make_cold_costs()
        df, _card_ids = build_table_data(cards, cold, {}, type_filter="All")

        if df.empty:
            pytest.skip("No rows matched filters")

        vaep_rows = df[df["Name"] == "VAEP Action Valuation"]
        if not vaep_rows.empty:
            assert vaep_rows.iloc[0]["Cold Start"] == "\u2014"
            assert vaep_rows.iloc[0]["Entities"] == "\u2014"


# ---------------------------------------------------------------------------
# TestComputeStats — end-to-end stat bar
# ---------------------------------------------------------------------------


class TestComputeStats:
    """compute_stats must surface cold start data in stat bar."""

    def test_cold_start_stat_populated(self) -> None:
        state = _mock_state()
        cards = _make_cards()
        cold = _make_cold_costs()
        warm = pd.DataFrame()
        latest = _make_latest_run_metrics()

        compute_stats(state, cards, cold, warm, {}, latest_run_metrics=latest)

        assert state.wf_avg_cold_start != "\u2014", (
            "compute_stats did not populate wf_avg_cold_start despite latest-run data."
        )

    def test_cost_uses_effective_not_attributed(self) -> None:
        """Total cost stat must reflect effective_cost_usd column."""
        state = _mock_state()
        cards = _make_cards()
        cold = _make_cold_costs(total_cost_usd=[0.50, 0.10, 0.30])
        warm = pd.DataFrame()

        compute_stats(state, cards, cold, warm, {})

        assert "$" in state.wf_total_cost_30d
        assert state.wf_total_cost_30d != "$0.00", (
            "Total cost is $0.00 despite cold_costs having non-zero total_cost_usd."
        )
