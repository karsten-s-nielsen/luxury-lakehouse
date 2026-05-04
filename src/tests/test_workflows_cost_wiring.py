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
    "guard_duration_seconds",
    "entity_count",
    "row_count",
    "pipeline_state",
]


def _make_cold_costs(**overrides: Any) -> pd.DataFrame:
    """Build a realistic cold_costs DataFrame (cost aggregates only, no timing)."""
    base = {
        "workflow_id": ["wf-vaep", "wf-xg-v2", "wf-football2vec"],
        "task_key": ["compute_spadl_vaep", "compute_xg_model_v2", "compute_embeddings_v1"],
        "total_cost_usd": [0.50, 0.10, 0.30],
        "total_dbu": [1.5, 0.3, 0.9],
        "run_count": [10, 5, 3],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _make_latest_run_metrics(**overrides: Any) -> pd.DataFrame:
    """Build latest-run-per-workflow DataFrame with timing + entity data."""
    base = {
        "workflow_id": ["wf-vaep", "wf-xg-v2", "wf-football2vec"],
        "cold_start_seconds": [45, 30, 60],
        "duration_seconds": [120, 15, 90],
        "guard_duration_seconds": [5, 2, 8],
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
        "wf-xg-v2": {
            "name": "xG Model v2 (Deep Sets)",
            "type": "train_infer",
            "execution": {
                "inference": {"runtime": "databricks", "trigger": "scheduled", "entry_point": "compute_xg_model_v2"},
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

    def test_query_selects_guard_duration_seconds(self) -> None:
        """fetch_latest_run_metrics() must include guard_duration_seconds in its SELECT.

        The dbt model exposes this column at fct_workflow_costs.sql:91, 141.
        Without it, the new Guard Duration column on the Workflows page renders dashes.
        """
        import inspect

        from queries.workflows import fetch_latest_run_metrics

        source = inspect.getsource(fetch_latest_run_metrics)
        assert "guard_duration_seconds" in source, (
            "fetch_latest_run_metrics() does not select guard_duration_seconds — "
            "the dbt model exposes it but the query does not pull it."
        )

    def test_latest_run_cols_includes_guard_duration_seconds(self) -> None:
        """_LATEST_RUN_COLS must list guard_duration_seconds.

        This is the column-list constant used to build the empty-fallback DataFrame.
        If the query selects guard_duration_seconds but the constant doesn't list it,
        downstream lookups in workflows_stats.py will KeyError on empty datasets.
        """
        from queries.workflows import _LATEST_RUN_COLS

        assert "guard_duration_seconds" in _LATEST_RUN_COLS


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

    def test_table_does_not_have_last_duration_column(self) -> None:
        """Regression guard: 'Last Duration' (Jobs API total) is replaced by the
        verifiable three-way decomposition (Cold Start + Guard Duration + Workflow Duration).
        """
        assert "Last Duration" not in WF_TABLE_COLS

    def test_table_has_guard_duration_column(self) -> None:
        assert "Guard Duration" in WF_TABLE_COLS

    def test_table_has_workflow_duration_column(self) -> None:
        assert "Workflow Duration" in WF_TABLE_COLS

    def test_temporal_columns_are_contiguous_and_in_order(self) -> None:
        """Cold Start | Guard Duration | Workflow Duration must be adjacent and in
        temporal order (env startup -> guard check -> main work)."""
        cs_idx = WF_TABLE_COLS.index("Cold Start")
        gd_idx = WF_TABLE_COLS.index("Guard Duration")
        wd_idx = WF_TABLE_COLS.index("Workflow Duration")
        assert gd_idx == cs_idx + 1, (
            f"Guard Duration must immediately follow Cold Start. "
            f"Cold Start at index {cs_idx}, Guard Duration at index {gd_idx}."
        )
        assert wd_idx == gd_idx + 1, (
            f"Workflow Duration must immediately follow Guard Duration. "
            f"Guard Duration at index {gd_idx}, Workflow Duration at index {wd_idx}."
        )

    def test_guard_duration_populated_from_latest_run(self) -> None:
        """Guard Duration cell shows the value from latest_run_metrics, formatted as Ns or NmN s."""
        cards = _make_cards()
        cold = _make_cold_costs()
        latest = _make_latest_run_metrics(
            workflow_id=["wf-vaep", "wf-xg-v2", "wf-football2vec"],
            cold_start_seconds=[45, 30, 60],
            duration_seconds=[120, 15, 90],
            guard_duration_seconds=[5, 2, 8],
            entity_count=[3000, 500, 1200],
            row_count=[0, 42, 87000],
            pipeline_state=["COMPLETED", "COMPLETED", "COMPLETED"],
        )
        df, _card_ids = build_table_data(cards, cold, {}, type_filter="All", latest_run_metrics=latest)

        if df.empty:
            pytest.skip("No rows matched filters")

        vaep_rows = df[df["Name"] == "VAEP Action Valuation"]
        if not vaep_rows.empty:
            assert vaep_rows.iloc[0]["Guard Duration"] == "5s", (
                f"Guard Duration should be '5s' for VAEP "
                f"(guard_duration_seconds=5 in fixture), got {vaep_rows.iloc[0]['Guard Duration']!r}"
            )

    def test_workflow_duration_populated_from_latest_run_not_jobs_api(self) -> None:
        """Workflow Duration must source from latest_run_metrics (cost table),
        NOT from the Databricks Jobs API which conflates cold start + guard + workflow.

        This is the regression guard for the source-of-truth swap. If a future change
        accidentally re-routes Workflow Duration to the Jobs API, this test catches it.
        """
        cards = _make_cards()
        cold = _make_cold_costs()
        latest = _make_latest_run_metrics(
            workflow_id=["wf-vaep", "wf-xg-v2", "wf-football2vec"],
            cold_start_seconds=[45, 30, 60],
            duration_seconds=[120, 15, 90],
            guard_duration_seconds=[5, 2, 8],
            entity_count=[3000, 500, 1200],
            row_count=[0, 42, 87000],
            pipeline_state=["COMPLETED", "COMPLETED", "COMPLETED"],
        )
        # Pass a job_runs dict with a deliberately-wrong duration to prove
        # the cell does NOT source from it.
        from datetime import datetime, timezone

        bad_jobs = {
            "wf-vaep": {
                "last_run": datetime(2026, 4, 13, tzinfo=timezone.utc),
                "duration_seconds": 9999,  # wrong on purpose
                "state": "TERMINATED",
            },
        }
        df, _card_ids = build_table_data(cards, cold, bad_jobs, type_filter="All", latest_run_metrics=latest)

        if df.empty:
            pytest.skip("No rows matched filters")

        vaep_rows = df[df["Name"] == "VAEP Action Valuation"]
        if not vaep_rows.empty:
            workflow_dur = vaep_rows.iloc[0]["Workflow Duration"]
            # 120 seconds → "2m 0s"
            assert workflow_dur == "2m 0s", (
                f"Workflow Duration should be '2m 0s' from cost table (duration_seconds=120), "
                f"got {workflow_dur!r}. If '2h 46m 39s' or '9999s', the Jobs API value leaked in."
            )

    def test_dash_when_no_guard_or_workflow_duration_data(self) -> None:
        """When latest_run_metrics is empty, both new columns show em-dash."""
        cards = _make_cards()
        cold = _make_cold_costs()
        df, _card_ids = build_table_data(cards, cold, {}, type_filter="All")

        if df.empty:
            pytest.skip("No rows matched filters")

        vaep_rows = df[df["Name"] == "VAEP Action Valuation"]
        if not vaep_rows.empty:
            assert vaep_rows.iloc[0]["Guard Duration"] == "\u2014"
            assert vaep_rows.iloc[0]["Workflow Duration"] == "\u2014"


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
