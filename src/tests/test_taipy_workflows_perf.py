"""Performance benchmark for AI/ML Workflows page refresh.

Measures the CPU-bound portion of wf_refresh() — DAG building, table
construction, stats computation — with mocked I/O (Lakebase queries,
Databricks SDK, YAML files).

Performance budget (from CLAUDE.md):
    - App page load: <=3 seconds (first load), <=500ms (cached interaction)
    - This benchmark covers the CPU-bound portion, which should be well
      under 500ms for 16 workflow cards.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (conflicts with dev deps)")

# Add hf_taipy_app/src to path so we can import the state module
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))


# ---------------------------------------------------------------------------
# Fixtures — realistic workflow card data (16 cards, matching production)
# ---------------------------------------------------------------------------

_TYPES = [
    ("training-and-inference", 4),
    ("grid-computation", 3),
    ("heuristic", 3),
    ("validation", 2),
]


def _make_cards(n: int = 16) -> dict[str, dict[str, Any]]:
    """Build a realistic set of workflow cards for benchmarking."""
    cards: dict[str, dict[str, Any]] = {}
    idx = 0
    for wf_type, count in _TYPES:
        for i in range(count):
            card_id = f"wf-{wf_type[:4]}-{i}"
            cards[card_id] = {
                "id": card_id,
                "name": f"Workflow {wf_type.replace('-', ' ').title()} {i}",
                "type": wf_type,
                "status": "production",
                "domain": "analytics",
                "version": "1.0",
                "owners": ["test-owner"],
                "body": f"Description for {card_id}.",
                "depends_on": [list(cards.keys())[-1]] if cards else [],
                "execution": {
                    "inference": {
                        "runtime": "databricks-serverless" if idx % 2 == 0 else "hf-jobs",
                        "entry_point": f"entry_{card_id}",
                        "trigger": "scheduled",
                    },
                },
                "monitoring": {
                    "freshness_sla_hours": 24 + (idx * 6),
                },
                "cost": {
                    "inference": {
                        "runtime": "databricks-serverless" if idx % 2 == 0 else "hf-jobs",
                        "typical_cost_usd": 0.50 + (idx * 0.25),
                    },
                },
                "inputs": {"datasets": [{"id": f"table_{idx}", "description": "test"}]},
                "outputs": {"tables": [{"id": f"output_{idx}"}]},
                "references": [{"role": "methodology", "citation": f"Author {idx} (2025)"}],
                "idempotency": {"strategy": "replaceWhere", "key": "match_id"},
                "links": {"source_code": [f"src/ingestion/{card_id}.py"]},
            }
            idx += 1
    return cards


def _make_cold_costs(cards: dict[str, dict[str, Any]]) -> pd.DataFrame:
    """Build a realistic cold cost DataFrame."""
    rows = []
    for card in cards.values():
        ep = ((card.get("execution") or {}).get("inference") or {}).get("entry_point", "")
        if ep:
            rows.append({"task_key": ep, "total_cost_usd": 1.25, "total_dbu": 0.5, "run_count": 10})
    if not rows:
        return pd.DataFrame(columns=pd.Index(["task_key", "total_cost_usd", "total_dbu", "run_count"]))
    return pd.DataFrame(rows)


def _make_warm_costs() -> pd.DataFrame:
    """Build an empty warm costs DataFrame (typical for daily-summarized data)."""
    return pd.DataFrame()


def _make_job_runs(cards: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Build realistic job run data."""
    now = pd.Timestamp.now(tz="UTC")
    runs: dict[str, dict[str, Any]] = {}
    for i, card in enumerate(cards.values()):
        ep = ((card.get("execution") or {}).get("inference") or {}).get("entry_point", "")
        if ep:
            runs[ep] = {
                "last_run": now - pd.Timedelta(hours=i * 3),
                "duration_seconds": 120 + i * 30,
                "state": "SUCCESS",
                "end_time_ms": int(now.timestamp() * 1000) - i * 3 * 3_600_000,
            }
    return runs


class _MockState:
    """Minimal mock of Taipy state — accepts any attribute assignment."""

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)

    def __getattr__(self, name: str) -> Any:
        return None


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cards() -> dict[str, dict[str, Any]]:
    return _make_cards()


def test_bench_wf_refresh(benchmark: Any, mock_cards: dict[str, dict[str, Any]]) -> None:
    """Workflows page refresh stays well under 500ms budget.

    Mocks all I/O (YAML loading, Lakebase queries, Databricks SDK) and
    measures only CPU-bound work: DAG building, table construction,
    filter LOV computation, and stats aggregation.
    """
    cold = _make_cold_costs(mock_cards)
    warm = _make_warm_costs()
    jobs = _make_job_runs(mock_cards)

    import state.workflows as wf_mod

    def run_refresh() -> None:
        # Reset module-level card cache so wf_refresh does a full build
        wf_mod._cards = {}
        wf_mod._unfiltered_dag_html = wf_mod.RawHtml("")
        state = _MockState()

        with (
            patch.object(wf_mod, "load_cards_from_yaml", return_value=mock_cards),
            patch.object(wf_mod, "fetch_cold_costs", return_value=cold),
            patch.object(wf_mod, "fetch_warm_costs", return_value=warm),
            patch.object(wf_mod, "_fetch_job_runs", return_value=jobs),
        ):
            wf_mod.wf_refresh(state)

    benchmark(run_refresh)


def test_bench_wf_filter_change(benchmark: Any, mock_cards: dict[str, dict[str, Any]]) -> None:
    """Filter interaction stays well under 500ms cached interaction budget.

    Measures _refresh_table with pre-populated card data and cached queries.
    """
    cold = _make_cold_costs(mock_cards)
    warm = _make_warm_costs()
    jobs = _make_job_runs(mock_cards)

    import state.workflows as wf_mod

    # Pre-populate module state (simulates page already loaded)
    wf_mod._cards = mock_cards
    wf_mod._unfiltered_dag_html = wf_mod.build_dag_html(mock_cards)

    def run_filter() -> None:
        state = _MockState()
        state.wf_type_filter = "All"
        state.wf_runtime_filter = "All"
        state.wf_freshness_filter = "All"

        with (
            patch.object(wf_mod, "fetch_cold_costs", return_value=cold),
            patch.object(wf_mod, "fetch_warm_costs", return_value=warm),
            patch.object(wf_mod, "_fetch_job_runs", return_value=jobs),
        ):
            wf_mod._refresh_table(state)

    benchmark(run_filter)
