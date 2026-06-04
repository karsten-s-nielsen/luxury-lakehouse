"""Per-game watchdog budget bumped to 2700; drain entry exposes a guarded override."""

import inspect

from analytics.action_context import drain
from ingestion import action_context


def test_watchdog_budget_default_2700():
    assert drain.WATCHDOG_BUDGET_S == 2700


def test_drain_worker_entry_wires_budget_override():
    src = inspect.getsource(action_context.main_drain_worker)
    assert "--watchdog-budget-s" in src
    assert "budget_s=budget_s" in src
