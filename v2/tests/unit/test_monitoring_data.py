"""The Taipy-free monitoring data module (page-binding plan PB1): the shared spend computation the public page and
the Taipy monitoring page both use must live here and import no Taipy, so the public page stays socket-free and
Taipy-free even once the monitoring page gains a module-level Markdown (PB5)."""

from __future__ import annotations

import subprocess
import sys
from typing import Any

from lakehouse.observability import RATE_TABLE_DATE, TaskRow
from lakehouse.ui.pages._monitoring_data import spend_line


def _row(**over: Any) -> TaskRow:
    base: dict[str, Any] = {
        "run_id": "r1",
        "task": "lakehouse.run",
        "parent_task": None,
        "layer": None,
        "started_at": "2026-10-28T09:00:00+00:00",
        "duration_seconds": 1.0,
        "row_count": None,
        "state": "ok",
        "visibility": None,
        "execution_context": "hf_job",
        "flavour": "cpu-basic",
        "cost_usd": None,
        "cost_error": None,
    }
    base.update(over)
    return TaskRow(**base)


def test_spend_line_cases() -> None:
    assert spend_line([_row(cost_usd=0.02)]) == f"Spend: est. $0.02 at {RATE_TABLE_DATE} rates (process time)"
    assert spend_line([_row(cost_usd=None, cost_error="missing flavour")]) == "Spend: not available — missing flavour"
    assert spend_line([_row()]) == "Spend: not available for this run"


def test_monitoring_data_imports_no_taipy() -> None:
    code = (
        "import lakehouse.ui.pages._monitoring_data, sys; "
        "assert 'taipy' not in sys.modules, sorted(m for m in sys.modules if 'taipy' in m)"
    )
    result = subprocess.run(  # noqa: S603 - trusted: our own constant code string and sys.executable
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
