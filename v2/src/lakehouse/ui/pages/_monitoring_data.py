"""Taipy-free monitoring computation (page-binding addendum §3): the spend estimate the Monitoring page and the
anonymous public page both render. It imports no Taipy and no socket, so the public page stays Taipy-free even once
the Taipy Monitoring page gains a module-level `Markdown`. Pure functions over `observability` types only."""

from __future__ import annotations

from lakehouse.observability import RATE_TABLE_DATE, TaskRow

__all__ = ["DBT_HEADERS", "STEP_HEADERS", "spend_line", "step_dict"]

# The per-step and dbt-edge table headers, shared so the Monitoring page (latest run, anon) and the Runs page's
# run-detail drill-down render the same step table from one source (F6 consistency).
STEP_HEADERS = ("Step", "Layer", "Duration", "Rows", "State")
DBT_HEADERS = ("From (silver)", "To (gold)")


def step_dict(row: TaskRow) -> dict[str, str]:
    """One run step as a native-table row dict (step, layer, duration, rows, state)."""
    return {
        "Step": row.task,
        "Layer": row.layer or "—",
        "Duration": f"{row.duration_seconds:.3f}s",
        "Rows": "—" if row.row_count is None else str(row.row_count),
        "State": row.state,
    }


def spend_line(rows: list[TaskRow]) -> str:
    """The run's spend as a dated estimate (spec §7.3). A missing or failed cost is surfaced, never shown as $0."""
    run_row = next((row for row in rows if row.cost_usd is not None or row.cost_error is not None), None)
    if run_row is None:
        return "Spend: not available for this run"
    if run_row.cost_error:
        return f"Spend: not available — {run_row.cost_error}"
    return f"Spend: est. ${run_row.cost_usd:.2f} at {RATE_TABLE_DATE} rates (process time)"
