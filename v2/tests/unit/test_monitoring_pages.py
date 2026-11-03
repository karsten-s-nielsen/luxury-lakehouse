"""The monitoring page and the Runs run-detail log view (spec §6.3, §7.2-§7.4, §9; plan Task 9; audit F6): the
spend/dependency view is anon-safe (no match identity), the operator-only pages refuse an anon caller, and the
run-detail logs are authenticated-only and show the most recent N newest-first."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from lakehouse.observability import DependencyView, LogRecord, TaskRow
from lakehouse.runs.run import RunCounts
from lakehouse.services.runs import RunSummary
from lakehouse.ui.shell import Services, anon_viewable, render_for

_NOW = datetime(2026, 10, 28, 9, 0, tzinfo=UTC)
_SERVICE_FIELDS = ("settings", "modules", "filters", "runs", "review", "schedule")


def _services(**overrides: Any) -> Services:
    base: dict[str, Any] = {k: cast("Any", object()) for k in _SERVICE_FIELDS}
    base.update(overrides)
    return Services(**base)


class _FakeRuns:
    def __init__(
        self,
        history: list[RunSummary],
        *,
        rows: list[TaskRow] | None = None,
        dep: DependencyView | None = None,
        logs: list[LogRecord] | None = None,
    ) -> None:
        self._history = history
        self._rows = rows or []
        self._dep = dep or DependencyView(run_edges=[], dbt_edges=[], note=None)
        self._logs = logs or []
        self.limit_seen: int | None = None

    def history(self, limit: int = 500) -> list[RunSummary]:
        return self._history[:limit]

    def task_rows(self, run_id: str) -> list[TaskRow]:
        return self._rows

    def dependency(self, run_id: str) -> DependencyView:
        return self._dep

    def logs(
        self, run_id: str, *, limit: int = 200, level: str | None = None, text: str | None = None
    ) -> list[LogRecord]:
        self.limit_seen = limit
        return self._logs[:limit]


def _run_row(**over: Any) -> TaskRow:
    base: dict[str, Any] = {
        "run_id": "r1",
        "task": "lakehouse.run",
        "parent_task": None,
        "layer": None,
        "started_at": "2026-10-28T09:00:00+00:00",
        "duration_seconds": 12.5,
        "row_count": None,
        "state": "ok",
        "visibility": None,
        "execution_context": "hf_job",
        "flavour": "cpu-basic",
        "cost_usd": 0.02,
        "cost_error": None,
    }
    base.update(over)
    return TaskRow(**base)


def _history() -> list[RunSummary]:
    return [RunSummary("r1", "schedule", "ok", _NOW, _NOW, RunCounts(), 0)]


def test_monitoring_public_no_match_identity() -> None:
    rows = [
        _run_row(),  # the run-level row carries the spend
        _run_row(  # a private-match step: a layer/duration/state, but no match name or id
            task="lakehouse.run.write",
            parent_task="lakehouse.run",
            layer="bronze",
            row_count=1000,
            visibility="private",
            execution_context=None,
            flavour=None,
            cost_usd=None,
        ),
    ]
    dep = DependencyView(
        run_edges=[("bronze", "resolution"), ("resolution", "dbt")], dbt_edges=[("stg_x", "dim_y")], note=None
    )
    services = _services(runs=_FakeRuns(_history(), rows=rows, dep=dep))

    public = render_for("monitoring", services, "public")
    operator = render_for("monitoring", services, "operator")
    assert public == operator  # anon-safe: no gating, the same content for both
    assert "est. $0.02 at 2026-10-26 rates (process time)" in public  # the dated spend estimate
    assert "lakehouse.run.write" in public and "bronze" in public  # steps shown
    assert "stg_x" in public and "dim_y" in public  # the silver→gold dependency
    assert "match" not in public.lower()  # no match name or id reaches the view (§6.4)


def test_operator_only_pages() -> None:
    services = _services()
    for route in ("runs", "review", "settings"):
        assert "sign in" in render_for(route, services, "public").lower()  # OBS-B31: operator-only


def test_run_detail_logs_authenticated_top_n() -> None:
    """The run-detail log view (folded from the Logs page into the Runs drill-down, audit F6): operator-only, the most
    recent N newest-first for the selected run."""
    assert anon_viewable("runs") is False  # Runs reads match-scoped detail: authenticated-only (§9)
    records = [
        LogRecord(
            ts=f"2026-10-28T09:00:0{i}",
            level="ERROR" if i == 0 else "INFO",
            logger="lakehouse.run",
            msg=f"line {i}",
            trace_id=None,
            span_id=None,
            visibility="public",
            extra={},
        )
        for i in range(3)
    ]  # newest first, as read_logs returns
    fake = _FakeRuns(_history(), logs=records)
    services = _services(runs=fake)

    assert "sign in" in render_for("runs", services, "public").lower()  # anon refused

    operator = render_for("runs", services, "operator")
    assert "line 0" in operator and "line 2" in operator
    assert operator.index("line 0") < operator.index("line 2")  # newest first
    assert fake.limit_seen == 200  # the most recent N (default 200)
