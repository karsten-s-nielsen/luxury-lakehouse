"""The Runs page as a bound Taipy page (page-binding PB6; audit F6): the run history, a non-blocking Run now, and the
run-detail drill-down — the selected run's steps, spend, dependencies and logs. The log view folds in the former Logs
page: `refresh` selects the latest run and caches its records; `on_log_filter` re-filters the cache client-side; and
`on_pick_run` reloads the detail for a newly picked run."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from lakehouse.observability import DependencyView, LogRecord
from lakehouse.runs.run import RunCounts
from lakehouse.services.runs import RunSummary
from lakehouse.ui.pages import runs
from lakehouse.ui.template import action_names

_NOW = datetime(2026, 10, 28, 9, 0, tzinfo=UTC)


def _cells(columns: dict[str, list[str]]) -> list[str]:
    return [cell for column in columns.values() for cell in column]


def _log(level: str, msg: str) -> LogRecord:
    return LogRecord(
        ts="2026-10-28T09:00:00",
        level=level,
        logger="lakehouse.run",
        msg=msg,
        trace_id=None,
        span_id=None,
        visibility="public",
        extra={},
    )


class _Runs:
    def __init__(self, logs: list[LogRecord]) -> None:
        self._logs = logs

    def history(self, limit: int = 500) -> list[RunSummary]:
        return [RunSummary("r1", "schedule", "ok", _NOW, _NOW, RunCounts(), 0)]

    def task_rows(self, run_id: str) -> list[Any]:
        return []

    def dependency(self, run_id: str) -> DependencyView:
        return DependencyView(run_edges=[], dbt_edges=[], note=None)

    def logs(self, run_id: str, *, limit: int = 200, level: str | None = None, text: str | None = None) -> list[Any]:
        return self._logs


def _services(logs: list[LogRecord]) -> Any:
    return SimpleNamespace(runs=_Runs(logs))


def _state() -> Any:
    return SimpleNamespace(runs_selected="", runs_log_level="", runs_log_text="")


def test_runs_defaults() -> None:
    from lakehouse.ui import filter_state

    # the sidebar Run selector binds the shared default scope (the sidebar binds no page module), not pages/runs.py
    assert filter_state.runs_selected == "" and filter_state.runs_run_lov == [] and not hasattr(runs, "runs_selected")
    assert runs.runs_log_level == ""
    assert action_names(runs.CONFIG) == ("on_run_now",)  # only the Run now button, derived from the Button


def test_runs_refresh_selects_latest_and_fills_logs() -> None:
    state = _state()
    runs.refresh(state, _services([_log("INFO", "started run"), _log("ERROR", "write failed")]))
    assert state.runs_selected == "r1" and state.runs_has_detail is True  # defaults to the latest run
    cells = _cells(state.runs_logs_rows)
    assert any("write failed" in c for c in cells) and any("started run" in c for c in cells)


def test_runs_history_shows_duration_not_end_time() -> None:
    """The history table shows how long each run took (wall-clock), not an end clock time; a still-running run reads
    "in flight"."""
    from datetime import timedelta

    class _Hist(_Runs):
        def history(self, limit: int = 500) -> list[RunSummary]:
            done = RunSummary("r1", "schedule", "ok", _NOW, _NOW + timedelta(seconds=83), RunCounts(), 0)
            flight = RunSummary("r2", "button", "running", _NOW, None, RunCounts(), 0)
            return [done, flight]

        def progress(self, run_id: str) -> Any:  # the in-flight run drives the progress line
            return SimpleNamespace(module_id="statsbomb", step="fetch", done=1, total=3)

    state = _state()
    runs.refresh(state, cast("Any", SimpleNamespace(runs=_Hist([]))))
    assert "Duration" in state.runs_rows and "Ended" not in state.runs_rows
    cells = _cells(state.runs_rows)
    assert "1m 23s" in cells  # 83s wall-clock stated as a duration
    assert "in flight" in cells  # the still-running run


def test_runs_on_log_filter_filters_cache() -> None:
    state = _state()
    runs.refresh(state, _services([_log("INFO", "started run"), _log("ERROR", "write failed")]))
    state.runs_log_level = "ERROR"
    runs.on_log_filter(state, None, "ERROR")  # the shared (state, services, value) handler shape
    cells = _cells(state.runs_logs_rows)
    assert any("write failed" in c for c in cells)  # the ERROR row
    assert all("started run" not in c for c in cells)  # the INFO row filtered out


def test_runs_on_pick_run_reloads_detail() -> None:
    state = _state()
    runs.on_pick_run(state, _services([_log("INFO", "hello")]), "r1")
    assert state.runs_selected == "r1"
    assert any("hello" in c for c in _cells(state.runs_logs_rows))


def test_runs_page_builds() -> None:
    from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

    assert isinstance(runs.page, Markdown)
