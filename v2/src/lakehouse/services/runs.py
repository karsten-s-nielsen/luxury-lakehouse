"""The Runs page's service (spec §17.4, §18, §19.5; plan Task 25): run history, the progress of a run, and "Run now".

History merges three sources, newest first: the run-history rows of the published catalog, the skip records that no
run has folded yet, and the progress objects of runs still running (not yet in history). "Run now" starts a detached
CLI run locally, or triggers the owner's HF scheduled job; a trigger that HF refuses is shown, never lost silently.
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal, Protocol

import duckdb

from lakehouse.errors import HttpStatusError
from lakehouse.observability import DependencyView, LogRecord, TaskRow, dependency_view, read_logs, read_task_rows
from lakehouse.runs.history import read_table
from lakehouse.runs.progress import PROGRESS_PREFIX, read_progress
from lakehouse.runs.progress import Progress as RunProgress
from lakehouse.runs.run import RunCounts, Trigger
from lakehouse.services.catalog import read_catalog
from lakehouse.services.settings import TIMELINE_LIMIT
from lakehouse.storage.base import Storage
from lakehouse.storage.lease import SKIP_PREFIX, SkipRecord

__all__ = [
    "HfRunTrigger",
    "LocalRunTrigger",
    "Progress",
    "RunService",
    "RunSummary",
    "RunTrigger",
    "TriggerResult",
    "run_counts",
]

_REFUSED: Final = "not started: a run is already active"


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    trigger: Trigger
    status: Literal["ok", "failed", "skipped", "running"]
    started_at: datetime
    ended_at: datetime | None
    counts: RunCounts
    error_count: int


@dataclass(frozen=True)
class Progress:
    run_id: str
    step: str
    module_id: str | None
    match_name: str | None  # the run's progress names no match: no raw id reaches a page
    done: int
    total: int | None
    updated_at: datetime


@dataclass(frozen=True)
class TriggerResult:
    started: bool
    run_id: str | None
    message: str


class RunTrigger(Protocol):
    def start(self) -> TriggerResult: ...


class _Triggerable(Protocol):
    def trigger(self) -> str: ...


class LocalRunTrigger:
    """Starts `python -m lakehouse.cli run --trigger button` as a detached process; the run takes the lease itself."""

    def __init__(self, *, command: Sequence[str] | None = None, popen: Callable[..., Any] = subprocess.Popen) -> None:
        self._command = (
            list(command)
            if command is not None
            else [
                sys.executable,
                "-m",
                "lakehouse.cli",
                "run",
                "--trigger",
                "button",
            ]
        )
        self._popen = popen

    def start(self) -> TriggerResult:
        options: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "close_fds": True,
        }
        if sys.platform == "win32":
            options["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            options["start_new_session"] = True
        self._popen(self._command, **options)
        return TriggerResult(True, None, "started: the run shows here when it takes the lease")


class HfRunTrigger:
    """Triggers the owner-created HF scheduled job (§18); it never creates a job."""

    def __init__(self, schedule: _Triggerable) -> None:
        self._schedule = schedule

    def start(self) -> TriggerResult:
        try:
            self._schedule.trigger()
        except HttpStatusError as exc:
            if exc.status == 409:
                return TriggerResult(False, None, _REFUSED)
            return TriggerResult(False, None, f"not started: HF refused the trigger (HTTP {exc.status})")
        return TriggerResult(True, None, "started: the HF job runs now and shows here when it takes the lease")


def run_counts(row: dict[str, Any]) -> RunCounts:
    """The counts of an `ops_runs` row."""
    return RunCounts(
        added=int(row["added"] or 0),
        changed=int(row["changed"] or 0),
        unchanged=int(row["unchanged"] or 0),
        failed=int(row["failed"] or 0),
        skipped=int(row["skipped"] or 0),
        excluded=int(row["excluded"] or 0),
    )


class RunService:
    def __init__(self, storage: Storage, *, trigger: RunTrigger) -> None:
        self._storage = storage
        self._trigger = trigger

    def _history_rows(self) -> tuple[list[dict[str, Any]], dict[str, int]]:
        def read(conn: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], dict[str, int]]:
            errors: dict[str, int] = {}
            for row in read_table(conn, "ops_errors"):
                if row["severity"] == "error":
                    errors[row["run_id"]] = errors.get(row["run_id"], 0) + 1
            return read_table(conn, "ops_runs"), errors

        return read_catalog(self._storage, read, empty=([], {}))

    def history(self, limit: int = 500) -> list[RunSummary]:
        """The runs, newest first, at most `limit` (a timeline: at most 2000)."""
        if not 1 <= limit <= TIMELINE_LIMIT:
            raise ValueError(f"limit must be 1 to {TIMELINE_LIMIT}")
        rows, errors = self._history_rows()
        summaries = [
            RunSummary(
                run_id=row["run_id"],
                trigger=row["trigger"] or "schedule",
                status=row["status"],
                started_at=row["started_at"],
                ended_at=row["ended_at"],
                counts=run_counts(row),
                error_count=errors.get(row["run_id"], 0),
            )
            for row in rows
        ]
        known = {summary.run_id for summary in summaries}
        for info in self._storage.list("lake", SKIP_PREFIX):
            record = SkipRecord.model_validate_json(self._storage.get("lake", info.key))
            if record.run_id not in known:  # not folded into history yet
                known.add(record.run_id)
                summaries.append(
                    RunSummary(record.run_id, "schedule", "skipped", record.at, record.at, RunCounts(skipped=1), 0)
                )
        for info in self._storage.list("lake", PROGRESS_PREFIX):
            progress = RunProgress.model_validate_json(self._storage.get("lake", info.key))
            if progress.status == "running" and progress.run_id not in known:
                trigger: Trigger = progress.trigger if progress.trigger in ("schedule", "button", "cli") else "schedule"  # type: ignore[assignment]
                summaries.append(
                    RunSummary(progress.run_id, trigger, "running", progress.started_at, None, RunCounts(), 0)
                )
        summaries.sort(key=lambda s: (s.started_at, s.run_id), reverse=True)
        return summaries[:limit]

    def progress(self, run_id: str) -> Progress:
        found = read_progress(self._storage, run_id)
        return Progress(
            run_id=found.run_id,
            step=found.step,
            module_id=found.module_id,
            match_name=None,
            done=found.done,
            total=found.total,
            updated_at=found.updated_at,
        )

    def trigger(self) -> TriggerResult:
        return self._trigger.start()

    def task_rows(self, run_id: str) -> list[TaskRow]:
        """The run's observability rows, one per step (spec §7.2; plan Task 9 monitoring page). Empty for a run with
        no observability object yet."""
        return read_task_rows(self._storage, run_id)

    def dependency(self, run_id: str) -> DependencyView:
        """The run's inter-layer dependency view: run-step layer edges plus the dbt silver→gold sub-DAG (spec §7.4)."""
        return dependency_view(self._storage, run_id)

    def logs(
        self, run_id: str, *, limit: int = 200, level: str | None = None, text: str | None = None
    ) -> list[LogRecord]:
        """The run's most recent `limit` collected log records, newest first, filtered by level and text (spec §9)."""
        return read_logs(self._storage, run_id, limit, level, text)
