"""The run-written observability object (spec §7.2; plan Task 2).

One `TaskRow` per run step, written by the run only (never the web app, OBS-B32), read by the Ops pages. It is a
small per-run object outside the DuckLake catalog, like `runs/progress.py`. The OTel exporter in `telemetry_export.py`
turns finished spans into rows and hands them to a `SpanSink`; `ObservabilityWriter` is the sink that persists them.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, Final, Protocol, cast, runtime_checkable

from lakehouse.logging import active_masker, record_to_payload
from lakehouse.storage.base import Root

__all__ = [
    "FLAVOUR_RATES_USD_PER_HOUR",
    "RATE_TABLE_DATE",
    "RUN_LAYER_ORDER",
    "DependencyView",
    "InMemorySink",
    "LogRecord",
    "ObservabilityWriter",
    "SpanSink",
    "TaskRow",
    "collect_logs",
    "dbt_manifest_key",
    "dbt_model_dag",
    "dependency_view",
    "log_object_key",
    "observability_key",
    "read_logs",
    "read_task_rows",
    "row_visibility",
    "run_cost",
    "write_task_rows",
]

# The run-step layers in execution order (cycle 1 §17.2). The dbt step contains the silver→gold sub-DAG (§7.4).
RUN_LAYER_ORDER: Final = ("bronze", "resolution", "dbt")

# HF Jobs prices, cycle 1 §22.2, dated 2026-10-26. An unknown flavour is a visible error, never a silent 0 (§7.3).
FLAVOUR_RATES_USD_PER_HOUR: Final[dict[str, float]] = {"cpu-basic": 0.01, "cpu-upgrade": 0.03}
RATE_TABLE_DATE: Final = "2026-10-26"


def run_cost(
    execution_context: str | None, flavour: str | None, duration_hours: float
) -> tuple[float | None, str | None]:
    """The run's spend (spec §7.3). A local run is free; an HF Job costs `flavour_rate` times `duration_hours`. A
    missing or unknown `execution_context` or `flavour` returns `(None, <error>)` — never a silent 0, and a missing
    context never defaults to `local` (OBS-B16 / OBS-B24)."""
    if execution_context == "local":
        return (0.0, None)
    if execution_context != "hf_job":
        return (None, f"missing or unknown execution_context {execution_context!r}")
    if flavour is None:
        return (None, "missing flavour for an hf_job run")
    rate = FLAVOUR_RATES_USD_PER_HOUR.get(flavour)
    if rate is None:
        return (None, f"unknown flavour {flavour!r} (not in the rate table dated {RATE_TABLE_DATE})")
    return (round(rate * duration_hours, 6), None)


@dataclass(frozen=True)
class TaskRow:
    """One run step. `cost_usd`/`cost_error`/`execution_context`/`flavour` carry the run-level spend (plan Task 3);
    `visibility` is `public`, `private` or `None` (a run-level step), and §6.4 never shows `None`/`private` to anon."""

    run_id: str
    task: str
    parent_task: str | None
    layer: str | None
    started_at: str
    duration_seconds: float
    row_count: int | None
    state: str
    visibility: str | None
    execution_context: str | None
    flavour: str | None
    cost_usd: float | None
    cost_error: str | None


def row_visibility(module_id: str | None, match_private: bool | None) -> str | None:
    """A step's visibility (spec §7.2, OBS-B22 / D2-SPEC-07). A step touching any private match is `private`; a
    module-level step (no match) of a `*_pining` module is `private` because pining modules mix public and private
    entries (cycle 1 §9); a public direct module is `public`; a run-level step with no module is `None`."""
    if match_private:
        return "private"
    if module_id is None:
        return None
    return "private" if module_id.endswith("_pining") else "public"


class _Store(Protocol):
    """The slice of `Storage` the writer and readers need (both the real `Storage` and a test fake satisfy it). It
    uses `get`, the real `Storage` method, not `read`."""

    def put(self, root: Root, key: str, data: bytes | Path) -> None: ...
    def get(self, root: Root, key: str) -> bytes: ...


def observability_key(run_id: str) -> str:
    return f"runs/observability/{run_id}.json"


def write_task_rows(storage: _Store, run_id: str, rows: list[TaskRow]) -> None:
    """Write the run's task rows to its observability object (overwrites; the full set each time, as the span
    processor exports incrementally)."""
    data = json.dumps([asdict(row) for row in rows]).encode("utf-8")
    storage.put("lake", observability_key(run_id), data)


def read_task_rows(storage: _Store, run_id: str) -> list[TaskRow]:
    try:
        raw = storage.get("lake", observability_key(run_id))
    except (KeyError, OSError):
        return []  # a run with no observability object yet (older runs, or before the first step)
    records: list[dict[str, object]] = json.loads(raw)
    return [TaskRow(**record) for record in records]  # type: ignore[arg-type] - json gives the row's own fields


@runtime_checkable
class SpanSink(Protocol):
    """Where the exporter sends the rows it builds from finished spans."""

    def export(self, rows: list[TaskRow]) -> None: ...


class InMemorySink:
    """A `SpanSink` that keeps the rows in memory (tests, and the clean-extension swap of plan Task 11)."""

    def __init__(self) -> None:
        self.rows: list[TaskRow] = []

    def export(self, rows: list[TaskRow]) -> None:
        self.rows.extend(rows)


class ObservabilityWriter:
    """A `SpanSink` that persists the rows to the run's observability object. Invoked only from the run entrypoint
    (never the web app, OBS-B32); it buffers the run's rows and rewrites the object on each export."""

    def __init__(self, storage: _Store, run_id: str) -> None:
        self._storage = storage
        self._run_id = run_id
        self._rows: list[TaskRow] = []

    def export(self, rows: list[TaskRow]) -> None:
        self._rows.extend(rows)
        write_task_rows(self._storage, self._run_id, self._rows)


def dbt_manifest_key(run_id: str) -> str:
    """Where the run retains dbt's `manifest.json`, beside its `run_results.json` (spec §21). `runs/dbt.py` writes
    it here; `dependency_view` reads it, so the DAG logic stays in this module and never imports `lakehouse.runs`."""
    return f"runs/dbt/{run_id}/manifest.json"


def _dag_from_manifest(manifest: Mapping[str, Any]) -> list[tuple[str, str]]:
    """The model→model edges of a dbt manifest: a staging (silver) model feeding a mart (gold) model (§7.4)."""
    nodes = cast("Mapping[str, Mapping[str, Any]]", manifest.get("nodes", {}))
    model_names = {uid: str(node["name"]) for uid, node in nodes.items() if node.get("resource_type") == "model"}
    edges: set[tuple[str, str]] = set()
    for uid, node in nodes.items():
        if uid not in model_names:
            continue
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep in model_names:
                edges.add((model_names[dep], model_names[uid]))
    return sorted(edges)


def dbt_model_dag(manifest_path: Path) -> list[tuple[str, str]]:
    """The silver→gold edges parsed from a dbt `manifest.json` on disk (spec §7.4, plan Task 4)."""
    return _dag_from_manifest(json.loads(Path(manifest_path).read_text(encoding="utf-8")))


@dataclass(frozen=True)
class DependencyView:
    """A run's inter-layer dependencies (spec §7.4). `run_edges` are the consecutive run-step layers in execution
    order (`bronze → resolution → dbt`); `dbt_edges` is the dbt step's silver→gold sub-DAG; `note` carries a visible
    reason when the sub-DAG is empty (no dbt step, or no manifest retained) instead of a crash."""

    run_edges: list[tuple[str, str]]
    dbt_edges: list[tuple[str, str]]
    note: str | None


def dependency_view(storage: _Store, run_id: str) -> DependencyView:
    """Build the run's dependency view from its task rows and retained dbt manifest. An absent manifest gives an
    empty `dbt_edges` and a visible `note` (Review Focus 5), never a crash."""
    rows = read_task_rows(storage, run_id)
    present = [layer for layer in RUN_LAYER_ORDER if any(row.layer == layer for row in rows)]
    run_edges = list(pairwise(present))
    try:
        raw = storage.get("lake", dbt_manifest_key(run_id))
    except (KeyError, OSError):
        return DependencyView(run_edges=run_edges, dbt_edges=[], note="no dbt manifest retained for this run")
    return DependencyView(run_edges=run_edges, dbt_edges=_dag_from_manifest(json.loads(raw)), note=None)


_LOG_KNOWN_FIELDS: Final[frozenset[str]] = frozenset(
    {"ts", "level", "logger", "msg", "trace_id", "span_id", "visibility"}
)


def log_object_key(run_id: str) -> str:
    """Where the run's collected log copy lives, one object per run in the §7.2 run-written object family (spec §9)."""
    return f"runs/logs/{run_id}.json"


@dataclass(frozen=True)
class LogRecord:
    """One collected log line (spec §9). `trace_id`/`span_id` correlate to the span (present only with the SDK on);
    `visibility` lets a later public view redact private-match records (§12); `extra` holds every other field."""

    ts: str
    level: str
    logger: str
    msg: str
    trace_id: str | None
    span_id: str | None
    visibility: str | None
    extra: dict[str, Any]


def _log_visibility(record: logging.LogRecord) -> str | None:
    """The record's visibility from its step's `module_id` (spec §9). Match-level privacy is set by the run in
    Task 10, as for the span exporter; a record with no module is run-level (`None`)."""
    module_id = getattr(record, "module_id", None)
    return row_visibility(module_id if isinstance(module_id, str) else None, None)


class _LogCollector(logging.Handler):
    """A log handler that collects a masked copy of every record into the run's log object (spec §9). The stdout
    masker is applied to **every** field, not only the message, so a secret in an extra field or a traceback never
    reaches the object or the viewer (Reviewer B OBS-B02)."""

    def __init__(self, storage: _Store, run_id: str) -> None:
        super().__init__()
        self._storage = storage
        self._run_id = run_id
        self._records: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = record_to_payload(record)
            payload["visibility"] = _log_visibility(record)
            masker = active_masker()
            if masker is not None:
                payload = {
                    key: (masker.mask_text(value) if isinstance(value, str) else value)
                    for key, value in payload.items()
                }
            self._records.append(payload)
            data = json.dumps(self._records, default=str).encode("utf-8")
            self._storage.put("lake", log_object_key(self._run_id), data)
        except Exception:  # noqa: BLE001 - a logging handler must never raise from emit (JsonLineHandler's contract)
            self.handleError(record)


def collect_logs(storage: _Store, run_id: str) -> logging.Handler:
    """A logging handler that persists a masked, trace-correlated copy of the run's logs to its log object. The run
    adds it to the `lakehouse` logger (plan Task 10); `read_logs` reads it back."""
    return _LogCollector(storage, run_id)


def read_logs(
    storage: _Store, run_id: str, limit: int, level: str | None = None, text: str | None = None
) -> list[LogRecord]:
    """The run's most recent `limit` collected records, newest first, filtered by `level` (exact) and `text`
    (substring over the message and logger name). A missing log object returns no records (spec §9)."""
    try:
        raw = storage.get("lake", log_object_key(run_id))
    except (KeyError, OSError):
        return []
    records = cast("list[dict[str, Any]]", json.loads(raw))
    needle = text.lower() if text is not None else None
    out: list[LogRecord] = []
    for record in reversed(records):
        if level is not None and record.get("level") != level:
            continue
        if needle is not None and needle not in f"{record.get('msg', '')} {record.get('logger', '')}".lower():
            continue
        out.append(_to_log_record(record))
        if len(out) >= limit:
            break
    return out


def _to_log_record(record: Mapping[str, Any]) -> LogRecord:
    return LogRecord(
        ts=str(record.get("ts", "")),
        level=str(record.get("level", "")),
        logger=str(record.get("logger", "")),
        msg=str(record.get("msg", "")),
        trace_id=record.get("trace_id"),
        span_id=record.get("span_id"),
        visibility=record.get("visibility"),
        extra={key: value for key, value in record.items() if key not in _LOG_KNOWN_FIELDS},
    )
