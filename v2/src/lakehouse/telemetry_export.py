"""The OpenTelemetry `SpanExporter` that turns finished run-step spans into `TaskRow`s and hands them to a `SpanSink`
(spec §7.2; plan Task 2). Installed by `telemetry.install_providers("export", exporter=LakeSpanExporter(sink))`."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from lakehouse.observability import SpanSink, TaskRow, row_visibility, run_cost

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import ReadableSpan

__all__ = ["LakeSpanExporter"]


def _iso(ns: int | None) -> str:
    return datetime.fromtimestamp((ns or 0) / 1e9, UTC).isoformat()


class LakeSpanExporter(SpanExporter):
    """Convert each finished span to a `TaskRow` and push the batch to `sink`. Keeps a span-id→name map so a child
    span's `parent_task` resolves to the parent span's name when the parent has already exported; a parent that
    exports later is reconciled by the dependency view (plan Task 4)."""

    def __init__(self, sink: SpanSink) -> None:
        self._sink = sink
        self._names: dict[int, str] = {}

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        rows: list[TaskRow] = []
        for span in spans:
            context = span.get_span_context()
            if context is None:
                continue
            self._names[context.span_id] = span.name
            parent = span.parent
            parent_id = parent.span_id if parent is not None else None
            attrs = dict(span.attributes or {})
            module_id = attrs.get("lakehouse.module_id")
            duration_seconds = round(((span.end_time or 0) - (span.start_time or 0)) / 1e9, 6)
            execution_context = _str_or_none(attrs.get("lakehouse.execution_context"))
            flavour = _str_or_none(attrs.get("lakehouse.flavour"))
            # cost is a run-level figure: only the span that carries the execution context (the run span) is priced,
            # so it is attributed once per run, never summed over the nested step spans (spec §7.3).
            cost_usd: float | None = None
            cost_error: str | None = None
            if execution_context is not None:
                cost_usd, cost_error = run_cost(execution_context, flavour, duration_seconds / 3600)
            explicit = _str_or_none(attrs.get("lakehouse.visibility"))  # the run sets it per match; else derive it
            rows.append(
                TaskRow(
                    run_id=str(attrs.get("lakehouse.run_id", "")),
                    task=span.name,
                    parent_task=self._names.get(parent_id) if parent_id is not None else None,
                    layer=_str_or_none(attrs.get("lakehouse.layer")),
                    started_at=_iso(span.start_time),
                    duration_seconds=duration_seconds,
                    row_count=_int_or_none(attrs.get("lakehouse.rows")),
                    state=str(attrs.get("lakehouse.state", "ok")),
                    visibility=explicit if explicit is not None else row_visibility(_str_or_none(module_id), None),
                    execution_context=execution_context,
                    flavour=flavour,
                    cost_usd=cost_usd,
                    cost_error=cost_error,
                )
            )
        self._sink.export(rows)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def _str_or_none(value: object) -> str | None:
    return str(value) if value is not None else None


def _int_or_none(value: object) -> int | None:
    return int(value) if isinstance(value, int) else None
