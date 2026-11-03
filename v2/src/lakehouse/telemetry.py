"""The OpenTelemetry API seam (spec §21, ADR-009).

Cycle 1 calls only the OpenTelemetry API. Without an SDK every call is a no-op. Span and attribute names are stable once
chosen: add a name here before any code uses it, and never rename one. Every step opens its span with `step_span`,
which also writes the step's JSON log line (source, row or byte count, elapsed time, status).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal, get_args

from opentelemetry import metrics, trace

if TYPE_CHECKING:
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

SpanName = Literal[
    "lakehouse.run",
    "lakehouse.run.lease",
    "lakehouse.run.config",
    "lakehouse.run.visibility_check",
    "lakehouse.run.list",
    "lakehouse.run.fetch",
    "lakehouse.run.archive",
    "lakehouse.run.parse",
    "lakehouse.run.write",
    "lakehouse.run.move",
    "lakehouse.run.move_batch",
    "lakehouse.run.commit",
    "lakehouse.run.resolve",
    "lakehouse.run.dbt",
    "lakehouse.run.merge",
    "lakehouse.run.cleanup",
    "lakehouse.restore",
    "lakehouse.relocate",
    "lakehouse.sweep",
]
AttributeName = Literal[
    "lakehouse.run_id",
    "lakehouse.trigger",
    "lakehouse.module_id",
    "lakehouse.provider",
    "lakehouse.channel",
    "lakehouse.match_key",
    "lakehouse.rows",
    "lakehouse.bytes",
    "lakehouse.catalog_seq",
    "lakehouse.lease_generation",
    "lakehouse.layer",
    "lakehouse.state",
    "lakehouse.execution_context",
    "lakehouse.flavour",
    "lakehouse.visibility",
]
AttributeValue = str | int

SPAN_NAMES: Final[frozenset[str]] = frozenset(get_args(SpanName))
ATTRIBUTE_NAMES: Final[frozenset[str]] = frozenset(get_args(AttributeName))
_PREFIX: Final = "lakehouse."

tracer: Final[trace.Tracer] = trace.get_tracer("lakehouse")
meter: Final[metrics.Meter] = metrics.get_meter("lakehouse")

__all__ = [
    "ATTRIBUTE_NAMES",
    "SPAN_NAMES",
    "AttributeName",
    "AttributeValue",
    "SpanName",
    "Step",
    "TelemetryProviders",
    "current_trace_ids",
    "install_providers",
    "meter",
    "record_skipped",
    "step_span",
    "tracer",
]


def current_trace_ids() -> tuple[str | None, str | None]:
    """The active span's `trace_id`/`span_id` as hex, or `(None, None)` when no SDK span is current (spec §7.1). With
    the API-only no-op provider the current span context is invalid, so logs carry the ids only when an SDK is on."""
    context = trace.get_current_span().get_span_context()
    if not context.is_valid:
        return (None, None)
    return (format(context.trace_id, "032x"), format(context.span_id, "016x"))


@dataclass
class TelemetryProviders:
    """The tracer and meter the run records to. `install_providers` sets them; `step_span` reads them. `off` is the
    cycle-1 no-op (the API without an SDK); `memory` keeps an in-memory span exporter for tests; `export` sends
    finished spans to the run's observability writer (plan Task 2)."""

    tracer: trace.Tracer
    meter: metrics.Meter
    span_exporter: InMemorySpanExporter | None = None


_NOOP_PROVIDERS: Final = TelemetryProviders(tracer=tracer, meter=meter)
_providers: TelemetryProviders = _NOOP_PROVIDERS


def install_providers(
    mode: Literal["off", "memory", "export"], *, exporter: object | None = None
) -> TelemetryProviders:
    """Install the telemetry providers for this process. `off` restores the cycle-1 no-op; `memory` records spans to
    an in-memory exporter (tests read `span_exporter`); `export` sends finished spans to `exporter` (a `SpanExporter`,
    the run's observability writer). It does not use the one-shot global `set_tracer_provider`, so a test can switch
    modes freely."""
    global _providers
    if mode == "off":
        _providers = _NOOP_PROVIDERS
        return _providers
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter as _InMemory

    tracer_provider = TracerProvider()
    span_exporter: InMemorySpanExporter | None = None
    if mode == "memory":
        span_exporter = _InMemory()
        tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    else:  # export
        if exporter is None:
            raise ValueError("install_providers('export') needs an exporter")
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))  # type: ignore[arg-type] - a SpanExporter
    _providers = TelemetryProviders(
        tracer=tracer_provider.get_tracer("lakehouse"),
        meter=MeterProvider().get_meter("lakehouse"),
        span_exporter=span_exporter,
    )
    return _providers


def record_skipped(
    name: SpanName, attributes: Mapping[AttributeName, AttributeValue] | None = None, *, log: logging.Logger
) -> None:
    """Record a step that did not run: a zero-duration span with `lakehouse.state="skipped"` and one JSON log line.
    A skipped step has no body, so this gives the `skipped` state a carrier (spec §7.1)."""
    if name not in SPAN_NAMES:
        raise ValueError(f"span name {name!r} is not in SPAN_NAMES")
    given = _checked((attributes or {}).items())
    with _providers.tracer.start_as_current_span(name, attributes={**given, "lakehouse.state": "skipped"}):
        pass
    source = given.get("lakehouse.module_id", given.get("lakehouse.provider", "lakehouse"))
    fields = {key.removeprefix(_PREFIX): value for key, value in given.items()}
    log.info(name, extra={"source": source, **fields, "state": "skipped", "elapsed_ms": 0, "status": "skipped"})


def _checked(items: Iterable[tuple[str, AttributeValue]]) -> dict[str, AttributeValue]:
    attributes = dict(items)
    unknown = sorted(set(attributes) - ATTRIBUTE_NAMES)
    if unknown:
        raise ValueError(f"attribute names {unknown} are not in ATTRIBUTE_NAMES")
    return attributes


class Step:
    """What a step learns while it runs (a row count, a catalog sequence); `step_span` sets it on the span and logs
    it."""

    def __init__(self) -> None:
        self.attributes: dict[str, AttributeValue] = {}

    def set(self, name: AttributeName, value: AttributeValue) -> None:
        self.attributes.update(_checked([(name, value)]))


@contextmanager
def step_span(
    name: SpanName, attributes: Mapping[AttributeName, AttributeValue] | None = None, *, log: logging.Logger
) -> Generator[Step, None, None]:
    """One step: an OpenTelemetry span with `attributes`, and one JSON log line when the step ends.

    The log line has `msg` (the span name), `source` (the module id, else the provider, else `lakehouse`), each
    attribute under its name without the `lakehouse.` prefix (`rows`, `bytes`, `catalog_seq`, ...), `elapsed_ms` and
    `status` (`ok`, or `error` when the step raised; the exception propagates).
    """
    if name not in SPAN_NAMES:
        raise ValueError(f"span name {name!r} is not in SPAN_NAMES")
    given = _checked((attributes or {}).items())
    step = Step()
    started = time.perf_counter()
    status = "error"
    try:
        with _providers.tracer.start_as_current_span(name, attributes=given) as span:
            try:
                yield step
                span.set_attributes(step.attributes)
                span.set_attribute("lakehouse.state", "ok")
                status = "ok"
            except BaseException:
                span.set_attribute("lakehouse.state", "failed")
                raise
    finally:
        found = {**given, **step.attributes}
        source = found.get("lakehouse.module_id", found.get("lakehouse.provider", "lakehouse"))
        fields = {key.removeprefix(_PREFIX): value for key, value in found.items()}
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        log.info(name, extra={"source": source, **fields, "elapsed_ms": elapsed_ms, "status": status})
