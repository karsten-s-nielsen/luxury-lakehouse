"""Clean-extension invariants (spec §12, test 15; plan Task 11): the observability backend is one swappable
`SpanSink`, and the log viewer's data source is behind one `_Store` interface. So the OTel Collector drops in as a
second sink with no change to the run's instrumentation, and a public log view swaps the store with no page rewrite."""

from __future__ import annotations

import logging
from pathlib import Path

from lakehouse.observability import InMemorySink, SpanSink, TaskRow, collect_logs, log_object_key, read_logs
from lakehouse.telemetry import install_providers, step_span

_LOG = logging.getLogger("lakehouse.test.cleanext")


def _run_one_step() -> None:
    with step_span(
        "lakehouse.run.write", {"lakehouse.module_id": "statsbomb_public", "lakehouse.layer": "bronze"}, log=_LOG
    ):
        pass


def test_clean_extension_sink_swap() -> None:
    first = InMemorySink()
    install_providers("export", exporter=_exporter(first))
    try:
        _run_one_step()
    finally:
        install_providers("off")
    assert [row.task for row in first.rows] == ["lakehouse.run.write"]

    captured: list[TaskRow] = []

    class _CollectorSink:
        def export(self, rows: list[TaskRow]) -> None:
            captured.extend(rows)

    assert isinstance(_CollectorSink(), SpanSink)  # a second backend satisfies the one interface

    install_providers("export", exporter=_exporter(_CollectorSink()))
    try:
        _run_one_step()  # identical instrumentation; only the sink changed
    finally:
        install_providers("off")
    assert [row.task for row in captured] == ["lakehouse.run.write"]


def test_log_source_behind_one_store() -> None:
    class _FakeStore:
        def __init__(self) -> None:
            self.objects: dict[tuple[str, str], bytes] = {}

        def put(self, root: str, key: str, data: bytes | Path) -> None:
            self.objects[(root, key)] = data if isinstance(data, bytes) else Path(data).read_bytes()

        def get(self, root: str, key: str) -> bytes:
            return self.objects[(root, key)]

    store = _FakeStore()
    handler = collect_logs(store, "r1")
    _LOG.addHandler(handler)
    try:
        _LOG.warning("one collected line")
    finally:
        _LOG.removeHandler(handler)

    records = read_logs(store, "r1", limit=10)
    assert [record.msg for record in records] == ["one collected line"]
    assert ("lake", log_object_key("r1")) in store.objects  # the data source is the one _Store seam


def _exporter(sink: SpanSink) -> object:
    from lakehouse.telemetry_export import LakeSpanExporter

    return LakeSpanExporter(sink)
