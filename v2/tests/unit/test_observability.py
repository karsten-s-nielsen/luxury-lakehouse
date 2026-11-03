"""The run-written observability object: row schema, visibility rule, writer/reader, and the OTel exporter seam
(spec §7.2; plan Task 2)."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

from lakehouse.observability import (
    InMemorySink,
    ObservabilityWriter,
    TaskRow,
    read_task_rows,
    row_visibility,
    write_task_rows,
)
from lakehouse.telemetry import install_providers, step_span
from lakehouse.telemetry_export import LakeSpanExporter

_LOG = logging.getLogger("lakehouse.test")


class _FakeStorage:
    """A dict-backed Storage good enough for `write_task_rows`/`read_task_rows` (put/read/exists)."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put(self, root: str, key: str, data: bytes | Path) -> None:
        self.objects[(root, key)] = data if isinstance(data, bytes) else Path(data).read_bytes()

    def get(self, root: str, key: str) -> bytes:
        return self.objects[(root, key)]

    def exists(self, root: str, key: str) -> bool:
        return (root, key) in self.objects


def test_row_visibility() -> None:
    assert row_visibility("idsse_pining", None) == "private"  # pining module-level is always private (§7.2, OBS-B22)
    assert row_visibility("skillcorner_pining", False) == "private"
    assert row_visibility("statsbomb_public", None) == "public"
    assert row_visibility("statsbomb_public", True) == "private"  # any private match in the step
    assert row_visibility(None, None) is None  # run-level step, never anon-visible per §6.4


def test_task_rows_attributes() -> None:
    sink = InMemorySink()
    install_providers("export", exporter=LakeSpanExporter(sink))
    try:
        with step_span(
            "lakehouse.run.write",
            {"lakehouse.module_id": "statsbomb_public", "lakehouse.layer": "bronze"},
            log=_LOG,
        ) as step:
            step.set("lakehouse.rows", 42)
        write = next(r for r in sink.rows if r.task == "lakehouse.run.write")
        assert (write.layer, write.state, write.row_count, write.visibility) == ("bronze", "ok", 42, "public")
    finally:
        install_providers("off")


def test_pining_module_step_is_private() -> None:
    sink = InMemorySink()
    install_providers("export", exporter=LakeSpanExporter(sink))
    try:
        with step_span(
            "lakehouse.run.list", {"lakehouse.module_id": "idsse_pining", "lakehouse.layer": "bronze"}, log=_LOG
        ):
            pass
        row = next(r for r in sink.rows if r.task == "lakehouse.run.list")
        assert row.visibility == "private"
    finally:
        install_providers("off")


def test_no_sdk_writes_no_rows() -> None:
    sink = InMemorySink()
    install_providers("off")
    with step_span("lakehouse.run.write", {"lakehouse.layer": "bronze"}, log=_LOG):
        pass
    assert sink.rows == []


def test_write_read_round_trip() -> None:
    storage = _FakeStorage()
    rows = [
        TaskRow(
            run_id="r1",
            task="lakehouse.run.write",
            parent_task="lakehouse.run",
            layer="bronze",
            started_at="2026-10-28T00:00:00+00:00",
            duration_seconds=1.5,
            row_count=10,
            state="ok",
            visibility="public",
            execution_context="local",
            flavour=None,
            cost_usd=0.0,
            cost_error=None,
        )
    ]
    write_task_rows(storage, "r1", rows)
    assert read_task_rows(storage, "r1") == rows


def test_observability_writer_run_only() -> None:
    """The web app has no import path to the observability WRITE/export path (OBS-B32): `telemetry_export` (the
    `LakeSpanExporter`) and `cli.observe_run` (the writer wiring) never reach it. The public page legitimately READS
    `lakehouse.observability` (spend/dependency), so that read module is allowed; only the export path is forbidden.
    Run in a fresh interpreter so an earlier test's import does not pollute sys.modules."""
    code = (
        "import lakehouse.web.main, sys; "
        "bad = sorted(m for m in sys.modules if m in {'lakehouse.telemetry_export'}); "
        "assert not bad, bad"
    )
    result = subprocess.run(  # noqa: S603 - trusted: our own constant code string and sys.executable
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_in_memory_and_writer_are_sinks() -> None:
    assert isinstance(InMemorySink(), object)
    assert hasattr(ObservabilityWriter, "export")
