"""The collected log copy: full-field masking, level/text filters, and trace/visibility correlation (spec §9, §7.1;
tests 12 and 13; plan Task 5)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from lakehouse.logging import install_secret_masking, remove_secret_masking
from lakehouse.observability import collect_logs, log_object_key, read_logs
from lakehouse.telemetry import install_providers, step_span

_SECRET = "s3cr3t-value-123"  # noqa: S105 - a test fixture value, not a real credential; carried by a known secret var


class _FakeStorage:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put(self, root: str, key: str, data: bytes | Path) -> None:
        self.objects[(root, key)] = data if isinstance(data, bytes) else Path(data).read_bytes()

    def get(self, root: str, key: str) -> bytes:
        return self.objects[(root, key)]

    def exists(self, root: str, key: str) -> bool:
        return (root, key) in self.objects


def test_log_viewer_filters_and_masking() -> None:
    storage = _FakeStorage()
    install_secret_masking({"HF_TOKEN": _SECRET})
    log = logging.getLogger("lakehouse.test.logcol")
    log.setLevel(logging.DEBUG)
    handler = collect_logs(storage, "r1")
    log.addHandler(handler)
    try:
        log.info("hello world", extra={"source": "statsbomb_public"})
        log.warning("danger here", extra={"source": "statsbomb_public"})
        try:
            raise ValueError(f"boom {_SECRET} inside the exception")
        except ValueError:
            log.error(f"failed with {_SECRET}", extra={"source": "statsbomb_public", "token": _SECRET}, exc_info=True)
    finally:
        log.removeHandler(handler)
        remove_secret_masking()

    stored = storage.objects[("lake", log_object_key("r1"))].decode("utf-8")
    assert _SECRET not in stored  # masked in the message, the extra field, and the traceback

    recent = read_logs(storage, "r1", limit=200)
    assert len(recent) == 3
    assert recent[0].level == "ERROR"  # newest first
    for record in recent:
        assert _SECRET not in record.msg
        assert _SECRET not in json.dumps(record.extra, default=str)

    errors = read_logs(storage, "r1", limit=200, level="ERROR")
    assert [r.level for r in errors] == ["ERROR"]

    danger = read_logs(storage, "r1", limit=200, text="danger")
    assert len(danger) == 1 and "danger" in danger[0].msg


def test_log_record_trace_and_visibility() -> None:
    storage = _FakeStorage()
    install_providers("memory")
    log = logging.getLogger("lakehouse.test.logtrace")
    log.setLevel(logging.INFO)
    handler = collect_logs(storage, "r2")
    log.addHandler(handler)
    try:
        with step_span(
            "lakehouse.run.write",
            {"lakehouse.module_id": "idsse_pining", "lakehouse.layer": "bronze"},
            log=log,
        ):
            log.info("inside span", extra={"source": "idsse_pining", "module_id": "idsse_pining"})
    finally:
        log.removeHandler(handler)
        install_providers("off")

    recent = read_logs(storage, "r2", limit=200)
    assert recent, "expected collected records"
    inside = next(r for r in recent if r.msg == "inside span")
    assert inside.trace_id is not None and inside.span_id is not None  # SDK on -> ids present
    assert inside.visibility == "private"  # pining module step
    for record in recent:
        assert record.visibility is not None  # every record under a module carries its visibility
