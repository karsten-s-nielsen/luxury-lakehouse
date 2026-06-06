"""Checkpoint-mismatch classifier: SQLSTATE-gated, error-field scoped, fail-safe (spec M2/M6/P9)."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.synced_table_heal import _CHECKPOINT_MISMATCH_SQLSTATE, is_checkpoint_mismatch_failure

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "synced_table" / "pipeline_events_checkpoint_mismatch.json"


class _FakePort:
    def __init__(self, events: list) -> None:
        self._events = events

    def latest_failed_events(self, pipeline_id: str) -> list:
        return self._events

    def get_synced_table_status(self, fqn: str) -> str:
        return "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"

    def get_pipeline_id(self, fqn: str) -> str:
        return "pid"


def test_sqlstate_constant() -> None:
    assert _CHECKPOINT_MISMATCH_SQLSTATE == "XXKST"


def test_matches_on_sqlstate_in_exception_message() -> None:
    port = _FakePort([{"error": {"exceptions": [{"message": "boom ... SQLSTATE: XXKST"}]}}])
    assert is_checkpoint_mismatch_failure(port, "pid") is True


def test_no_match_on_other_failure() -> None:
    port = _FakePort([{"error": {"exceptions": [{"message": "[OUT_OF_MEMORY] executor died"}]}}])
    assert is_checkpoint_mismatch_failure(port, "pid") is False


def test_failsafe_on_query_error() -> None:
    class _Boom:
        def latest_failed_events(self, pipeline_id: str) -> list:
            raise RuntimeError("api down")

        def get_synced_table_status(self, fqn: str) -> str:
            return "UNKNOWN"

        def get_pipeline_id(self, fqn: str) -> str:
            return "pid"

    assert is_checkpoint_mismatch_failure(_Boom(), "pid") is False


def test_field_scoped_not_whole_blob() -> None:
    # P9: the marker appearing in an UNRELATED field must NOT match — only the error/exception text.
    port = _FakePort([{"details": "history mentions XXKST elsewhere", "error": {"exceptions": [{"message": "ok"}]}}])
    assert is_checkpoint_mismatch_failure(port, "pid") is False


def test_matches_real_event_fixture() -> None:
    events = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    assert is_checkpoint_mismatch_failure(_FakePort(events), "pid") is True
