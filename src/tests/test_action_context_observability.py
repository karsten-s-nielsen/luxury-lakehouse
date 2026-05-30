"""Unit tests for the AC-1 for-each iteration observability helpers.

``_iteration_fingerprint`` + ``_iteration_summary`` emit structured single-line
JSON records (`AC1_FINGERPRINT` / `AC1_SUMMARY`) at the start + end of every
``compute_action_context_iteration`` task so ops can grep/aggregate them
without opening the Databricks UI per-iteration.

Critical invariants tested:
1. Same fingerprint_hash for both start + end records (joinable in log aggregation).
2. fingerprint_hash is deterministic over (provider, ids, period) — reruns with
   identical inputs share the same hash for diffing.
3. zero-row matches surface in the summary so silent-drop cases are visible
   without per-line log inspection.
"""

from __future__ import annotations

import logging
from typing import cast

import pytest

pytest.importorskip("databricks.sdk")  # action_context module-level imports it

from ingestion.action_context import _BatchHeartbeat, _iteration_fingerprint, _iteration_summary


def test_fingerprint_has_all_required_fields() -> None:
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=["J03WMX", "J03WN1"],
        period_filter=1,
        catalog="soccer_analytics",
        schema="dev_gold",
    )
    required = {
        "event",
        "fingerprint_hash",
        "provider",
        "n_match_ids",
        "match_ids_sample",
        "period_filter",
        "catalog",
        "schema",
        "silly_kicks_version",
        "wheel_version",
        "databricks_run_id",
        "databricks_task_run_id",
    }
    assert required <= set(fp.keys()), f"missing fields: {required - set(fp.keys())}"
    assert fp["event"] == "ac1_iteration_start"
    assert fp["provider"] == "idsse"
    assert fp["n_match_ids"] == 2
    assert fp["period_filter"] == 1


def test_fingerprint_is_deterministic_over_inputs() -> None:
    """Same inputs → same hash (joinable on rerun for diff)."""
    a = _iteration_fingerprint(
        provider="idsse",
        ids=["J03WMX", "J03WN1"],
        period_filter=1,
        catalog="soccer_analytics",
        schema="dev_gold",
    )
    b = _iteration_fingerprint(
        provider="idsse",
        ids=["J03WMX", "J03WN1"],
        period_filter=1,
        catalog="soccer_analytics",
        schema="dev_gold",
    )
    assert a["fingerprint_hash"] == b["fingerprint_hash"]


def test_fingerprint_hash_is_input_order_independent() -> None:
    """Match id list order should not affect the hash — same logical input."""
    a = _iteration_fingerprint(
        provider="idsse",
        ids=["A", "B"],
        period_filter=1,
        catalog="c",
        schema="s",
    )
    b = _iteration_fingerprint(
        provider="idsse",
        ids=["B", "A"],
        period_filter=1,
        catalog="c",
        schema="s",
    )
    assert a["fingerprint_hash"] == b["fingerprint_hash"]


def test_fingerprint_hash_changes_when_provider_changes() -> None:
    a = _iteration_fingerprint(
        provider="idsse",
        ids=["X"],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    b = _iteration_fingerprint(
        provider="metrica",
        ids=["X"],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    assert a["fingerprint_hash"] != b["fingerprint_hash"]


def test_fingerprint_sample_caps_at_3_ids() -> None:
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=[f"M{i}" for i in range(10)],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    assert len(cast(list[str], fp["match_ids_sample"])) == 3
    assert fp["n_match_ids"] == 10


def test_fingerprint_sample_returns_all_when_few_ids() -> None:
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=["A", "B"],
        period_filter=None,
        catalog="c",
        schema="s",
    )
    assert fp["match_ids_sample"] == ["A", "B"]


def test_summary_pairs_with_fingerprint_via_hash() -> None:
    """The summary record carries the same fingerprint_hash so log aggregation can JOIN."""
    fp = _iteration_fingerprint(
        provider="idsse",
        ids=["X"],
        period_filter=1,
        catalog="c",
        schema="s",
    )
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash=str(fp["fingerprint_hash"]),
        per_match_written={"X": 100},
        elapsed_seconds=5.0,
    )
    assert summary["fingerprint_hash"] == fp["fingerprint_hash"]
    assert summary["event"] == "ac1_iteration_end"


def test_summary_aggregates_rows_correctly() -> None:
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={"A": 100, "B": 50, "C": 0},
        elapsed_seconds=10.5,
    )
    assert summary["n_matches_processed"] == 3
    assert summary["total_rows_written"] == 150
    assert summary["elapsed_seconds"] == 10.5


def test_summary_surfaces_zero_row_matches() -> None:
    """Silent-drop cases must surface in the summary, not vanish."""
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={"A": 100, "B": 0, "C": 0, "D": 50},
        elapsed_seconds=1.0,
    )
    assert summary["n_matches_zero_rows"] == 2
    assert summary["zero_row_match_sample"] == ["B", "C"]


def test_summary_zero_row_sample_caps_at_3() -> None:
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={f"M{i}": 0 for i in range(10)},
        elapsed_seconds=1.0,
    )
    assert len(cast(list[str], summary["zero_row_match_sample"])) == 3
    assert summary["n_matches_zero_rows"] == 10


def test_summary_empty_zero_row_sample_when_all_succeed() -> None:
    summary = _iteration_summary(
        provider="idsse",
        fingerprint_hash="abc123",
        per_match_written={"A": 100, "B": 50},
        elapsed_seconds=1.0,
    )
    assert summary["n_matches_zero_rows"] == 0
    assert summary["zero_row_match_sample"] == []


# ── _BatchHeartbeat lifecycle tests (Spark-agnostic) ─────────────────────


def _capture_logger() -> tuple[logging.Logger, list[str]]:
    """Return (logger, captured_messages_list). Logger appends formatted msgs to list."""
    captured: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record.getMessage())

    log = logging.getLogger("test_batch_heartbeat")
    log.handlers = [_Capture()]
    log.setLevel(logging.INFO)
    return log, captured


def test_heartbeat_logs_at_interval_then_stops_on_exit() -> None:
    """Standard lifecycle: enter → tick(s) → exit → thread joins cleanly."""
    import time

    counter = [0]
    log, captured = _capture_logger()

    with _BatchHeartbeat(
        read_progress=lambda: counter[0],
        interval_s=0.05,
        logger=log,
        label="test_batches",
    ):
        time.sleep(0.07)  # one tick
        counter[0] = 5
        time.sleep(0.06)  # second tick
    # After __exit__, the thread should be stopped.
    assert any("HEARTBEAT test_batches=" in m for m in captured), captured
    # At least one tick saw counter == 5.
    assert any("test_batches=5" in m for m in captured), captured


def test_heartbeat_does_not_log_immediately_on_start() -> None:
    """First wait is BEFORE the first log so we don't emit '0 batches' on start."""
    log, captured = _capture_logger()
    with _BatchHeartbeat(
        read_progress=lambda: 0,
        interval_s=10.0,
        logger=log,
        label="x",
    ):
        # Immediate exit — no tick should fire because interval is 10s.
        pass
    assert all("HEARTBEAT" not in m for m in captured), captured


def test_heartbeat_thread_cleanup_on_exception_in_with_block() -> None:
    """If the with-block raises, the heartbeat thread must still stop."""
    log, _captured = _capture_logger()
    hb = _BatchHeartbeat(read_progress=lambda: 0, interval_s=0.05, logger=log)
    with pytest.raises(RuntimeError, match="boom"), hb:
        raise RuntimeError("boom")
    # Thread reference cleared after exit.
    assert hb._thread is None


def test_heartbeat_swallows_read_progress_exceptions() -> None:
    """A flaky read_progress must NOT crash the heartbeat thread or the worker."""
    import time

    call_count = [0]

    def _flaky_read() -> int:
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("simulated transient")
        return 7

    log, captured = _capture_logger()
    with _BatchHeartbeat(
        read_progress=_flaky_read,
        interval_s=0.03,
        logger=log,
        label="flaky",
    ):
        time.sleep(0.10)  # multiple ticks: first raises, subsequent succeed
    warnings = [m for m in captured if "heartbeat read_progress failed" in m]
    assert warnings, f"expected at least one warning about the flaky read; got: {captured}"
    successes = [m for m in captured if "flaky=7" in m]
    assert successes, f"expected at least one successful tick after recovery; got: {captured}"


def test_heartbeat_rejects_nonpositive_interval() -> None:
    log, _ = _capture_logger()
    with pytest.raises(ValueError, match="interval_s must be > 0"):
        _BatchHeartbeat(read_progress=lambda: 0, interval_s=0.0, logger=log)
    with pytest.raises(ValueError, match="interval_s must be > 0"):
        _BatchHeartbeat(read_progress=lambda: 0, interval_s=-1.0, logger=log)


def test_heartbeat_thread_is_daemon() -> None:
    """Daemon thread ensures process exit isn't blocked by a stuck heartbeat."""
    import time

    log, _ = _capture_logger()
    with _BatchHeartbeat(read_progress=lambda: 0, interval_s=1.0, logger=log) as hb:
        time.sleep(0.01)
        assert hb._thread is not None
        assert hb._thread.daemon, "heartbeat thread MUST be daemon to allow clean process exit"
