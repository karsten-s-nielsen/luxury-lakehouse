"""Offline-pure tests for DeltaWorkQueue.prune (action_context_work_queue self-prune).

The queue accumulates per-run scratch rows (one batch per daily run, keyed by run_id);
nothing ever deleted them, so the table grew unbounded. ``prune`` sweeps rows older than
the retention window at preflight start (runs as the SP that owns observability). These
tests exercise the SQL builder + the affected-row extraction OFFLINE (no Spark); the live
round trip is covered by the serverless integration test in test_action_context_queue.py.
"""

from __future__ import annotations

import pytest

from ingestion.action_context_queue import (
    _QUEUE_RETENTION_DAYS,
    DeltaWorkQueue,
    _affected_rows,
    _prune_sql,
)


def test_default_retention_is_7_days() -> None:
    assert _QUEUE_RETENTION_DAYS == 7


def test_prune_sql_shape() -> None:
    sql = _prune_sql("cat.observability.action_context_work_queue", 7)
    assert sql == (
        "DELETE FROM cat.observability.action_context_work_queue "
        "WHERE _ingested_at < CURRENT_TIMESTAMP - INTERVAL 7 DAYS"
    )


def test_prune_sql_embeds_retention_days() -> None:
    assert "INTERVAL 30 DAYS" in _prune_sql("t", 30)


@pytest.mark.parametrize("bad", [0, -1, -7])
def test_prune_sql_rejects_non_positive(bad: int) -> None:
    with pytest.raises(ValueError, match="retention_days must be positive"):
        _prune_sql("t", bad)


def test_prune_sql_coerces_to_int_blocking_injection() -> None:
    # int() coercion makes a non-int payload raise rather than reach the SQL string.
    with pytest.raises((ValueError, TypeError)):
        _prune_sql("t", "7; DROP TABLE x")  # type: ignore[arg-type]


class _FakeResult:
    def __init__(self, row: object) -> None:
        self._row = row

    def first(self) -> object:
        return self._row


class _FakeSpark:
    """Records the SQL it is handed and returns a canned DELETE result row."""

    def __init__(self, affected: object) -> None:
        self.queries: list[str] = []
        self._affected = affected

    def sql(self, query: str) -> _FakeResult:
        self.queries.append(query)
        if self._affected is None:
            return _FakeResult(None)
        return _FakeResult({"num_affected_rows": self._affected})


def test_prune_issues_delete_and_returns_count() -> None:
    spark = _FakeSpark(affected=42)
    q = DeltaWorkQueue(spark, catalog="cat")  # type: ignore[arg-type]
    deleted = q.prune(retention_days=7)
    assert deleted == 42
    assert spark.queries == [
        "DELETE FROM cat.observability.action_context_work_queue "
        "WHERE _ingested_at < CURRENT_TIMESTAMP - INTERVAL 7 DAYS"
    ]


def test_prune_uses_default_retention_when_unspecified() -> None:
    spark = _FakeSpark(affected=0)
    q = DeltaWorkQueue(spark, catalog="cat")  # type: ignore[arg-type]
    q.prune()
    assert f"INTERVAL {_QUEUE_RETENTION_DAYS} DAYS" in spark.queries[0]


def test_affected_rows_returns_zero_when_no_result_row() -> None:
    assert _affected_rows(_FakeResult(None)) == 0


def test_affected_rows_returns_zero_when_column_absent() -> None:
    assert _affected_rows(_FakeResult({})) == 0
