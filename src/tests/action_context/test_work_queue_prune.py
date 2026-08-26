"""Offline-pure tests for DeltaWorkQueue.prune (action_context_work_queue self-prune).

The queue accumulates per-run scratch rows (one batch per daily run, keyed by run_id);
nothing ever deleted them, so the table grew unbounded. ``prune`` sweeps rows older than
the retention window at preflight start (runs as the SP that owns observability). These
tests exercise the SQL builder + the affected-row extraction OFFLINE (no Spark); the live
round trip is covered by the serverless integration test in test_drain_adapters.py.
"""

from __future__ import annotations

import pytest

from ingestion.drain_adapters import (
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


# ---------------------------------------------------------------------------
# D8 (Task 8, Step 4): prune must never delete the run the GATE is about to audit.
#
# `verify_action_context_drain` reads the work queue for THIS run's `run_id` and derives its
# expected-WORKER set from it. If prune could remove the current run's rows, the gate would see an
# empty queue, expect no drain workers at all, and return COMPLETE on a run it never examined --
# a silently muted gate, which is the exact failure class this whole design exists to prevent.
#
# Two independent properties make that impossible, and both are asserted below rather than assumed:
#   (a) prune's predicate is purely AGE-based on `_ingested_at` -- it cannot target a `run_id`; and
#   (b) prune runs at PREFLIGHT, BEFORE this run's enqueue -- so the current run's rows do not yet
#       exist when it fires, and by the time the gate runs they are seconds-to-hours old against a
#       7-day window (the drain's own budget is 8 h).
# ---------------------------------------------------------------------------


def test_prune_predicate_cannot_target_a_run_id() -> None:
    """(a) Age-based only. A run_id-scoped DELETE is the one shape that could strand the gate."""
    sql = _prune_sql("cat.observability.action_context_work_queue", _QUEUE_RETENTION_DAYS)
    assert "run_id" not in sql, f"prune must never predicate on run_id -- it would strand the D8 gate: {sql}"
    assert "_ingested_at" in sql


def test_prune_runs_BEFORE_the_enqueue_it_must_not_delete() -> None:  # noqa: N802
    """(b) ORDERING, from the source: in ``main_preflight``, ``prune()`` precedes ``enqueue(...)``.

    Read off the AST rather than trusted from a comment: if a future refactor moved the prune AFTER
    the enqueue, the age predicate would still not match this run's brand-new rows -- but the two
    guarantees would no longer be independent, and only this assertion would notice.
    """
    import ast
    import inspect
    import textwrap

    from ingestion.action_context import main_preflight

    tree = ast.parse(textwrap.dedent(inspect.getsource(main_preflight)))
    # Sorted by LINE, not by ast.walk order (which is breadth-first and would compare a
    # top-level call against a nested one on the wrong axis).
    found = sorted(
        (node.lineno, node.func.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in ("prune", "enqueue")
    )
    calls = [attr for _lineno, attr in found]
    assert "prune" in calls, "main_preflight no longer prunes the work queue"
    assert "enqueue" in calls, "main_preflight no longer enqueues -- the parser has drifted"
    assert calls.index("prune") < calls.index("enqueue"), (
        "main_preflight must prune BEFORE enqueueing this run's batch. Pruning after the enqueue "
        "puts the run the D8 gate audits inside the prune's blast radius."
    )


def test_retention_window_dwarfs_the_gates_read_horizon() -> None:
    """The gate reads the queue within the same job run (drain budget: 8 h). A retention window
    shorter than a day would put that read inside the prune's reach on a long backlog drain."""
    assert _QUEUE_RETENTION_DAYS >= 1


def test_affected_rows_returns_zero_when_column_absent() -> None:
    assert _affected_rows(_FakeResult({})) == 0
