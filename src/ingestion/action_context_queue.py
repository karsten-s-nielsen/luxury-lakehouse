"""Spark/Delta/dbutils adapters for the AC-1 worker-drain fan-out (ADR-037).

Implements the pure ports from ``analytics.action_context.drain``. The pure core
stays Spark-free; everything that touches Spark/Delta lives here.

IMPORTANT: pyspark is a Databricks-runtime-only dependency (not installed locally /
in CI). So this module must stay IMPORTABLE OFFLINE — pyspark imports are
TYPE_CHECKING-only or function-local, and the queue schema is a plain pyspark-free
column list with a lazily-built ``StructType``. Only the methods that actually run on
Databricks (``enqueue`` / ``ensure_table`` / ``units_for_worker`` / ``process``)
touch Spark, via the injected session.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from analytics.action_context.drain import GameTimeoutError, WorkAssignment
from analytics.action_context.work_unit import WorkUnit

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _DeleteResult(Protocol):
    """Structural type for a Databricks DELETE result (a Spark ``DataFrame`` in prod, a fake in tests).

    Only ``first()`` is consumed — it yields the single ``num_affected_rows`` row, or ``None``.
    A ``Protocol`` (not the nominal ``pyspark.sql.DataFrame``) so the offline test fakes type-check too.
    """

    def first(self) -> Any: ...


_QUEUE_TABLE = "action_context_work_queue"
_QUEUE_SCHEMA = "observability"

# Retention for the self-prune at preflight start. Queue rows are ephemeral per-run scratch
# (one batch per daily run, keyed by run_id); a run drains within hours, so anything older than
# a week is dead. 7 days mirrors the time-based retention shape used by dbt fct_workflow_costs.
_QUEUE_RETENTION_DAYS = 7

# SINGLE SOURCE OF TRUTH for the queue columns (P5), as (name, simpleString type,
# nullable) — pyspark-free so the parity test + DDL string work OFFLINE. Excludes
# _ingested_at, which write_delta_table auto-adds. ``_queue_struct()`` builds the
# StructType lazily at runtime (Databricks only).
_QUEUE_COLUMNS: list[tuple[str, str, bool]] = [
    ("run_id", "string", False),
    ("worker_id", "int", False),
    ("seq", "bigint", False),
    ("provider", "string", False),
    ("match_id", "string", False),
    ("period", "int", True),
    ("frame_range_lo", "bigint", True),
    ("frame_range_hi", "bigint", True),
    ("est_cost", "double", False),
    # Ghost-GK backend policy carried per-unit (ADR-035 amendment): resolved at preflight, stamped on
    # each unit, read by the drain worker. Nullable for back-compat with rows enqueued before the column
    # existed (``_row_to_work_unit`` reads NULL → "fft-cic").
    ("kde_backend", "string", True),
]


def queue_columns_sql() -> str:
    """CREATE TABLE column list derived from ``_QUEUE_COLUMNS`` (P5), + ``_ingested_at``."""
    cols = [f"{name} {sql_type}" for name, sql_type, _ in _QUEUE_COLUMNS]
    cols.append("_ingested_at timestamp")
    return ", ".join(cols)


def _prune_sql(table: str, retention_days: int = _QUEUE_RETENTION_DAYS) -> str:
    """Build the age-based DELETE for stale queue rows (pure — no Spark).

    ``retention_days`` is coerced via ``int()`` (defence-in-depth against any non-int payload
    reaching the SQL string) and must be positive. ``_ingested_at`` is the audit column
    ``write_delta_table`` stamps on every enqueue.
    """
    days = int(retention_days)
    if days <= 0:
        raise ValueError(f"retention_days must be positive, got {retention_days!r}")
    # S608 suppressed: no user input reaches the string — ``days`` is int()-coerced above and
    # ``table`` is the internal catalog-derived FQN (same trusted pattern as ``ensure_table``'s
    # CREATE TABLE in this module). Parameterised binding is not available for DELETE DDL here.
    return f"DELETE FROM {table} WHERE _ingested_at < CURRENT_TIMESTAMP - INTERVAL {days} DAYS"  # noqa: S608


def _affected_rows(result: _DeleteResult) -> int:
    """Extract ``num_affected_rows`` from a Databricks DELETE result; 0 if unavailable."""
    row = result.first()
    if row is None:
        return 0
    try:
        return int(row["num_affected_rows"])
    except (KeyError, ValueError, TypeError):
        return 0


def _queue_struct():
    from pyspark.sql import types as T  # noqa: N812

    mapping = {"string": T.StringType, "int": T.IntegerType, "bigint": T.LongType, "double": T.DoubleType}
    return T.StructType(
        [T.StructField(name, mapping[sql_type](), nullable) for name, sql_type, nullable in _QUEUE_COLUMNS]
    )


def _row_to_work_unit(row) -> WorkUnit:
    """Reconstruct a ``WorkUnit`` from a queue row (Spark ``Row`` or mapping).

    Pure (no Spark) so the round-trip contract is unit-testable. A NULL/absent ``kde_backend`` (rows
    enqueued before the column existed) reads back as the ``"fft-cic"`` default.
    """
    lo = row["frame_range_lo"]
    frame_range = (lo, row["frame_range_hi"]) if lo is not None else None
    kde_backend = row["kde_backend"] if row["kde_backend"] else "fft-cic"
    return WorkUnit(
        provider=row["provider"],
        match_id=row["match_id"],
        period=row["period"],
        frame_range=frame_range,
        kde_backend=kde_backend,
    )


class DeltaWorkQueue:
    """Durable work-queue over ``{catalog}.observability.action_context_work_queue``."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        self._spark = spark
        self._catalog = catalog  # P7: store directly, don't re-split the FQN
        self._table = f"{catalog}.{_QUEUE_SCHEMA}.{_QUEUE_TABLE}"

    def ensure_table(self) -> None:
        # P4: create the schema too, so a fresh catalog (test tmp_catalog) works;
        # idempotent + cheap. In dev/prod observability already exists (watermarks).
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{_QUEUE_SCHEMA}")
        self._spark.sql(f"CREATE TABLE IF NOT EXISTS {self._table} ({queue_columns_sql()}) USING DELTA")

    def prune(self, retention_days: int = _QUEUE_RETENTION_DAYS) -> int:
        """Delete queue rows older than ``retention_days`` (by ``_ingested_at``); return rows deleted.

        Sweeps stale per-run scratch rows so the queue does not grow unbounded across daily runs.
        Idempotent and safe to call before every enqueue. Runs as the preflight task's SP, which holds
        MODIFY on ``observability``. The Spark-touching seam; the SQL builder (``_prune_sql``) and the
        affected-row extraction (``_affected_rows``) are pure + unit-tested offline.
        """
        return _affected_rows(self._spark.sql(_prune_sql(self._table, retention_days)))

    def enqueue(self, run_id: str, assignments: list[WorkAssignment]) -> None:
        from ingestion.utils import write_delta_table

        rows = []
        for a in assignments:
            lo, hi = a.unit.frame_range or (None, None)
            rows.append(
                (
                    run_id,
                    a.worker_id,
                    a.seq,
                    a.unit.provider,
                    a.unit.match_id,
                    a.unit.period,
                    lo,
                    hi,
                    a.est_cost,
                    a.unit.kde_backend,
                )
            )
        sdf = self._spark.createDataFrame(rows, schema=_queue_struct())
        write_delta_table(
            sdf,
            self._catalog,  # P7
            _QUEUE_SCHEMA,
            _QUEUE_TABLE,
            replace_where=f"run_id = '{run_id}'",
            row_count=len(rows),
        )

    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
        from pyspark.sql import functions as F  # noqa: N812

        df = (
            self._spark.table(self._table)
            .where((F.col("run_id") == run_id) & (F.col("worker_id") == worker_id))
            .orderBy("seq")
        )
        return [_row_to_work_unit(r) for r in df.collect()]


class SparkInterruptWatchdog:
    """Per-game watchdog: runs fn on a worker thread; interruptTag on timeout.

    Thread-locality invariant (B2): addTag is thread-local to the ops issued on that
    thread, so it is called INSIDE the worker thread that runs fn; interruptTag is
    cross-thread by tag string. Do NOT refactor fn onto a shared pool without
    preserving addTag-on-the-fn-thread.

    Tracking (Spark job) -> interruptTag cancels -> thread returns (no leak).
    Event-only (driver pandas) -> not cancellable -> thread abandoned (bounded by
    drain_worker's _MAX_ABANDONED_THREADS); ``live_abandoned_count`` counts the alive ones.

    Spark-free to import (the session is injected) -> the threading/re-raise logic is
    unit-testable offline with a stub session.
    """

    def __init__(self, spark: SparkSession, interrupt_grace_s: float = 5.0) -> None:
        self._spark = spark
        self._grace = interrupt_grace_s  # post-interrupt join: interrupted threads return here
        self._abandoned: list[threading.Thread] = []  # P3: track refs, count LIVE ones
        self._last_interrupted_ops: list[str] = []  # P6: interruptTag's return (proof of cancel)

    @property
    def live_abandoned_count(self) -> int:
        # P3: prune threads that have since finished (freed memory) and count only
        # those still alive -> a CONCURRENT memory-pressure bound, not a lifetime tally.
        self._abandoned = [t for t in self._abandoned if t.is_alive()]
        return len(self._abandoned)

    def run(self, fn: Callable[[], int], label: str, timeout_s: float) -> int:
        tag = f"ac1-{label}"
        box: dict[str, object] = {}

        def _target() -> None:
            try:
                self._spark.addTag(tag)  # thread-local: must be on this thread (B2)
                box["value"] = fn()
            except Exception as exc:  # noqa: BLE001 -- captured to replay on controller thread (N2/P8)
                box["error"] = exc

        t = threading.Thread(target=_target, name=f"ac1-watchdog-{label}", daemon=True)
        t.start()
        t.join(timeout_s)
        if t.is_alive():
            try:
                # interruptTag returns the ids of operations it cancelled (P6: proof of cancel)
                self._last_interrupted_ops = list(self._spark.interruptTag(tag) or [])
            finally:
                t.join(self._grace)  # interruptible (tracking) threads return here; non-interruptible stay alive
            if t.is_alive():
                self._abandoned.append(t)  # event-only / driver hang; bounded by drain loop (P3)
            raise GameTimeoutError(label)
        if "error" in box:
            raise box["error"]  # type: ignore[misc]  # re-raise fn's exception (type+traceback, N2)
        return int(box["value"])  # type: ignore[arg-type]


class SparkGameProcessor:
    """Dispatches a WorkUnit to the existing _process_* functions (unchanged).

    Loads the xT grid ONCE per worker (at construction), not per unit.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        from ingestion.action_context import _load_xt_grid_from_delta

        self._spark = spark
        self._catalog = catalog
        self._schema = schema
        self._logger = logging.getLogger("action_context_drain")
        self._xt_grid, self._xt_l, self._xt_w = _load_xt_grid_from_delta(spark, catalog, schema, self._logger)

    def process(self, unit: WorkUnit) -> int:
        from ingestion.action_context import (
            _is_tracking_provider,
            _process_event_only_match,
            _process_statsbomb_match,
            _process_tracking_match,
        )

        if _is_tracking_provider(unit.provider):
            return _process_tracking_match(
                self._spark,
                self._catalog,
                self._schema,
                unit.provider,
                unit.match_id,
                unit.period,
                self._xt_grid,
                self._xt_l,
                self._xt_w,
                self._logger,
                kde_backend=unit.kde_backend,
            )
        if unit.provider == "statsbomb":
            return _process_statsbomb_match(
                self._spark,
                self._catalog,
                self._schema,
                unit.match_id,
                self._xt_grid,
                self._xt_l,
                self._xt_w,
                self._logger,
                kde_backend=unit.kde_backend,
            )
        if unit.provider == "wyscout":
            return _process_event_only_match(
                self._spark, self._catalog, self._schema, "wyscout", unit.match_id, self._logger
            )
        raise ValueError(f"unknown provider: {unit.provider}")
