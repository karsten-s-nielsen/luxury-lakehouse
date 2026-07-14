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
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from analytics.action_context.drain import SB360_WORKER_ID, GameTimeoutError, WorkAssignment
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


# ─────────────────────────── D9: the unit-event log ───────────────────────────
#
# TOPOLOGY — PER-WORKER TABLES (decided by the Task-2 spike, 2026-07-13).
# One Delta table per WRITER (``_w0`` .. ``_w7`` + ``_sb360``), plus a UNION ALL VIEW named
# ``action_context_unit_events`` — which is the ONLY name the gate ever reads.
#
# Why: D9 makes ~390 one-row commits from 8 concurrent drivers. Measured on the real job, a
# single shared table cost **p50 9.7 s per append** at 8-way concurrency vs **1.66 s uncontended**
# (0 failures either way — ADR-038's jittered backoff absorbs the contention into LATENCY). The
# pre-registered threshold (750 ms) was breached 13x, so we take ADR-038's own elimination route
# (b) — "split into multiple tables" — the only option that makes ``_delta_log`` contention
# STRUCTURALLY IMPOSSIBLE rather than merely retried.
#
# ``PARTITIONED BY (event_date)`` is for RETENTION (a partition drop, not a tombstone-generating
# DELETE) and read-pruning ONLY. Partitioning is NOT a contention control (ADR-038: ``_delta_log``
# serialization is inherent to a single TABLE) — splitting tables is what fixed it.
_EVENT_TABLE = "action_context_unit_events"  # the VIEW; per-worker tables suffix it
_EVENT_SCHEMA = "observability"

# One event table per for_each drain worker. Pinned to ``_ActionContextGuard._N_DRAIN_WORKERS``
# (a module-level import would be circular — ``ingestion.action_context`` imports this module's
# adapters); parity is guarded by test_unit_event_sink.py::test_event_worker_count_matches_the_drain_fan_out.
_N_EVENT_WORKERS = 8

# Sentinel unit identity for a ``slice_completed`` event: it belongs to a WORKER, not a unit, but
# ``provider``/``match_id`` are NOT NULL. The gate filters on ``state``, never on these.
_SLICE_SENTINEL = "__slice__"

# SINGLE SOURCE OF TRUTH for the unit-event columns, same convention as ``_QUEUE_COLUMNS``:
# (name, simpleString type, nullable) — pyspark-free so the parity test + DDL string work OFFLINE.
# Excludes ``_ingested_at``, which write_delta_table auto-adds.
_EVENT_COLUMNS: list[tuple[str, str, bool]] = [
    ("run_id", "string", False),
    ("worker_id", "int", False),
    ("provider", "string", False),
    ("match_id", "string", False),
    ("period", "int", True),  # NULL for sb360 (match-grain; it exits the per-period drain)
    ("state", "string", False),  # running | succeeded | failed | timed_out | slice_completed
    ("started_at", "timestamp", True),
    ("ended_at", "timestamp", True),
    ("rows_written", "bigint", True),
    ("error", "string", True),
    # ``slice_completed`` ONLY: the count of unit-event rows lost to the fail-open writes. It is the
    # SOLE channel by which telemetry loss reaches the gate, which reads persisted tables only.
    ("write_failures", "int", True),
    ("event_date", "date", False),  # M4: partition key — MUST be a real column
]


def event_columns_sql() -> str:
    """CREATE TABLE column list derived from ``_EVENT_COLUMNS``, + ``_ingested_at``."""
    cols = [f"{name} {sql_type}" for name, sql_type, _ in _EVENT_COLUMNS]
    cols.append("_ingested_at timestamp")
    return ", ".join(cols)


def event_table_for_worker(worker_id: int) -> str:
    """The per-writer event table for ``worker_id`` (or the sb360 sentinel). Never the view."""
    if worker_id == SB360_WORKER_ID:
        return f"{_EVENT_TABLE}_sb360"
    if not 0 <= worker_id < _N_EVENT_WORKERS:
        raise ValueError(
            f"worker_id {worker_id!r} has no event table: expected 0..{_N_EVENT_WORKERS - 1} "
            f"or the sb360 sentinel ({SB360_WORKER_ID})"
        )
    return f"{_EVENT_TABLE}_w{worker_id}"


def event_table_names() -> list[str]:
    """Every physical event table, in the order the UNION ALL view stacks them."""
    return [event_table_for_worker(w) for w in range(_N_EVENT_WORKERS)] + [event_table_for_worker(SB360_WORKER_ID)]


def event_view_sql(catalog: str) -> str:
    """The UNION ALL view the gate reads — same columns as a single table would have."""
    # S608 suppressed: no user input reaches the string — the table names are module constants and
    # ``catalog`` is the internal catalog FQN (same trusted pattern as ``ensure_table``'s CREATE TABLE).
    parts = [f"SELECT * FROM {catalog}.{_EVENT_SCHEMA}.{t}" for t in event_table_names()]  # noqa: S608
    return f"CREATE OR REPLACE VIEW {catalog}.{_EVENT_SCHEMA}.{_EVENT_TABLE} AS " + " UNION ALL ".join(parts)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _event_row(
    *,
    run_id: str,
    worker_id: int,
    provider: str | None,
    match_id: str | None,
    period: int | None,
    state: str,
    started_at: datetime | None,
    ended_at: datetime | None,
    rows_written: int | None,
    error: str | None,
    write_failures: int | None,
) -> dict[str, Any]:
    """Pure row builder — so the NOT-NULL contract is testable without Spark (V5).

    ``event_date`` is NOT NULL and ``write_delta_table`` auto-adds ONLY ``_ingested_at``: nothing
    else would populate it, and the first production write would fail on the NOT-NULL partition
    column. It is derived from the event's own timestamp (``ended_at`` for a terminal, ``started_at``
    for a ``running``, wall-clock for a unit-less ``slice_completed``).

    ``provider``/``match_id`` are NOT NULL but a ``slice_completed`` has no unit -> ``_SLICE_SENTINEL``.
    Key order mirrors ``_EVENT_COLUMNS`` (rows are written positionally against it).
    """
    stamp = ended_at or started_at or _utcnow()
    return {
        "run_id": run_id,
        "worker_id": worker_id,
        "provider": provider or _SLICE_SENTINEL,
        "match_id": match_id or _SLICE_SENTINEL,
        "period": period,
        "state": state,
        "started_at": started_at,
        "ended_at": ended_at,
        "rows_written": rows_written,
        "error": error,
        "write_failures": write_failures,
        "event_date": stamp.date(),
    }


def _event_struct():
    from pyspark.sql import types as T  # noqa: N812

    mapping = {
        "string": T.StringType,
        "int": T.IntegerType,
        "bigint": T.LongType,
        "timestamp": T.TimestampType,
        "date": T.DateType,
    }
    return T.StructType(
        [T.StructField(name, mapping[sql_type](), nullable) for name, sql_type, nullable in _EVENT_COLUMNS]
    )


class DeltaUnitEventSink:
    """Per-unit lifecycle events over ``{catalog}.observability.action_context_unit_events_*``.

    Implements ``analytics.action_context.drain.UnitEventSink``. One instance per WRITER (a drain
    worker, or sb360) — every write goes to that writer's own table, so the ``_delta_log``
    contention that made a shared table cost 9.7 s/append is structurally impossible.

    Failure policies (see the port's docstring — they are load-bearing):
    ``unit_started`` / ``flush_terminals`` are FAIL-OPEN and count lost rows into
    ``write_failures``; ``slice_completed`` is FAIL-LOUD and carries that count to the gate.
    """

    def __init__(self, spark: SparkSession, catalog: str, logger: logging.Logger | None = None) -> None:
        self._spark = spark
        self._catalog = catalog
        self._logger = logger or logging.getLogger("action_context_events")
        self._terminals: list[dict[str, Any]] = []
        self._write_failures = 0

    @property
    def write_failures(self) -> int:
        """Unit-event ROWS lost to the fail-open writes. Reaches the gate via ``slice_completed``."""
        return self._write_failures

    def ensure_tables(self) -> None:
        """Create the schema, every per-worker table, and the UNION ALL view. Idempotent.

        **PREFLIGHT ONLY — it is the single owner of creation.** The view is created with
        ``CREATE OR REPLACE VIEW``, which is NOT idempotent under concurrency: two tasks issuing it
        against the same view at the same time is a metastore race that can throw. And
        ``compute_action_context_statsbomb`` does NOT depend on ``preflight_action_context``
        (main.tf), so the two tasks OVERLAP — while both run at ``max_retries = 0``. sb360 therefore
        calls ``ensure_own_table`` instead; the 8-way drain calls neither.
        """
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{_EVENT_SCHEMA}")
        for table in event_table_names():
            self._ensure_one_table(table)
        self._spark.sql(event_view_sql(self._catalog))

    def ensure_own_table(self, worker_id: int) -> None:
        """Create ONLY this writer's own event table. Idempotent, and **it never touches the view.**

        The narrow half of ``ensure_tables``, for a writer that must be able to write before (or
        concurrently with) preflight: sb360 does not depend on preflight, so it cannot inherit
        preflight's creation, yet its ``slice_completed`` is FAIL-LOUD and would die on a missing
        table on the very first run. ``CREATE TABLE IF NOT EXISTS`` on the writer's OWN table races
        with nothing; ``CREATE OR REPLACE VIEW`` on a view a second task is concurrently replacing
        does — which is why that statement stays in preflight's hands alone.
        """
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{_EVENT_SCHEMA}")
        self._ensure_one_table(event_table_for_worker(worker_id))

    def _ensure_one_table(self, table: str) -> None:
        self._spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self._catalog}.{_EVENT_SCHEMA}.{table} "
            f"({event_columns_sql()}) USING DELTA PARTITIONED BY (event_date)"
        )

    def unit_started(self, run_id: str, worker_id: int, unit: WorkUnit) -> None:
        """FAIL-OPEN. Written BEFORE processing — it is the OOM-visibility guarantee, so it can
        never be batched: an OOM-killed driver flushes no buffer."""
        row = _event_row(
            run_id=run_id,
            worker_id=worker_id,
            provider=unit.provider,
            match_id=unit.match_id,
            period=unit.period,
            state="running",
            started_at=_utcnow(),
            ended_at=None,
            rows_written=None,
            error=None,
            write_failures=None,
        )
        self._write_fail_open([row], worker_id, "unit_started")

    def units_started(self, run_id: str, worker_id: int, units: Sequence[WorkUnit]) -> None:
        """BATCHED ``running`` — ONE commit for a whole slice. FAIL-OPEN. **sb360 only.**

        Deliberately NOT on the ``UnitEventSink`` port: that port is the DRAIN's contract, and the
        drain must never acquire this shape. The drain writes ``running`` PER UNIT because it
        processes units SERIALLY — an OOM at unit k must leave units 0..k-1 terminal and unit k
        ``running``, and a buffered batch would be lost with the driver. sb360 is ONE distributed
        cogroup job (ADR-058) that either runs or dies as a whole, so a single pre-job batch carries
        exactly the same OOM information at 1 commit instead of N (up to 323 on a backlog run).
        """
        if not units:
            return
        rows = [
            _event_row(
                run_id=run_id,
                worker_id=worker_id,
                provider=u.provider,
                match_id=u.match_id,
                period=u.period,
                state="running",
                started_at=_utcnow(),
                ended_at=None,
                rows_written=None,
                error=None,
                write_failures=None,
            )
            for u in units
        ]
        self._write_fail_open(rows, worker_id, "units_started")

    def unit_finished(
        self,
        run_id: str,
        worker_id: int,
        unit: WorkUnit,
        *,
        state: str,
        rows_written: int | None,
        error: str | None,
    ) -> None:
        """Buffered — terminal state is RECONSTRUCTIBLE from results, which is the licence to batch.
        Written by ``flush_terminals``."""
        self._terminals.append(
            _event_row(
                run_id=run_id,
                worker_id=worker_id,
                provider=unit.provider,
                match_id=unit.match_id,
                period=unit.period,
                state=state,
                started_at=None,
                ended_at=_utcnow(),
                rows_written=rows_written,
                error=error,
                write_failures=None,
            )
        )

    def flush_terminals(self) -> None:
        """FAIL-OPEN (ADR-002: telemetry loss must never become data loss). Were this fail-loud, the
        gate's ``UNVERIFIABLE`` verdict — whose whole purpose is lost unit events — would be
        unreachable, because a lossy worker would have died instead."""
        if not self._terminals:
            return
        pending, self._terminals = self._terminals, []
        for worker_id in sorted({int(r["worker_id"]) for r in pending}):
            rows = [r for r in pending if int(r["worker_id"]) == worker_id]
            self._write_fail_open(rows, worker_id, "flush_terminals")

    def slice_completed(self, run_id: str, worker_id: int) -> None:
        """FAIL-LOUD. The ONLY channel by which ``write_failures`` reaches the gate (a different
        task, reading persisted tables only). If it cannot land, the gate's evidence is unusable —
        so the worker task must fail rather than let the gate reason on a half-truth.

        Emitted by IDLE workers too (P4): a silent idle worker is indistinguishable from a DEAD one.
        """
        row = _event_row(
            run_id=run_id,
            worker_id=worker_id,
            provider=None,
            match_id=None,
            period=None,
            state="slice_completed",
            started_at=None,
            ended_at=_utcnow(),
            rows_written=None,
            error=None,
            write_failures=self._write_failures,
        )
        self._write([row], worker_id)

    def _write_fail_open(self, rows: list[dict[str, Any]], worker_id: int, what: str) -> None:
        try:
            self._write(rows, worker_id)
        except Exception as exc:
            # FAIL-OPEN by design: telemetry loss must never become data loss (ADR-002). The loss is
            # NOT silent — ERROR-level with a traceback (never warning: those are invisible in
            # error-log queries) AND counted into write_failures, which rides to the gate on
            # slice_completed and taints this worker's verdict (UNVERIFIABLE, not COMPLETE).
            # BLE001 does not fire here: ruff exempts a broad catch logged with exc_info=True.
            self._write_failures += len(rows)
            self._logger.error(
                "ac1_unit_event_write_failed op=%s worker_id=%d rows_lost=%d total_write_failures=%d err=%s",
                what,
                worker_id,
                len(rows),
                self._write_failures,
                exc,
                exc_info=True,
            )

    def _write(self, rows: list[dict[str, Any]], worker_id: int) -> None:
        from ingestion.utils import write_delta_table

        payload = [tuple(row[name] for name, _t, _n in _EVENT_COLUMNS) for row in rows]
        sdf = self._spark.createDataFrame(payload, schema=_event_struct())
        # mode="append" is MANDATORY (§0d): write_delta_table DEFAULTS to mode="overwrite", so the
        # natural call would WIPE this append-only log on every event — leaving one row and a gate
        # that accuses a healthy drain on every run. Measured in the spike: 392 default-mode
        # "appends" left ONE row. Guarded by test_sink_writes_are_APPEND_not_the_overwrite_DEFAULT.
        write_delta_table(
            sdf,
            self._catalog,
            _EVENT_SCHEMA,
            event_table_for_worker(worker_id),
            mode="append",
            row_count=len(rows),
        )


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

    def count_for_run(self, run_id: str) -> int:
        """Queue rows PERSISTED for ``run_id`` — the read half of preflight's enqueue round-trip.

        Spec §11: the D8 gate compares queue rows against unit events, so a run where the planner
        discovered N and ``enqueue`` persisted M < N is self-consistently SHORT — invisible to every
        downstream check. Preflight is the only place that holds both numbers, so it is the only
        place that can assert them. One bounded aggregate; no rows reach the driver.
        """
        from pyspark.sql import functions as F  # noqa: N812

        return int(self._spark.table(self._table).where(F.col("run_id") == run_id).count())

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
        # statsbomb (ADR-058) EXITS the drain — processed as a single distributed cogroup job by
        # main_statsbomb, never enqueued here. A stray statsbomb unit (or wyscout / any non-AC
        # provider, ADR-057) fails loud rather than silently reverting to the slow per-match path.
        raise ValueError(
            f"provider {unit.provider!r} is not drain-processed "
            "(statsbomb runs via main_statsbomb / _process_statsbomb_matches; wyscout is out of scope)"
        )
