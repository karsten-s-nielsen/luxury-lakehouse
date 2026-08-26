"""Impure Spark entry points for the ``tracking_marts`` worker-drain (ADR-037 fan-out reuse).

The three driver-sequential tracking-grain writers (``off_ball_runs_writer`` /
``defensive_credit_writer`` / ``gkdv_writer``) are replaced by ONE consolidated drain that reuses the
proven ``analytics.action_context.drain`` fan-out core through the ``drain_name``-generalized adapters
(``ingestion.drain_adapters``). This module holds the three impure entry points:

1. ``main_tracking_marts_preflight`` — discover OPEN units (events-based, cross-run skip-guard),
   ``assign_workers`` LPT-bin-pack across ``_N_TRACKING_MARTS_WORKERS``, fill the work-queue, emit the
   worker-id list + run_id as task values for the ``for_each_task``.
2. ``main_tracking_marts_drain_worker`` — drain this worker's slice via ``TrackingMartsProcessor`` and
   the pure ``drain_worker``; ``raise_on_failed_units`` at the end (ADR-067).
3. ``main_gkdv_pool`` — the single-driver ``pool_keepers`` reduce over ``bronze.gkdv_observations``
   (gkdv pooling is cross-game, so it is a separate task after the drain, not a per-unit step).

**Mirror of ``ingestion.action_context``'s ``main_preflight`` / ``main_drain_worker``.** The differences
are exactly: ``drain_name="tracking_marts"`` + ``include_sb360=False`` on the adapters (there is no sb360
task in this drain — G1); the skip-guard is a CROSS-RUN ``succeeded``-events read (§3.1); the processor is
``TrackingMartsProcessor`` (not ``SparkGameProcessor``). The pure cores (``assign_workers`` /
``drain_worker`` / ``WATCHDOG_BUDGET_S``) are reused UNCHANGED.

IMPORTANT: pyspark is a Databricks-runtime-only dependency (not installed locally / in CI). So this
module must stay IMPORTABLE OFFLINE — pyspark imports are TYPE_CHECKING-only or function-local, and the
Spark adapters (``DeltaWorkQueue`` / ``DeltaUnitEventSink`` / ``TrackingMartsProcessor``) are imported
inside the functions that actually run on Databricks. Tests patch them at their source module.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from analytics.action_context.drain import WATCHDOG_BUDGET_S, assign_workers, drain_worker
from ingestion.action_context import _resolve_run_id, _set_task_value, raise_on_failed_units
from ingestion.drain_adapters import _EVENT_SCHEMA
from ingestion.tracking_marts_driver import discover_tracking_units
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from pyspark.sql import SparkSession

    from analytics.action_context.work_unit import WorkUnit

logger = logging.getLogger(__name__)

# Pinned == the TF ``for_each_task`` concurrency == the event-worker count (``_N_EVENT_WORKERS`` in
# ``drain_adapters``); parity is enforced by the Task-10 conformance test. Mirrors
# ``_ActionContextGuard._N_DRAIN_WORKERS = 8``.
_N_TRACKING_MARTS_WORKERS = 8

_DRAIN_NAME = "tracking_marts"
#: The UNION ALL event view the cross-run skip-guard reads (built by ``DeltaUnitEventSink.ensure_tables``
#: with ``include_sb360=False``). Lives in the ``observability`` schema (``_EVENT_SCHEMA``).
_EVENT_VIEW = f"{_DRAIN_NAME}_unit_events"

_SUCCEEDED = "succeeded"


# ── skip-guard: the pure core + its evidence reader (G2 — CROSS-RUN, succeeded-only) ──


def succeeded_keys_from_events(
    events: Iterable[tuple[str, str, int | None, str]],
) -> frozenset[tuple[str, str, int | None]]:
    """The done-set: DISTINCT ``(provider, match_id, period)`` with a ``succeeded`` terminal.

    ``events`` rows are ``(provider, match_id, period, state)`` — deliberately WITHOUT ``run_id``. That
    absence IS the cross-run property (§3.1): a unit that ``succeeded`` under ANY run is done. A per-run
    read would leave every unit always-open and the chronic timeout would go unfixed silently. Only
    ``succeeded`` counts — a ``failed`` / ``timed_out`` (or never-run) unit stays OPEN and is re-enumerated.
    """
    return frozenset(
        (str(provider), str(match_id), None if period is None else int(period))
        for provider, match_id, period, state in events
        if state == _SUCCEEDED
    )


def open_units(
    universe: Sequence[WorkUnit],
    done_keys: frozenset[tuple[str, str, int | None]],
    *,
    full: bool,
) -> list[WorkUnit]:
    """``universe`` MINUS ``done_keys`` (pure). ``full=True`` bypasses the subtraction, returns the whole set."""
    if full:
        return list(universe)
    return [u for u in universe if (u.provider, str(u.match_id), u.period) not in done_keys]


def _succeeded_unit_keys(spark: SparkSession, catalog: str) -> frozenset[tuple[str, str, int | None]]:
    """Read the cross-run ``succeeded`` done-set from the unit-event view (no ``run_id`` filter — §3.1)."""
    from pyspark.sql import functions as F  # noqa: N812

    rows = (
        spark.table(f"{catalog}.{_EVENT_SCHEMA}.{_EVENT_VIEW}")
        .where(F.col("state") == _SUCCEEDED)
        .select(
            F.col("provider").cast("string"),
            F.col("match_id").cast("string"),
            F.col("period").cast("bigint"),
        )
        .distinct()
        .collect()
    )
    return succeeded_keys_from_events(
        (str(r[0]), str(r[1]), None if r[2] is None else int(r[2]), _SUCCEEDED) for r in rows
    )


def discover_open_units(spark: SparkSession, catalog: str, *, full: bool) -> list[WorkUnit]:
    """OPEN tracking-marts units = every discovered ``(provider, match_id, period)`` MINUS the cross-run
    ``succeeded`` done-set (§3.1). ``full=True`` returns the whole universe (skips the events read).

    N3 (durable operator foot-gun): because "done" is a cross-run ``succeeded`` unit-event — NOT an
    output-``left_anti`` — TRUNCATING any of the four output bronze tables leaves its units marked done,
    so daily incremental runs will SKIP them and the table stays empty until a ``--full`` run. Any op that
    clears an output table MUST be followed by ``preflight_tracking_marts --full`` (see the ADR).
    """
    from analytics.action_context.work_unit import WorkUnit

    triples = discover_tracking_units(spark, catalog)
    universe = [WorkUnit(provider=p, match_id=m, period=per) for p, m, per in triples]
    done_keys: frozenset[tuple[str, str, int | None]] = frozenset() if full else _succeeded_unit_keys(spark, catalog)
    return open_units(universe, done_keys, full=full)


def _parse_full_flag(raw: object) -> bool:
    """The ``--full`` job parameter arrives as a string (empty when unset). Truthy on ``1/true/yes/full``."""
    return str(raw or "").strip().lower() in {"1", "true", "yes", "full"}


# ── entry points ──────────────────────────────────────────────────────


def main_tracking_marts_preflight() -> None:
    """Preflight (ADR-037): discover OPEN units, LPT-bin-pack across ``_N_TRACKING_MARTS_WORKERS``, fill
    the work-queue, and emit the constant worker-id list + run_id as task values for the ``for_each``.

    Mirrors ``ingestion.action_context.main_preflight`` — with ``drain_name="tracking_marts"`` +
    ``include_sb360=False`` (no sb360 task in this drain) and the cross-run events-based skip-guard.
    """
    args = parse_ingestion_args(
        "Preflight: discover open tracking-marts units and fill the work-queue",
        extra_args=[
            (
                "--run-id",
                {
                    "type": str,
                    "default": None,
                    "help": "Job-level run id (from {{job.run_id}}); shared with the drain workers.",
                },
            ),
            (
                "--full",
                {
                    "type": str,
                    "default": None,
                    "help": "Force a FULL re-enumeration (ignore the succeeded skip-guard). Required after "
                    "truncating/dropping any of the four output bronze tables (see ADR N3). Empty/0 = "
                    "incremental (default).",
                },
            ),
        ],
    )
    task_logger = configure_logging("tracking_marts_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    # Spark adapter imported function-locally: it pulls pyspark, and this module must stay importable
    # offline. Tests patch it at its source (ingestion.drain_adapters.*).
    from ingestion.drain_adapters import DeltaUnitEventSink, DeltaWorkQueue

    # D9: preflight is the SINGLE OWNER of creation — all 8 per-worker tables AND the UNION view — and
    # this runs BEFORE the nothing-to-do early-return below (the gate reads the view even on a quiet run).
    # include_sb360=False: this drain has NO sb360 task, so no phantom _sb360 table/view-arm (G1).
    DeltaUnitEventSink(spark, args.catalog, task_logger, drain_name=_DRAIN_NAME, include_sb360=False).ensure_tables()

    full = _parse_full_flag(getattr(args, "full", None))
    units = discover_open_units(spark, args.catalog, full=full)
    if not units:
        task_logger.info("Tracking-marts preflight: nothing to do")
        _set_task_value("tracking_marts_run_id", "", task_logger)
        _set_task_value("tracking_marts_worker_ids", [], task_logger)
        return

    assignments = assign_workers(units, _N_TRACKING_MARTS_WORKERS)
    run_id = _resolve_run_id(args)
    queue = DeltaWorkQueue(spark, args.catalog, drain_name=_DRAIN_NAME)
    queue.ensure_table()
    # Self-prune stale per-run scratch rows before enqueueing this run's batch (retention).
    pruned = queue.prune()
    if pruned:
        task_logger.info("Tracking-marts preflight: pruned %d stale work-queue rows (retention)", pruned)
    queue.enqueue(run_id, assignments)
    # THE ENQUEUE ROUND-TRIP (spec §11, mirrors AC): the gate audits the drain by comparing queue rows
    # against unit events, so a silently SHORT queue (planner discovered N, write persisted M < N) reads
    # as self-consistently COMPLETE. Preflight is the only place that holds both numbers. Fail-loud.
    persisted = queue.count_for_run(run_id)
    if persisted != len(assignments):
        raise RuntimeError(
            f"Tracking-marts preflight enqueue round-trip FAILED (run_id={run_id}): "
            f"assigned {len(assignments)} units but the work-queue persisted {persisted}. "
            "The drain -- and the gate that audits it -- would run on a silently SHORT queue."
        )

    worker_ids = [str(i) for i in range(_N_TRACKING_MARTS_WORKERS)]
    _set_task_value("tracking_marts_run_id", run_id, task_logger)
    _set_task_value("tracking_marts_worker_ids", worker_ids, task_logger)
    task_logger.info(
        "Tracking-marts preflight: %d units across %d workers (run_id=%s, full=%s)",
        len(units),
        len(worker_ids),
        run_id,
        full,
    )


def _parse_budget(raw: object) -> int:
    """Resolve the per-game watchdog budget (empty -> ``WATCHDOG_BUDGET_S`` = 2700). Fail-loud on garbage."""
    val = str(raw or "").strip()
    if not val:
        return WATCHDOG_BUDGET_S
    try:
        budget_s = int(val)
    except ValueError as exc:
        raise SystemExit(f"--watchdog-budget-s must be an integer, got {val!r}") from exc
    if budget_s <= 0:
        raise SystemExit(f"--watchdog-budget-s must be > 0, got {budget_s}")
    return budget_s


def _run_worker(
    spark: SparkSession,
    catalog: str,
    schema: str,
    worker_id: int,
    run_id: str,
    budget_s: int,
    task_logger: logging.Logger,
) -> None:
    """Drain one worker's slice (the testable core of ``main_tracking_marts_drain_worker``).

    NO ``ensure_tables()`` here — preflight (a single writer) owns creation before its own early-return
    (8 concurrent drivers on CREATE-IF-NOT-EXISTS + a view is the metastore contention the per-worker
    topology exists to avoid). An idle worker still emits ``slice_completed`` (P4) so the gate can tell an
    idle worker from a dead one. ``raise_on_failed_units`` fails the TASK if any unit failed (ADR-067).
    """
    from ingestion.drain_adapters import (
        DeltaUnitEventSink,
        DeltaWorkQueue,
        SparkInterruptWatchdog,
    )
    from ingestion.tracking_marts_processor import TrackingMartsProcessor

    queue = DeltaWorkQueue(spark, catalog, drain_name=_DRAIN_NAME)
    sink = DeltaUnitEventSink(spark, catalog, task_logger, drain_name=_DRAIN_NAME, include_sb360=False)
    units = queue.units_for_worker(run_id, worker_id)
    if not units:
        task_logger.info("Tracking-marts drain worker %d: no units for run %s -- exiting", worker_id, run_id)
        # P4: an IDLE worker must still SAY IT RAN, or the gate reads it as DEAD and cries wolf on a
        # tiny daily run.
        sink.slice_completed(run_id, worker_id)
        return

    processor = TrackingMartsProcessor(spark, catalog, schema)
    watchdog = SparkInterruptWatchdog(spark)
    summary = drain_worker(
        queue,
        processor,
        watchdog,
        run_id,
        worker_id,
        task_logger,
        sink=sink,
        units=units,
        budget_s=budget_s,
    )
    task_logger.info(
        "Tracking-marts drain worker %d complete: processed=%d failed=%d timed_out=%d rows=%d",
        worker_id,
        summary.processed,
        summary.failed,
        summary.timed_out,
        summary.total_rows,
    )
    raise_on_failed_units(summary, run_id=run_id)


def main_tracking_marts_drain_worker() -> None:
    """for-each worker (ADR-037): drain this worker's slice of the tracking-marts queue.

    Receives ``--worker-id "{{input}}"`` and ``--run-id`` from the preflight task value. The per-game
    watchdog (2700 s default, overridable via ``--watchdog-budget-s``) bounds each unit; the task
    timeout (8 h) bounds the whole drain. Mirrors ``ingestion.action_context.main_drain_worker``.
    """
    args = parse_ingestion_args(
        "Drain a worker's tracking-marts queue slice",
        extra_args=[
            ("--worker-id", {"type": str, "default": None, "help": "for-each worker index"}),
            ("--run-id", {"type": str, "default": None, "help": "preflight run id (task value)"}),
            (
                "--watchdog-budget-s",
                {
                    "type": str,
                    "default": None,
                    "help": "Per-game watchdog budget seconds (default/empty -> WATCHDOG_BUDGET_S=2700).",
                },
            ),
        ],
    )
    task_logger = configure_logging("tracking_marts_drain")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    raw_wid = getattr(args, "worker_id", None)
    run_id = getattr(args, "run_id", None)
    if raw_wid is None or not str(raw_wid).strip():
        raise SystemExit("--worker-id is required")
    if not run_id or not str(run_id).strip():
        task_logger.info("Empty run_id (preflight found nothing) -- tracking-marts drain worker exits cleanly")
        return
    worker_id = int(str(raw_wid).strip())
    run_id = str(run_id).strip()
    budget_s = _parse_budget(getattr(args, "watchdog_budget_s", None))

    _run_worker(spark, args.catalog, args.schema, worker_id, run_id, budget_s, task_logger)


# ── gkdv pooling reduce (single-driver — pooling is cross-game, not per-unit) ──


def _pool_gkdv(spark: SparkSession, catalog: str) -> int:
    """Pool ALL ``bronze.gkdv_observations`` per keeper x (comp, season) -> ``gkdv_keeper_pooled``.

    The whole-corpus ``pool_keepers`` reduce that the per-unit drain deliberately split OUT (pooling
    needs every game a keeper played, so it cannot run per-unit). Reads the full intermediate to the
    driver, pools, and writes per-provider ``replaceWhere`` — mirroring ``gkdv_writer.run_pipeline``'s
    end-stage write loop. Returns the total pooled keeper rows written.
    """
    from ingestion.gkdv_writer import (
        _MIN_GAMES,
        _MIN_NONZERO,
        _TRACKING_PROVIDERS,
        _pooled_struct_type,
        pool_keepers,
    )
    from ingestion.gkdv_writer import (
        BRONZE_TABLE as POOLED_TABLE,
    )
    from ingestion.tracking_marts_processor import GKDV_OBS_TABLE
    from ingestion.utils import write_delta_table
    from shared.constants import DEFAULT_BRONZE_SCHEMA

    # Bounded keeper-frame observation table (NOT tracking-scale) — see _topandas_exemptions.yml.
    observations = spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{GKDV_OBS_TABLE}").toPandas()
    pooled = pool_keepers(observations, min_nonzero=_MIN_NONZERO, min_games=_MIN_GAMES, want_threat=True)
    schema_out = _pooled_struct_type()

    total = 0
    for provider in _TRACKING_PROVIDERS:
        slice_pdf = pooled[pooled["data_source"] == provider]
        sdf = spark.createDataFrame(slice_pdf, schema=schema_out)
        total += write_delta_table(
            sdf,
            catalog,
            DEFAULT_BRONZE_SCHEMA,
            POOLED_TABLE,
            replace_where=f"data_source = '{provider}'",
            logger=logger,
        )
    logger.info("gkdv_pool: wrote %d pooled keeper rows across %d providers", total, len(_TRACKING_PROVIDERS))
    return total


def main_gkdv_pool() -> None:
    """Entry point ``compute_gkdv_pool``: the single-driver gkdv pooling reduce (runs after the drain)."""
    args = parse_ingestion_args("Pool gkdv keeper observations -> bronze.gkdv_keeper_pooled")
    task_logger = configure_logging("gkdv_pool")

    # gkdv is gated off pending its perf project (ADR-082 amendment): the drain writes no observations, so
    # the reduce has nothing to pool. Short-circuit BEFORE touching Spark — the task stays in the job so the
    # perf project re-enables the whole path by flipping one constant. Single source of truth: GKDV_ENABLED.
    from ingestion.tracking_marts_processor import GKDV_ENABLED

    if not GKDV_ENABLED:
        task_logger.info("gkdv_pool: gkdv scoring is gated off (GKDV_ENABLED=False) -- skipping pool, no rows written")
        return

    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    total = _pool_gkdv(spark, args.catalog)
    task_logger.info("gkdv_pool complete -- %d pooled keeper rows written", total)
