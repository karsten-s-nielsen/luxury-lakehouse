"""The tracking-marts drain-completeness gate: the IMPURE entry point (``verify_tracking_marts_drain``).

The fan-in task (``run_if = ALL_DONE``) that runs after the 8-way ``compute_tracking_marts`` drain and
asserts the drain actually FINISHED its work (ADR-068). Mirrors ``ingestion.action_context_gate.main``.

**This module holds NO rules.** Every rule lives in the pure ``analytics.action_context.drain_gate`` —
reused UNCHANGED except for its Task-1B ``extra_expected_workers`` parameter. All this module does is:
resolve ``run_id``, adapt the persisted tables into the pure core's inputs, re-run the planner for the
diagnostic, log the report, and turn ``must_raise`` into an exit code.

TWO tracking-marts-specific deltas from the AC gate:

* **G1 — ``extra_expected_workers=frozenset()``.** This drain has NO sb360 task, so the sb360 sentinel
  worker (``-1``) must NOT be expected — otherwise its (never-emitted) ``slice_completed`` reads as a dead
  worker and the gate reports ``DRAIN_FAILED`` on EVERY run, muting the real verdict. The AC gate passes
  the default (the sb360 sentinel); this one passes the empty set.
* **N1 — the results cross-check sums rows across ALL FOUR output tables per unit**
  (``off_ball_runs`` + ``action_defensive_credit`` + ``defensive_credit_attributions`` +
  ``gkdv_observations``). Omitting the AGG table would misclassify a dead worker's agg-only unit as
  ``in_flight`` instead of ``completed_terminal_lost`` in the V6 reconstruction.

``run_if = ALL_DONE`` (spec §6): the task runs even when the drain FAILED — so only ``INCOMPLETE`` (and a
planner alarm) fail the task; ``DRAIN_FAILED`` / ``UNVERIFIABLE`` are REPORTS (the job already failed, and
the gate's job there is to SAY WHAT DIED, not mask the drain's real exception with its own).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from analytics.action_context.drain_gate import (
    PlannerInputs,
    QueueRow,
    UnitEvent,
    UnitKey,
    Verdict,
    enforce,
    evaluate,
    expected_units,
)
from ingestion.defensive_credit_writer import AGG_TABLE, LONG_TABLE
from ingestion.drain_adapters import _EVENT_SCHEMA, _QUEUE_SCHEMA
from ingestion.off_ball_runs_writer import BRONZE_TABLE as OFF_BALL_TABLE
from ingestion.tracking_marts_drain import discover_open_units
from ingestion.tracking_marts_processor import GKDV_OBS_TABLE
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from shared.constants import DEFAULT_BRONZE_SCHEMA

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_DRAIN_NAME = "tracking_marts"
_QUEUE_TABLE = f"{_DRAIN_NAME}_work_queue"
_EVENT_VIEW = f"{_DRAIN_NAME}_unit_events"

#: N1 — the four bronze outputs a tracking-marts unit writes. A unit's "rows landed" count for the V6
#: dead-worker reconstruction is the SUM across all four (they share the (data_source, match_id,
#: period_id) grain). Omitting any one misclassifies a partial-write dead unit.
_OUTPUT_TABLES: tuple[str, ...] = (OFF_BALL_TABLE, AGG_TABLE, LONG_TABLE, GKDV_OBS_TABLE)


def _read_queue(spark: SparkSession, catalog: str, run_id: str) -> list[QueueRow]:
    """The drain's contract for this run. Bounded: one batch per run."""
    from pyspark.sql import functions as F  # noqa: N812

    df = (
        spark.table(f"{catalog}.{_QUEUE_SCHEMA}.{_QUEUE_TABLE}")
        .where(F.col("run_id") == run_id)
        .select("worker_id", "provider", "match_id", "period")
    )
    return [
        QueueRow(
            worker_id=int(row["worker_id"]),
            provider=str(row["provider"]),
            match_id=str(row["match_id"]),
            period=None if row["period"] is None else int(row["period"]),
        )
        for row in df.collect()
    ]


def _read_events(spark: SparkSession, catalog: str, run_id: str) -> list[UnitEvent]:
    """The unit-event log for this run — read through the sb360-free UNION ALL view (the physical
    per-worker topology is the sink's business). RUN-SCOPED (units are re-enqueued across runs)."""
    from pyspark.sql import functions as F  # noqa: N812

    df = (
        spark.table(f"{catalog}.{_EVENT_SCHEMA}.{_EVENT_VIEW}")
        .where(F.col("run_id") == run_id)
        .select("worker_id", "provider", "match_id", "period", "state", "rows_written", "error", "write_failures")
    )
    return [
        UnitEvent(
            worker_id=int(row["worker_id"]),
            provider=str(row["provider"]),
            match_id=str(row["match_id"]),
            period=None if row["period"] is None else int(row["period"]),
            state=str(row["state"]),
            rows_written=None if row["rows_written"] is None else int(row["rows_written"]),
            error=row["error"],
            write_failures=None if row["write_failures"] is None else int(row["write_failures"]),
        )
        for row in df.collect()
    ]


def _read_result_counts(
    spark: SparkSession, catalog: str, keys: list[UnitKey]
) -> dict[tuple[str, str, int | None], int]:
    """What LANDED, per ``(provider, match_id, period_id)`` — SUMMED across all four output tables (N1).

    SCOPED to this run's match ids. One bounded aggregate per (provider, table); at most
    ``len(keys) x len(_OUTPUT_TABLES)`` rows reach the driver. All four outputs share the
    ``(data_source, match_id, period_id)`` grain, so a unit's landed-rows total is their sum.
    """
    from pyspark.sql import functions as F  # noqa: N812

    matches: dict[str, set[str]] = defaultdict(set)
    for key in keys:
        matches[key.provider].add(key.match_id)

    counts: dict[tuple[str, str, int | None], int] = {}
    for provider, match_ids in matches.items():
        sorted_ids = sorted(match_ids)
        for table in _OUTPUT_TABLES:
            rows = (
                spark.table(f"{catalog}.{DEFAULT_BRONZE_SCHEMA}.{table}")
                .filter((F.col("data_source") == provider) & F.col("match_id").cast("string").isin(sorted_ids))
                .groupBy(
                    F.col("match_id").cast("string").alias("_mid"),
                    F.col("period_id").cast("bigint").alias("_period"),
                )
                .count()
                .collect()
            )
            for row in rows:
                period = None if row["_period"] is None else int(row["_period"])
                key = (provider, str(row["_mid"]), period)
                counts[key] = counts.get(key, 0) + int(row["count"])
    return counts


def _remaining_units(spark: SparkSession, catalog: str) -> frozenset[UnitKey]:
    """Re-run THE PLANNER (``discover_open_units``, not a re-implementation) and return what it still
    enumerates. After a healthy drain every unit has a ``succeeded`` terminal, so the cross-run skip-guard
    subtracts them and ``remaining`` is empty. A unit that claimed ``succeeded`` yet still shows up here is
    the V7 alarm (its rows did not land)."""
    units = discover_open_units(spark, catalog, full=False)
    return frozenset(UnitKey(provider=u.provider, match_id=str(u.match_id), period=u.period) for u in units)


def _resolve_run_id(raw: object) -> str:
    """``{{job.run_id}}`` — required, non-empty. NEVER the preflight task value (which is ``""`` on a
    nothing-to-do run). An empty value dies LOUD naming the correct source rather than auditing a run with
    no events."""
    run_id = str(raw or "").strip()
    if not run_id:
        raise SystemExit(
            "--run-id is required and must be the JOB run id. Wire it to '{{job.run_id}}' -- NOT to "
            "{{tasks.preflight_tracking_marts.values.tracking_marts_run_id}}, which is EMPTY on a "
            "nothing-to-do run (the gate would then audit run '' and find no events)."
        )
    return run_id


def _log_report(report_text: str, verdict: Verdict, task_logger: logging.Logger) -> None:
    """COMPLETE at INFO; every other verdict at ERROR (never WARNING — invisible in error-log queries,
    ADR-002). A DRAIN_FAILED / UNVERIFIABLE gate does not fail the task, so the log line is its only signal."""
    if verdict is Verdict.COMPLETE:
        task_logger.info("%s", report_text)
    else:
        task_logger.error("%s", report_text)


def main() -> None:
    """Entry point ``verify_tracking_marts_drain``."""
    args = parse_ingestion_args(
        "Verify the tracking-marts drain finished its work for this run",
        extra_args=[
            (
                "--run-id",
                {
                    "type": str,
                    "default": None,
                    "help": "The JOB run id -- wire this to '{{job.run_id}}'. NOT the preflight task value "
                    "(that is empty on a nothing-to-do run).",
                },
            ),
        ],
    )
    task_logger = configure_logging("tracking_marts_gate")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    run_id = _resolve_run_id(getattr(args, "run_id", None))
    catalog = args.catalog

    queue = _read_queue(spark, catalog, run_id)
    events = _read_events(spark, catalog, run_id)
    # The mart read is scoped to the SAME unit set the rules evaluate -- via the pure helper, not a second
    # definition here (two definitions of "expected" would let a unit be judged against an unfetched count).
    keys = list(expected_units(queue=queue, events=events))
    result_counts = _read_result_counts(spark, catalog, keys)
    planner = PlannerInputs(enqueued=len(queue), remaining=_remaining_units(spark, catalog))

    report = evaluate(
        run_id=run_id,
        queue=queue,
        events=events,
        result_counts=result_counts,
        planner=planner,
        # G1: no sb360 task in this drain -> the sb360 sentinel worker must NOT be expected, else the gate
        # reports DRAIN_FAILED every run.
        extra_expected_workers=frozenset(),
    )
    _log_report(report.render(), report.verdict, task_logger)
    # The ONLY place the task fails: INCOMPLETE (a clean worker's unit never ran / its rows did not land)
    # and the planner's write-landed alarm raise; DRAIN_FAILED / UNVERIFIABLE have already reported at ERROR.
    enforce(report)
