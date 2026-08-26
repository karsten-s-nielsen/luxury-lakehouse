"""D8 — the action-context drain-completeness gate: the IMPURE entry point.

The fan-in task (``verify_action_context_drain``) that runs after BOTH ``compute_action_context``
(the 8-way drain ``for_each``) and ``compute_action_context_statsbomb`` (sb360, ADR-058) and asserts
that the drain actually **finished its work**.

**This module holds NO rules.** Every rule lives in the pure ``analytics.action_context.drain_gate``
— which is what makes all four verdicts reachable in an offline unit test. All this module does is:

1. resolve ``run_id``,
2. adapt three persisted tables into the pure core's inputs,
3. re-run the planner for the M2/V7 diagnostic,
4. log the report, and
5. turn ``must_raise`` into an exit code.

THE EVIDENCE INVARIANT (spec §0)
--------------------------------
The gate's evidence comes ONLY from persisted tables, on an explicit allowlist: the **work queue**,
the **unit-event view**, and the **results mart** (the cross-check). Its only task-value/parameter
inputs are ``run_id`` and ``catalog`` — **parameters, never evidence**. Nothing from process memory.
This is a checkable rule because the same defect was introduced TWICE before it existed: a gate fed
``summary.timed_out``, then one fed ``sink.write_failures`` — both in-memory objects living inside a
*drain worker*, read by a gate that runs in a **different task, in a different process**.

The planner re-run (step 3) reads bronze through ``_ActionContextGuard.discover_units`` — i.e.
through **the planner itself**, the same function preflight used. That is deliberate: the diagnostic
must dissent from the *same* discovery the queue was built from (a re-implementation here would
compare the drain against a second, drifting definition of "work"), and it keeps every table name in
this module inside the allowlist.

``run_if = ALL_DONE`` (spec §6)
-------------------------------
The task runs even when the drain FAILED — otherwise a dead worker would skip the gate and D9's
whole OOM-visibility payoff (``running`` written BEFORE processing) would be delivered to nobody.
Consequently only ``INCOMPLETE`` — and a planner alarm — fail the task. ``DRAIN_FAILED`` and
``UNVERIFIABLE`` are REPORTS: the job has already failed, and the gate's job there is to **say what
died**, not to mask the drain's real exception with its own.
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
from ingestion.action_context import _TABLE_NAME as _RESULTS_TABLE
from ingestion.drain_adapters import _EVENT_SCHEMA, _EVENT_TABLE, _QUEUE_SCHEMA, _QUEUE_TABLE
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

#: The provider whose ids are float-formatted in bronze (``"3788746.0"``); its results-mart join key
#: is canonicalized exactly as ``_find_sb360_new_ids`` does (ADR-019). A bare ``cast("string")`` on a
#: double-typed id yields ``"3788746.0"`` and silently drops from ``.isin``.
_SB360_PROVIDER = "statsbomb"


def _read_queue(spark: SparkSession, catalog: str, run_id: str) -> list[QueueRow]:
    """The drain's contract for this run. Bounded: one batch per run (measured: 374 units)."""
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
    """The unit-event log for this run — read through the UNION ALL **view**, never the per-worker
    tables (the physical topology is the sink's business; the gate must not know the fan-out width).

    RUN-SCOPED (spec §3): units are re-enqueued across runs, so a latest-across-all-runs read would
    misattribute a prior run's terminal to a fresh unit. Bounded: ~2 events per unit + one
    ``slice_completed`` per writer.
    """
    from pyspark.sql import functions as F  # noqa: N812

    df = (
        spark.table(f"{catalog}.{_EVENT_SCHEMA}.{_EVENT_TABLE}")
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
    spark: SparkSession, catalog: str, schema: str, keys: list[UnitKey]
) -> dict[tuple[str, str, int | None], int]:
    """What LANDED, per ``(provider, match_id, period_id)`` — the cross-check's independent re-read.

    SCOPED to this run's match ids: unscoped, it scans the whole mart. One bounded aggregate per
    provider (at most ``len(keys)`` rows reach the driver).

    The mart is PER-PERIOD while sb360 units are MATCH grain (``period`` NULL) — the pure core rolls
    a match's periods up, which is why the counts are returned per-period rather than per-unit.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.action_context import _canon_key

    matches: dict[str, set[str]] = defaultdict(set)
    for key in keys:
        matches[key.provider].add(key.match_id)

    counts: dict[tuple[str, str, int | None], int] = {}
    for provider, match_ids in matches.items():
        key_col = _canon_key("match_id") if provider == _SB360_PROVIDER else F.col("match_id").cast("string")
        rows = (
            spark.table(f"{catalog}.{schema}.{_RESULTS_TABLE}")
            .filter((F.col("data_source") == provider) & key_col.isin(sorted(match_ids)))
            .groupBy(key_col.alias("_mid"), F.col("period_id").cast("bigint").alias("_period"))
            .count()
            .collect()
        )
        for row in rows:
            period = None if row["_period"] is None else int(row["_period"])
            counts[(provider, str(row["_mid"]), period)] = int(row["count"])
    return counts


def _remaining_units(spark: SparkSession, catalog: str, schema: str) -> frozenset[UnitKey]:
    """Re-run THE PLANNER (not a re-implementation of it) and return what it still enumerates.

    Two teeth hang off this (M2 + V7), and one blind spot is worth stating plainly (W2): because the
    diagnostic re-runs the SAME function preflight used, it **cannot** catch an UNDER-enumerating
    planner — a planner whose join matches nothing returns ∅ here too, so ``enqueued == 0`` *and*
    ``remaining == 0`` and the tooth never fires. Its real job is an empty queue caused by something
    OTHER than the planner (e.g. a failed enqueue), and V7's write-landed check. The under-
    enumeration class is held by Task 5's two-sided planner tests + the live 374-count gate.

    sb360 is NOT included: it is never enqueued and has its own discovery, so folding it in would
    make "empty queue + remaining > 0" fire on the perfectly ordinary state *"the drain is caught up
    and new statsbomb matches arrived"*.
    """
    from ingestion.action_context import _ActionContextGuard

    units = _ActionContextGuard().discover_units(spark, catalog, schema)
    return frozenset(UnitKey(provider=u.provider, match_id=str(u.match_id), period=u.period) for u in units)


def _resolve_run_id(raw: object) -> str:
    """``{{job.run_id}}`` — and NOT the preflight task value (§0c-bis).

    ``preflight_action_context`` sets ``action_context_run_id`` to ``""`` on a nothing-to-do run,
    while ``compute_action_context_statsbomb`` — which does not even depend on preflight, so the task
    value is not resolvable there — files its events under ``{{job.run_id}}``. A gate wired to the
    task value would therefore audit run ``""``, find no sb360 ``slice_completed``, and report
    **DRAIN_FAILED every quiet day**: crying wolf on the most common run there is.

    So an empty value dies LOUD and names the right source, rather than silently auditing a run that
    has no events. ``{{job.run_id}}`` is never empty, so this can only fire on a mis-wired task.
    """
    run_id = str(raw or "").strip()
    if not run_id:
        raise SystemExit(
            "--run-id is required and must be the JOB run id. Wire it to '{{job.run_id}}' -- NOT to "
            "{{tasks.preflight_action_context.values.action_context_run_id}}, which is EMPTY on a "
            "nothing-to-do run while sb360 files its events under the real job run id (the gate would "
            "then report DRAIN_FAILED every quiet day)."
        )
    return run_id


def _log_report(report_text: str, verdict: Verdict, task_logger: logging.Logger) -> None:
    """COMPLETE at INFO; every other verdict at **ERROR**.

    Never WARNING (ADR-002): warning-level logs are invisible in error-log queries -- that is what
    hid the 2026-04-12 warm-tier cost-hook blocker for 62+ hours. A DRAIN_FAILED / UNVERIFIABLE gate
    does not fail the task, so the log line is the ONLY signal it emits; it has to be findable.
    """
    if verdict is Verdict.COMPLETE:
        task_logger.info("%s", report_text)
    else:
        task_logger.error("%s", report_text)


def main() -> None:
    """Entry point ``verify_action_context_drain``."""
    args = parse_ingestion_args(
        "D8: verify the action-context drain finished its work for this run",
        extra_args=[
            (
                "--run-id",
                {
                    "type": str,
                    "default": None,
                    "help": "The JOB run id -- wire this to '{{job.run_id}}'. NOT the preflight task "
                    "value: that is empty on a nothing-to-do run, while sb360 (which does not depend "
                    "on preflight) files its events under the job run id.",
                },
            ),
        ],
    )
    task_logger = configure_logging("action_context_gate")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    run_id = _resolve_run_id(getattr(args, "run_id", None))
    catalog, schema = args.catalog, args.schema

    queue = _read_queue(spark, catalog, run_id)
    events = _read_events(spark, catalog, run_id)
    # The mart read is scoped to the SAME unit set the rules evaluate -- via the pure helper, not a
    # second definition here. Two definitions of "expected" would let a unit be judged against a
    # count that was never fetched.
    keys = list(expected_units(queue=queue, events=events))
    result_counts = _read_result_counts(spark, catalog, schema, keys)
    planner = PlannerInputs(enqueued=len(queue), remaining=_remaining_units(spark, catalog, schema))

    report = evaluate(
        run_id=run_id,
        queue=queue,
        events=events,
        result_counts=result_counts,
        planner=planner,
    )
    _log_report(report.render(), report.verdict, task_logger)
    # The ONLY place the task fails. INCOMPLETE (a clean worker's unit never ran / its rows did not
    # land) and the planner's write-landed alarm raise; DRAIN_FAILED and UNVERIFIABLE have already
    # said everything they have to say, in the log, at ERROR.
    enforce(report)
