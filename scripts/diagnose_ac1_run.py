#!/usr/bin/env python3
"""Option E diagnostic: full task + for-each iteration tree for specific runs.

For each given run_id: dump every task (incl. for-each iterations) with state
and task-run-id, then fetch get_run_output (driver logs + error) for the
preflight, the for-each iterations, and any FAILED/TIMEDOUT/RUNNING task — and
print the log tail. This locates the ACTUAL hanging iteration sub-task (the
parent for-each task only shows UPSTREAM_FAILED/CANCELED) and shows where its
driver log went silent (AC1_FINGERPRINT -> ... -> ?).

Usage:
    uv run python scripts/diagnose_ac1_run.py RUN_ID [RUN_ID ...]
"""

from __future__ import annotations

import sys

from databricks.sdk import WorkspaceClient

from ingestion.databricks_auth import workspace_client

_INTERESTING_KEYS = ("action_context", "preflight")
_LOG_TAIL_CHARS = 4000


def _state(obj) -> str:
    st = getattr(obj, "state", None)
    if st is None:
        return "NO_STATE"
    rs = getattr(st, "result_state", None)
    lc = getattr(st, "life_cycle_state", None)
    rs_v = rs.value if rs else "-"
    lc_v = lc.value if lc else "-"
    return f"{lc_v}/{rs_v}"


def _fmt_ms(ms) -> str:
    if not ms:
        return "?"
    import datetime as _dt

    return _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.timezone.utc).strftime("%H:%M:%S")


def _timing(t) -> str:
    """setup + exec durations (ms→s) and start/end wall times for a task attempt."""
    start = getattr(t, "start_time", None)
    end = getattr(t, "end_time", None)
    setup = getattr(t, "setup_duration", None) or 0
    execd = getattr(t, "execution_duration", None) or 0
    total = (end - start) / 1000 if (start and end) else None
    total_s = f"{total:.0f}s" if total is not None else "?"
    return (
        f"start={_fmt_ms(start)} end={_fmt_ms(end)} wall={total_s} setup={setup / 1000:.0f}s exec={execd / 1000:.0f}s"
    )


def _dump_output(w: WorkspaceClient, label: str, task_run_id: int) -> None:
    print(f"\n----- LOGS: {label} (task_run={task_run_id}) -----")
    try:
        out = w.jobs.get_run_output(run_id=task_run_id)
    except Exception as exc:  # noqa: BLE001 — diagnostic, surface any fetch error
        print(f"  <get_run_output failed: {exc}>")
        return
    err = getattr(out, "error", None)
    err_trace = getattr(out, "error_trace", None)
    logs = getattr(out, "logs", None) or ""
    logs_truncated = getattr(out, "logs_truncated", None)
    if err:
        print(f"  ERROR: {err}")
    if err_trace:
        print(f"  ERROR_TRACE (tail):\n{err_trace[-_LOG_TAIL_CHARS:]}")
    if logs:
        print(f"  LOGS truncated={logs_truncated}, len={len(logs)}; tail:")
        print(logs[-_LOG_TAIL_CHARS:])
    if not (err or err_trace or logs):
        print("  <no error / no logs returned by get_run_output>")


def _process_run(w: WorkspaceClient, run_id: int) -> None:
    print(f"\n{'=' * 78}\nRUN {run_id}\n{'=' * 78}")
    run = w.jobs.get_run(run_id=run_id)
    print(f"  run state: {_state(run)}")

    tasks = list(run.tasks or [])
    for t in tasks:
        key = (t.task_key or "").lower()
        if any(k in key for k in _INTERESTING_KEYS) or "timedout" in _state(t).lower():
            print(f"  TASK {t.task_key:34s} {_state(t):24s} task_run={t.run_id}  {_timing(t)}")
        else:
            print(f"  TASK {t.task_key:34s} {_state(t):24s} task_run={t.run_id}")

    # For-each iterations live on run.iterations (Databricks for_each_task).
    iterations = list(getattr(run, "iterations", None) or [])
    if iterations:
        print(f"\n  -- {len(iterations)} for-each iteration(s) --")
        for it in iterations:
            print(f"  ITER {it.task_key:38s} {_state(it):24s} task_run={it.run_id}")

    # Fetch logs for: preflight + iterations + any non-clean task.
    seen: set[int] = set()
    candidates = []
    for t in tasks:
        key = (t.task_key or "").lower()
        if any(k in key for k in _INTERESTING_KEYS):
            candidates.append((t.task_key, t.run_id))
    for it in iterations:
        candidates.append((f"iter:{it.task_key}", it.run_id))

    for label, rid in candidates:
        if rid and rid not in seen:
            seen.add(rid)
            _dump_output(w, label, rid)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: diagnose_ac1_run.py RUN_ID [RUN_ID ...]")
        return 2
    w = workspace_client()
    for raw in sys.argv[1:]:
        _process_run(w, int(raw))
    return 0


if __name__ == "__main__":
    sys.exit(main())
