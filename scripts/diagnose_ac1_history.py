#!/usr/bin/env python3
"""Option A diagnostic: AC-1 task run history + fingerprint extraction.

Pulls run history for the mega-job, isolates the two AC-1 compute tasks
(compute_action_context, compute_tracking_context), prints their per-run
state timeline, finds the last SUCCESS (if any), and extracts the
AC1_FINGERPRINT line (wheel_version + silly_kicks_version) from the driver
log of the most relevant run.

This answers: has AC-1 EVER succeeded on serverless, and if so, on what
(wheel, silly-kicks) combo? If it never succeeded, this is a never-shipped
path, not a regression.

Usage:
    uv run python scripts/diagnose_ac1_history.py [--runs N] [--job-id ID]

Auth: databricks-sdk default chain (DATABRICKS_HOST + token, or profile).
"""

from __future__ import annotations

import argparse
import re
import sys

from databricks.sdk import WorkspaceClient

TARGET_TASKS = ("compute_action_context", "compute_tracking_context")
_FINGERPRINT_RE = re.compile(r"AC1_FINGERPRINT.*$", re.MULTILINE)


def _state_of(task) -> str:
    if task.state and task.state.result_state:
        return task.state.result_state.value
    if task.state and task.state.life_cycle_state:
        return task.state.life_cycle_state.value
    return "UNKNOWN"


def _fmt_ts(ms: int | None) -> str:
    if not ms:
        return "?"
    # Avoid Date.now-style nondeterminism concerns: pure formatting of epoch ms.
    import datetime as _dt

    return _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M")


def _extract_fingerprint(w: WorkspaceClient, task_run_id: int) -> str | None:
    try:
        out = w.jobs.get_run_output(run_id=task_run_id)
    except Exception as exc:  # noqa: BLE001 — diagnostic script, surface any fetch failure
        return f"<could not fetch logs: {exc}>"
    logs = out.logs or ""
    matches = _FINGERPRINT_RE.findall(logs)
    if matches:
        return matches[-1].strip()
    err = out.error or ""
    if err:
        return f"<no fingerprint; error tail: {err[-300:]}>"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=150, help="Max number of recent runs to scan")
    parser.add_argument("--job-id", type=int, default=302697362345215, help="Job ID")
    args = parser.parse_args()

    w = WorkspaceClient()

    print(f"Scanning up to {args.runs} completed runs of job {args.job_id} for AC-1 tasks...\n")

    # task_key -> list of (run_start_ms, state, task_run_id, job_run_id)
    history: dict[str, list[tuple[int, str, int, int]]] = {k: [] for k in TARGET_TASKS}

    count = 0
    for run in w.jobs.list_runs(job_id=args.job_id, completed_only=True, expand_tasks=True, limit=25):
        count += 1
        if count > args.runs:
            break
        for task in run.tasks or []:
            if task.task_key in TARGET_TASKS:
                history[task.task_key].append((run.start_time or 0, _state_of(task), task.run_id, run.run_id))

    print(f"Scanned {count} runs.\n")

    for task_key in TARGET_TASKS:
        rows = history[task_key]
        print(f"=== {task_key} ===")
        if not rows:
            print("  NO RUNS FOUND in scanned window — task never dispatched here.\n")
            continue
        rows.sort(key=lambda r: r[0], reverse=True)
        # Timeline (most recent first)
        for start_ms, state, _task_run_id, job_run_id in rows:
            print(f"  {_fmt_ts(start_ms)}  [{state:>9}]  job_run={job_run_id}")

        succeeded = [r for r in rows if r[1] == "SUCCESS"]
        print()
        if succeeded:
            start_ms, _state, task_run_id, job_run_id = succeeded[0]
            print(f"  LAST SUCCESS: {_fmt_ts(start_ms)} (job_run={job_run_id}, task_run={task_run_id})")
            fp = _extract_fingerprint(w, task_run_id)
            print(f"  FINGERPRINT @ last success: {fp}")
        else:
            print("  *** NEVER SUCCEEDED in scanned window ***")
            # Pull fingerprint from most recent run to confirm current versions + hang point.
            start_ms, state, task_run_id, _job_run_id = rows[0]
            fp = _extract_fingerprint(w, task_run_id)
            print(f"  FINGERPRINT @ most recent ({state}): {fp}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
