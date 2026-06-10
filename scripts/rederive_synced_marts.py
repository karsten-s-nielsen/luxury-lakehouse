#!/usr/bin/env python3
"""Strand-safe re-derive of TRIGGERED-synced gold marts (ADR-043).

The ONLY operator entry point for re-deriving a mart whose Lakebase synced table is
scheduling_policy=TRIGGERED. Classifies each selected mart (pure planner) into:
  D  — incremental + match_id-filtered: `dbt build` with reprocess_match_ids (MERGE,
       CDF partial-update, no strand), then trigger+wait the synced table.
  T  — `table` mart: plain `dbt build` (atomic create-or-replace, count-safe — the daily
       stage-3 path), then trigger+wait with --fail-on-strand. Since the 2026-06-10 platform
       change the rebuild STRANDS the TRIGGERED synced table and the ADR-041 heal recreates
       it (brief re-snapshot downtime; ADR-043 amendment 2) — this tool exits loud on the
       strand so "success" always means a FRESH synced table.
  B  — merge-all incremental: delete synced -> `dbt build --full-refresh`
       (allow_triggered_full_refresh) -> recreate synced -> grants+indexes.

SNAPSHOT marts are skipped (immune). Composes existing scripts; this file is the thin
executor adapter (the planning logic lives in ingestion.rederive_planner).

Usage:
    uv run --extra sdk python scripts/rederive_synced_marts.py --select fct_action_values --provider idsse
    uv run --extra sdk python scripts/rederive_synced_marts.py --select tag:marts --match-ids 12,34 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from ingestion.rederive_planner import PlanStep, plan_rederive
from shared.constants import IDENTIFIER_RE

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DBT_PROJECT = _REPO_ROOT / "dbt_project"

_CATALOG = "soccer_analytics"
_BRONZE_SCHEMA = "bronze"  # live-confirmed: soccer_analytics.bronze.spadl_actions (9.7M rows). NOT dev_bronze.
_DAILY_JOB_ID = 302697362345215  # soccer-analytics-ingestion-dev (mega-job)
_MODEL_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Static per-mart downtime hints for --dry-run (B path re-snapshot is the cost, T5).
_LARGE_B_MARTS = frozenset({"fct_action_context", "fct_player_embeddings"})


def _parse_model_names(dbt_ls_stdout: str) -> set[str]:
    """Extract model names from `dbt ls --output name` output (ignore log noise)."""
    return {line.strip() for line in dbt_ls_stdout.splitlines() if _MODEL_NAME_RE.match(line.strip())}


def _downtime_estimate(model: str, action: str) -> str:
    if action == "D":
        return "none (in-place MERGE)"
    if action == "T":
        return (
            "brief (rebuild strands the synced table since the 2026-06-10 platform change; "
            "the ADR-041 heal recreates it — re-snapshot downtime, ADR-043 amendment 2)"
        )
    if model in _LARGE_B_MARTS:
        return "MINUTES — size a maintenance window (synced re-snapshot of a multi-million-row table)"
    return "seconds-to-minutes (small synced re-snapshot)"


def _validate_match_ids(steps: list[PlanStep], *, match_ids: list[int]) -> None:
    """A D step with no match ids is a no-op re-derive — fail loud rather than silently do nothing."""
    if any(s.action == "D" for s in steps) and not match_ids:
        print(
            "ERROR: selection includes D (per-match) marts but no --provider/--match-ids given. "
            "A D re-derive with no match ids changes nothing. Supply --provider <p> or --match-ids a,b.",
            file=sys.stderr,
        )
        raise SystemExit(2)


def _resolve_selected_models(selector: str) -> set[str]:
    res = subprocess.run(  # noqa: S603
        # --quiet (m3): suppress dbt log lines so a stray lowercase token can't be parsed as a model.
        ["dbt", "ls", "--quiet", "--resource-type", "model", "--select", selector, "--output", "name"],  # noqa: S607
        cwd=str(_DBT_PROJECT),
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        print(f"ERROR: `dbt ls` failed:\n{res.stderr}", file=sys.stderr)
        raise SystemExit(1)
    return _parse_model_names(res.stdout)


def _warehouse_id() -> str:
    import os

    m = re.search(r"/warehouses/([a-f0-9]+)$", os.environ.get("DATABRICKS_HTTP_PATH", ""))
    if not m:
        print("ERROR: cannot resolve warehouse id from DATABRICKS_HTTP_PATH", file=sys.stderr)
        raise SystemExit(2)
    return m.group(1)


def _match_ids_for_provider(provider: str) -> list[int]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    if not IDENTIFIER_RE.match(provider):
        print(f"ERROR: invalid --provider {provider!r}", file=sys.stderr)
        raise SystemExit(2)
    ws = WorkspaceClient()
    warehouse_id = _warehouse_id()
    stmt = (
        # Only trusted module constants (_CATALOG/_BRONZE_SCHEMA) are interpolated; the
        # user-supplied provider is bound via the :provider parameter, never interpolated.
        f"select distinct match_id from {_CATALOG}.{_BRONZE_SCHEMA}.spadl_actions "  # noqa: S608
        "where data_source = :provider and match_id is not null"
    )
    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=stmt,
        parameters=[StatementParameterListItem(name="provider", value=provider)],
        wait_timeout="50s",
    )
    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        print(f"ERROR: match-id query did not succeed: {resp.status}", file=sys.stderr)
        raise SystemExit(1)
    rows = (resp.result.data_array if resp.result else None) or []
    return sorted(int(r[0]) for r in rows if r and r[0] is not None)


def _assert_daily_job_idle(force: bool) -> None:
    """Refuse to run while the daily ingestion job is active (real job state, not the clock — T4)."""
    if force:
        return
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    active = list(ws.jobs.list_runs(job_id=_DAILY_JOB_ID, active_only=True))
    if active:
        ids = ", ".join(str(r.run_id) for r in active)
        print(
            f"ERROR: daily ingestion job {_DAILY_JOB_ID} has active run(s) [{ids}]. "
            "A concurrent D MERGE/B rebuild can conflict with the daily MERGE. "
            "Re-run after it finishes, or pass --force to override.",
            file=sys.stderr,
        )
        raise SystemExit(3)


def _run(cmd: list[str], *, cwd: str | None = None) -> None:
    print(f"  $ {' '.join(cmd)}", flush=True)
    res = subprocess.run(cmd, cwd=cwd, check=False)  # noqa: S603
    if res.returncode != 0:
        print(f"ERROR: step failed (exit {res.returncode}): {' '.join(cmd)}", file=sys.stderr)
        raise SystemExit(res.returncode)


def _execute_d(step: PlanStep) -> None:
    print(f"[D] {step.model} — MERGE reprocess (no downtime)")
    _run(["dbt", "build", "--select", step.model, "--vars", json.dumps(step.dbt_vars)], cwd=str(_DBT_PROJECT))
    _run(
        [
            sys.executable,
            "-m",
            "ingestion.refresh_synced_tables",
            "--tables",
            step.synced_table,
            "--wait",
            "--fail-on-strand",
        ]
    )


def _execute_t(step: PlanStep) -> None:
    # Plain rebuild of a `table` mart, then trigger+wait. Since the 2026-06-10 Databricks rollout a
    # plain create-or-replace STRANDS the TRIGGERED synced table (XXKST — supervised cycle proof,
    # ADR-043 amendment 2); the ADR-041 heal recreates it. --fail-on-strand makes THIS tool exit
    # loud on the strand (the synced table serves STALE data until the heal lands) instead of the
    # 2026-06-10 incident's "Done — synced tables online" false success banner.
    print(f"[T] {step.model} — plain rebuild ({_downtime_estimate(step.model, 'T')})")
    _run(["dbt", "build", "--select", step.model], cwd=str(_DBT_PROJECT))
    _run(
        [
            sys.executable,
            "-m",
            "ingestion.refresh_synced_tables",
            "--tables",
            step.synced_table,
            "--wait",
            "--fail-on-strand",
        ]
    )


def _execute_b(step: PlanStep) -> None:
    print(f"[B] {step.model} — delete synced -> full-refresh -> recreate ({_downtime_estimate(step.model, 'B')})")
    _run([sys.executable, "scripts/delete_synced_table.py", step.synced_table], cwd=str(_REPO_ROOT))
    _run(
        ["dbt", "build", "--select", step.model, "--full-refresh", "--vars", json.dumps(step.dbt_vars)],
        cwd=str(_DBT_PROJECT),
    )
    _run(
        ["uv", "run", "--extra", "sdk", "python", "scripts/create_synced_table.py", step.synced_table],
        cwd=str(_REPO_ROOT),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Strand-safe re-derive of TRIGGERED-synced gold marts.")
    parser.add_argument("--select", required=True, help="dbt selector (e.g. fct_action_values, tag:marts)")
    parser.add_argument("--provider", default="", help="Re-derive all matches of this data_source (D marts)")
    parser.add_argument("--match-ids", default="", help="Comma-separated match_ids (D marts)")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Full-rebuild selected marts via the B path (delete->full-refresh->recreate). "
        "Use for a D mart's schema/contract change — the tripwire blocks a bare dbt --full-refresh.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the resolved plan and exit")
    parser.add_argument("--force", action="store_true", help="Run even if the daily ingestion job is active")
    args = parser.parse_args()

    if args.provider and args.match_ids:
        print("ERROR: pass --provider OR --match-ids, not both", file=sys.stderr)
        return 2

    selected = _resolve_selected_models(args.select)
    if not selected:
        print(f"No models matched selector {args.select!r}", file=sys.stderr)
        return 1

    if args.match_ids:
        match_ids = sorted(int(x) for x in args.match_ids.split(",") if x.strip())
    elif args.provider:
        match_ids = _match_ids_for_provider(args.provider)
    else:
        match_ids = []

    steps = plan_rederive(selected, match_ids, rebuild=args.rebuild)
    if not steps:
        print("No TRIGGERED synced marts in selection — nothing to do (SNAPSHOT marts are strand-immune).")
        return 0

    _validate_match_ids(steps, match_ids=match_ids)

    print(f"Plan ({len(steps)} step(s); {len(match_ids)} match id(s)):")
    for s in steps:
        print(f"  [{s.action}] {s.model} -> {s.synced_table} | downtime: {_downtime_estimate(s.model, s.action)}")
        print(f"        vars: {json.dumps(s.dbt_vars)}")
    if args.dry_run:
        print("\n--dry-run: no changes made.")
        return 0

    _assert_daily_job_idle(args.force)

    ran_b = False
    for s in steps:
        if s.action == "D":
            _execute_d(s)
        elif s.action == "T":
            _execute_t(s)
        else:
            _execute_b(s)
            ran_b = True

    if ran_b:
        print("Re-applying grants + indexes after B rebuild(s)...")
        _run(
            [
                "uv",
                "run",
                "--extra",
                "sdk",
                "python",
                "scripts/maintain_synced_tables.py",
                "--skip-heal",
                "--skip-refresh",
            ],
            cwd=str(_REPO_ROOT),
        )

    print("Done — re-derive complete, synced tables online.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
