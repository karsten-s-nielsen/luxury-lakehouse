#!/usr/bin/env python3
"""Synced table maintenance: grant permissions → refresh → create indexes → verify.

Orchestrates the operational procedure for synced table maintenance
as a single command, encoding what was previously a manual sequence.

Steps:
    -1. Fix event_log_* ownership drift (post-recreation hygiene)
    0. Grant SP permissions (database-project CAN_USE + pipeline CAN_RUN)
    0.5. Grant Taipy SP PG SELECT on synced tables (ADR-005; run_lakebase_grants)
    1. Refresh synced tables (trigger SNAPSHOT updates)
    2. Create indexes (PG btree + HNSW)
    3. Verify indexes (EXPLAIN ANALYZE)

Usage:
    python scripts/maintain_synced_tables.py --catalog soccer_analytics --schema dev_gold
    python scripts/maintain_synced_tables.py --skip-refresh   # grants + indexes only
    python scripts/maintain_synced_tables.py --skip-grants    # skip step 0
    python scripts/maintain_synced_tables.py --dry-run        # print commands
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

_LOG_SOURCE = "maintain_synced_tables"


def _log(event: str, **kwargs: object) -> None:
    """Emit a structured JSON-line log to stdout."""
    record = {"source": _LOG_SOURCE, "event": event, **kwargs}
    print(json.dumps(record), flush=True)


def _run_step(name: str, cmd: list[str], dry_run: bool) -> tuple[bool, float]:
    """Run *cmd* as a subprocess step, logging timing and outcome.

    Returns ``(success, elapsed_seconds)``.  In dry-run mode the command is
    printed but not executed and success is always ``True``.
    """
    _log("step_start", step=name, cmd=" ".join(cmd))

    if dry_run:
        _log("step_dry_run", step=name, cmd=" ".join(cmd))
        return True, 0.0

    t0 = time.monotonic()
    result = subprocess.run(cmd, check=False)  # noqa: S603
    elapsed = time.monotonic() - t0
    success = result.returncode == 0

    _log(
        "step_complete" if success else "step_failed",
        step=name,
        returncode=result.returncode,
        elapsed_s=round(elapsed, 2),
    )
    return success, elapsed


def main() -> int:
    """Orchestrate synced table maintenance."""
    parser = argparse.ArgumentParser(
        description="Synced table maintenance: refresh → create indexes → verify.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--catalog", default="soccer_analytics", help="Unity Catalog catalog name")
    parser.add_argument("--schema", default="dev_gold", help="Target schema (default: dev_gold)")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--skip-grants", action="store_true", help="Skip Step 0 (grant SP permissions)")
    parser.add_argument("--skip-refresh", action="store_true", help="Skip Step 1 (refresh synced tables)")
    parser.add_argument("--skip-verify", action="store_true", help="Skip Step 3 (EXPLAIN ANALYZE verification)")
    args = parser.parse_args()

    _log(
        "maintenance_start",
        catalog=args.catalog,
        schema=args.schema,
        dry_run=args.dry_run,
        skip_refresh=args.skip_refresh,
        skip_verify=args.skip_verify,
    )

    base_args = ["--catalog", args.catalog, "--schema", args.schema]
    total_elapsed = 0.0

    # ── Step -1: Fix pipeline event_log_* ownership drift ────────────────────
    # When a synced table is recreated via the Databricks UI, the pipeline's
    # event_log_* table gets owned by whoever performed the recreation
    # instead of the dbt-owners-{env} group. The ingestion SP then cannot
    # trigger pipeline refreshes. Idempotent (already_correct = no-op).
    # Uses --skip-trigger-refresh because Step 1 below handles refresh.
    if not args.skip_grants:
        ok, elapsed = _run_step(
            name="fix_event_log_ownership",
            cmd=[sys.executable, "scripts/fix_event_log_ownership.py", "--skip-trigger-refresh"],
            dry_run=args.dry_run,
        )
        total_elapsed += elapsed
        if not ok:
            _log("maintenance_aborted", reason="event_log_ownership_failed", total_elapsed_s=round(total_elapsed, 2))
            return 1
    else:
        _log("step_skipped", step="fix_event_log_ownership")

    # ── Step 0: Grant SP permissions on database project + pipelines ─────────
    # Idempotent. Required so the staging Taipy admin endpoint and the daily
    # Databricks job task can call w.postgres.get_synced_table() and
    # /api/2.0/pipelines/{id}/updates as the hf_app_v2 / ingestion SPs. Must
    # be re-run after any synced table recreation (pipeline_ids may change).
    if not args.skip_grants:
        ok, elapsed = _run_step(
            name="grant_synced_table_permissions",
            cmd=[sys.executable, "scripts/grant_synced_table_permissions.py"],
            dry_run=args.dry_run,
        )
        total_elapsed += elapsed
        if not ok:
            _log("maintenance_aborted", reason="grants_failed", total_elapsed_s=round(total_elapsed, 2))
            return 1
    else:
        _log("step_skipped", step="grant_synced_table_permissions")

    # ── Step 0.5: Grant Taipy SP PG SELECT on synced tables (ADR-005) ────────
    # Separate from Step 0: Step 0 grants Databricks-level CAN_USE on the
    # database project + CAN_RUN on pipelines (used by the staging admin
    # endpoint and the ingestion SP for trigger-refresh). Step 0.5 grants
    # PG-level SELECT inside Lakebase so the Taipy app SP (hf_app_v2) can
    # read the synced tables via JDBC. Both are required after any synced
    # table recreation; without Step 0.5, the Taipy deploy gate (ADR-005)
    # fails with `DRIFT: SP ... is missing SELECT on N synced table(s)`.
    # run_lakebase_grants.py auto-discovers Lakebase DNS — no env wiring.
    if not args.skip_grants:
        ok, elapsed = _run_step(
            name="lakebase_grants_taipy_sp",
            cmd=[sys.executable, "scripts/run_lakebase_grants.py"],
            dry_run=args.dry_run,
        )
        total_elapsed += elapsed
        if not ok:
            _log(
                "maintenance_aborted",
                reason="lakebase_grants_failed",
                total_elapsed_s=round(total_elapsed, 2),
            )
            return 1
    else:
        _log("step_skipped", step="lakebase_grants_taipy_sp")

    # ── Step 1: Refresh synced tables ────────────────────────────────────────
    if not args.skip_refresh:
        ok, elapsed = _run_step(
            name="refresh_synced_tables",
            cmd=[sys.executable, "-m", "ingestion.refresh_synced_tables", "--wait", *base_args],
            dry_run=args.dry_run,
        )
        total_elapsed += elapsed
        if not ok:
            _log("maintenance_aborted", reason="refresh_failed", total_elapsed_s=round(total_elapsed, 2))
            return 1
    else:
        _log("step_skipped", step="refresh_synced_tables")

    # ── Step 2: Create indexes ────────────────────────────────────────────────
    ok, elapsed = _run_step(
        name="create_indexes",
        cmd=[sys.executable, "scripts/create_indexes.py"],
        dry_run=args.dry_run,
    )
    total_elapsed += elapsed
    if not ok:
        _log("maintenance_aborted", reason="index_creation_failed", total_elapsed_s=round(total_elapsed, 2))
        return 1

    # ── Step 3: Verify indexes ────────────────────────────────────────────────
    if not args.skip_verify:
        ok, elapsed = _run_step(
            name="verify_indexes",
            cmd=[sys.executable, "scripts/create_indexes.py", "--verify"],
            dry_run=args.dry_run,
        )
        total_elapsed += elapsed
        if not ok:
            # Verification warnings are soft — log and exit 0.
            _log(
                "verify_warnings",
                message="Verification step reported issues (soft warning — exiting 0)",
                total_elapsed_s=round(total_elapsed, 2),
            )
            _log("maintenance_complete", status="ok_with_warnings", total_elapsed_s=round(total_elapsed, 2))
            return 0
    else:
        _log("step_skipped", step="verify_indexes")

    _log("maintenance_complete", status="ok", total_elapsed_s=round(total_elapsed, 2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
