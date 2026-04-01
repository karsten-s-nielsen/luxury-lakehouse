#!/usr/bin/env python3
"""Synced table maintenance: refresh → create indexes → verify.

Orchestrates the operational procedure for synced table maintenance
as a single command, encoding what was previously a manual sequence.

Usage:
    python scripts/maintain_synced_tables.py --catalog soccer_analytics --schema dev_gold
    python scripts/maintain_synced_tables.py --skip-refresh   # indexes only
    python scripts/maintain_synced_tables.py --dry-run         # print commands
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

    # ── Step 1: Refresh synced tables ────────────────────────────────────────
    if not args.skip_refresh:
        ok, elapsed = _run_step(
            name="refresh_synced_tables",
            cmd=[sys.executable, "scripts/refresh_synced_tables.py", "--wait", *base_args],
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
        cmd=[sys.executable, "scripts/create_indexes.py", *base_args],
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
            cmd=[sys.executable, "scripts/create_indexes.py", "--verify", *base_args],
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
