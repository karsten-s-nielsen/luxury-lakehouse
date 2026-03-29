#!/usr/bin/env python3
"""Ensure the Databricks SQL warehouse is RUNNING before executing a command.

The databricks-sql-connector (v4.1.x) has a retry-sleep bug that makes
auto-resume unreliable: each retry sleeps at least ``delay_max`` (60 s),
so only ~15 attempts fit in the 900 s retry window.  If the warehouse is
STOPPED when dbt (or any Thrift client) connects, the session often times
out before a connection is established.

This script checks the warehouse state via the REST API, starts it if
needed, polls until RUNNING, and then ``exec``s the given command.

Usage:
    python scripts/ensure_warehouse.py -- uv run dbt build --select +fct_formation_labels --profiles-dir .
    python scripts/ensure_warehouse.py  # just start the warehouse, no command

Environment:
    DATABRICKS_HOST          Workspace hostname (no https://)
    DATABRICKS_TOKEN         PAT or OAuth token
    DATABRICKS_HTTP_PATH     /sql/1.0/warehouses/<id>

Requires:
    requests (already a project dependency)
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time

import requests

logger = logging.getLogger("ensure_warehouse")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# ── Config ───────────────────────────────────────────────────────────────────

_POLL_INTERVAL_S = 5
_MAX_WAIT_S = 180  # 3 min — serverless typically starts in 10-30 s


def _env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        logger.error("Environment variable %s is not set", key)
        sys.exit(1)
    return val


def _warehouse_id() -> str:
    """Extract warehouse ID from DATABRICKS_HTTP_PATH (/sql/1.0/warehouses/<id>).

    Resilient to MSYS path conversion: Git Bash converts ``/sql/...`` to
    ``C:/Program Files/Git/sql/...``.  We only need the last path segment.
    """
    http_path = _env("DATABRICKS_HTTP_PATH")
    parts = http_path.rstrip("/").split("/")
    return parts[-1]


def _api_url(warehouse_id: str, suffix: str = "") -> str:
    host = _env("DATABRICKS_HOST").rstrip("/")
    return f"https://{host}/api/2.0/sql/warehouses/{warehouse_id}{suffix}"


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_env('DATABRICKS_TOKEN')}"}


# ── Core ─────────────────────────────────────────────────────────────────────


def get_state(warehouse_id: str) -> str:
    """Return the current warehouse state (RUNNING, STOPPED, STARTING, etc.)."""
    resp = requests.get(
        _api_url(warehouse_id),
        headers=_headers(),
        timeout=(10, 30),
        verify=True,
    )
    resp.raise_for_status()
    return resp.json()["state"]


def start_warehouse(warehouse_id: str) -> None:
    """Send a start request (no-op if already RUNNING/STARTING)."""
    resp = requests.post(
        _api_url(warehouse_id, "/start"),
        headers=_headers(),
        timeout=(10, 30),
        verify=True,
    )
    resp.raise_for_status()
    logger.info("Start request accepted")


def wait_until_running(warehouse_id: str) -> None:
    """Poll until the warehouse is RUNNING or timeout."""
    t0 = time.monotonic()
    while True:
        state = get_state(warehouse_id)
        elapsed = time.monotonic() - t0
        logger.info("Warehouse state: %s  (%.0fs elapsed)", state, elapsed)

        if state == "RUNNING":
            return

        if state in ("STOPPED", "STOPPING"):
            # Warehouse stopped (maybe auto-stopped while we were polling).
            # Re-issue start.
            logger.warning("Warehouse is %s — re-sending start request", state)
            start_warehouse(warehouse_id)

        if elapsed > _MAX_WAIT_S:
            logger.error(
                "Warehouse did not reach RUNNING within %ds (last state: %s)",
                _MAX_WAIT_S,
                state,
            )
            sys.exit(1)

        time.sleep(_POLL_INTERVAL_S)


def main() -> None:
    warehouse_id = _warehouse_id()
    state = get_state(warehouse_id)
    logger.info("Warehouse %s state: %s", warehouse_id, state)

    if state == "RUNNING":
        logger.info("Warehouse is already RUNNING")
    else:
        if state not in ("STARTING", "RESUMING"):
            start_warehouse(warehouse_id)
        wait_until_running(warehouse_id)

    # If a command was given after --, exec it
    if "--" in sys.argv:
        cmd_start = sys.argv.index("--") + 1
        cmd = sys.argv[cmd_start:]
        if cmd:
            logger.info("Executing: %s", " ".join(cmd))
            result = subprocess.run(cmd)  # noqa: S603 — cmd is sys.argv from the invoking user, not untrusted input
            sys.exit(result.returncode)

    logger.info("Done — warehouse is RUNNING")


if __name__ == "__main__":
    main()
