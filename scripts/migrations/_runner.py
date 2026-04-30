"""Reusable idempotent migration executor for scripts/migrations/*.sql.

Submits each statement through the Databricks SDK's
``WorkspaceClient.statement_execution.execute_statement()`` (REST
``POST /api/2.0/sql/statements/``) instead of the legacy
``databricks-sql-connector`` Thrift path. Validated by the
``probe-sdk-statement-execution`` workflow run on PR #239 (commit
e54019c) — the SDK path works in the live-build CI environment where
the connector consistently exhausted its 900s retry budget without
ever opening a session (PR #235 runs 25189954469 / 25190951314 /
25192157073, three identical failures across cold-and-warm warehouse
states and both static / OIDC token types).

Behaviour preserved from the legacy connector path:

- Multi-statement files split on ``;``.
- Comment-only lines stripped before split.
- ALTER TABLE ADD COLUMNS with a single new column is skipped via a
  pre-check ``DESCRIBE TABLE`` lookup, since Databricks SQL does not
  support ``ADD COLUMN IF NOT EXISTS`` in the ALTER TABLE clause.
- All other statements (UPDATE, SET TBLPROPERTIES, CREATE TABLE IF
  NOT EXISTS, GRANT, etc.) execute unconditionally — relying on each
  migration file being idempotent by construction (per CLAUDE.md
  "Project Conventions" → Bronze migrations auto-apply contract).

Auth resolution is the SDK default chain — PAT (DATABRICKS_TOKEN)
locally, github-oidc in CI (DATABRICKS_AUTH_TYPE=github-oidc). The
SDK also auto-starts a STOPPED warehouse on the first
``execute_statement`` call, so the previous ``ensure_warehouse.py``
prelude is no longer needed.

Usage:
    uv run python scripts/migrations/_runner.py \\
        scripts/migrations/2026-04-30-add-idsse-tracking-match-metadata.sql

Environment:
    DATABRICKS_HOST       Workspace hostname (with or without scheme).
    DATABRICKS_HTTP_PATH  /sql/1.0/warehouses/<id> — last segment is parsed as warehouse_id.
    DATABRICKS_TOKEN      PAT (local). Auto-resolved by SDK.
    DATABRICKS_CLIENT_ID + DATABRICKS_AUTH_TYPE=github-oidc  (CI path.)
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_ADD_RE = re.compile(
    r"ALTER\s+TABLE\s+(\S+)\s+ADD\s+COLUMNS\s*\(\s*(\w+)\s+\w+",
    re.IGNORECASE,
)

# Polling envelope for a single statement. With 2s sleep that gives ~180s of
# active polling per statement. ALTER ADD COLUMNS / UPDATE / SET TBLPROPERTIES /
# CREATE TABLE IF NOT EXISTS all complete in <10s on a running warehouse;
# warehouse cold-start can add 30-60s on a serverless PRO 2X-Small (the
# workspace baseline), so 180s gives ~3x headroom for the first statement of
# a fresh CI run.
_MAX_POLL_ROUNDS = 90


def _exec(w: WorkspaceClient, warehouse_id: str, statement: str) -> list[list]:
    """Execute a single SQL statement and poll until terminal state.

    Returns the result rows on SUCCEEDED. Raises ``RuntimeError`` with the
    statement and underlying server message on FAILED / CANCELED / timeout.
    """
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )
    sid = resp.statement_id
    for _ in range(_MAX_POLL_ROUNDS):
        if resp.status and resp.status.state == StatementState.SUCCEEDED:
            return resp.result.data_array if resp.result and resp.result.data_array else []
        if resp.status and resp.status.state in (StatementState.FAILED, StatementState.CANCELED):
            msg = resp.status.error.message if resp.status.error else "unknown"
            raise RuntimeError(f"statement failed: {msg}\nstatement: {statement[:200]}")
        time.sleep(2)
        if not sid:
            raise RuntimeError("no statement_id from execute_statement")
        resp = w.statement_execution.get_statement(sid)
    raise RuntimeError(f"timeout polling statement after {_MAX_POLL_ROUNDS * 2}s")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("migration", type=pathlib.Path)
    args = parser.parse_args()

    w = WorkspaceClient()
    warehouse_id = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").split("/")[-1]

    def col_exists(table: str, col: str) -> bool:
        rows = _exec(w, warehouse_id, f"DESCRIBE TABLE {table}")
        return col in {r[0] for r in rows if r[0] and not r[0].startswith("#")}

    raw = args.migration.read_text()
    code_only = "\n".join(ln for ln in raw.splitlines() if not ln.lstrip().startswith("--"))
    for stmt in code_only.split(";"):
        s = stmt.strip()
        if not s:
            continue
        head = s.splitlines()[0][:100]
        m = _ADD_RE.match(s)
        if m:
            table, col = m.group(1), m.group(2)
            if col_exists(table, col):
                print(f"Skipping (col exists): {head}")
                continue
        print(f"Executing: {head}")
        _exec(w, warehouse_id, s)
        print("  OK")

    print("Migration complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
