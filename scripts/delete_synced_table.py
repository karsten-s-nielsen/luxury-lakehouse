#!/usr/bin/env python3
"""Delete a Databricks synced table and drop its PG ghost table.

Usage:
    python scripts/delete_synced_table.py fct_player_stats_synced

Requires:
    - DATABRICKS_HOST and DATABRICKS_TOKEN env vars (or OAuth config)
    - Lakebase endpoint must be reachable
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from typing import TYPE_CHECKING

import psycopg2
import requests

from shared.constants import IDENTIFIER_RE

# PR-Cycle-B (2026-05-01): databricks-sdk is in the [sdk] optional extra.
# Lazy-import keeps this module importable without the extra installed.
if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
else:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        WorkspaceClient = None  # type: ignore[assignment, misc]

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
ENDPOINT_NAME = os.environ.get(
    "LAKEBASE_ENDPOINT_NAME", "projects/soccer-analytics-dev/branches/production/endpoints/primary"
)
PG_DATABASE = "databricks_postgres"


def _get_pg_token(ws: WorkspaceClient) -> tuple[str, str]:
    """Get a PG credential token and extract the username."""
    import base64
    import json

    host = (ws.config.host or "").rstrip("/")
    auth_headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]

    resp = requests.post(
        f"{host}/api/2.0/postgres/credentials",
        headers={**auth_headers, "Content-Type": "application/json"},
        json={"endpoint": ENDPOINT_NAME, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    token = resp.json()["token"]

    # Extract username from JWT sub claim
    payload_b64 = token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    payload = json.loads(base64.b64decode(payload_b64))
    username = payload["sub"]

    return token, username


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete a synced table and its PG ghost")
    parser.add_argument("table_name", help="Synced table name, e.g. fct_player_stats_synced")
    args = parser.parse_args()

    table_name: str = args.table_name

    # Validate table name to prevent SQL injection in DDL statement
    if not IDENTIFIER_RE.match(table_name):
        print(f"ERROR: Invalid table name '{table_name}': must match {IDENTIFIER_RE.pattern}")
        sys.exit(1)

    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    ws = WorkspaceClient()

    # The two-step delete (SDK delete + PG ghost drop) is the canonical lifecycle; the
    # implementations now live in the shared thin adapters (single source of truth).
    from ingestion.lakebase_endpoint import derive_lakebase_dns
    from ingestion.synced_table_lifecycle import PsycopgGhostAdapter, SdkWriterAdapter

    # Step 1: Delete synced table via Databricks SDK
    print(f"\n[1/2] Deleting synced table: {full_name}")
    try:
        deleted = SdkWriterAdapter(ws).sdk_delete(full_name)
        print("  OK — synced table deleted" if deleted else "  Not found — may already be deleted. Continuing.")
    except Exception as exc:
        print(f"  ERROR: {exc}")
        print("  (Continuing to PG cleanup.)")

    # Step 2: Drop ghost PG table
    print(f"\n[2/2] Dropping PG ghost table: {SCHEMA}.{table_name}")
    try:
        token, username = _get_pg_token(ws)
        print(f"  PG user: {username}")
        # Derive the endpoint DNS (LAKEBASE_HOST is an optional local-dev override only) — ADR-041.
        lakebase_host = derive_lakebase_dns(ws, endpoint_name=ENDPOINT_NAME)

        def _pg_connect():
            return psycopg2.connect(
                host=lakebase_host,
                port=5432,
                dbname=PG_DATABASE,
                user=username,
                password=token,
                sslmode="require",
                options="-c statement_timeout=30000",
            )

        PsycopgGhostAdapter(_pg_connect).drop_pg_ghost(SCHEMA, table_name)
        print("  OK — PG table dropped")
    except Exception as exc:
        print(f"  ERROR dropping PG ghost: {exc}")
        print("  You may need to drop it manually via psql.")
        sys.exit(1)

    print(f"\nDone. Recreate {table_name} via scripts/migrate_synced_tables.py or SDK.")


if __name__ == "__main__":
    main()
