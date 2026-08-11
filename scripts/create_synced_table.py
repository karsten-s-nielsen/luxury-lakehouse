#!/usr/bin/env python3
"""Create a single Databricks synced table from its canonical SYNCED_TABLES config.

Symmetric to scripts/delete_synced_table.py. The config (source mart, PK, scheduling policy) is
resolved from ingestion.refresh_synced_tables.SYNCED_TABLES (single source of truth). A freshly
created TRIGGERED synced table auto-starts its initial sync, so this also waits until it is online.

Usage:
    uv run --extra sdk python scripts/create_synced_table.py fct_action_context_synced
"""

from __future__ import annotations

import argparse
import re
import sys

from ingestion.databricks_auth import workspace_client

IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a single synced table from SYNCED_TABLES")
    parser.add_argument("table_name", help="Synced table name, e.g. fct_action_context_synced")
    args = parser.parse_args()
    table_name: str = args.table_name

    if not IDENTIFIER_RE.match(table_name):
        print(f"ERROR: Invalid table name '{table_name}': must match {IDENTIFIER_RE.pattern}")
        return 1

    from ingestion.refresh_synced_tables import SYNCED_TABLES, wait_until_online
    from ingestion.synced_table_lifecycle import SdkWriterAdapter

    cfg = next((c for c in SYNCED_TABLES if c.name == table_name), None)
    if cfg is None:
        known = ", ".join(sorted(c.name for c in SYNCED_TABLES))
        print(f"ERROR: '{table_name}' not in SYNCED_TABLES. Known: {known}")
        return 1

    ws = workspace_client()
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    print(f"[1/2] Creating synced table: {full_name}")
    SdkWriterAdapter(ws).create_synced_table(cfg, CATALOG, SCHEMA)
    print("  OK — create requested; initial sync auto-started.")
    print(f"[2/2] Waiting until online: {full_name}")
    wait_until_online(full_name, timeout_s=1200, poll_interval_s=15)
    print("  OK — synced table online.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
