#!/usr/bin/env python3
"""Create PG indexes on Lakebase synced tables for Streamlit query performance.

Lakebase synced tables are partitioned internally. Indexes must be created on
the parent table WITHOUT the ``ONLY`` keyword so PostgreSQL cascades them to
child partitions (where the data actually lives). ``CREATE INDEX ... ON ONLY``
produces indexes that exist only on the parent and are never used by queries.

Run this script after every synced table recreation (the recreation drops all
custom indexes).

Usage:
    python scripts/create_indexes.py

Requires:
    - ``databricks`` CLI configured with an OAUTH profile
    - Network access to the Lakebase endpoint
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
import time
import uuid

import psycopg2
import requests

DATABRICKS_HOST = "https://dbc-48322be9-16be.cloud.databricks.com"
LAKEBASE_HOST = "ep-spring-rain-d2i6lozx.database.us-east-1.cloud.databricks.com"
ENDPOINT_NAME = "projects/soccer-analytics-dev/branches/production/endpoints/primary"
PG_DATABASE = "databricks_postgres"
SCHEMA = "dev_gold"

# Index definitions: (index_name, table, columns)
# Indexes are created with IF NOT EXISTS for idempotency.
INDEXES: list[tuple[str, str, str]] = [
    # fct_tracking_frames_synced — 38M+ rows, drives Pitch Control page
    ("idx_tracking_match_id", "fct_tracking_frames_synced", "match_id"),
    ("idx_tracking_source_provider", "fct_tracking_frames_synced", "source_provider"),
    ("idx_tracking_provider_match", "fct_tracking_frames_synced", "source_provider, match_id"),
    ("idx_tracking_match_frame", "fct_tracking_frames_synced", "match_id, frame"),
    ("idx_tracking_match_period_frame", "fct_tracking_frames_synced", "match_id, period, frame"),
]


def _get_pg_credential() -> tuple[str, str]:
    """Get a PG credential token via Databricks CLI OAuth."""
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", "OAUTH"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    auth_token = json.loads(result.stdout)["access_token"]

    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/postgres/credentials",
        headers={"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"},
        json={"endpoint": ENDPOINT_NAME, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    pg_token: str = resp.json()["token"]

    payload_b64 = pg_token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    username: str = json.loads(base64.b64decode(payload_b64))["sub"]

    return pg_token, username


def main() -> None:
    """Create all indexes idempotently."""
    pg_token, username = _get_pg_credential()
    print(f"PG user: {username}")

    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=PG_DATABASE,
        user=username,
        password=pg_token,
        sslmode="require",
        # 10 minutes — index creation on 38M rows can take a while
        options="-c statement_timeout=600000",
    )
    conn.autocommit = True

    created = 0
    errors = 0

    with conn.cursor() as cur:
        for idx_name, table, columns in INDEXES:
            fqn = f"{SCHEMA}.{table}"
            ddl = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {fqn} ({columns})"
            try:
                print(f"  {idx_name} ON {table}({columns})...", end=" ", flush=True)
                t0 = time.time()
                cur.execute(ddl)
                elapsed = time.time() - t0
                print(f"OK ({elapsed:.1f}s)")
                created += 1
            except Exception as exc:
                print(f"ERROR: {exc}")
                errors += 1

        # Verify child partition indexes exist
        print("\nVerifying child partition indexes...")
        cur.execute(
            """
            SELECT c.relname AS partition, i.indexname, i.indexdef
            FROM pg_inherits inh
            JOIN pg_class c ON c.oid = inh.inhrelid
            JOIN pg_class p ON p.oid = inh.inhparent
            JOIN pg_namespace pn ON p.relnamespace = pn.oid
            JOIN pg_indexes i ON i.tablename = c.relname
            WHERE pn.nspname = %s
            ORDER BY c.relname, i.indexname
            """,
            (SCHEMA,),
        )
        child_indexes = cur.fetchall()
        if child_indexes:
            current_partition = ""
            for partition, iname, _idef in child_indexes:
                if partition != current_partition:
                    print(f"\n  Partition: {partition}")
                    current_partition = partition
                print(f"    {iname}")
        else:
            print("  WARNING: No child partition indexes found!")

    conn.close()

    print(f"\nSummary: {created} processed (IF NOT EXISTS), {errors} errors")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
