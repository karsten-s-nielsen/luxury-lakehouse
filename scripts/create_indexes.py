#!/usr/bin/env python3
"""Create PG indexes on Lakebase synced tables for Streamlit query performance.

Lakebase synced tables are partitioned internally. Indexes must be created on
the parent table WITHOUT the ``ONLY`` keyword so PostgreSQL cascades them to
child partitions (where the data actually lives). ``CREATE INDEX ... ON ONLY``
produces indexes that exist only on the parent and are never used by queries.

Run this script after every synced table recreation (the recreation drops all
custom indexes).

Usage:
    python scripts/create_indexes.py           # Create indexes only
    python scripts/create_indexes.py --verify  # Create indexes + EXPLAIN ANALYZE

Requires:
    - ``databricks`` CLI configured with an OAUTH profile
    - Network access to the Lakebase endpoint
"""

from __future__ import annotations

import argparse
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
    # ── fct_tracking_frames_synced — 38M+ rows (Pitch Control) ──────────
    ("idx_tracking_match_id", "fct_tracking_frames_synced", "match_id"),
    ("idx_tracking_source_provider", "fct_tracking_frames_synced", "source_provider"),
    ("idx_tracking_provider_match", "fct_tracking_frames_synced", "source_provider, match_id"),
    ("idx_tracking_match_frame", "fct_tracking_frames_synced", "match_id, frame"),
    ("idx_tracking_match_period_frame", "fct_tracking_frames_synced", "match_id, period, frame"),
    # ── fct_passes_synced — 3.17M+ rows ─────────────────────────────────
    # P-1, PN-1: pass_map + pass_network (comp + team + match exact)
    ("idx_passes_comp_team_match", "fct_passes_synced", "competition_id, team_id, match_id"),
    # H-1: heat_map player filter (comp + player, no team)
    ("idx_passes_comp_player", "fct_passes_synced", "competition_id, player_id"),
    # F-3: player filter EXISTS subquery (player_id + team_id)
    ("idx_passes_player_team", "fct_passes_synced", "player_id, team_id"),
    # ── fct_shots_synced — 100K+ rows ───────────────────────────────────
    # S-1, H-1: shot_map + heat_map (comp + optional team + optional player)
    ("idx_shots_comp_team_player", "fct_shots_synced", "competition_id, team_id, player_id"),
    # F-3: player filter EXISTS subquery (player_id + team_id)
    ("idx_shots_player_team", "fct_shots_synced", "player_id, team_id"),
    # ── fct_action_values_synced — 2M+ rows ─────────────────────────────
    # AV-2, AV-3: action breakdown + player options (comp + team + player prefix)
    ("idx_action_values_comp_team_player", "fct_action_values_synced", "competition_id, team_id, player_id"),
    # AV-4: match action timeline (match_id)
    ("idx_action_values_match_id", "fct_action_values_synced", "match_id"),
    # ── fct_player_stats_synced — 10K+ rows ───────────────────────────
    # F-3, R-1, AV-1: player filter + radar + VAEP rankings (competition_id prefix)
    ("idx_player_stats_comp_id", "fct_player_stats_synced", "competition_id"),
    # R-2: radar player lookup (competition_id + player_id)
    ("idx_player_stats_comp_player", "fct_player_stats_synced", "competition_id, player_id"),
    # ── fct_physical_stats_synced — ~600 rows (Movement Analysis) ──
    # MA-1: match filter (match_id)
    ("idx_physical_stats_match", "fct_physical_stats_synced", "match_id"),
    # MA-2: player + match lookup (player_id, match_id)
    ("idx_physical_stats_player_match", "fct_physical_stats_synced", "player_id, match_id"),
    # MA-3: provider filter in recursive CTE match listing
    ("idx_physical_stats_provider", "fct_physical_stats_synced", "source_provider, match_id"),
    # ── fct_match_summary_synced — ~3K rows (PPDA) ──────────────────
    # MS-1: competition + PPDA filter
    ("idx_match_summary_comp_id", "fct_match_summary_synced", "competition_id"),
]

# Verification queries for --verify flag: (description, query)
# Each query should exercise a specific index on a fact table >100K rows.
# Uses LIMIT 1 to keep execution fast; EXPLAIN ANALYZE still shows the plan.
# Note: SCHEMA is a module-level constant, not user input — S608 suppressed.
VERIFY_QUERIES: list[tuple[str, str]] = [
    (
        "fct_passes: comp+team+match (idx_passes_comp_team_match)",
        f"SELECT * FROM {SCHEMA}.fct_passes_synced"  # noqa: S608
        " WHERE competition_id = 11 AND team_id = 217 AND match_id = 3788741 LIMIT 1",
    ),
    (
        "fct_passes: player+team EXISTS (idx_passes_player_team)",
        f"SELECT 1 FROM {SCHEMA}.fct_passes_synced WHERE player_id = 5503 AND team_id = 217 LIMIT 1",  # noqa: S608
    ),
    (
        "fct_shots: comp+team+player prefix (idx_shots_comp_team_player)",
        f"SELECT * FROM {SCHEMA}.fct_shots_synced WHERE competition_id = 11 LIMIT 1",  # noqa: S608
    ),
    (
        "fct_action_values: comp+team (idx_action_values_comp_team_player)",
        f"SELECT * FROM {SCHEMA}.fct_action_values_synced"  # noqa: S608
        " WHERE competition_id = 11 AND team_id = 217 LIMIT 1",
    ),
    (
        "fct_action_values: match_id (idx_action_values_match_id)",
        f"SELECT * FROM {SCHEMA}.fct_action_values_synced WHERE match_id = 3788741 LIMIT 1",  # noqa: S608
    ),
    (
        "fct_player_stats: comp_id (idx_player_stats_comp_id)",
        f"SELECT * FROM {SCHEMA}.fct_player_stats_synced WHERE competition_id = 11 LIMIT 1",  # noqa: S608
    ),
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


def _create_indexes(conn: psycopg2.extensions.connection) -> int:
    """Create all indexes idempotently. Returns number of errors."""
    created = 0
    errors = 0

    with conn.cursor() as cur:
        for idx_name, table, columns in INDEXES:
            fqn = f"{SCHEMA}.{table}"
            # All values are compile-time constants from INDEXES — no user input.
            ddl = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {fqn} ({columns})"
            try:
                print(f"  {idx_name} ON {table}({columns})...", end=" ", flush=True)
                t0 = time.time()
                cur.execute(ddl)
                elapsed = time.time() - t0
                print(f"OK ({elapsed:.1f}s)")
                created += 1
            except psycopg2.OperationalError:
                # Connection-level error — no point continuing
                raise
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

    print(f"\nSummary: {created} processed (IF NOT EXISTS), {errors} errors")
    return errors


def _verify_indexes(conn: psycopg2.extensions.connection) -> int:
    """Run EXPLAIN ANALYZE on representative queries and check for Index Scan.

    Returns number of hard errors (exceptions). Seq Scan warnings are
    informational — PG legitimately prefers sequential scans on small tables
    or when LIMIT 1 makes startup cost dominant.
    """
    print("\n" + "=" * 60)
    print("EXPLAIN ANALYZE verification")
    print("=" * 60)

    error_count = 0
    seq_scan_count = 0

    with conn.cursor() as cur:
        for description, query in VERIFY_QUERIES:
            print(f"\n  {description}")
            try:
                cur.execute(f"EXPLAIN ANALYZE {query}")
                plan_lines = [row[0] for row in cur.fetchall()]

                # Check if any line mentions Seq Scan on the fact table
                uses_seq_scan = any("Seq Scan" in line for line in plan_lines)
                uses_index = any("Index" in line for line in plan_lines)

                if uses_index and not uses_seq_scan:
                    print("    PASS — Index Scan detected")
                elif uses_seq_scan:
                    # Not a hard error — PG may legitimately prefer seq scan
                    # for small tables or LIMIT 1 queries with low selectivity.
                    print("    INFO — Seq Scan chosen by planner (index exists, planner prefers seq)")
                    seq_scan_count += 1
                else:
                    print("    INFO — No Seq Scan or Index Scan found in plan")

                # Print first few lines for context
                for line in plan_lines[:5]:
                    print(f"    | {line}")
                if len(plan_lines) > 5:
                    print(f"    | ... ({len(plan_lines) - 5} more lines)")

            except Exception as exc:
                print(f"    ERROR: {exc}")
                error_count += 1

    print(f"\nVerification: {len(VERIFY_QUERIES)} queries checked")
    if seq_scan_count:
        print(f"  {seq_scan_count} used Seq Scan (planner choice, not missing index)")
    return error_count


def main() -> None:
    """Create all indexes idempotently, with optional EXPLAIN verification."""
    parser = argparse.ArgumentParser(description="Create PG indexes on Lakebase synced tables.")
    parser.add_argument("--verify", action="store_true", help="Run EXPLAIN ANALYZE after index creation")
    args = parser.parse_args()

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

    try:
        errors = _create_indexes(conn)

        if args.verify:
            errors += _verify_indexes(conn)
    finally:
        conn.close()

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
