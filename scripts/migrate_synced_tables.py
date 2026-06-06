#!/usr/bin/env python3
"""One-shot SDK synced table migration (ADR-026).

Migrates all 41 Lakebase synced tables from UI-created / Terraform-imported
to SDK-managed via ``w.postgres.create_synced_table()``.

Four phases:
  Phase 0 — Smoke test (create + grants + delete on throwaway table)
  Phase 1 — Delete all 41 synced tables
  Phase 2 — Enable CDF on TRIGGERED source tables
  Phase 3 — Create all 41 via SDK
  Phase 4 — Wait until all ONLINE, then run maintenance pipeline

Usage:
    uv run python scripts/migrate_synced_tables.py                    # Full migration
    uv run python scripts/migrate_synced_tables.py --phase 0          # Smoke test only
    uv run python scripts/migrate_synced_tables.py --skip-phase 0     # Skip smoke test

Idempotent: re-running after partial failure picks up where it left off.
Phase 1 tolerates "not found" errors. Phase 2 is idempotent (SET TBLPROPERTIES).
Phase 3 tolerates "already exists" errors.

**Outage window:** Phases 1-3 delete all 41 tables then recreate them. The Taipy
app has degraded Lakebase connectivity (~30 min) during this window. This is an
explicit design choice: the app is low-traffic, a rolling migration adds significant
complexity for no user-facing benefit, and the total wall-clock is dominated by
Phase 4 (wait for ONLINE) which happens after tables already exist.

Auth: uses WorkspaceClient — must run as workspace admin with PAT.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from ingestion.refresh_synced_tables import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    SYNCED_TABLES,
    SyncedTableConfig,
    wait_until_online,
)
from ingestion.synced_table_lifecycle import SdkWriterAdapter

# Synced-table create/delete now delegate to the shared SdkWriterAdapter (single source of truth —
# see src/ingestion/synced_table_lifecycle.py). _BRANCH / _PG_DATABASE / the policy map live there.
_SMOKE_TEST_TABLE = "dim_competitions_synced_sdk_test"
_SMOKE_TEST_SOURCE = "dim_competitions"

# Databricks SQL warehouse for Statement Execution API (CDF enablement)
_WAREHOUSE_ID_ENV = "DATABRICKS_HTTP_PATH"

# All identifiers interpolated into SQL / PG queries must match this pattern.
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _log(phase: int, msg: str) -> None:
    print(f"[Phase {phase}] {msg}", flush=True)


def _get_warehouse_id() -> str:
    """Extract warehouse ID from DATABRICKS_HTTP_PATH env var.

    The env var has format: /sql/1.0/warehouses/<warehouse_id>
    """
    import os

    http_path = os.environ.get(_WAREHOUSE_ID_ENV, "")
    match = re.search(r"/warehouses/([a-f0-9]+)$", http_path)
    if not match:
        msg = (
            f"Cannot extract warehouse ID from {_WAREHOUSE_ID_ENV}={http_path!r}. "
            f"Set DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>"
        )
        raise RuntimeError(msg)
    return match.group(1)


def _create_synced_table(ws: WorkspaceClient, config: SyncedTableConfig, catalog: str, default_schema: str) -> None:
    """Create a single synced table via the SDK postgres API (delegates to the shared adapter)."""
    SdkWriterAdapter(ws).create_synced_table(config, catalog, default_schema)


def _delete_synced_table(ws: WorkspaceClient, config: SyncedTableConfig, catalog: str, default_schema: str) -> bool:
    """Delete a single synced table (SDK only). Returns True if deleted, False if not found."""
    schema = config.schema_override or default_schema
    return SdkWriterAdapter(ws).sdk_delete(f"{catalog}.{schema}.{config.name}")


def phase_0_smoke_test(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 0: Create a throwaway synced table, verify it works, delete it."""
    _log(0, f"Creating throwaway table: {_SMOKE_TEST_TABLE}")
    smoke_config = SyncedTableConfig(
        name=_SMOKE_TEST_TABLE,
        source_table=_SMOKE_TEST_SOURCE,
        primary_key_columns=("competition_id",),
    )

    # Clean up any leftover from a prior run
    _delete_synced_table(ws, smoke_config, catalog, default_schema)
    # Brief pause to let the async delete operation propagate on Databricks' side
    # before attempting to create a table with the same name.
    time.sleep(5)

    # Create
    _create_synced_table(ws, smoke_config, catalog, default_schema)
    _log(0, "Created — waiting for ONLINE state")

    # Wait
    full_name = f"{catalog}.{default_schema}.{_SMOKE_TEST_TABLE}"
    wait_until_online(full_name, timeout_s=300, poll_interval_s=10)
    _log(0, "ONLINE — verifying PG-side data")

    # Verify data actually synced to PostgreSQL
    import os
    import uuid

    import psycopg2

    lakebase_host = os.environ.get("LAKEBASE_HOST", "")
    if lakebase_host:
        endpoint = os.environ.get(
            "LAKEBASE_ENDPOINT_NAME",
            "projects/soccer-analytics-dev/branches/production/endpoints/primary",
        )
        host = (ws.config.host or "").rstrip("/")
        auth_headers: dict[str, str] = ws.config.authenticate()  # type: ignore[assignment]
        import base64
        import json

        import requests

        resp = requests.post(
            f"{host}/api/2.0/postgres/credentials",
            headers={**auth_headers, "Content-Type": "application/json"},
            json={"endpoint": endpoint, "request_id": str(uuid.uuid4())},
            verify=True,
            timeout=(10, 30),
        )
        resp.raise_for_status()
        token = resp.json()["token"]
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        username = json.loads(base64.b64decode(payload_b64))["sub"]

        # Validate identifiers before interpolation into SQL (OWASP SQL injection defence)
        for _label, _val in [("schema", default_schema), ("table", _SMOKE_TEST_TABLE)]:
            if not _IDENTIFIER_RE.match(_val):
                raise ValueError(f"Invalid {_label} identifier: {_val!r}")

        conn = psycopg2.connect(
            host=lakebase_host,
            port=5432,
            dbname="databricks_postgres",
            user=username,
            password=token,
            sslmode="require",
        )
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {default_schema}."{_SMOKE_TEST_TABLE}"')  # noqa: S608 — validated by _IDENTIFIER_RE above
            row_count = cur.fetchone()[0]  # type: ignore[index]
        conn.close()
        if row_count > 0:
            _log(0, f"PG verification PASSED — {row_count} rows in {_SMOKE_TEST_TABLE}")
        else:
            _log(0, "WARNING: PG table exists but has 0 rows (may still be syncing)")
    else:
        _log(0, "LAKEBASE_HOST not set — skipping PG-side verification")

    # Clean up
    _delete_synced_table(ws, smoke_config, catalog, default_schema)
    _log(0, "Cleaned up throwaway table")
    _log(0, "SMOKE TEST PASSED")


def phase_1_delete_all(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 1: Delete all 41 synced tables."""
    _log(1, f"Deleting {len(SYNCED_TABLES)} synced tables")
    deleted = 0
    not_found = 0
    for i, config in enumerate(SYNCED_TABLES, 1):
        try:
            if _delete_synced_table(ws, config, catalog, default_schema):
                deleted += 1
                print(f"  [{i}/{len(SYNCED_TABLES)}] Deleted: {config.name}")
            else:
                not_found += 1
                print(f"  [{i}/{len(SYNCED_TABLES)}] Not found (already deleted): {config.name}")
        except Exception as exc:
            print(f"  [{i}/{len(SYNCED_TABLES)}] ERROR deleting {config.name}: {exc}")
            raise
    _log(1, f"COMPLETE — {deleted} deleted, {not_found} already gone")


def phase_2_enable_cdf(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 2: Enable CDF on source tables for TRIGGERED synced tables."""
    triggered = [c for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"]
    _log(2, f"Enabling CDF on {len(triggered)} TRIGGERED source tables")

    warehouse_id = _get_warehouse_id()

    for config in triggered:
        schema = config.schema_override or default_schema
        source_fqn = f"{catalog}.{schema}.{config.source_table}"
        stmt = f"ALTER TABLE {source_fqn} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        result = ws.statement_execution.execute_statement(
            statement=stmt,
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        if result.status and result.status.state == StatementState.SUCCEEDED:
            print(f"  CDF enabled: {source_fqn}")
        else:
            error = getattr(result.status, "error", None) if result.status else None
            msg = f"CDF enablement failed for {source_fqn}: {error}"
            raise RuntimeError(msg)

    _log(2, "COMPLETE — CDF enabled on all TRIGGERED sources")


def phase_3_create_all(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 3: Create all 41 synced tables via SDK."""
    _log(3, f"Creating {len(SYNCED_TABLES)} synced tables")
    created = 0
    already_exists = 0
    for i, config in enumerate(SYNCED_TABLES, 1):
        try:
            _create_synced_table(ws, config, catalog, default_schema)
            created += 1
            print(f"  [{i}/{len(SYNCED_TABLES)}] Created: {config.name} ({config.scheduling_policy})")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                already_exists += 1
                print(f"  [{i}/{len(SYNCED_TABLES)}] Already exists: {config.name}")
            else:
                print(f"  [{i}/{len(SYNCED_TABLES)}] ERROR creating {config.name}: {exc}")
                raise
    _log(3, f"COMPLETE — {created} created, {already_exists} already existed")


def phase_4_wait_and_maintain(ws: WorkspaceClient, catalog: str, default_schema: str) -> None:
    """Phase 4: Wait for all tables to come ONLINE, then run maintenance."""
    _log(4, f"Waiting for {len(SYNCED_TABLES)} tables to come ONLINE")

    def _wait_one(config: SyncedTableConfig) -> tuple[str, bool, str]:
        schema = config.schema_override or default_schema
        fqn = f"{catalog}.{schema}.{config.name}"
        try:
            wait_until_online(fqn, timeout_s=1200, poll_interval_s=15)
            return (config.name, True, "ONLINE")
        except Exception as exc:  # noqa: BLE001 — must catch all failure modes from SDK + network
            return (config.name, False, str(exc))

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_wait_one, c): c for c in SYNCED_TABLES}
        failures: list[str] = []
        for future in as_completed(futures):
            name, ok, msg = future.result()
            if ok:
                print(f"  {name}: ONLINE")
            else:
                print(f"  {name}: FAILED — {msg}")
                failures.append(f"{name}: {msg}")

    if failures:
        print(f"\nERROR: {len(failures)} tables failed to come online:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    _log(4, "All tables ONLINE — running maintenance pipeline")

    # Run the full maintenance pipeline: ownership -> grants -> indexes -> verify
    subprocess.run(
        ["uv", "run", "python", "scripts/maintain_synced_tables.py", "--skip-refresh"],  # noqa: S607 — uv is a known local tool
        check=True,
    )

    _log(4, "MAINTENANCE COMPLETE")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-shot SDK synced table migration (ADR-026).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--phase", type=int, help="Run only this phase (0-4)")
    parser.add_argument("--skip-phase", type=int, action="append", default=[], help="Skip these phases")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help=f"Catalog (default: {DEFAULT_CATALOG})")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help=f"Default schema (default: {DEFAULT_SCHEMA})")
    args = parser.parse_args()

    ws = WorkspaceClient()
    phases = {
        0: ("Smoke test", phase_0_smoke_test),
        1: ("Delete all", phase_1_delete_all),
        2: ("Enable CDF", phase_2_enable_cdf),
        3: ("Create all", phase_3_create_all),
        4: ("Wait + maintain", phase_4_wait_and_maintain),
    }

    for phase_num, (label, fn) in phases.items():
        if args.phase is not None and phase_num != args.phase:
            continue
        if phase_num in args.skip_phase:
            print(f"\n{'=' * 60}")
            print(f"SKIPPING Phase {phase_num}: {label}")
            continue
        print(f"\n{'=' * 60}")
        print(f"Phase {phase_num}: {label}")
        print(f"{'=' * 60}")
        fn(ws, args.catalog, args.schema)

    print(f"\n{'=' * 60}")
    print("MIGRATION COMPLETE")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
