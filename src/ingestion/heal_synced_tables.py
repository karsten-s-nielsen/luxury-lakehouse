"""Privileged synced-table heal entry point (spec — the remediation half of the detect/heal split).

Runs in the PG + warehouse-capable maintenance identity (NOT the daily-job SP). Composes the four
thin adapters into ``HealPorts``, finds the stranded (checkpoint-broken) TRIGGERED synced tables —
either an explicit ``--tables`` list (from the daily task's ``workflow_dispatch``) or a full scan —
and runs ``run_heal_pass``. Recreated tables are regranted/reindexed by the grants+indexes passes that
run AFTER this in the maintenance flow (ordering is load-bearing — spec P5).

Console-script entry: ``heal_synced_tables`` (registered in pyproject.toml).

Kill-switch: ``SYNCED_TABLE_HEAL_ENABLED=0`` disables all destructive heal (spec P3). Resolved here
at the CLI boundary and injected, so the policy (``run_heal_pass``) stays pure (review R6).
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import requests

from ingestion.lakebase_endpoint import derive_lakebase_dns
from ingestion.refresh_synced_tables import DEFAULT_CATALOG, DEFAULT_SCHEMA, SYNCED_TABLES
from ingestion.synced_table_heal import HealPorts, is_checkpoint_mismatch_failure, run_heal_pass
from ingestion.synced_table_lifecycle import (
    PsycopgGhostAdapter,
    SdkReaderAdapter,
    SdkWriterAdapter,
    WarehouseCdfAdapter,
)
from ingestion.synced_table_strand_state import StrandStateStore, WarehouseStrandStateBackend
from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

from ingestion.databricks_auth import workspace_client

logger = logging.getLogger(__name__)

_PG_DATABASE = "databricks_postgres"
_ENDPOINT_NAME = os.environ.get(
    "LAKEBASE_ENDPOINT_NAME", "projects/soccer-analytics-dev/branches/production/endpoints/primary"
)


def _warehouse_id() -> str:
    """Warehouse id from DATABRICKS_HTTP_PATH (/sql/1.0/warehouses/<id>)."""
    import re

    m = re.search(r"/warehouses/([a-f0-9]+)$", os.environ.get("DATABRICKS_HTTP_PATH", ""))
    if not m:
        raise RuntimeError("Cannot resolve warehouse id from DATABRICKS_HTTP_PATH for ensure_cdf")
    return m.group(1)


def _make_sql_exec(ws: WorkspaceClient):
    from databricks.sdk.service.sql import StatementState

    warehouse_id = _warehouse_id()

    def _exec(statement: str) -> None:
        resp = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=statement, wait_timeout="30s"
        )
        state = resp.status.state if resp.status else None
        if state != StatementState.SUCCEEDED:
            raise RuntimeError(f"warehouse statement did not succeed ({state}): {statement[:80]}")

    return _exec


def _make_pg_connect(ws: WorkspaceClient):
    host = (ws.config.host or "").rstrip("/")
    resp = requests.post(
        f"{host}/api/2.0/postgres/credentials",
        headers={**ws.config.authenticate(), "Content-Type": "application/json"},  # type: ignore[dict-item]
        json={"endpoint": _ENDPOINT_NAME, "request_id": str(uuid.uuid4())},
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    token = resp.json()["token"]
    payload = json.loads(base64.b64decode(token.split(".")[1] + "==="))
    username = payload["sub"]
    # Derive the endpoint DNS the same way create_indexes / run_lakebase_grants do (ADR-041): no
    # hand-set LAKEBASE_HOST var needed in CI; the env var is honoured only as a local-dev override.
    lakebase_host = derive_lakebase_dns(ws, endpoint_name=_ENDPOINT_NAME)

    def _connect():
        import psycopg2

        return psycopg2.connect(
            host=lakebase_host,
            port=5432,
            dbname=_PG_DATABASE,
            user=username,
            password=token,
            sslmode="require",
            options="-c statement_timeout=30000",
        )

    return _connect


def _discover_stranded(reader: SdkReaderAdapter, catalog: str, schema: str, names: list[str]) -> list[str]:
    """Of the candidate synced-table names, those whose latest pipeline update is the checkpoint mismatch."""
    stranded: list[str] = []
    for name in names:
        fqn = f"{catalog}.{schema}.{name}"
        try:
            pid = reader.get_pipeline_id(fqn)
        except Exception:  # noqa: BLE001 -- missing/unreadable table is not a strand; skip + log
            logger.warning("heal scan: could not resolve pipeline for %s -- skipping", name, exc_info=True)
            continue
        if is_checkpoint_mismatch_failure(reader, pid):
            stranded.append(name)
    return stranded


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    parser = argparse.ArgumentParser(description="Recreate checkpoint-broken synced tables (privileged).")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--tables", default="", help="Comma-separated synced-table names; empty = scan all TRIGGERED")
    args = parser.parse_args()

    enabled = os.environ.get("SYNCED_TABLE_HEAL_ENABLED", "1") == "1"
    if not enabled:
        print("synced-table heal disabled by kill-switch (SYNCED_TABLE_HEAL_ENABLED=0) -- nothing to do")
        return

    ws = workspace_client()
    reader = SdkReaderAdapter(ws)
    sql_exec = _make_sql_exec(ws)  # one warehouse exec, shared by ensure-CDF and strand-state recording
    ports = HealPorts(
        reader=reader,
        writer=SdkWriterAdapter(ws),
        ghost=PsycopgGhostAdapter(_make_pg_connect(ws)),
        warehouse=WarehouseCdfAdapter(sql_exec),
    )

    configs_by_name = {c.name: c for c in SYNCED_TABLES}
    if args.tables.strip():
        requested = [t.strip() for t in args.tables.split(",") if t.strip()]
        for t in requested:
            if not IDENTIFIER_RE.match(t):
                print(f"ERROR: invalid table name {t!r}", file=sys.stderr)
                sys.exit(2)
        candidates = [t for t in requested if t in configs_by_name]
    else:
        candidates = [c.name for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"]

    stranded = _discover_stranded(reader, args.catalog, args.schema, candidates)
    print(f"heal scan: {len(stranded)} stranded of {len(candidates)} candidates: {stranded}")

    # Spark-free: the maintenance runner has no pyspark, so record `healed` via the warehouse (ADR-041).
    state = StrandStateStore(WarehouseStrandStateBackend(sql_exec, args.catalog))
    outcomes = run_heal_pass(
        ports,
        stranded,
        configs_by_name,
        args.catalog,
        args.schema,
        state,
        now=datetime.now(tz=timezone.utc),
        enabled=enabled,
    )
    failed = [n for n, o in outcomes.items() if o.name == "HEAL_FAILED"]
    print(f"heal pass: {outcomes}")
    if failed:
        print(f"ERROR: heal failed for {failed}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
