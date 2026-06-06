"""Serverless e2e for synced-table checkpoint self-heal — the ONLY test that exercises the live
destructive path (PG ghost drop, CDF ensure, real checkpoint reset). Asserts the risk dimensions
(spec M3/M6), not just "comes online".

SCAFFOLDING / GATED: requires a live Databricks workspace (SDK + warehouse + Lakebase PG) and the
``RUN_SERVERLESS_TESTS=1`` opt-in. Skipped in normal offline CI; run nightly / on PRs touching
``synced_table_lifecycle`` or ``synced_table_heal`` via .github/workflows/synced-table-heal-e2e.yml.

Decoupled from dbt (review R2): the failure trigger is *only* a new source-table id, so the test
reproduces it with plain ``CREATE OR REPLACE TABLE`` — no dbt build / UC-catalog-creation. The proof
is about the heal, not about dbt.

Required env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH (warehouse), LAKEBASE_HOST.
Fill the ``<TEST_CATALOG>`` / ``<TEST_SCHEMA>`` markers with a disposable catalog/schema.
"""

from __future__ import annotations

import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_SERVERLESS_TESTS") != "1",
    reason="requires a live Databricks workspace + warehouse + Lakebase PG (RUN_SERVERLESS_TESTS=1)",
)

_CATALOG = os.environ.get("HEAL_E2E_CATALOG", "soccer_analytics")
_SCHEMA = os.environ.get("HEAL_E2E_SCHEMA", "<TEST_SCHEMA>")
_SOURCE = "fct_heal_e2e_src"
_SYNCED = "fct_heal_e2e_src_synced"


def test_heal_resets_checkpoint_and_resumes_incremental_cdf() -> None:
    from databricks.sdk import WorkspaceClient

    from ingestion.heal_synced_tables import _make_pg_connect, _make_sql_exec
    from ingestion.refresh_synced_tables import SyncedTableConfig
    from ingestion.synced_table_heal import HealOutcome, HealPorts, heal_synced_table, is_checkpoint_mismatch_failure
    from ingestion.synced_table_lifecycle import (
        PsycopgGhostAdapter,
        SdkReaderAdapter,
        SdkWriterAdapter,
        WarehouseCdfAdapter,
    )

    ws = WorkspaceClient()
    sql = _make_sql_exec(ws)
    reader = SdkReaderAdapter(ws)
    ports = HealPorts(
        reader=reader,
        writer=SdkWriterAdapter(ws),
        ghost=PsycopgGhostAdapter(_make_pg_connect(ws)),
        warehouse=WarehouseCdfAdapter(sql),
    )
    cfg = SyncedTableConfig(_SYNCED, _SOURCE, ("id",), "TRIGGERED", schema_override=_SCHEMA)
    fqn = f"{_CATALOG}.{_SCHEMA}.{_SYNCED}"
    src = f"{_CATALOG}.{_SCHEMA}.{_SOURCE}"

    # (a) seed a CDF-enabled source + its synced table.
    sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    sql(f"INSERT INTO {src} VALUES (1), (2), (3)")
    ports.writer.create_synced_table(cfg, _CATALOG, _SCHEMA)
    assert ports.writer.wait_until_online(fqn, timeout_s=900) == "SYNCED_TABLE_ONLINE"

    # (b) DROP+CREATE the source to mint a NEW Delta table id -> reproduce the checkpoint mismatch.
    sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    sql(f"INSERT INTO {src} VALUES (1), (2), (3), (4)")
    pid = reader.get_pipeline_id(fqn)
    ports.writer.trigger_refresh(pid)
    deadline = time.monotonic() + 600
    while reader.get_synced_table_status(fqn) != "SYNCED_TABLE_ONLINE_PIPELINE_FAILED":
        assert time.monotonic() < deadline, "source-recreate did not produce a pipeline failure"
        time.sleep(15)
    assert is_checkpoint_mismatch_failure(reader, pid), "failure is not the XXKST checkpoint mismatch"

    # (c) heal it, (d) assert HEALED + a FRESH full sync (4 rows, not an 'already exists' no-op).
    assert heal_synced_table(ports, cfg, _CATALOG, _SCHEMA) is HealOutcome.HEALED
    conn = _make_pg_connect(ws)()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM {_SCHEMA}."{_SYNCED}"')
            row = cur.fetchone()
            assert row is not None and row[0] == 4, "checkpoint not reset to a fresh full sync of the recreated source"
    finally:
        conn.close()

    # (e) incremental CDF still works after the heal (go-forward path — L7).
    sql(f"INSERT INTO {src} VALUES (5)")
    ports.writer.trigger_refresh(reader.get_pipeline_id(fqn))
    ports.writer.wait_until_online(fqn, timeout_s=600)
    conn = _make_pg_connect(ws)()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM {_SCHEMA}."{_SYNCED}"')
            row = cur.fetchone()
            assert row is not None and row[0] == 5, "incremental CDF sync did not propagate the appended row"
    finally:
        conn.close()
        ports.writer.sdk_delete(fqn)
        ports.ghost.drop_pg_ghost(_SCHEMA, _SYNCED)
        sql(f"DROP TABLE IF EXISTS {src}")
