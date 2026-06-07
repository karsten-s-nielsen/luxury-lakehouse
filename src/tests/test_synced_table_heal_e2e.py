"""Serverless e2e for synced-table checkpoint self-heal — the ONLY test that exercises the live
destructive path (PG ghost drop, CDF ensure, real checkpoint reset). Asserts the risk dimensions
(spec M3/M6), not just "comes online".

SCAFFOLDING / GATED: requires a live Databricks workspace (SDK + warehouse + Lakebase PG) and the
``RUN_SERVERLESS_TESTS=1`` opt-in. Skipped in normal offline CI. NOT a PR gate (it drives live DLT +
Lakebase, mutates real infra, and takes several minutes); runs nightly + on-demand via workflow_dispatch
in .github/workflows/synced-table-heal-e2e.yml. The deterministic offline suite is the merge gate.

Decoupled from dbt (review R2): the failure trigger is *only* a new source-table id, so the test
reproduces it with a plain ``DROP TABLE`` + ``CREATE TABLE`` — no dbt build / UC-catalog-creation.
(``CREATE OR REPLACE TABLE`` is NOT enough: it overwrites in place and keeps the table id, so the
stream never sees a different id — a real DROP+CREATE is required.) The proof is about the heal,
not about dbt.

Required env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH (warehouse). The Lakebase host
is derived from the Databricks REST API (ADR-041) — no LAKEBASE_HOST needed. ``HEAL_E2E_SCHEMA`` is
the only operator input (a disposable UC schema, default ``heal_e2e``); the test creates it if absent.

The committed-offset precondition (re-characterised live 2026-06-07): a source DROP+CREATE strands the
stream with ``DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`` / SQLSTATE ``XXKST`` **only if the synced
table already has a committed streaming offset** — i.e. at least one *incremental* CDF sync has run
after the initial snapshot (which every real synced table has by the time a ``dbt --full-refresh`` hits
it). An initial-snapshot-only table is instead re-snapshotted cleanly by DLT on a source-id change (the
update COMPLETEs), so phase (a2) below does one incremental sync to commit an offset before the recreate.

Detection: DLT retries the failed update with growing backoff (~6 attempts, "failed more than 2 times")
before the synced-table status settles to ``SYNCED_TABLE_ONLINE_PIPELINE_FAILED`` — that can take ~13
min. So the strand is detected the same way the heal's own preflight detects it: poll
``is_checkpoint_mismatch_failure`` (True within ~1-2 min of the first update failing), NOT by waiting for
the PIPELINE_FAILED status. The guard only fires if the recreate COMPLETEs *despite* the committed offset
— a genuine DLT behaviour change (NOT a heal-code regression), worth re-characterising.

Teardown is in an outer ``finally`` so a repro-timeout / guard-fail never leaks the throwaway synced
table + its DLT pipeline into the next run (a leaked, already-online pipeline gets reused and re-snapshots
cleanly, silently poisoning every subsequent run).
"""

from __future__ import annotations

import os
import time
from typing import Any

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_SERVERLESS_TESTS") != "1",
    reason="requires a live Databricks workspace + warehouse + Lakebase PG (RUN_SERVERLESS_TESTS=1)",
)

_CATALOG = os.environ.get("HEAL_E2E_CATALOG", "soccer_analytics")
_SCHEMA = os.environ.get("HEAL_E2E_SCHEMA", "heal_e2e")
_SOURCE = "fct_heal_e2e_src"
_SYNCED = "fct_heal_e2e_src_synced"

# Pipeline-update terminal state (databricks.sdk UpdateInfoState value) used by the repro guard.
_UPDATE_COMPLETED = "COMPLETED"


def _start_update_and_id(ws: Any, pipeline_id: str) -> str:
    """Trigger a refresh and return the update_id. The production ``trigger_refresh`` port
    intentionally discards the id (it only needs fire-and-forget + active-update tolerance); the e2e
    needs the id to watch *that specific* update's terminal state for the repro guard below."""
    resp = ws.pipelines.start_update(pipeline_id=pipeline_id)
    uid = getattr(resp, "update_id", None)
    if not uid:
        raise AssertionError(f"start_update returned no update_id for pipeline {pipeline_id}")
    return uid


def _update_terminal_state(ws: Any, pipeline_id: str, update_id: str) -> str | None:
    """Canonical state of a single pipeline update (``CREATED``/``WAITING_FOR_RESOURCES``/``RUNNING``/
    ``COMPLETED``/``FAILED``/...), or ``None`` if unreadable. Used to distinguish a genuine repro
    (``FAILED``) from the new auto-recover behaviour (``COMPLETED``)."""
    upd = ws.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = getattr(getattr(upd, "update", None), "state", None)
    return getattr(state, "value", state) if state is not None else None


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

    # Teardown lives in an OUTER finally: a repro-timeout or guard-fail in phase (b) must NOT leak the
    # throwaway synced table + its DLT pipeline. A leaked, already-online pipeline gets REUSED by the
    # next run (create -> "edit settings") and then a source-recreate re-snapshots cleanly instead of
    # stranding, silently poisoning every subsequent run's repro.
    try:
        # (a) seed a CDF-enabled source + its synced table (disposable schema created if absent).
        sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
        sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        sql(f"INSERT INTO {src} VALUES (1), (2), (3)")
        ports.writer.create_synced_table(cfg, _CATALOG, _SCHEMA)
        assert ports.writer.wait_until_online(fqn, timeout_s=900) == "SYNCED_TABLE_ONLINE"

        # (a2) commit a streaming offset: one INCREMENTAL CDF sync after the initial snapshot. Without
        # this, a source-id change is re-snapshotted cleanly by DLT (the update COMPLETEs) and nothing
        # strands — only a committed offset turns a new source id into the XXKST mismatch (the state
        # every real synced table is in when a dbt --full-refresh recreates its source). See module docstring.
        pid = reader.get_pipeline_id(fqn)
        sql(f"INSERT INTO {src} VALUES (4)")
        ports.writer.trigger_refresh(pid)
        assert ports.writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        # (b) DROP then CREATE the source to mint a genuinely NEW Delta table id -> reproduce the
        # checkpoint mismatch the same way a dbt --full-refresh does. NB: `CREATE OR REPLACE TABLE`
        # overwrites the data in place and KEEPS the table id, so it does NOT reproduce the mismatch
        # (empirically confirmed: a create-or-replace recreate never failed the stream). A real DROP +
        # CREATE is required to get a new id and the DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE error.
        sql(f"DROP TABLE IF EXISTS {src}")
        sql(f"CREATE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        sql(f"INSERT INTO {src} VALUES (1), (2), (3), (4)")
        # Capture the update_id so the guard can watch THIS update's terminal state.
        update_id = _start_update_and_id(ws, pid)
        # Detect the strand the way the heal's preflight does — is_checkpoint_mismatch_failure goes True
        # within ~1-2 min of the first update failing. Do NOT wait for SYNCED_TABLE_ONLINE_PIPELINE_FAILED:
        # DLT retries with growing backoff and that status can lag ~13 min behind the first failure.
        deadline = time.monotonic() + 900
        while not is_checkpoint_mismatch_failure(reader, pid):
            # Guard: if OUR triggered update COMPLETEs cleanly despite the committed offset, the strand no
            # longer reproduces. Fail FAST + loud (not a 900s silent timeout) — this is a genuine Databricks
            # DLT behaviour change, NOT a heal-code regression (the ADR-041 heal path is prod-validated).
            if _update_terminal_state(ws, pid, update_id) == _UPDATE_COMPLETED:
                pytest.fail(
                    "Heal-repro precondition no longer holds: the source DROP+CREATE COMPLETED cleanly even "
                    "though a streaming offset was committed first (phase a2), so it did NOT produce the "
                    "DIFFERENT_DELTA_TABLE / XXKST checkpoint mismatch this proof relies on. That is a genuine "
                    "Databricks DLT behaviour change (NOT a heal-code regression — the heal path is "
                    f"prod-validated); the strand mode must be re-characterised. (pipeline={pid}, update={update_id})",
                    pytrace=False,
                )
            assert time.monotonic() < deadline, "source-recreate did not produce the checkpoint mismatch within 900s"
            time.sleep(15)

        # (c) heal it, (d) assert HEALED + a FRESH full sync (4 rows, not an 'already exists' no-op).
        assert heal_synced_table(ports, cfg, _CATALOG, _SCHEMA) is HealOutcome.HEALED
        conn = _make_pg_connect(ws)()
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM {_SCHEMA}."{_SYNCED}"')
                row = cur.fetchone()
                assert row is not None and row[0] == 4, "checkpoint not reset to a fresh full sync of recreated source"
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
    finally:
        # Always tear down — even on a repro-timeout / guard-fail / assertion in any phase above.
        ports.writer.sdk_delete(fqn)
        ports.ghost.drop_pg_ghost(_SCHEMA, _SYNCED)
        sql(f"DROP TABLE IF EXISTS {src}")
