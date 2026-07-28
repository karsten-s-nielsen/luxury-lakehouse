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


def _sweep_leaked_event_logs(ws: Any) -> None:
    """Drop the ``event_log_<pipeline_uuid>`` tables DLT leaves behind. Best-effort.

    ``sdk_delete`` removes the synced table and its DLT pipeline, but the event-log table
    DLT provisioned in this schema OUTLIVES the pipeline -- one orphan per run, forever.
    Found 2026-07-28: ``event_log_9548d8c1_cace_47d9_b6e3_a0d7543ca254`` from the 07-20 run
    still sat here while its pipeline returned ``ResourceDoesNotExist``.

    Swept, not targeted by pipeline id: the id is unresolvable in the teardown path where
    the synced table was never created, and this schema is disposable and single-purpose,
    so any ``event_log_*`` in it is by definition this test's litter. The workflow
    serialises runs (concurrency group) and these tests run sequentially, so no live
    pipeline owns one at teardown time.

    NOT to be confused with ``scripts/fix_event_log_ownership.py``, the other ``event_log_*``
    handler: that one REPAIRS OWNERSHIP of live production pipelines' event logs (conventions.md
    -> Lakebase Ops). This one DELETES dead ones, and only ever inside the disposable
    ``HEAL_E2E_SCHEMA``. The two never touch the same tables.

    Best-effort by design: this runs inside a ``finally``, where a raised exception would
    REPLACE the assertion that actually failed. Logged at ERROR (never warning, per
    ADR-002) so a persistent sweep failure is visible in error-log queries rather than
    accumulating silently -- which is precisely how the orphan above went unnoticed.
    """
    try:
        for t in ws.tables.list(catalog_name=_CATALOG, schema_name=_SCHEMA):
            if (t.name or "").startswith("event_log_"):
                ws.tables.delete(full_name=f"{_CATALOG}.{_SCHEMA}.{t.name}")
                print(f"swept leaked DLT event log: {t.name}")
    # Broad by intent: ANY failure here (auth, listing, delete race) must yield to the
    # assertion this finally is unwinding from.
    except Exception as exc:
        print(f"ERROR: event-log sweep failed ({type(exc).__name__}: {exc}); orphan may remain")


# Pipeline-update terminal state (databricks.sdk UpdateInfoState value) used by the repro guard.
_UPDATE_COMPLETED = "COMPLETED"


def _assert_replace_converges_healing_if_stranded(
    *,
    ws: Any,
    reader: Any,
    fqn: str,
    pipeline_id: str,
    catalog: str,
    schema: str,
    table: str,
    cfg: Any,
    heal_ports: Any,
    expected_ids: list[int],
    deadline_s: int = 1800,
    poll_s: int = 20,
) -> None:
    """Converge-or-heal contract for the T-mechanism (ADR-043 amendment 2, 2026-06-10).

    The platform contract CHANGED on 2026-06-10 (Databricks rollout): a plain
    ``CREATE OR REPLACE`` of a CDF source now STRANDS its TRIGGERED synced table with the
    XXKST checkpoint mismatch (proven live: e2e run 27287508318 caught it in 44s; the
    supervised production cycle on fct_pausa_values stranded + HEALED the same afternoon).
    So "strand-free" is no longer the assertable claim. The durable invariant is:

      a T-mart rebuild + refresh ends with a CONVERGED, ONLINE synced table,
      with the ADR-041 heal as the sanctioned recovery mechanism.

    Behaviour:
      - strand signal fires (``is_checkpoint_mismatch_failure``, ~1-2 min) -> run the REAL
        ``heal_synced_table`` and assert HEALED, then keep polling for convergence;
      - PG row set converges to the replaced source's rows -> PASS. If it converged WITHOUT
        needing the heal, print LOUDLY — that means the platform reverted to pre-2026-06-10
        strand-free behaviour and ADR-043 amendment 2 should be re-characterised;
      - terminal failure status without the strand signal, heal failure, or deadline -> FAIL
        with the full state/row timeline.

    The PG read tolerates a vanished relation mid-poll: the heal's delete->recreate window
    briefly drops the PG table — that is the heal working, not a failure.
    """
    from ingestion.heal_synced_tables import _make_pg_connect
    from ingestion.synced_table_heal import HealOutcome, heal_synced_table, is_checkpoint_mismatch_failure

    pg_connect = _make_pg_connect(ws)
    start = time.monotonic()
    timeline: list[str] = []
    healed = False
    while True:
        elapsed = int(time.monotonic() - start)
        try:
            state = reader.get_synced_table_status(fqn)
        except Exception as exc:  # transient control-plane timeout (DeadlineExceeded) — keep polling
            state = f"<unreadable: {type(exc).__name__}>"
        ids: list[int] | str
        try:
            conn = pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT id FROM {schema}."{table}" ORDER BY id')
                    ids = [int(r[0]) for r in cur.fetchall()]
            finally:
                conn.close()
        # heal's delete->recreate window drops the PG relation / transient connect — keep polling
        except Exception as exc:
            ids = f"<unreadable: {type(exc).__name__}>"
        timeline.append(f"t+{elapsed}s state={state} healed={healed} pg_ids={ids}")
        if ids == expected_ids:
            mode = "via heal (current platform contract)" if healed else "WITHOUT heal"
            print(f"T-mechanism converged {mode}:\n  " + "\n  ".join(timeline))
            if not healed:
                print(
                    "NOTE: CREATE OR REPLACE no longer stranded — the platform appears to have REVERTED "
                    "to pre-2026-06-10 strand-free behaviour. Re-characterise ADR-043 amendment 2."
                )
            return
        if not healed and is_checkpoint_mismatch_failure(reader, pipeline_id):
            timeline.append(f"t+{elapsed}s STRAND detected (XXKST) -> invoking heal_synced_table")
            outcome = heal_synced_table(heal_ports, cfg, catalog, schema)
            timeline.append(f"t+{int(time.monotonic() - start)}s heal outcome={outcome.name}")
            if outcome is not HealOutcome.HEALED:
                pytest.fail(
                    f"strand detected but heal returned {outcome.name} — the ADR-041 recovery path is "
                    "broken for the T-mechanism.\n  " + "\n  ".join(timeline),
                    pytrace=False,
                )
            healed = True
        elif not healed and state in ("SYNCED_TABLE_OFFLINE", "SYNCED_TABLE_OFFLINE_FAILED"):
            pytest.fail(
                f"synced table reached terminal failure state {state} WITHOUT the strand signal.\n  "
                + "\n  ".join(timeline),
                pytrace=False,
            )
        if elapsed > deadline_s:
            pytest.fail(
                f"PG rows never converged to {expected_ids} within {deadline_s}s (healed={healed}).\n  "
                + "\n  ".join(timeline),
                pytrace=False,
            )
        time.sleep(poll_s)


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
        _sweep_leaked_event_logs(ws)


def test_d_mechanism_delete_insert_keeps_synced_online() -> None:
    """Positive proof (ADR-043): the D re-derive mechanism — in-place DELETE + INSERT on the
    SAME source table (no DROP, no CREATE OR REPLACE -> table id unchanged) followed by one
    incremental CDF refresh — keeps the TRIGGERED synced table SYNCED_TABLE_ONLINE and converges
    row counts. This is the data-plane guarantee that the D path cannot strand.

    The negative half (a new-id source overwrite DOES strand) is locked by
    test_heal_resets_checkpoint_and_resumes_incremental_cdf above. This harness never runs dbt,
    so the on-run-start tripwire is not exercised here (it is proven in the offline dbt-compile
    path); this test proves only the data-plane mechanism.
    """
    from databricks.sdk import WorkspaceClient

    from ingestion.heal_synced_tables import _make_pg_connect, _make_sql_exec
    from ingestion.refresh_synced_tables import SyncedTableConfig
    from ingestion.synced_table_lifecycle import PsycopgGhostAdapter, SdkReaderAdapter, SdkWriterAdapter

    ws = WorkspaceClient()
    sql = _make_sql_exec(ws)
    reader = SdkReaderAdapter(ws)
    writer = SdkWriterAdapter(ws)
    # m1: unique throwaway names so this test never collides with the heal test under pytest-xdist.
    src_name = "fct_heal_e2e_d_src"
    synced_name = "fct_heal_e2e_d_src_synced"
    cfg = SyncedTableConfig(synced_name, src_name, ("id",), "TRIGGERED", schema_override=_SCHEMA)
    fqn = f"{_CATALOG}.{_SCHEMA}.{synced_name}"
    src = f"{_CATALOG}.{_SCHEMA}.{src_name}"

    try:
        sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
        sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        sql(f"INSERT INTO {src} VALUES (1), (2), (3)")
        writer.create_synced_table(cfg, _CATALOG, _SCHEMA)
        assert writer.wait_until_online(fqn, timeout_s=900) == "SYNCED_TABLE_ONLINE"

        # Commit a streaming offset (the precondition that makes an overwrite strand).
        pid = reader.get_pipeline_id(fqn)
        sql(f"INSERT INTO {src} VALUES (4)")
        writer.trigger_refresh(pid)
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        # D mechanism: in-place DELETE + re-INSERT on the SAME table (no DROP/REPLACE).
        sql(f"DELETE FROM {src} WHERE id IN (2, 4)")
        sql(f"INSERT INTO {src} VALUES (2), (4), (5)")
        writer.trigger_refresh(reader.get_pipeline_id(fqn))
        # Must stay ONLINE (no strand) — the table id never changed.
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        conn = _make_pg_connect(ws)()
        try:
            with conn.cursor() as cur:
                # B-2 fix: query synced_name (this test's table), NOT the module _SYNCED (the heal test's).
                cur.execute(f'SELECT count(*) FROM {_SCHEMA}."{synced_name}"')
                row = cur.fetchone()
                assert row is not None and row[0] == 5, "D-mechanism CDF did not converge row count (1,2,3,4,5)"
        finally:
            conn.close()
    finally:
        writer.sdk_delete(fqn)
        PsycopgGhostAdapter(_make_pg_connect(ws)).drop_pg_ghost(_SCHEMA, synced_name)
        sql(f"DROP TABLE IF EXISTS {src}")
        _sweep_leaked_event_logs(ws)


def test_t_mechanism_create_or_replace_converges_healing_if_stranded() -> None:
    """T-mechanism contract (ADR-043 amendment 2): a plain `CREATE OR REPLACE TABLE … AS SELECT`
    (what dbt's `table` materialization emits for fct_pausa_values) followed by a
    triggered refresh ends with a CONVERGED, ONLINE synced table — with the ADR-041 heal as the
    sanctioned recovery when the refresh strands.

    HISTORY: until 2026-06-10 this test asserted "create-or-replace is strand-free" (same Delta id →
    stream survives). A Databricks rollout that day broke the assumption — the refresh now fails with
    the XXKST checkpoint mismatch (e2e run 27287508318; supervised production cycle on
    fct_pausa_values stranded + HEALED the same afternoon). The test now asserts the durable
    invariant and LOUDLY reports if the platform reverts to strand-free (re-characterise the ADR).

    Contrast with test_heal_resets_checkpoint_and_resumes_incremental_cdf, which uses DROP+CREATE (a
    NEW table id) to reproduce the strand the heal was originally built for.
    """
    from databricks.sdk import WorkspaceClient

    from ingestion.heal_synced_tables import _make_pg_connect, _make_sql_exec
    from ingestion.refresh_synced_tables import SyncedTableConfig
    from ingestion.synced_table_heal import HealPorts
    from ingestion.synced_table_lifecycle import (
        PsycopgGhostAdapter,
        SdkReaderAdapter,
        SdkWriterAdapter,
        WarehouseCdfAdapter,
    )

    ws = WorkspaceClient()
    sql = _make_sql_exec(ws)
    reader = SdkReaderAdapter(ws)
    writer = SdkWriterAdapter(ws)
    src_name = "fct_heal_e2e_t_src"
    synced_name = "fct_heal_e2e_t_src_synced"
    cfg = SyncedTableConfig(synced_name, src_name, ("id",), "TRIGGERED", schema_override=_SCHEMA)
    fqn = f"{_CATALOG}.{_SCHEMA}.{synced_name}"
    src = f"{_CATALOG}.{_SCHEMA}.{src_name}"

    try:
        sql(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_SCHEMA}")
        sql(f"CREATE OR REPLACE TABLE {src} (id BIGINT) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        sql(f"INSERT INTO {src} VALUES (1), (2), (3)")
        writer.create_synced_table(cfg, _CATALOG, _SCHEMA)
        assert writer.wait_until_online(fqn, timeout_s=900) == "SYNCED_TABLE_ONLINE"

        # Commit a streaming offset (the precondition that makes a NEW-id overwrite strand).
        pid = reader.get_pipeline_id(fqn)
        sql(f"INSERT INTO {src} VALUES (4)")
        writer.trigger_refresh(pid)
        assert writer.wait_until_online(fqn, timeout_s=600) == "SYNCED_TABLE_ONLINE"

        # T mechanism: atomic CREATE OR REPLACE TABLE ... AS SELECT (same id, full replace) — exactly
        # what dbt's table materialization does for the fct_pausa_values table mart. Contract: CONVERGE, healing if
        # stranded (the current platform strands this — ADR-043 amendment 2).
        sql(
            f"CREATE OR REPLACE TABLE {src} TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') "
            "AS SELECT * FROM VALUES (1),(2),(3),(4),(5) AS t(id)"
        )
        pid_replace = reader.get_pipeline_id(fqn)
        writer.trigger_refresh(pid_replace)
        _assert_replace_converges_healing_if_stranded(
            ws=ws,
            reader=reader,
            fqn=fqn,
            pipeline_id=pid_replace,
            catalog=_CATALOG,
            schema=_SCHEMA,
            table=synced_name,
            cfg=cfg,
            heal_ports=HealPorts(
                reader=reader,
                writer=writer,
                ghost=PsycopgGhostAdapter(_make_pg_connect(ws)),
                warehouse=WarehouseCdfAdapter(sql),
            ),
            expected_ids=[1, 2, 3, 4, 5],
        )
    finally:
        writer.sdk_delete(fqn)
        PsycopgGhostAdapter(_make_pg_connect(ws)).drop_pg_ghost(_SCHEMA, synced_name)
        sql(f"DROP TABLE IF EXISTS {src}")
        _sweep_leaked_event_logs(ws)
