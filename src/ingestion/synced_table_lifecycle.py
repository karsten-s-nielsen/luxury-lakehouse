"""Synced-table lifecycle — thin driven ports + adapters (hexagonal seam, spec M1 / review B).

Four thin, interface-segregated ports so each consumer depends only on what it needs, and so
"detect never destroys" is a *type* guarantee (spec P2): detection depends only on the read port,
so the daily-job service principal — which constructs only ``SdkReaderAdapter`` — has no
destructive method to call by construction.

| Port                     | Tech                | Ops                                                |
|--------------------------|---------------------|----------------------------------------------------|
| ``SyncedTableReaderPort``| databricks-sdk (RO) | get_synced_table_status, get_pipeline_id, latest_failed_events |
| ``SyncedTableWriterPort``| databricks-sdk (RW) | create_synced_table, sdk_delete, trigger_refresh, wait_until_online |
| ``PostgresGhostPort``    | psycopg2            | drop_pg_ghost                                      |
| ``WarehousePort``        | SQL warehouse       | ensure_cdf                                         |

Consumers: detection (refresh, SP) → Reader only; the heal use-case → all four; ``migrate`` →
Writer + Warehouse; ``delete_synced_table`` → Writer + Ghost.

Import-safe offline: databricks-sdk / psycopg2 are imported lazily inside methods.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

# post-0.114.0 (confirmed Task 0b): ws.postgres.{get,create,delete}_synced_table +
# ws.pipelines.{list_pipeline_events,start_update}. This SDK surface moves — verify on bumps (L2).
_BRANCH = "projects/soccer-analytics-dev/branches/production"
_PG_DATABASE = "databricks_postgres"
SYNCED_TABLE_ONLINE_STATE = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"
_ONLINE_OK_STATE = "SYNCED_TABLE_ONLINE"
# States wait_until_online returns from EARLY (no point polling out the full budget).
# SYNCED_TABLE_ONLINE_PIPELINE_FAILED is the strand signature (table still serving, DLT
# pipeline given up): before 2026-06-10 it was absent here, so a genuine strand silently
# burned the whole timeout before being reported instead of returning the moment DLT
# settled on failure (~13 min after the first failed update — ADR-041 re-characterisation).
_TERMINAL_FAILURE_STATES = (
    "SYNCED_TABLE_OFFLINE",
    "SYNCED_TABLE_OFFLINE_FAILED",
    "SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
)
_NOT_FOUND_MARKERS = ("not found", "does not exist")
# A freshly (re)created synced table auto-starts its initial sync, so an explicit start_update races
# with it: "An active update '<id>' already exists for pipeline '<id>'." That in-flight update IS the
# refresh we wanted, so trigger_refresh tolerates this conflict (idempotent) and lets wait_until_online
# observe the result — mirrors the HTTP-409 tolerance in refresh_synced_tables._trigger_refresh.
_ACTIVE_UPDATE_MARKERS = ("active update", "already exists for pipeline")


# --------------------------------------------------------------------------------------------- ports
class SyncedTableReaderPort(Protocol):
    """Read-only SDK ops — the only port the daily-job service principal touches."""

    def get_synced_table_status(self, fqn: str) -> str: ...
    def get_pipeline_id(self, fqn: str) -> str: ...
    def latest_failed_events(self, pipeline_id: str) -> list[dict[str, Any]]: ...


class SyncedTableWriterPort(Protocol):
    """Destructive SDK control-plane ops."""

    def create_synced_table(self, config: Any, catalog: str, schema: str) -> None: ...
    def sdk_delete(self, fqn: str) -> bool: ...
    def trigger_refresh(self, pipeline_id: str) -> None: ...
    def wait_until_online(self, fqn: str, timeout_s: int = 1800) -> str: ...


class PostgresGhostPort(Protocol):
    """The PG "ghost" table left after an SDK delete of a FOREIGN synced table."""

    def drop_pg_ghost(self, schema: str, table: str) -> None: ...


class WarehousePort(Protocol):
    """Warehouse SQL — enabling CDF on a source mart before a TRIGGERED (re)create."""

    def ensure_cdf(self, source_fqn: str) -> None: ...


# ------------------------------------------------------------------------------------------ adapters
class SdkReaderAdapter:
    """``SyncedTableReaderPort`` over the Databricks SDK. Read-only by construction — has no
    delete/create/ghost/CDF method, so a consumer handed this cannot destroy anything (P2)."""

    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    def _meta(self, fqn: str) -> Any:
        return self._ws.postgres.get_synced_table(name=f"synced_tables/{fqn}")  # type: ignore[attr-defined]

    def get_synced_table_status(self, fqn: str) -> str:
        status = getattr(self._meta(fqn), "status", None)
        raw = getattr(status, "detailed_state", None) if status else None
        return raw.value if raw is not None and hasattr(raw, "value") else (str(raw) if raw else "UNKNOWN")

    def get_pipeline_id(self, fqn: str) -> str:
        status = getattr(self._meta(fqn), "status", None)
        pid = getattr(status, "pipeline_id", None) if status else None
        if not pid:
            raise RuntimeError(f"Synced table {fqn} has no pipeline_id in status")
        return pid

    def latest_failed_events(self, pipeline_id: str) -> list[dict[str, Any]]:
        """Error events of the LATEST pipeline update only (spec P9 scoping).

        ``list_pipeline_events`` returns newest-first; the first event's ``origin.update_id`` is the
        latest update. Scoping to it means a stale historical failure can never match the classifier.
        """
        events = [
            e.as_dict() for e in self._ws.pipelines.list_pipeline_events(pipeline_id=pipeline_id, max_results=250)
        ]
        latest_uid = next(
            (e.get("origin", {}).get("update_id") for e in events if e.get("origin", {}).get("update_id")), None
        )
        scoped = [e for e in events if e.get("origin", {}).get("update_id") == latest_uid] if latest_uid else events
        return [e for e in scoped if e.get("error")]


class SdkWriterAdapter:
    """``SyncedTableWriterPort`` over the Databricks SDK (create / delete / trigger / wait)."""

    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    def create_synced_table(self, config: Any, catalog: str, schema: str) -> None:
        from databricks.sdk.service.postgres import SyncedTable, SyncedTableSyncedTableSpec
        from databricks.sdk.service.postgres import (
            SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedulingPolicy,
        )

        eff_schema = config.schema_override or schema
        policy = {"SNAPSHOT": SchedulingPolicy.SNAPSHOT, "TRIGGERED": SchedulingPolicy.TRIGGERED}[
            config.scheduling_policy
        ]
        self._ws.postgres.create_synced_table(  # type: ignore[attr-defined]
            synced_table=SyncedTable(
                spec=SyncedTableSyncedTableSpec(
                    source_table_full_name=f"{catalog}.{eff_schema}.{config.source_table}",
                    branch=_BRANCH,
                    primary_key_columns=list(config.primary_key_columns),
                    scheduling_policy=policy,
                    postgres_database=_PG_DATABASE,
                    create_database_objects_if_missing=True,
                ),
            ),
            synced_table_id=f"{catalog}.{eff_schema}.{config.name}",
        )

    def sdk_delete(self, fqn: str) -> bool:
        try:
            self._ws.postgres.delete_synced_table(name=f"synced_tables/{fqn}")  # type: ignore[attr-defined]
            return True
        except Exception as exc:
            if any(m in str(exc).lower() for m in _NOT_FOUND_MARKERS):
                return False
            raise

    def trigger_refresh(self, pipeline_id: str) -> None:
        try:
            self._ws.pipelines.start_update(pipeline_id=pipeline_id)
        except Exception as exc:
            # The sync we asked for is already running (e.g. a just-created table's initial sync) —
            # treat as a no-op rather than a heal-failing error. Every other failure propagates.
            msg = str(exc).lower()
            if all(m in msg for m in _ACTIVE_UPDATE_MARKERS):
                return
            raise

    def _status(self, fqn: str) -> str:
        meta = self._ws.postgres.get_synced_table(name=f"synced_tables/{fqn}")  # type: ignore[attr-defined]
        status = getattr(meta, "status", None)
        raw = getattr(status, "detailed_state", None) if status else None
        return raw.value if raw is not None and hasattr(raw, "value") else (str(raw) if raw else "UNKNOWN")

    def wait_until_online(self, fqn: str, timeout_s: int = 1800) -> str:
        start = time.monotonic()
        last = "UNKNOWN"
        while True:
            # A poll loop with minutes of budget must not die because ONE poll failed: the
            # control-plane GET intermittently times out (DeadlineExceeded after the SDK's own
            # retries — bit the heal e2e setup AND the maintenance pipeline-id lookup on
            # 2026-06-10). Record the unreadable poll and keep polling; the timeout still bounds.
            try:
                last = self._status(fqn)
            except Exception as exc:  # noqa: BLE001 — transient control-plane read; loop is bounded by timeout_s
                logger.error("wait_until_online: status poll for %s failed (%s) — retrying next cycle", fqn, exc)
                last = f"UNREADABLE: {type(exc).__name__}"
            if last == SYNCED_TABLE_ONLINE_STATE:
                return _ONLINE_OK_STATE
            if last in _TERMINAL_FAILURE_STATES or time.monotonic() - start > timeout_s:
                return last
            time.sleep(15)


class PsycopgGhostAdapter:
    """``PostgresGhostPort`` via psycopg2. ``pg_connect`` is injected (a no-arg callable returning a
    live psycopg2 connection) so this is unit-testable offline."""

    def __init__(self, pg_connect: Callable[[], Any]) -> None:
        self._pg_connect = pg_connect

    def drop_pg_ghost(self, schema: str, table: str) -> None:
        conn = self._pg_connect()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS {schema}."{table}"')  # quoted ident; schema validated upstream
        finally:
            conn.close()


class WarehouseCdfAdapter:
    """``WarehousePort`` — ``sql_exec`` runs a single warehouse SQL statement (injected)."""

    def __init__(self, sql_exec: Callable[[str], None]) -> None:
        self._sql_exec = sql_exec

    def ensure_cdf(self, source_fqn: str) -> None:
        # Idempotent — same statement as the operator Phase 2 (migrate_synced_tables.py).
        self._sql_exec(f"ALTER TABLE {source_fqn} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
