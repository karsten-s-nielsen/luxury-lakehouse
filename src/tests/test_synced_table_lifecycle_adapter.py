"""Thin SDK / PG / warehouse adapters: type-guaranteed read-only reader + op wiring (M1/H1/P2/P9)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.synced_table_lifecycle import (
    PsycopgGhostAdapter,
    SdkReaderAdapter,
    SdkWriterAdapter,
    WarehouseCdfAdapter,
)


def _event(update_id: str, *, error: bool):
    d: dict[str, Any] = {"origin": {"update_id": update_id}}
    if error:
        d["error"] = {"exceptions": [{"message": f"boom {update_id} SQLSTATE: XXKST"}]}
    return SimpleNamespace(as_dict=lambda d=d: d)


def test_reader_adapter_has_no_destructive_methods() -> None:
    a = SdkReaderAdapter(MagicMock())
    for destructive in ("sdk_delete", "drop_pg_ghost", "ensure_cdf", "create_synced_table", "trigger_refresh"):
        assert not hasattr(a, destructive), f"Reader adapter must not expose {destructive} (P2)"


def test_get_pipeline_id_reads_status() -> None:
    ws = MagicMock()
    ws.postgres.get_synced_table.return_value = SimpleNamespace(status=SimpleNamespace(pipeline_id="pid-123"))
    assert SdkReaderAdapter(ws).get_pipeline_id("cat.dev_gold.fct_x_synced") == "pid-123"


def test_latest_failed_events_scopes_to_latest_update() -> None:
    ws = MagicMock()
    ws.pipelines.list_pipeline_events.return_value = [
        _event("u2", error=True),  # newest-first: u2 is latest
        _event("u2", error=False),
        _event("u1", error=True),  # stale history -> excluded (P9)
    ]
    out = SdkReaderAdapter(ws).latest_failed_events("pid")
    assert {e["origin"]["update_id"] for e in out} == {"u2"} and all(e.get("error") for e in out)


def test_writer_sdk_delete_calls_postgres_delete() -> None:
    ws = MagicMock()
    SdkWriterAdapter(ws).sdk_delete("cat.dev_gold.fct_x_synced")
    ws.postgres.delete_synced_table.assert_called_once()


def test_writer_sdk_delete_tolerates_not_found() -> None:
    ws = MagicMock()
    ws.postgres.delete_synced_table.side_effect = RuntimeError("Synced table not found")
    assert SdkWriterAdapter(ws).sdk_delete("cat.dev_gold.fct_x_synced") is False


def test_writer_trigger_refresh_starts_update() -> None:
    ws = MagicMock()
    SdkWriterAdapter(ws).trigger_refresh("pid-9")
    ws.pipelines.start_update.assert_called_once_with(pipeline_id="pid-9")


def test_writer_trigger_refresh_tolerates_active_update_conflict() -> None:
    # A just-(re)created synced table auto-starts its initial sync; start_update then conflicts.
    ws = MagicMock()
    ws.pipelines.start_update.side_effect = RuntimeError(
        "An active update '6f2f0020' already exists for pipeline '3f9e10ed'."
    )
    SdkWriterAdapter(ws).trigger_refresh("pid-9")  # must NOT raise — that sync is the refresh we wanted


def test_writer_trigger_refresh_propagates_other_errors() -> None:
    ws = MagicMock()
    ws.pipelines.start_update.side_effect = RuntimeError("PERMISSION_DENIED on pipeline")
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        SdkWriterAdapter(ws).trigger_refresh("pid-9")


def test_ghost_adapter_executes_drop() -> None:
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    PsycopgGhostAdapter(pg_connect=lambda: conn).drop_pg_ghost("dev_gold", "fct_x_synced")
    assert "DROP TABLE IF EXISTS" in cur.execute.call_args[0][0]


def test_warehouse_adapter_alters_tblproperties() -> None:
    sql = MagicMock()
    WarehouseCdfAdapter(sql_exec=sql).ensure_cdf("cat.dev_gold.fct_x")
    assert "enableChangeDataFeed" in sql.call_args[0][0]
