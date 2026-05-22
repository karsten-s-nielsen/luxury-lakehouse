"""Tests for scripts/migrate_synced_tables.py — migration script unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_get_warehouse_id_extracts_from_http_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_warehouse_id must extract the warehouse ID from DATABRICKS_HTTP_PATH."""
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc123def456")

    import scripts.migrate_synced_tables as mod

    assert mod._get_warehouse_id() == "abc123def456"


def test_get_warehouse_id_raises_on_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_warehouse_id must raise RuntimeError when env var is missing."""
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

    import scripts.migrate_synced_tables as mod

    with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
        mod._get_warehouse_id()


def test_get_warehouse_id_raises_on_malformed_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_warehouse_id must raise on paths that don't match the expected format."""
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/clusters/abc123")

    import scripts.migrate_synced_tables as mod

    with pytest.raises(RuntimeError, match="Cannot extract warehouse ID"):
        mod._get_warehouse_id()


def test_create_synced_table_uses_typed_sdk_objects() -> None:
    """_create_synced_table must pass typed SyncedTable + SyncedTableSyncedTableSpec."""
    from databricks.sdk.service.postgres import (
        SyncedTable,
        SyncedTableSyncedTableSpec,
    )
    from databricks.sdk.service.postgres import (
        SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedulingPolicy,
    )

    import scripts.migrate_synced_tables as mod
    from ingestion.refresh_synced_tables import SyncedTableConfig

    mock_ws = MagicMock()
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
        scheduling_policy="TRIGGERED",
    )

    mod._create_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")

    mock_ws.postgres.create_synced_table.assert_called_once()
    call_kwargs = mock_ws.postgres.create_synced_table.call_args
    assert call_kwargs.kwargs["synced_table_id"] == "soccer_analytics.dev_gold.fct_test_synced"

    synced_table = call_kwargs.kwargs["synced_table"]
    assert isinstance(synced_table, SyncedTable)
    assert isinstance(synced_table.spec, SyncedTableSyncedTableSpec)
    assert synced_table.spec.source_table_full_name == "soccer_analytics.dev_gold.fct_test"
    assert synced_table.spec.scheduling_policy == SchedulingPolicy.TRIGGERED
    assert synced_table.spec.primary_key_columns == ["test_id"]


def test_delete_synced_table_returns_true_on_success() -> None:
    """_delete_synced_table returns True when deletion succeeds."""
    import scripts.migrate_synced_tables as mod
    from ingestion.refresh_synced_tables import SyncedTableConfig

    mock_ws = MagicMock()
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )

    result = mod._delete_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")
    assert result is True
    mock_ws.postgres.delete_synced_table.assert_called_once_with(
        name="synced_tables/soccer_analytics.dev_gold.fct_test_synced"
    )


def test_delete_synced_table_returns_false_on_not_found() -> None:
    """_delete_synced_table returns False when table doesn't exist."""
    import scripts.migrate_synced_tables as mod
    from ingestion.refresh_synced_tables import SyncedTableConfig

    mock_ws = MagicMock()
    mock_ws.postgres.delete_synced_table.side_effect = Exception("Resource not found")
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )

    result = mod._delete_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")
    assert result is False


def test_delete_synced_table_raises_on_unexpected_error() -> None:
    """_delete_synced_table must propagate non-not-found errors."""
    import scripts.migrate_synced_tables as mod
    from ingestion.refresh_synced_tables import SyncedTableConfig

    mock_ws = MagicMock()
    mock_ws.postgres.delete_synced_table.side_effect = RuntimeError("Permission denied")
    config = SyncedTableConfig(
        name="fct_test_synced",
        source_table="fct_test",
        primary_key_columns=("test_id",),
    )

    with pytest.raises(RuntimeError, match="Permission denied"):
        mod._delete_synced_table(mock_ws, config, "soccer_analytics", "dev_gold")


def test_phase_skip_phase_conflict_skips(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """--phase 0 --skip-phase 0 should skip phase 0 (skip wins), not execute it."""
    import argparse

    import scripts.migrate_synced_tables as mod

    # Mock WorkspaceClient to avoid live API calls
    mock_ws = MagicMock()
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)

    # Patch argparse to return the conflicting args
    monkeypatch.setattr(
        mod.argparse.ArgumentParser,
        "parse_args",
        lambda self, args=None, namespace=None: argparse.Namespace(
            phase=0,
            skip_phase=[0],
            catalog="test",
            schema="test",
        ),
    )

    mod.main()
    captured = capsys.readouterr()
    assert "SKIPPING Phase 0" in captured.out
    # phase_0_smoke_test should NOT have been called
    mock_ws.postgres.create_synced_table.assert_not_called()


def test_fix_event_log_get_pipeline_id_uses_sdk() -> None:
    """fix_event_log_ownership._get_pipeline_id must accept ws kwarg (not host/headers)
    and call ws.postgres.get_synced_table with the synced_tables/ prefix."""
    import scripts.fix_event_log_ownership as mod

    mock_ws = MagicMock()
    mock_status = MagicMock()
    mock_status.pipeline_id = "12345678-1234-1234-1234-123456789abc"
    mock_meta = MagicMock()
    mock_meta.status = mock_status
    mock_ws.postgres.get_synced_table.return_value = mock_meta

    # Must accept ws= keyword (not host=/headers=)
    pipeline_id = mod._get_pipeline_id(
        ws=mock_ws,
        catalog="soccer_analytics",
        schema="dev_gold",
        table="fct_test_synced",
    )
    assert pipeline_id == "12345678-1234-1234-1234-123456789abc"
    mock_ws.postgres.get_synced_table.assert_called_once_with(
        name="synced_tables/soccer_analytics.dev_gold.fct_test_synced"
    )
