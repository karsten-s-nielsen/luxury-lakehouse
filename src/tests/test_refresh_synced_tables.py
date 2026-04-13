"""Tests for ingestion.refresh_synced_tables — auth and table list invariants."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest


def test_get_auth_headers_uses_workspace_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_auth_headers must obtain headers from WorkspaceClient.config.authenticate."""
    mock_ws = MagicMock()
    mock_ws.config.authenticate.return_value = {"Authorization": "Bearer test-token-123"}

    monkeypatch.setattr(
        "ingestion.refresh_synced_tables.WorkspaceClient",
        lambda: mock_ws,
    )

    from ingestion.refresh_synced_tables import _get_auth_headers

    headers = _get_auth_headers()

    assert headers == {"Authorization": "Bearer test-token-123"}
    mock_ws.config.authenticate.assert_called_once()


def test_get_auth_headers_does_not_call_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_auth_headers must NOT shell out to the Databricks CLI."""
    mock_ws = MagicMock()
    mock_ws.config.authenticate.return_value = {"Authorization": "Bearer x"}
    monkeypatch.setattr(
        "ingestion.refresh_synced_tables.WorkspaceClient",
        lambda: mock_ws,
    )

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("subprocess.run must not be called from _get_auth_headers")

    monkeypatch.setattr(subprocess, "run", _fail)

    from ingestion.refresh_synced_tables import _get_auth_headers

    _get_auth_headers()  # must not raise


def test_synced_tables_list_has_34_entries() -> None:
    """SYNCED_TABLES drift guard — should match the 34 tables in Terraform."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    assert len(SYNCED_TABLES) == 34


def test_get_host_uses_workspace_client_not_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_host must resolve via WorkspaceClient.config.host, not os.environ.

    Regression test for: daily Databricks job failed with KeyError('DATABRICKS_HOST')
    because the env var is not set in the job runtime even though OAuth M2M auth
    works. WorkspaceClient.config.host handles that case via runtime context.
    """
    import ingestion.refresh_synced_tables as mod

    mock_ws = MagicMock()
    mock_ws.config.host = "https://test.databricks.com"
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)

    assert mod._get_host() == "https://test.databricks.com"


def test_get_host_adds_https_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_host must add https:// prefix when WorkspaceClient returns bare host."""
    import ingestion.refresh_synced_tables as mod

    mock_ws = MagicMock()
    mock_ws.config.host = "test.databricks.com"
    monkeypatch.setattr(mod, "WorkspaceClient", lambda: mock_ws)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)

    assert mod._get_host() == "https://test.databricks.com"


def test_get_host_caches_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_host must cache the result after first lookup (perf: 102 HTTP calls/run)."""
    import ingestion.refresh_synced_tables as mod

    call_count = [0]

    def _counted() -> MagicMock:
        call_count[0] += 1
        m = MagicMock()
        m.config.host = "https://cached.test"
        return m

    monkeypatch.setattr(mod, "WorkspaceClient", _counted)
    monkeypatch.setattr(mod, "_CACHED_HOST", None)

    mod._get_host()
    mod._get_host()
    mod._get_host()

    assert call_count[0] == 1, "WorkspaceClient should be instantiated exactly once"


def test_get_pipeline_id_uses_provided_catalog_and_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_pipeline_id must build the URL from caller-provided catalog/schema, never module state."""
    captured: dict[str, str] = {}

    def mock_get(url: str, **_kwargs: object) -> MagicMock:
        captured["url"] = url
        resp = MagicMock()
        resp.json.return_value = {"data_synchronization_status": {"pipeline_id": "pipe-xyz"}}
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr("ingestion.refresh_synced_tables.requests.get", mock_get)
    # _get_host() reads DATABRICKS_HOST at runtime; mock it to avoid env dependence in CI
    monkeypatch.setattr("ingestion.refresh_synced_tables._get_host", lambda: "https://test.databricks.com")

    from ingestion.refresh_synced_tables import _get_pipeline_id

    pipeline_id = _get_pipeline_id(
        "fct_shots_synced",
        {"Authorization": "Bearer x"},
        catalog="alt_catalog",
        schema="alt_schema",
    )

    assert pipeline_id == "pipe-xyz"
    assert "alt_catalog.alt_schema.fct_shots_synced" in captured["url"]


def test_main_rejects_invalid_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() must exit non-zero when --catalog fails identifier validation (SQL injection guard)."""
    monkeypatch.setattr("sys.argv", ["refresh_synced_tables", "--catalog", "drop; table users--"])

    from ingestion.refresh_synced_tables import main

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2


def test_main_rejects_invalid_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() must exit non-zero when --schema fails identifier validation."""
    monkeypatch.setattr("sys.argv", ["refresh_synced_tables", "--schema", "1invalid"])

    from ingestion.refresh_synced_tables import main

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2
