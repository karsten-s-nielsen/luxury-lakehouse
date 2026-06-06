"""Lakebase host derivation: override-wins, REST discovery, fail-loud (ADR-041)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion import lakebase_endpoint
from ingestion.lakebase_endpoint import derive_lakebase_dns


def _ws() -> Any:
    ws = MagicMock()
    ws.config.host = "https://example.cloud.databricks.com"
    ws.config.authenticate.return_value = {"Authorization": "Bearer x"}
    return ws


def test_explicit_override_wins_without_network() -> None:
    ws = _ws()
    assert derive_lakebase_dns(ws, override="my-host.db") == "my-host.db"
    ws.config.authenticate.assert_not_called()  # no REST call when overridden


def test_env_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKEBASE_HOST", "env-host.db")
    assert derive_lakebase_dns(_ws()) == "env-host.db"


def test_discovers_single_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKEBASE_HOST", raising=False)
    payload = {"endpoints": [{"status": {"hosts": {"host": "ep-primary.database.us-east-1.cloud.databricks.com"}}}]}
    monkeypatch.setattr(
        lakebase_endpoint.requests,
        "get",
        lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload),
    )
    assert derive_lakebase_dns(_ws()) == "ep-primary.database.us-east-1.cloud.databricks.com"


def test_matches_endpoint_by_suffix_when_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKEBASE_HOST", raising=False)
    payload = {
        "endpoints": [
            {"name": ".../endpoints/secondary", "status": {"hosts": {"host": "ep-secondary.db"}}},
            {"name": ".../endpoints/primary", "status": {"hosts": {"host": "ep-primary.db"}}},
        ]
    }
    monkeypatch.setattr(
        lakebase_endpoint.requests,
        "get",
        lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload),
    )
    assert derive_lakebase_dns(_ws()) == "ep-primary.db"


def test_raises_when_no_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKEBASE_HOST", raising=False)
    monkeypatch.setattr(
        lakebase_endpoint.requests,
        "get",
        lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"endpoints": []}),
    )
    with pytest.raises(RuntimeError, match="No Lakebase endpoints"):
        derive_lakebase_dns(_ws())
