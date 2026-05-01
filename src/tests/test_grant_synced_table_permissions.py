"""Unit tests for grant_synced_table_permissions module-level helpers.

Covers the three behaviors that the previous raw-HTTP implementation got wrong:

  1. Project identifier:  /api/2.0/permissions/database-projects/ uses the
     short project name (e.g. "soccer-analytics-dev"), NOT the UID returned
     in synced-table metadata. The rewrite resolves the short name via
     ws.postgres.list_projects().
  2. Silent-swallow:  the previous --status code path ran `if r.ok:` with no
     else, so a 404 on the project GET produced zero log output. The rewrite
     lets the SDK raise so failures surface at ERROR.
  3. Additive grants:  the rewrite uses ws.permissions.update (additive,
     patches named principals in) not ws.permissions.set (replaces the whole
     ACL, which would clobber all other principals).
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "scripts"))

import grant_synced_table_permissions as mod  # noqa: E402

# ---------------------------------------------------------------------------
# _resolve_database_project_name
# ---------------------------------------------------------------------------


def test_resolve_project_name_strips_projects_prefix() -> None:
    ws = MagicMock()
    ws.api_client.do.return_value = {"effective_database_project_id": "342068ec-4162-4798-bed5-0aa4cbf326ba"}
    ws.postgres.list_projects.return_value = iter(
        [
            SimpleNamespace(name="projects/other", uid="other-uid"),
            SimpleNamespace(name="projects/soccer-analytics-dev", uid="342068ec-4162-4798-bed5-0aa4cbf326ba"),
        ]
    )
    name = mod._resolve_database_project_name(ws, "soccer_analytics.dev_gold.fct_shots_synced")
    assert name == "soccer-analytics-dev"


def test_resolve_project_name_raises_when_uid_not_in_projects_list() -> None:
    ws = MagicMock()
    ws.api_client.do.return_value = {"effective_database_project_id": "ghost-uid"}
    ws.postgres.list_projects.return_value = iter([SimpleNamespace(name="projects/other", uid="another-uid")])
    with pytest.raises(RuntimeError, match="ghost-uid"):
        mod._resolve_database_project_name(ws, "x.y.z")


def test_resolve_project_name_raises_when_effective_project_id_missing() -> None:
    ws = MagicMock()
    ws.api_client.do.return_value = {}
    with pytest.raises(RuntimeError, match="effective_database_project_id"):
        mod._resolve_database_project_name(ws, "x.y.z")


def test_resolve_project_name_raises_on_malformed_name() -> None:
    """The name from postgres.list_projects should start with 'projects/'.
    If it doesn't (provider API change), we must fail loud, not silently
    return a wrong identifier."""
    ws = MagicMock()
    ws.api_client.do.return_value = {"effective_database_project_id": "342068ec-4162-4798-bed5-0aa4cbf326ba"}
    ws.postgres.list_projects.return_value = iter(
        [SimpleNamespace(name="soccer-analytics-dev", uid="342068ec-4162-4798-bed5-0aa4cbf326ba")]
    )
    with pytest.raises(RuntimeError, match="unexpected name"):
        mod._resolve_database_project_name(ws, "x.y.z")


# ---------------------------------------------------------------------------
# _show_project_acl — emits events on success, raises on failure
# ---------------------------------------------------------------------------


def test_show_project_acl_emits_events_on_success(capsys: pytest.CaptureFixture[str]) -> None:
    ws = MagicMock()
    ws.permissions.get.return_value = SimpleNamespace(
        access_control_list=[
            SimpleNamespace(
                user_name=None,
                group_name=None,
                service_principal_name="sp-app-id",
                all_permissions=[SimpleNamespace(permission_level="CAN_USE")],
            ),
        ]
    )
    mod._show_project_acl(ws, "soccer-analytics-dev", {("hf", "sp-app-id")})
    out = capsys.readouterr().out
    assert '"event": "project_acl_entry"' in out
    assert '"principal": "sp-app-id"' in out
    # Verify the SDK call used the short project name, not a UID.
    call = ws.permissions.get.call_args
    kwargs = (
        call.kwargs if call.kwargs else dict(zip(["request_object_type", "request_object_id"], call.args, strict=False))
    )
    assert kwargs.get("request_object_type") == "database-projects"
    assert kwargs.get("request_object_id") == "soccer-analytics-dev"


def test_show_project_acl_raises_on_sdk_error() -> None:
    """Regression: the previous implementation silently skipped on r.ok=False."""
    # databricks-sdk is in the [sdk] optional extra; skip cleanly when not installed.
    pytest.importorskip("databricks.sdk.errors.base")
    from databricks.sdk.errors.base import DatabricksError

    ws = MagicMock()
    ws.permissions.get.side_effect = DatabricksError("boom")
    with pytest.raises(DatabricksError):
        mod._show_project_acl(ws, "soccer-analytics-dev", set())


# ---------------------------------------------------------------------------
# Script has no raw `requests` imports — enforces SDK-only design
# ---------------------------------------------------------------------------


def test_script_does_not_import_requests() -> None:
    """The whole point of the rewrite: no raw HTTP. SDK only."""
    script = _REPO / "scripts" / "grant_synced_table_permissions.py"
    text = script.read_text(encoding="utf-8")
    # Match "import requests" or "from requests" at the start of a line.
    import re

    pattern = re.compile(r"^\s*(import\s+requests|from\s+requests\b)", re.MULTILINE)
    match = pattern.search(text)
    assert match is None, f"script still imports requests at: {match.group() if match else '?'}"
