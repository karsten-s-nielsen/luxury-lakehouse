"""Guards for the Taipy-SP resolution in scripts/run_lakebase_grants.py.

The SP application_id is resolved LIVE via the Databricks SDK (by display name),
NOT via ``terraform output`` — terraform runs only in CI, so a LOCAL app deploy
has no initialized terraform (the false-negative that blocked the 2026-07-09
staging deploy: `terraform output` failed with "provider plugins not installed"
even though the grants were healthy). These tests lock that decoupling and the
display-name anti-drift contract (ADR-005).
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "run_lakebase_grants.py"
_SP_TF = _REPO / "terraform" / "modules" / "service_principals" / "main.tf"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("run_lakebase_grants", _SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fake_sp(app_id: str, name: str) -> MagicMock:
    sp = MagicMock()
    sp.application_id = app_id
    sp.display_name = name
    return sp


def test_display_name_matches_terraform() -> None:
    """The hardcoded display name must equal the terraform ``hf_app`` SP display
    name (with environment=dev). Catches a terraform rename that would silently
    break SDK resolution."""
    mod = _load_module()
    tf = _SP_TF.read_text(encoding="utf-8")
    m = re.search(
        r'resource\s+"databricks_service_principal"\s+"hf_app"\s*\{[^}]*?display_name\s*=\s*"([^"]+)"',
        tf,
        re.DOTALL,
    )
    assert m, "hf_app SP display_name not found in terraform/modules/service_principals/main.tf"
    expected = m.group(1).replace("${var.environment}", "dev")
    assert mod._HF_APP_SP_DISPLAY_NAME == expected


def test_no_terraform_shellout() -> None:
    """The resolver must not shell out to terraform — the whole point of the fix.

    Targets the removed subprocess invocation specifically; a benign doc comment
    ("matches terraform output ``lakebase_endpoint_name``") must not trip it.
    """
    text = _SCRIPT.read_text(encoding="utf-8")
    assert "import subprocess" not in text
    assert "subprocess.run" not in text
    assert '"terraform", "output"' not in text  # the removed cmd = ["terraform", "output", ...]
    assert "_resolve_sp_uuid_from_terraform" not in text


def test_env_override_short_circuits_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_module()
    monkeypatch.setenv(mod._SP_APP_ID_ENV, "  override-uuid-123  ")
    with patch("databricks.sdk.WorkspaceClient") as wc:
        assert mod._resolve_sp_application_id() == "override-uuid-123"
        wc.assert_not_called()


def test_sdk_resolves_unique_match(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_module()
    monkeypatch.delenv(mod._SP_APP_ID_ENV, raising=False)
    fake_ws = MagicMock()
    fake_ws.service_principals.list.return_value = iter([_fake_sp("resolved-123", mod._HF_APP_SP_DISPLAY_NAME)])
    with patch("databricks.sdk.WorkspaceClient", return_value=fake_ws):
        assert mod._resolve_sp_application_id() == "resolved-123"


def test_sdk_rejects_ambiguous_or_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero or multiple matches is a hard error — never a silent guess."""
    mod = _load_module()
    monkeypatch.delenv(mod._SP_APP_ID_ENV, raising=False)
    name = mod._HF_APP_SP_DISPLAY_NAME
    for matches in ([], [_fake_sp("a", name), _fake_sp("b", name)]):
        fake_ws = MagicMock()
        fake_ws.service_principals.list.return_value = iter(matches)
        with patch("databricks.sdk.WorkspaceClient", return_value=fake_ws), pytest.raises(RuntimeError):
            mod._resolve_sp_application_id()
