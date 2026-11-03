"""Unit tests for `scripts/check_hf_token.py`, the HF token scope verifier (impl review B HFB-IMPL-07).

The script is loaded by path (it lives in `scripts/`, not the `lakehouse` package). The tests cover the pure logic:
`_probe` status classification, `_token_meta` parsing, and `_evaluate` per role — the pre-fix `_ok` wrongly counted
any exception (a 5xx/timeout) as a denial, which these pin against.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

V2_ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("v2_check_hf_token", V2_ROOT / "scripts" / "check_hf_token.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


chk = _load()


class _Resp:
    def __init__(self, code: int) -> None:
        self.status_code = code


class _HttpError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"HTTP {code}")
        self.response = _Resp(code)


def _raises(exc: Exception) -> Any:
    def _f() -> None:
        raise exc

    return _f


def test_probe_classifies_by_status() -> None:
    assert chk._probe(lambda: None) == ("readable", "ok")
    for code in (401, 403, 404):
        assert chk._probe(_raises(_HttpError(code)))[0] == "denied"
    # a 5xx, a timeout or a network failure is INCONCLUSIVE, never a denial (the pre-fix `_ok` got this wrong).
    assert chk._probe(_raises(_HttpError(503)))[0] == "error"
    assert chk._probe(_raises(TimeoutError("slow")))[0] == "error"


class _FakeApi:
    def __init__(self, access_token: dict[str, Any]) -> None:
        self._access_token = access_token

    def whoami(self) -> dict[str, Any]:
        return {"auth": {"accessToken": self._access_token}}


def test_token_meta_reports_role_and_scope() -> None:
    meta, lines = chk._token_meta(_FakeApi({"displayName": "t", "role": "fineGrained", "fineGrained": {"scoped": []}}))
    assert meta["role"] == "fineGrained"
    assert any("role='fineGrained'" in line for line in lines)


def _res(**kw: str) -> dict[str, tuple[str, str]]:
    return {name: (state, "") for name, state in kw.items()}


def test_evaluate_space_pass() -> None:
    results = _res(lake_read="readable", lake_write="readable", restricted_read="denied", control_read="denied")
    assert chk._evaluate("space", results, fine_grained=True) == []


def test_evaluate_space_fails_on_readable_restricted() -> None:
    results = _res(lake_read="readable", lake_write="readable", restricted_read="readable", control_read="denied")
    assert any("restricted" in f for f in chk._evaluate("space", results, fine_grained=True))


def test_evaluate_space_fails_on_non_fine_grained() -> None:
    results = _res(lake_read="readable", lake_write="readable", restricted_read="denied", control_read="denied")
    assert any("fine-grained" in f for f in chk._evaluate("space", results, fine_grained=False))


def test_evaluate_space_read_only_write_skipped_passes() -> None:
    results = _res(lake_read="readable", lake_write="skipped", restricted_read="denied", control_read="denied")
    assert chk._evaluate("space", results, fine_grained=True) == []


def test_evaluate_job_requires_bucket_writes_and_control_read() -> None:
    ok = _res(lake_write="readable", restricted_write="readable", control_read="readable")
    assert chk._evaluate("job", ok, fine_grained=True) == []
    bad = _res(lake_write="readable", restricted_write="denied", control_read="readable")
    assert any("restricted" in f for f in chk._evaluate("job", bad, fine_grained=True))


def test_evaluate_jobs_requires_scheduled_job_read() -> None:
    """The job-control token (`LAKEHOUSE_HF_JOBS_TOKEN`) passes when it can read the scheduled job and fails (naming
    the classic-write-role fix) when it cannot — the credential that produced the Settings 403."""
    assert chk._evaluate("jobs", {"job_read": ("readable", "ok")}, fine_grained=False) == []
    fails = chk._evaluate("jobs", {"job_read": ("denied", "HTTP 403")}, fine_grained=True)
    assert any("scheduled job" in f and "write-role" in f for f in fails)


def test_verdict_inconclusive_exits_2() -> None:
    results = _res(lake_read="error", lake_write="readable", restricted_read="denied", control_read="denied")
    code, _ = chk._verdict("space", results, fine_grained=True)
    assert code == 2  # an inconclusive probe (5xx/timeout) exits 2 — never a pass, never a denial


def test_verdict_pass_and_fail() -> None:
    ok = _res(lake_read="readable", lake_write="readable", restricted_read="denied", control_read="denied")
    assert chk._verdict("space", ok, fine_grained=True) == (0, [])
    bad = _res(lake_read="readable", lake_write="readable", restricted_read="readable", control_read="denied")
    code, lines = chk._verdict("space", bad, fine_grained=True)
    assert code == 1
    assert lines
