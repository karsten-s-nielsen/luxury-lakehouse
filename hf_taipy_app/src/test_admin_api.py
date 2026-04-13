"""Tests for hf_taipy_app/src/admin_api.py.

Run from inside hf_taipy_app/src so the flat-import paths resolve:
    cd hf_taipy_app/src && python -m pytest test_admin_api.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests
from flask import Flask


@pytest.fixture(autouse=True)
def _reset_cache() -> object:
    """Each test starts and ends with an empty in-memory cache."""
    import cache

    cache._cache.clear()
    yield
    cache._cache.clear()


# --- _validate_hf_admin: input shape ---


def test_validate_missing_header() -> None:
    from admin_api import _validate_hf_admin

    ok, status, msg = _validate_hf_admin(None)

    assert ok is False
    assert status == 401
    assert "missing" in msg.lower() or "malformed" in msg.lower()


def test_validate_no_bearer_prefix() -> None:
    from admin_api import _validate_hf_admin

    ok, status, _ = _validate_hf_admin("Token abc123")

    assert ok is False
    assert status == 401


def test_validate_token_format_invalid() -> None:
    from admin_api import _validate_hf_admin

    ok, status, _ = _validate_hf_admin("Bearer not-a-real-hf-token")

    assert ok is False
    assert status == 401


# --- _validate_hf_admin: HF API responses ---


def _make_resp(status_code: int, payload: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.json = MagicMock(return_value=payload or {})
    return resp


def test_validate_hf_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(401))

    ok, status, msg = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 401
    assert "revoked" in msg.lower() or "invalid" in msg.lower()


def test_validate_hf_returns_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(503))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 503


def test_validate_hf_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    def _raise(*_: object, **__: object) -> None:
        raise requests.Timeout()

    monkeypatch.setattr("admin_api.requests.get", _raise)

    ok, status, msg = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 503
    assert "timeout" in msg.lower()


# --- _validate_hf_admin: org/role checks ---


def test_validate_user_not_in_org(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "stranger",
        "orgs": [{"name": "other-org", "roleInOrg": "admin"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, msg = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 403
    assert "luxury-lakehouse" in msg or "member" in msg.lower()


def test_validate_user_wrong_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "reader",
        "orgs": [{"name": "luxury-lakehouse", "roleInOrg": "read"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 403


def test_validate_user_admin_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "karsten",
        "orgs": [{"name": "luxury-lakehouse", "roleInOrg": "admin"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is True
    assert status == 200


def test_validate_user_write_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "writer",
        "orgs": [{"name": "luxury-lakehouse", "roleInOrg": "write"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is True
    assert status == 200


# --- /api/cache/clear endpoint ---


def test_endpoint_no_auth_returns_401() -> None:
    from admin_api import build_admin_blueprint

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    with app.test_client() as client:
        resp = client.post("/api/cache/clear")

    assert resp.status_code == 401


def test_endpoint_valid_auth_clears_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    import cache
    from admin_api import build_admin_blueprint

    monkeypatch.setattr("admin_api._validate_hf_admin", lambda _h: (True, 200, "karsten"))

    cache._cache["k1"] = (1.0, "v1")
    cache._cache["k2"] = (2.0, "v2")
    assert cache.cache_size() == 2

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    with app.test_client() as client:
        resp = client.post(
            "/api/cache/clear",
            headers={"Authorization": "Bearer hf_xxx"},
        )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["cleared"] is True
    assert body["entries_cleared"] == 2
    assert body["refresh_synced_triggered"] is False
    assert cache.cache_size() == 0


def test_endpoint_with_refresh_synced_triggers_background(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import build_admin_blueprint

    monkeypatch.setattr("admin_api._validate_hf_admin", lambda _h: (True, 200, "karsten"))

    refresh_calls: list[bool] = []
    monkeypatch.setattr(
        "admin_api._trigger_synced_refresh_async",
        lambda: refresh_calls.append(True),
    )

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    with app.test_client() as client:
        resp = client.post(
            "/api/cache/clear?refresh_synced=1",
            headers={"Authorization": "Bearer hf_xxx"},
        )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["cleared"] is True
    assert body["refresh_synced_triggered"] is True
    assert refresh_calls == [True]


def test_endpoint_denied_request_logs_no_token(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Denied requests must log status + reason but never the token value."""
    import logging

    from admin_api import build_admin_blueprint

    monkeypatch.setattr(
        "admin_api._validate_hf_admin",
        lambda _h: (False, 401, "Token invalid or revoked"),
    )

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    secret_token = "hf_should_never_be_logged_xxxxxxxxxxxxxx"  # pragma: allowlist secret
    with caplog.at_level(logging.INFO, logger="admin_api"):
        with app.test_client() as client:
            client.post(
                "/api/cache/clear",
                headers={"Authorization": f"Bearer {secret_token}"},
            )

    # Token must NEVER appear in any log record
    for record in caplog.records:
        assert secret_token not in record.getMessage()
        assert secret_token not in str(record.args or "")
