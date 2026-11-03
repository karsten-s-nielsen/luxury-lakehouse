"""The web server and sign-in (spec §19, §20; plan Task 27): every Ops route needs a signed-in user; HF mode runs an
OAuth code flow with an allow-list; local mode checks one Argon2id operator; sessions are HttpOnly and SameSite=Lax
(Secure in HF mode, dropped for the local LAN install); every state-changing request needs a CSRF token; no secret
reaches a rendered page.

The HF OAuth handshake is injected (`hf_login`), so the allow-list and the session are tested without a real
identity provider; the local path uses the real `AccountService` on a lake that the kit made."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from lakekit import LakeKit

from lakehouse.config.model import load_defaults
from lakehouse.services.account import AccountService
from lakehouse.web.app import create_app

PASSWORD = "correct horse battery staple"  # noqa: S105 - a test value
CLIENT_SECRET = "oauth-client-secret-value-123"  # noqa: S105 - a test value
HF_ENV = {
    "OAUTH_CLIENT_ID": "client-id",
    "OAUTH_CLIENT_SECRET": CLIENT_SECRET,
    "OPENID_PROVIDER_URL": "https://huggingface.co",
    "OAUTH_SCOPES": "openid profile",
    "OPS_ALLOWED_USERS": "alice, karsten",
    "LAKEHOUSE_SESSION_SECRET": "a-long-enough-session-secret-value",
}


def _settings() -> Any:
    return load_defaults("laptop")


def _local_app(kit: LakeKit, *, env: Mapping[str, str] | None = None) -> Any:
    merged = {"LAKEHOUSE_SESSION_SECRET": "a-long-enough-session-secret-value", **(env or {})}
    app = create_app(_settings(), mode="local", accounts=AccountService(kit.storage), env=merged)
    app.testing = True
    return app


def _hf_app(*, hf_login: Any = None, env: Mapping[str, str] | None = None) -> Any:
    app = create_app(_settings(), mode="hf", env={**HF_ENV, **(env or {})}, hf_login=hf_login)
    app.testing = True
    return app


def _client(app: Any) -> Any:
    return app.test_client()


def _csrf(client: Any, path: str = "/signin") -> str:
    page = client.get(path, base_url="https://localhost")
    token = page.headers.get("X-CSRF-Token")
    assert token
    return token


def test_anonymous_redirected_to_signin(kit: LakeKit) -> None:
    client = _client(_local_app(kit))
    reply = client.get("/ops/ping", base_url="https://localhost")
    assert reply.status_code == 302 and "/signin" in reply.headers["Location"]
    signin = client.get("/signin", base_url="https://localhost")
    assert signin.status_code == 200  # the sign-in page itself is open


def test_local_password_argon2(kit: LakeKit) -> None:
    AccountService(kit.storage).create("ops", PASSWORD)
    stored = kit.storage.get("lake", "config/account.json").decode()
    assert "$argon2id$" in stored and PASSWORD not in stored
    client = _client(_local_app(kit))
    token = _csrf(client)
    wrong = client.post(
        "/signin", data={"username": "ops", "password": "wrong", "csrf_token": token}, base_url="https://localhost"
    )
    assert wrong.status_code == 200 and b"did not match" in wrong.data  # stays on the page, no session
    assert client.get("/ops/ping", base_url="https://localhost").status_code == 302
    ok = client.post(
        "/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url="https://localhost"
    )
    assert ok.status_code == 302 and urlsplit(str(ok.headers["Location"])).path == "/"  # Taipy at / (Option 3)
    assert client.get("/ops/ping", base_url="https://localhost").data == b"pong"


def test_global_guard_protects_undecorated_routes(kit: LakeKit) -> None:
    """A route added without `@login_required` (as a Taipy page is in Task 28) is still behind the global guard."""
    app = _local_app(kit)
    app.add_url_rule("/ops/plain", "plain", lambda: "plain")  # no @login_required: only the global guard covers it
    client = _client(app)
    assert client.get("/ops/plain", base_url="https://localhost").status_code == 302
    AccountService(kit.storage).create("ops", PASSWORD)
    token = _csrf(client)
    client.post(
        "/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url="https://localhost"
    )
    assert client.get("/ops/plain", base_url="https://localhost").data == b"plain"


def test_user_not_on_allow_list_refused(kit: LakeKit) -> None:
    refused = _client(_hf_app(hf_login=lambda: "mallory"))
    with refused.session_transaction() as prior:
        prior["user"] = "a-stale-session"  # a prior session must be dropped when the re-auth is refused
    reply = refused.get("/auth/callback", base_url="https://localhost")
    assert reply.status_code == 403 and b"not allowed" in reply.data
    with refused.session_transaction() as after:
        assert "user" not in after  # the refusal cleared the stale session
    assert refused.get("/ops/ping", base_url="https://localhost").status_code == 302
    allowed = _client(_hf_app(hf_login=lambda: "karsten"))
    ok = allowed.get("/auth/callback", base_url="https://localhost")
    assert ok.status_code == 302 and urlsplit(str(ok.headers["Location"])).path == "/"  # Taipy at / (Option 3)
    assert allowed.get("/ops/ping", base_url="https://localhost").data == b"pong"


def test_csrf_required_on_post(kit: LakeKit) -> None:
    client = _client(_local_app(kit))
    AccountService(kit.storage).create("ops", PASSWORD)
    no_token = client.post("/signin", data={"username": "ops", "password": PASSWORD}, base_url="https://localhost")
    assert no_token.status_code == 400 and b"CSRF" in no_token.data
    token = _csrf(client)
    bad = client.post(
        "/signin",
        data={"username": "ops", "password": PASSWORD, "csrf_token": token + "x"},
        base_url="https://localhost",
    )
    assert bad.status_code == 400


def test_session_cookie_flags(kit: LakeKit) -> None:
    AccountService(kit.storage).create("ops", PASSWORD)
    client = _client(_local_app(kit))
    token = _csrf(client)
    reply = client.post(
        "/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url="https://localhost"
    )
    cookies = reply.headers.get_all("Set-Cookie")
    session = next(c for c in cookies if c.startswith("session="))
    lowered = session.lower()
    # Local mode is a LAN install reached over plain http, so the session cookie is NOT Secure (a Secure cookie is
    # dropped on http except on localhost, which would block sign-in from another LAN host); it stays HttpOnly +
    # SameSite=Lax. HF mode keeps Secure — asserted in test_session_cookie_secure_per_mode.
    assert "httponly" in lowered and "samesite=lax" in lowered
    assert "secure" not in lowered


def test_session_cookie_secure_per_mode(kit: LakeKit) -> None:
    """HF mode is served over HTTPS, so its session cookie keeps the Secure flag; the local LAN install drops it so
    sign-in works over plain http from another host (Option B)."""
    assert _hf_app().config["SESSION_COOKIE_SECURE"] is True
    assert _local_app(kit).config["SESSION_COOKIE_SECURE"] is False


def test_signin_page_is_styled(kit: LakeKit) -> None:
    """The sign-in page wears the shared lakehouse chrome (spec §29.18 styling port): the one inline <style> with the
    palette tokens, the brand wordmark and the login card — not a bare unstyled form. The form stays intact."""
    signin = _client(_local_app(kit)).get("/signin", base_url="https://localhost")
    body = signin.get_data(as_text=True)
    assert "<style>" in body and "--color-primary" in body  # the one shared inline style block (tokens + styles.css)
    assert "ll-login-card" in body and "Lakehouse Ops" in body  # the login card + the brand wordmark
    assert 'name="csrf_token"' in body and 'name="password"' in body  # the form itself is unchanged


def test_no_secret_in_rendered_pages(kit: LakeKit) -> None:
    hf = _client(_hf_app())
    for path in ("/signin", "/auth/login"):
        reply = hf.get(path, base_url="https://localhost", follow_redirects=False)
        body = reply.get_data(as_text=True) + "".join(f"{k}:{v}" for k, v in reply.headers.items())
        assert CLIENT_SECRET not in body
    local = _client(_local_app(kit, env={"HF_TOKEN": "hf_a_secret_token_value_123456"}))
    body = local.get("/signin", base_url="https://localhost").get_data(as_text=True)
    assert "hf_a_secret_token_value_123456" not in body


def test_hf_login_redirect_uri_is_https_behind_proxy() -> None:
    """Behind HF's TLS-terminating proxy the in-container request scheme is http; the OAuth redirect_uri must still be
    https or huggingface.co/oauth/authorize rejects the flow with 400. /auth/login pins the callback scheme to https
    regardless of the request scheme (regression: the earlier url_for(_external=True) leaked the http scheme, which the
    test suite missed because every other test uses base_url=https://localhost)."""
    hf = _client(_hf_app())
    reply = hf.get("/auth/login", base_url="http://karstenskyt-ops.hf.space", follow_redirects=False)
    assert reply.status_code == 302
    redirect_uri = parse_qs(urlsplit(str(reply.headers["Location"])).query)["redirect_uri"][0]
    assert redirect_uri == "https://karstenskyt-ops.hf.space/auth/callback"


def test_local_mode_without_account_cannot_sign_in(kit: LakeKit) -> None:
    client = _client(_local_app(kit))  # no account created
    token = _csrf(client)
    reply = client.post(
        "/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url="https://localhost"
    )
    assert reply.status_code == 200 and b"did not match" in reply.data


def test_hf_mode_needs_the_space_variables() -> None:
    with pytest.raises(ValueError, match="OAUTH_CLIENT_ID"):
        create_app(_settings(), mode="hf", env={"LAKEHOUSE_SESSION_SECRET": "x" * 32})


def test_session_secret_required_on_hf() -> None:
    env = {k: v for k, v in HF_ENV.items() if k != "LAKEHOUSE_SESSION_SECRET"}
    with pytest.raises(ValueError, match="LAKEHOUSE_SESSION_SECRET"):  # fail fast, not a random per-process key (S2)
        create_app(_settings(), mode="hf", env=env)


def test_forged_login_id_refused(kit: LakeKit) -> None:
    client = _client(_local_app(kit))
    with client.session_transaction() as prior:
        prior["user"] = "ops"  # a valid-signature cookie with a login_id the server never minted
        prior["login_id"] = "forged-not-in-the-valid-set"
    reply = client.get("/ops/ping", base_url="https://localhost")
    assert reply.status_code == 302 and "/signin" in reply.headers["Location"]


def test_signin_redirects_to_root(kit: LakeKit) -> None:
    """A correct sign-in lands on `/` — the Taipy operator app lives at the root, not under `/ops` (Option 3)."""
    AccountService(kit.storage).create("ops", PASSWORD)
    client = _client(_local_app(kit))
    token = _csrf(client)
    reply = client.post(
        "/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url="https://localhost"
    )
    assert reply.status_code == 302 and urlsplit(str(reply.headers["Location"])).path == "/"


def test_signout_is_confirmed_then_revokes(kit: LakeKit) -> None:
    AccountService(kit.storage).create("ops", PASSWORD)
    client = _client(_local_app(kit))
    token = _csrf(client)
    client.post(
        "/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url="https://localhost"
    )
    assert client.get("/ops/ping", base_url="https://localhost").data == b"pong"  # signed in
    # Sign out is a confirmation step, not one click: GET /signout only renders the confirm page and does NOT revoke.
    confirm = client.get("/signout", base_url="https://localhost")
    assert confirm.status_code == 200
    body = confirm.get_data(as_text=True)
    assert 'action="/signout"' in body and 'method="post"' in body and 'name="csrf_token"' in body  # a POST to confirm
    assert "ll-login-card" in body and "Lakehouse Ops" in body  # the shared chrome, not a bare page
    assert client.get("/ops/ping", base_url="https://localhost").data == b"pong"  # still signed in after the GET
    # Only the POST (CSRF-protected) actually signs out.
    out = client.post("/signout", data={"csrf_token": _csrf(client)}, base_url="https://localhost")
    assert out.status_code == 302 and "/signin" in out.headers["Location"]
    assert client.get("/ops/ping", base_url="https://localhost").status_code == 302  # login revoked
