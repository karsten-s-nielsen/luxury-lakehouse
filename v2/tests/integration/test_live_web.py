"""The live Taipy web app end to end (live-web addendum §2; plan L4/L5): Option 3 — Taipy serves the operator app at
`/`, the guard serves the public page for an anonymous `GET /`, and the Taipy socket admits only a signed-in operator
with a valid login. Each test builds the real app via `main.serve(run=False)` over the kit lake; no server is started
(the socket is driven through flask-socketio's in-process test client, the HTTP routes through Flask's test client).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import active, save_settings

from lakehouse.services.account import AccountService
from lakehouse.services.authz import Principal, Role
from lakehouse.storage.roots import ResolvedRoots
from lakehouse.wiring import build_services

pytestmark = pytest.mark.integration

PASSWORD = "correct horse battery staple"  # noqa: S105 - a test value
SECRET = "a-long-enough-session-secret-value"  # noqa: S105 - a test value
_BASE = "https://localhost"


def _env(kit: LakeKit) -> dict[str, str]:
    return {
        "LAKEHOUSE_LAKE_ROOT": str(kit.roots.lake.local_path),
        "LAKEHOUSE_RESTRICTED_ROOT": str(kit.roots.restricted.local_path),
        "LAKEHOUSE_SESSION_SECRET": SECRET,
    }


def _live(kit: LakeKit) -> tuple[Any, Any]:
    """The real Flask app + live Taipy Gui, built but not served."""
    save_settings(kit.storage, [active("idsse_public")])
    AccountService(kit.storage).create("ops", PASSWORD)
    from lakehouse.web import main

    app, gui = main.serve(_env(kit), run=False)
    app.testing = True
    return app, gui


def _signin(app: Any) -> Any:
    """A Flask test client with a signed-in operator session (its own cookie jar)."""
    client = app.test_client()
    token = client.get("/signin", base_url=_BASE).headers["X-CSRF-Token"]
    client.post("/signin", data={"username": "ops", "password": PASSWORD, "csrf_token": token}, base_url=_BASE)
    return client


def test_taipy_socket_rejects_non_write(kit: LakeKit) -> None:
    app, gui = _live(kit)
    ws = gui._server._ws
    anon = ws.test_client(app, flask_test_client=app.test_client())
    assert not anon.is_connected()  # an anonymous caller never joins the operator socket (addendum §0)
    operator = ws.test_client(app, flask_test_client=_signin(app))
    assert operator.is_connected()  # a signed-in operator with a valid login does


def test_signout_revokes_socket(kit: LakeKit) -> None:
    """An HTTP sign-out severs the live socket at once, without the client sending a message (Reviewer B LWE-B14)."""
    app, gui = _live(kit)
    client = _signin(app)
    operator = gui._server._ws.test_client(app, flask_test_client=client)
    assert operator.is_connected()
    token = client.get("/signout", base_url=_BASE).headers["X-CSRF-Token"]  # the confirm page; GET does not sign out
    client.post("/signout", data={"csrf_token": token}, base_url=_BASE)  # the POST signs out and severs the socket
    assert not operator.is_connected()  # the server disconnected it, not the client


def test_stale_session_redirects_http(kit: LakeKit) -> None:
    """A valid cookie whose login is no longer in the valid set (a restart) is refused on every HTTP route, so the
    operator re-authenticates rather than meeting a dead UI (Reviewer B B18)."""
    app, _gui = _live(kit)
    client = _signin(app)
    assert client.get("/ops/ping", base_url=_BASE).data == b"pong"
    app.config["LAKEHOUSE_VALID_LOGINS"]._ids.clear()  # emulate a restart: valid set empty, the cookie survives
    reply = client.get("/taipy-init", base_url=_BASE)
    assert reply.status_code == 302 and "/signin" in reply.headers["Location"]


def test_live_socket_origin_lock(kit: LakeKit) -> None:
    app, gui = _live(kit)
    client = app.test_client()
    probe = "/socket.io/?EIO=4&transport=polling"
    bad = client.get(probe, headers={"Origin": "https://evil.example"}, base_url=_BASE)
    assert bad.status_code == 400  # a foreign origin handshake is refused by the engineio origin check
    ok = client.get(probe, headers={"Origin": "http://localhost:7860"}, base_url=_BASE)
    assert ok.status_code == 200  # the install's own origin is accepted
    # the lock is a real origin list; removing it (a wildcard or an empty list) would let any origin in
    origins = gui._server._ws.server.eio.cors_allowed_origins
    assert origins != "*" and origins != []


def test_taipy_endpoints_require_login(kit: LakeKit) -> None:
    app, _gui = _live(kit)
    anon = app.test_client()
    for path in ("/taipy-init", "/taipy-jsx/monitoring", "/some/catch/all"):
        reply = anon.get(path, base_url=_BASE)
        assert reply.status_code == 302 and "/signin" in reply.headers["Location"]
    # an anonymous GET / is the public page, in place of the Taipy app (Option 3)
    public = anon.get("/", base_url=_BASE)
    public_body = public.get_data(as_text=True)
    assert public.status_code == 200 and "Monitoring" in public_body and "Sign in" in public_body
    assert public.headers.get("Cache-Control") == "no-store" and "Cookie" in (public.headers.get("Vary") or "")
    # an OPERATOR GET / is the Taipy app, NOT the public page (Reviewer A D3-SPEC-05)
    operator = _signin(app).get("/", base_url=_BASE)
    operator_body = operator.get_data(as_text=True)
    assert operator.status_code == 200
    assert "Sign in for the full operations app" not in operator_body  # not the public body
    assert operator.headers.get("Cache-Control") == "no-store" and "Cookie" in (operator.headers.get("Vary") or "")


def _module_active(kit: LakeKit, module_id: str) -> bool:
    """Read one module's active flag back from the lake (a fresh service over the same storage)."""
    services = build_services(kit.storage, ResolvedRoots(roots=kit.roots, source="bootstrap"), env={})
    return next(m.active for m in services.modules.list_modules() if m.module_id == module_id)


def test_live_action_through_dispatch(kit: LakeKit) -> None:
    """An operator action over the live socket routes through the guarded dispatch and the service write persists; a
    caller without WRITE is refused and nothing changes (plan L5). The installed `gui.on_action` handler is driven
    exactly as Taipy's `__on_action` drives it (a socket message carries the sid); the socket transport itself is
    covered by L4 and the shape guard."""
    from flask import request

    app, gui = _live(kit)
    connections = app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-op", "sid-op", Principal("ops", frozenset({Role.OPERATOR})))
    connections.bind("login-anon", "sid-anon", Principal(None, frozenset({Role.ANONYMOUS})))
    assert _module_active(kit, "idsse_public") is True  # seeded active

    # the operator turns the module off through the live dispatch
    op_state = SimpleNamespace(filter_module="idsse_public", draft_active=False)
    with app.test_request_context():
        request.sid = "sid-op"  # type: ignore[attr-defined] - flask-socketio sets this on a real socket message
        gui.on_action(op_state, "apply", {"action": "on_module_active"})
    assert _module_active(kit, "idsse_public") is False  # the write persisted
    assert str(op_state.ll_action_notice).startswith("Done")

    # a caller without WRITE is refused by dispatch; nothing changes
    bad_state = SimpleNamespace(filter_module="idsse_public", draft_active=True)
    with app.test_request_context():
        request.sid = "sid-anon"  # type: ignore[attr-defined]
        gui.on_action(bad_state, "apply", {"action": "on_module_active"})
    assert _module_active(kit, "idsse_public") is False  # still off: the refusal changed nothing
    assert "permission" in str(bad_state.ll_action_notice).lower()
