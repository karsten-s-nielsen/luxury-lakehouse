"""The Ops web app (spec §19, §20; plan Task 27): a Flask app with sign-in, sessions and CSRF, on which Task 28
mounts the Taipy Ops pages.

`create_app` builds the app in one of two modes. HF mode runs an OAuth code flow against Hugging Face with an
allow-list (`auth_hf`); local mode checks one Argon2id operator (`auth_local`). Every request but the open sign-in
endpoints needs a signed-in user, so a Taipy page mounted later is covered without a per-page decorator. Sessions are
HttpOnly, Secure (over HTTPS) and SameSite=Lax; every state-changing request needs the session's CSRF token.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import Callable, Mapping
from functools import wraps
from typing import TYPE_CHECKING, Final, Literal

from flask import Flask, Response, redirect, request, session
from flask.typing import ResponseReturnValue

from lakehouse.config.model import Settings
from lakehouse.services.account import AccountService
from lakehouse.web.auth_hf import CSRF_EXEMPT, HfLogin, hf_blueprint
from lakehouse.web.auth_local import SESSION_USER, local_blueprint
from lakehouse.web.csrf import install_csrf
from lakehouse.web.public import render_public_html
from lakehouse.web.sessions import ConnectionRegistry, ValidLogins

if TYPE_CHECKING:
    from lakehouse.services.runs import RunService

__all__ = ["SIGNIN_PATH", "create_app", "current_user", "login_required"]

SIGNIN_PATH = "/signin"
_SESSION_SECRET_ENV = "LAKEHOUSE_SESSION_SECRET"  # noqa: S105 - an environment variable name

# Defence-in-depth response headers on every response (audit S2). The app is operator-only behind sign-in and
# same-origin; these pin HTTPS (HSTS), forbid framing (clickjacking) and MIME sniffing, and add a CSP. The
# `script-src`/`style-src` allowances are what the bundled Taipy frontend needs (inline config + `eval`) and
# `connect-src` the socket.io channel; the strong directives (`object-src`/`base-uri`/`form-action`/
# `frame-ancestors`) hold regardless. The exact `script-src` can be tightened only once a browser confirms what the
# live Taipy Space does not need — not verifiable here (no browser), so this is the safe Taipy-compatible baseline.
# `font-src` allows `fonts.gstatic.com` for the Material Symbols icon `@font-face` in `ui/styles.css`; the previous
# `'self' data:` blocked it, so every icon rendered as its literal ligature word (UI finding 2026-11-02). Self-hosting
# that one static `.ttf` instance would remove the external allowance, if tighter isolation is wanted later.
_SECURITY_HEADERS: Final[dict[str, str]] = {
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self' data: https://fonts.gstatic.com; "
        "connect-src 'self' ws: wss:; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'"
    ),
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}
# The endpoints an anonymous visitor may reach: the whole sign-in flow and Flask's static files.
_OPEN_ENDPOINTS = frozenset(
    {
        "auth_local.signin_page",
        "auth_local.signin",
        "auth_local.signout_confirm",
        "auth_local.signout",
        "auth_hf.signin_page",
        "auth_hf.login",
        "auth_hf.callback",
        "auth_hf.signout",
        "static",
    }
)


def current_user() -> str | None:
    """The signed-in operator, or `None`."""
    user = session.get(SESSION_USER)
    return str(user) if user else None


def login_required[**P, R](view: Callable[P, R]) -> Callable[P, ResponseReturnValue | R]:
    """Send an anonymous visitor to the sign-in page. Used on explicit Ops routes; the global guard covers the rest."""

    @wraps(view)
    def guard(*args: P.args, **kwargs: P.kwargs) -> ResponseReturnValue | R:
        if current_user() is None:
            return redirect(SIGNIN_PATH)
        return view(*args, **kwargs)

    return guard


def _secret_key(env: Mapping[str, str], mode: str) -> str:
    """The session signing key. HF/production requires `LAKEHOUSE_SESSION_SECRET` (fail fast) so sessions survive a
    restart and span workers (finding S2); local mode falls back to a fresh per-process key for convenience."""
    secret = env.get(_SESSION_SECRET_ENV)
    if secret:
        return secret
    if mode == "hf":
        raise ValueError(f"HF mode needs {_SESSION_SECRET_ENV} set (sessions must survive restart and span workers)")
    return secrets.token_urlsafe(32)


def _account_service(accounts: AccountService | None, env: Mapping[str, str]) -> AccountService:
    if accounts is not None:
        return accounts
    # the composition root resolves the lake; web never imports storage directly
    from lakehouse.wiring import build_account_service

    return build_account_service(env)


def create_app(
    settings: Settings,
    *,
    mode: Literal["hf", "local"],
    accounts: AccountService | None = None,
    env: Mapping[str, str] | None = None,
    hf_login: HfLogin | None = None,
    public_runs: RunService | None = None,
) -> Flask:
    """The Ops Flask app. `accounts` (local mode) and `env` are injected in tests; in production the composition root
    resolves them from the environment. `hf_login` is the HF user-name resolver (the real token exchange by default).
    `public_runs` backs the anonymous public page at `/` (live-web addendum §2.1); the composition root passes the
    `RunService`."""
    chosen = dict(os.environ if env is None else env)
    app = Flask("lakehouse.web")
    config: dict[str, object] = app.config
    config["SECRET_KEY"] = _secret_key(chosen, mode)
    config["SESSION_COOKIE_HTTPONLY"] = True
    # HF mode is HTTPS, so the session cookie is Secure. Local mode is a LAN install reached over plain http (the
    # operator machine or another host on the LAN); a Secure cookie is dropped on http except on `localhost`, which
    # would block sign-in from any other host. So local mode omits Secure — the documented local-install tradeoff
    # (a trusted LAN, no real secrets beyond the test account). HF mode keeps it.
    config["SESSION_COOKIE_SECURE"] = mode == "hf"
    config["SESSION_COOKIE_SAMESITE"] = "Lax"
    config["LAKEHOUSE_MODE"] = mode
    config["LAKEHOUSE_SETTINGS"] = settings

    valid_logins = ValidLogins()
    connections = ConnectionRegistry()
    config["LAKEHOUSE_VALID_LOGINS"] = valid_logins
    config["LAKEHOUSE_CONNECTIONS"] = connections

    if mode == "hf":
        hf = hf_blueprint(app, chosen, hf_login=hf_login, logins=valid_logins, connections=connections)
        app.register_blueprint(hf)
        exempt = CSRF_EXEMPT
    else:
        account_service = _account_service(accounts, chosen)
        app.register_blueprint(local_blueprint(account_service, logins=valid_logins, connections=connections))
        exempt = frozenset[str]()
    install_csrf(app, exempt=exempt)

    @app.before_request
    def _require_login() -> ResponseReturnValue | None:
        if request.endpoint in _OPEN_ENDPOINTS:
            return None
        # one validity rule for the HTTP guard and the socket (addendum §2.2, Reviewer B LWE-B11).
        if current_user() is not None and valid_logins.is_valid(session):
            return None  # an operator falls through: Taipy serves `/` and its own routes (Option 3)
        # A signed-in session whose login_id is no longer valid (restart or sign-out) is stale: clear the cookie.
        # A plain anonymous session is left intact so its sign-in CSRF token survives the redirect.
        if current_user() is not None:
            session.clear()
        # Option 3: Taipy owns `/`, so the guard returns the public page in place for an anonymous GET / (§2.1);
        # every other Taipy route (/, /taipy-init, catch-all) sends an anonymous caller to sign in.
        if request.method == "GET" and request.path == "/" and public_runs is not None:
            return Response(render_public_html(public_runs), mimetype="text/html")
        return redirect(SIGNIN_PATH)

    @app.after_request
    def _no_store_on_root(response: Response) -> Response:
        # `/` serves two bodies by cookie (the public page vs the Taipy shell); a shared cache must never serve one
        # to the other, so mark every `/` response uncacheable and cookie-varying (Reviewer B LWE-B20).
        if request.path == "/":
            response.headers["Cache-Control"] = "no-store"
            response.vary.add("Cookie")
        return response

    @app.after_request
    def _security_headers(response: Response) -> Response:
        # Set the defence-in-depth headers on every response (audit S2); `setdefault` never overrides a header a
        # handler already set (e.g. the CSRF token or `Cache-Control` above).
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response

    @app.get("/ops/ping")
    @login_required
    def ping() -> Response:
        return Response("pong", mimetype="text/plain")

    return app
