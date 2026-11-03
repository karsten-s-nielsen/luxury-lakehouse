"""HF sign-in (spec §18, §19, §20; plan Task 27): an OAuth authorisation-code flow against the Hugging Face identity
provider, with an allow-list.

The Space variables `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OPENID_PROVIDER_URL` and `OAUTH_SCOPES` configure the
`authlib` client; `OPS_ALLOWED_USERS` is the allow-list. `/auth/login` sends the operator to the provider; the
provider calls `/auth/callback`, where the code is exchanged, the user name is read and checked against the
allow-list, and the session is set. The client secret never reaches a page: it is sent only to the token endpoint.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Final, Protocol, cast

from authlib.integrations.flask_client import OAuth
from flask import Blueprint, Flask, redirect, session, url_for
from flask.typing import ResponseReturnValue

from lakehouse.web.auth_local import SESSION_USER
from lakehouse.web.sessions import ConnectionRegistry, ValidLogins

__all__ = ["CSRF_EXEMPT", "REQUIRED_VARS", "HfLogin", "allowed_users", "hf_blueprint"]

REQUIRED_VARS: Final = ("OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET", "OPENID_PROVIDER_URL", "OAUTH_SCOPES")
_PROVIDER = "huggingface"
HfLogin = Callable[[], str]


class _OAuthClient(Protocol):
    """The part of the authlib OAuth client this module uses; the untyped library is cast to it at one boundary."""

    def authorize_redirect(self, redirect_uri: str) -> ResponseReturnValue: ...

    def authorize_access_token(self) -> dict[str, Any]: ...


def _client(oauth: OAuth) -> _OAuthClient:
    return cast("_OAuthClient", oauth.create_client(_PROVIDER))  # pyright: ignore[reportUnknownMemberType]


def allowed_users(env: Mapping[str, str]) -> frozenset[str]:
    """The operators allowed in (`OPS_ALLOWED_USERS`, comma or space separated). Empty means nobody signs in."""
    raw = env.get("OPS_ALLOWED_USERS", "")
    return frozenset(name for name in raw.replace(",", " ").split() if name)


def _register(app: Flask, env: Mapping[str, str]) -> OAuth:
    missing = [name for name in REQUIRED_VARS if not env.get(name)]
    if missing:
        raise ValueError(f"HF mode needs the Space variables {', '.join(missing)}")
    oauth = OAuth(app)
    oauth.register(  # pyright: ignore[reportUnknownMemberType]
        name=_PROVIDER,
        client_id=env["OAUTH_CLIENT_ID"],
        client_secret=env["OAUTH_CLIENT_SECRET"],
        server_metadata_url=f"{env['OPENID_PROVIDER_URL'].rstrip('/')}/.well-known/openid-configuration",
        client_kwargs={"scope": env["OAUTH_SCOPES"]},
    )
    return oauth


def hf_blueprint(
    app: Flask,
    env: Mapping[str, str],
    *,
    hf_login: HfLogin | None = None,
    logins: ValidLogins,
    connections: ConnectionRegistry,
) -> Blueprint:
    """The HF sign-in blueprint. `hf_login` returns the authenticated user name; the default runs the real token
    exchange. It is injected in tests so the allow-list and the session are checked without a real provider."""
    oauth = _register(app, env)
    allowed = allowed_users(env)
    blueprint = Blueprint("auth_hf", __name__)

    def _default_login() -> str:
        token = _client(oauth).authorize_access_token()
        info: dict[str, Any] = token.get("userinfo") or {}
        return str(info.get("preferred_username") or info.get("name") or "")

    resolve_user: HfLogin = hf_login if hf_login is not None else _default_login

    @blueprint.get("/signin")
    def signin_page() -> ResponseReturnValue:
        return redirect(url_for("auth_hf.login"))

    @blueprint.get("/auth/login")
    def login() -> ResponseReturnValue:
        # Force https on the callback redirect_uri. The Space sits behind HF's TLS-terminating proxy, so the
        # in-container request scheme is http; an http redirect_uri makes huggingface.co/oauth/authorize reject the
        # flow with 400 (it must match the registered https callback). auth_hf runs only on HF, where the external
        # scheme is always https, so pinning it here is correct, not a guess.
        callback = url_for("auth_hf.callback", _external=True, _scheme="https")
        return _client(oauth).authorize_redirect(callback)

    @blueprint.get("/auth/callback")
    def callback() -> ResponseReturnValue:
        username = resolve_user()
        if not username or username not in allowed:
            session.clear()
            return "This Hugging Face account is not allowed to use the Ops app.", 403
        session.clear()
        session[SESSION_USER] = username
        logins.mint(session)  # the valid login id, the single validity authority (addendum §2.4)
        return redirect("/")  # the Taipy operator app lives at the root (Option 3)

    @blueprint.get("/signout")
    def signout() -> ResponseReturnValue:
        login_id = logins.revoke(session)  # read before clearing the session
        session.clear()
        if login_id is not None:
            connections.disconnect(login_id)  # sever the live socket at once (addendum §2.4)
        return redirect("/signin")

    return blueprint


# the callback is the provider's redirect target, so it runs its own identity check and is CSRF-exempt
CSRF_EXEMPT: Final = frozenset({"auth_hf.callback"})
