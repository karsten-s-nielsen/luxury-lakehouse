"""The live Taipy socket: the origin lock and the connect/message/disconnect auth wrap (spec §6.2; live-web addendum
§2).

Cycle 2's standalone `build_socketio` is gone (it created a second `SocketIO` the live app never started — false
assurance). The live app locks and wraps Taipy's OWN socket: `install_origin` is the value passed to Taipy's
`server_config.socketio.cors_allowed_origins`, and `guard_live_socket` re-registers the connect/message/disconnect
handlers on `gui._server._ws` so that only a signed-in operator with a valid login joins the socket, a sign-out
severs the live connection at once, and a revoked login is refused per message. Taipy's own per-connection work stays
in `gui._handle_connect` / `gui._manage_message` / `gui._handle_disconnect`; this adds the gate in front (the
community socket has no per-message auth seam — addendum §0).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, cast

from lakehouse.services.authz import Permission, resolve_principal
from lakehouse.web.sessions import SESSION_LOGIN_ID, ConnectionRegistry, ValidLogins

__all__ = ["guard_live_socket", "install_origin"]

_DEFAULT_PORT: Final = "7860"


def install_origin(env: Mapping[str, str], mode: str) -> str | list[str]:
    """The install's own origin for the `cors_allowed_origins` lock. HF mode uses the Space origin; local mode uses the
    localhost origins on the app port, plus any host listed in `LAKEHOUSE_APP_ORIGINS`. Never a wildcard."""
    if mode == "hf":
        host = env.get("SPACE_HOST")
        if host:
            return f"https://{host}"
        space_id = env.get("SPACE_ID")
        if space_id:
            return f"https://{space_id.replace('/', '-')}.hf.space"
        raise ValueError("HF mode needs SPACE_HOST or SPACE_ID for the socket.io origin lock")
    port = env.get("APP_PORT", _DEFAULT_PORT)
    origins = [f"http://localhost:{port}", f"http://127.0.0.1:{port}"]
    # A local install is reached over the LAN too (the app on one host, a browser on another). Each extra host the
    # install serves is listed explicitly in LAKEHOUSE_APP_ORIGINS (comma-separated, each a full scheme://host[:port]) —
    # never a wildcard — so the Taipy socket accepts those origins. Empty by default: localhost only.
    extra = [origin.strip() for origin in env.get("LAKEHOUSE_APP_ORIGINS", "").split(",") if origin.strip()]
    return origins + extra


def _sid() -> str:
    """The current socket id. `flask.request.sid` is set by flask-socketio inside a socket handler but is untyped."""
    from flask import request

    return cast("str", request.sid)  # type: ignore[attr-defined]


def guard_live_socket(
    ws: Any,
    gui: Any,
    *,
    valid_logins: ValidLogins,
    connections: ConnectionRegistry,
) -> None:
    """Wrap Taipy's socket handlers on `ws` (`gui._server._ws`) for authz + live revocation, and register the
    sign-out disconnector. A connect from a caller that cannot WRITE, or whose login is not valid, is rejected; a
    message from a connection whose login has since been revoked is refused and the socket severed."""
    from flask import session  # Flask is a web-layer dependency (import-linter)

    connections.set_disconnector(lambda sid: ws.server.disconnect(sid, namespace="/"))

    @ws.on("connect")
    def _connect() -> bool:
        principal = resolve_principal(session)
        if not principal.can(Permission.WRITE) or not valid_logins.is_valid(session):
            return False  # anonymous, non-WRITE, or stale: never admitted to the operator socket (addendum §0)
        connections.bind(str(session[SESSION_LOGIN_ID]), _sid(), principal)
        gui._handle_connect()
        return True

    @ws.on("message")
    def _message(message: Mapping[str, Any], *args: Any) -> None:
        sid = _sid()
        if not valid_logins.is_valid_id(connections.login_of(sid)):
            ws.server.disconnect(sid, namespace="/")  # a revoked/stale connection is severed, not served
            return
        if "type" in message:
            gui._manage_message(message["type"], message)  # Taipy's own message pipeline (U/A/RU/DU/GDT/…)

    @ws.on("disconnect")
    def _disconnect() -> None:
        connections.unbind(_sid())
        gui._handle_disconnect()
