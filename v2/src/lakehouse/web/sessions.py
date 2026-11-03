"""Server-side session validity and the socket connection registry (spec §6.2; live-web addendum §2.4).

`ValidLogins` is the **single validity authority** for the HTTP guard and the Taipy socket: a `login_id` minted at
sign-in and held here is checked on every HTTP request and every socket message, so a restart (empty set) or a
sign-out invalidates both together — no dead UI, no cookie outliving sign-out. `ConnectionRegistry` maps a
`login_id` to its live socket id(s), so an HTTP sign-out disconnects the socket at once (not on its next message).

Both are per-app instances created in `create_app` and shared with the auth blueprints, the guard and the socket
wrap. They are process-local; a multi-worker deployment backs them with a shared store.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable, Mapping, MutableMapping
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from lakehouse.services.authz import Principal

__all__ = ["SESSION_LOGIN_ID", "ConnectionRegistry", "ValidLogins"]

SESSION_LOGIN_ID: Final = "login_id"


class ValidLogins:
    """The set of valid login ids. The single validity authority (addendum §2.4)."""

    def __init__(self) -> None:
        self._ids: set[str] = set()

    def mint(self, session: MutableMapping[str, object]) -> str:
        """Create a login id, record it as valid, and store it in the session."""
        login_id = secrets.token_urlsafe(32)
        self._ids.add(login_id)
        session[SESSION_LOGIN_ID] = login_id
        return login_id

    def is_valid(self, session: Mapping[str, object]) -> bool:
        """Whether the session carries a login id that is still valid."""
        login_id = session.get(SESSION_LOGIN_ID)
        return self.is_valid_id(str(login_id) if login_id else None)

    def is_valid_id(self, login_id: str | None) -> bool:
        """Whether a bare login id is still valid. The socket message wrap uses this: a socket event carries no
        reliable session, so the id captured at connect is re-checked here per message (addendum §2.4)."""
        return bool(login_id) and str(login_id) in self._ids

    def revoke(self, session: Mapping[str, object]) -> str | None:
        """Invalidate the session's login id (sign-out). Returns it so the caller can disconnect its sockets."""
        login_id = session.get(SESSION_LOGIN_ID)
        if login_id:
            self._ids.discard(str(login_id))
            return str(login_id)
        return None


class ConnectionRegistry:
    """A `login_id -> set[sid]` map bound at socket connect, with a disconnect seam the socket layer fills in, so a
    sign-out severs the live socket immediately (addendum §2.4, Reviewer B LWE-B14)."""

    def __init__(self) -> None:
        self._by_login: dict[str, set[str]] = {}
        self._by_sid: dict[str, str] = {}
        self._principals: dict[str, Principal] = {}
        self._disconnector: Callable[[str], None] | None = None

    def set_disconnector(self, disconnector: Callable[[str], None]) -> None:
        """The socket layer (L4) registers how to disconnect one `sid`."""
        self._disconnector = disconnector

    def bind(self, login_id: str, sid: str, principal: Principal) -> None:
        """Bind a live socket to its login id and its connect-time principal. The principal is captured here because
        a later socket message carries no reliable session, so the action dispatcher reads it back by `sid` (L5)."""
        self._by_login.setdefault(login_id, set()).add(sid)
        self._by_sid[sid] = login_id
        self._principals[sid] = principal

    def unbind(self, sid: str) -> None:
        self._principals.pop(sid, None)
        login_id = self._by_sid.pop(sid, None)
        if login_id is not None:
            remaining = self._by_login.get(login_id)
            if remaining is not None:
                remaining.discard(sid)
                if not remaining:
                    self._by_login.pop(login_id, None)

    def sids(self, login_id: str) -> frozenset[str]:
        return frozenset(self._by_login.get(login_id, set()))

    def login_of(self, sid: str) -> str | None:
        """The login id bound to a live socket id, or `None`. The socket message wrap uses it to re-check validity."""
        return self._by_sid.get(sid)

    def principal_of(self, sid: str) -> Principal | None:
        """The connect-time principal bound to a live socket id, or `None`. The action dispatcher attributes a write
        to this principal and gates on its permissions (L5)."""
        return self._principals.get(sid)

    def disconnect(self, login_id: str) -> None:
        """Disconnect every live socket bound to `login_id` (a no-op until the socket layer registers how)."""
        if self._disconnector is not None:
            for sid in self.sids(login_id):
                self._disconnector(sid)
