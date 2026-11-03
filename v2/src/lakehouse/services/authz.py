"""Authorization for the Ops actions and reads (spec §6; plan Task 6 + the live-web-enforcement addendum).

Authorization decides on a `Principal`'s permissions, never on a specific username, so roles are added later as a
`ROLE_PERMISSIONS` entry, not a refactor. `dispatch` is the one guarded action path: it refuses a principal that
lacks the action's permission (deny-by-default). CSRF for state-changing HTTP form posts lives in the web layer
(`web.csrf`), not here — the Taipy socket's defence is its origin lock + operator-only connect + a valid login
(addendum §2, Reviewer B LWE-B02/B13). `scoped_reads` is the visibility-scoped read path: a principal without
`VIEW_PRIVATE` sees only `public` rows, never `private` or `null` (spec §6.4).

`SESSION_USER` must match `web.auth_local.SESSION_USER`; a parity test guards the drift so this services module never
imports the web layer (which would reverse the layering and cycle).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Literal, cast

__all__ = [
    "SESSION_USER",
    "ActionRegistry",
    "ActionSpec",
    "Permission",
    "Principal",
    "Role",
    "Scope",
    "ScopedReads",
    "Unauthorized",
    "default_registry",
    "dispatch",
    "register_action",
    "resolve_principal",
    "scoped_reads",
]

SESSION_USER: Final = "user"  # == web.auth_local.SESSION_USER (parity-tested)

# Transitional: the cycle-2 `shell.render_for` signature still uses this literal. The live-web addendum removes
# `render_for` (operators-only Taipy), and this alias with it (plan L4 reconciliation).
Scope = Literal["public", "operator"]


class Role(Enum):
    ANONYMOUS = "anonymous"
    OPERATOR = "operator"


class Permission(Enum):
    VIEW_PUBLIC = "view_public"
    VIEW_PRIVATE = "view_private"
    WRITE = "write"


# The only role -> permission map. Adding a role (e.g. a read-only VIEWER = {VIEW_PUBLIC, VIEW_PRIVATE}) is one entry;
# a role without WRITE is never admitted to the Taipy socket and is served a separate read surface (addendum §3).
ROLE_PERMISSIONS: Final[dict[Role, frozenset[Permission]]] = {
    Role.ANONYMOUS: frozenset({Permission.VIEW_PUBLIC}),
    Role.OPERATOR: frozenset({Permission.VIEW_PUBLIC, Permission.VIEW_PRIVATE, Permission.WRITE}),
}


class Unauthorized(Exception):  # noqa: N818 - the spec/brief interface name (§6), matching werkzeug's `Unauthorized`
    """A principal lacked the permission an action or read required (spec §6)."""


@dataclass(frozen=True)
class Principal:
    """Who is calling, and what roles they hold. The single input to every authorization decision."""

    user: str | None
    roles: frozenset[Role]

    def can(self, permission: Permission) -> bool:
        """Deny-by-default: true only if one of the principal's roles grants `permission`."""
        return any(permission in ROLE_PERMISSIONS.get(role, frozenset()) for role in self.roles)


def resolve_principal(session: Mapping[str, Any]) -> Principal:
    """The caller's principal from the session. Today a signed-in user is the sole `OPERATOR` role; an unsigned
    caller is `ANONYMOUS`. The one seam a future identity source (OIDC claims, an account role) plugs into."""
    user = session.get(SESSION_USER)
    if user:
        return Principal(str(user), frozenset({Role.OPERATOR}))
    return Principal(None, frozenset({Role.ANONYMOUS}))


def _row_visibility(row: object) -> str | None:
    """The row's authoritative visibility. An encrypted-local bronze or listing row carries the `_visibility`
    column — the authoritative restricted copy (plan T9) — while `ops_matches` and the `dim_*` tables carry
    `visibility`. Prefer `_visibility` when the row has it, else fall back to `visibility`. A row that carries
    `_visibility` is trusted to it even when its source `visibility` disagrees: that is the whole point of the
    column (a public-flagged listing record of a private match must read private)."""
    if isinstance(row, Mapping):
        mapping = cast("Mapping[str, object]", row)
        value = mapping["_visibility"] if "_visibility" in mapping else mapping.get("visibility")
    else:
        value = getattr(row, "_visibility", None)
        if value is None:
            value = getattr(row, "visibility", None)
    return value if isinstance(value, str) else None


@dataclass(frozen=True)
class ScopedReads:
    """A visibility-scoped read facade bound to one caller's principal (spec §6.4). Immutable: a page cannot widen
    its own scope."""

    services: Any
    principal: Principal

    def visible[T](self, rows: Iterable[T]) -> list[T]:
        """An operator (`VIEW_PRIVATE`) sees rows in full; a caller without it sees only rows whose visibility is
        explicitly `public` — `private` and `null` are never visible to them (spec §6.4)."""
        if self.principal.can(Permission.VIEW_PRIVATE):
            return list(rows)
        return [row for row in rows if _row_visibility(row) == "public"]

    def operator_only[T](self, rows: Iterable[T]) -> list[T]:
        """A read that may expose a private match: refused unless the principal `can(VIEW_PRIVATE)` (spec §6.3)."""
        if not self.principal.can(Permission.VIEW_PRIVATE):
            raise Unauthorized("operator-only read for a caller without VIEW_PRIVATE")
        return list(rows)


def scoped_reads(services: Any, principal: Principal) -> ScopedReads:
    """The visibility-scoped read path for a caller (spec §6.4)."""
    return ScopedReads(services=services, principal=principal)


@dataclass(frozen=True)
class ActionSpec:
    handler: Callable[..., Any]
    permission: Permission


class ActionRegistry:
    """The guarded action dispatch (spec §6.2 point 2). `dispatch` is the one invocation path; it refuses a principal
    that lacks the action's permission (deny-by-default), so no registered action is unchecked. An unknown action is
    refused. The handler is passed the authenticated `Principal`, so a write is attributed to the signed-in operator,
    never a client-sent name. No CSRF token here — that is the HTTP form layer's job (`web.csrf`)."""

    def __init__(self) -> None:
        self._actions: dict[str, ActionSpec] = {}

    def __contains__(self, name: object) -> bool:
        return name in self._actions

    def register(self, name: str, handler: Callable[..., Any], *, permission: Permission = Permission.WRITE) -> None:
        """Register an action and the permission it requires (a write: `WRITE`; the operator-only preview read:
        `VIEW_PRIVATE`)."""
        self._actions[name] = ActionSpec(handler=handler, permission=permission)

    def dispatch(self, action_name: str, principal: Principal, **kwargs: Any) -> Any:
        spec = self._actions.get(action_name)
        if spec is None:
            raise Unauthorized(f"unknown action {action_name!r}")
        if not principal.can(spec.permission):
            raise Unauthorized(f"{spec.permission.value} required")
        return spec.handler(principal, **kwargs)


default_registry: Final = ActionRegistry()


def register_action(name: str, handler: Callable[..., Any], *, permission: Permission = Permission.WRITE) -> None:
    """Register an action on the default registry (spec §6.2)."""
    default_registry.register(name, handler, permission=permission)


def dispatch(action_name: str, principal: Principal, **kwargs: Any) -> Any:
    """Dispatch an action through the default registry's guard (spec §6.2)."""
    return default_registry.dispatch(action_name, principal, **kwargs)
