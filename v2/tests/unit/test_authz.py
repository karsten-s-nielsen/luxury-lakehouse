"""Authorization: the role-ready principal/permission model, the visibility-scoped read path, and the fail-closed
guarded action dispatch (spec §6; plan Task 6 + the live-web addendum)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass

import pytest

from lakehouse.services import authz
from lakehouse.services.authz import (
    SESSION_USER,
    ActionRegistry,
    Permission,
    Principal,
    Role,
    Unauthorized,
    resolve_principal,
    scoped_reads,
)

_ANON = Principal(None, frozenset({Role.ANONYMOUS}))
_OPERATOR = Principal("op", frozenset({Role.OPERATOR}))


@dataclass(frozen=True)
class _Row:
    visibility: str | None


def test_principal_permissions() -> None:
    assert authz.ROLE_PERMISSIONS[Role.ANONYMOUS] == frozenset({Permission.VIEW_PUBLIC})
    assert authz.ROLE_PERMISSIONS[Role.OPERATOR] == frozenset(
        {Permission.VIEW_PUBLIC, Permission.VIEW_PRIVATE, Permission.WRITE}
    )
    assert _ANON.can(Permission.VIEW_PUBLIC) and not _ANON.can(Permission.WRITE)
    assert _OPERATOR.can(Permission.WRITE) and _OPERATOR.can(Permission.VIEW_PRIVATE)
    assert resolve_principal({}) == _ANON
    assert resolve_principal({SESSION_USER: "op"}) == _OPERATOR
    assert not Principal("x", frozenset()).can(Permission.VIEW_PUBLIC)  # deny-by-default: no roles, no permissions


def test_public_read_scope() -> None:
    rows = [_Row("public"), _Row("private"), _Row(None)]
    pub = scoped_reads(services=None, principal=_ANON)
    op = scoped_reads(services=None, principal=_OPERATOR)

    assert [r.visibility for r in pub.visible(rows)] == ["public"]  # no VIEW_PRIVATE -> only public rows
    assert [r.visibility for r in op.visible(rows)] == ["public", "private", None]  # VIEW_PRIVATE -> all

    assert pub.visible([{"visibility": "private"}, {"visibility": "public"}]) == [{"visibility": "public"}]

    assert op.operator_only(rows) == rows
    with pytest.raises(Unauthorized):
        pub.operator_only(rows)

    # a page cannot widen its own scope: the principal is fixed at creation
    assert pub.principal is _ANON
    with pytest.raises(FrozenInstanceError):
        pub.principal = _OPERATOR  # type: ignore[misc] - asserting immutability


def test_action_requires_write_permission() -> None:
    registry = ActionRegistry()
    called: list[dict[str, object]] = []

    def _toggle(principal: Principal, **kwargs: object) -> str:
        called.append({"operator": principal.user, **kwargs})
        return "ok"

    registry.register("toggle", _toggle, permission=Permission.WRITE)

    with pytest.raises(Unauthorized):  # a principal without WRITE is refused (no token involved)
        registry.dispatch("toggle", _ANON, module_id="statsbomb")
    with pytest.raises(Unauthorized):  # unknown action is refused (fail closed)
        registry.dispatch("ghost", _OPERATOR)

    assert registry.dispatch("toggle", _OPERATOR, module_id="statsbomb") == "ok"
    # the handler sees the authenticated operator, never a client-sent one
    assert called == [{"operator": "op", "module_id": "statsbomb"}]


def test_read_action_requires_view_private() -> None:
    registry = ActionRegistry()
    seen: list[str] = []

    def _preview(principal: Principal, *, module_id: str) -> str:
        seen.append(module_id)
        return f"preview {module_id}"

    registry.register("preview", _preview, permission=Permission.VIEW_PRIVATE)  # an operator-only read

    with pytest.raises(Unauthorized):  # an anon caller lacks VIEW_PRIVATE
        registry.dispatch("preview", _ANON, module_id="statsbomb")
    assert registry.dispatch("preview", _OPERATOR, module_id="statsbomb") == "preview statsbomb"
    assert seen == ["statsbomb"]


def test_session_key_parity() -> None:
    """The authz session key must equal the web layer's, so this services module never imports `web`."""
    from lakehouse.web.auth_local import SESSION_USER as WEB_USER

    assert authz.SESSION_USER == WEB_USER
