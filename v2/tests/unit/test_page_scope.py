"""Per-caller page content (spec §6.2 point 3, §6.3; test 1; plan Task 7): a public caller is refused the
operator-only pages and sees no private content; the access map marks the monitoring view anon-viewable. `render_for`
runs the page's `refresh` against a recording state and resolves its display variables (the one render path)."""

from __future__ import annotations

from types import ModuleType
from typing import Any, cast

import pytest

import lakehouse.ui.shell as shell
from lakehouse.ui.shell import Services, anon_viewable, render_for

_SERVICE_FIELDS = ("settings", "modules", "filters", "runs", "review", "schedule")


class _EmptyRuns:
    """A runs service with no history: monitoring renders "No runs yet.", runs renders an empty table."""

    def history(self, limit: int = 500) -> list[Any]:
        return []


def _services(**overrides: Any) -> Services:
    base: dict[str, Any] = {k: cast("Any", object()) for k in _SERVICE_FIELDS}
    base.update(overrides)
    return Services(**base)


def test_anon_public_page_no_private() -> None:
    services = _services()
    for route in ("runs", "review", "schedule", "settings", "filters", "modules"):
        notice = render_for(route, services, "public")
        assert "sign in" in notice.lower()  # operator-only page: no service read, no private row rendered

    assert anon_viewable("monitoring") is True  # spend/dependency view carries no match identity (§6.3)
    assert anon_viewable("runs") is False  # runs errors may carry private match names
    assert anon_viewable("runs") is False  # Runs (with its run-detail logs, audit F6) is authenticated-only (§9)


def test_operator_and_anon_route_render_full() -> None:
    services = _services(runs=_EmptyRuns())
    # the operator sees the full page (the one render path runs refresh), not the sign-in notice
    operator_runs = render_for("runs", services, "operator")
    assert "sign in" not in operator_runs.lower() and "Runs" in operator_runs
    # an anon-viewable route renders for an anonymous caller (monitoring carries no match identity)
    anon_monitoring = render_for("monitoring", services, "public")
    assert "no runs yet" in anon_monitoring.lower()


def test_render_for_memoizes_the_per_call_scans(monkeypatch: pytest.MonkeyPatch) -> None:
    """Audit O1: `render_for`'s per-call `vars()` scans and route lookup are memoized — a second render of the same
    route re-scans nothing. The content is identical with or without the cache."""
    shell._PAGE_BY_ROUTE.clear()  # pyright: ignore[reportPrivateUsage] - reset the process caches for a clean count
    shell._DEFAULTS_BY_ROUTE.clear()  # pyright: ignore[reportPrivateUsage]
    scanned: list[str] = []
    real = shell._module_defaults  # pyright: ignore[reportPrivateUsage]

    def _spy(module: ModuleType) -> dict[str, Any]:
        scanned.append(module.__name__)
        return real(module)

    monkeypatch.setattr(shell, "_module_defaults", _spy)
    services = _services(runs=_EmptyRuns())

    first = render_for("monitoring", services, "public")
    after_first = len(scanned)
    second = render_for("monitoring", services, "public")

    assert after_first > 0, "the first render scans the module defaults"
    assert len(scanned) == after_first, "the second render re-scans nothing (memoized)"
    assert first == second, "memoization does not change the rendered content"
    assert "monitoring" in shell._PAGE_BY_ROUTE  # pyright: ignore[reportPrivateUsage] - the route map filled once
