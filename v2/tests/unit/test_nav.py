"""The per-caller navigation (live-web addendum §1; plan L6): the operator sidebar lists every Ops page and a
Sign-out link; the anonymous public page lists Monitoring and a Sign-in link. The two surfaces never share a nav."""

from __future__ import annotations

from typing import Any, cast

from lakehouse.ui import shell
from lakehouse.web.public import render_public_html

_PAGE_TITLES = (
    "Monitoring",
    "Data sources",
    "Runs",
    "Review queue",
    "Settings",
)


class _NoRuns:
    """A RunService stand-in with no history, so the public page renders its frame without reading run detail."""

    def history(self, limit: int = 500) -> list[Any]:
        return []


def test_nav_operator_and_public() -> None:
    nav = shell.nav_markdown()
    for title in _PAGE_TITLES:
        assert title in nav  # the operator reaches every page
    assert "Sign out" in nav and "/signout" in nav

    html = render_public_html(cast("Any", _NoRuns()))
    assert "Monitoring" in html and "Sign in" in html and "/signin" in html
    assert "Sign out" not in html  # the public surface never offers Sign out
