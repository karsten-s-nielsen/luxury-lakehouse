"""The anonymous public page at `/` (live-web addendum §2.1; plan L3): read-only Monitoring, no match identity, no
socket, and `web.public` pulls in no Taipy."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from lakekit import LakeKit

from lakehouse.config.model import load_defaults
from lakehouse.observability import DependencyView, TaskRow
from lakehouse.runs.run import RunCounts
from lakehouse.services.account import AccountService
from lakehouse.services.runs import RunSummary
from lakehouse.web.app import create_app

_NOW = datetime(2026, 10, 28, 9, 0, tzinfo=UTC)
_ENV = {"LAKEHOUSE_SESSION_SECRET": "a-long-enough-session-secret-value"}


def _run_row(**over: Any) -> TaskRow:
    base: dict[str, Any] = {
        "run_id": "r1",
        "task": "lakehouse.run",
        "parent_task": None,
        "layer": None,
        "started_at": "2026-10-28T09:00:00+00:00",
        "duration_seconds": 12.5,
        "row_count": None,
        "state": "ok",
        "visibility": None,
        "execution_context": "hf_job",
        "flavour": "cpu-basic",
        "cost_usd": 0.02,
        "cost_error": None,
    }
    base.update(over)
    return TaskRow(**base)


class _FakeRuns:
    def history(self, limit: int = 500) -> list[RunSummary]:
        return [RunSummary("r1", "schedule", "ok", _NOW, _NOW, RunCounts(), 0)]

    def task_rows(self, run_id: str) -> list[TaskRow]:
        return [_run_row(), _run_row(task="lakehouse.run.write", layer="bronze", cost_usd=None)]

    def dependency(self, run_id: str) -> DependencyView:
        return DependencyView(run_edges=[("bronze", "resolution")], dbt_edges=[("stg_x", "dim_y")], note=None)


def test_public_page_anon_no_identity(kit: LakeKit) -> None:
    app = create_app(
        load_defaults("laptop"),
        mode="local",
        accounts=AccountService(kit.storage),
        env=_ENV,
        public_runs=cast("Any", _FakeRuns()),
    )
    app.testing = True
    reply = app.test_client().get("/", base_url="https://localhost")  # anonymous

    assert reply.status_code == 200
    body = reply.get_data(as_text=True)
    assert "est. $0.02 at 2026-10-26 rates (process time)" in body  # the dated spend estimate
    assert "stg_x" in body and "dim_y" in body  # the silver -> gold dependency
    assert "Sign in" in body
    assert 'class="sidebar"' in body and "ll-nav-active" in body  # the shared sidebar chrome, Monitoring active
    assert "Operations" in body  # the Operations group header, grouped like the operator nav
    assert "ll-nav-trailing" in body  # Sign in is spaced off the group, so it is not mistaken for a page
    assert "Lakehouse Ops" in body  # the brand wordmark from the one shared source (build_brand_html)
    assert "match" not in body.lower()  # no match name or id (§6.4)
    assert "socket.io" not in body  # a static page: no socket
    assert "<style>" in body and "--color-primary" in body  # the shared stylesheet is inlined (review B-M4)
    assert "ll-footer" in body  # ui/styles.css content is present, not just the tokens
    # one URL (`/`) serves two bodies by cookie, so a cache must never reuse the anon body for an operator (B20)
    assert reply.headers.get("Cache-Control") == "no-store"
    assert "Cookie" in (reply.headers.get("Vary") or "")


def test_styles_css_free_of_match_substring() -> None:
    """The privacy guard scans the stylesheet source directly (V2UI-12): public.py inlines the whole ui/styles.css
    into the anonymous body, and the body is scanned for a leaked identity word, so the CSS must never contain
    "match" (even "matches")."""
    from pathlib import Path

    import lakehouse.ui as ui_pkg

    css = (Path(ui_pkg.__file__).resolve().parent / "styles.css").read_text(encoding="utf-8")
    assert "match" not in css.lower()


def test_shared_css_fails_loud_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing ui/styles.css is a LOUD failure (spec S06 / review V2UI-IMPL-21, V2UI-IMPL-27): the one-source
    `read_styles_css` lets the read error propagate; it never returns an empty string that would serve an unstyled
    page. Seen to fail: restore a `try/except OSError: return ""` fallback and this test no longer raises."""
    from pathlib import Path

    from lakehouse.ui.template import read_styles_css

    def _boom(*_args: Any, **_kwargs: Any) -> str:
        raise FileNotFoundError("ui/styles.css is missing")

    monkeypatch.setattr(Path, "read_text", _boom)
    with pytest.raises(OSError, match=r"styles\.css"):
        read_styles_css()


def test_palette_is_one_source_for_operator_and_public() -> None:
    """The operator stylekit and the public page's :root tokens both derive from the one Taipy-free palette module
    (spec S06 / A-SPEC-03), so the amber/paper palette cannot drift between the two surfaces."""
    from lakehouse.ui.palette import PALETTE, css_root_block, palette_stylekit
    from lakehouse.ui.shell import STYLEKIT

    assert STYLEKIT == palette_stylekit()  # the operator stylekit is the palette
    root = css_root_block()
    for key in ("color_primary", "color_secondary", "color_paper", "color_background"):
        assert PALETTE[key] in root  # the public :root block carries the same values


def test_public_nav_matches_monitoring_page() -> None:
    """The public sidebar groups Monitoring under the same section header, with the same label + icon, as the
    Monitoring page config — so the one anon-viewable page reads identically on the public page and the operator
    shell. One source: `web.public` is Taipy-free and cannot import the Taipy page module, so this test pins them."""
    from lakehouse.ui.pages import monitoring
    from lakehouse.web.public import _NAV  # pyright: ignore[reportPrivateUsage]

    group = next(g for g in _NAV if any(link.href == "/" for link in g.links))
    assert group.header == monitoring.CONFIG.nav_section.name  # same "Operations" section as the operator nav
    link = next(link for link in group.links if link.href == "/")
    assert (link.label, link.icon) == (monitoring.TITLE, monitoring.ICON)


def test_public_module_imports_no_taipy() -> None:
    code = (
        "import lakehouse.web.public, sys; "
        "assert 'taipy' not in sys.modules, sorted(m for m in sys.modules if 'taipy' in m)"
    )
    result = subprocess.run(  # noqa: S603 - trusted: our own constant code string and sys.executable
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
