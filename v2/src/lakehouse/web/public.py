"""The anonymous public page (live-web addendum §2.1): a read-only, socket-free view of the Monitoring data — spend
per run and the inter-layer dependency view, which carry no match identity (§6.4) — served as plain HTML at `/`.

Taipy owns `/` (Option 3), so this is NOT a Flask route: the web guard (`app._require_login`) returns this HTML in
place of the Taipy app for an anonymous `GET /`. It has no socket, no callbacks and no per-client state, so it is
immune to the Taipy socket surface that forces the operators-only split (addendum §0). It reuses
`ui/pages/monitoring.py`'s spend text (a pure function, no Taipy), and the shared `ui/styles.css`, so the anonymous
page carries the lakehouse styling (spec §29.18 styling port).
"""

from __future__ import annotations

import html
from typing import TYPE_CHECKING

from lakehouse.ui.pages._monitoring_data import spend_line
from lakehouse.ui.template import (
    NavGroup,
    NavLink,
    build_brand_html,
    build_footer_html,
    build_sidebar_html,
    inline_style_block,
)

if TYPE_CHECKING:
    from lakehouse.services.runs import RunService

__all__ = ["render_public_html"]

# The anonymous sidebar: the same grouped nav the operator sees (`build_nav` / `nav_markdown`), pared to the one
# anon-viewable page — the "Operations" section with Monitoring (served at `/`, ANON_VIEWABLE_ROUTES in shell.py) —
# plus a trailing Sign in link, exactly as the operator nav appends Sign out. The section header + the Monitoring
# label/icon are pinned to the Monitoring page config by test_public_nav_matches_monitoring_page (one source: this
# Taipy-free module cannot import that Taipy page). `build_sidebar_html` / `build_brand_html` / `build_footer_html`
# render the same chrome the operator shell uses, so the two pages cannot drift (spec §29.18 styling port).
_NAV: tuple[NavGroup, ...] = (NavGroup("Operations", (NavLink("/", "Monitoring", "monitoring"),)),)
_TRAILING: tuple[NavLink, ...] = (NavLink("/signin", "Sign in", "login"),)

# The operator Taipy app gets the --color-* tokens from the Stylekit (gui.run(stylekit=...)); the public page is plain
# Flask HTML with no Taipy, so it inlines the SAME `<style>` (palette tokens + the shared ui/styles.css) from the one
# Taipy-free source, `template.inline_style_block` (spec S06 / V2UI-10: one source, no duplicated token literal). A
# missing stylesheet is a LOUD failure there (`read_styles_css`), never a silent unstyled page.
#
# GUARD-RAIL: this inlines the full ui/styles.css into the anonymous body, and test_public_page_anon_no_identity scans
# that body with `assert "match" not in body.lower()` to catch a leaked fixture name or id. So ui/styles.css must never
# contain the substring "match" — even an innocent word like "matches" trips it. The guard stays strict; reword the
# stylesheet (use "fixture", "per-game" or "the same"), never the guard.
_STYLE = inline_style_block()


def render_public_html(runs: RunService) -> str:
    """The public Monitoring page as plain HTML (no socket, no identity), in the shared lakehouse theme."""
    history = runs.history(limit=1)
    if not history:
        body = "<p>No runs yet.</p>"
    else:
        latest = history[0]
        rows = runs.task_rows(latest.run_id)
        dependency = runs.dependency(latest.run_id)
        spend = html.escape(spend_line(rows))
        steps = "".join(
            f"<tr><td>{html.escape(r.task)}</td><td>{html.escape(r.layer or '—')}</td>"
            f"<td>{r.duration_seconds:.3f}s</td><td>{html.escape(r.state)}</td></tr>"
            for r in rows
        )
        run_edges = " · ".join(f"{html.escape(src)} → {html.escape(dst)}" for src, dst in dependency.run_edges) or "—"
        dbt_rows = "".join(
            f"<tr><td>{html.escape(src)}</td><td>{html.escape(dst)}</td></tr>" for src, dst in dependency.dbt_edges
        )
        body = (
            f"<p><strong>{spend}</strong></p>"
            "<h2>Steps</h2>"
            '<div class="ll-table-wrap">'
            "<table><thead><tr><th>Step</th><th>Layer</th><th>Duration</th><th>State</th></tr></thead>"
            f"<tbody>{steps or '<tr><td>(no steps recorded)</td></tr>'}</tbody></table></div>"
            "<h2>Layer dependencies</h2>"
            f"<p>Run layers: {run_edges}</p>"
            '<div class="ll-table-wrap">'
            "<table><thead><tr><th>From (silver)</th><th>To (gold)</th></tr></thead>"
            f"<tbody>{dbt_rows or '<tr><td>(no dbt models)</td></tr>'}</tbody></table></div>"
        )
    nav = build_sidebar_html(_NAV, active_href="/", trailing=_TRAILING)
    return (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>Lakehouse — Monitoring</title>"
        f"{_STYLE}</head><body class='ll-root'>"
        f"<div class='ll-public-layout'>{nav}"
        "<main class='ll-public-main ll-content'>"
        f'<div class="ll-title-row">{build_brand_html()}</div>'
        "<p>Spend per run and the inter-layer dependency view. Sign in for the full operations app.</p>"
        f"{body}"
        f"{build_footer_html()}"
        "</main></div></body></html>"
    )
