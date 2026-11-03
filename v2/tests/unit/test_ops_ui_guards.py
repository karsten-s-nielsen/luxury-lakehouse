"""The gold-standard Ops UI invariants (design 2026-10-29): no page writes Taipy markup, every CSS class the template
can emit has a rule, every page carries the chrome and the shared notice, and the css_file seam is a basename."""

from __future__ import annotations

import re
from importlib import import_module
from pathlib import Path

import lakehouse.ui.pages as pages_pkg
from lakehouse.ui import shell
from lakehouse.ui.template import (
    Button,
    Control,
    NavSection,
    PageConfig,
    Prose,
    Section,
    Table,
    Text,
    build_page,
    build_sidebar_section,
)

_PAGES_DIR = Path(pages_pkg.__file__).resolve().parent
_STYLES = (Path(shell.__file__).resolve().parent / "styles.css").read_text(encoding="utf-8")


def test_no_page_writes_taipy_markup() -> None:
    """Goal 1: a page file writes no Taipy control markup or CSS class — all of it lives in the template. This is the
    exact rev-1 regression (a page re-added a <|…|> control); the suite, not a manual review, must catch it now
    (spec S07a / A-SPEC-01)."""
    for name in shell.PAGE_MODULES:
        src = (_PAGES_DIR / f"{name}.py").read_text(encoding="utf-8")
        assert "<|" not in src, f"{name}.py writes Taipy control markup"
        assert "class_name=" not in src, f"{name}.py writes a CSS class"
        module = import_module(f"lakehouse.ui.pages.{name}")
        assert hasattr(module, "CONFIG") and hasattr(module, "refresh"), f"{name} must expose a PageConfig + refresh"
        assert not hasattr(module, "page_markdown"), f"{name} still has the removed string renderer page_markdown"


def _emitted_classes(markup: str) -> set[str]:
    classes: set[str] = set()
    for match in re.findall(r"class_name=([A-Za-z0-9 _-]+)", markup):
        classes.update(match.split())
    for match in re.findall(r'class="([^"]+)"', markup):
        classes.update(match.split())
    return classes


def test_every_emittable_class_has_a_css_rule() -> None:
    """Goal: every class the template can emit has a rule in styles.css, so no control ships unstyled (the anti-goal).
    A full page + the sidebar + the root chrome + the sign-in notice exercise every emitter (spec S07b / A-SPEC-02)."""
    cfg = PageConfig(
        title="T",
        icon="database",
        nav_section=NavSection("Operations"),
        description="d",
        body=(
            Prose("p"),
            Text("t"),
            Table("rows"),
            Table("r2", render_var="g"),
            Button("B", "on_b", primary=True),
            Button("C", "on_c"),
            Section("S", (Control("selector", "v", label="L", lov="lov", help="h"), Control("toggle", "w", label="W"))),
        ),
        sidebar_widgets=(Control("selector", "s", label="Src", lov="l"), Control("toggle", "tg", label="Tg")),
    )
    markup = (
        build_page(cfg)
        + build_sidebar_section("Filters", cfg.sidebar_widgets, condition='current_page == "x"')
        + shell.build_root_page(shell.nav_markdown(), "")
        + shell._SIGNIN_NOTICE  # pyright: ignore[reportPrivateUsage]
    )
    for cls in _emitted_classes(markup):
        # a complete selector token, not a substring: `.ll-table` must not be satisfied by `.ll-table-foo` (B-22)
        assert re.search(rf"\.{re.escape(cls)}(?![\w-])", _STYLES), f"class {cls!r} is emitted but has no CSS rule"


def test_every_page_carries_chrome_and_shared_notice() -> None:
    """Every page's built Markdown has the lakehouse header and the shared action notice (spec S02)."""
    for name in shell.PAGE_MODULES:
        cfg = import_module(f"lakehouse.ui.pages.{name}").CONFIG
        md = build_page(cfg)
        assert md.lstrip().startswith("## "), f"{name} has no page header"
        assert "ll-action-notice" in md and "{ll_action_notice}" in md, f"{name} drops the shared notice"


def test_tuple_lov_selectors_bind_by_id() -> None:
    """The Source and Tournaments selectors on the Data sources page use (id, label) lov, so each must set
    value_by_id, or Taipy binds the whole tuple and the pick/save break (V2UI-IMPL-23)."""
    from lakehouse.ui.pages import modules
    from lakehouse.ui.template import iter_controls

    tuple_lov_vars = {"filter_module", "filter_tournaments"}
    for control in iter_controls(modules.CONFIG):
        if control.var in tuple_lov_vars:
            assert control.value_by_id, f"{control.var} has an (id, label) lov but value_by_id is not set"


def test_css_file_is_a_basename() -> None:
    """The seam fact 1: css_file is the basename served over http, never an absolute file:// path."""
    assert shell.STYLES_CSS.name == "styles.css" and shell.STYLES_CSS.is_file()


def test_root_chrome_emits_the_lakehouse_classes() -> None:
    """build_root_page emits the lakehouse chrome classes (review B-M5 / V2UI-07). Seen to fail: drop ll-title-row
    (or any chrome class) from build_root_page and the brand/sidebar/footer lose their styling hooks."""
    chrome = shell.build_root_page(shell.nav_markdown(), shell._sidebar_sections())  # pyright: ignore[reportPrivateUsage]
    for cls in ("sidebar", "ll-title-row", "ll-brand", "ll-footer", "ll-nav-header"):
        assert cls in chrome, f"root chrome missing {cls}"
    assert "<|content|>" in chrome  # the page-content slot
