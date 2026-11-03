"""The declarative Ops page template (plan Task 28; the v2 port of the v1 `hf_taipy_app/src/page_template.py`).

A page is data, not a hand-written layout: a `PageConfig` (title, icon, nav section, description, an ordered `body` of
elements, and optional sidebar filter widgets) is turned into Taipy Markdown by `build_page`. All structural wrapping
and CSS classes live here; a page file carries no Taipy markup. `build_header_from_config` gives every page the
lakehouse page header (icon + title + description), `build_nav` builds the sidebar navigation grouped by section, and
`build_sidebar_section` places filter widgets in the sidebar with the shared filter-box styling. The classes
(`ll-page-header`, `ll-filter-box`, `ll-table`, `ll-primary`, …) are styled by `ui/styles.css`.

The `body` is ONE ordered tuple of heterogeneous elements (`Prose`, `Text`, `Table`, `Section`, `Control`, `Button`),
rendered top to bottom in list order, so a page's exact element order is explicit (no implicit content/sections
split). `Prose` is a fixed, constant sentence; `Text` is a reactive variable; a `Section` is a form group with its own
ordered items. There is one `Control` vocabulary for the sidebar and the body: placement, not kind, picks the wrapper
(`build_sidebar_section` wraps a `Control` in `ll-filter-box`; a `Section` renders it plainly).

This module generates strings only: it imports no Taipy and no lake package, so the import-linter contract (the UI
reaches the lake only through the services layer) holds.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Final, Literal, cast

__all__ = [
    "BRAND_ICON",
    "BRAND_WORD",
    "CHANGE_CALLBACK",
    "FOOTER_LABEL",
    "FOOTER_LEAD",
    "FOOTER_URL",
    "TABLE_HEIGHT",
    "Button",
    "Control",
    "Element",
    "NavGroup",
    "NavLink",
    "NavSection",
    "PageConfig",
    "PageEntry",
    "Prose",
    "Section",
    "Table",
    "Text",
    "action_names",
    "assert_unique_control_vars",
    "build_brand_html",
    "build_footer_html",
    "build_header_from_config",
    "build_nav",
    "build_page",
    "build_sidebar_html",
    "build_sidebar_section",
    "control_var_names",
    "escape_label",
    "inline_style_block",
    "iter_controls",
    "read_styles_css",
    "table_columns",
]

ControlKind = Literal["selector", "selector_multi", "input", "toggle"]

# The one control-level change callback every marked control names (defined in `lakehouse.ui.shell`, the default
# module where Taipy's by-name lookup finds it). Named here so the template stays Taipy-free.
CHANGE_CALLBACK: Final = "ll_on_control_change"

# The shared action-outcome notice variable (lives in the shell default scope). The template renders it at the foot of
# every page (spec S02), gated so an empty notice renders nothing (A-SPEC-07). Named here so build_page can reference
# it without importing shell.
NOTICE_VAR = "ll_action_notice"


def escape_label(text: str) -> str:
    """Make a control or button label safe inside `<|…|label=…|>` (spec C01). Taipy reads `|` as a property
    separator and `{` / `}` as a binding, so a label that holds one breaks the control or creates a stray binding.
    Replace them with lookalikes that render the same to a reader."""
    return text.replace("|", "│").replace("{", "❴").replace("}", "❵")  # noqa: RUF001 - intentional lookalikes


def table_columns(rows: list[dict[str, str]], headers: Sequence[str]) -> dict[str, list[str]]:
    """Convert a list of row dicts (header -> cell) into Taipy's `table` column format: {header: [cell, ...]}, in
    `headers` order. The Taipy `table` control renders a dict of equal-length column lists; a list of row dicts does
    NOT render (it leaves the table empty). Pages build row dicts, then pass them through this for the display var."""
    return {header: [row.get(header, "") for row in rows] for header in headers}


@dataclass(frozen=True)
class NavSection:
    """A navigation group in the sidebar. Pages in the same section render under one header, in registration order."""

    name: str
    icon: str = ""


@dataclass(frozen=True)
class Control:
    """One input, in the body (inside a `Section`) or the sidebar (`sidebar_widgets`). Data only: the template adds
    all wrapping and styling.

    `kind`: `selector` (single pick, dropdown), `selector_multi` (multi pick), `input` (free text) or `toggle`.
    `lov` names the list-of-values variable for a selector. `on_change` MARKS that the control's variable has a
    services-backed handler (a function of that name in the page module); the template emits the one shared callback
    `on_change=ll_on_control_change`, never the handler itself (Taipy would call it as `(state, var, value)` with no
    services). `shell.build_gui` collects the marked variables into a dispatcher over the services, which the shared
    callback forwards to (spec S03). `condition` is a Taipy render expression (empty means always shown). `help`
    renders an info icon with a native tooltip.
    """

    kind: ControlKind
    var: str
    label: str = ""
    lov: str = ""
    on_change: str = ""
    condition: str = ""
    help: str = ""
    value_by_id: bool = False  # selector with an (id, label) lov: bind the id, not the whole tuple (Taipy value_by_id)


@dataclass(frozen=True)
class Button:
    """An action button. `action` is the `on_action` handler name (dispatched by the live-action layer). `primary`
    adds `class_name=ll-primary` (the amber emphasis)."""

    label: str
    action: str
    primary: bool = False


@dataclass(frozen=True)
class Prose:
    """A fixed sentence or paragraph, a constant in the page config (no bound variable). Rendered as static Markdown
    in the page definition, so a page carries its fixed text without pushing it into reactive state."""

    markdown: str


@dataclass(frozen=True)
class Text:
    """A reactive block of Markdown text, filled by `refresh` into `var`."""

    var: str


# The default table height. Taipy's `table` caps its scroll container at `height` (default 80vh), so a long table
# filled most of the screen. 24rem shows about ten rows and scrolls inside the box; a shorter table shrinks to fit.
TABLE_HEIGHT: Final = "24rem"


@dataclass(frozen=True)
class Table:
    """A native Taipy `table` bound to a column-dict variable (`table_columns`). `render_var` gates a no-data table:
    the `table` control ignores `render`, so the template wraps it in a `<|part|render=…|>` that hides on empty.
    `height` caps the scroll box (a shorter table shrinks to fit); the rows past it scroll inside the table."""

    var: str
    render_var: str = ""
    class_name: str = "ll-table"
    height: str = TABLE_HEIGHT


@dataclass(frozen=True)
class Section:
    """A form group: a subtitle and its own ordered `items` (any element). The order inside the section is the
    `items` order. `subtitle_var` names a reactive subtitle (for example the selected source's name) that replaces
    the fixed `subtitle` on the page; `render_var` hides the whole section while that variable is false."""

    subtitle: str
    items: tuple[Element, ...] = ()
    subtitle_var: str = ""
    render_var: str = ""


Element = Prose | Text | Table | Control | Button | Section


@dataclass(frozen=True)
class PageConfig:
    """Everything unique to a page; the template renders the rest. A page file provides only this data and a
    `refresh`. `body` is the ordered element list; `sidebar_widgets` are the per-page filter controls."""

    title: str
    icon: str
    nav_section: NavSection
    description: str = ""
    body: tuple[Element, ...] = ()
    sidebar_widgets: tuple[Control, ...] = ()
    reads_match_data: bool = True
    """Whether the page reads any match-scoped data (a match/player/team row). Default-safe: `True` means the page
    may expose a private match, so it is operator-only and must not be in `ANON_VIEWABLE_ROUTES`. Only a page that
    reads none — `monitoring` (run steps, spend, layer edges) — sets it `False` and may be anonymously viewable
    (spec §6.3, plan T11)."""


@dataclass(frozen=True)
class PageEntry:
    """One page in the navigation registry; list order is display order."""

    route: str
    config: PageConfig


def _help_icon(text: str) -> str:
    return f' <span class="ll-help material-symbols-outlined" title="{text}">info</span>' if text else ""


def build_header_from_config(cfg: PageConfig) -> str:
    """The lakehouse page header: the icon + title as a heading, and the description as a reference line.

    Emitted as Markdown — a `##` heading with an inline icon `<span>`, then an inline `<span class="ll-reference">`
    for the description. It sits in the page DEFINITION (where Taipy renders Markdown and inline HTML); it must NOT go
    inside a `<|{var}|text|mode=markdown|>` value, because Taipy escapes HTML in a variable value, so the icon `<span>`
    would show as literal text. A converted page puts `{_HEADER}` in its page definition and fills only data into its
    body variables.

    CRITICAL: no Markdown thematic break (`---`). A `---` placed before a `<|{var}|text|>` control in the same page
    definition silently breaks that control's reactive binding — the value is set server-side but never reaches the
    client (verified live on Taipy 4.1.1). The header's underline divider is drawn by CSS (the `.ll-reference`
    border), never by `---`."""
    icon = f'<span class="material-symbols-outlined">{cfg.icon}</span> ' if cfg.icon else ""
    parts = [f"## {icon}{cfg.title}"]
    if cfg.description:
        parts.append(f'<span class="ll-reference">{cfg.description}</span>')
    return "\n\n".join(parts)


def build_nav(registry: list[PageEntry]) -> str:
    """The sidebar navigation: one section header per `NavSection` (in first-seen order), then its page links."""
    sections: dict[str, list[PageEntry]] = {}
    for entry in registry:
        sections.setdefault(entry.config.nav_section.name, []).append(entry)
    parts: list[str] = []
    for name, entries in sections.items():
        parts.append(f"<|part|class_name=ll-nav-header|\n**{name}**\n|>\n")
        for entry in entries:
            parts.append(
                f'[<span class="material-symbols-outlined">{entry.config.icon}</span> '
                f"{entry.config.title}](/{entry.route})\n"
            )
    return "\n".join(parts)


# The brand wordmark + footer, and an HTML sidebar renderer, shared so the operator shell (Taipy markup) and the public
# page (plain HTML) cannot drift. All Taipy-free, so `web.public` imports them without pulling Taipy in
# (test_public_module_imports_no_taipy). They use the same CSS classes the operator chrome does.
BRAND_ICON: Final = "warehouse"
BRAND_WORD: Final = "Lakehouse Ops"
# The footer is one sentence, not a lone link: a bare "Published Datasets" link at the foot of an operations page gave
# no hint of what it opened or why it was there. The lead says what the link is; the label is the link text.
FOOTER_LEAD: Final = "The lake publishes its public data as"
FOOTER_LABEL: Final = "datasets on Hugging Face"
FOOTER_URL: Final = "https://huggingface.co/luxury-lakehouse"


@dataclass(frozen=True)
class NavLink:
    """One static-nav link (the public sidebar): an href, a goal-oriented label and a Material Symbols icon."""

    href: str
    label: str
    icon: str


@dataclass(frozen=True)
class NavGroup:
    """A sidebar section: a header label and the page links under it, mirroring one `NavSection` in `build_nav`, so the
    public page groups its pages the same way the operator shell does (e.g. "Operations" > Monitoring)."""

    header: str
    links: Sequence[NavLink]


def _icon_span(icon: str) -> str:
    return f'<span class="material-symbols-outlined">{icon}</span>'


def _nav_anchor(link: NavLink, active_href: str, extra: str = "") -> str:
    active = " ll-nav-active" if link.href == active_href else ""
    return f'<a href="{link.href}" class="ll-nav-link{active}{extra}">{_icon_span(link.icon)} {link.label}</a>'


def build_sidebar_html(groups: Sequence[NavGroup], *, active_href: str, trailing: Sequence[NavLink] = ()) -> str:
    """The sidebar navigation as plain HTML — the static counterpart to `build_nav`'s Taipy markup. Each group renders
    an `.ll-nav-header` then its links, exactly as `build_nav` does per `NavSection`; `trailing` links (e.g. Sign in)
    render after the groups. The first trailing link gets `ll-nav-trailing`, which adds the same gap `.ll-nav-header`
    puts between groups, so a trailing action stands apart and is not mistaken for a page. The link whose href is
    `active_href` gets `ll-nav-active`. Same `.sidebar` / `.ll-nav-header` / `.material-symbols-outlined` classes as
    the operator shell, so the two surfaces match."""
    rows = ['<nav class="sidebar">']
    for group in groups:
        rows.append(f'<p class="ll-nav-header"><strong>{group.header}</strong></p>')
        rows.extend(_nav_anchor(link, active_href) for link in group.links)
    for index, link in enumerate(trailing):
        rows.append(_nav_anchor(link, active_href, " ll-nav-trailing" if index == 0 else ""))
    rows.append("</nav>")
    return "".join(rows)


def build_brand_html() -> str:
    """The brand wordmark as HTML (the static counterpart to the shell's `_BRAND`), in the `.ll-brand` amber style."""
    return f'<span class="ll-brand">{_icon_span(BRAND_ICON)} {BRAND_WORD}</span>'


def build_footer_html() -> str:
    """The footer link as HTML (the static counterpart to the shell's `_FOOTER`), in the `.ll-footer` style."""
    return f'<div class="ll-footer">{FOOTER_LEAD} <a href="{FOOTER_URL}">{FOOTER_LABEL}</a>.</div>'


_STYLES_CSS = Path(__file__).resolve().parent / "styles.css"


def read_styles_css() -> str:
    """The Ops stylesheet text — the one `ui/styles.css` the Taipy shell loads. A missing file is a LOUD failure
    (spec S06): the read error propagates, so a socket-free page never renders silently unstyled."""
    return _STYLES_CSS.read_text(encoding="utf-8")


def inline_style_block() -> str:
    """The shared inline `<style>` (the palette `:root` tokens + the whole `ui/styles.css`) that the socket-free HTML
    pages (public monitoring, sign-in) inline, so they wear the same lakehouse theme as the Taipy operator shell. One
    source: both pages call this, so the theme cannot drift (spec §29.18 styling port)."""
    from lakehouse.ui.palette import css_root_block

    return f"<style>{css_root_block()}{read_styles_css()}</style>"


def _control_markup(control: Control, *, label: bool = True) -> str:
    """The bare Taipy control markup for one `Control` (no wrapper). A control marked with a handler gets
    `on_change=ll_on_control_change` — the ONE shared callback (`shell.ll_on_control_change`), never the handler
    itself: Taipy would call a handler as `(state, var, value)` with no services, so the shared callback forwards to the
    Gui-level dispatcher, which looks up the handler by variable and passes the services (spec S03). Taipy calls a
    control-level callback only when the operator changes that control, never for a server-side write. A label with
    `|`/`{`/`}` is escaped (spec C01)."""
    text = escape_label(control.label) if label else ""
    label_prop = f"|label={text}" if text else ""
    change = f"|on_change={CHANGE_CALLBACK}" if control.on_change else ""
    if control.kind in ("selector", "selector_multi"):
        multi = "|multiple" if control.kind == "selector_multi" else ""
        # value_by_id: with an (id, label) lov, Taipy would otherwise bind the whole tuple, not the id (V2UI-IMPL-23)
        vbi = "|value_by_id=True" if control.value_by_id else ""
        return f"<|{{{control.var}}}|selector|lov={{{control.lov}}}{multi}|dropdown{vbi}{label_prop}{change}|>"
    if control.kind == "input":
        return f"<|{{{control.var}}}|input{label_prop}{change}|>"
    return f"<|{{{control.var}}}|toggle{label_prop}{change}|>"  # toggle


def _build_sidebar_control(control: Control) -> str:
    """One sidebar filter control (the sidebar placement). A toggle has no outline of its own, so it sits in the
    `ll-filter-box` (a drawn outline plus an `ll-filter-label` floating label). A selector or an input is an MUI
    outlined control that draws its own outline and floating label, so it renders bare: boxing it too drew two
    outlines and squeezed its label (the v1 pattern boxes only the controls without an outline). The `ll-control`
    wrapper places the help icon at the control's right edge (styles.css)."""
    render = (
        f"<|part|class_name=ll-control|render={{{control.condition}}}|"
        if control.condition
        else "<|part|class_name=ll-control|"
    )
    parts = [render]
    if control.kind == "toggle":
        label = escape_label(control.label)
        parts += ["<|part|class_name=ll-filter-box ll-toggle-box|", "<|part|class_name=ll-filter-label|", label, "|>"]
        parts.append(_control_markup(control, label=False))  # the box carries the label
        parts.append("|>")
    else:
        parts.append(_control_markup(control))
    if control.help:
        parts.append(f'<span class="ll-help material-symbols-outlined" title="{control.help}">info</span>')
    parts.append("|>")
    return "\n".join(parts)


def build_sidebar_section(header: str, widgets: Sequence[Control], *, condition: str = "") -> str:
    """A sidebar section (a header plus its filter controls) with uniform spacing. `condition` gates the whole
    section; a widget without its own condition inherits it, so it hides and shows with the section."""
    base = "<|part|class_name=ll-sidebar-section|"
    open_tag = f"{base}render={{{condition}}}|" if condition else base
    parts = [open_tag, "<|part|class_name=ll-section-header|", f"### {header}", "|>", ""]
    for widget in widgets:
        effective = widget if widget.condition or not condition else replace(widget, condition=condition)
        parts.append(_build_sidebar_control(effective))
        parts.append("")
    parts.append("|>")
    return "\n".join(parts)


def _build_button(button: Button) -> str:
    cls = "|class_name=ll-primary" if button.primary else ""
    return f"<|{escape_label(button.label)}|button|on_action={button.action}{cls}|>"


def _build_button_row(buttons: Sequence[Button]) -> str:
    """Consecutive buttons as one action row (`ll-actions`): side by side with a uniform gap, so a group of actions
    reads as one unit instead of a loose vertical stack. The buttons share one line, so Taipy places them in one
    paragraph, which the row lays out."""
    return "<|part|class_name=ll-actions|\n" + " ".join(_build_button(button) for button in buttons) + "\n|>"


def _build_table(table: Table) -> str:
    height = f"|height={table.height}" if table.height else ""
    inner = f"<|{{{table.var}}}|table|show_all{height}|class_name={table.class_name}|>"
    if table.render_var:
        # the `table` control ignores `render`, so a no-data table is hidden by a `part` wrapper
        return f"<|part|render={{{table.render_var}}}|\n{inner}\n|>"
    return inner


def _build_elements(elements: Sequence[Element]) -> list[str]:
    """Render elements in order, merging each run of consecutive `Button`s into one action row (every page and every
    section gets the same row layout, with no per-page markup)."""
    parts: list[str] = []
    index = 0
    while index < len(elements):
        element = elements[index]
        if isinstance(element, Button):
            run: list[Button] = []
            while index < len(elements) and isinstance(elements[index], Button):
                run.append(cast("Button", elements[index]))
                index += 1
            parts.append(_build_button_row(run))
        else:
            parts.append(_build_element(element))
            index += 1
        parts.append("")
    return parts


def _build_element(element: Element) -> str:
    if isinstance(element, Prose):
        return element.markdown
    if isinstance(element, Text):
        return f"<|{{{element.var}}}|text|mode=markdown|>"
    if isinstance(element, Table):
        return _build_table(element)
    if isinstance(element, Button):
        return _build_button(element)
    if isinstance(element, Control):
        markup = _control_markup(element) + _help_icon(element.help)
        render = f"|render={{{element.condition}}}" if element.condition else ""
        if element.kind != "toggle" and element.help:
            # a selector or input fills the width, so an inline icon would wrap under it: the `ll-control` wrapper
            # places it at the control's right edge (a toggle keeps the icon inline, beside its label)
            return f"<|part|class_name=ll-control{render}|\n{markup}\n|>"
        if render:
            return f"<|part{render}|\n{markup}\n|>"
        return markup
    return _build_section(element)


def _build_section(section: Section) -> str:
    open_tag = (
        f"<|part|class_name=ll-section|render={{{section.render_var}}}|"
        if section.render_var
        else "<|part|class_name=ll-section|"
    )
    subtitle = f"<|{{{section.subtitle_var}}}|text|>" if section.subtitle_var else section.subtitle
    parts = [open_tag, "<|part|class_name=ll-subtitle|", subtitle, "|>", ""]
    parts += _build_elements(section.items)
    parts.append("|>")
    return "\n".join(parts)


def _build_notice() -> str:
    """The shared action-outcome notice, rendered at the foot of every page (S02) and hidden when empty
    (A-SPEC-07). `on_navigate` clears it before each page's refresh (S11), so it never follows the operator."""
    return (
        f"<|part|render={{len({NOTICE_VAR}) > 0}}|class_name=ll-action-notice|\n"
        f"<|{{{NOTICE_VAR}}}|text|mode=markdown|>\n|>"
    )


def build_page(cfg: PageConfig) -> str:
    """The page Markdown: the header, then the `body` elements in order, then the shared action notice. No metrics
    rail (removed: no cycle-1 page uses one — spec S12 / A-SPEC-09)."""
    parts = [build_header_from_config(cfg), ""]
    parts += _build_elements(cfg.body)
    parts.append(_build_notice())
    return "\n".join(parts)


def _iter_elements(elements: Sequence[Element]) -> Iterator[Element]:
    for element in elements:
        yield element
        if isinstance(element, Section):
            yield from _iter_elements(element.items)


def iter_controls(cfg: PageConfig) -> Iterator[Control]:
    """Every `Control` the page declares, in the body (inside any `Section`) and in `sidebar_widgets`. `shell`
    builds the `on_change` dispatch map and the unique-variable check from this (spec S03 / C06)."""
    for element in _iter_elements(cfg.body):
        if isinstance(element, Control):
            yield element
    yield from cfg.sidebar_widgets


def control_var_names(cfg: PageConfig) -> list[str]:
    """The bound variable of every control, for the duplicate-name check (the by-variable `on_change` dispatch needs
    unique names — spec C06)."""
    return [control.var for control in iter_controls(cfg)]


def assert_unique_control_vars(configs: Iterator[PageConfig] | Sequence[PageConfig]) -> None:
    """Raise if two controls across the pages bind the same variable. The Gui-level `on_change` dispatcher keys on
    the variable name, so a duplicate would be ambiguous (spec C06)."""
    seen: set[str] = set()
    for cfg in configs:
        for var in control_var_names(cfg):
            if var in seen:
                raise ValueError(f"duplicate control variable {var!r}: the on_change dispatch needs unique names")
            seen.add(var)


def action_names(cfg: PageConfig) -> tuple[str, ...]:
    """The distinct `on_action` names of every `Button` the page declares, in first-seen order. The page's `ACTIONS`
    derive from this, so the two never drift (spec S07c)."""
    seen: dict[str, None] = {}
    for element in _iter_elements(cfg.body):
        if isinstance(element, Button):
            seen.setdefault(element.action, None)
    return tuple(seen)
