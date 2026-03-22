"""Shared page template — generates consistent layout for all pages and sidebar widgets.

Page content: build_page() with PageConfig / SubView.
Sidebar widgets: build_sidebar_widgets() with SidebarWidget list.
All structural patterns, CSS classes, and wrapping live here — never in page files.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Literal

# ---------------------------------------------------------------------------
# Sidebar widget template
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SidebarWidget:
    """One widget in the sidebar filter panel.

    The template handles ALL wrapping and styling. The widget definition
    provides only data: what type, which variable, which label, which callback,
    and when to show it. Zero styling information.
    """

    kind: Literal["dropdown", "dropdown_multi", "slider", "toggle"]
    var: str  # state variable name
    label: str  # human-readable label
    on_change: str  # callback function name
    # Visibility condition (Taipy expression string). Empty = always visible.
    condition: str = ""
    # Dropdown-specific
    lov: str = ""  # lov variable name (required for dropdown/dropdown_multi)
    # Slider-specific
    slider_min: str = ""  # min variable or literal
    slider_max: str = ""  # max variable or literal
    slider_step: str = ""  # step (optional)
    slider_range_labels: tuple[str, str] = ("", "")  # static (min_label, max_label)
    slider_range_vars: tuple[str, str] = ("", "")  # dynamic: (min_var, max_var) — Taipy bindings
    # Toggle filter-box label override (if empty, uses self.label)
    filter_box_label: str = ""
    # Dependency fields — template generates render conditions from these.
    # depends_on: parent state variable name. Widget hidden until parent is not None.
    depends_on: str = ""
    # depends_value: show only when parent equals this specific value (requires depends_on).
    depends_value: str = ""
    # depends_lov_populated: show only when this widget's own LOV has entries.
    depends_lov_populated: bool = False
    # Slider-specific: debounce delay in ms. Callback fires only after user stops
    # moving for this duration. Prevents expensive re-renders during drag.
    change_delay: int = 0
    help: str = ""  # tooltip help text (rendered as info icon next to widget)


def _build_render_condition(w: SidebarWidget) -> str:
    """Build compound render condition from page visibility + dependency fields.

    Combines condition (page visibility), depends_on (parent check),
    depends_value (parent value match), and depends_lov_populated (LOV gate)
    into a single Taipy expression string joined with 'and'.
    """
    parts: list[str] = []
    if w.condition:
        parts.append(w.condition)
    if w.depends_on:
        if w.depends_value:
            parts.append(f'{w.depends_on} == "{w.depends_value}"')
        else:
            parts.append(f"{w.depends_on} is not None")
    if w.depends_lov_populated and w.lov:
        parts.append(f"len({w.lov}) > 0")
    return " and ".join(parts) if parts else ""


def _slider_val(v: str, lb: str, rb: str) -> str:
    """Format a slider min/max value — numeric literals stay bare, variable names get braces."""
    try:
        float(v)
        return v
    except ValueError:
        return f"{lb}{v}{rb}"


def _build_sidebar_widget(w: SidebarWidget, f: bool) -> str:
    """Generate markdown for a single sidebar widget with consistent wrapping.

    Args:
        w: The widget definition.
        f: True if inside an f-string context (double braces needed).

    All widgets get identical <|part|render=...|> wrapping for uniform spacing.
    """
    lb = "{{" if f else "{"
    rb = "}}" if f else "}"

    parts: list[str] = []

    # Outer wrapper — render condition combines page visibility + dependencies
    render_cond = _build_render_condition(w)
    if render_cond:
        parts.append(f"<|part|render={lb}{render_cond}{rb}|")
    else:
        parts.append("<|part|")

    if w.kind in ("dropdown", "dropdown_multi"):
        multi = "|multiple" if w.kind == "dropdown_multi" else ""
        parts.append(
            f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}|dropdown|label={w.label}|on_change={w.on_change}|>"
        )

    elif w.kind == "slider":
        box_label = w.filter_box_label or w.label
        parts.append("<|part|class_name=ll-filter-box|")
        parts.append("<|part|class_name=ll-filter-label|")
        parts.append(box_label)
        parts.append("|>")

        step_attr = f"|step={w.slider_step}" if w.slider_step else ""
        delay_attr = f"|change_delay={w.change_delay}" if w.change_delay else ""
        parts.append(
            f"<|{lb}{w.var}{rb}|slider"
            f"|min={_slider_val(w.slider_min, lb, rb)}|max={_slider_val(w.slider_max, lb, rb)}"
            f"{step_attr}{delay_attr}|on_change={w.on_change}|>"
        )
        has_range = w.slider_range_labels[0] or w.slider_range_vars[0] or w.slider_range_vars[1]
        if has_range:
            # Range labels below slider. Each label is either static or dynamic.
            # Use Taipy <|part|> for the container (not HTML <span>) so
            # dynamic Taipy text bindings render correctly.
            start_label = w.slider_range_labels[0]
            end_label = w.slider_range_labels[1]
            start_var = w.slider_range_vars[0]
            end_var = w.slider_range_vars[1]

            parts.append("")
            parts.append("<|part|class_name=ll-slider-range|")
            if start_var:
                parts.append(f"<|{lb}{start_var}{rb}|text|class_name=ll-range-start|>")
            else:
                parts.append(f"<|{{'{start_label}'}}|text|class_name=ll-range-start|raw|>")
            parts.append("")  # blank line forces separate md-para blocks
            if end_var:
                parts.append(f"<|{lb}{end_var}{rb}|text|class_name=ll-range-end|>")
            else:
                parts.append(f"<|{{'{end_label}'}}|text|class_name=ll-range-end|raw|>")
            parts.append("|>")
        parts.append("|>")  # close filter-box

    elif w.kind == "toggle":
        box_label = w.filter_box_label or w.label
        parts.append("<|part|class_name=ll-filter-box ll-toggle-box|")
        parts.append("<|part|class_name=ll-filter-label|")
        parts.append(box_label)
        parts.append("|>")
        parts.append(f"<|{lb}{w.var}{rb}|toggle|on_change={w.on_change}|>")
        parts.append("|>")  # close filter-box

    if w.help:
        parts.append(f'<span class="ll-help material-symbols-outlined" title="{w.help}">info</span>')

    parts.append("|>")  # close outer wrapper
    return "\n".join(parts)


def build_sidebar_section(
    header: str,
    widgets: list[SidebarWidget],
    condition: str = "",
    f_string: bool = True,
) -> str:
    """Generate a complete sidebar section (header + widgets) with uniform structure.

    Args:
        header: Section heading text (e.g., "Filters", "Search").
        widgets: List of widget definitions.
        condition: Taipy render condition for the entire section header.
        f_string: True if output will be embedded in an f-string (double braces).
    """
    lb = "{{" if f_string else "{"
    rb = "}}" if f_string else "}"
    parts: list[str] = []

    # Section container — flex column with gap for uniform spacing.
    # Hidden widgets (render=false) collapse and gap adjusts automatically.
    if condition:
        parts.append(f"<|part|class_name=ll-sidebar-section|render={lb}{condition}{rb}|")
    else:
        parts.append("<|part|class_name=ll-sidebar-section|")

    # Section header
    parts.append("<|part|class_name=ll-section-header|")
    parts.append(f"### {header}")
    parts.append("|>")
    parts.append("")

    # Widgets — each individually wrapped for uniform spacing.
    # Widgets without their own condition inherit the section condition.
    for w in widgets:
        effective_w = w
        if not w.condition and condition:
            # Inherit section condition so widget hides with the section.
            # replace() preserves depends_on/depends_value/depends_lov_populated.
            effective_w = replace(w, condition=condition)
        parts.append(_build_sidebar_widget(effective_w, f_string))
        parts.append("")

    parts.append("|>")  # close section container
    return "\n".join(parts)


def build_nav(registry: list[PageEntry]) -> str:
    """Generate sidebar nav markdown from page registry.

    Groups pages by nav_section, preserving registration order.
    Section header order = first page encountered for each section.
    """
    sections: dict[str, list[PageEntry]] = {}
    for entry in registry:
        sections.setdefault(entry.config.nav_section, []).append(entry)

    parts: list[str] = []
    for section_name, entries in sections.items():
        parts.append(f"<|part|class_name=ll-nav-header|\n**{section_name}**\n|>\n")
        for entry in entries:
            parts.append(
                f'[<span class="material-symbols-outlined">{entry.config.icon}</span>'
                f" {entry.config.title}](/{entry.route})\n"
            )
    return "\n".join(parts)


@dataclass(frozen=True)
class ContentBlock:
    """One piece of content in the diagram area.

    Pages provide only data — kind, variable, optional header/caption.
    The template controls all structural wrapping and styling.
    """

    kind: Literal["image", "table", "text", "expandable_table"]
    var: str  # state variable name
    header: str = ""  # optional subtitle above the block (ll-subtitle)
    condition: str = ""  # Taipy render condition (empty = auto len(var) > 0)
    table_page_size: int = 50  # table and expandable_table only
    caption: str = ""  # static text below the block
    caption_var: str = ""  # dynamic caption (state variable)
    caption_condition: str = ""  # render condition for caption


@dataclass(frozen=True)
class ContentRow:
    """One horizontal row of ContentBlocks in the diagram area.

    Single block = full width. Multiple blocks = CSS grid columns.
    Use explicit columns to force alignment across consecutive rows.
    """

    blocks: list[ContentBlock] = field(default_factory=list)
    columns: int = 0  # 0 = auto (len(blocks)), explicit forces grid column count
    condition: str = ""  # row-level render condition (gates entire row)


@dataclass(frozen=True)
class Metric:
    """A single metric displayed in the right column.

    Pages provide only data (label, variable, help text, delta).
    The template controls all styling — no CSS class names in page definitions.
    """

    label: str
    var: str
    help_text: str = ""
    delta_var: str = ""
    lower_is_better: bool = False  # True = inverse delta styling (e.g., Brier Score)


@dataclass(frozen=True)
class Citation:
    """An academic or tool reference. Template controls rendering."""

    label: str  # e.g., "Rathke (2017)" or "XGBoost"
    url: str = ""  # link URL (empty = plain text)


@dataclass(frozen=True)
class PageConfig:
    """Everything unique to a page. The template handles the rest.

    Pages provide ONLY data — icon, title, description, citations, content vars.
    All structural wrapping, CSS classes, and styling are template-controlled.
    """

    title: str
    icon: str
    nav_section: str  # "Match Analysis", "Player Analysis", "Advanced"
    description: str  # plain text describing the page (no markdown styling)
    citations: list[Citation] = field(default_factory=list)  # academic/tool references
    empty_message: str = ""  # static text shown when no data
    empty_condition: str = ""  # Taipy render condition for empty state
    metrics: list[Metric] = field(default_factory=list)
    content: list[ContentRow] = field(default_factory=list)
    # Optional: scope/status variables shown above the image
    scope_vars: list[str] = field(default_factory=list)
    # Optional: data freshness variable shown below the image
    freshness_var: str = ""
    warning_var: str = ""  # state variable for "no data" warnings (ll-warning-box)
    footer_var: str = ""  # state variable for footer text (citations, scope notes)
    # Optional: multi-view sub-views (when set, replaces single-view layout)
    sub_views: list[SubView] = field(default_factory=list)


@dataclass(frozen=True)
class PageEntry:
    """One page in the navigation registry. Order in the registry list = display order."""

    route: str
    config: PageConfig
    markdown: str


@dataclass(frozen=True)
class SubView:
    """One view in a multi-view page (e.g., Rankings, Breakdown, Timeline).

    If metrics are provided, generates 3fr/1fr layout (content left, metrics right).
    If no metrics, generates full-width content (table or image).
    """

    condition: str  # Taipy render condition, e.g., 'selected_sub_view == "Rankings"'
    content: list[ContentRow] = field(default_factory=list)
    # Scale reference notes rendered above the content grid as ll-reference blocks.
    scale_notes: list[str] = field(default_factory=list)
    # Right column
    metrics: list[Metric] = field(default_factory=list)
    # Empty state (primary)
    empty_message: str = ""
    empty_condition: str = ""
    # Empty state (fallback — shown when primary doesn't match, e.g., "no tracking data")
    fallback_empty_message: str = ""
    fallback_empty_condition: str = ""
    warning_var: str = ""
    scope_vars: list[str] = field(default_factory=list)


def _build_metric(m: Metric) -> str:
    """Generate markdown for a single metric block.

    All styling decisions are made here — pages only provide data.
    """
    help_span = ""
    if m.help_text:
        help_span = f' <span class="ll-help material-symbols-outlined" title="{m.help_text}">info</span>'

    lines = [
        "<|part|class_name=ll-metric|",
        f"{m.label}{help_span}",
        "",
        f"### <|{{{m.var}}}|text|>",
    ]

    if m.delta_var:
        delta_class = "ll-metric-delta-inverse" if m.lower_is_better else "ll-metric-delta"
        lines.append("")
        lines.append(f"<|{{{m.delta_var}}}|text|class_name={delta_class}|>")

    lines.append("|>")
    return "\n".join(lines)


def _build_content_block(block: ContentBlock, page_title: str) -> str:
    """Generate markdown for a single content block.

    Auto-generates render condition len(var) > 0 when condition is empty.
    """
    parts: list[str] = []
    cond = block.condition or f"len({block.var}) > 0"

    # Outer wrapper with render condition
    parts.append(f"<|part|render={{{cond}}}|")

    # Optional header (subtitle above content — skip for expandable_table
    # where header is the toggle label, not a separate subtitle)
    if block.header and block.kind != "expandable_table":
        parts.append("<|part|class_name=ll-subtitle|")
        parts.append(block.header)
        parts.append("|>")

    # Content by kind
    if block.kind == "image":
        parts.append(f"<|{{{block.var}}}|image|label={page_title}|width=100%|>")
    elif block.kind == "table":
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}|>")
    elif block.kind == "text":
        parts.append(f"<|{{{block.var}}}|text|>")
    elif block.kind == "expandable_table":
        # header is required for expandable — used as toggle label.
        parts.append(f"<|{block.header}|expandable|expanded=False|")
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}|>")
        parts.append("|>")

    parts.append("|>")  # close render condition

    # Optional caption (static or dynamic)
    if block.caption:
        parts.append("<|part|class_name=ll-reference|")
        parts.append(block.caption)
        parts.append("|>")
    if block.caption_var:
        cap_cond = block.caption_condition or f"len({block.caption_var}) > 0"
        parts.append(f"<|part|render={{{cap_cond}}}|class_name=ll-reference|")
        parts.append(f"<|{{{block.caption_var}}}|text|>")
        parts.append("|>")

    return "\n".join(parts)


def _build_content_row(row: ContentRow, page_title: str) -> str:
    """Generate markdown for a content row (one or more blocks).

    Single block = full width. Multiple blocks = CSS grid.
    """
    parts: list[str] = []

    # Row-level render condition
    if row.condition:
        parts.append(f"<|part|render={{{row.condition}}}|")

    n_cols = row.columns or len(row.blocks)
    if len(row.blocks) > 1:
        parts.append(f"<|part|class_name=ll-content-row ll-content-cols-{n_cols}|")

    for block in row.blocks:
        parts.append(_build_content_block(block, page_title))
        parts.append("")

    if len(row.blocks) > 1:
        parts.append("|>")  # close grid

    if row.condition:
        parts.append("|>")  # close row condition

    return "\n".join(parts)


def _build_sub_view(sv: SubView, page_title: str) -> str:
    """Generate markdown for a single conditional sub-view.

    ALWAYS uses 3fr/1fr CSS grid layout. The right column is reserved for
    metrics — if no metrics, the right column is empty but still takes space.
    This prevents content from spilling into the metrics area.

    Taipy limitation: <|layout|> inside <|part|render=...|> loses column
    definitions. Workaround: use <|part|class_name=ll-grid-3-1|> (CSS grid).
    """
    parts: list[str] = []

    # Outer wrapper with render condition
    parts.append(f"<|part|render={{{sv.condition}}}|")
    parts.append("")

    # Scale reference notes (template-controlled styling)
    for note in sv.scale_notes:
        parts.append("<|part|class_name=ll-reference|")
        parts.append(note)
        parts.append("|>")

    # Scope variables (above content grid, below scale notes)
    for scope_v in sv.scope_vars:
        parts.append(f"<|part|render={{len({scope_v}) > 0}}|")
        parts.append(f"<|{{{scope_v}}}|text|>")
        parts.append("|>")
        parts.append("")

    # ALWAYS use 3fr/1fr grid — right column reserved even if empty
    parts.append("<|part|class_name=ll-grid-3-1|")
    parts.append("")

    # Left column: content
    parts.append("<|part|")

    for row in sv.content:
        parts.append(_build_content_row(row, page_title))
        parts.append("")

    if sv.empty_condition:
        parts.append(f"<|part|render={{{sv.empty_condition}}}|class_name=ll-info-box|")
        parts.append(sv.empty_message)
        parts.append("|>")

    if sv.fallback_empty_condition:
        parts.append(f"<|part|render={{{sv.fallback_empty_condition}}}|class_name=ll-info-box|")
        parts.append(sv.fallback_empty_message)
        parts.append("|>")

    if sv.warning_var:
        parts.append(f"<|part|render={{len({sv.warning_var}) > 0}}|class_name=ll-warning-box|")
        parts.append(f"<|{{{sv.warning_var}}}|text|>")
        parts.append("|>")

    parts.append("|>")  # close left column
    parts.append("")

    # Right column: metrics (or empty reserved space)
    parts.append("<|part|class_name=ll-metrics-column|")
    for m in sv.metrics:
        parts.append(_build_metric(m))
        parts.append("")
    parts.append("|>")
    parts.append("")

    parts.append("|>")  # close grid part

    parts.append("")
    parts.append("|>")  # close render condition part
    return "\n".join(parts)


def _render_citations(citations: list[Citation]) -> str:
    """Render citation list as formatted reference text. Template-controlled styling."""
    if not citations:
        return ""
    parts = []
    for c in citations:
        if c.url:
            parts.append(f"[{c.label}]({c.url})")
        else:
            parts.append(c.label)
    return " ".join(parts)


def build_header_from_config(cfg: PageConfig) -> str:
    """Generate the standard page header from PageConfig.

    Pages provide only data (title, icon, description, citations).
    All structural wrapping and styling is template-controlled.
    """
    ref_text = cfg.description
    citation_text = _render_citations(cfg.citations)
    if citation_text:
        ref_text = f"{ref_text} {citation_text}" if ref_text else citation_text

    return "\n".join(
        [
            "<|part|class_name=ll-page-header|",
            f'## <span class="material-symbols-outlined">{cfg.icon}</span> {cfg.title}',
            "",
            "<|part|class_name=ll-reference|",
            ref_text,
            "|>",
            "|>",
            "",
            "---",
        ]
    )


def build_page(cfg: PageConfig) -> str:
    """Generate the standard page template markdown from config."""
    # --- Page header (constrained width) ---
    parts = [build_header_from_config(cfg), ""]

    if cfg.sub_views:
        # Multi-view page: conditional sub-view blocks
        for sv in cfg.sub_views:
            parts.append(_build_sub_view(sv, cfg.title))
            parts.append("")
    else:
        # Single-view page: standard 3fr/1fr layout
        parts.append("<|layout|columns=3fr 1fr|gap=1rem|")
        parts.append("")

        # Left column: scope, diagram, freshness
        parts.append("<|part|")

        # Scope variables
        for sv in cfg.scope_vars:
            parts.append(f"<|part|render={{len({sv}) > 0}}|")
            parts.append(f"<|{{{sv}}}|text|>")
            parts.append("|>")
            parts.append("")

        # Content rows
        for row in cfg.content:
            parts.append(_build_content_row(row, cfg.title))
            parts.append("")

        # Empty state
        if cfg.empty_condition:
            parts.append(f"<|part|render={{{cfg.empty_condition}}}|class_name=ll-info-box|")
            parts.append(f"{cfg.empty_message}")
            parts.append("|>")

        # Warning state (no-data — amber box, distinct from guidance)
        if cfg.warning_var:
            parts.append(f"<|part|render={{len({cfg.warning_var}) > 0}}|class_name=ll-warning-box|")
            parts.append(f"<|{{{cfg.warning_var}}}|text|>")
            parts.append("|>")

        # Data freshness
        if cfg.freshness_var:
            parts.append("")
            parts.append(f"<|part|render={{len({cfg.freshness_var}) > 0}}|")
            parts.append(f"<|{{{cfg.freshness_var}}}|text|class_name=ll-reference|>")
            parts.append("|>")

        if cfg.footer_var:
            parts.append("")
            parts.append(f"<|part|render={{len({cfg.footer_var}) > 0}}|")
            parts.append(f"<|{{{cfg.footer_var}}}|text|class_name=ll-reference|>")
            parts.append("|>")

        parts.append("|>")
        parts.append("")

        # Right column: metrics
        parts.append("<|part|class_name=ll-metrics-column|")
        for m in cfg.metrics:
            parts.append(_build_metric(m))
            parts.append("")
        parts.append("|>")
        parts.append("")

        parts.append("|>")

    return "\n".join(parts)
