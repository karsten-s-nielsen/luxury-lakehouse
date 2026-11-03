"""The declarative Ops page template (plan Task 28): the generated Taipy Markdown has the lakehouse structure, the
ordered `body` renders in order, and the shared helpers (`action_names`, `iter_controls`, `escape_label`) hold."""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.ui.template import (
    Button,
    Control,
    NavSection,
    PageConfig,
    PageEntry,
    Prose,
    Section,
    Table,
    Text,
    action_names,
    assert_unique_control_vars,
    build_header_from_config,
    build_nav,
    build_page,
    build_sidebar_section,
    control_var_names,
    escape_label,
    iter_controls,
)

_SECTION = NavSection("Operations")


def _cfg(**changes: Any) -> PageConfig:
    return PageConfig(title="Runs", icon="play_circle", nav_section=_SECTION, **changes)


def test_header_has_icon_title_description() -> None:
    md = build_header_from_config(_cfg(description="The run history."))
    assert md.lstrip().startswith("## ")  # a Markdown heading (renders in a page definition), not Taipy control markup
    assert 'material-symbols-outlined">play_circle</span> Runs' in md
    assert '<span class="ll-reference">The run history.</span>' in md
    assert "---" not in md  # NO thematic break: a `---` before a <|{var}|text|> control kills its reactive binding


def test_body_renders_elements_in_order() -> None:
    cfg = _cfg(
        body=(
            Prose("A fixed sentence."),
            Text("runs_progress"),
            Table("runs_rows"),
            Button("Run now", "on_run_now", primary=True),
        )
    )
    md = build_page(cfg)
    assert "## " in md and "play_circle" in md  # the Markdown header
    assert "A fixed sentence." in md  # the static prose element (no variable)
    assert "<|{runs_progress}|text|mode=markdown|>" in md
    assert "<|{runs_rows}|table|show_all|height=24rem|class_name=ll-table|>" in md
    assert "<|Run now|button|on_action=on_run_now|class_name=ll-primary|>" in md
    # order: prose before text before table before button
    assert md.index("A fixed sentence.") < md.index("{runs_progress}") < md.index("{runs_rows}") < md.index("Run now")


def test_body_has_no_metrics_rail() -> None:
    md = build_page(_cfg(body=(Text("x"),)))
    assert "ll-metrics-column" not in md and "columns=4fr 1fr" not in md  # the metrics rail was removed (S12)


def test_table_render_var_wraps_in_part() -> None:
    md = build_page(_cfg(body=(Table("rows", render_var="has_run"),)))
    assert "<|part|render={has_run}|" in md  # a no-data table hides via a part wrapper (the table ignores render)
    assert "<|{rows}|table|show_all|height=24rem|class_name=ll-table|>" in md


def test_every_table_is_height_bounded() -> None:
    """A table caps its scroll box (Taipy's default 80vh filled the screen with one long table); a page may set its
    own height. Seen to fail: drop `height` from `_build_table` and the markup carries no bound."""
    assert "|height=24rem|" in build_page(_cfg(body=(Table("rows"),)))
    assert "|height=12rem|" in build_page(_cfg(body=(Table("rows", height="12rem"),)))


def test_consecutive_buttons_share_one_action_row() -> None:
    """A run of buttons renders as ONE `ll-actions` row on one line (side by side), not a loose vertical stack; a
    button separated by another element starts its own row."""
    md = build_page(_cfg(body=(Button("A", "on_a"), Button("B", "on_b", primary=True), Text("x"), Button("C", "on_c"))))
    assert md.count("<|part|class_name=ll-actions|") == 2
    row = "<|A|button|on_action=on_a|> <|B|button|on_action=on_b|class_name=ll-primary|>"
    assert row in md  # both buttons on one line, so Taipy lays them out in one paragraph
    assert action_names(_cfg(body=(Button("A", "on_a"), Button("B", "on_b")))) == ("on_a", "on_b")


def test_section_reactive_subtitle_and_render_gate() -> None:
    """A Section may title itself from a variable (the picked source's name, never a generic "This source") and hide
    itself until a variable is true (no control acts on an unpicked source)."""
    md = build_page(_cfg(body=(Section("Source", (Text("x"),), subtitle_var="heading", render_var="loaded"),)))
    assert "<|part|class_name=ll-section|render={loaded}|" in md
    assert "<|{heading}|text|>" in md and "\nSource\n" not in md  # the reactive title replaces the fixed one


def test_section_groups_items_with_subtitle() -> None:
    cfg = _cfg(
        body=(
            Section(
                "Change a source",
                (
                    Control("selector", "active_module", label="Source", lov="module_ids"),
                    Button("Apply", "on_go", True),
                ),
            ),
        )
    )
    md = build_page(cfg)
    assert "ll-section" in md and "Change a source" in md
    assert "<|{active_module}|selector|lov={module_ids}|dropdown|label=Source|>" in md
    assert md.index("Change a source") < md.index("active_module")  # subtitle before the control


def test_value_by_id_selector_emits_the_flag() -> None:
    """A selector with an (id, label) lov sets value_by_id so Taipy binds the id, not the whole tuple
    (V2UI-IMPL-23). A string-lov selector emits no value_by_id."""
    tuple_lov = build_sidebar_section("F", (Control("selector", "src", label="S", lov="opts", value_by_id=True),))
    assert "value_by_id=True" in tuple_lov
    string_lov = build_page(_cfg(body=(Section("S", (Control("selector", "x", label="X", lov="o"),)),)))
    assert "value_by_id" not in string_lov


def test_control_on_change_names_the_shared_callback() -> None:
    """A marked control names the ONE shared callback, never its own handler (Taipy would call the handler without
    the services; the shared callback forwards to the dispatcher — spec S03). An unmarked control emits none."""
    md = build_page(_cfg(body=(Section("F", (Control("input", "q", label="Q", on_change="on_q"),)),)))
    assert "|on_change=ll_on_control_change|" in md and "on_q" not in md
    plain = build_page(_cfg(body=(Section("F", (Control("input", "q", label="Q"),)),)))
    assert "on_change" not in plain


def test_shared_notice_rendered_on_every_page() -> None:
    """build_page always emits the shared ll_action_notice at the foot, hidden when empty (S02 / A-SPEC-07)."""
    md = build_page(_cfg(body=(Text("x"),)))  # a page with no notice field
    assert "ll-action-notice" in md
    assert "render={len(ll_action_notice) > 0}" in md  # hidden on empty
    assert "<|{ll_action_notice}|text|mode=markdown|>" in md


def test_build_nav_groups_by_section_in_order() -> None:
    data = NavSection("Data")
    registry = [
        PageEntry("runs", _cfg()),
        PageEntry("modules", PageConfig(title="Modules", icon="dataset", nav_section=data)),
    ]
    nav = build_nav(registry)
    assert "**Operations**" in nav and "**Data**" in nav
    assert nav.index("Operations") < nav.index("Data")
    assert "](/runs)" in nav and "](/modules)" in nav


def test_sidebar_section_places_filter_controls() -> None:
    widgets = (
        Control("selector", "filter_module", label="Source", lov="module_ids", on_change="on_pick"),
        Control("toggle", "draft_360", label="360 only"),
    )
    md = build_sidebar_section("Filters", widgets, condition='current_page == "filters"')
    assert "ll-sidebar-section" in md and "### Filters" in md
    # the marked control names the shared callback, never its own handler ("on_pick")
    assert "<|{filter_module}|selector|lov={module_ids}|dropdown|label=Source|on_change=ll_on_control_change|>" in md
    assert "ll-toggle-box" in md and "<|{draft_360}|toggle|>" in md
    assert 'render={current_page == "filters"}' in md


def test_sidebar_boxes_only_controls_without_an_outline() -> None:
    """A selector or input (an MUI outlined control with its own outline and floating label) renders bare; only a
    toggle gets the drawn `ll-filter-box`. Seen to fail: box the selector too and it draws two outlines and squeezes
    its label ("Tournaments" showed as "To…")."""
    selector = build_sidebar_section("F", (Control("selector", "v", label="L", lov="lov"),))
    field = build_sidebar_section("F", (Control("input", "q", label="Q"),))
    toggle = build_sidebar_section("F", (Control("toggle", "t", label="T"),))
    assert "ll-filter-box" not in selector and "ll-filter-box" not in field
    assert "ll-filter-box ll-toggle-box" in toggle and "ll-filter-label" in toggle


def test_widget_inherits_section_condition() -> None:
    md = build_sidebar_section("F", (Control("selector", "v", label="L", lov="lov"),), condition="show")
    assert md.count("render={show}") >= 2  # the section container and the widget both gate on the condition


def test_action_names_and_controls_walk_sections() -> None:
    cfg = _cfg(
        body=(
            Button("Top", "on_top"),
            Section("S", (Control("input", "a"), Button("In", "on_in"), Button("Top", "on_top"))),
        ),
        sidebar_widgets=(Control("selector", "b"),),
    )
    assert action_names(cfg) == ("on_top", "on_in")  # distinct, first-seen order, across sections
    assert control_var_names(cfg) == ["a", "b"]  # controls in the body (inside a Section) and the sidebar
    assert [c.var for c in iter_controls(cfg)] == ["a", "b"]


def test_escape_label_neutralizes_control_delimiters() -> None:
    out = escape_label("a|b{c}d")
    assert "|" not in out and "{" not in out and "}" not in out  # spec C01: a label cannot break the control


def test_duplicate_control_var_is_rejected() -> None:
    """Two controls that bind the same variable are rejected at build time — the by-variable on_change dispatch
    needs unique names (spec C06)."""
    one = _cfg(body=(Section("A", (Control("input", "dup"),)),))
    two = _cfg(body=(Section("B", (Control("toggle", "dup"),)),))
    assert_unique_control_vars([one])  # a single page with distinct vars is fine
    with pytest.raises(ValueError, match="duplicate control variable 'dup'"):
        assert_unique_control_vars([one, two])
