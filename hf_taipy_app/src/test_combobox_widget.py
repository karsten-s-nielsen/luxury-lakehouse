"""Tests for SidebarWidget(kind="combobox") — WAI-ARIA APG combobox rendering.

The combobox kind emits a single `<|{var}|ll_ext.combobox|...|>` fragment
that maps to the in-repo Taipy GUI extension at
`hf_taipy_app/src/extensions/ll_ext/`. These tests check the emitted
Markdown contract; end-to-end ARIA / keyboard-nav behaviour is covered
by Puppeteer runs against the live app (see the Path C verification
matrix).
"""

from __future__ import annotations

from page_template import SidebarWidget, _build_sidebar_widget


def _make_combobox() -> SidebarWidget:
    return SidebarWidget(
        "combobox",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        depends_on="selected_competition",
        required=False,
        search_var="player_search_query",
        on_search_change="on_player_search_change",
        search_label="Search players",
    )


def test_combobox_emits_ll_ext_markup() -> None:
    md = _build_sidebar_widget(_make_combobox(), f=False)
    assert "<|{selected_player}|ll_ext.combobox" in md
    assert "|lov={player_lov}" in md
    assert "|label=Player (optional)" in md  # required=False appends suffix
    assert "|placeholder=Search players" in md
    assert "|search={player_search_query}" in md
    assert "|on_change=on_player_change" in md
    assert "|on_search=on_player_search_change" in md
    assert "|debounce_ms=300" in md
    assert "|class_name=ll-combobox" in md


def test_combobox_default_placeholder_when_search_label_empty() -> None:
    w = SidebarWidget(
        "combobox",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        search_var="q",
        on_search_change="cb",
        search_label="",
    )
    md = _build_sidebar_widget(w, f=False)
    assert "|placeholder=Type to search\u2026" in md


def test_combobox_required_true_has_no_optional_suffix() -> None:
    w = SidebarWidget(
        "combobox",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        required=True,
        search_var="q",
        on_search_change="cb",
    )
    md = _build_sidebar_widget(w, f=False)
    assert "|label=Player|" in md
    assert "(optional)" not in md


def test_combobox_render_condition_wraps_element() -> None:
    """Outer <|part|render=...|> wraps the combobox so it hides when depends_on fails."""
    md = _build_sidebar_widget(_make_combobox(), f=False)
    assert "render={selected_competition is not None}" in md
    render_pos = md.index("render={selected_competition is not None}")
    combobox_pos = md.index("|ll_ext.combobox")
    close_pos = md.rindex("|>")
    assert render_pos < combobox_pos < close_pos


def test_combobox_custom_change_delay() -> None:
    w = SidebarWidget(
        "combobox",
        "ps_selected_player",
        "Player",
        "on_ps_selected_player_change",
        lov="ps_player_lov",
        search_var="ps_player_search_query",
        on_search_change="on_ps_player_search_change",
        search_change_delay=500,
    )
    md = _build_sidebar_widget(w, f=False)
    assert "|debounce_ms=500" in md


def test_combobox_help_icon_still_rendered() -> None:
    w = SidebarWidget(
        "combobox",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        search_var="player_search_query",
        on_search_change="on_player_search_change",
        help="Search any player and pick one.",
    )
    md = _build_sidebar_widget(w, f=False)
    assert 'class="ll-help' in md
    assert 'title="Search any player and pick one."' in md
