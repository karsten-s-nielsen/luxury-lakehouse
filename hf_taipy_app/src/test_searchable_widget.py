"""Tests for SidebarWidget(searchable=True) — server-driven autocomplete rendering."""

from __future__ import annotations

from page_template import SidebarWidget, _build_sidebar_widget


def _make_searchable_dropdown() -> SidebarWidget:
    return SidebarWidget(
        "dropdown",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        depends_on="selected_competition",
        required=False,
        searchable=True,
        search_var="player_search_query",
        on_search_change="on_player_search_change",
        search_label="Search players",
    )


def test_searchable_default_false() -> None:
    """Backwards-compatible default: existing widgets without `searchable` are unaffected."""
    w = SidebarWidget("dropdown", "selected_team", "Team", "on_team_change", lov="team_lov")
    assert w.searchable is False
    md = _build_sidebar_widget(w, f=False)
    # No input, no search class, just the selector.
    assert "|input|" not in md
    assert "ll-search-input" not in md
    assert "|selector|lov={team_lov}" in md


def test_searchable_true_renders_input_and_selector() -> None:
    """searchable=True renders BOTH a debounced input AND an inline scrollable list selector.

    Critically the selector does NOT have the |dropdown| flag — it renders as a plain
    always-visible list so typed-search results appear live without a click.
    """
    md = _build_sidebar_widget(_make_searchable_dropdown(), f=False)
    # Input markup with the search query var, debounce, callback, label, and class
    assert "<|{player_search_query}|input" in md
    assert "on_change=on_player_search_change" in md
    assert "change_delay=300" in md
    assert "label=Search players" in md
    assert "class_name=ll-search-input" in md
    # Selector renders as inline list (not dropdown) with ll-search-results class
    assert "<|{selected_player}|selector|lov={player_lov}" in md
    assert "class_name=ll-search-results" in md
    assert "|dropdown" not in md  # CRITICAL: no dropdown — inline list for live results
    assert "|filter" not in md  # client-side filter must not coexist with backend search


def test_searchable_render_condition_wraps_pair() -> None:
    """Outer <|part|render=...|> wraps both elements so they hide together when depends_on fails."""
    md = _build_sidebar_widget(_make_searchable_dropdown(), f=False)
    # depends_on=selected_competition produces "selected_competition is not None" condition
    assert "render={selected_competition is not None}" in md
    # The render wrapper opens before the input and closes after the selector.
    render_pos = md.index("render={selected_competition is not None}")
    input_pos = md.index("|input")
    selector_pos = md.index("|selector|")
    close_pos = md.rindex("|>")
    assert render_pos < input_pos < selector_pos < close_pos


def test_searchable_custom_change_delay() -> None:
    """search_change_delay is configurable per widget (default 300 ms)."""
    w = SidebarWidget(
        "dropdown",
        "ps_selected_player",
        "Player",
        "on_ps_selected_player_change",
        lov="ps_player_lov",
        searchable=True,
        search_var="ps_player_search_query",
        on_search_change="on_ps_player_search_change",
        search_change_delay=500,
    )
    md = _build_sidebar_widget(w, f=False)
    assert "change_delay=500" in md


def test_searchable_help_icon_still_rendered() -> None:
    """help= produces an info icon — must work with searchable too."""
    w = SidebarWidget(
        "dropdown",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        searchable=True,
        search_var="player_search_query",
        on_search_change="on_player_search_change",
        help="Search any player and pick one.",
    )
    md = _build_sidebar_widget(w, f=False)
    assert 'class="ll-help' in md
    assert 'title="Search any player and pick one."' in md


def test_searchable_does_not_emit_filter_flag() -> None:
    """Setting both filterable=True and searchable=True must not produce |filter|.

    Backend search supersedes client-side filter; emitting both would double-filter.
    """
    w = SidebarWidget(
        "dropdown",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        searchable=True,
        search_var="player_search_query",
        on_search_change="on_player_search_change",
        filterable=True,  # ignored when searchable=True
    )
    md = _build_sidebar_widget(w, f=False)
    assert "|filter" not in md
