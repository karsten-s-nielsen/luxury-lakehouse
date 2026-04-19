"""Tests for the SidebarWidget.required field + (optional) label auto-suffix."""

from __future__ import annotations

from page_template import SidebarWidget, _build_sidebar_widget


def test_sidebar_widget_required_defaults_true() -> None:
    """New field has safe default — existing widgets unchanged."""
    w = SidebarWidget("dropdown", "selected_team", "Team", "on_team_change", lov="team_lov")
    assert w.required is True


def test_sidebar_widget_required_false_label_gets_optional_suffix() -> None:
    """required=False appends ' (optional)' to the rendered label."""
    w = SidebarWidget(
        "dropdown",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        required=False,
    )
    md = _build_sidebar_widget(w, f=False)
    assert "label=Player (optional)" in md


def test_sidebar_widget_required_true_label_unchanged() -> None:
    """Bare label is preserved when required=True."""
    w = SidebarWidget(
        "dropdown",
        "selected_competition",
        "Competition",
        "on_competition_change",
        lov="competition_lov",
        required=True,
    )
    md = _build_sidebar_widget(w, f=False)
    assert "label=Competition|" in md
    assert "(optional)" not in md
