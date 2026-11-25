"""The AI/ML Workflows page (spec D9; plan Task 9): registration + the DAG render."""

from __future__ import annotations

from html import escape
from types import SimpleNamespace
from typing import Any, cast

from lakehouse.cards import SLICE_CARDS
from lakehouse.ui.pages import workflows
from lakehouse.ui.pages._workflows_dag import build_dag_html, dag_frame_height
from lakehouse.ui.shell import PAGE_MODULES
from lakehouse.ui.template import Content, Prose, RawHtml


def test_workflows_page_registered() -> None:
    assert "workflows" in PAGE_MODULES  # the shell builds it into the nav + routes
    assert workflows.ROUTE == "workflows"
    assert workflows.CONFIG.nav_section.name  # a goal-oriented nav section (not an implementation label)
    assert workflows.CONFIG.reads_match_data is False  # pipeline structure only — no match identity


def test_dag_html_has_a_node_per_card_and_edges_from_depends_on() -> None:
    html = build_dag_html(SLICE_CARDS)
    assert "<svg" in html
    for card in SLICE_CARDS:  # one node per card
        assert f'data-card-id="{card.id}"' in html, card.id
    expected_edges = sum(len(card.depends_on) for card in SLICE_CARDS)  # one edge per depends_on entry
    assert html.count('class="ll-dag-edge"') == expected_edges


def test_dag_frame_height_matches_the_svg_height() -> None:
    """The content-provider iframe is sized to the SVG's own pixel height (the content part does not auto-size). The
    helper and the SVG share one layout computation, so the frame cannot clip or over-pad the diagram."""
    height = dag_frame_height(SLICE_CARDS)
    assert height > 0
    assert f'height="{height}"' in build_dag_html(SLICE_CARDS)  # the SVG's own height attribute


def test_dag_embedded_via_content_provider_not_prose() -> None:
    """The DAG is embedded through a RawHtml content provider (an iframe), NOT inline `Prose`. Taipy's Markdown->JSX
    layer drops a raw `<svg>` ("Component svg is not registered", seen live on the Mac 2026-11-05), so `Prose(svg)`
    renders nothing. The module binds a `RawHtml` DAG var the page embeds with `<|part|content=…|>`."""
    body = workflows.CONFIG.body
    assert any(isinstance(element, Content) for element in body)  # a content provider, not Prose
    assert not any(isinstance(element, Prose) for element in body)  # no inline-SVG Prose (it would be dropped)
    dag = workflows.workflows_dag_html
    assert isinstance(dag, RawHtml)  # the bound var is the opaque-HTML marker the shell's provider renders
    assert "<svg" in str(dag) and "data-card-id=" in str(dag)  # it carries the real SVG DAG


def _node(html: str, card_id: str) -> str:
    return html.split(f'data-card-id="{card_id}"', 1)[1].split("</g>", 1)[0]


def test_dag_marks_every_card_that_is_not_active() -> None:
    """An excluded, incomplete or planned card must not look like a working one (calc-layer spec D10, anti-goal D6):
    a dashed outline, the status as a second text line, and the note as the hover title."""
    html = build_dag_html(SLICE_CARDS)
    for card in SLICE_CARDS:
        node = _node(html, card.id)
        if card.status == "active":
            assert "stroke-dasharray" not in node, card.id
            continue
        assert "stroke-dasharray" in node, card.id
        assert f">{card.status}<" in node, card.id
        if card.note:
            assert f"<title>{escape(card.note)}</title>" in node, card.id
    legend = html.rsplit("</g>", 1)[1]  # everything after the last node
    for status in ("planned", "excluded", "incomplete"):  # the legend explains the dashed outline
        assert status in legend, status


def test_cards_table_shows_the_status_and_the_note() -> None:
    state = SimpleNamespace(workflows_cards_rows={})
    workflows.refresh(state, cast(Any, None))
    rows: dict[str, list[str]] = state.workflows_cards_rows
    assert list(rows) == ["Step", "Type", "Status", "Note", "Depends on"]
    for card in SLICE_CARDS:
        i = rows["Step"].index(card.name)
        assert rows["Status"][i] == card.status, card.id
        assert rows["Note"][i] == (card.note or "—"), card.id
