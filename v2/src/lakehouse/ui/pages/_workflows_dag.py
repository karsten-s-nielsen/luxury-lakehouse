"""Render the calc-layer cards as a static SVG DAG for the AI/ML Workflows page (spec D9; plan Task 9).

The page cannot use a CDN charting library (the v2 CSP blocks external hosts) and the cards are a fixed, small graph,
so the DAG is a self-contained inline SVG built here — no JavaScript, no external asset, CSP-safe. Nodes are laid out
in columns by their longest dependency depth (so the two compute layers read left to right), coloured by card type,
with one edge per `depends_on` entry. A card that is not `active` (planned, excluded, incomplete) has a dashed
outline, its status as a second text line, and its note as the hover title (calc-layer spec D10). This module is
Taipy-free and pure (cards -> HTML string), so it is unit-tested directly, like `_monitoring_data`.
"""

from __future__ import annotations

from collections.abc import Sequence
from html import escape

from lakehouse.cards import WorkflowCard

__all__ = ["build_dag_html", "dag_frame_height"]

_COL_W = 240  # horizontal gap between dependency layers
_ROW_H = 76  # vertical gap between nodes in a layer
_NODE_W = 196
_NODE_H = 48
_MARGIN = 20

# Node fill per card type. Legible dark text on light fills in both themes; the planned sink is a muted grey.
_FILL: dict[str, str] = {
    "source": "#dbeafe",
    "compute": "#bfdbfe",
    "view": "#dcfce7",
    "mart": "#fde68a",
    "sink": "#e5e7eb",
}
_LEGEND = ("source", "compute", "view", "mart", "sink")


def _depth(cards: Sequence[WorkflowCard]) -> dict[str, int]:
    """Each card's longest dependency chain length — its column. A source is 0; a node is 1 + the deepest parent."""
    by_id = {card.id: card for card in cards}
    memo: dict[str, int] = {}

    def depth(cid: str) -> int:
        if cid not in memo:
            deps = by_id[cid].depends_on
            memo[cid] = 0 if not deps else 1 + max(depth(dep) for dep in deps)
        return memo[cid]

    return {card.id: depth(card.id) for card in cards}


def _layout(cards: Sequence[WorkflowCard]) -> tuple[dict[str, tuple[int, int]], int, int]:
    """Place each card at (x, y) by dependency depth (its column) and row in that column, and return the positions with
    the SVG's pixel width and height. `build_dag_html` draws from this; `dag_frame_height` sizes the content-provider
    iframe from the SAME computation, so the frame and the diagram cannot drift."""
    depth = _depth(cards)
    layers: dict[int, list[WorkflowCard]] = {}
    for card in cards:
        layers.setdefault(depth[card.id], []).append(card)

    pos: dict[str, tuple[int, int]] = {}
    for layer_index, column in sorted(layers.items()):
        for row_index, card in enumerate(column):
            pos[card.id] = (_MARGIN + layer_index * _COL_W, _MARGIN + row_index * _ROW_H)

    width = _MARGIN + (max(layers) + 1) * _COL_W
    height = _MARGIN + max(len(column) for column in layers.values()) * _ROW_H + 60  # + legend row
    return pos, width, height


def dag_frame_height(cards: Sequence[WorkflowCard]) -> int:
    """The SVG's pixel height — the AI/ML Workflows page sizes the content-provider iframe to it (the Taipy content
    part does not auto-size). Shares `_layout` with `build_dag_html`, so the frame matches the diagram exactly."""
    return _layout(cards)[2]


def build_dag_html(cards: Sequence[WorkflowCard]) -> str:
    """A self-contained inline-SVG DAG of the cards (no JS, no CDN; CSP-safe), embedded on the page through a Taipy
    content provider (an iframe — `template.Content` + a `RawHtml` var), which is the only path that renders a raw
    `<svg>` (Taipy's Markdown->JSX layer drops it). One `<g data-card-id=...>` per card and one
    `<line class="ll-dag-edge">` per `depends_on` edge."""
    pos, width, height = _layout(cards)

    edges: list[str] = []
    for card in cards:
        x2, y2 = pos[card.id]
        for dep in card.depends_on:
            x1, y1 = pos[dep]
            edges.append(
                f'<line class="ll-dag-edge" x1="{x1 + _NODE_W}" y1="{y1 + _NODE_H // 2}" '
                f'x2="{x2}" y2="{y2 + _NODE_H // 2}" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#ll-arrow)" />'
            )

    nodes: list[str] = []
    for card in cards:
        x, y = pos[card.id]
        fill = _FILL.get(card.type, "#e5e7eb")
        active = card.status == "active"
        dash = "" if active else ' stroke-dasharray="4 3"'
        title = f"<title>{escape(card.note)}</title>" if card.note else ""
        name_y = y + _NODE_H // 2 + (4 if active else -2)
        status_line = (
            ""
            if active
            else f'<text x="{x + _NODE_W // 2}" y="{y + _NODE_H // 2 + 13}" text-anchor="middle" font-size="10" '
            f'font-style="italic" fill="#475569">{escape(card.status)}</text>'
        )
        nodes.append(
            f'<g data-card-id="{escape(card.id)}">{title}'
            f'<rect x="{x}" y="{y}" width="{_NODE_W}" height="{_NODE_H}" rx="8" fill="{fill}" '
            f'stroke="#475569" stroke-width="1"{dash} />'
            f'<text x="{x + _NODE_W // 2}" y="{name_y}" text-anchor="middle" '
            f'font-size="12" fill="#0f172a">{escape(card.name)}</text>'
            f"{status_line}"
            "</g>"
        )

    legend_y = height - 34
    legend: list[str] = []
    for i, kind in enumerate(_LEGEND):
        lx = _MARGIN + i * 120
        legend.append(
            f'<rect x="{lx}" y="{legend_y}" width="14" height="14" rx="3" fill="{_FILL[kind]}" '
            f'stroke="#475569" stroke-width="1" />'
            f'<text x="{lx + 20}" y="{legend_y + 12}" font-size="11" fill="#475569">{kind}</text>'
        )
    lx = _MARGIN + len(_LEGEND) * 120
    legend.append(
        f'<rect x="{lx}" y="{legend_y}" width="14" height="14" rx="3" fill="#ffffff" '
        f'stroke="#475569" stroke-width="1" stroke-dasharray="4 3" />'
        f'<text x="{lx + 20}" y="{legend_y + 12}" font-size="11" fill="#475569">'
        "dashed: planned, excluded or incomplete (hover a step for why)</text>"
    )

    marker = (
        '<defs><marker id="ll-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#94a3b8" /></marker></defs>'
    )
    return (
        f'<div class="ll-dag" role="img" aria-label="Calc-layer workflow DAG">'
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f"{marker}{''.join(edges)}{''.join(nodes)}{''.join(legend)}"
        "</svg></div>"
    )
