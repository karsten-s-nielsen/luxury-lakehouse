"""The AI/ML Workflows page (spec D9; plan Task 9): the calc-layer forward DAG + a per-card detail table.

The DAG is pipeline STRUCTURE (the typed `SLICE_CARDS`), not match data, so it is a static self-contained SVG
(`build_dag_html`, CSP-safe, no CDN) embedded through a Taipy content provider (a `Content` element bound to a `RawHtml`
var — an iframe, the only path that renders a raw `<svg>`; a `Prose` inline SVG is dropped by Taipy's Markdown as
"Component svg is not registered"). The page reads no match/team/player identity. The detail table lists every card
(step, type, status, note, dependencies) so the structure is readable without decoding the diagram. The note says why
a card is not plainly active (calc-layer spec D10).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.cards import SLICE_CARDS
from lakehouse.ui.pages._workflows_dag import build_dag_html, dag_frame_height
from lakehouse.ui.template import Content, NavSection, PageConfig, RawHtml, Table, build_page, table_columns

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = ["CONFIG", "ICON", "ROUTE", "TITLE", "page", "refresh"]

TITLE = "AI/ML Workflows"
ICON = "account_tree"
ROUTE = "workflows"
_CARD_HEADERS = ("Step", "Type", "Status", "Note", "Depends on")

CONFIG = PageConfig(
    title=TITLE,
    icon=ICON,
    nav_section=NavSection("Operations"),
    description="The calc-layer forward DAG: tracking and event sources through the compute layers to the mart.",
    body=(
        Content(var="workflows_dag_html", height=f"{dag_frame_height(SLICE_CARDS)}px"),
        Table("workflows_cards_rows"),
    ),
    reads_match_data=False,  # pipeline structure (card metadata) only — no match/team/player identity (spec §6.3)
)

# Bound client state (built at module scope). The DAG is static pipeline structure, built once and rendered through a
# content provider (an iframe) — the one path that renders a raw <svg> (Taipy's Markdown drops it); only the detail
# table is reactive.
workflows_dag_html: RawHtml = RawHtml(build_dag_html(SLICE_CARDS))
workflows_cards_rows: dict[str, list[str]] = {}


def refresh(state: Any, services: Services) -> None:
    """Fill the per-card detail table (display-only; the DAG itself is static structure in the page definition)."""
    rows = [
        {
            "Step": card.name,
            "Type": card.type,
            "Status": card.status,
            "Note": card.note or "—",
            "Depends on": ", ".join(card.depends_on) or "—",
        }
        for card in SLICE_CARDS
    ]
    state.workflows_cards_rows = table_columns(rows, _CARD_HEADERS)


page = Markdown(build_page(CONFIG))
