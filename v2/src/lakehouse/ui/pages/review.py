"""The Review queue page (spec §15.5, §19.5): each pair to review with both records' names (never a raw id), the
match score with its scale and direction spelled out, and the markers for fallbacks — "N links accepted without
review", and "not listed here" when the queue is longer than the page shows."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.services.review import RANKING_LIMIT, QueueItem
from lakehouse.ui.template import (
    Button,
    Control,
    NavSection,
    PageConfig,
    Section,
    Table,
    Text,
    build_page,
    table_columns,
)

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = ["CONFIG", "ICON", "NOT_LISTED", "ROUTE", "TITLE", "page", "refresh"]

TITLE = "Review queue"
ICON = "rule"
ROUTE = "review"
NOT_LISTED = "not listed here"
CONFIG = PageConfig(
    title=TITLE,
    icon=ICON,
    nav_section=NavSection("Operations"),
    description="Each candidate pair to review, with both records' names and the match score.",
    body=(
        Text("review_notes"),
        Table("review_rows"),
        Section(
            "Accept recommended",
            (
                Control(
                    "selector",
                    "decide_entity",
                    label="Entity",
                    help="Limit the recommended batch to one entity kind; blank accepts all kinds.",
                    lov="review_entity_options",
                ),
                Control(
                    "input",
                    "accept_batch_filter",
                    label="Batch filter",
                    help="Only accept recommended pairs whose batch id contains this text.",
                ),
                Button("Accept recommended", "on_accept_recommended", primary=True),
            ),
        ),
        Section(
            "Decide one pair",
            (
                Control(
                    "selector",
                    "decide_item",
                    label="Item",
                    help="The one candidate pair to decide.",
                    lov="review_item_ids",
                ),
                Control(
                    "selector",
                    "decide_action",
                    label="Action",
                    help="confirm the match, reject it, or hold it for later.",
                    lov="decide_action_options",
                ),
                Control("input", "decide_reason", label="Reason", help="A short note stored with the decision."),
                Button("Decide selected", "on_decide"),
            ),
        ),
        Section(
            "Undo",
            (
                Control(
                    "selector",
                    "undo_batch_id",
                    label="Batch",
                    help="Pick a recent accept batch to undo; it reverses that batch's accepted links on the next run.",
                    lov="review_batch_options",
                    value_by_id=True,  # lov is (batch_id, label); bind the id, not the tuple (audit F4)
                ),
                Button("Undo last batch", "on_undo_batch"),
            ),
        ),
    ),
)

# Bound client state (page-binding PB7): the six names the review actions read, plus the display and the selector
# options (built at module scope, fact 8).
decide_entity: str = ""
accept_batch_filter: str = ""
undo_batch_id: str = ""
decide_item: str = ""
decide_action: str = "confirm"
decide_reason: str = ""
review_notes: str = ""  # the notes + score explanation shown above the table
review_rows: dict[str, list[str]] = {}  # the candidate-pair native table (Taipy column format)
review_item_ids: list[str] = []
review_entity_options: list[str] = [""]
decide_action_options: list[str] = ["confirm", "reject", "later"]
review_batch_options: list[tuple[str, str]] = []  # (batch_id, label) for the undo selector (audit F4)

_REVIEW_HEADERS = ("Kind", "Records", "Score", "What the score means", "State")


def _record(name_pair: QueueItem) -> str:
    left, right = name_pair.left, name_pair.right
    left_team = f" ({left.team})" if left.team else ""
    right_team = f" ({right.team})" if right.team else ""
    return f"{left.name}{left_team} ↔ {right.name}{right_team}"


def _flag(item: QueueItem) -> str:
    return "recommended" if item.recommended else ("held" if item.held else "review")


def _item_dict(item: QueueItem) -> dict[str, str]:
    return {
        "Kind": item.entity,
        "Records": _record(item),
        "Score": f"{item.score:.0f}",
        "What the score means": item.score_meaning,
        "State": _flag(item),
    }


def _notes(accepted: int, pending: list[str], queue_len: int) -> str:
    """The notes above the table, from the already-fetched counts (no service calls here — `refresh` gets everything
    in one `services.review.page()` attach)."""
    notes = [f"**{accepted} links accepted without review** (accepted as recommended)."]
    if queue_len >= RANKING_LIMIT:
        notes.append(f"Showing the first {RANKING_LIMIT}; more are {NOT_LISTED}.")
    if pending:
        notes.append(f"{len(pending)} request(s) wait for the next run.")
    return (
        f"{'\n\n'.join(notes)}\n\n"
        "The score runs 0 to 100; higher is more alike. Each row says what the score means for that pair.\n"
    )


def refresh(state: Any, services: Services) -> None:
    """Fill the queue + notes and the selector options (§4). The decide/undo inputs bind directly. One
    `services.review.page()` call gathers the queue, the accepted count and the pending requests in a SINGLE catalog
    attach (the Review page's §19.5 cached budget is 500 ms; three separate attaches overran it — V2UI-IMPL-18)."""
    page = services.review.page()
    queue = page.items
    state.review_notes = _notes(page.unreviewed_count, page.pending, len(queue))
    state.review_rows = table_columns([_item_dict(item) for item in queue], _REVIEW_HEADERS)
    state.review_item_ids = [item.item_id for item in queue]
    state.review_entity_options = ["", *sorted({item.entity for item in queue})]
    state.review_batch_options = services.review.recent_batches()  # the undo selector options (audit F4)


page = Markdown(build_page(CONFIG))
