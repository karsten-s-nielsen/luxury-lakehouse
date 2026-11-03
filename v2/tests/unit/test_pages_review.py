"""The review page as a bound Taipy page (page-binding plan PB7): it gains input controls for the six variables
`_collect` reads — decide_entity, accept_batch_filter, undo_batch_id, decide_item, decide_action, decide_reason — so
accept-recommended, undo-batch and decide become functional. `refresh` fills the queue and the selector options."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from lakehouse.ui.pages import review
from lakehouse.ui.template import action_names


def _cells(columns: dict[str, list[str]]) -> list[str]:
    return [cell for column in columns.values() for cell in column]


def _item(item_id: str, entity: str, *, recommended: bool) -> Any:
    return SimpleNamespace(
        item_id=item_id,
        entity=entity,
        left=SimpleNamespace(name="Record A", team="Team 1"),
        right=SimpleNamespace(name="Record B", team="Team 2"),
        score=90.0,
        score_meaning="very alike",
        recommended=recommended,
        held=False,
    )


class _Review:
    def queue(self, limit: int = 50) -> list[Any]:
        return [_item("i1", "player", recommended=True), _item("i2", "team", recommended=False)]

    def unreviewed_count(self) -> int:
        return 5

    def pending_requests(self) -> list[str]:
        return []

    def page(self, limit: int = 50) -> Any:
        from lakehouse.services.review import ReviewPage

        return ReviewPage(
            items=self.queue(limit), unreviewed_count=self.unreviewed_count(), pending=self.pending_requests()
        )

    def recent_batches(self, limit: int = 20) -> list[tuple[str, str]]:
        return [("b1", "2026-10-28 09:00 · player · all")]


def _services() -> Any:
    return SimpleNamespace(review=_Review())


def test_review_defaults() -> None:
    assert review.decide_entity == "" and review.accept_batch_filter == "" and review.undo_batch_id == ""
    assert review.decide_item == "" and review.decide_action == "confirm" and review.decide_reason == ""
    assert action_names(review.CONFIG) == ("on_accept_recommended", "on_decide", "on_undo_batch")


def test_review_refresh_fills() -> None:
    state = SimpleNamespace()
    review.refresh(state, _services())
    assert any("Record A" in c for c in _cells(state.review_rows))
    assert "links accepted without review" in state.review_notes
    assert state.review_item_ids == ["i1", "i2"]
    assert state.review_entity_options == ["", "player", "team"]  # "" = all, then the queue's kinds
    assert state.review_batch_options == [("b1", "2026-10-28 09:00 · player · all")]  # the undo selector (audit F4)


def test_review_page_builds() -> None:
    from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

    assert isinstance(review.page, Markdown)
