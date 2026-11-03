"""The review queue's service (spec §15.5, §15.6, §19.5; plan Task 25).

The queue reads `ops_review_queue` with both records' attributes from `ops_entity_records` (names, birth date, team,
jersey, position, matches: never a raw id). A decision or a request is a config-side object written through
`DecisionStore` under the config lock; the next run applies it. "N links accepted without review" counts the links
accepted as recommended.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final, Literal

import duckdb

from lakehouse.resolve.decisions import (
    AcceptRequest,
    CannotLink,
    DecisionStore,
    MustLink,
    UndoRequest,
    applied_requests,
    new_request_id,
)
from lakehouse.resolve.model import ENTITIES, Entity, RecordRef
from lakehouse.resolve.tables import link_table, read_rows
from lakehouse.services.catalog import read_catalog
from lakehouse.storage.base import Storage

__all__ = ["RANKING_LIMIT", "QueueItem", "RecordView", "ReviewPage", "ReviewService", "score_meaning"]

RANKING_LIMIT: Final = 500  # spec §19.5: a ranking shows at most 500 rows


@dataclass(frozen=True)
class RecordView:
    name: str
    birth_date: date | None
    team: str | None
    jersey: str | None
    position: str | None
    matches: int | None


@dataclass(frozen=True)
class QueueItem:
    item_id: str
    entity: Entity
    left: RecordView
    right: RecordView
    score: float
    score_meaning: str
    recommended: bool
    held: bool


@dataclass(frozen=True)
class ReviewPage:
    """Everything the Review page shows, gathered in ONE catalog attach (review V2UI-IMPL-18)."""

    items: list[QueueItem]
    unreviewed_count: int
    pending: list[str]


def score_meaning(score: float, *, recommended: bool, held: bool, measured: bool) -> str:
    """The score in plain words: its scale and direction, and what it means for the queue (spec §15.5)."""
    scale = f"score {score:.0f} of 100, higher is more alike"
    if held:
        return f"held after an undo: decide it by hand ({scale})"
    if not measured:
        return f"waits for review: no measured bands for this kind yet ({scale})"
    if recommended:
        return f"recommended: clearly the best candidate ({scale})"
    return f"waits for review: not clearly better than the next candidate ({scale})"


def _view(record: dict[str, Any] | None) -> RecordView:
    if record is None:
        return RecordView("(record no longer in the lake)", None, None, None, None, None)
    return RecordView(
        name=str(record["name"]),
        birth_date=record["birth_date"],
        team=record["team"],
        jersey=record["jersey"],
        position=record["position"],
        matches=record["matches"],
    )


class ReviewService:
    def __init__(self, storage: Storage, *, clock: Any = None) -> None:
        self._storage = storage
        self._store = DecisionStore(storage)
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))

    def _rows(self) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
        def read(
            conn: duckdb.DuckDBPyConnection,
        ) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
            records = {
                (str(r["entity"]), str(r["provider"]), str(r["native_id"])): r
                for r in read_rows(conn, "ops_entity_records")
            }
            return read_rows(conn, "ops_review_queue"), records

        return read_catalog(self._storage, read, empty=([], {}))

    def _items(
        self, rows: list[dict[str, Any]], records: dict[tuple[str, str, str], dict[str, Any]], limit: int
    ) -> list[QueueItem]:
        ordered = sorted(rows, key=lambda r: (not r["recommended"], -float(r["score"]), str(r["item_id"])))
        items: list[QueueItem] = []
        for row in ordered[:limit]:
            entity = str(row["entity"])
            measured = not str(row["reason"]).startswith("no measured bands")
            items.append(
                QueueItem(
                    item_id=str(row["item_id"]),
                    entity=entity,  # type: ignore[arg-type]
                    left=_view(records.get((entity, str(row["provider_a"]), str(row["native_id_a"])))),
                    right=_view(records.get((entity, str(row["provider_b"]), str(row["native_id_b"])))),
                    score=float(row["score"]),
                    score_meaning=score_meaning(
                        float(row["score"]),
                        recommended=bool(row["recommended"]),
                        held=bool(row["held"]),
                        measured=measured,
                    ),
                    recommended=bool(row["recommended"]),
                    held=bool(row["held"]),
                )
            )
        return items

    def queue(self, limit: int = RANKING_LIMIT) -> list[QueueItem]:
        """The queue, recommended items first, then by score; at most `limit` (a ranking: at most 500)."""
        if not 1 <= limit <= RANKING_LIMIT:
            raise ValueError(f"limit must be 1 to {RANKING_LIMIT}")
        rows, records = self._rows()
        return self._items(rows, records, limit)

    def page(self, limit: int = RANKING_LIMIT) -> ReviewPage:
        """Everything the Review page shows, in ONE catalog attach (review V2UI-IMPL-18): the ranked queue, the
        accepted-without-review count, and the pending request ids. The page's `refresh` calls this once, instead of
        `queue()` + `unreviewed_count()` + `pending_requests()` — three separate attaches. One attach of the Review
        page's data renders in ~0.7 s warm on the fixture lake (this session's per-page measurement; the reads
        dominate, not the attach count). The §19.5 cached budget is 500 ms, met on HF where the object cache warms."""
        if not 1 <= limit <= RANKING_LIMIT:
            raise ValueError(f"limit must be 1 to {RANKING_LIMIT}")

        def read(
            conn: duckdb.DuckDBPyConnection,
        ) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]], set[str], int]:
            records = {
                (str(r["entity"]), str(r["provider"]), str(r["native_id"])): r
                for r in read_rows(conn, "ops_entity_records")
            }
            rows = read_rows(conn, "ops_review_queue")
            applied = applied_requests(conn)
            unreviewed = sum(
                1
                for entity in ENTITIES
                for r in read_rows(conn, link_table(entity))
                if r["accepted_by"] == "recommended"
            )
            return rows, records, applied, unreviewed

        rows, records, applied, unreviewed = read_catalog(self._storage, read, empty=([], {}, set[str](), 0))
        decisions = self._store.load()
        requested = [*decisions.accept_requests, *decisions.undo_requests]
        pending = [r.request_id for r in sorted(requested, key=lambda r: r.requested_at) if r.request_id not in applied]
        return ReviewPage(items=self._items(rows, records, limit), unreviewed_count=unreviewed, pending=pending)

    def decide(
        self, item_id: str, action: Literal["confirm", "reject", "later"], *, operator: str, reason: str
    ) -> None:
        """Confirm (must link) or reject (cannot link) a queue item; "later" leaves it in the queue."""
        rows, _ = self._rows()
        row = next((r for r in rows if r["item_id"] == item_id), None)
        if row is None:
            raise KeyError(f"no queue item {item_id!r} in the current lake: reload the queue")
        if action == "later":
            return
        a = RecordRef(row["provider_a"], str(row["native_id_a"]))
        b = RecordRef(row["provider_b"], str(row["native_id_b"]))
        kind = MustLink if action == "confirm" else CannotLink
        self._store.add(
            kind(entity=row["entity"], a=a, b=b, decided_by=operator, decided_at=self._clock(), reason=reason)
        )

    def accept_recommended(self, entity: Entity | None, batch_filter: str | None, *, operator: str) -> str:
        """An "Accept recommended" request for the next run; its id (which is also the batch id)."""
        request = AcceptRequest(
            request_id=new_request_id(self._clock()),
            entity=entity,
            batch_filter=batch_filter,
            requested_by=operator,
            requested_at=self._clock(),
        )
        self._store.add(request)
        return request.request_id

    def undo_batch(self, batch_id: str, *, operator: str) -> str:
        """An "Undo batch" request for the next run; its id."""
        request = UndoRequest(
            request_id=new_request_id(self._clock()),
            batch_id=batch_id,
            requested_by=operator,
            requested_at=self._clock(),
        )
        self._store.add(request)
        return request.request_id

    def recent_batches(self, limit: int = 20) -> list[tuple[str, str]]:
        """Recent accept batches, most recent first, as (batch_id, label) for the Review undo selector (audit F4). The
        batch id is the accept request id; the label is its time, entity and filter, so the operator picks a batch
        instead of typing a raw id. A store read only (no catalog attach), so it does not widen the §19.5 page
        budget."""
        batches = sorted(self._store.load().accept_requests, key=lambda r: r.requested_at, reverse=True)[:limit]
        return [
            (r.request_id, f"{r.requested_at:%Y-%m-%d %H:%M} · {r.entity or 'all'} · {r.batch_filter or 'all'}")
            for r in batches
        ]

    def export(self, path: Path) -> int:
        """`lakehouse decisions export`: every must-link and cannot-link to a JSON file (requests stay); the count."""
        return self._store.export(path)

    def import_(self, path: Path) -> int:
        """`lakehouse decisions import`: the decisions of an exported file; the count of new ones."""
        return self._store.import_(path)

    def pending_requests(self) -> list[str]:
        """The requests that no published run has applied yet ("pending until the next run")."""
        applied = read_catalog(self._storage, applied_requests, empty=set[str]())
        decisions = self._store.load()
        requested = [*decisions.accept_requests, *decisions.undo_requests]
        return [r.request_id for r in sorted(requested, key=lambda r: r.requested_at) if r.request_id not in applied]

    def unreviewed_count(self) -> int:
        """ "N links accepted without review": the links accepted as recommended."""

        def read(conn: duckdb.DuckDBPyConnection) -> int:
            return sum(
                1
                for entity in ENTITIES
                for r in read_rows(conn, link_table(entity))
                if r["accepted_by"] == "recommended"
            )

        return read_catalog(self._storage, read, empty=0)
