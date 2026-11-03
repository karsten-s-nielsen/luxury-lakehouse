"""Review decisions and requests: config-side objects (spec §8.3, §15.5, §15.6; plan Task 22).

The operator side writes them, always under `ConfigLock`; a run only reads them, once at its start, and records the
requests it applied in the catalog table `ops_applied_requests` — it never writes a config-side object.

- `config/decisions/<decision id>.json`: a must-link or cannot-link with who, when and why. The id is a hash of the
  content, so the same decision saved twice is one object, and a later decision on the same pair is a new object
  (the latest one wins).
- `config/requests/<request id>.json`: an "Accept recommended" or an "Undo batch" request. A request id names one
  request forever: saving other content under it is refused.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Final, Literal

import duckdb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

from lakehouse.config.store import ConfigLock
from lakehouse.errors import ConflictError
from lakehouse.ids.native import PROVIDERS
from lakehouse.resolve.model import Entity, RecordRef, pair
from lakehouse.resolve.tables import read_rows
from lakehouse.storage.base import Storage

__all__ = [
    "DECISIONS_PREFIX",
    "REQUESTS_PREFIX",
    "AcceptRequest",
    "CannotLink",
    "DecisionSet",
    "DecisionStore",
    "MustLink",
    "UndoRequest",
    "applied_requests",
    "new_request_id",
]

DECISIONS_PREFIX: Final = "config/decisions/"
REQUESTS_PREFIX: Final = "config/requests/"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class _Decision(_Frozen):
    entity: Entity
    a: RecordRef
    b: RecordRef
    decided_by: str
    decided_at: AwareDatetime
    reason: str

    @model_validator(mode="after")
    def _checked(self) -> _Decision:
        if not self.decided_by.strip():
            raise ValueError("a decision needs the operator's name")
        if self.a.provider == self.b.provider:
            raise ValueError(f"a decision pairs records of two providers, not {self.a} and {self.b}")
        return self

    @property
    def pair(self) -> tuple[RecordRef, RecordRef]:
        return pair(self.a, self.b)

    @property
    def decision_id(self) -> str:
        a, b = self.pair
        text = "|".join(
            (
                type(self).__name__,
                self.entity,
                a.provider,
                a.native_id,
                b.provider,
                b.native_id,
                self.decided_at.isoformat(),
            )
        )
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


class MustLink(_Decision):
    kind: Literal["must_link"] = "must_link"


class CannotLink(_Decision):
    kind: Literal["cannot_link"] = "cannot_link"


class _Request(_Frozen):
    request_id: str
    requested_by: str
    requested_at: AwareDatetime

    @field_validator("request_id")
    @classmethod
    def _safe_id(cls, value: str) -> str:
        if not _REQUEST_ID.match(value):
            raise ValueError(f"a request id is letters, digits, '.', '_' and '-' only: {value!r}")
        return value

    @field_validator("requested_by")
    @classmethod
    def _operator(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("a request needs the operator's name")
        return value


class AcceptRequest(_Request):
    """Accept the recommended candidate of every queued link of `entity` (all entities when `None`), or of the
    provider pair that `batch_filter` names (`"<provider>,<provider>"`)."""

    kind: Literal["accept"] = "accept"
    entity: Entity | None
    batch_filter: str | None

    @field_validator("batch_filter")
    @classmethod
    def _filter(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parts = [part.strip() for part in value.split(",")]
        if len(parts) != 2 or parts[0] == parts[1] or not all(part in PROVIDERS for part in parts):
            raise ValueError(f"a batch filter names two providers as '<provider>,<provider>', not {value!r}")
        return ",".join(sorted(parts))

    def selects(self, a: RecordRef, b: RecordRef) -> bool:
        return self.batch_filter is None or self.batch_filter == ",".join(sorted((a.provider, b.provider)))


class UndoRequest(_Request):
    """Undo every link of one batch accepted as recommended, and hold its pairs (§15.6)."""

    kind: Literal["undo"] = "undo"
    batch_id: str


Item = MustLink | CannotLink | AcceptRequest | UndoRequest
_ITEM: TypeAdapter[Item] = TypeAdapter(Annotated[Item, Field(discriminator="kind")])
_DECISIONS: TypeAdapter[list[MustLink | CannotLink]] = TypeAdapter(
    list[Annotated[MustLink | CannotLink, Field(discriminator="kind")]]
)


@dataclass(frozen=True)
class DecisionSet:
    """The decisions and requests of one run, read once at its start."""

    must_links: list[MustLink]
    cannot_links: list[CannotLink]
    accept_requests: list[AcceptRequest]
    undo_requests: list[UndoRequest]


def _key(item: Item) -> str:
    if isinstance(item, MustLink | CannotLink):
        return f"{DECISIONS_PREFIX}{item.decision_id}.json"
    return f"{REQUESTS_PREFIX}{item.request_id}.json"


def new_request_id(now: datetime) -> str:
    return f"req-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


class DecisionStore:
    """The review decisions and requests of one lake."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage
        self._lock = ConfigLock(storage)

    def _read(self, key: str) -> Item | None:
        try:
            raw = self._storage.get("lake", key)
        except FileNotFoundError:
            return None
        return _ITEM.validate_json(raw)

    def load(self) -> DecisionSet:
        items = [
            _ITEM.validate_json(self._storage.get("lake", info.key))
            for prefix in (DECISIONS_PREFIX, REQUESTS_PREFIX)
            for info in sorted(self._storage.list("lake", prefix), key=lambda i: i.key)
        ]
        return DecisionSet(
            must_links=sorted(
                (i for i in items if isinstance(i, MustLink)), key=lambda d: (d.decided_at, d.decision_id)
            ),
            cannot_links=sorted(
                (i for i in items if isinstance(i, CannotLink)), key=lambda d: (d.decided_at, d.decision_id)
            ),
            accept_requests=sorted(
                (i for i in items if isinstance(i, AcceptRequest)), key=lambda r: (r.requested_at, r.request_id)
            ),
            undo_requests=sorted(
                (i for i in items if isinstance(i, UndoRequest)), key=lambda r: (r.requested_at, r.request_id)
            ),
        )

    def _put(self, item: Item) -> bool:
        checked = _ITEM.validate_python(item.model_dump())  # a model_copy is not validated: check it here
        key = _key(checked)
        with self._lock:
            existing = self._read(key)
            if existing is None:
                self._storage.put("lake", key, checked.model_dump_json(indent=2).encode("utf-8"))
                return True
            if existing == checked:
                return False
        raise ConflictError(f"{key} already holds another object: a request or decision is never changed")

    def add(self, item: Item) -> None:
        self._put(item)

    def export(self, path: Path) -> int:
        """Write every must-link and cannot-link to `path` as JSON; requests are not exported. The count."""
        loaded = self.load()
        decisions = sorted([*loaded.must_links, *loaded.cannot_links], key=lambda d: (d.decided_at, d.decision_id))
        path.write_text(
            json.dumps([d.model_dump(mode="json") for d in decisions], indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return len(decisions)

    def import_(self, path: Path) -> int:
        """Add the decisions of an exported file; the count of new ones (the ones already here are skipped)."""
        decisions: Sequence[MustLink | CannotLink] = _DECISIONS.validate_json(path.read_bytes())
        return sum(self._put(decision) for decision in decisions)


def applied_requests(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """The ids of every request a published run applied (`ops_applied_requests`)."""
    return {str(row["request_id"]) for row in read_rows(conn, "ops_applied_requests")}
