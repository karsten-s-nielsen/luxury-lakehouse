"""The truth set of the resolution quality gate (spec §15.4 to §15.6, §29.3; plan Tasks 22 and 34).

The repository holds it as ids only, one CSV per entity type under `tests/fixtures/truthset/`. Only the owner labels
it (`truthset_owner`). The owner's hand-checked queue decisions also grow it; a link accepted as recommended never
does. `measure` compares a truth set with the link table of its entity type.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

import duckdb
from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator

from lakehouse.ids.native import Provider
from lakehouse.resolve.decisions import CannotLink, DecisionSet, MustLink
from lakehouse.resolve.model import PLURALS, Entity, RecordRef, pair
from lakehouse.resolve.tables import link_table, read_rows

__all__ = [
    "TRUTHSET_COLUMNS",
    "QualityReport",
    "TruthRow",
    "measure",
    "read_truthset",
    "truthset_file",
    "truthset_rows_from_decisions",
    "validate_truthset",
]

TRUTHSET_COLUMNS: Final = (
    "provider_a",
    "native_id_a",
    "provider_b",
    "native_id_b",
    "label",
    "labelled_by",
    "labelled_at",
    "evidence_ref",
)
_RECOMMENDED_EVIDENCE: Final = "batch:"


class TruthRow(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_a: Provider
    native_id_a: str
    provider_b: Provider
    native_id_b: str
    label: Literal["match", "non_match"]
    labelled_by: str
    labelled_at: AwareDatetime
    evidence_ref: str

    @model_validator(mode="after")
    def _checked(self) -> TruthRow:
        if self.provider_a == self.provider_b:
            raise ValueError("a truth row pairs records of two providers")
        if not self.evidence_ref.strip():
            raise ValueError("a truth row needs its evidence reference")
        return self

    @property
    def pair(self) -> tuple[RecordRef, RecordRef]:
        return pair(RecordRef(self.provider_a, self.native_id_a), RecordRef(self.provider_b, self.native_id_b))


def validate_truthset(rows: Sequence[TruthRow], *, owner: str | None) -> None:
    """Refuse a truth set with a row that the owner did not label, a row from a link accepted as recommended, or a
    pair labelled twice."""
    if not owner:
        raise ValueError("set truthset_owner first: only the owner labels the truth set")
    seen: set[tuple[RecordRef, RecordRef]] = set()
    for row in rows:
        if row.labelled_by != owner:
            raise ValueError(f"a truth row is labelled by {row.labelled_by!r}, not by the owner")
        if row.evidence_ref.startswith(_RECOMMENDED_EVIDENCE):
            raise ValueError("a link accepted as recommended never enters the truth set")
        if row.pair in seen:
            raise ValueError(f"the pair {row.pair} is labelled twice")
        seen.add(row.pair)


@dataclass(frozen=True)
class QualityReport:
    """Precision and recall over the labelled pairs; `None` when nothing measures it (never a made-up 1.0)."""

    precision: float | None
    recall: float | None
    auto_band_precision: float | None
    labelled: int


def _ratio(hits: int, total: int) -> float | None:
    return hits / total if total else None


def measure(conn: duckdb.DuckDBPyConnection, rows: Sequence[TruthRow], entity: Entity) -> QualityReport:
    """Precision of every link and of the automatic band, and recall after queue review, of `entity`'s links."""
    links = {
        pair(RecordRef(r["provider_a"], str(r["native_id_a"])), RecordRef(r["provider_b"], str(r["native_id_b"]))): str(
            r["accepted_by"]
        )
        for r in read_rows(conn, link_table(entity))
    }
    hits = misses = false = auto_hits = auto_false = 0
    for row in rows:
        accepted_by = links.get(row.pair)
        if row.label == "match":
            if accepted_by is None:
                misses += 1
            else:
                hits += 1
                auto_hits += accepted_by == "auto"
        elif accepted_by is not None:
            false += 1
            auto_false += accepted_by == "auto"
    return QualityReport(
        precision=_ratio(hits, hits + false),
        recall=_ratio(hits, hits + misses),
        auto_band_precision=_ratio(auto_hits, auto_hits + auto_false),
        labelled=len(rows),
    )


def truthset_rows_from_decisions(decisions: DecisionSet, *, owner: str, entity: Entity) -> list[TruthRow]:
    """The truth rows of the owner's hand-checked decisions on `entity` (§15.5): a must-link is a `match`, a
    cannot-link a `non_match`; the latest decision on a pair wins. Requests and their batches give no row."""
    latest: dict[tuple[RecordRef, RecordRef], MustLink | CannotLink] = {}
    owned = (
        d for d in [*decisions.must_links, *decisions.cannot_links] if d.entity == entity and d.decided_by == owner
    )
    for decision in sorted(owned, key=lambda d: (d.decided_at, d.decision_id)):
        latest[decision.pair] = decision
    rows: list[TruthRow] = []
    for (a, b), decision in sorted(latest.items()):
        rows.append(
            TruthRow(
                provider_a=a.provider,
                native_id_a=a.native_id,
                provider_b=b.provider,
                native_id_b=b.native_id,
                label="match" if isinstance(decision, MustLink) else "non_match",
                labelled_by=owner,
                labelled_at=decision.decided_at,
                evidence_ref=f"decision:{decision.decision_id}",
            )
        )
    return rows


def truthset_file(entity: Entity) -> str:
    """The file name of an entity type's part of the truth set, for example `matches.csv`."""
    return f"{PLURALS[entity]}.csv"


def read_truthset(path: Path) -> list[TruthRow]:
    """The rows of one truth-set CSV; its header must be exactly `TRUTHSET_COLUMNS`."""
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != TRUTHSET_COLUMNS:
            raise ValueError(f"{path.name}: the header must be {', '.join(TRUTHSET_COLUMNS)}")
        return [TruthRow.model_validate(row) for row in reader]
