"""`resolve_all`: one resolution pass of a run (spec §14.3, §15.3 to §15.6; plan Task 22).

For each entity type, in the order of `ENTITIES` (matches use the team clusters of the same pass):

1. read the records from bronze and score the candidates of every provider pair;
2. apply, in the order of §15.6: (1) the hand decisions — the latest decision on a pair wins, a must-link links, a
   cannot-link blocks the pair everywhere; (2) the pending undo requests — the links of the batch go, and its pairs
   are held; then the links accepted as recommended in earlier batches stay; the bands — the automatic band links,
   the reject band drops, the middle band queues (no measured bands: every candidate queues); (3) the pending accept
   requests and (4) the `auto_accept_recommended` setting accept recommended, unheld queue items;
3. write the link table `gold_<entity>_links` and the queue, cluster the links (`nway`), write the canonical
   pointers, and log every pointer that moved, with its old value, new value, run id and reason.

A record already linked to a record of another provider (by hand, or by an earlier batch) gets no second candidate
with that provider. The pass records each applied request in `ops_applied_requests`, so a request is applied once;
it writes catalog tables only, never a config-side object (§8.3).
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final

import duckdb

from lakehouse.config.model import Settings
from lakehouse.ids.keys import team_key
from lakehouse.ids.native import Provider
from lakehouse.logging import get_logger
from lakehouse.resolve.bands import Bands, band_of, is_recommended, load_bands
from lakehouse.resolve.competitions import competition_candidates
from lakehouse.resolve.decisions import (
    AcceptRequest,
    CannotLink,
    DecisionSet,
    DecisionStore,
    MustLink,
    applied_requests,
)
from lakehouse.resolve.matches import match_candidates
from lakehouse.resolve.model import ENTITIES, PLURALS, PROVIDER_RANK, Candidate, Entity, EntityRecord, RecordRef
from lakehouse.resolve.nway import resolve_clusters
from lakehouse.resolve.players import player_candidates
from lakehouse.resolve.records import load_records
from lakehouse.resolve.tables import insert, link_table, read_rows, replace_rows
from lakehouse.resolve.teams import team_candidates
from lakehouse.storage.base import Storage

__all__ = ["ResolutionResult", "resolve_all", "run_resolution"]

_log = get_logger(__name__)
_SETTING_BATCH: Final = "{run_id}.auto"
Pair = tuple[RecordRef, RecordRef]


@dataclass(frozen=True)
class ResolutionResult:
    links_written: Mapping[Entity, int]
    queued: Mapping[Entity, int]
    accepted_batches: list[str]
    pointer_moves: int


@dataclass(frozen=True)
class _Link:
    a: RecordRef
    b: RecordRef
    confidence: float
    layer: int
    method: str
    accepted_by: str
    batch_id: str | None
    decided_by: str | None
    run_id: str
    linked_at: datetime


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _ref(row: Mapping[str, Any], side: str) -> RecordRef:
    return RecordRef(row[f"provider_{side}"], str(row[f"native_id_{side}"]))


def _candidates(
    entity: Entity, records: Mapping[Provider, Sequence[EntityRecord]], team_clusters: Mapping[RecordRef, RecordRef]
) -> list[Candidate]:
    if entity == "player":
        return player_candidates(records)
    if entity == "team":
        return team_candidates(records)
    if entity == "competition":
        return competition_candidates(records)
    return match_candidates(records, team_clusters)


def _item_id(entity: Entity, key: Pair) -> str:
    a, b = key
    text = f"{entity}|{a.provider}|{a.native_id}|{b.provider}|{b.native_id}"
    return f"{entity}-{hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]}"


class _Entity:
    """The state of one entity type during a pass."""

    def __init__(self, entity: Entity, records: Mapping[Provider, Sequence[EntityRecord]], run_id: str, at: datetime):
        self.entity: Entity = entity
        self.run_id = run_id
        self.at = at
        self.by_ref: dict[RecordRef, EntityRecord] = {r.ref: r for found in records.values() for r in found}
        self.links: dict[Pair, _Link] = {}
        self.queue: dict[Pair, Candidate] = {}
        self._partner: dict[tuple[RecordRef, Provider], RecordRef] = {}

    def keep(self, link: _Link) -> None:
        self.links[(link.a, link.b)] = link
        self._partner[(link.a, link.b.provider)] = link.b
        self._partner[(link.b, link.a.provider)] = link.a

    def link(
        self,
        candidate: Candidate | None,
        key: Pair,
        *,
        accepted_by: str,
        batch: str | None = None,
        decided_by: str | None = None,
    ) -> None:
        a, b = key
        self.keep(
            _Link(
                a,
                b,
                candidate.score if candidate is not None else 100.0,
                candidate.layer if candidate is not None else 0,
                candidate.method if candidate is not None else "hand",
                accepted_by,
                batch,
                decided_by,
                self.run_id,
                self.at,
            )
        )

    def shadowed(self, key: Pair) -> bool:
        """Either record already links to another record of the other one's provider."""
        a, b = key
        return self._partner.get((a, b.provider), b) != b or self._partner.get((b, a.provider), a) != a

    def link_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "provider_a": link.a.provider,
                "native_id_a": link.a.native_id,
                "key_a": self.by_ref[link.a].key,
                "provider_b": link.b.provider,
                "native_id_b": link.b.native_id,
                "key_b": self.by_ref[link.b].key,
                "confidence": link.confidence,
                "layer": link.layer,
                "method": link.method,
                "accepted_by": link.accepted_by,
                "batch_id": link.batch_id,
                "decided_by": link.decided_by,
                "run_id": link.run_id,
                "linked_at": link.linked_at,
            }
            for _, link in sorted(self.links.items())
        ]


def _latest_decisions(decisions: DecisionSet, entity: Entity) -> dict[Pair, MustLink | CannotLink]:
    ordered = sorted(
        (d for d in [*decisions.must_links, *decisions.cannot_links] if d.entity == entity),
        key=lambda d: (d.decided_at, d.decision_id),
    )
    latest: dict[Pair, MustLink | CannotLink] = {}
    for decision in ordered:
        latest[decision.pair] = decision
    return latest


def _move_reason(old: RecordRef, new: RecordRef, ref: RecordRef, clusters: Mapping[RecordRef, RecordRef]) -> str:
    if old in clusters and clusters[old] == clusters[ref]:
        if PROVIDER_RANK[new.provider] < PROVIDER_RANK[old.provider]:
            return "a higher-ranked record joined the cluster"
        return "the cluster changed"
    return "the canonical record left the cluster"


def _pointers(conn: duckdb.DuckDBPyConnection, state: _Entity, clusters: Mapping[RecordRef, RecordRef]) -> int:
    previous = {
        RecordRef(row["provider"], str(row["native_id"])): row
        for row in read_rows(conn, "ops_canonical_pointers", entity=state.entity)
    }
    rows: list[dict[str, Any]] = []
    moves: list[dict[str, Any]] = []
    for ref, canonical in sorted(clusters.items()):
        record, canonical_record = state.by_ref[ref], state.by_ref[canonical]
        rows.append(
            {
                "entity": state.entity,
                "provider": ref.provider,
                "native_id": ref.native_id,
                "record_key": record.key,
                "canonical_provider": canonical.provider,
                "canonical_native_id": canonical.native_id,
                "canonical_key": canonical_record.key,
                "run_id": state.run_id,
                "updated_at": state.at,
            }
        )
        before = previous.get(ref)
        if before is not None and int(before["canonical_key"]) != canonical_record.key:
            old = RecordRef(before["canonical_provider"], str(before["canonical_native_id"]))
            moves.append(
                {
                    "entity": state.entity,
                    "provider": ref.provider,
                    "native_id": ref.native_id,
                    "record_key": record.key,
                    "old_canonical_key": int(before["canonical_key"]),
                    "new_canonical_key": canonical_record.key,
                    "reason": _move_reason(old, canonical, ref, clusters),
                    "run_id": state.run_id,
                    "moved_at": state.at,
                }
            )
    replace_rows(conn, "ops_canonical_pointers", rows, entity=state.entity)
    insert(conn, "ops_pointer_moves", moves)
    return len(moves)


def resolve_all(
    conn: duckdb.DuckDBPyConnection,
    *,
    decisions: DecisionSet,
    bands: Mapping[Entity, Bands | None],
    settings: Settings,
    run_id: str,
    now: Callable[[], datetime] = _utc_now,
) -> ResolutionResult:
    """One resolution pass over every entity type (see the module docstring)."""
    at = now()
    applied = applied_requests(conn)
    pending_undo = sorted(
        (u for u in decisions.undo_requests if u.request_id not in applied),
        key=lambda u: (u.requested_at, u.request_id),
    )
    pending_accept: list[AcceptRequest] = sorted(
        (r for r in decisions.accept_requests if r.request_id not in applied),
        key=lambda r: (r.requested_at, r.request_id),
    )
    undo_of = {u.batch_id: u for u in pending_undo}
    held_batches: dict[tuple[str, Pair], str] = {
        (str(row["entity"]), (_ref(row, "a"), _ref(row, "b"))): str(row["batch_id"])
        for row in read_rows(conn, "ops_holds")
    }
    new_holds: list[dict[str, Any]] = []
    accepted: Counter[str] = Counter()
    undone: Counter[str] = Counter()
    links_written: dict[Entity, int] = {}
    queued: dict[Entity, int] = {}
    moves = 0
    team_clusters: dict[RecordRef, RecordRef] = {}
    setting_batch = _SETTING_BATCH.format(run_id=run_id)
    for entity in ENTITIES:
        records = load_records(conn, entity)
        state = _Entity(entity, records, run_id, at)
        candidates = _candidates(entity, records, team_clusters)
        by_pair = {c.key: c for c in candidates}
        entity_bands = bands.get(entity)
        # (1) hand decisions
        latest = _latest_decisions(decisions, entity)
        blocked = {key for key, d in latest.items() if isinstance(d, CannotLink)}
        for key, decision in sorted(latest.items()):
            if isinstance(decision, MustLink) and key[0] in state.by_ref and key[1] in state.by_ref:
                state.link(by_pair.get(key), key, accepted_by="hand", decided_by=decision.decided_by)
        # (2) undo requests, then the earlier batches that stay
        previous = read_rows(conn, link_table(entity))
        for row in previous:
            key = (_ref(row, "a"), _ref(row, "b"))
            batch = row["batch_id"]
            if row["accepted_by"] != "recommended" or batch is None:
                continue
            if batch in undo_of:
                undone[batch] += 1
                if (entity, key) not in held_batches:
                    held_batches[(entity, key)] = batch
                    new_holds.append(
                        {
                            "entity": entity,
                            "provider_a": key[0].provider,
                            "native_id_a": key[0].native_id,
                            "provider_b": key[1].provider,
                            "native_id_b": key[1].native_id,
                            "batch_id": batch,
                            "request_id": undo_of[batch].request_id,
                            "run_id": run_id,
                            "held_at": at,
                        }
                    )
                continue
            if (
                key in blocked
                or key in state.links
                or key[0] not in state.by_ref
                or key[1] not in state.by_ref
                or state.shadowed(key)
            ):
                continue
            state.keep(
                _Link(
                    key[0],
                    key[1],
                    float(row["confidence"]),
                    int(row["layer"]),
                    str(row["method"]),
                    "recommended",
                    str(batch),
                    None,
                    str(row["run_id"]),
                    row["linked_at"],
                )
            )
        held = {key for (held_entity, key) in held_batches if held_entity == entity}
        # the bands
        for candidate in candidates:
            key = candidate.key
            if key in state.links or key in blocked or state.shadowed(key):
                continue
            band = band_of(candidate.score, entity_bands)
            if band == "auto":
                state.link(candidate, key, accepted_by="auto")
            elif band == "queue":
                state.queue[key] = candidate

        # (3) accept requests, (4) the setting
        for request in pending_accept:
            if request.entity in (None, entity):
                accepted[request.request_id] += _accept(state, held, entity_bands, request.request_id, request.selects)
        if settings.auto_accept_recommended:
            accepted[setting_batch] += _accept(state, held, entity_bands, setting_batch, _every_pair)
        replace_rows(conn, link_table(entity), state.link_rows())
        replace_rows(conn, "ops_entity_records", _record_rows(state), entity=entity)
        replace_rows(conn, "ops_review_queue", _queue_rows(state, held_batches, entity_bands), entity=entity)
        clusters = resolve_clusters(
            state.by_ref,
            [
                Candidate.of(link.a, link.b, score=link.confidence, layer=link.layer, method=link.method)
                for link in state.links.values()
            ],
        )
        if entity == "team":
            team_clusters = clusters
        moves += _pointers(conn, state, clusters)
        links_written[entity], queued[entity] = len(state.links), len(state.queue)
        _log.info(
            "resolution",
            extra={
                "entity": entity,
                "rows": len(state.by_ref),
                "candidates": len(candidates),
                "links": len(state.links),
                "queued": len(state.queue),
            },
        )
    insert(conn, "ops_holds", new_holds)
    batches = [r.request_id for r in pending_accept] + [setting_batch]
    insert(
        conn,
        "ops_applied_requests",
        [
            *(
                {
                    "request_id": r.request_id,
                    "kind": "accept",
                    "batch_id": r.request_id,
                    "links": accepted[r.request_id],
                    "applied_at": at,
                    "run_id": run_id,
                }
                for r in pending_accept
            ),
            *(
                {
                    "request_id": u.request_id,
                    "kind": "undo",
                    "batch_id": u.batch_id,
                    "links": undone[u.batch_id],
                    "applied_at": at,
                    "run_id": run_id,
                }
                for u in pending_undo
            ),
        ],
    )
    return ResolutionResult(
        links_written=links_written,
        queued=queued,
        accepted_batches=[batch for batch in batches if accepted[batch] > 0],
        pointer_moves=moves,
    )


def run_resolution(
    conn: duckdb.DuckDBPyConnection,
    *,
    storage: Storage,
    settings: Settings,
    run_id: str,
    now: Callable[[], datetime] = _utc_now,
) -> ResolutionResult:
    """The resolution step of a run (§15.3): the decisions and requests read once from the config side, the measured
    bands of every entity type from the catalog, then `resolve_all`."""
    decisions = DecisionStore(storage).load()
    bands: dict[Entity, Bands | None] = {entity: load_bands(conn, entity) for entity in ENTITIES}
    return resolve_all(conn, decisions=decisions, bands=bands, settings=settings, run_id=run_id, now=now)


def _birth_date(text: str | None) -> date | None:
    try:
        return None if text is None else date.fromisoformat(text)
    except ValueError:
        return None  # a source date that is not a calendar date stays out of the dimension


def _team_key(ref: RecordRef | None) -> int | None:
    return None if ref is None else team_key(ref.provider, ref.native_id)


def _record_rows(state: _Entity) -> list[dict[str, Any]]:
    """The records of the pass for the dbt dimensions (plan Task 23): natural keys, the generator's key, attributes."""
    return [
        {
            "entity": state.entity,
            "provider": ref.provider,
            "native_id": ref.native_id,
            "record_key": record.key,
            "name": record.name,
            "short_name": record.short_name,
            "birth_date": _birth_date(record.birth_date),
            "team": record.team,
            "jersey": record.jersey,
            "position": record.position,
            "area": record.area,
            "matches": record.matches,
            "match_date": record.match_date,
            "home_team_key": _team_key(record.home_team),
            "away_team_key": _team_key(record.away_team),
            "native_competition_id": record.native_competition_id,
            "competition_key": record.competition_key,
            "native_season_id": record.native_season_id,
            "season_key": record.season_key,
            "season_name": record.season_name,
            "run_id": state.run_id,
            "updated_at": state.at,
        }
        for ref, record in sorted(state.by_ref.items())
    ]


def _every_pair(a: RecordRef, b: RecordRef) -> bool:
    del a, b
    return True


def _accept(
    state: _Entity,
    held: set[Pair],
    bands: Bands | None,
    batch: str,
    selects: Callable[[RecordRef, RecordRef], bool],
) -> int:
    """Accept every recommended, unheld queue item that `selects` takes, as `batch`; the count."""
    count = 0
    for key, candidate in sorted(state.queue.items()):
        if key in held or not is_recommended(candidate, bands) or not selects(*key) or state.shadowed(key):
            continue
        state.link(candidate, key, accepted_by="recommended", batch=batch)
        del state.queue[key]
        count += 1
    return count


def _queue_rows(
    state: _Entity, held_batches: Mapping[tuple[str, Pair], str], bands: Bands | None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, candidate in sorted(state.queue.items()):
        batch = held_batches.get((state.entity, key))
        if batch is not None:
            reason = f"held after the undo of batch {batch}: a person confirms or rejects it"
        elif bands is None:
            reason = f"no measured bands for {PLURALS[state.entity]}"
        else:
            reason = "between the automatic and the reject band"
        rows.append(
            {
                "item_id": _item_id(state.entity, key),
                "entity": state.entity,
                "provider_a": key[0].provider,
                "native_id_a": key[0].native_id,
                "key_a": state.by_ref[key[0]].key,
                "provider_b": key[1].provider,
                "native_id_b": key[1].native_id,
                "key_b": state.by_ref[key[1]].key,
                "score": candidate.score,
                "runner_up": candidate.runner_up,
                "layer": candidate.layer,
                "method": candidate.method,
                "recommended": batch is None and is_recommended(candidate, bands),
                "held": batch is not None,
                "reason": reason,
                "run_id": state.run_id,
                "queued_at": state.at,
            }
        )
    return rows
