"""The single-writer lease and the skip records (spec §10.6; plan Task 7).

The lease is a chain of generation objects `runs/lease/<generation>.json` (12 digits, zero-padded) in the lake root.
A generation object is only ever created with `put_if_absent`, so exactly one taker wins each generation:

- **Acquire.** Read the highest generation N. If none exists, or N is released, or N's expiry plus `TAKEOVER_GRACE`
  has passed, create N+1. A lost create, or a fresh listing that shows a generation above N+1, gives a skip record
  and no lease. An unparsable generation counts as held until its storage `mtime` plus `LEASE_TTL` plus
  `TAKEOVER_GRACE`.
- **Renew.** Only the holder writes its own generation (expiry and heartbeat), with a plain `put`.
- **`assert_held()`.** Passes only when generation N+1 does not exist, the own object still names this run, and the
  holder's own clock shows at least `COMMIT_MARGIN` before expiry.
- **`confirm()`.** A write step calls it; it runs `assert_held()` only when the last passing `assert_held()` is older
  than one `HEARTBEAT` (T3 spec D5). It never trusts a renew: a renew is a plain `put` that succeeds after a takeover.
- **Release.** Stop the heartbeat first, then write `released`, so no late heartbeat can clear the flag.

**The run anchor and the acquisition marker (T3 spec D5).** After it wins generation g, and before it returns the
lease, `acquire` stores two write-once objects beside the generation object: the run anchor `<g>.anchor`, then the
acquisition marker `<g>.acquired`, whose storage `modified` time is the generation's start. A failed store releases
the generation and raises `LeaseMarkerError`, so the caller writes nothing. Generation g's anchor covers the storage
interval from g's marker to the earliest marker of any later generation; the orphan sweep does not count a doomed
object inside a surviving anchor's interval, and removes an anchor once a sweep has examined its whole interval.
Pruning deletes a marker with its generation and never deletes an anchor.

**On HF the lease ledger is the fence of record (T6 spec D1).** The HF S3 gateway does not make create-if-absent atomic
under concurrency, so a storage that provides a lease ledger (`Storage.lease_ledger()`, an HF writer) wins a generation
only by a ledger commit whose parent is the head it read: (1) read the head; (2) the current generation is the higher
of the ledger's and the bucket's highest; (3) decide as above, and a ledger generation without a bucket object is held
until the commit's server time plus `LEASE_TTL` plus `TAKEOVER_GRACE`; (4) commit the next generation; (5) decide every
outcome other than a clean success by reading the head again (a head that names this run is a win, another holder a
lost race, anything else stops loudly); (6) create the bucket generation object (a second fence), the run anchor and the
marker; (7) read the head again after the bucket object and before returning the lease. `assert_held` reads the ledger
too, and an unreadable ledger fails closed. The heartbeat and the release stay bucket writes.

The constants are not settings, because the lease is taken before the config is read.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from lakehouse.errors import (
    IntegrityError,
    LeaseLedgerError,
    LeaseLostError,
    LeaseMarkerError,
    LedgerConflictError,
    LedgerUnavailableError,
)
from lakehouse.logging import get_logger
from lakehouse.storage.base import Storage
from lakehouse.storage.ledger import LeaseLedger, LedgerEntry, LedgerHead

__all__ = [
    "COMMIT_MARGIN",
    "HEARTBEAT",
    "LEASE_PREFIX",
    "LEASE_TTL",
    "SKIP_PREFIX",
    "TAKEOVER_GRACE",
    "AnchorInterval",
    "Clock",
    "Lease",
    "LeaseState",
    "RunAnchor",
    "SkipRecord",
    "anchor_intervals",
    "anchor_key",
    "fold_skip_records",
    "lease_key",
    "marker_key",
    "remove_anchors",
    "write_skip_record",
]

LEASE_TTL: Final = timedelta(minutes=10)
HEARTBEAT: Final = timedelta(seconds=60)
TAKEOVER_GRACE: Final = timedelta(minutes=2)  # tolerates clock skew between machines
COMMIT_MARGIN: Final = timedelta(seconds=60)
LEASE_PREFIX: Final = "runs/lease/"
SKIP_PREFIX: Final = "runs/skipped/"
KEEP_NEWEST: Final = 20
PRUNE_AFTER: Final = timedelta(days=30)
HEARTBEAT_THREAD: Final = "lakehouse-lease-heartbeat"
_GENERATION_KEY: Final = re.compile(r"^runs/lease/(\d{12})\.json$")
_ANCHOR_KEY: Final = re.compile(r"^runs/lease/(\d{12})\.anchor$")
_MARKER_KEY: Final = re.compile(r"^runs/lease/(\d{12})\.acquired$")
_RUN_ID: Final = re.compile(r"^[A-Za-z0-9_.-]+$")
_RUN_IN_PROGRESS: Final = "skipped: run in progress"

Clock = Callable[[], datetime]

_log = get_logger(__name__)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def lease_key(generation: int) -> str:
    if generation < 1:
        raise ValueError(f"a lease generation starts at 1, not {generation}")
    return f"{LEASE_PREFIX}{generation:012d}.json"


def anchor_key(generation: int) -> str:
    """The generation's run anchor. It lies under the gateway prefix `runs/lease/` (HF `put_if_absent` works only on
    gateway keys) and does not match the generation-object pattern, so the generation listing ignores it."""
    return lease_key(generation).removesuffix(".json") + ".anchor"


def marker_key(generation: int) -> str:
    """The generation's write-once acquisition marker; its storage `modified` time is the generation's start."""
    return lease_key(generation).removesuffix(".json") + ".acquired"


class RunAnchor(BaseModel):
    """The content of a run anchor: which generation and run it covers (a restore removes anchors by run id)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    generation: int = Field(ge=1)
    run_id: str = Field(min_length=1)


@dataclass(frozen=True)
class AnchorInterval:
    """The storage-time interval that one surviving run anchor covers (T3 spec D5).

    `start` is the generation's own marker time (`None`: no marker, so the generation wrote nothing and the anchor
    covers nothing). `end` is the earliest marker time of any later generation (`None`: no later marker, an open
    interval). The intervals of consecutive generations leave no gap, even when markers are stored out of order."""

    generation: int
    run_id: str
    start: datetime | None
    end: datetime | None

    def covers(self, modified: datetime) -> bool:
        if self.start is None or modified < self.start:
            return False
        return self.end is None or modified < self.end

    def examined_before(self, started: datetime) -> bool:
        """True when a sweep that started at `started` has examined every object this anchor covers: the interval
        ended before the start (or it covers nothing), so every object in it was older than the sweep's start."""
        return self.start is None or (self.end is not None and self.end < started)


def anchor_intervals(storage: Storage) -> list[AnchorInterval]:
    """Every surviving run anchor with its interval, from one listing of `runs/lease/`."""
    markers: dict[int, datetime] = {}
    anchors: list[tuple[int, str]] = []
    for info in storage.list("lake", LEASE_PREFIX):
        if match := _MARKER_KEY.match(info.key):
            markers[int(match.group(1))] = info.modified
        elif match := _ANCHOR_KEY.match(info.key):
            anchors.append((int(match.group(1)), info.key))
    intervals: list[AnchorInterval] = []
    for generation, key in sorted(anchors):
        try:
            anchor = RunAnchor.model_validate_json(storage.get("lake", key))
        except ValidationError as exc:
            raise IntegrityError(f"run anchor {key} is not readable: {exc}") from exc
        later = [modified for marked, modified in markers.items() if marked > generation]
        intervals.append(
            AnchorInterval(generation, anchor.run_id, markers.get(generation), min(later) if later else None)
        )
    return intervals


def remove_anchors(storage: Storage, generations: Iterable[int]) -> None:
    for generation in generations:
        storage.delete("lake", anchor_key(generation), missing_ok=True)


class LeaseState(BaseModel):
    """One generation object."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    generation: int = Field(ge=1)
    holder_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    started_at: AwareDatetime
    expires_at: AwareDatetime
    heartbeat_at: AwareDatetime
    released: bool


class SkipRecord(BaseModel):
    """A run that could not take the lease (spec §10.6); the next holder folds it into run history."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str
    reason: str
    at: AwareDatetime
    blocking_run_id: str | None


def _skip_key(run_id: str) -> str:
    if not _RUN_ID.match(run_id):
        raise ValueError(f"invalid run id {run_id!r}")
    return f"{SKIP_PREFIX}{run_id}.json"


def write_skip_record(
    storage: Storage, *, run_id: str, reason: str, blocking_run_id: str | None, clock: Clock = _utc_now
) -> None:
    """Write `runs/skipped/<run_id>.json` in the lake root. It is outside the catalog, so no lease is needed."""
    record = SkipRecord(run_id=run_id, reason=reason, at=clock(), blocking_run_id=blocking_run_id)
    storage.put("lake", _skip_key(run_id), record.model_dump_json().encode("utf-8"))
    _log.info("run skipped", extra={"run_id": run_id, "reason": reason, "blocking_run_id": blocking_run_id})


def fold_skip_records(storage: Storage, *, record: Callable[[SkipRecord], None]) -> list[SkipRecord]:
    """Hand each skip record to `record` (which writes it into run history, Task 21), then remove it.

    A record is removed only after `record` returned, so an error in `record` keeps it for the next holder.
    """
    folded: list[SkipRecord] = []
    for info in storage.list("lake", SKIP_PREFIX):
        try:
            skip = SkipRecord.model_validate_json(storage.get("lake", info.key))
        except ValidationError as exc:
            raise IntegrityError(f"skip record {info.key} is not readable: {exc}") from exc
        record(skip)
        storage.delete("lake", info.key, missing_ok=True)
        folded.append(skip)
    return folded


def _generations(storage: Storage) -> list[int]:
    found: list[int] = []
    for info in storage.list("lake", LEASE_PREFIX):
        match = _GENERATION_KEY.match(info.key)
        if match:
            found.append(int(match.group(1)))
    return sorted(found)


def _read_state(storage: Storage, generation: int) -> LeaseState | None:
    """The generation object, or `None` when it is unparsable (for example a partial write)."""
    try:
        return LeaseState.model_validate_json(storage.get("lake", lease_key(generation)))
    except ValidationError:
        return None


def _store_anchor_and_marker(storage: Storage, won: LeaseState) -> None:
    """Store the run anchor, then the acquisition marker, of a won generation (T3 spec D5). A store that fails marks
    the generation released, so the next run takes over at once, and raises `LeaseMarkerError`.

    An anchor or marker that already exists is different: only the generation's winner writes them, so another taker
    holds this same generation (a store whose create is not atomic let two takers win it, as the HF gateway did in
    T3 Task 12 / audit T6). This taker then raises and writes nothing more; marking the generation released would
    release the other taker's lease and let a third taker in at once."""
    generation = won.generation
    body = RunAnchor(generation=generation, run_id=won.run_id).model_dump_json().encode("utf-8")
    for key, what in ((anchor_key(generation), "run anchor"), (marker_key(generation), "acquisition marker")):
        if not _store_once(storage, won, key, body):
            raise LeaseMarkerError(
                f"the {what} of lease generation {generation} already exists: another taker holds this generation"
            )


def _store_once(storage: Storage, won: LeaseState, key: str, body: bytes) -> bool:
    """`put_if_absent` of one anchor or marker; a store error releases the won generation and raises."""
    try:
        return storage.put_if_absent("lake", key, body)
    except Exception as exc:
        released = won.model_copy(update={"released": True})
        storage.put("lake", lease_key(won.generation), released.model_dump_json().encode("utf-8"))
        raise LeaseMarkerError(
            f"lease generation {won.generation} could not store its run anchor or acquisition marker: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def _held_until(storage: Storage, generation: int, state: LeaseState | None) -> datetime:
    """When the generation is free to take over (after expiry plus grace)."""
    if state is None:
        return storage.stat("lake", lease_key(generation)).modified + LEASE_TTL + TAKEOVER_GRACE
    return state.expires_at + TAKEOVER_GRACE


def _read_head(ledger: LeaseLedger, when: str) -> LedgerHead:
    """The ledger head; any failure stops the acquisition loudly (an outage or an unreadable entry is never a free
    lease, T6 spec D1)."""
    try:
        return ledger.head()
    except Exception as exc:
        raise LeaseLedgerError(f"the lease ledger could not be read {when}: {type(exc).__name__}: {exc}") from exc


def _confirm_entry(ledger: LeaseLedger, entry: LedgerEntry, when: str) -> None:
    """D1 step 7: the ledger head must still name this run's generation; a newer generation means a winner that
    stalled past the 12-minute rule was taken over, so it raises and writes nothing more."""
    head = _read_head(ledger, when)
    if head.entry != entry:
        named = head.entry.generation if head.entry is not None else None
        raise LeaseLostError(
            f"the lease ledger names lease generation {named} ({when}): run {entry.run_id} lost lease generation "
            f"{entry.generation}"
        )


class Lease:
    """A held lease generation. Create it only with `Lease.acquire`."""

    def __init__(
        self,
        storage: Storage,
        state: LeaseState,
        started_storage_time: datetime,
        clock: Clock,
        *,
        ledger: LeaseLedger | None = None,
        entry: LedgerEntry | None = None,
    ) -> None:
        self._storage = storage
        self._state = state
        self._clock = clock
        self._ledger = ledger  # the HF fence of record, with the entry this run committed (T6 spec D1)
        self._entry = entry
        self._lock = threading.Lock()
        self._stopped = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._heartbeat_error: str | None = None
        self._confirmed_at = clock()  # the acquisition is the first confirmation
        self.started_storage_time = started_storage_time

    def __repr__(self) -> str:
        return f"Lease(generation={self.generation}, run_id={self._state.run_id!r})"

    @property
    def generation(self) -> int:
        return self._state.generation

    @property
    def run_id(self) -> str:
        return self._state.run_id

    @classmethod
    def acquire(cls, storage: Storage, *, holder_id: str, run_id: str, clock: Clock = _utc_now) -> Lease | None:
        """Take the next generation, or write a skip record and return `None`."""
        _skip_key(run_id)  # validate the run id before any write
        ledger = storage.lease_ledger()
        if ledger is not None:
            return cls._acquire_on_ledger(storage, ledger, holder_id=holder_id, run_id=run_id, clock=clock)
        generations = _generations(storage)
        top = generations[-1] if generations else 0
        if top:
            state = _read_state(storage, top)
            free_at = _held_until(storage, top, state)
            if not (state is not None and state.released) and clock() <= free_at:
                holder = state.run_id if state is not None else None
                reason = f"{_RUN_IN_PROGRESS} (lease generation {top} is held until {free_at.isoformat()})"
                write_skip_record(storage, run_id=run_id, reason=reason, blocking_run_id=holder, clock=clock)
                return None
        now = clock()
        new = LeaseState(
            generation=top + 1,
            holder_id=holder_id,
            run_id=run_id,
            started_at=now,
            expires_at=now + LEASE_TTL,
            heartbeat_at=now,
            released=False,
        )
        if not storage.put_if_absent("lake", lease_key(new.generation), new.model_dump_json().encode("utf-8")):
            winner = _read_state(storage, new.generation)
            reason = f"{_RUN_IN_PROGRESS} (another run took lease generation {new.generation} first)"
            blocking = winner.run_id if winner is not None else None
            write_skip_record(storage, run_id=run_id, reason=reason, blocking_run_id=blocking, clock=clock)
            return None
        fresh = _generations(storage)
        if any(generation > new.generation for generation in fresh):
            abandoned = new.model_copy(update={"released": True})
            storage.put("lake", lease_key(new.generation), abandoned.model_dump_json().encode("utf-8"))
            reason = f"{_RUN_IN_PROGRESS} (the lease listing was stale: a generation above {new.generation} exists)"
            write_skip_record(storage, run_id=run_id, reason=reason, blocking_run_id=None, clock=clock)
            return None
        _store_anchor_and_marker(storage, new)
        lease = cls(storage, new, storage.stat("lake", lease_key(new.generation)).modified, clock)
        lease._prune(fresh)
        _log.info("lease acquired", extra={"run_id": run_id, "generation": new.generation})
        return lease

    @classmethod
    def _acquire_on_ledger(
        cls, storage: Storage, ledger: LeaseLedger, *, holder_id: str, run_id: str, clock: Clock
    ) -> Lease | None:
        """T6 spec D1 steps 1-7 (see the module docstring)."""
        head = _read_head(ledger, "before the acquisition")
        generations = _generations(storage)
        ledger_top = head.entry.generation if head.entry is not None else 0
        top = max(ledger_top, generations[-1] if generations else 0)
        if top:
            if storage.exists("lake", lease_key(top)):
                state = _read_state(storage, top)
                free_at = _held_until(storage, top, state)
                released = state is not None and state.released
                holder = state.run_id if state is not None else None
            else:  # a ledger generation whose winner crashed or stalled before its bucket write: never free at once
                released = False
                free_at = head.committed_at + LEASE_TTL + TAKEOVER_GRACE
                holder = head.entry.run_id if head.entry is not None else None
            if not released and clock() <= free_at:
                reason = f"{_RUN_IN_PROGRESS} (lease generation {top} is held until {free_at.isoformat()})"
                write_skip_record(storage, run_id=run_id, reason=reason, blocking_run_id=holder, clock=clock)
                return None
        now = clock()
        new = LeaseState(
            generation=top + 1,
            holder_id=holder_id,
            run_id=run_id,
            started_at=now,
            expires_at=now + LEASE_TTL,
            heartbeat_at=now,
            released=False,
        )
        entry = LedgerEntry(generation=new.generation, holder_id=holder_id, run_id=run_id, acquired_at=now)
        try:
            ledger.claim(entry, parent=head.sha)
        except LedgerUnavailableError as exc:
            raise LeaseLedgerError(f"the lease ledger refused lease generation {new.generation}: {exc}") from exc
        except LedgerConflictError:
            # the parent check failed, so this commit did not land and another did (HTTP 409, measured in T6 Task 1): a
            # lost race by itself. A head read only names the winner, and right after the winner's commit it can still
            # show the old head (two-process HF Job, 2026-11-01), so it never decides the outcome here.
            try:
                after = _read_head(ledger, "after a lost race")
            except LeaseLedgerError:
                after = head  # the winner stays unnamed; the race is lost either way
            named = after.sha != head.sha and after.entry is not None and after.entry != entry
            reason = f"{_RUN_IN_PROGRESS} (another run committed lease generation {new.generation} first)"
            write_skip_record(
                storage,
                run_id=run_id,
                reason=reason,
                blocking_run_id=after.entry.run_id if named and after.entry is not None else None,
                clock=clock,
            )
            return None
        except Exception as exc:
            after = _read_head(ledger, "after an unclear commit outcome")
            if after.entry != entry:  # the commit did not land for this run
                if after.sha != head.sha and after.entry is not None and after.entry.generation >= new.generation:
                    reason = f"{_RUN_IN_PROGRESS} (another run took lease generation {after.entry.generation} first)"
                    write_skip_record(
                        storage, run_id=run_id, reason=reason, blocking_run_id=after.entry.run_id, clock=clock
                    )
                    return None
                raise LeaseLedgerError(
                    f"the lease ledger commit of generation {new.generation} has no clear outcome: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
        if not storage.put_if_absent("lake", lease_key(new.generation), new.model_dump_json().encode("utf-8")):
            raise LeaseMarkerError(
                f"lease generation {new.generation} exists in the bucket although the ledger gave it to run {run_id}: "
                "a writer outside the ledger created it"
            )
        _confirm_entry(ledger, entry, "after the bucket generation object")
        _store_anchor_and_marker(storage, new)
        _confirm_entry(ledger, entry, "before returning the lease")
        started = storage.stat("lake", lease_key(new.generation)).modified
        lease = cls(storage, new, started, clock, ledger=ledger, entry=entry)
        lease._prune(_generations(storage))
        _log.info("lease acquired", extra={"run_id": run_id, "generation": new.generation, "fence": "ledger"})
        return lease

    def _prune(self, generations: list[int]) -> None:
        """Delete generations outside the newest `KEEP_NEWEST` whose expiry is more than `PRUNE_AFTER` old."""
        cutoff = self._clock() - PRUNE_AFTER
        for generation in generations[:-KEEP_NEWEST]:
            state = _read_state(self._storage, generation)
            expiry = state.expires_at if state is not None else _held_until(self._storage, generation, None)
            if expiry < cutoff:
                # the marker first: a crash in between leaves a generation without a marker, which covers nothing
                self._storage.delete("lake", marker_key(generation), missing_ok=True)
                self._storage.delete("lake", lease_key(generation), missing_ok=True)

    def _write(self, state: LeaseState) -> None:
        self._storage.put("lake", lease_key(state.generation), state.model_dump_json().encode("utf-8"))
        self._state = state

    def renew(self) -> None:
        """Extend the expiry with a plain `put` of the own generation. After `release`, it writes nothing."""
        with self._lock:
            if self._stopped:
                return
            now = self._clock()
            self._write(self._state.model_copy(update={"expires_at": now + LEASE_TTL, "heartbeat_at": now}))

    def start_heartbeat(self, *, interval_s: float | None = None) -> None:
        """Renew every `HEARTBEAT` (or `interval_s`) on a daemon thread until `release`."""
        if self._thread is not None:
            raise RuntimeError("the lease heartbeat already runs")
        interval = HEARTBEAT.total_seconds() if interval_s is None else interval_s
        self._thread = threading.Thread(target=self._beat, args=(interval,), name=HEARTBEAT_THREAD, daemon=True)
        self._thread.start()

    def _beat(self, interval: float) -> None:
        while not self._stop.wait(interval):
            try:
                self.renew()
            except Exception as exc:  # noqa: BLE001 - the heartbeat outlives a transient storage error; the expiry still fences
                self._heartbeat_error = f"{type(exc).__name__}: {exc}"
                _log.warning(
                    "lease heartbeat failed",
                    exc_info=True,
                    extra={"run_id": self._state.run_id, "generation": self.generation},
                )

    def assert_held(self) -> None:
        """Raise `LeaseLostError` unless this run still holds the lease with at least `COMMIT_MARGIN` left."""
        if self._ledger is not None:  # the HF fence of record (T6 spec D1); an unreadable ledger fails closed
            try:
                head = self._ledger.head()
            except Exception as exc:
                raise LeaseLostError(
                    f"cannot confirm lease generation {self.generation}: the lease ledger is unreadable: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if head.entry != self._entry:
                named = head.entry.generation if head.entry is not None else None
                raise LeaseLostError(
                    f"the lease ledger names lease generation {named}: run {self._state.run_id} lost lease generation "
                    f"{self.generation}"
                )
        later = self.generation + 1
        if self._storage.exists("lake", lease_key(later)):
            raise LeaseLostError(f"lease generation {later} exists: run {self._state.run_id} lost the lease")
        own = _read_state(self._storage, self.generation)
        if own is None or own.run_id != self._state.run_id or own.holder_id != self._state.holder_id:
            raise LeaseLostError(f"lease generation {self.generation} no longer names run {self._state.run_id}")
        if own.released:
            raise LeaseLostError(f"run {self._state.run_id} released lease generation {self.generation}")
        now = self._clock()
        if now > own.expires_at - COMMIT_MARGIN:
            detail = f"; last heartbeat error: {self._heartbeat_error}" if self._heartbeat_error else ""
            raise LeaseLostError(
                f"less than the commit margin ({COMMIT_MARGIN.total_seconds():.0f} s) is left before lease generation "
                f"{self.generation} expires at {own.expires_at.isoformat()}{detail}"
            )
        self._confirmed_at = now

    def confirm(self) -> None:
        """Before a write step: `assert_held()` when the last passing one is older than one `HEARTBEAT` (T3 spec D5).
        A stalled writer that lost its lease then stops at its next write step, not at its next publish."""
        if self._clock() - self._confirmed_at > HEARTBEAT:
            self.assert_held()

    def release(self) -> None:
        """Stop the heartbeat, then mark the own generation released. The next run can take over at once."""
        with self._lock:
            self._stopped = True
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        with self._lock:
            if not self._state.released:
                self._write(self._state.model_copy(update={"released": True}))
        _log.info("lease released", extra={"run_id": self._state.run_id, "generation": self.generation})
