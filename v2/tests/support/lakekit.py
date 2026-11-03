"""Shared lake set-up for the catalog unit and e2e tests (Tasks 11 and 12): a helper module, not a test.

Simulated days: `LakeKit.day(d)` moves the kit's clock to `T0 + d days`, where `T0` lies 40 days in the past. The
working catalog stamps `published_at` from that clock, and `LakeKit.commit` stamps the new DuckLake snapshots with it,
so snapshot expiry and version retention see the simulated day. Storage times (`mtime`) stay real, and so does each
stub lease's `started_storage_time`, which is what the sweep's age fence compares.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from lakehouse.bronze.schemas import TRACKING_PLAYERS_CORE, BronzeTable, bronze_table
from lakehouse.bronze.writer import (
    Visibility,
    WrittenFile,
    register_match_files,
    replace_match_files,
    store_written_file,
    write_tracking_file,
)
from lakehouse.catalog.cleanup import CleanupReport, run_cleanup
from lakehouse.catalog.manifest import VersionKind
from lakehouse.catalog.moves import MoveRecord, move_match_files
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import VersionInfo, WorkingCatalog
from lakehouse.config.model import Settings, load_defaults
from lakehouse.errors import LeaseLostError, LedgerConflictError, LedgerUnavailableError
from lakehouse.raw.archive import RawArchive
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import Root, RootMap, StorageRootInfo
from lakehouse.storage.ledger import LedgerEntry, LedgerHead
from lakehouse.storage.local import LocalStorage

PLAYERS: BronzeTable = bronze_table("skillcorner_tracking_players")
T0 = datetime.now(UTC).replace(microsecond=0) - timedelta(days=40)


def _real_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class StubLease:
    generation: int = 1
    run_id: str = "run-1"
    held: bool = True
    started_storage_time: datetime = field(default_factory=_real_now)
    checks: int = 0

    def assert_held(self) -> None:
        self.checks += 1
        if not self.held:
            raise LeaseLostError("stub: lease lost")

    def confirm(self) -> None:
        """The write-time lease check (T3 spec D5): the stub checks every time."""
        self.assert_held()


class Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now


def lake_roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def players(match_key: int, frames: int, offset: int) -> pa.Table:
    """A players table whose `x_mm` is `frame + offset`, so a sum identifies the content."""
    return pa.table(
        {
            "match_key": pa.array([match_key] * frames, pa.int64()),
            "period": pa.array([1] * frames, pa.int8()),
            "frame": pa.array(range(frames), pa.int32()),
            "player_key": pa.array([11] * frames, pa.int64()),
            "x_mm": pa.array([f + offset for f in range(frames)], pa.int32()),
            "y_mm": pa.array([0] * frames, pa.int32()),
            "is_visible": pa.array([True] * frames, pa.bool_()),
        },
        schema=TRACKING_PLAYERS_CORE,
    )


def content_sum(frames: int, offset: int) -> int:
    return sum(f + offset for f in range(frames))


@dataclass
class LakeKit:
    base: Path
    roots: RootMap
    storage: LocalStorage
    runs: list[str] = field(default_factory=list[str])
    clock: Clock = field(default_factory=lambda: Clock(T0))
    settings: Settings = field(default_factory=lambda: load_defaults("laptop"))
    leases: dict[int, StubLease] = field(default_factory=dict[int, StubLease])

    def day(self, day: float) -> None:
        self.clock.now = T0 + timedelta(days=day)

    def catalog(self, run_id: str = "run-1", lease: StubLease | None = None) -> WorkingCatalog:
        self.runs.append(run_id)
        work = self.base / "work" / f"{run_id}-{len(self.runs)}"
        held = lease if lease is not None else StubLease(run_id=run_id)
        cat = WorkingCatalog.open(self.storage, self.settings, held, work_dir=work, clock=self.clock)
        self.leases[id(cat)] = held
        return cat

    def lease_of(self, cat: WorkingCatalog) -> StubLease:
        return self.leases[id(cat)]

    def stamp(self, cat: WorkingCatalog) -> None:
        """Give every DuckLake snapshot not yet stamped the kit's simulated time."""
        path = cat.detach()
        conn = duckdb.connect(str(path))
        try:
            conn.execute(
                "UPDATE ducklake_snapshot SET snapshot_time = ? WHERE snapshot_time > ?",
                [self.clock.now, T0 + timedelta(days=39)],
            )
            conn.execute("CHECKPOINT")
        finally:
            conn.close()
        cat.attach()

    def commit(self, cat: WorkingCatalog, kind: VersionKind = "run_end") -> VersionInfo:
        self.stamp(cat)
        return cat.commit(kind)

    def cleanup(self, cat: WorkingCatalog, *, allow_large_sweep: bool = False) -> CleanupReport:
        """A run's end: cleanup at the simulated day, then the final catalog commit (§10.3 steps 6 to 8)."""
        report = run_cleanup(
            cat,
            self.storage,
            settings=self.settings,
            lease=self.lease_of(cat),
            allow_large_sweep=allow_large_sweep,
            now=self.clock.now,
        )
        self.commit(cat)
        return report

    def written(self, match_key: int, *, visibility: Visibility, frames: int, offset: int) -> WrittenFile:
        path = write_tracking_file(
            PLAYERS,
            players(match_key, frames, offset),
            match_key=match_key,
            content_version="v1",
            out_dir=self.base / "out",
        )
        return store_written_file(
            self.storage, path, table=PLAYERS, visibility=visibility, rows=frames, content_version="v1"
        )

    def add_match(
        self, cat: WorkingCatalog, match_key: int, *, visibility: Visibility, frames: int, offset: int
    ) -> WrittenFile:
        file = self.written(match_key, visibility=visibility, frames=frames, offset=offset)
        register_match_files(cat, [file])
        return file

    def replace_match(
        self, cat: WorkingCatalog, match_key: int, *, visibility: Visibility, frames: int, offset: int
    ) -> WrittenFile:
        file = self.written(match_key, visibility=visibility, frames=frames, offset=offset)
        replace_match_files(cat, PLAYERS, match_key, [file])
        return file

    def read_match(self, match_key: int) -> tuple[int, int]:
        """`(rows, sum(x_mm))` of the match through a fresh reader, by value."""
        reader = CatalogReader(self.storage)
        try:
            with reader.session() as conn:
                row = conn.execute(
                    f"SELECT count(*), coalesce(sum(x_mm), 0) FROM lake.{PLAYERS} WHERE match_key = ?",  # noqa: S608 - constant name
                    [match_key],
                ).fetchone()
        finally:
            reader.close()
        assert row is not None
        return int(row[0]), int(row[1])

    def objects(self, root: str, prefix: str = "data/bronze/") -> list[str]:
        return [info.key for info in self.storage.list("lake" if root == "lake" else "restricted", prefix)]


def move_now(
    cat: WorkingCatalog, storage: Storage, match_key: int, to_root: Root, *, archive: RawArchive | None = None
) -> list[MoveRecord]:
    """A match move published at once with a direct commit, for tests below the run engine. The run publishes a move
    through its checkpoint policy (T3 spec D2); `move_match_files` takes the publish as an argument."""
    return move_match_files(cat, storage, match_key, to_root, archive=archive, publish=lambda: cat.commit("in_run"))


@dataclass
class FakeLedger:
    """A scripted `LeaseLedger` (T6 spec D1) for the lease tests. `script` gives the outcome of each `claim` in turn:
    "ok"; "conflict" (another holder's commit of the same generation lands first: a parent-check failure);
    "lost_reply" (the commit lands but its reply is lost); "rate_limit" (HTTP 429); "outage" (a connection error).
    `later_heads` are entries that later `head()` calls report, in turn, after the first commit (a stalled winner's
    re-read sees a newer generation). `head_error` makes every `head()` raise. `stale_reads` makes that many `head()`
    calls after a "conflict" still report the head from before it (the Hub's `repo_info` lags a commit)."""

    entry: LedgerEntry | None = None
    committed_at: datetime = field(default_factory=_real_now)
    script: list[str] = field(default_factory=list[str])
    later_heads: list[LedgerEntry] = field(default_factory=list[LedgerEntry])
    head_error: BaseException | None = None
    stale_reads: int = 0
    on_claim: Any = None  # called inside claim, before it lands (for example: an old image writes the bucket)
    commits: list[LedgerEntry] = field(default_factory=list[LedgerEntry])
    heads: int = 0
    _seq: int = 0
    _committed: bool = False
    _stale: LedgerHead | None = None

    @property
    def sha(self) -> str:
        return f"c{self._seq}"

    def _land(self, entry: LedgerEntry) -> None:
        self.entry = entry
        self._seq += 1
        self.committed_at = _real_now()
        self.commits.append(entry)
        self._committed = True

    def head(self) -> LedgerHead:
        self.heads += 1
        if self.head_error is not None:
            raise self.head_error
        if self._stale is not None and self.stale_reads > 0:
            self.stale_reads -= 1
            return self._stale
        if self._committed and self.later_heads:
            self._land(self.later_heads.pop(0))
        return LedgerHead(sha=self.sha, entry=self.entry, committed_at=self.committed_at)

    def claim(self, entry: LedgerEntry, *, parent: str) -> str:
        if self.on_claim is not None:
            self.on_claim()
        outcome = self.script.pop(0) if self.script else "ok"
        if parent != self.sha:
            raise LedgerConflictError(f"fake: the head moved past {parent}")
        if outcome == "conflict":
            self._stale = LedgerHead(sha=self.sha, entry=self.entry, committed_at=self.committed_at)
            self._land(entry.model_copy(update={"holder_id": "other-host", "run_id": "other-run"}))
            raise LedgerConflictError("fake: another holder committed first")
        if outcome == "lost_reply":
            self._land(entry)
            raise TimeoutError("fake: the commit landed but its reply was lost")
        if outcome == "rate_limit":
            raise LedgerUnavailableError("fake: HTTP 429")
        if outcome == "outage":
            raise ConnectionError("fake: the Hub is unreachable")
        self._land(entry)
        return self.sha


class LedgerStorage(LocalStorage):
    """A local storage that fences its lease on a `FakeLedger`, as an HF writer does (T6 spec D1)."""

    def __init__(self, roots: RootMap, ledger: FakeLedger) -> None:
        super().__init__(roots)
        self.ledger = ledger

    def lease_ledger(self) -> FakeLedger:  # type: ignore[override]
        return self.ledger
