"""The checkpoint policy: every in-run catalog publish of a run (T3 spec D1, D4, D8).

A deferrable site calls `maybe(site)`: it publishes only when `minutes` (T) have passed since the last publish or
`matches` (M) units are pending. A barrier site calls `now(site)`: it always publishes. The run end calls `run_end()`.
There is one working catalog, so every publish carries all pending work. Before each publish the policy runs the
bronze integrity guard (`assert_bronze_integrity`); a guard failure raises `GuardStopError` and nothing is published.
A lost lease raises `LeaseLostError` from the commit, before anything is stored.

After each in-run publish the policy deletes this run's own in-run versions whose successor has been published for
at least the grace G, so a reader that opened a version a moment earlier keeps it (D4). It knows its versions from
the sequences it published; it never lists the version store per publish. After the run-end commit it runs the
§10.8 version pruning, with the grace, which also removes a crashed predecessor's in-run versions. A delete that a
Windows reader blocks is postponed and retried at the next publish, and the version still counts as retained.
"""

from __future__ import annotations

import itertools
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from lakehouse.bronze.writers import assert_bronze_integrity
from lakehouse.catalog.cleanup import delete_versions, plan_version_pruning
from lakehouse.catalog.manifest import VersionKind
from lakehouse.catalog.publish import listed_sequences
from lakehouse.catalog.restore import read_restores
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import CheckpointConfig, RetentionConfig
from lakehouse.logging import get_logger
from lakehouse.runs._common import RUN_LOGGER_NAME, ErrorSink
from lakehouse.storage.base import Storage

__all__ = ["CheckpointPolicy", "PublishRecord", "PublishTotals"]

_MB: Final = 1024 * 1024

_log = get_logger(RUN_LOGGER_NAME)


@dataclass(frozen=True)
class PublishRecord:
    seq: int
    kind: VersionKind
    reason: str
    size: int
    at: datetime


@dataclass(frozen=True)
class PublishTotals:
    publishes: int
    bytes_published: int
    max_retained: int
    guard_ms: int
    compaction_ms: int


class CheckpointPolicy:
    """Decides and performs every catalog publish of one run (see the module docstring)."""

    def __init__(
        self,
        cat: WorkingCatalog,
        config: CheckpointConfig,
        *,
        retention: RetentionConfig,
        run_id: str,
        now: Callable[[], datetime],
        errors: ErrorSink,
        storage: Storage,
    ) -> None:
        self._cat = cat
        self._config = config
        self._retention = retention
        self._run_id = run_id
        self._clock = now
        self._errors = errors
        self._storage = storage
        self._grace = timedelta(minutes=config.grace_minutes)
        self._last = now()
        self._units = 0
        self._published: list[PublishRecord] = []
        self._pruned: set[int] = set()
        self._retained = len(listed_sequences(storage))  # listed once; publishes and confirmed deletes keep it
        self._max_retained = self._retained
        self._bytes = 0
        self._guard_ms = 0
        self._compaction_ms = 0
        self._warned: set[str] = set()
        self._before_publish: list[Callable[[], None]] = []

    def on_publish(self, flush: Callable[[], None]) -> None:
        """Run `flush` before every publish, ahead of the guard, so the publish carries what it wrote (T3 spec O-5)."""
        self._before_publish.append(flush)

    @property
    def published(self) -> Sequence[PublishRecord]:
        return tuple(self._published)

    @property
    def retained_versions(self) -> int:
        return self._retained

    def maybe(self, reason: str) -> PublishRecord | None:
        """A deferrable site: publish when T has passed or M units are pending; otherwise keep the work pending."""
        self._units += 1
        if self._clock() - self._last >= timedelta(minutes=self._config.minutes):
            return self._publish("in_run", "threshold_time", site=reason, prune=self._prune_own)
        if self._units >= self._config.matches:
            return self._publish("in_run", "threshold_matches", site=reason, prune=self._prune_own)
        return None

    def now(self, reason: str) -> PublishRecord:
        """A barrier site: always publish (a destructive storage action or a privacy change depends on it)."""
        return self._publish("in_run", reason, site=reason, prune=self._prune_own)

    def run_end(self) -> PublishRecord:
        """The run-end publish, then the §10.8 pruning with the grace, still under the lease (D4)."""
        return self._publish("run_end", "run_end", site="run_end", prune=self._prune_after_run_end)

    def totals(self) -> PublishTotals:
        return PublishTotals(
            publishes=len(self._published),
            bytes_published=self._bytes,
            max_retained=self._max_retained,
            guard_ms=self._guard_ms,
            compaction_ms=self._compaction_ms,
        )

    # --- internals ---------------------------------------------------------------------------------------------------

    def _publish(
        self, kind: VersionKind, reason: str, *, site: str, prune: Callable[[PublishRecord], None]
    ) -> PublishRecord:
        for flush in self._before_publish:
            flush()
        started = time.perf_counter()
        assert_bronze_integrity(self._cat)
        guard_ms = int((time.perf_counter() - started) * 1000)
        info = self._cat.commit(kind)
        compaction_ms = info.compaction.ms if info.compaction is not None else 0
        self._compaction_ms += compaction_ms
        if info.compaction is not None and info.compaction.failed_check is not None:
            self._errors(  # the original catalog was published; the failed rewrite is never silent (T3 spec D7)
                kind="catalog_compaction",
                message=(f"the catalog rewrite failed ({info.compaction.failed_check}); the original was published"),
                severity="warning",
            )
        record = PublishRecord(seq=info.seq, kind=kind, reason=reason, size=info.size, at=info.published_at)
        self._published.append(record)
        self._units = 0
        self._last = self._clock()
        self._retained += 1
        self._bytes += info.size
        self._guard_ms += guard_ms
        prune(record)
        self._max_retained = max(self._max_retained, self._retained)
        _log.info(
            "catalog publish",
            extra={
                "run_id": self._run_id,
                "seq": record.seq,
                "kind": kind,
                "reason": reason,
                "site": site,
                "bytes": record.size,
                "bytes_published_run": self._bytes,
                "retained_versions": self._retained,
                "guard_ms": guard_ms,
                "compaction_ms": compaction_ms,
            },
        )
        self._warn_growth(record)
        return record

    def _prune_own(self, current: PublishRecord) -> None:
        """Delete this run's own in-run versions whose successor was published at least G ago (D4). A version whose
        delete was postponed is not in `_pruned`, so the next publish selects it again."""
        cutoff = self._clock() - self._grace
        due = [
            earlier.seq
            for earlier, later in itertools.pairwise(self._published)
            if earlier.kind == "in_run" and earlier.seq not in self._pruned and later.at <= cutoff
        ]
        self._delete([seq for seq in due if seq != current.seq])

    def _prune_after_run_end(self, current: PublishRecord) -> None:
        dropped = frozenset(seq for record in read_restores(self._cat.conn) for seq in record.dropped_seqs)
        due = plan_version_pruning(
            self._storage,
            self._retention,
            dropped=dropped,
            now=self._clock(),
            current_seq=current.seq,
            grace=self._grace,
        )
        self._delete(due)

    def _delete(self, seqs: Sequence[int]) -> None:
        if not seqs:
            return
        confirmed = set(delete_versions(self._storage, seqs))
        self._pruned |= confirmed
        self._retained -= len(confirmed)

    def _warn_growth(self, record: PublishRecord) -> None:
        if self._retained > self._config.warn_versions and "versions" not in self._warned:
            self._warned.add("versions")
            self._errors(
                kind="catalog_growth",
                message=(
                    f"{self._retained} retained catalog versions (warning threshold {self._config.warn_versions})"
                ),
                severity="warning",
            )
        # A storage whose conditional writes stop at a size refuses a larger catalog copy (HF, T6 spec D2): the warning
        # then fires at half that limit at the latest, whatever threshold the saved config holds.
        limit = self._storage.single_part_limit
        threshold = self._config.warn_catalog_mb * _MB
        if limit is not None:
            threshold = min(threshold, limit // 2)
        if record.size > threshold and "size" not in self._warned:
            self._warned.add("size")
            refusal = f"; the storage refuses a copy above {limit // _MB} MB" if limit is not None else ""
            self._errors(
                kind="catalog_growth",
                message=(
                    f"the published catalog copy is {record.size // _MB} MB "
                    f"(warning threshold {threshold // _MB} MB{refusal})"
                ),
                severity="warning",
            )
