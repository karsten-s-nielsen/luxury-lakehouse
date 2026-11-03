"""One ingestion run (spec §9, §10.3, §12.3, §17, §29.13, §29.14; plan Task 21).

The steps, in order: take the lease (or write a skip record and return `skipped`); open the working catalog from
`CURRENT`; fold skip records into run history; delete the leftovers of public-to-private moves and prune the
settled raw records; read the saved config once; list every pining module that holds ingested matches in full, for
visibility, and move each match whose flag changed (no download); for each active module, list, filter, classify each
match against lineage (new, changed, unchanged, visibility changed, excluded), fetch, archive, parse, choose the kept
copy (§9), write the bronze files to the root of the match's visibility, register them and commit per match; then the
resolution of every entity type (§15.3, Task 22), dbt (Task 23), cleanup (§10.3 steps 6 and 7), the
run-history row, the run-end commit
and the lease release. A match that fails is recorded and the run goes on (§12.3); a lost lease, a catalog
conflict or a guard stop ends the run.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import threading
import uuid
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any, Final, Literal, Protocol, cast

from lakehouse.bronze.listings import listing_key
from lakehouse.bronze.writers import select_listing_writer
from lakehouse.catalog.cleanup import run_cleanup
from lakehouse.catalog.maintenance import MaintenanceReport, maintain_tables
from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.moves import delete_moved_leftovers, prune_settled_raw_records, read_moves
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import ModuleId, ResourceCaps, Settings
from lakehouse.config.store import ConfigStore
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import ConfigError, GuardStopError, UnmappedFieldError
from lakehouse.ids.keys import match_key
from lakehouse.ids.native import Channel, Provider
from lakehouse.lineage.model import LineageRow, public_contents, read_lineage
from lakehouse.logging import get_logger
from lakehouse.parsers.base import ListingParser, ProviderParser
from lakehouse.parsers.pining import PiningListingParser
from lakehouse.raw.archive import Delivery, RawArchive
from lakehouse.resolve.links import run_resolution
from lakehouse.runs._common import (
    FATAL as _FATAL,
)
from lakehouse.runs._common import (
    FullListing,
)
from lakehouse.runs._common import (
    ModuleContext as _ModuleContext,
)
from lakehouse.runs._common import (
    Tally as _Tally,
)
from lakehouse.runs.checkpoint import CheckpointPolicy
from lakehouse.runs.dbt import DbtResult, run_dbt
from lakehouse.runs.history import (
    ErrorRow,
    MatchStep,
    ModuleRun,
    Outcome,
    RunRow,
    error_message,
    read_table,
    replace_listing,
    write_errors,
    write_module_run,
    write_run,
    write_steps,
)
from lakehouse.runs.match_files import MatchFiles
from lakehouse.runs.match_writer import MatchWriter
from lakehouse.runs.memory import peak_footprint_mb, peak_rss_mb
from lakehouse.runs.progress import Progress, write_progress
from lakehouse.runs.visibility import VisibilityMover
from lakehouse.sources.base import (
    ChannelAdapter,
    ListingSource,
    PreviewResult,
    selected_match_ids,
    tournament_selected,
)
from lakehouse.storage.base import Storage
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV, resolve_encryption_key
from lakehouse.storage.keys import CanonicalKey
from lakehouse.storage.lease import Lease, SkipRecord, fold_skip_records
from lakehouse.telemetry import AttributeName, AttributeValue, step_span

__all__ = ["RunCounts", "RunError", "RunResult", "Trigger", "exit_code", "new_run_id", "run_ingestion"]

Trigger = Literal["schedule", "button", "cli"]
Status = Literal["ok", "failed", "skipped"]
_EXIT: Final[dict[Status, int]] = {"ok": 0, "failed": 1, "skipped": 3}
_log = get_logger(__name__)


@dataclass(frozen=True)
class RunCounts:
    added: int = 0
    changed: int = 0
    unchanged: int = 0
    failed: int = 0
    skipped: int = 0
    excluded: int = 0
    quarantined: int = 0


@dataclass(frozen=True)
class RunError:
    module_id: str | None
    native_match_id: str | None
    kind: str
    message: str


@dataclass(frozen=True)
class RunResult:
    run_id: str
    status: Status
    counts: RunCounts
    errors: list[RunError]


class DbtStep(Protocol):
    """The dbt step of a run: `run_dbt` (plan Task 23). `None` in its place skips dbt; only tests of other steps do."""

    def __call__(self, cat: WorkingCatalog, *, run_id: str, caps: ResourceCaps) -> DbtResult: ...


def exit_code(result: RunResult) -> int:
    """0 for `ok`, 1 for `failed`, 3 for `skipped`."""
    return _EXIT[result.status]


def new_run_id(now: datetime) -> str:
    """A run id: the UTC start time and a random suffix (`20261027T120000Z-1a2b3c4d`)."""
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _run_attrs(rid: str, trigger: str, env: Mapping[str, str] | None) -> dict[AttributeName, AttributeValue]:
    """The `lakehouse.run` span's attributes, including the spend context read from the run environment (spec §7.3):
    `execution_context` (default `local`) and the HF-Job `flavour`. The exporter prices the run from these."""
    source = env or {}
    attrs: dict[AttributeName, AttributeValue] = {
        "lakehouse.run_id": rid,
        "lakehouse.trigger": trigger,
        "lakehouse.execution_context": source.get("LAKEHOUSE_EXECUTION_CONTEXT", "local"),
    }
    flavour = source.get("LAKEHOUSE_HF_FLAVOUR")
    if flavour:
        attrs["lakehouse.flavour"] = flavour
    return attrs


class _Stop(Exception):  # noqa: N818 - an internal control signal: the run was asked to stop
    pass


def _signal_unchanged(ref: ArtifactRef, row: LineageRow) -> bool:
    """Whether the listed signal says the stored copy is current (§17.3): pining compares `artifact_meta.sha256`
    with the delivery hash, the other channels compare the change signal."""
    if ref.expected_sha256 is not None:
        return ref.expected_sha256 == row.delivery_sha256
    return ref.change_signal is not None and ref.change_signal == row.change_signal


class _Run:
    def __init__(
        self,
        *,
        storage: Storage,
        trigger: Trigger,
        adapters: Mapping[ModuleId, ChannelAdapter],
        parsers: Mapping[Provider, ProviderParser],
        now: Callable[[], datetime],
        run_id: str,
        lease: Lease,
        settings: Settings,
        cat: WorkingCatalog,
        work_dir: Path,
        stop: Callable[[], bool],
    ) -> None:
        self.storage, self.trigger, self.adapters, self.parsers = storage, trigger, adapters, parsers
        self.now, self.run_id, self.lease, self.settings, self.cat = now, run_id, lease, settings, cat
        self.work_dir, self.stop = work_dir, stop
        self.archive = RawArchive(storage, enabled=settings.raw_archive, key=self.cat.encryption_key)
        self.checkpoints = CheckpointPolicy(
            self.cat,
            settings.checkpoint,
            retention=settings.retention,
            run_id=run_id,
            now=now,
            errors=self._error,
            storage=storage,
        )  # every in-run publish goes through the policy (T3 spec D1)
        self._files = MatchFiles(
            self.cat, self.storage, self.archive, self.run_id, self.now, checkpoints=self.checkpoints
        )
        self._visibility = VisibilityMover(
            self.cat, self.settings, self.adapters, self._files, self._error, confirm_lease=self.lease.confirm
        )
        self._writer = MatchWriter(
            self.cat,
            self.storage,
            self.work_dir,
            self.run_id,
            self.now,
            self._files,
            self._error,
            checkpoints=self.checkpoints,
        )
        self.errors: list[RunError] = []
        self.failed_run = False
        self.maintenance: MaintenanceReport | None = None
        self.terminated = False
        self.started_at = now()
        self.progress = Progress(
            run_id=run_id,
            trigger=trigger,
            status="running",
            step="start",
            module_id=None,
            done=0,
            total=None,
            started_at=self.started_at,
            updated_at=self.started_at,
            message=None,
        )
        self.matches_done = 0
        self.totals = _Tally()
        self.force_parse = False  # the offline re-parse parses whatever the signals say (§13.2)
        write_progress(storage, self.progress)

    # --- bookkeeping -------------------------------------------------------------------------------------------

    def _step(self, step: str, module_id: str | None = None) -> None:
        self.progress = self.progress.advance(at=self.now(), step=step, module_id=module_id, done=self.matches_done)
        write_progress(self.storage, self.progress)

    def _error(
        self,
        *,
        kind: str,
        message: str,
        severity: Literal["error", "warning"],
        module_run_id: str | None = None,
        module_id: str | None = None,
        native: str | None = None,
    ) -> None:
        write_errors(
            self.cat.conn,
            self.run_id,
            [ErrorRow(module_run_id, module_id, native, kind, severity, message, self.now())],
        )
        if severity == "error":
            self.errors.append(RunError(module_id, native, kind, message))
            self.failed_run = True

    def _check_stop(self) -> None:
        if self.stop():
            raise _Stop
        # every write step passes here: a writer that lost its lease stops before it writes (T3 spec D5)
        self.lease.confirm()

    # --- start of the run ---------------------------------------------------------------------------------------

    def fold_skips(self) -> None:
        def record(skip: SkipRecord) -> None:
            write_run(
                self.cat.conn,
                RunRow(skip.run_id, None, "skipped", skip.reason, skip.at, skip.at, 0, 0, 0, 0, 0, 0),
            )
            self.checkpoints.now("fold_skips")  # a barrier: the history row is durable before its skip record goes

        fold_skip_records(self.storage, record=record)

    def settle_moves(self) -> None:
        current = self.cat.current
        manifest: frozenset[CanonicalKey] = (
            frozenset(read_manifest(self.storage, current.seq).canonical_keys) if current is not None else frozenset()
        )
        public = public_contents(read_lineage(self.cat.conn))
        delete_moved_leftovers(
            self.storage, read_moves(self.cat.conn), current_manifest=manifest, public_contents=public
        )
        prune_settled_raw_records(self.cat.conn, self.storage, public_contents=public)

    # --- visibility (§9, §29.14) ----------------------------------------------------------------------------------

    def visibility_pass(self) -> dict[ModuleId, list[SourceEntry]]:
        """Run the visibility pass and OR its failure flag into the run's (plan Task 3; `VisibilityMover`)."""
        listings, failed = self._visibility.run()
        self.failed_run = self.failed_run or failed
        return listings

    # --- modules ------------------------------------------------------------------------------------------------

    def modules(self, listings: Mapping[ModuleId, list[SourceEntry]]) -> None:
        for module in self.settings.modules:
            if not module.active:
                continue
            self._check_stop()
            module_run_id = f"{self.run_id}.{module.module_id}"
            started = self.now()
            adapter = self.adapters.get(module.module_id)
            if not module.filter:
                self._error(
                    kind="empty_filter",
                    message=f"{module.module_id} is active with an empty filter: nothing is downloaded",
                    severity="warning",
                    module_run_id=module_run_id,
                    module_id=module.module_id,
                )
                write_module_run(
                    self.cat.conn,
                    ModuleRun(module_run_id, self.run_id, module.module_id, "ok", started, self.now(), 0, 0, 0, 0, 0),
                )
                continue
            if adapter is None or adapter.provider not in self.parsers:
                self._error(
                    kind="module_unavailable",
                    message=f"no adapter or parser for {module.module_id}",
                    severity="error",
                    module_run_id=module_run_id,
                    module_id=module.module_id,
                )
                continue
            context = _ModuleContext(module, adapter, self.parsers[adapter.provider], module_run_id)
            status: str = "ok"
            try:
                self._module(context, listings.get(module.module_id))
            except _Stop:
                status = "failed"
                raise
            finally:
                self._record_module(context, started, status)

    def _record_module(self, context: _ModuleContext, started: datetime, status: str) -> None:
        """The module run's steps and history row, and its counts added to the run's."""
        tally = context.tally
        write_steps(self.cat.conn, context.steps)
        write_module_run(
            self.cat.conn,
            ModuleRun(
                context.module_run_id,
                self.run_id,
                context.module.module_id,
                "failed" if tally.failed else status,
                started,
                self.now(),
                tally.added,
                tally.changed,
                tally.unchanged,
                tally.failed,
                tally.excluded,
            ),
        )
        for name in ("added", "changed", "unchanged", "failed", "excluded", "quarantined"):
            setattr(self.totals, name, getattr(self.totals, name) + getattr(tally, name))

    # --- the offline re-parse (§13.2) ----------------------------------------------------------------------------

    def reparse(self, targets: Sequence[_Target]) -> None:
        """Re-parse each target from the raw archive through the normal write path (moves, precedence, lineage)."""
        by_module: dict[ModuleId, list[_Target]] = {}
        for target in targets:
            by_module.setdefault(target.entry.module_id, []).append(target)
        modules = {m.module_id: m for m in self.settings.modules}
        for module_id, items in sorted(by_module.items()):
            self._check_stop()
            first = items[0]
            context = _ModuleContext(
                modules[module_id], first.adapter, self.parsers[first.adapter.provider], f"{self.run_id}.{module_id}"
            )
            started = self.now()
            status = "ok"
            try:
                self._match_steps(context, [t.entry for t in items])
            except _Stop:
                status = "failed"
                raise
            finally:
                self._record_module(context, started, status)

    def _listed(self, context: _ModuleContext, full: list[SourceEntry] | None) -> list[SourceEntry]:
        module = context.module
        with step_span(
            "lakehouse.run.list",
            {"lakehouse.module_id": module.module_id, "lakehouse.layer": "bronze"},
            log=_log,
        ) as step:
            if full is None and context.adapter.channel == "pining":
                full = cast("FullListing", context.adapter).list_all_entries()
            if full is not None:  # a pining module: the run applies the clauses to the one full listing
                wanted = selected_match_ids(module)
                entries = [
                    e
                    for e in full
                    if tournament_selected(module, e.competition_id or "", e.season_id or "")
                    and (wanted is None or e.native_match_id in wanted)
                ]
            else:
                entries = list(context.adapter.list_entries())
            step.set("lakehouse.rows", len(entries))
        private = context.adapter.channel == "pining"
        for warning in getattr(context.adapter, "warnings", []):
            self._error(
                kind="adapter_warning",
                # a warning on a pining channel can quote private content: only its presence is stored (§9, audit T5)
                message=f"{module.module_id}: a warning on a private channel (text withheld)"
                if private
                else str(warning),
                severity="warning",
                module_run_id=context.module_run_id,
                module_id=module.module_id,
            )
        if module.statsbomb_360_only:
            entries = [e for e in entries if e.has_360 is True]  # §29.13: no fetch for a match without 360 data
        return entries

    def _module(self, context: _ModuleContext, full: list[SourceEntry] | None) -> None:
        try:
            entries = self._listed(context, full)
        except _FATAL:
            raise
        except Exception as exc:  # noqa: BLE001 - a module whose listing fails is recorded and the other modules run (§12.3)
            # a pining error can quote private content: store its class only, like the per-match handlers (§9, audit T5)
            visibility: Literal["public", "private"] = "private" if context.adapter.channel == "pining" else "public"
            self._error(
                kind="list_failed",
                message=f"{context.module.module_id}: {error_message(exc, visibility=visibility)}",
                severity="error",
                module_run_id=context.module_run_id,
                module_id=context.module.module_id,
            )
            context.tally.failed += 1
            return
        self._step("list", context.module.module_id)
        listed_keys = {match_key(context.adapter.provider, e.native_match_id) for e in entries}
        self._count_excluded(context, listed_keys)
        self._match_steps(context, entries)
        if isinstance(context.adapter, ListingSource):
            self._listings(context, context.adapter)

    def _match_steps(self, context: _ModuleContext, entries: Sequence[SourceEntry]) -> None:
        for listed in entries:
            self._check_stop()
            outcome = self._match_guarded(context, listed)
            context.tally.add(outcome)
            context.steps.append(
                MatchStep(
                    step_id=f"{context.module_run_id}.{listed.native_match_id}",
                    run_id=self.run_id,
                    module_run_id=context.module_run_id,
                    module_id=context.module.module_id,
                    match_key=match_key(context.adapter.provider, listed.native_match_id),
                    native_match_id=listed.native_match_id,
                    outcome=outcome,
                    visibility=listed.visibility,
                    recorded_at=self.now(),
                )
            )
            self.matches_done += 1
            self._step("match", context.module.module_id)

    # --- source-level listings (§10.5) ----------------------------------------------------------------------------

    def _listing_parser(self, context: _ModuleContext) -> ListingParser | None:
        if context.adapter.channel == "pining":
            return PiningListingParser(context.adapter.provider)
        return context.parser if isinstance(context.parser, ListingParser) else None

    def _listings(self, context: _ModuleContext, source: ListingSource) -> None:
        parser = self._listing_parser(context)
        if parser is None:
            return
        known = {row["listing_key"]: row for row in read_table(self.cat.conn, "ops_listings")}
        module_id = context.module.module_id
        writer = select_listing_writer(self.cat, self.storage)
        for kind in source.listing_kinds():
            self._check_stop()
            key = listing_key(context.module.module_id, kind)
            try:
                with step_span(
                    "lakehouse.run.list", {"lakehouse.module_id": module_id, "lakehouse.layer": "bronze"}, log=_log
                ) as step:
                    delivery = source.fetch_listing(kind)
                    step.set("lakehouse.bytes", len(delivery.raw))
                self.archive.store(delivery)
                if key in known and known[key]["content_sha256"] == delivery.content_sha256:
                    continue  # the same listing: nothing to write (§17.3)
                table = parser.listing_table(kind)
                records = parser.listing(
                    kind, delivery.decoded, run_id=self.run_id, source_hash=delivery.content_sha256
                )
                with step_span(
                    "lakehouse.run.write", {"lakehouse.module_id": module_id, "lakehouse.layer": "bronze"}, log=_log
                ) as step:
                    step.set(
                        "lakehouse.rows",
                        writer.write(
                            table,
                            records,
                            key=key,
                            visibility_column=parser.listing_visibility(kind),
                            source_visibility=delivery.visibility,
                            content_version=delivery.content_sha256[:16],
                            out_dir=self.work_dir / "files" / "listings",
                        ),
                    )
                row = {
                    "listing_key": key,
                    "module_id": context.module.module_id,
                    "kind": kind,
                    "bronze_table": table,
                    "content_sha256": delivery.content_sha256,
                    "run_id": self.run_id,
                    "updated_at": self.now(),
                }
                with step_span(
                    "lakehouse.run.commit", {"lakehouse.module_id": module_id, "lakehouse.layer": "bronze"}, log=_log
                ) as step:
                    writer.swap(table, key, also=lambda row=row: replace_listing(self.cat.conn, row))
                    # deferrable; the policy runs the integrity guard before the publish (S1-IMPL-14, T3 spec D1)
                    record = self.checkpoints.maybe("listing")
                    if record is not None:
                        step.set("lakehouse.catalog_seq", record.seq)
            except _FATAL:
                raise
            except Exception as exc:  # noqa: BLE001 - a failed listing is recorded and the run goes on (§12.3)
                private = context.adapter.channel == "pining"
                self._error(
                    kind="listing_failed",
                    message=f"{kind}: {error_message(exc, visibility='private' if private else 'public')}",
                    severity="error",
                    module_run_id=context.module_run_id,
                    module_id=context.module.module_id,
                )

    def _count_excluded(self, context: _ModuleContext, listed_keys: set[int]) -> None:
        """A match this channel ingested that the filter now excludes is `excluded`: kept, not updated, counted
        (§8.1, §29.13); a selected match that the source no longer lists is left as it is (§9)."""
        ops = self._files.ops_matches()
        channel, provider = context.adapter.channel, context.adapter.provider
        ours = {r.match_key for r in read_lineage(self.cat.conn) if r.channel == channel and r.provider == provider}
        for key in sorted(ours - listed_keys):
            row = ops.get(key)
            if row is None:
                continue
            if not tournament_selected(context.module, row["competition_id"] or "", row["season_id"] or ""):
                context.tally.excluded += 1

    def _match_guarded(self, context: _ModuleContext, listed: SourceEntry) -> Outcome:
        try:
            return self._match(context, listed)
        except _FATAL:
            raise
        except Exception as exc:  # noqa: BLE001 - a match failure is recorded (ops_errors) and the run goes on (§12.3)
            if isinstance(exc, json.JSONDecodeError) and context.adapter.channel == "public":
                # A public source file that is not valid JSON is corrupt UPSTREAM (StatsBomb open-data ships
                # NUL-corrupted 360 files, e.g. match 3845506). We cannot parse it, but one bad public file must
                # not fail the whole ingest: record it loudly (ops_errors, warning) and quarantine the match, so
                # the run completes. Pining/owner-tier data is held to the strict bar and takes the fatal path.
                self._error(
                    kind="source_corrupt",
                    message=error_message(exc, visibility=listed.visibility),
                    severity="warning",
                    module_run_id=context.module_run_id,
                    module_id=context.module.module_id,
                    native=listed.native_match_id,
                )
                _log.warning(
                    "match quarantined: corrupt public source",
                    extra={"module_id": context.module.module_id, "native_match_id": listed.native_match_id},
                )
                return "quarantined"
            kind = "unmapped_field" if isinstance(exc, UnmappedFieldError) else type(exc).__name__
            self._error(
                kind=kind,
                message=error_message(exc, visibility=listed.visibility),
                severity="error",
                module_run_id=context.module_run_id,
                module_id=context.module.module_id,
                native=listed.native_match_id,
            )
            _log.warning(
                "match failed",
                extra={"module_id": context.module.module_id, "native_match_id": listed.native_match_id, "kind": kind},
            )
            return "failed"

    # --- one match ----------------------------------------------------------------------------------------------

    def _match(self, context: _ModuleContext, listed: SourceEntry) -> Outcome:
        adapter, provider, channel = context.adapter, context.adapter.provider, context.adapter.channel
        key = match_key(provider, listed.native_match_id)
        lineage = [r for r in read_lineage(self.cat.conn) if r.match_key == key]
        ours = [r for r in lineage if r.channel == channel]
        newest: dict[str, LineageRow] = {}
        for row in sorted(ours, key=lambda r: (r.first_seen, r.last_listed)):
            newest[row.artifact] = row
        previous = self._files.ops_matches().get(key)
        same_signals = (
            bool(newest)
            and set(newest) == set(listed.artifacts)
            and all(_signal_unchanged(ref, newest[artifact]) for artifact, ref in listed.artifacts.items())
        )
        if same_signals and not self.force_parse:
            self._writer.listed_again(key, context, listed)
            return self._visibility_only(key, previous, channel, listed)
        where: dict[AttributeName, AttributeValue] = {
            "lakehouse.match_key": key,
            "lakehouse.layer": "bronze",
            "lakehouse.module_id": context.module.module_id,
            "lakehouse.provider": adapter.provider,
            "lakehouse.channel": adapter.channel,
        }
        with step_span("lakehouse.run.fetch", where, log=_log) as step:
            deliveries = {artifact: adapter.fetch(listed, artifact) for artifact in sorted(listed.artifacts)}
            step.set("lakehouse.bytes", sum(len(d.raw) for d in deliveries.values()))
        with step_span("lakehouse.run.archive", where, log=_log) as step:
            for delivery in deliveries.values():
                self.archive.store(delivery)
            step.set("lakehouse.bytes", sum(len(d.raw) for d in deliveries.values()))
        same_content = (
            bool(newest)
            and set(newest) == set(deliveries)
            and all(newest[a].content_sha256 == d.content_sha256 for a, d in deliveries.items())
        )
        if same_content and not self.force_parse:  # a new signal for the same bytes: no re-parse (§17.3)
            self._writer.refresh_signals(key, channel, deliveries)
            self._writer.listed_again(key, context, listed)
            return self._visibility_only(key, previous, channel, listed)
        with step_span("lakehouse.run.parse", where, log=_log) as step:
            parsed = context.parser.parse(listed, deliveries, run_id=self.run_id)
            step.set("lakehouse.rows", sum(table.num_rows for table in parsed.tables.values()))
        choice = self._writer.choose(key, channel, listed, parsed, lineage)
        if choice.kept != channel:
            self._writer.record_not_kept(key, context, listed, deliveries, parsed, choice)
            return "unchanged"
        outcome: Outcome = "changed" if previous is not None else "added"
        self._writer.write(key, context, listed, deliveries, parsed, choice, previous)
        return outcome

    def _visibility_only(
        self, key: int, previous: Mapping[str, Any] | None, channel: Channel, listed: SourceEntry
    ) -> Outcome:
        if previous is not None and previous["kept_channel"] == channel and previous["visibility"] != listed.visibility:
            self._files.move(key, listed.visibility, previous)
            return "moved"
        return "unchanged"

    # --- end of the run -----------------------------------------------------------------------------------------

    def finish(self, *, dbt: DbtStep | None) -> Status:
        self._writer.flush_unchanged()  # resolution, dbt and the run end read the refreshed rows (T3 spec O-5)
        if not self.terminated:
            self.lease.confirm()  # resolution writes (T3 spec D5)
            with step_span("lakehouse.run.resolve", {"lakehouse.layer": "resolution"}, log=_log) as step:
                resolution = run_resolution(
                    self.cat.conn, storage=self.storage, settings=self.settings, run_id=self.run_id, now=self.now
                )
                step.set("lakehouse.rows", sum(resolution.links_written.values()))
            if dbt is not None:
                self.lease.confirm()  # dbt writes (T3 spec D5)
                with step_span("lakehouse.run.dbt", {"lakehouse.layer": "dbt"}, log=_log) as step:
                    built = dbt(self.cat, run_id=self.run_id, caps=self.settings.caps)
                    step.set("lakehouse.rows", len(built.results))
                if not built.success:
                    failed = [r.unique_id for r in built.results if r.status in ("error", "fail", "runtime error")]
                    self._error(
                        kind="dbt_failed",
                        message=(
                            f"dbt build failed: {', '.join(failed) or 'no node result'} (see {built.run_results_key})"
                        ),
                        severity="error",
                    )
            self.lease.confirm()  # maintenance writes (T12 spec D3)
            with step_span("lakehouse.run.merge", {"lakehouse.layer": "maintenance"}, log=_log) as step:
                try:
                    self.maintenance = maintain_tables(
                        self.cat, budget_s=float(self.settings.maintenance.merge_budget_seconds)
                    )
                    step.set("lakehouse.rows", sum(t.files_before - t.files_after for t in self.maintenance.tables))
                except GuardStopError as exc:
                    # a guard stop leaves every file as it was; the run still cleans up and publishes its data, and
                    # ends failed (Reviewer B T12-PLAN-04)
                    self.problem("maintenance_guard", str(exc))
            with step_span("lakehouse.run.cleanup", log=_log):
                run_cleanup(
                    self.cat,
                    self.storage,
                    settings=self.settings,
                    lease=self.lease,
                    allow_large_sweep=False,
                    now=self.now(),
                )
        status: Status = "failed" if self.failed_run or self.terminated else "ok"
        totals = self.totals
        write_run(
            self.cat.conn,
            RunRow(
                self.run_id,
                self.trigger,
                status,
                "terminated" if self.terminated else None,
                self.started_at,
                self.now(),
                totals.added,
                totals.changed,
                totals.unchanged,
                totals.failed,
                0,
                totals.excluded,
            ),
        )
        with step_span("lakehouse.run.commit", log=_log) as step:
            # the guard, the run-end commit, then the §10.8 pruning with the grace, under the lease (T3 spec D1, D4)
            step.set("lakehouse.catalog_seq", self.checkpoints.run_end().seq)
        totals = self.checkpoints.totals()
        _log.info(
            "catalog totals",
            extra={
                "run_id": self.run_id,
                "publishes": totals.publishes,
                "bytes_published": totals.bytes_published,
                "max_retained": totals.max_retained,
                "guard_ms": totals.guard_ms,
                "compaction_ms": totals.compaction_ms,
                "merged_files": sum(t.files_before - t.files_after for t in self.maintenance.tables)
                if self.maintenance is not None
                else 0,
                "merge_ms": self.maintenance.ms if self.maintenance is not None else 0,
            },
        )
        self.progress = self.progress.advance(at=self.now(), step="done", status=status, done=self.matches_done)
        write_progress(self.storage, self.progress)
        # §26 item 9: the run's own memory, read from `hf jobs logs`; the footprint is the true macOS peak (audit T11)
        _log.info(
            "run memory",
            extra={"run_id": self.run_id, "peak_rss_mb": peak_rss_mb(), "peak_footprint_mb": peak_footprint_mb()},
        )
        return status

    def problem(self, kind: str, message: str) -> None:
        """An error of the run itself (not of one match): recorded, and the run ends `failed`."""
        self._error(kind=kind, message=message, severity="error")

    def stopped(self) -> None:
        """The run was asked to stop: it ends `failed` with reason `terminated` after the matches it committed."""
        self.terminated = True
        self._error(kind="terminated", message="the run was asked to stop (SIGTERM)", severity="error")

    def counts(self) -> RunCounts:
        t = self.totals
        return RunCounts(
            added=t.added,
            changed=t.changed,
            unchanged=t.unchanged,
            failed=t.failed,
            excluded=t.excluded,
            quarantined=t.quarantined,
        )


ReparseTarget = Literal["all"] | tuple[Provider, str]


@dataclass(frozen=True)
class _Target:
    entry: SourceEntry
    adapter: ChannelAdapter


class _ArchiveAdapter:
    """A channel adapter that serves the kept deliveries of one module's matches from the raw archive (§13.2): the
    re-parse calls no source."""

    def __init__(
        self,
        module_id: ModuleId,
        provider: Provider,
        channel: Channel,
        rows: Mapping[str, Mapping[str, LineageRow]],
        archive: RawArchive,
        now: Callable[[], datetime],
    ) -> None:
        self._module_id: ModuleId = module_id
        self._provider: Provider = provider
        self._channel: Channel = channel
        self._rows, self._archive, self._now = rows, archive, now

    @property
    def module_id(self) -> ModuleId:
        return self._module_id

    @property
    def provider(self) -> Provider:
        return self._provider

    @property
    def channel(self) -> Channel:
        return self._channel

    def list_entries(self) -> Iterator[SourceEntry]:
        raise RuntimeError("the re-parse lists no source")

    def preview(self) -> PreviewResult:
        raise RuntimeError("the re-parse previews no source")

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        row = self._rows[entry.native_match_id][artifact]
        if self._archive.locate(row.content_sha256) is None:
            raise FileNotFoundError(
                f"no raw copy of {artifact} in the archive: the raw archive was off when it arrived (§13.2)"
            )
        return Delivery.from_archive(
            provider=self._provider,
            channel=self._channel,
            native_match_id=entry.native_match_id,
            artifact=artifact,
            decoded=self._archive.read(row.content_sha256),
            wrapper=row.wrapper,
            delivery_sha256=row.delivery_sha256,
            content_sha256=row.content_sha256,
            visibility=row.visibility,
            change_signal=row.change_signal,
            fetched_at=self._now(),
        )


def _reparse_targets(run: _Run, target: ReparseTarget, archive: RawArchive) -> tuple[list[_Target], int, list[str]]:
    """The matches to re-parse: every match of an older parser version (`"all"`), or the one named; the count of
    matches already at the current version; and the problems that make a target impossible."""
    conn = run.cat.conn
    lineage = read_lineage(conn)
    sources = read_table(conn, "ops_match_sources")
    kept: dict[int, dict[str, dict[str, LineageRow]]] = {}
    for row in sorted(lineage, key=lambda r: (r.first_seen, r.last_listed)):
        if row.kept:
            kept.setdefault(row.match_key, {}).setdefault(row.channel, {})[row.artifact] = row
    targets: list[_Target] = []
    problems: list[str] = []
    current = 0
    adapters: dict[tuple[ModuleId, Channel], _ArchiveAdapter] = {}
    rows_by_module: dict[tuple[ModuleId, Channel], dict[str, dict[str, LineageRow]]] = {}
    found = False
    for match in sorted(read_table(conn, "ops_matches"), key=lambda m: (m["provider"], m["native_match_id"])):
        provider, native = match["provider"], match["native_match_id"]
        parser = run.parsers.get(provider)
        if target == "all":
            if parser is None:
                continue
            if match["parser_version"] == parser.version:
                current += 1
                continue
        elif (provider, native) != target:
            continue
        found = True
        if parser is None:
            problems.append(f"no parser for {provider}")
            continue
        channel = match["kept_channel"]
        rows = kept.get(match["match_key"], {}).get(channel)
        module_id = next(
            (s["module_id"] for s in sources if s["match_key"] == match["match_key"] and s["channel"] == channel), None
        )
        if not rows or module_id is None:
            problems.append(f"{provider} match {native}: no kept delivery is recorded")
            continue
        place = (module_id, channel)
        rows_by_module.setdefault(place, {})[native] = rows
        if place not in adapters:
            adapters[place] = _ArchiveAdapter(module_id, provider, channel, rows_by_module[place], archive, run.now)
        channel_id = next(iter(rows.values())).channel_id
        entry = SourceEntry(
            module_id=module_id,
            native_match_id=native,
            competition_id=match["competition_id"],
            competition_name=match["competition_name"],
            season_id=match["season_id"],
            season_name=match["season_name"],
            visibility=match["visibility"],
            artifacts={
                artifact: ArtifactRef(key=artifact, change_signal=row.change_signal, expected_sha256=None, size=None)
                for artifact, row in sorted(rows.items())
            },
            listed_at=run.now(),
            channel_match_id=channel_id if channel_id != native else None,
        )
        targets.append(_Target(entry, adapters[place]))
    if target != "all" and not found:
        problems.append(f"no ingested {target[0]} match {target[1]}")
    return targets, current, problems


def reparse_matches(
    *,
    storage: Storage,
    parsers: Mapping[Provider, ProviderParser],
    target: ReparseTarget,
    now: Callable[[], datetime] = _utc_now,
    run_id: str | None = None,
    holder_id: str = "local",
    work_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
    dbt: DbtStep | None = run_dbt,
    stop: Callable[[], bool] | None = None,
) -> RunResult:
    """The offline re-parse (§13.2): one run that re-parses, from the raw archive, every match whose parser version
    is older than its parser's (`"all"`), or the one match named, and then resolves, builds dbt and cleans up like
    any run. No source is called. A match whose raw copy is missing fails visibly."""
    _check_dbt_key(storage, env, dbt)
    rid = run_id or new_run_id(now())
    with step_span("lakehouse.run.lease", {"lakehouse.run_id": rid}, log=_log) as step:
        lease = Lease.acquire(storage, holder_id=holder_id, run_id=rid, clock=now)
        if lease is not None:
            step.set("lakehouse.lease_generation", lease.generation)
    if lease is None:
        return RunResult(rid, "skipped", RunCounts(skipped=1), [])
    lease.start_heartbeat()
    folder = work_dir if work_dir is not None else Path.cwd() / ".lakehouse-work" / rid
    try:
        with (
            _terminate_flag() as flag,
            step_span("lakehouse.run", _run_attrs(rid, "cli", env), log=_log),
        ):
            with step_span("lakehouse.run.config", log=_log):
                settings, _ = ConfigStore(storage).load()
            cat = WorkingCatalog.open(storage, settings, lease, work_dir=folder / "catalog", env=env, clock=now)
            try:
                run = _Run(
                    storage=storage,
                    trigger="cli",
                    adapters={},
                    parsers=parsers,
                    now=now,
                    run_id=rid,
                    lease=lease,
                    settings=settings,
                    cat=cat,
                    work_dir=folder,
                    stop=lambda: flag.is_set() or (stop is not None and stop()),
                )
                run.force_parse = True
                run.fold_skips()
                run.settle_moves()
                targets, current, problems = _reparse_targets(run, target, run.archive)
                run.totals.unchanged += current
                for problem in problems:
                    run.problem("reparse_target", problem)
                try:
                    run.reparse(targets)
                except _Stop:
                    run.stopped()
                status = run.finish(dbt=dbt)
                return RunResult(rid, status, run.counts(), run.errors)
            finally:
                cat.close()
    finally:
        lease.release()
        shutil.rmtree(folder / "files", ignore_errors=True)


def _check_dbt_key(storage: Storage, env: Mapping[str, str] | None, dbt: DbtStep | None) -> None:
    """Audit T10: the dbt step reads the encryption key from the process environment (an `env_var` reference in its
    rendered profile), not from the run's `env`. A run whose `env` resolves another key would ingest everything and
    then fail at dbt ("Wrong encryption key"), so it stops before it takes the lease. The error names the variable,
    never a value. The CLI passes the process environment, so a CLI run always passes."""
    if dbt is None or env is None:
        return
    if resolve_encryption_key(storage.roots, env) != resolve_encryption_key(storage.roots, os.environ):
        raise ConfigError(
            f"{ENCRYPTION_KEY_ENV} differs between the run's environment and the process environment, and the dbt "
            "step reads the process environment: set the same key in both"
        )


@contextmanager
def _terminate_flag() -> Generator[threading.Event, None, None]:
    """SIGTERM sets the flag; the run stops at its next safe point. The old handler comes back afterwards."""
    flag = threading.Event()
    if threading.current_thread() is not threading.main_thread():
        yield flag
        return

    def handle(signum: int, frame: FrameType | None) -> None:
        flag.set()

    old = signal.signal(signal.SIGTERM, handle)
    try:
        yield flag
    finally:
        signal.signal(signal.SIGTERM, old)


def run_ingestion(
    *,
    storage: Storage,
    trigger: Trigger,
    adapters: Mapping[ModuleId, ChannelAdapter],
    parsers: Mapping[Provider, ProviderParser],
    now: Callable[[], datetime] = _utc_now,
    run_id: str | None = None,
    holder_id: str = "local",
    work_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
    dbt: DbtStep | None = run_dbt,
    stop: Callable[[], bool] | None = None,
    settings: Settings | None = None,
) -> RunResult:
    """One run (see the module docstring). A held lease gives `skipped` (exit code 3) after a skip record. `settings`
    is the saved config the caller already read (the CLI builds the adapters from it), so the run reads it once."""
    _check_dbt_key(storage, env, dbt)
    rid = run_id or new_run_id(now())
    with step_span("lakehouse.run.lease", {"lakehouse.run_id": rid}, log=_log) as step:
        lease = Lease.acquire(storage, holder_id=holder_id, run_id=rid, clock=now)
        if lease is not None:
            step.set("lakehouse.lease_generation", lease.generation)
    if lease is None:
        return RunResult(rid, "skipped", RunCounts(skipped=1), [])
    lease.start_heartbeat()
    folder = work_dir if work_dir is not None else Path.cwd() / ".lakehouse-work" / rid
    try:
        with (
            _terminate_flag() as flag,
            step_span("lakehouse.run", _run_attrs(rid, trigger, env), log=_log),
        ):
            with step_span("lakehouse.run.config", log=_log):
                # read once: a later save is not seen by this run
                loaded = settings if settings is not None else ConfigStore(storage).load()[0]
            cat = WorkingCatalog.open(storage, loaded, lease, work_dir=folder / "catalog", env=env, clock=now)
            try:
                run = _Run(
                    storage=storage,
                    trigger=trigger,
                    adapters=adapters,
                    parsers=parsers,
                    now=now,
                    run_id=rid,
                    lease=lease,
                    settings=loaded,
                    cat=cat,
                    work_dir=folder,
                    stop=lambda: flag.is_set() or (stop is not None and stop()),
                )
                run.fold_skips()
                run.settle_moves()
                try:
                    listings = run.visibility_pass()
                    run.modules(listings)
                except _Stop:
                    run.stopped()
                status = run.finish(dbt=dbt)
                return RunResult(rid, status, run.counts(), run.errors)
            finally:
                cat.close()
    finally:
        lease.release()
        shutil.rmtree(folder / "files", ignore_errors=True)
