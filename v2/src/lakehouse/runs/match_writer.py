"""The per-match kept-copy write and lineage (M1 extraction, plan Task 4).

`MatchWriter` owns choosing the kept copy (§9), writing the bronze files to the root of the match's
visibility, and recording lineage/sources. `_Run._match` orchestrates fetch → archive → parse and then calls
`choose`, `record_not_kept`, `write`, `refresh_signals` and `listed_again`. The writer reads only the identity
fields of `ModuleContext` (`module`, `adapter`, `parser`, `module_run_id`), never the loop's `tally`/`steps`.
It depends on `MatchFiles` for a visibility move and on `_common`; it never imports `run.py`.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lakehouse.bronze.schemas import BronzeTable
from lakehouse.bronze.writers import Put, WrittenRow, select_writer
from lakehouse.catalog.duck import insert_rows
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.entries import SourceEntry
from lakehouse.ids.native import Channel, Provider
from lakehouse.lineage.model import LINEAGE_TABLE, LineageRow, write_lineage
from lakehouse.lineage.precedence import Choice, CopyInfo, choose_copy
from lakehouse.logging import get_logger
from lakehouse.parsers.base import ParsedMatch
from lakehouse.raw.archive import Delivery
from lakehouse.runs._common import (
    AUDIT_NAMES,
    RUN_LOGGER_NAME,
    THREE_SIXTY,
    TRACKING_ARTIFACTS,
    ErrorSink,
    ModuleContext,
    content_version,
    content_version_of,
    nonnull_fields,
    text_or_none,
)
from lakehouse.runs.history import (
    MatchFile,
    MatchRow,
    MatchSource,
    replace_match,
    upsert_sources,
    write_coordinate_systems,
)
from lakehouse.runs.list_fields import MISMATCH_KIND, list_fields_mismatch
from lakehouse.runs.match_files import MatchFiles
from lakehouse.storage.base import Storage
from lakehouse.telemetry import step_span

if TYPE_CHECKING:
    from lakehouse.runs.checkpoint import CheckpointPolicy

_log = get_logger(RUN_LOGGER_NAME)


class MatchWriter:
    """Choose the kept copy, write its bronze files and record lineage (spec §9)."""

    def __init__(
        self,
        cat: WorkingCatalog,
        storage: Storage,
        work_dir: Path,
        run_id: str,
        now: Callable[[], datetime],
        files: MatchFiles,
        errors: ErrorSink,
        *,
        checkpoints: CheckpointPolicy,
    ) -> None:
        self.cat = cat
        self.storage = storage
        self.work_dir = work_dir
        self.run_id = run_id
        self.now = now
        self.files = files
        self.errors = errors
        self.checkpoints = checkpoints
        self._bronze = select_writer(cat, storage)
        # The unchanged-match writes wait in memory and go in one transaction before the next publish (T3 spec O-5):
        # on the encrypted lake each statement stored files and added a snapshot, about 3 and 4 per unchanged match.
        self._listed: list[tuple[int, str, datetime]] = []
        self._signals: list[tuple[int, str, str, str, str | None, str | None]] = []
        self._sources: dict[tuple[int, str], MatchSource] = {}
        checkpoints.on_publish(self.flush_unchanged)

    def listed_again(self, key: int, context: ModuleContext, listed: SourceEntry) -> None:
        at = self.now()
        self._listed.append((key, context.adapter.channel, at))
        source = self._source(key, context, listed, at)
        self._sources[(source.match_key, source.module_id)] = source

    def refresh_signals(self, key: int, channel: Channel, deliveries: Mapping[str, Delivery]) -> None:
        for artifact, delivery in deliveries.items():
            self._signals.append(
                (key, channel, artifact, delivery.content_sha256, delivery.change_signal, delivery.delivery_sha256)
            )

    def flush_unchanged(self) -> None:
        """Write the buffered unchanged-match rows in one DuckLake transaction (T3 spec O-5). The checkpoint policy
        calls it before every publish, and the run end before resolution and dbt."""
        if not (self._listed or self._signals or self._sources):
            return
        listed, signals, sources = self._listed, self._signals, list(self._sources.values())
        self._listed, self._signals, self._sources = [], [], {}
        conn = self.cat.conn
        conn.execute("BEGIN")
        try:
            if signals:
                rows = ", ".join("(?, ?, ?, ?, ?, ?)" for _ in signals)
                conn.execute(
                    f"UPDATE lake.main.{LINEAGE_TABLE} AS l SET change_signal = v.change_signal, "  # noqa: S608 - constant name, placeholders
                    f"delivery_sha256 = v.delivery_sha256 FROM (VALUES {rows}) "
                    "AS v(match_key, channel, artifact, content_sha256, change_signal, delivery_sha256) "
                    "WHERE l.match_key = v.match_key AND l.channel = v.channel AND l.artifact = v.artifact "
                    "AND l.content_sha256 = v.content_sha256",
                    [value for row in signals for value in row],
                )
            if listed:
                rows = ", ".join("(?, ?, ?)" for _ in listed)
                conn.execute(
                    f"UPDATE lake.main.{LINEAGE_TABLE} AS l SET last_listed = v.listed_at FROM (VALUES {rows}) "  # noqa: S608 - constant name, placeholders
                    "AS v(match_key, channel, listed_at) WHERE l.match_key = v.match_key AND l.channel = v.channel",
                    [value for row in listed for value in row],
                )
            upsert_sources(conn, sources)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def _source(self, key: int, context: ModuleContext, listed: SourceEntry, at: datetime) -> MatchSource:
        return MatchSource(
            match_key=key,
            module_id=context.module.module_id,
            channel=context.adapter.channel,
            channel_id=listed.channel_match_id or listed.native_match_id,
            last_listed=at,
        )

    def choose(
        self, key: int, channel: Channel, listed: SourceEntry, parsed: ParsedMatch, lineage: Sequence[LineageRow]
    ) -> Choice:
        """The kept copy (§9): the pining copy only when it has fields with values that the public copy lacks."""
        this = CopyInfo(channel, listed.visibility, content_version_of(parsed), nonnull_fields(parsed))
        other_channel: Channel = "pining" if channel == "public" else "public"
        others = [r for r in lineage if r.channel == other_channel]
        if not others:
            return choose_copy(this if channel == "public" else None, this if channel == "pining" else None)
        fields = (
            {
                str(row[0]): bool(row[1])
                for row in self.cat.conn.execute(
                    "SELECT field, nonnull FROM lake.main.ops_copy_fields WHERE match_key = ? AND channel = ?",
                    [key, other_channel],
                ).fetchall()
            }
            if self._has_copy_fields()
            else {}
        )
        visibility = max(others, key=lambda r: r.first_seen).visibility
        other = CopyInfo(other_channel, visibility, others[-1].content_sha256, fields)
        return choose_copy(this if channel == "public" else other, this if channel == "pining" else other)

    def _has_copy_fields(self) -> bool:
        found = self.cat.conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'ops_copy_fields'"
        ).fetchone()
        return bool(found and found[0])

    def _copy_fields(self, key: int, channel: Channel, parsed: ParsedMatch) -> None:
        self.cat.conn.execute(
            "CREATE TABLE IF NOT EXISTS lake.main.ops_copy_fields "
            "(match_key BIGINT NOT NULL, channel VARCHAR NOT NULL, field VARCHAR NOT NULL, nonnull BOOLEAN NOT NULL)"
        )
        self.cat.conn.execute(
            "DELETE FROM lake.main.ops_copy_fields WHERE match_key = ? AND channel = ?", [key, channel]
        )
        rows = [[key, channel, name, value] for name, value in sorted(nonnull_fields(parsed).items())]
        # one statement, so one file per write on the encrypted-local path, not one per field (T3 spec D6)
        insert_rows(self.cat.conn, "ops_copy_fields", ("match_key", "channel", "field", "nonnull"), rows)

    def _lineage_rows(
        self,
        key: int,
        provider: Provider,
        channel: Channel,
        listed: SourceEntry,
        deliveries: Mapping[str, Delivery],
        *,
        kept: bool,
        reason: str,
    ) -> list[LineageRow]:
        at = self.now()
        return [
            LineageRow(
                match_key=key,
                provider=provider,
                channel=channel,
                channel_id=listed.channel_match_id or listed.native_match_id,
                artifact=artifact,
                change_signal=delivery.change_signal,
                delivery_sha256=delivery.delivery_sha256,
                content_sha256=delivery.content_sha256,
                wrapper=delivery.wrapper,
                first_seen=at,
                last_listed=at,
                visibility=delivery.visibility,
                kept=kept,
                reason=reason,
            )
            for artifact, delivery in sorted(deliveries.items())
        ]

    def record_not_kept(
        self,
        key: int,
        context: ModuleContext,
        listed: SourceEntry,
        deliveries: Mapping[str, Delivery],
        parsed: ParsedMatch,
        choice: Choice,
    ) -> None:
        provider, channel = context.adapter.provider, context.adapter.channel
        write_lineage(
            self.cat.conn,
            self._lineage_rows(key, provider, channel, listed, deliveries, kept=False, reason=choice.reason),
        )
        self._copy_fields(key, channel, parsed)
        self.listed_again(key, context, listed)
        self.checkpoints.maybe("not_kept")  # deferrable: the policy publishes by time or pending units (T3 spec D1)

    def write(
        self,
        key: int,
        context: ModuleContext,
        listed: SourceEntry,
        deliveries: Mapping[str, Delivery],
        parsed: ParsedMatch,
        choice: Choice,
        previous: Mapping[str, Any] | None,
    ) -> None:
        provider, channel = context.adapter.provider, context.adapter.channel
        visibility = choice.visibility
        if previous is not None and previous["visibility"] == "public" and visibility == "private":
            self.files.move(key, "private", previous)  # §9: the old files move first, then the new content is written
        version = content_version(deliveries)
        out_dir = self.work_dir / "files" / str(key)
        tracking_hash = next((d.content_sha256 for a, d in sorted(deliveries.items()) if a in TRACKING_ARTIFACTS), None)
        delivered = frozenset(d.content_sha256 for d in deliveries.values())
        puts: list[Put] = []
        sources: dict[str, str | None] = {}
        for table, data in sorted(parsed.tables.items()):
            if data.num_rows == 0:
                continue  # a bronze file is never empty; the match's old rows of the table are deleted
            if table.endswith(("_tracking_players", "_tracking_frames")):
                puts.append(Put(table, data, is_tracking=True, schema=None, source_hash=None))
                sources[table] = tracking_hash
            else:
                hashes = (
                    {str(h) for h in data.column("_source_hash").to_pylist()}
                    if "_source_hash" in data.column_names
                    else set(delivered)
                )
                base = data.drop_columns([c for c in AUDIT_NAMES if c in data.column_names])
                one = next(iter(hashes)) if len(hashes) == 1 else None
                puts.append(Put(table, data, is_tracking=False, schema=base.schema, source_hash=one or delivered))
                sources[table] = one  # several deliveries: per-row _source_hash, no single table hash
        written: list[WrittenRow] = []
        with step_span(
            "lakehouse.run.write",
            {"lakehouse.match_key": key, "lakehouse.module_id": context.module.module_id, "lakehouse.layer": "bronze"},
            log=_log,
        ) as step:
            for put in puts:
                written.append(
                    self._bronze.write_table(
                        put,
                        match_key=key,
                        visibility=visibility,
                        content_version=version,
                        run_id=self.run_id,
                        out_dir=out_dir,
                        ingested_at=self.now(),
                    )
                )
            step.set("lakehouse.rows", sum(row.rows for row in written))
        old_tables = {BronzeTable(f.bronze_table) for f in self.files.files_of(key)}
        at = self.now()
        has_360 = self._has_360(key, context, listed, parsed)
        competitions = parsed.names.competitions.to_pylist()
        named = competitions[0] if len(competitions) == 1 else {}
        match = MatchRow(
            match_key=key,
            provider=provider,
            native_match_id=listed.native_match_id,
            visibility=visibility,
            kept_channel=channel,
            kept_reason=choice.reason,
            competition_id=text_or_none(named.get("competition_id", listed.competition_id)),
            competition_name=text_or_none(named.get("competition_name", listed.competition_name)),
            season_id=text_or_none(named.get("season_id", listed.season_id)),
            season_name=text_or_none(named.get("season_name", listed.season_name)),
            has_360_frames=has_360,
            run_id=self.run_id,
            updated_at=at,
            parser_version=context.parser.version,
        )
        files = [
            MatchFile(key, provider, w.table, w.root, w.path, w.rows, at, self.run_id, sources.get(w.table))
            for w in written
        ]
        lineage_rows = self._lineage_rows(key, provider, channel, listed, deliveries, kept=True, reason=choice.reason)
        if channel == "pining":
            for line in list_fields_mismatch(listed, parsed.names):
                self.errors(
                    kind=MISMATCH_KIND,
                    message=line,
                    severity="warning",
                    module_run_id=context.module_run_id,
                    module_id=context.module.module_id,
                    native=listed.native_match_id,
                )

        def also() -> None:
            write_lineage(self.cat.conn, [])  # creates the table on a new lake
            self.cat.conn.execute(f"UPDATE lake.main.{LINEAGE_TABLE} SET kept = false WHERE match_key = ?", [key])  # noqa: S608 - constant name
            write_lineage(self.cat.conn, lineage_rows)
            replace_match(self.cat.conn, match, [self._source(key, context, listed, at)], files)
            write_coordinate_systems(
                self.cat.conn, match_key=key, provider=provider, run_id=self.run_id, systems=parsed.coordinate_systems
            )
            self._copy_fields(key, channel, parsed)

        with step_span(
            "lakehouse.run.commit",
            {"lakehouse.match_key": key, "lakehouse.module_id": context.module.module_id, "lakehouse.layer": "bronze"},
            log=_log,
        ) as step:
            self._bronze.swap(key, written, old_tables, also=also)
            # deferrable: the policy runs the integrity guard before the publish that carries this write (T3 spec D1)
            record = self.checkpoints.maybe("match")
            if record is not None:
                step.set("lakehouse.catalog_seq", record.seq)
        if previous is not None and previous["visibility"] == "private" and visibility == "public":
            self.files.move(key, "public", {**match.as_row()})  # the remaining restricted files follow (§9); the
            # move re-applies visibility atomically (its own commit), keeping the invariant the guard above checked.
        shutil.rmtree(out_dir, ignore_errors=True)

    def _has_360(self, key: int, context: ModuleContext, listed: SourceEntry, parsed: ParsedMatch) -> bool | None:
        if context.adapter.provider != "statsbomb":
            return None
        table = next((t for name, t in parsed.tables.items() if name == THREE_SIXTY), None)
        frames = table is not None and table.num_rows > 0
        if listed.has_360 is not None and listed.has_360 != frames:
            self.errors(
                kind="has_360_mismatch",
                message=f"the listing says has_360={listed.has_360}, the bronze has {'' if frames else 'no '}360 rows",
                severity="warning",
                module_run_id=context.module_run_id,
                module_id=context.module.module_id,
                native=listed.native_match_id,
            )
        return frames
