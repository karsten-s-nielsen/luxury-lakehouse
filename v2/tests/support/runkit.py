"""Run test helpers: a fake channel adapter, a fake provider parser and saved settings (plan Task 21).

The fake parser writes one `<provider>_fake_events` table per match (the delivery's text, one row per line) and, for
StatsBomb with a `three_sixty` delivery, rows in `statsbomb_three_sixty`, so the run's steps can be tested without
the real parsers; `tests/e2e/test_run_local.py` runs the real ones.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.config.model import AllClause, CheckpointConfig, ModuleConfig, ModuleId, Settings, load_defaults
from lakehouse.config.store import ConfigStore
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import UnmappedFieldError
from lakehouse.ids.keys import match_key
from lakehouse.ids.native import Channel, Provider
from lakehouse.parsers.base import CoordinateSystem, NamesBundle, ParsedMatch
from lakehouse.parsers.columns import with_audit
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult
from lakehouse.storage.base import Storage

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
SYSTEM = CoordinateSystem(
    origin="centre",
    units="m",
    pitch_length=105.0,
    pitch_width=68.0,
    x_direction="x",
    y_direction="y",
    play_direction=None,
    source_ref="test",
)


def entry(
    module_id: ModuleId,
    native: str,
    *,
    signals: Mapping[str, str] | None = None,
    visibility: str = "public",
    has_360: bool | None = None,
    competition: tuple[str | None, str | None] = ("C1", "S1"),
    expected: Mapping[str, str] | None = None,
) -> SourceEntry:
    """A listed match with one artifact per signal (default `events` with signal `s1`)."""
    refs = {
        artifact: ArtifactRef(
            key=artifact, change_signal=signal, expected_sha256=(expected or {}).get(artifact), size=None
        )
        for artifact, signal in (signals or {"events": "s1"}).items()
    }
    return SourceEntry(
        module_id=module_id,
        native_match_id=native,
        competition_id=competition[0],
        competition_name=competition[0],
        season_id=competition[1],
        season_name=competition[1],
        visibility=visibility,  # type: ignore[arg-type]
        artifacts=refs,
        listed_at=NOW,
        has_360=has_360,
    )


class FakeAdapter:
    """A `ChannelAdapter` over in-memory entries and file bytes; it counts listings and fetches."""

    def __init__(
        self,
        module_id: ModuleId,
        provider: Provider,
        channel: Channel,
        entries: Sequence[SourceEntry],
        files: Mapping[tuple[str, str], bytes],
    ) -> None:
        self._module_id: ModuleId = module_id
        self._provider: Provider = provider
        self._channel: Channel = channel
        self.entries = list(entries)
        self.files = dict(files)
        self.warnings: list[str] = []
        self.fetched: list[tuple[str, str]] = []
        self.listed = 0
        self.full_listings: list[str] = []
        self.fail_listing: Exception | None = None
        self.on_list: Any = None
        self.on_fetch: Any = None  # called after each fetch is counted

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
        self.listed += 1
        if self.on_list is not None:
            self.on_list()
        if self.fail_listing is not None:
            raise self.fail_listing
        return iter(list(self.entries))

    def list_all_entries(self) -> list[SourceEntry]:
        """Every entry, whatever the module's clauses: the full visibility listing of a pining module."""
        self.full_listings.append("full")
        if self.fail_listing is not None:
            raise self.fail_listing
        return list(self.entries)

    def preview(self) -> PreviewResult:
        return PreviewResult(tournaments=[], fetched_at=NOW)

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        self.fetched.append((entry.native_match_id, artifact))
        if self.on_fetch is not None:
            self.on_fetch()
        return Delivery.received(
            provider=self._provider,
            channel=self._channel,
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=self.files[(entry.native_match_id, artifact)],
            visibility=entry.visibility,
            change_signal=entry.artifacts[artifact].change_signal,
            fetched_at=NOW,
        )


def fake_table(provider: str) -> BronzeTable:
    return bronze_table(f"{provider}_fake_events")


class FakeParser:
    """A `ProviderParser`: one row per line of each delivery in `<provider>_fake_events`; `bad` lines are an
    unmapped field; a StatsBomb `three_sixty` delivery gives `statsbomb_three_sixty` rows."""

    def __init__(self, provider: Provider, *, version: str = "1") -> None:
        self._provider: Provider = provider
        self.version_text = version
        self.parsed: list[str] = []  # the native ids parsed, in order

    @property
    def provider(self) -> Provider:
        return self._provider

    @property
    def version(self) -> str:
        return self.version_text

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]:
        return frozenset({"line"})

    def bronze_columns(self, artifact: str) -> frozenset[str]:
        return frozenset({"line"})

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:
        self.parsed.append(entry.native_match_id)
        key = match_key(self._provider, entry.native_match_id)
        tables: dict[BronzeTable, pa.Table] = {}
        for artifact, delivery in sorted(deliveries.items()):
            lines = delivery.decoded.decode().splitlines()
            if "bad" in lines:
                raise UnmappedFieldError(artifact, ["bad"])
            name = bronze_table("statsbomb_three_sixty") if artifact == "three_sixty" else fake_table(self._provider)
            table = pa.table(
                {
                    "match_key": pa.array([key] * len(lines), pa.int64()),
                    "artifact": pa.array([artifact] * len(lines), pa.string()),
                    "line": pa.array(lines, pa.string()),
                }
            )
            table = with_audit(table, run_id=run_id, source_hash=delivery.content_sha256, at=NOW)
            tables[name] = pa.concat_tables([tables[name], table]) if name in tables else table
        competitions = pa.table(
            {
                "competition_id": pa.array([entry.competition_id], pa.string()),
                "competition_name": pa.array([entry.competition_name], pa.string()),
                "season_id": pa.array([entry.season_id], pa.string()),
                "season_name": pa.array([entry.season_name], pa.string()),
            }
        )
        names = NamesBundle(players=pa.table({}), teams=pa.table({}), competitions=competitions)
        return ParsedMatch(tables=tables, coordinate_systems={a: SYSTEM for a in deliveries}, names=names)


def save_settings(
    storage: Storage,
    modules: Sequence[ModuleConfig],
    *,
    raw_archive: bool = False,
    checkpoint: CheckpointConfig | None = None,
) -> Settings:
    """The laptop defaults with only `modules` active (the others inactive), saved as the lake's config. `checkpoint`
    replaces the T3 publish cadence (for example `matches=1` for a test whose subject is not the cadence)."""
    defaults = load_defaults("laptop")
    chosen = {module.module_id: module for module in modules}
    all_modules = [chosen.get(m.module_id, replace_module(m, active=False)) for m in defaults.modules]
    update: dict[str, Any] = {"modules": all_modules, "raw_archive": raw_archive}
    if checkpoint is not None:
        update["checkpoint"] = checkpoint
    settings = defaults.model_copy(update=update)
    store = ConfigStore(storage)
    try:
        _, version = store.load()
    except FileNotFoundError:
        version = 0
    store.save(settings, expected_version=version, operator="test")
    return settings


def _no_errors(**_: Any) -> None:
    return None


def checkpoints_for(cat: Any, storage: Storage, *, now: Any = None, **config: int) -> Any:
    """A real T3 checkpoint policy for a test that builds the run collaborators directly. A barrier (`now`) always
    publishes, so a move still publishes at once; `config` sets the `CheckpointConfig` fields."""
    from lakehouse.runs.checkpoint import CheckpointPolicy

    return CheckpointPolicy(
        cat,
        CheckpointConfig(**config),
        retention=load_defaults("laptop").retention,
        run_id=cat.run_id,
        now=now if now is not None else (lambda: NOW),
        errors=_no_errors,
        storage=storage,
    )


def replace_module(module: ModuleConfig, **changes: Any) -> ModuleConfig:
    return module.model_copy(update=changes)


def active(module_id: ModuleId, *clauses: Any, statsbomb_360_only: bool = False) -> ModuleConfig:
    return ModuleConfig(
        module_id=module_id,
        active=True,
        filter=list(clauses) if clauses else [AllClause()],
        statsbomb_360_only=statsbomb_360_only,
    )


def with_entry(adapter: FakeAdapter, native: str, **changes: Any) -> None:
    adapter.entries = [replace(e, **changes) if e.native_match_id == native else e for e in adapter.entries]


class ListingAdapter(FakeAdapter):
    """A fake adapter with source-level listings (spec §10.5): `listing_kinds()` and `fetch_listing(kind)`."""

    def __init__(self, *args: Any, listings: Mapping[str, bytes], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.listings = dict(listings)
        self.listing_fetches: list[str] = []

    def listing_kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self.listings))

    def fetch_listing(self, kind: str) -> Delivery:
        self.listing_fetches.append(kind)
        body = self.listings[kind]
        private = b'"private"' in body
        return Delivery.received(
            provider=self.provider,
            channel=self.channel,
            native_match_id="",
            artifact=kind,
            raw=body,
            visibility="private" if private else "public",
            change_signal=None,
            fetched_at=NOW,
        )


class ListingParser(FakeParser):
    """A fake parser for listings: one row per JSON record `{"id", "visibility"}` in `<provider>_fake_<kind>`."""

    def listing_table(self, kind: str) -> BronzeTable:
        return bronze_table(f"{self.provider}_fake_{kind}")

    def listing_visibility(self, kind: str) -> str | None:
        return "visibility"

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table:
        import json

        records = json.loads(decoded)
        table = pa.table(
            {
                "id": pa.array([r["id"] for r in records], pa.string()),
                "visibility": pa.array([r.get("visibility", "public") for r in records], pa.string()),
            }
        )
        return with_audit(table, run_id=run_id, source_hash=source_hash, at=NOW)
