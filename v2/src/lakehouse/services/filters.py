"""The filter editor's service (spec §8.1, §29.13; plan Task 25): the source preview, and what a draft filter would
change.

A preview lists the source's tournaments and never ingests: it writes nothing to the lake. A diff compares the
matches that the current filter and the draft select (the run's own selection rule: the clauses, then the StatsBomb
360-only option) with the matches the lake holds from the module. A narrowed filter never deletes data, so the diff
has no "removed" count: an ingested match that the draft leaves out stops updating, and an ingested match that the
current filter leaves out and the draft selects resumes updating.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

import duckdb

from lakehouse.config.model import Clause, ModuleConfig, ModuleId
from lakehouse.config.store import ConfigStore
from lakehouse.entries import SourceEntry
from lakehouse.runs.history import read_table
from lakehouse.services.catalog import read_catalog
from lakehouse.sources.base import ChannelAdapter, PreviewResult, selected_match_ids, tournament_selected
from lakehouse.storage.base import Storage

__all__ = ["AdapterFactory", "FilterDiff", "FilterService"]

AdapterFactory = Callable[[ModuleConfig], ChannelAdapter]


@dataclass(frozen=True)
class FilterDiff:
    added: int
    stop_updating: int
    resume_updating: int


def _selected(module: ModuleConfig, entries: Sequence[SourceEntry]) -> set[str]:
    wanted = selected_match_ids(module)
    chosen = {
        e.native_match_id
        for e in entries
        if tournament_selected(module, e.competition_id or "", e.season_id or "")
        and (wanted is None or e.native_match_id in wanted)
        and (not module.statsbomb_360_only or e.has_360 is True)
    }
    return chosen if module.filter else set()


class FilterService:
    def __init__(self, storage: Storage, adapters: AdapterFactory) -> None:
        self._storage = storage
        self._adapters = adapters

    def _module(self, module_id: ModuleId) -> ModuleConfig:
        settings, _ = ConfigStore(self._storage).load()
        found = next((m for m in settings.modules if m.module_id == module_id), None)
        if found is None:
            raise KeyError(f"no module {module_id!r}")
        return found

    def preview(self, module_id: ModuleId) -> PreviewResult:
        """The source's tournaments with match counts (a pining module without the list-only token says so)."""
        return self._adapters(self._module(module_id)).preview()

    def _listed(self, module: ModuleConfig) -> list[SourceEntry]:
        adapter: Any = self._adapters(module)
        if hasattr(adapter, "list_all_entries"):  # pining: one full listing, the clauses are applied here
            return list(cast("Callable[[], list[SourceEntry]]", adapter.list_all_entries)())
        return list(adapter.list_entries())

    def _ingested(self, module_id: ModuleId) -> set[str]:
        def read(conn: duckdb.DuckDBPyConnection) -> set[str]:
            keys = {row["match_key"] for row in read_table(conn, "ops_match_sources") if row["module_id"] == module_id}
            return {str(row["native_match_id"]) for row in read_table(conn, "ops_matches") if row["match_key"] in keys}

        return read_catalog(self._storage, read, empty=set[str]())

    def diff(self, module_id: ModuleId, draft: list[Clause], *, statsbomb_360_only: bool | None = None) -> FilterDiff:
        """What saving `draft` (and the 360-only option, when given) would change for the module."""
        current = self._module(module_id)
        proposed = current.model_copy(
            update={
                "filter": draft,
                "statsbomb_360_only": current.statsbomb_360_only if statsbomb_360_only is None else statsbomb_360_only,
            }
        )
        now = _selected(current, self._listed(current))
        then = _selected(proposed, self._listed(proposed))
        ingested = self._ingested(module_id)
        return FilterDiff(
            added=len(then - ingested),
            stop_updating=len((now & ingested) - then),
            resume_updating=len((ingested - now) & then),
        )
