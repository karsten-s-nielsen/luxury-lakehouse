"""The visibility pass of the run engine (M1 extraction, plan Task 3).

`VisibilityMover.run()` lists every pining module that holds ingested matches, in full, and moves each match
whose kept copy's visibility flag changed (spec §9, §29.14). It returns the full listings (which the active
modules reuse) and whether the run failed: a failed full listing writes a warning through the injected error
sink and marks the run failed, which the `_Run.visibility_pass` delegator ORs into the run's `failed_run`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import ModuleId, Settings
from lakehouse.entries import SourceEntry
from lakehouse.ids.keys import match_key
from lakehouse.ids.native import Provider
from lakehouse.lineage.model import read_lineage
from lakehouse.logging import get_logger
from lakehouse.runs._common import FATAL, RUN_LOGGER_NAME, ErrorSink, FullListing
from lakehouse.runs.history import error_message
from lakehouse.runs.match_files import MatchFiles
from lakehouse.sources.base import ChannelAdapter
from lakehouse.telemetry import step_span

_log = get_logger(RUN_LOGGER_NAME)


class VisibilityMover:
    """List pining holders in full and move each match whose kept-copy flag changed (spec §9, §29.14)."""

    def __init__(
        self,
        cat: WorkingCatalog,
        settings: Settings,
        adapters: Mapping[ModuleId, ChannelAdapter],
        files: MatchFiles,
        errors: ErrorSink,
        *,
        confirm_lease: Callable[[], None],
    ) -> None:
        self.cat = cat
        self.settings = settings
        self.adapters = adapters
        self.files = files
        self.errors = errors
        self.confirm_lease = confirm_lease  # before each move: a writer that lost its lease stops (T3 spec D5)
        self._public: list[tuple[int, Mapping[str, Any]]] = []  # private-to-public moves waiting for their batch

    def run(self) -> tuple[dict[ModuleId, list[SourceEntry]], bool]:
        lineage = read_lineage(self.cat.conn)
        holders = {row.provider for row in lineage if row.channel == "pining"}
        listings: dict[ModuleId, list[SourceEntry]] = {}
        failed = False
        for module in self.settings.modules:
            adapter = self.adapters.get(module.module_id)
            if not module.module_id.endswith("_pining"):
                continue
            provider = module.module_id.removesuffix("_pining")
            if provider not in holders and not module.active:
                continue
            if provider not in holders:
                continue  # an active module with no ingested match: its listing is the normal one
            try:
                with step_span(
                    "lakehouse.run.visibility_check",
                    {"lakehouse.module_id": module.module_id, "lakehouse.layer": "bronze"},
                    log=_log,
                ) as step:
                    if adapter is None:
                        raise LookupError(f"no adapter for {module.module_id}")
                    entries = cast("FullListing", adapter).list_all_entries()
                    step.set("lakehouse.rows", len(entries))
            except FATAL:
                raise
            except Exception as exc:  # noqa: BLE001 - a failed visibility listing is a visible warning row and fails the run (plan Task 21)
                self.errors(
                    kind="visibility_check_failed",
                    # a pining listing can quote private content: the class only, as the sibling handlers (audit T5)
                    message=f"{module.module_id}: {error_message(exc, visibility='private')}",
                    severity="warning",
                    module_id=module.module_id,
                )
                failed = True
                continue
            listings[module.module_id] = entries
            self._apply_flags(module.module_id, entries)
        self._flush_public()  # the end of the visibility pass closes the last private-to-public batch (T3 spec D3)
        return listings, failed

    def _apply_flags(self, module_id: ModuleId, entries: Sequence[SourceEntry]) -> None:
        ops = self.files.ops_matches()
        lineage = read_lineage(self.cat.conn)
        for listed in entries:
            provider = cast("Provider", module_id.removesuffix("_pining"))
            key = match_key(provider, listed.native_match_id)
            row = ops.get(key)
            if row is None:
                continue
            ours = [r for r in lineage if r.match_key == key and r.channel == "pining"]
            if not ours:
                continue
            if row["kept_channel"] == "pining" and row["visibility"] != listed.visibility:
                self.confirm_lease()
                if listed.visibility == "public":  # private to public: batched, so it waits only for the batch
                    self._public.append((key, row))
                    if len(self._public) >= self.settings.checkpoint.move_batch:
                        self._flush_public()
                else:  # the privacy direction is never batched: its own publish, at once (decision 10)
                    self.files.move(key, listed.visibility, row)

    def _flush_public(self) -> None:
        pending, self._public = self._public, []
        self.files.move_public_batch(pending)
