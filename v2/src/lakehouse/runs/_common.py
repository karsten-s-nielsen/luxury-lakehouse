"""Shared leaf types, constants and helpers for the run engine (M1 extraction, plan Task 1).

Both `run.py` and its extracted collaborators (`match_files`, `visibility`, `match_writer`) use these names.
They live here so no collaborator imports `run.py` — the import-linter layers contract forbids that back-edge.

`run.py` keeps its own module-level `_log = get_logger(__name__)` unchanged; the collaborators use
`_log = get_logger(RUN_LOGGER_NAME)`, which resolves to the same logger object by name, so every module logs
under `lakehouse.runs.run`.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final, Literal, Protocol

from lakehouse.bronze.schemas import AUDIT_COLUMNS
from lakehouse.config.model import ModuleConfig
from lakehouse.entries import SourceEntry
from lakehouse.errors import ConflictError, GuardStopError, LeaseLostError
from lakehouse.parsers.base import ParsedMatch, ProviderParser
from lakehouse.raw.archive import Delivery
from lakehouse.runs.history import MatchStep, Outcome
from lakehouse.sources.base import ChannelAdapter

RUN_LOGGER_NAME: Final = "lakehouse.runs.run"
FATAL: Final = (LeaseLostError, ConflictError, GuardStopError)
THREE_SIXTY: Final = "statsbomb_three_sixty"
TRACKING_ARTIFACTS: Final = frozenset({"tracking", "positions", "tracking_parquet"})
AUDIT_NAMES: Final = tuple(AUDIT_COLUMNS.names)


class FullListing(Protocol):
    """A pining adapter's listing of every entry, whatever the module's clauses (the visibility listing)."""

    def list_all_entries(self) -> list[SourceEntry]: ...


class ErrorSink(Protocol):
    """The run's error/warning recorder (`_Run._error`), injected into the collaborators (plan Task 1)."""

    def __call__(
        self,
        *,
        kind: str,
        message: str,
        severity: Literal["error", "warning"],
        module_run_id: str | None = None,
        module_id: str | None = None,
        native: str | None = None,
    ) -> None: ...


@dataclass
class Tally:
    added: int = 0
    changed: int = 0
    unchanged: int = 0
    failed: int = 0
    excluded: int = 0
    quarantined: int = 0

    def add(self, outcome: Outcome) -> None:
        if outcome in ("added", "changed", "unchanged", "failed", "excluded", "quarantined"):
            setattr(self, outcome, getattr(self, outcome) + 1)
        elif outcome == "moved":
            self.unchanged += 1


@dataclass
class ModuleContext:
    module: ModuleConfig
    adapter: ChannelAdapter
    parser: ProviderParser
    module_run_id: str
    tally: Tally = field(default_factory=Tally)
    steps: list[MatchStep] = field(default_factory=list[MatchStep])


def content_version(deliveries: Mapping[str, Delivery]) -> str:
    digest = hashlib.sha256("".join(d.content_sha256 for _, d in sorted(deliveries.items())).encode()).hexdigest()
    return digest[:16]


def content_version_of(parsed: ParsedMatch) -> str:
    digest = hashlib.sha256()
    for name, table in sorted(parsed.tables.items()):
        digest.update(name.encode())
        digest.update(str(table.num_rows).encode())
    return digest.hexdigest()


def nonnull_fields(parsed: ParsedMatch) -> dict[str, bool]:
    fields: dict[str, bool] = {}
    for table_name, table in parsed.tables.items():
        for name in table.column_names:
            if name not in AUDIT_NAMES:
                fields[f"{table_name}.{name}"] = table.column(name).null_count < table.num_rows
    return fields


def text_or_none(value: object) -> str | None:
    return None if value is None else str(value)
