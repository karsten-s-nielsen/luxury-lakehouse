"""The source adapter contract (spec §7, §8.4, §17; plan Task 14).

A channel adapter lists a module's matches with their change signals, gives the filter-editor preview, and fetches
one artifact as a `Delivery`. It never parses: a provider parser turns deliveries into bronze, whatever the channel.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable

from lakehouse.config.model import AllClause, MatchClause, ModuleConfig, ModuleId
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.ids.native import Channel, Provider
from lakehouse.raw.archive import Delivery

__all__ = [
    "ArtifactRef",
    "ChannelAdapter",
    "ListingSource",
    "PreviewResult",
    "PreviewTournament",
    "SourceEntry",
    "selected_match_ids",
    "tournament_selected",
]


@dataclass(frozen=True)
class PreviewTournament:
    competition_id: str
    competition_name: str
    season_id: str
    season_name: str
    match_count: int | None
    count_note: str | None
    match_count_360: int | None = None
    share_360_note: str | None = None
    competition_available_360: datetime | None = None

    def share_360_text(self) -> str | None:
        """`"<n> matches · <m> with 360 (<p> %)"`, or the note when a count is unknown; never a guessed number.
        `None` for a tournament without 360 information (a non-StatsBomb source)."""
        if self.match_count_360 is not None and self.match_count is not None:
            share = round(100 * self.match_count_360 / self.match_count) if self.match_count else 0
            return f"{self.match_count} matches · {self.match_count_360} with 360 ({share} %)"
        return self.share_360_note

    def available_360_text(self) -> str | None:
        """The competition-season's `match_available_360` (StatsBomb public module, §29.13)."""
        if self.competition_available_360 is not None:
            return f"360 data since {self.competition_available_360:%Y-%m-%d}"
        if self.match_count_360 is not None or self.share_360_note is not None:
            return "no 360 data for this season"
        return None


@dataclass(frozen=True)
class PreviewResult:
    tournaments: list[PreviewTournament]
    fetched_at: datetime
    notes: list[str] = field(default_factory=list[str])


class ChannelAdapter(Protocol):
    @property
    def module_id(self) -> ModuleId: ...

    @property
    def provider(self) -> Provider: ...

    @property
    def channel(self) -> Channel: ...

    def list_entries(self) -> Iterator[SourceEntry]: ...

    def preview(self) -> PreviewResult: ...

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery: ...


@runtime_checkable
class ListingSource(Protocol):
    """An adapter whose source has source-level listings (spec §10.5): a match, player or competition list."""

    def listing_kinds(self) -> tuple[str, ...]: ...

    def fetch_listing(self, kind: str) -> Delivery: ...


def tournament_selected(module: ModuleConfig, competition_id: str, season_id: str) -> bool:
    """Whether a clause of the module selects the competition-season. An "all" or match clause selects every one;
    the match clause filters by id afterwards (`selected_match_ids`)."""
    for clause in module.filter:
        if isinstance(clause, AllClause | MatchClause):
            return True
        if clause.competition_id == competition_id and (clause.seasons == "all" or season_id in clause.seasons):
            return True
    return False


def selected_match_ids(module: ModuleConfig) -> frozenset[str] | None:
    """The match ids that the module's match clauses name, or `None` when a competition or "all" clause applies."""
    if any(not isinstance(clause, MatchClause) for clause in module.filter):
        return None
    return frozenset(m for clause in module.filter if isinstance(clause, MatchClause) for m in clause.match_ids)
