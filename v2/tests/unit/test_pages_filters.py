"""The what-to-download editor logic (page-binding PB3; audit F7): `filters.refresh` fills the preview and seeds the
draft from the selected module's saved filter; the shared filter state lives in `lakehouse.ui.filter_state`. The editor
now renders on the Data sources page (`modules`), whose wiring is tested in test_pages_modules."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from lakehouse.config.model import CompetitionClause
from lakehouse.sources.base import PreviewResult, PreviewTournament
from lakehouse.ui import filter_state
from lakehouse.ui.pages import filters


def _cells(columns: dict[str, list[str]]) -> list[str]:
    return [cell for column in columns.values() for cell in column]


_TOURNAMENT = PreviewTournament(
    competition_id="c1",
    competition_name="Comp One",
    season_id="s1",
    season_name="2024",
    match_count=5,
    count_note=None,
)
_MODULE = SimpleNamespace(
    module_id="statsbomb_public",
    filter=[CompetitionClause(competition_id="c1", seasons=["s1"])],
    statsbomb_360_only=True,
)


class _Filters:
    def preview(self, module_id: str) -> PreviewResult:
        return PreviewResult(tournaments=[_TOURNAMENT], fetched_at=datetime(2026, 10, 28, tzinfo=UTC))


class _Modules:
    def list_modules(self) -> list[Any]:
        return [
            SimpleNamespace(module_id="statsbomb_public", display_name="StatsBomb"),
            SimpleNamespace(module_id="idsse_public", display_name="IDSSE"),
        ]


class _Settings:
    def load(self) -> tuple[Any, int]:
        return (SimpleNamespace(modules=[_MODULE]), 1)


def _services() -> Any:
    return SimpleNamespace(filters=_Filters(), modules=_Modules(), settings=_Settings())


def test_filter_state_defaults() -> None:
    """No source is preselected (the v1 pattern): with nothing selected, the first pick of any source fires the change
    that loads its editor (a preselected source fired no change when re-picked)."""
    assert (
        filter_state.filter_module == ""
        and filter_state.draft_360 is False
        and filter_state.draft_is_statsbomb is False
        and filter_state.draft_clauses == []
        and filter_state.filter_diff == ""
    )


def test_draft_edits_clear_a_shown_preview() -> None:
    """A Preview describes one draft; any draft edit (tournaments or the 360 option) clears it, so a stale preview is
    never shown beside a changed draft."""
    state = SimpleNamespace(filter_module="statsbomb_public")
    filters.refresh(state, _services())
    state.filter_diff = "**If you save this filter:** no change"
    filter_state.on_pick_tournaments(state, None, ["c1:s1"])
    assert state.filter_diff == ""
    state.filter_diff = "**If you save this filter:** no change"
    filter_state.on_pick_360(state, None, True)
    assert state.filter_diff == ""


def test_filters_refresh_fills_and_seeds() -> None:
    state = SimpleNamespace(filter_module="statsbomb_public")
    filters.refresh(state, _services())
    assert any("Comp One" in cell for cell in _cells(state.filter_preview_rows))
    assert state.filter_module_ids == [("statsbomb_public", "StatsBomb"), ("idsse_public", "IDSSE")]
    assert state.draft_clauses == [CompetitionClause(competition_id="c1", seasons=["s1"])]  # seeded from the filter
    assert state.draft_360 is True
    assert "c1:s1" in state.filter_tournament_map
    assert state.draft_stale is False  # a clean reload leaves the draft usable (S13)


def test_on_pick_tournaments_builds_clauses() -> None:
    state = SimpleNamespace(filter_module="statsbomb_public")
    filters.refresh(state, _services())
    filter_state.on_pick_tournaments(state, None, ["c1:s1"])  # the shared (state, services, value) handler shape
    assert state.draft_clauses == [CompetitionClause(competition_id="c1", seasons=["s1"])]


def test_filter_notes_shows_preview_notes() -> None:
    """refresh surfaces PreviewResult.notes (public-tier marker, unknown keys) into filter_notes — never dropped
    (V2UI-11). Seen to fail: drop the filter_notes fill and the source notes vanish from the page."""

    class _NotesFilters:
        def preview(self, module_id: str) -> PreviewResult:
            return PreviewResult(
                tournaments=[_TOURNAMENT],
                fetched_at=datetime(2026, 10, 28, tzinfo=UTC),
                notes=["private tournaments need the list token"],
            )

    services: Any = SimpleNamespace(filters=_NotesFilters(), modules=_Modules(), settings=_Settings())
    state = SimpleNamespace(filter_module="statsbomb_public")
    filters.refresh(state, services)
    assert "private tournaments need the list token" in state.filter_notes


def test_360_gate_value() -> None:
    """refresh sets draft_is_statsbomb True for a StatsBomb module and False otherwise — the gate for the sidebar 360
    toggle (spec §29.13 / S07d). The surviving-mutant guard: a non-StatsBomb module must read False."""
    sb = SimpleNamespace(filter_module="statsbomb_public")
    filters.refresh(sb, _services())
    assert sb.draft_is_statsbomb is True
    ws = SimpleNamespace(filter_module="wyscout_public")
    filters.refresh(ws, _services())
    assert ws.draft_is_statsbomb is False
