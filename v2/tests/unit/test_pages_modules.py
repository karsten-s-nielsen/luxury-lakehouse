"""The Data sources page as a bound Taipy page (page-binding PB2; audit F7): the module table plus one Source selector
that drives both the active toggle and the folded-in what-to-download editor. `refresh` fills the table, seeds the
source and fills the editor and the active flag; `on_pick_source` reloads the selected source; `on_pick_tournaments`
(shared) builds the draft clauses. State is driven with a plain namespace, as Taipy drives the client state."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from lakehouse.config.model import CompetitionClause, ModuleId
from lakehouse.services.modules import ModuleStatus
from lakehouse.sources.base import PreviewResult, PreviewTournament
from lakehouse.ui.pages import modules
from lakehouse.ui.template import action_names


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


def _status(module_id: ModuleId, *, active: bool) -> ModuleStatus:
    return ModuleStatus(
        module_id=module_id,
        display_name=module_id.replace("_", " ").title(),
        active=active,
        filter_summary="all tournaments",
        match_count=3,
        last_run=None,
        token_status=None,
        warning=None,
    )


class _Modules:
    def list_modules(self) -> list[ModuleStatus]:
        return [_status("idsse_public", active=True), _status("statsbomb_public", active=False)]


class _Filters:
    def preview(self, module_id: str) -> PreviewResult:
        return PreviewResult(tournaments=[_TOURNAMENT], fetched_at=datetime(2026, 10, 28, tzinfo=UTC))


class _Settings:
    def load(self) -> tuple[Any, int]:
        return (SimpleNamespace(modules=[]), 1)


def _services() -> Any:
    return SimpleNamespace(modules=_Modules(), filters=_Filters(), settings=_Settings())


def test_modules_defaults() -> None:
    assert modules.draft_active is False and modules.module_rows == {}
    # the four actions the page declares (its Buttons), in first-seen order: Run now (under the table), the active
    # toggle's Apply, and the two folded-in editor actions (audit F7).
    assert action_names(modules.CONFIG) == ("on_run_now", "on_module_active", "on_preview_filter", "on_save_filter")
    # only the Source selector stays in the sidebar; the tournament picker and the 360 option sit beside their table
    assert [control.var for control in modules.CONFIG.sidebar_widgets] == ["filter_module"]


def test_modules_refresh_selects_nothing_and_prompts() -> None:
    """On navigate the page fills the table + the Source selector, selects NO source (the v1 pattern) and does not
    load the editor (audit D1). A preselected source was a trap: re-picking it fired no change, so its editor never
    loaded. The page shows the pick-a-source prompt, hides the source sections and guards Save."""
    state = SimpleNamespace()
    modules.refresh(state, _services())
    assert any("Idsse Public" in c for c in _cells(state.module_rows))  # the table (a display name, never a raw id)
    assert state.filter_module_ids == [("idsse_public", "Idsse Public"), ("statsbomb_public", "Statsbomb Public")]
    assert state.filter_module == ""  # nothing preselected
    assert state.editor_loaded is False and state.source_heading == "" and state.download_heading == ""
    assert state.source_prompt == modules.PICK_SOURCE_PROMPT
    assert state.filter_preview_rows == {"Tournament": [], "Matches": []}  # editor not loaded: empty, no preview call
    assert state.draft_stale is True and state.draft_active is False  # Save guarded; no source, so not active


def test_modules_on_pick_source_loads_editor() -> None:
    """Picking a source loads its editor on demand (audit D1): `on_pick_source` sets `editor_loaded`, so the preview,
    tournaments and saved draft load for the picked source, the sections are titled with its name, and the draft is
    no longer stale."""
    state = SimpleNamespace()
    modules.refresh(state, _services())
    modules.on_pick_source(state, _services(), "statsbomb_public")  # the shared (state, services, value) handler shape
    assert state.editor_loaded is True
    assert state.filter_module == "statsbomb_public"
    assert any("Comp One" in c for c in _cells(state.filter_preview_rows))  # the editor loaded the source preview
    assert state.source_heading == "Statsbomb Public"  # the section names the source, never "This source"
    assert state.download_heading == "What Statsbomb Public downloads" and state.source_prompt == ""
    assert state.draft_active is False  # the newly opened (off) source's active flag
    assert state.draft_stale is False  # a clean load (S13)


def test_modules_unknown_picked_source_falls_back_to_prompt() -> None:
    """A picked module that is no longer configured is never substituted by another: the page clears the pick and
    prompts again."""
    state = SimpleNamespace(filter_module="wyscout_public", editor_loaded=True)
    modules.refresh(state, _services())
    assert state.filter_module == "" and state.editor_loaded is False
    assert state.source_prompt == modules.PICK_SOURCE_PROMPT


def test_all_clause_seeds_every_tournament_with_a_note() -> None:
    """A saved "all" filter selects every listed tournament in the picker (an empty picker read as "nothing selected"
    while the module downloaded everything), and a note says that editing the picker replaces the live "all"."""
    from lakehouse.config.model import AllClause
    from lakehouse.ui.pages.filters import ALL_SELECTED_NOTE

    class _AllSettings:
        def load(self) -> tuple[Any, int]:
            module = SimpleNamespace(module_id="statsbomb_public", filter=[AllClause()], statsbomb_360_only=False)
            return (SimpleNamespace(modules=[module]), 1)

    services: Any = SimpleNamespace(modules=_Modules(), filters=_Filters(), settings=_AllSettings())
    state = SimpleNamespace()
    modules.on_pick_source(state, services, "statsbomb_public")
    assert state.filter_tournaments == ["c1:s1"]
    assert state.draft_clauses == [AllClause()]  # untouched, the saved live "all" is kept
    assert state.filter_notes.startswith(ALL_SELECTED_NOTE)


def test_modules_on_pick_tournaments_builds_clauses() -> None:
    state = SimpleNamespace()
    modules.on_pick_source(state, _services(), "statsbomb_public")  # open the source to load the tournament map
    modules.on_pick_tournaments(state, None, ["c1:s1"])  # re-exported from the shared filter_state
    assert state.draft_clauses == [CompetitionClause(competition_id="c1", seasons=["s1"])]


def test_unsaved_draft_survives_an_apply_refresh_and_reloads_after_save() -> None:
    """An Apply (or Run now) re-runs the page refresh. That refresh keeps an unsaved tournament edit of the picked
    source instead of re-seeding the draft from the saved filter (which silently discarded the edit). Once the draft is
    saved (it equals the saved filter), the refresh reloads the editor as before. Seen to fail: call filters.refresh
    unconditionally in modules.refresh and the edited draft snaps back to the saved [AllClause]."""
    from lakehouse.config.model import AllClause

    saved: list[Any] = [AllClause()]

    class _Saved:
        def load(self) -> tuple[Any, int]:
            module = SimpleNamespace(module_id="statsbomb_public", filter=list(saved), statsbomb_360_only=False)
            return (SimpleNamespace(modules=[module]), 1)

    class _CountingFilters(_Filters):
        calls = 0

        def preview(self, module_id: str) -> PreviewResult:
            _CountingFilters.calls += 1
            return super().preview(module_id)

    services: Any = SimpleNamespace(modules=_Modules(), filters=_CountingFilters(), settings=_Saved())
    state = SimpleNamespace()
    modules.on_pick_source(state, services, "statsbomb_public")
    modules.on_pick_tournaments(state, None, ["c1:s1"])  # an unsaved edit: one competition instead of "all"
    edited = [CompetitionClause(competition_id="c1", seasons=["s1"])]
    loads = _CountingFilters.calls

    modules.refresh(state, services)  # the refresh after an Apply
    assert state.draft_clauses == edited and state.filter_tournaments == ["c1:s1"]  # the edit survives
    assert _CountingFilters.calls == loads  # and the external listing is not re-read

    saved[:] = edited  # the operator saves the filter
    modules.refresh(state, services)  # the refresh after the Save
    assert _CountingFilters.calls == loads + 1 and state.draft_clauses == edited  # reloaded from the saved filter


def test_modules_page_builds() -> None:
    from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

    assert isinstance(modules.page, Markdown)
