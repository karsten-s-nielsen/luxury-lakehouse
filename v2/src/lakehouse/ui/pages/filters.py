"""The "What to download" editor logic (spec §8, §19, §29.13; audit F7): for one module, the source's tournaments
with match counts, and a preview of what a filter change would add or stop. The StatsBomb module also shows the "Only
matches with 360 freeze frames" option and each tournament's 360 share; no other module shows it.

Audit F7 folded this editor into the Data sources page (`modules.py`): one Source selection drives both the active
toggle and the what-to-download editor, so the operator does not pick the source twice. This module is therefore a
helper (no `page`/`CONFIG`): `modules.refresh` calls `refresh` here to fill the editor for the selected source, and
the shared sidebar filter state still lives in `lakehouse.ui.filter_state`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from lakehouse.config.model import STATSBOMB_MODULES, AllClause, Clause, CompetitionClause, ModuleId
from lakehouse.services.filters import FilterDiff
from lakehouse.ui.template import table_columns

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = ["ALL_SELECTED_NOTE", "THREE_SIXTY_LABEL", "diff_text", "has_unsaved_draft", "refresh"]

THREE_SIXTY_LABEL = "Only matches with 360 freeze frames"


def diff_text(diff: FilterDiff) -> str:
    """The plain-words effect of a filter change (spec §19.5). "no change" when nothing moves; "(their data stays)"
    whenever a match would stop updating, because its already-downloaded data is kept."""
    if diff.added == 0 and diff.stop_updating == 0 and diff.resume_updating == 0:
        return "no change"
    core = (
        f"+{diff.added} matches would be added · "
        f"{diff.stop_updating} would stop updating · "
        f"{diff.resume_updating} would resume updating"
    )
    return f"{core} (their data stays)" if diff.stop_updating > 0 else core


def _preview_cols(preview: Any, module_id: str) -> dict[str, list[str]]:
    """The tournaments as Taipy's native-table column format. The StatsBomb modules add a "360 share" column; no
    other module shows it (spec §29.13)."""
    is_statsbomb = module_id in STATSBOMB_MODULES
    headers = ("Tournament", "Matches", "360 share") if is_statsbomb else ("Tournament", "Matches")
    dicts: list[dict[str, str]] = []
    for tournament in preview.tournaments:
        name = f"{tournament.competition_name} {tournament.season_name}".strip()
        count = (
            str(tournament.match_count) if tournament.match_count is not None else (tournament.count_note or "unknown")
        )
        row = {"Tournament": name, "Matches": count}
        if is_statsbomb:
            row["360 share"] = tournament.share_360_text() or "—"
        dicts.append(row)
    return table_columns(dicts, headers)


ALL_SELECTED_NOTE = (
    "> Every tournament is selected, including any the source adds later. Changing the picker replaces that with "
    "exactly the tournaments you pick."
)


def _keys_from_clauses(clauses: list[Clause], tournament_map: dict[str, tuple[str, str]]) -> list[str]:
    """The tournament keys the current clauses select, for seeding the multiselect from the saved filter. An
    `AllClause` selects every listed tournament (an empty picker would read as "nothing selected" while the module
    downloads everything)."""
    if any(isinstance(clause, AllClause) for clause in clauses):
        return list(tournament_map)
    keys: list[str] = []
    for clause in clauses:
        if not isinstance(clause, CompetitionClause):
            continue
        seasons = (
            clause.seasons
            if clause.seasons != "all"
            else [season for (comp, season) in tournament_map.values() if comp == clause.competition_id]
        )
        keys.extend(
            f"{clause.competition_id}:{season}"
            for season in seasons
            if f"{clause.competition_id}:{season}" in tournament_map
        )
    return keys


def has_unsaved_draft(state: Any, services: Services, module_id: str) -> bool:
    """Whether the editor holds unsaved edits for `module_id`: the draft was loaded from this module and its clauses or
    its 360 option now differ from the saved filter. A page refresh after an Apply or a Run now keeps such a draft
    (reloading it would silently discard the operator's edits); after a Save the draft equals the saved filter, so the
    refresh reloads it."""
    if getattr(state, "draft_module", "") != module_id:
        return False
    settings, _ = services.settings.load()
    module = next((m for m in settings.modules if m.module_id == module_id), None)
    if module is None:
        return False
    draft = list(getattr(state, "draft_clauses", []))
    return draft != list(module.filter) or bool(getattr(state, "draft_360", False)) != module.statsbomb_360_only


def refresh(state: Any, services: Services) -> None:
    """Fill the what-to-download editor for the selected module and seed the draft from that module's current filter
    (§4). Keyed on the shared `filter_module`, which `modules.refresh` has checked is a configured, picked module (no
    source is ever substituted); reloads after a Source pick and after an action, both of which carry the services. A
    reload replaces the draft, so any shown preview is cleared."""
    module_id = cast("ModuleId", getattr(state, "filter_module", ""))
    state.draft_is_statsbomb = module_id in STATSBOMB_MODULES  # gates the 360 toggle (spec §29.13)
    state.filter_diff = ""
    # (id, label) tuples: the Source selector shows the display name, the bound value stays the module id.
    state.filter_module_ids = [(module.module_id, module.display_name) for module in services.modules.list_modules()]
    preview = services.filters.preview(module_id)
    lov: list[Any] = []
    tournament_map: dict[str, tuple[str, str]] = {}
    for tournament in preview.tournaments:
        key = f"{tournament.competition_id}:{tournament.season_id}"
        lov.append((key, f"{tournament.competition_name} {tournament.season_name}".strip()))
        tournament_map[key] = (tournament.competition_id, tournament.season_id)
    state.filter_tournament_lov = lov
    state.filter_tournament_map = tournament_map
    state.filter_preview_rows = _preview_cols(preview, module_id)
    settings, _ = services.settings.load()
    module = next((m for m in settings.modules if m.module_id == module_id), None)
    state.draft_clauses = list(module.filter) if module is not None else []
    notes = [f"> {note}" for note in preview.notes]
    if any(isinstance(clause, AllClause) for clause in state.draft_clauses):
        notes.insert(0, ALL_SELECTED_NOTE)
    state.filter_notes = "\n\n".join(notes)
    state.draft_360 = bool(getattr(module, "statsbomb_360_only", False)) if module is not None else False
    state.filter_tournaments = _keys_from_clauses(state.draft_clauses, tournament_map)
    state.draft_module = module_id  # the module this draft was loaded from (modules.refresh keeps its unsaved edits)
    state.draft_stale = False  # a clean reload: the draft matches the selected module (S13)
