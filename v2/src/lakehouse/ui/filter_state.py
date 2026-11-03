"""Shared sidebar and filter-draft state for the Ops pages (plan Task 28; the v1 single-scope pattern).

Every page's sidebar filter controls (the Data sources Source selector, the Runs Run selector) live in the SHARED left
sidebar, which is part of the root page. Taipy binds the root page's variables from the Gui's default module
(`lakehouse.ui.shell`), never from a sub-page module, so EVERY variable a sidebar control binds (its value and its
lov) MUST be declared here, in that default scope. A sidebar variable declared in its page module binds to nothing:
the Run selector showed no options and no value while the Runs page filled them (test_sidebar_vars_are_shared guards
this). `shell` imports every name from here, which places them in the default module; a page then resolves the same
names to this one shared copy (Taipy's default-module fallback), so the control and the page read and write ONE
variable.

The "What to download" draft (the tournaments, the 360 option, the draft clauses) also lives here: the Data sources
body binds it, and the `_collect` action contract reads `filter_module`, `draft_clauses` and `draft_360` from here
(the page-binding guard allows a shared-module declaration).
"""

from __future__ import annotations

from typing import Any

from lakehouse.config.model import Clause, CompetitionClause

__all__ = [
    "draft_360",
    "draft_clauses",
    "draft_is_statsbomb",
    "draft_module",
    "draft_stale",
    "filter_diff",
    "filter_module",
    "filter_module_ids",
    "filter_tournament_lov",
    "filter_tournament_map",
    "filter_tournaments",
    "on_pick_360",
    "on_pick_tournaments",
    "runs_run_lov",
    "runs_selected",
]

# Bound client state (page-binding PB3). `filter_module`/`draft_clauses`/`draft_360` are the exact names `_collect`
# reads for on_preview_filter/on_save_filter after the §5 rename.
# No source is selected until the operator picks one (the v1 pattern). A preselected source was a trap: its editor
# loads on the Source on_change, and re-picking the already-selected source fires no change, so its tournaments never
# loaded. With nothing selected, the first pick of ANY source fires the change and loads its editor.
filter_module: str = ""
draft_360: bool = False
# Gates the "360 freeze frames" toggle to the StatsBomb modules (spec §29.13); `filters.refresh` sets it.
draft_is_statsbomb: bool = False
draft_clauses: list[Clause] = []
filter_module_ids: list[str] = []
filter_tournament_lov: list[Any] = []
filter_tournament_map: dict[str, tuple[str, str]] = {}
filter_tournaments: list[str] = []
# The Save guard reads this (spec S13): a failed Source reload (the on_change dispatcher sets it) leaves the draft
# stale, so a save would write the OLD module's clauses into the NEW module (V2UI-04 on the error path). A clean reload
# clears it; `on_save_filter` refuses the save while it is set.
draft_stale: bool = False
# The result of the last Preview (what saving the draft would add or stop), shown beside the Preview button. Any draft
# edit clears it, so a preview never describes a draft the operator has since changed.
filter_diff: str = ""
# The module whose saved filter the draft was last loaded from (`filters.refresh` sets it). A page refresh keeps an
# unsaved draft of THIS module (an Apply or Run now must not discard the operator's edits); see `modules.refresh`.
draft_module: str = ""

# The Runs page sidebar Run selector (value_by_id over (run_id, label) tuples). Declared here, not in pages/runs.py,
# because the sidebar binds only the default scope (see the module docstring).
runs_selected: str = ""
runs_run_lov: list[tuple[str, str]] = []


def on_pick_tournaments(state: Any, services: Any, value: list[str]) -> None:
    """Build `draft_clauses` from the selected tournament keys (grouped into one CompetitionClause per competition).
    Uses the cached map, so it does not use `services` (the shared handler shape is `(state, services, value)` per
    spec C06). The draft changed, so a shown preview is stale and is cleared."""
    tournament_map: dict[str, tuple[str, str]] = getattr(state, "filter_tournament_map", {})
    by_competition: dict[str, list[str]] = {}
    for key in value:
        competition, season = tournament_map.get(key, ("", ""))
        if competition:
            by_competition.setdefault(competition, []).append(season)
    state.draft_clauses = [
        CompetitionClause(competition_id=competition, seasons=seasons)
        for competition, seasons in by_competition.items()
    ]
    state.filter_diff = ""


def on_pick_360(state: Any, services: Any, value: bool) -> None:
    """The 360-only option changed: the draft changed, so a shown preview is stale and is cleared (the bound
    `draft_360` already holds the new value; the shared handler shape is `(state, services, value)`)."""
    state.filter_diff = ""
