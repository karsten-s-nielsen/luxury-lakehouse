"""The Data sources page (spec §6, §8, §19; audit F7): every module with its display name, its filter in plain words,
its match count and its last run, and — for the one selected source — the "what to download" editor (its tournaments
and a preview of what a filter change would add or stop). One Source selector drives both the active toggle and the
editor, so the operator picks the source once (audit F7 folded the former "What to download" page in here). A module
whose count is unknown shows "count not available", never a guessed number.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.services.modules import ModuleStatus
from lakehouse.ui.filter_state import on_pick_360, on_pick_tournaments
from lakehouse.ui.pages import filters
from lakehouse.ui.pages.filters import THREE_SIXTY_LABEL
from lakehouse.ui.template import (
    Button,
    Control,
    NavSection,
    PageConfig,
    Prose,
    Section,
    Table,
    Text,
    build_page,
    table_columns,
)

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = [
    "CONFIG",
    "COUNT_UNAVAILABLE",
    "ICON",
    "PICK_SOURCE_PROMPT",
    "ROUTE",
    "TITLE",
    "on_pick_360",
    "on_pick_source",
    "on_pick_tournaments",
    "page",
    "refresh",
    "status_line",
]

TITLE = "Data sources"
ICON = "database"
ROUTE = "modules"
COUNT_UNAVAILABLE = "count not available"
CONFIG = PageConfig(
    title=TITLE,
    icon=ICON,
    nav_section=NavSection("Operations"),
    description="Every data source, its filter in plain words, its match count and its last run.",
    body=(
        Prose("Each row is one provider through one channel. The count is the matches already in the lake."),
        # one row per configured source (a short, fixed list): a taller cap than the default keeps every source in view
        Table("module_rows", height="40rem"),
        Prose("Run now starts one run of every active source and returns at once; follow it on the Runs page."),
        Button("Run now", "on_run_now"),
        Text("source_prompt"),
        # The picked source's own settings, titled with its name (never a generic "This source"), hidden until a
        # source is picked so no control acts on an unselected source.
        Section(
            "Source",
            (
                Control(
                    "toggle",
                    "draft_active",
                    label="Active",
                    help="Whether a run (scheduled or Run now) includes this source. Apply saves the change.",
                ),
                Button("Apply", "on_module_active", primary=True),
            ),
            subtitle_var="source_heading",
            render_var="editor_loaded",
        ),
        # The what-to-download editor: the tournament picker sits beside its table and its Preview / Save buttons
        # (it was in the sidebar, apart from the table it edits), and the steps say how the editor is used.
        Section(
            "What to download",
            (
                Prose(
                    "Pick the tournaments to download. **Preview** shows what the change would add or stop; "
                    "**Save filter** keeps it. A match that stops updating keeps the data already downloaded."
                ),
                Control(
                    "selector_multi",
                    "filter_tournaments",
                    label="Tournaments",
                    help="The competitions and seasons to download for this source. Clearing all downloads none.",
                    lov="filter_tournament_lov",
                    on_change="on_pick_tournaments",
                    value_by_id=True,  # lov is (key, label); bind the tournament keys, not the tuples (V2UI-IMPL-23)
                ),
                Control(
                    "toggle",
                    "draft_360",
                    label=THREE_SIXTY_LABEL,
                    help="Limit StatsBomb to matches that carry 360 freeze-frame tracking.",
                    on_change="on_pick_360",
                    condition="draft_is_statsbomb",
                ),
                Table("filter_preview_rows"),
                Text("filter_notes"),
                Button("Preview", "on_preview_filter"),
                Button("Save filter", "on_save_filter", primary=True),
                Text("filter_diff"),
            ),
            subtitle_var="download_heading",
            render_var="editor_loaded",
        ),
    ),
    sidebar_widgets=(
        # One Source selector drives both the active toggle (on_module_active) and the editor (on_save_filter): its
        # on_change loads the picked module's tournaments, saved draft, 360 gate and active flag (audit F7). Bound to
        # the shared `filter_module` (the exact name the save action reads), so the operator never picks the source
        # twice. Nothing is preselected (filter_state), so the first pick of any source loads its editor.
        Control(
            "selector",
            "filter_module",
            label="Source",
            help="Pick a provider and channel to see its settings and edit what it downloads.",
            lov="filter_module_ids",
            on_change="on_pick_source",
            value_by_id=True,  # lov is (id, label); bind the module id, not the tuple (V2UI-IMPL-23)
        ),
    ),
)

# Bound client state (page-binding PB2): this page's own display variables. The source selection and the editor draft
# (`filter_module`, `draft_clauses`, `draft_360`, the tournament variables) are the SHARED sidebar state in
# lakehouse.ui.filter_state, placed in the default scope by the shell; this page's refresh fills them through `state`.
module_rows: dict[str, list[str]] = {}  # the native-table data as Taipy's column format {header: [cell, ...]}
module_active_map: dict[str, bool] = {}
draft_active: bool = False
filter_preview_rows: dict[str, list[str]] = {}  # the tournaments native table (2 or 3 columns by module)
filter_notes: str = ""  # the source preview notes (public-tier marker, unknown keys): never dropped (V2UI-11)
editor_loaded: bool = False  # the what-to-download editor loads on demand (opening a source), not on navigate (D1)
source_prompt: str = ""  # the pick-a-source prompt, shown while no source is picked
source_heading: str = ""  # the picked source's name, the title of its settings section
download_heading: str = ""  # "What <source> downloads", the title of the editor section

_TABLE_COLUMNS = ("Source", "State", "What is selected", "Matches in lake", "Last run")


def _status_row(module: ModuleStatus) -> dict[str, str]:
    """One module as a native-table row dict for the Taipy `table` control."""
    count = str(module.match_count) if module.match_count is not None else COUNT_UNAVAILABLE
    state = "active" if module.active else "off"
    last = module.last_run
    last_text = "never run" if last is None else f"{last.status} ({last.counts.added + last.counts.changed} written)"
    extras: list[str] = []
    if "360 data only" in module.filter_summary:
        extras.append("360 only")
    if module.token_status == "public-only":  # noqa: S105 - a token status, not a password
        extras.append("public entries only (no token)")
    if module.warning:
        extras.append(module.warning)
    note = f" · {' · '.join(extras)}" if extras else ""
    return {
        "Source": module.display_name,
        "State": state,
        "What is selected": module.filter_summary,
        "Matches in lake": count,
        "Last run": f"{last_text}{note}",
    }


def status_line(module: ModuleStatus) -> str:
    """One module as a plain row string: the display name and the filter in plain words, never a raw id (kept for
    the format tests and a later text export)."""
    row = _status_row(module)
    return (
        f"| {row['Source']} | {row['State']} | {row['What is selected']} | "
        f"{row['Matches in lake']} | {row['Last run']} |"
    )


PICK_SOURCE_PROMPT = "> Pick a source in the sidebar to see its settings and edit what it downloads."


def _prompt_editor(state: Any) -> None:
    """No source picked yet: the pick-a-source prompt, the source sections hidden, and empty editor state, with the
    draft marked stale so a Save is refused until a source is picked and its saved filter loads (S13 / audit D1)."""
    state.editor_loaded = False
    state.source_prompt = PICK_SOURCE_PROMPT
    state.source_heading = ""
    state.download_heading = ""
    state.filter_preview_rows = table_columns([], ("Tournament", "Matches"))
    state.filter_tournament_lov = []
    state.filter_tournaments = []
    state.filter_tournament_map = {}
    state.filter_notes = ""
    state.filter_diff = ""
    state.draft_clauses = []
    state.draft_360 = False
    state.draft_is_statsbomb = False
    state.draft_module = ""
    state.draft_stale = True


def refresh(state: Any, services: Services) -> None:
    """Fill the module table + the Source selector, and — once the operator has picked a source — that source's
    settings and its what-to-download editor (§4). Nothing is preselected (filter_state), and the editor loads on
    demand (audit ledger T8 / doc-audit D1): the source preview is an external listing that never warms the Space
    object cache, so loading it on first navigate overran the §19.5 page budget. `on_pick_source` sets
    `editor_loaded`; until then the page shows a pick-a-source prompt and the navigate stays a lake read."""
    modules = services.modules.list_modules()
    state.module_rows = table_columns([_status_row(module) for module in modules], _TABLE_COLUMNS)
    state.module_active_map = {module.module_id: module.active for module in modules}
    # The Source selector lov is a lake read (the module list), filled every refresh so the selector works at once.
    state.filter_module_ids = [(module.module_id, module.display_name) for module in modules]
    names = {module.module_id: module.display_name for module in modules}
    module_id = str(getattr(state, "filter_module", "") or "")
    if module_id not in names:  # nothing picked yet, or a picked module that is no longer configured
        state.filter_module = ""
        _prompt_editor(state)
    elif getattr(state, "editor_loaded", False):
        # An unsaved draft of this source survives the refresh that follows an Apply or a Run now; otherwise load the
        # tournaments, the preview and the saved draft for the picked source.
        if not filters.has_unsaved_draft(state, services, module_id):
            filters.refresh(state, services)
        state.source_prompt = ""
        state.source_heading = names[module_id]
        state.download_heading = f"What {names[module_id]} downloads"
    else:
        _prompt_editor(state)
    state.draft_active = bool(getattr(state, "module_active_map", {}).get(getattr(state, "filter_module", ""), False))


def on_pick_source(state: Any, services: Services, value: str) -> None:
    """Open the picked Source and load its editor: its tournaments, its saved draft, its 360 gate and its active flag.
    Opening a source is the on-demand editor load (D1), and it fixes the stale-clause save bug (V2UI-04): without this
    reload a save would write the old module's clauses into the new module. Dispatched by the Gui-level on_change."""
    state.editor_loaded = True
    state.filter_module = value
    refresh(state, services)


page = Markdown(build_page(CONFIG))
