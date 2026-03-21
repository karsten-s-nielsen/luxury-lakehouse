# Widget Dependencies Design

**Date:** 2026-03-21
**Branch:** `spike/taipy-proof-of-concept`
**Status:** Approved design, pending implementation

## Problem

Sidebar widgets in the Taipy spike have no dependency logic — all widgets are visible as soon as the page loads, showing empty dropdowns. In Streamlit, widgets progressively appear as their parent widget gets a value selected (e.g., Team is hidden until Competition is chosen). This is critical for cognitive load: users should only see what's actionable right now.

33 sidebar widgets exist across two sections (`_FILTER_WIDGETS` with 28 entries, `_SEARCH_WIDGETS` with 5 entries). Dependencies were identified by comparing Streamlit source (the reference implementation) against the current Taipy widget definitions. ~20 are missing entirely, 3 are hardcoded as raw condition strings, and 1 is partially implemented.

## Design

### `SidebarWidget` additions

Three new optional fields on the existing `SidebarWidget` dataclass:

```python
depends_on: str = ""              # parent widget's state variable name
depends_value: str = ""           # show only when parent equals this value
depends_lov_populated: bool = False  # show only when this widget's own LOV has entries
```

### Condition generation

`_build_sidebar_widget()` in `page_template.py` currently passes the `condition` field directly as the Taipy `render=` expression. With dependencies, it builds a compound expression by `and`-joining all present parts:

| Source | Expression generated | When used |
|--------|---------------------|-----------|
| `condition` | Used as-is | Always (page visibility) |
| `depends_on` (no `depends_value`) | `{depends_on} is not None` | Simple cascade — parent must have a value |
| `depends_on` + `depends_value` | `{depends_on} == "{depends_value}"` | View-conditional — parent must equal specific value |
| `depends_lov_populated` | `len({lov}) > 0` | Data-availability gate — widget's own LOV must be non-empty |

If only `condition` is set (no `depends_on`), behavior is unchanged from today.

### Label fix

The shared Player widget (`selected_player`, pages: Shot Map, Heat Map, Player Impact) is optional on all three pages — results display without selecting a player. Label changes from `"Player"` to `"Player (optional)"`.

### Widget dependency assignments

All 33 widgets across both sections, with their dependency configuration:

**Condition column legend**: "page tuple" = existing `condition=f"current_page in {_FOO_PAGES}"` unchanged. "section" = no `condition`, inherits from section. Specific values shown where the condition changes from current.

**Shared filters:**

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` |
|--------|----------|-------------|-----------------|------------------------|
| Competition | `selected_competition` | — | — | — |
| Team | `selected_team` | `selected_competition` | — | — |
| Match | `selected_match` | `selected_team` | — | — |
| Player (optional) | `selected_player` | `selected_competition` | — | — |
| Players (1-3) | `selected_players_multi` | `selected_competition` | — | — |
| Metrics | `pr_selected_metrics` | `selected_competition` | — | — |
| xG Model | `selected_xg_model` | — | — | — |
| Min passes | `min_passes` | — | — | — |
| Min minutes | `min_minutes` | `selected_competition` | — | — |
| Progressive toggle | `pm_show_progressive` | — | — | — |
| Line-breaking toggle | `pm_show_line_breaking` | — | — | — |

**Tracking filters:**

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` |
|--------|----------|-------------|-----------------|------------------------|
| Provider | `selected_provider` | — | — | — |
| Tracking Match | `selected_tracking_match` | `selected_provider` | — | — |
| View (sub-view) | `selected_sub_view` | — | — | — |

**Pass Timing:**

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` |
|--------|----------|-------------|-----------------|------------------------|
| PAUSA Match | `pt_selected_match` | — | — | — |
| Team (optional) | `pt_selected_team` | `pt_selected_match` | — | — |
| Player (optional) | `pt_selected_player` | `pt_selected_match` | — | — |

**Defensive Impact:**

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` |
|--------|----------|-------------|-----------------|------------------------|
| Competition (DEFCON) | `dv_selected_comp` | — | — | — |
| Team (optional) | `dv_selected_team` | `dv_selected_comp` | — | — |
| View | `dv_current_view` | `dv_selected_comp` | — | — |
| Player (Breakdown) | `dv_selected_breakdown_player` | `dv_current_view` | `"Breakdown"` | `True` |
| Player (Timeline) | `dv_selected_timeline_player` | `dv_current_view` | `"Timeline"` | `True` |
| Match (Timeline) | `dv_selected_timeline_match` | `dv_current_view` | `"Timeline"` | `True` |

**Pitch Control:**

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` |
|--------|----------|-------------|-----------------|------------------------|
| Half | `pc_half` | `selected_tracking_match` | — | — |
| Model | `pc_model` | `selected_tracking_match` | — | — |
| Velocity toggle | `pc_show_velocity` | `selected_tracking_match` | — | — |
| Time slider | `pc_elapsed_seconds` | *(hybrid — keeps existing condition with `pc_max_seconds > 1`)* | — | — |

**Player Similarity (Filters section):**

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` | Notes |
|--------|----------|-------------|-----------------|------------------------|-------|
| Compare with | `ps_selected_compare` | — | — | `True` | Keeps `condition='current_page == "Player-Similarity"'` for page scoping (Player-Similarity is not in `_FILTER_HEADER_PAGES`, so section condition won't cover it). `depends_lov_populated` replaces the `len(ps_results_data) > 0` check — `ps_compare_lov` and `ps_results_data` are always populated/cleared synchronously in `state/player_similarity.py` lines 546-547 / 332-334. |

**Player Similarity (Search section):**

These 5 widgets are in `_SEARCH_WIDGETS`, rendered in the Search sidebar section (visible only on Player-Similarity page via section condition `current_page in {_SIMILARITY_PAGES}`).

| Widget | Variable | `depends_on` | `depends_value` | `depends_lov_populated` | Notes |
|--------|----------|-------------|-----------------|------------------------|-------|
| Search by | `ps_search_mode` | — | — | — | Always visible (root widget for search) |
| Player | `ps_selected_player` | — | — | — | Always visible (populated dynamically by search mode) |
| Results | `ps_result_count` | — | — | — | Always visible |
| Filter by competition | `ps_filter_by_competition` | — | — | — | Toggle, always visible |
| Competition | `ps_selected_competition` | `ps_filter_by_competition` | — | — | Currently `condition="ps_filter_by_competition"`. Migrate to `depends_on="ps_filter_by_competition"` — template generates `ps_filter_by_competition is not None`. Since toggles are bool (never None), this evaluates to `True` when toggle is True and `True` when toggle is False (bool is not None). **This requires special handling**: for bool/toggle parents, `depends_on` should check truthiness, not `is not None`. Use `condition="ps_filter_by_competition"` (keep as-is) — the raw condition is the correct Taipy expression for a bool check. |

### Template changes

**`_build_render_condition()`** — new function in `page_template.py`:

```python
def _build_render_condition(w: SidebarWidget) -> str:
    """Build compound render condition from widget fields."""
    parts: list[str] = []
    if w.condition:
        parts.append(w.condition)
    if w.depends_on:
        if w.depends_value:
            parts.append(f'{w.depends_on} == "{w.depends_value}"')
        else:
            parts.append(f"{w.depends_on} is not None")
    if w.depends_lov_populated and w.lov:
        parts.append(f"len({w.lov}) > 0")
    return " and ".join(parts) if parts else ""
```

**Integration into `_build_sidebar_widget()`** — replace the current `w.condition` usage in the `render=` attribute:

```python
# Before:
if w.condition:
    parts.append(f"<|part|render={lb}{w.condition}{rb}|")
else:
    parts.append("<|part|")

# After:
render_cond = _build_render_condition(w)
if render_cond:
    parts.append(f"<|part|render={lb}{render_cond}{rb}|")
else:
    parts.append("<|part|")
```

The `{lb}`/`{rb}` brace wrappers are already handled by the existing code — `_build_render_condition` returns a plain expression string.

**`build_sidebar_section()` fix** — replace manual `SidebarWidget` construction (lines 166-179) with:

```python
from dataclasses import replace
effective_w = replace(w, condition=condition)
```

This preserves all fields including the new `depends_on`/`depends_value`/`depends_lov_populated`.

### Defensive Impact condition migration example

Before (hardcoded):
```python
SidebarWidget("dropdown", "dv_selected_breakdown_player", "Player", ...,
    condition='current_page == "Defensive-Impact" and dv_current_view == "Breakdown" and len(dv_breakdown_player_lov) > 0',
    lov="dv_breakdown_player_lov",
)
```

After (declarative):
```python
SidebarWidget("dropdown", "dv_selected_breakdown_player", "Player", ...,
    condition=f"current_page in {_DEFCON_PAGES}",
    depends_on="dv_current_view",
    depends_value="Breakdown",
    depends_lov_populated=True,
    lov="dv_breakdown_player_lov",
)
```

The template generates: `render={current_page in (...) and dv_current_view == "Breakdown" and len(dv_breakdown_player_lov) > 0}`

### What does NOT change

- **`condition` field**: Stays for page visibility. Raw condition strings are only used for the Pitch Control time slider edge case (documented as deferred design decision #3 in `memory/project_taipy_deferred_design.md`).
- **Page visibility tuples** (`_COMP_PAGES`, `_TEAM_PAGES`, etc.): Stay as-is (deferred design decision #2).
- **CSS**: No styling changes. Hidden widgets collapse via Taipy's `render=false` — the `ll-sidebar-section` flex gap adjusts automatically (verified during widget spacing work).
- **State callbacks**: No changes. `on_competition_change`, `on_team_change`, etc. continue to populate child LOVs. The dependency system only controls visibility, not data flow.
- **`build_sidebar_section()`**: One required fix — the condition-inheritance code (lines 166-179) manually constructs a new `SidebarWidget` by copying fields. This will silently drop the new `depends_on`/`depends_value`/`depends_lov_populated` fields. Fix: replace manual construction with `dataclasses.replace(w, condition=condition)` which automatically preserves all fields including new ones.

### Edge cases

**Pitch Control time slider**: Uses hybrid `condition` containing both page visibility and `pc_max_seconds > 1`. This is the one widget where `depends_on` can't express the dependency (numeric threshold, not `is not None`). Documented as deferred design decision #3 — if a second widget needs this pattern, add `depends_expression` field.

**Player Similarity "Compare with"**: Depends on search results existing (`len(ps_results_data) > 0`) but this is expressed via `depends_lov_populated=True` since its LOV (`ps_compare_lov`) is populated from results. No raw condition string needed.

### Future: clean path to full spec-driven visibility

When deferred design decision #2 is implemented:
1. `condition` field replaced by `pages: list[str]` (or derived from `PageConfig`)
2. Template generates `current_page in (...)` from the list
3. `depends_on` / `depends_value` / `depends_lov_populated` stay unchanged
4. No widget definitions change except swapping `condition=` for `pages=`

## Files Changed

| File | Change |
|------|--------|
| `page_template.py` | Add 3 fields to `SidebarWidget`. Add `_build_render_condition()`. Update `_build_sidebar_widget()` to use it. Fix `build_sidebar_section()` to use `dataclasses.replace()` instead of manual construction. |
| `template.py` | Update all 33 `SidebarWidget(...)` definitions across `_FILTER_WIDGETS` and `_SEARCH_WIDGETS`: add `depends_on`/`depends_value`/`depends_lov_populated` where applicable. Remove hardcoded dependency logic from `condition` strings (3 Defensive Impact widgets). Fix Player label to "Player (optional)". Search section Competition widget keeps `condition="ps_filter_by_competition"` (bool check, not migrated to `depends_on`). |

## Acceptance Criteria

All verification via Puppeteer against Chrome on localhost.

1. Team dropdown hidden until Competition selected (verify on Shot Map).
2. Match dropdown hidden until Team selected (verify on Pass Map).
3. Player (optional) dropdown hidden until Competition selected (verify on Shot Map).
4. Pitch Control Half/Model/Velocity hidden until Tracking Match selected.
5. Defensive Impact Breakdown Player hidden until View = "Breakdown" AND LOV populated.
6. Pass Timing Team/Player hidden until PAUSA Match selected.
7. Player Similarity "Compare with" hidden until results LOV populated.
8. Player Similarity Competition filter hidden until "Filter by competition" toggle is on.
9. Pitch Control Time slider still works with hybrid condition (`pc_max_seconds > 1`).
10. No hardcoded dependency logic in any `condition` string (except PC Time slider and PS Competition bool check).
11. Player widget label shows "Player (optional)" on Shot Map, Heat Map, Player Impact.
12. Widget spacing test page — hidden widgets collapse correctly (flex gap adjusts).
13. `build_sidebar_section()` uses `dataclasses.replace()` — verified by code inspection.
