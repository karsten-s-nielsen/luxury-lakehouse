# UI-2: Filter Cascade Decouple + Match Optionality + Slider Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three filter/widget UX issues: preserve match selection on team change, remove dead-end "All" from match-required pages, and harden all 9 sliders against the Taipy snap-back bug.

**Architecture:** All changes are in the Taipy app layer (`hf_taipy_app/`). Callback logic in `state/shared.py` gets page-aware match preservation + a new `match_lov_required` LOV variable. Widget definitions in `template.py` get LOV swap, tab gating, and `change_delay`. Three page-specific state modules get the slider state-preservation pattern.

**Tech Stack:** Python (Taipy state callbacks), Taipy Markdown templates

**Spec:** `docs/superpowers/specs/2026-05-14-ui2-filter-cascade-and-slider-hardening-design.md`

---

## File Map

| File | Role | Changes |
|------|------|---------|
| `hf_taipy_app/src/state/shared.py` | Shared filter state + cascade callbacks | Add `match_lov_required` state var + `_MATCH_REQUIRED_PAGE_NAMES` constant; rewrite `on_team_change` match reset; populate `match_lov_required` in both cascade callbacks; harden `on_min_passes_change` + `on_min_minutes_change` |
| `hf_taipy_app/src/template.py` | Widget definitions | Swap match-required widget LOV; add GK slider tab gating; add `change_delay=300` to 7 slider widgets |
| `hf_taipy_app/src/test_render.py` | Taipy app entry point (state var declarations for binding) | Add `match_lov_required = []` |
| `hf_taipy_app/src/state/pass_timing.py` | Pass Timing page state | Harden 3 slider callbacks |
| `hf_taipy_app/src/state/player_similarity.py` | Player Similarity page state | Harden 1 slider callback |
| `src/tests/test_taipy_lov_sync.py` | Drift guard | Assert `_MATCH_REQUIRED_PAGE_NAMES` == `set(_MATCH_REQUIRED_PAGES)` |
| `pyproject.toml` | Pytest config | Add `hf_taipy_app/src` to `testpaths` |
| `hf_taipy_app/src/conftest.py` | Pytest collection config | `collect_ignore` for `test_render.py` (app entry point, not a test) |
| `docs/ui-cycles/ui-consistency-roadmap.md` | Audit tracker | Close findings #10, #31 |
| `TODO.md` | Action items | Mark UI-2 complete |

---

### Task 1: Add `match_lov_required` state variable, constant, and binding declaration

**Files:**
- Modify: `hf_taipy_app/src/state/shared.py:44` (add state var), `:78-125` (add to `__all__`)
- Modify: `hf_taipy_app/src/test_render.py:11` (add binding declaration)

- [ ] **Step 1: Add state variable and constant to shared.py**

In `hf_taipy_app/src/state/shared.py`, add `match_lov_required` after `match_lov` (line 44):

```python
match_lov: list[str] = []
match_lov_required: list[str] = []  # Same as match_lov but without "All" (for match-required pages)
```

Add the page-names constant after the internal lookup maps comment block (after line 127, before the maps):

```python
# Page names where match selection is required (mirrors template.py _MATCH_REQUIRED_PAGES).
# Used by on_team_change for page-aware fallback when the selected match drops
# out of the narrowed LOV.  Defined here to avoid circular import from template.
_MATCH_REQUIRED_PAGE_NAMES = frozenset(("Pass-Map", "Pass-Network", "Match-Summary"))
```

Add `"match_lov_required"` to `__all__` (after `"match_lov"` on line 89):

```python
    "match_lov",
    "match_lov_required",
```

- [ ] **Step 2: Add binding declaration to test_render.py**

In `hf_taipy_app/src/test_render.py`, after line 11 (`match_lov = []`), add:

```python
match_lov_required = []
```

This is required because `test_render.py` is the Taipy app entry point (not a pytest test). It declares all state variables at module level for Taipy's binding system. The template references `{match_lov_required}` in the match-required widget (Task 3), so the binding must exist at startup.

- [ ] **Step 3: Verify import**

Run: `cd hf_taipy_app/src && python -c "from state.shared import match_lov_required; print('OK')"`
Expected: `OK`

---

### Task 2: Rewrite `on_team_change` match reset + populate `match_lov_required` in both callbacks

**Files:**
- Modify: `hf_taipy_app/src/state/shared.py:392-498`

- [ ] **Step 1: Update `on_competition_change` to populate `match_lov_required`**

In `on_competition_change()`, after line 434 (`state.match_lov = ...`), add:

```python
        state.match_lov_required = [label for label, _mk, _mid in matches]
```

The full block (lines 432-435) becomes:

```python
        matches = fetch_matches(comp_key, None)
        _match_map = {label: (mk, mid) for label, mk, mid in matches}
        state.match_lov = [_ALL_LABEL] + [label for label, _mk, _mid in matches]
        state.match_lov_required = [label for label, _mk, _mid in matches]
```

- [ ] **Step 2: Rewrite `on_team_change` match handling**

Replace lines 467-480 in `on_team_change()`:

Old code:
```python
    state.selected_match = _ALL_LABEL
    state.selected_player = _ALL_LABEL
    state.selected_players_multi = []
    # Reset the player-search input — team narrows scope, so any prior typed
    # query may now match a different set; clearing keeps semantics predictable.
    state.player_search_query = ""
    # PR 5b: clear cached player identities; team-narrowed scope changes
    # which player labels are valid resolutions.
    _player_identity_map = {}

    try:
        matches = fetch_matches(comp_key, team_id)
        _match_map = {label: (mk, mid) for label, mk, mid in matches}
        state.match_lov = [_ALL_LABEL] + [label for label, _mk, _mid in matches]
```

New code:
```python
    state.selected_player = _ALL_LABEL
    state.selected_players_multi = []
    # Reset the player-search input — team narrows scope, so any prior typed
    # query may now match a different set; clearing keeps semantics predictable.
    state.player_search_query = ""
    # PR 5b: clear cached player identities; team-narrowed scope changes
    # which player labels are valid resolutions.
    _player_identity_map = {}

    try:
        matches = fetch_matches(comp_key, team_id)
        _match_map = {label: (mk, mid) for label, mk, mid in matches}
        new_labels = [_ALL_LABEL] + [label for label, _mk, _mid in matches]
        required_labels = [label for label, _mk, _mid in matches]
        state.match_lov = new_labels
        state.match_lov_required = required_labels

        # Preserve match selection if still valid; page-aware fallback otherwise.
        # On match-required pages "All" is not in the LOV, so fall back to the
        # first available match.  On optional pages fall back to "All".
        if state.selected_match not in new_labels:
            if state.current_page in _MATCH_REQUIRED_PAGE_NAMES and required_labels:
                state.selected_match = required_labels[0]
            else:
                state.selected_match = _ALL_LABEL
```

Key change: `state.selected_match = _ALL_LABEL` is removed from the unconditional reset block and replaced with the conditional logic after `match_lov` is rebuilt.

- [ ] **Step 3: Verify no syntax errors**

Run: `cd hf_taipy_app/src && python -c "from state.shared import on_team_change, on_competition_change; print('OK')"`
Expected: `OK`

---

### Task 3: Swap match-required widget LOV in `template.py`

**Files:**
- Modify: `hf_taipy_app/src/template.py:499-509`

- [ ] **Step 1: Change LOV on the match-required widget**

In `template.py`, line 505, change:
```python
        lov="match_lov",
```
to:
```python
        lov="match_lov_required",
```

The match-optional widget at line 516 keeps `lov="match_lov"` — no change.

- [ ] **Step 2: Verify template renders**

Run: `cd hf_taipy_app/src && python -c "from template import SIDEBAR_MD; print('match_lov_required' in SIDEBAR_MD, 'match_lov' in SIDEBAR_MD)"`
Expected: `True True` (both LOV names appear — required for the required widget, original for the optional widget)

---

### Task 4: GK slider tab gating

**Files:**
- Modify: `hf_taipy_app/src/template.py:622-634`

- [ ] **Step 1: Add sub-view condition to GK min_minutes slider**

In `template.py`, line 627, change:
```python
        condition=f"current_page in {_GK_PAGES}",
```
to:
```python
        condition=f"current_page in {_GK_PAGES} and selected_sub_view == 'Rankings'",
```

- [ ] **Step 2: Verify no syntax error in template**

Run: `cd hf_taipy_app/src && python -c "from template import SIDEBAR_MD; print('Rankings' in SIDEBAR_MD)"`
Expected: `True`

---

### Task 5: Slider state-preservation — shared callbacks

**Files:**
- Modify: `hf_taipy_app/src/state/shared.py:574-581`

- [ ] **Step 1: Harden `on_min_passes_change`**

Replace lines 574-576:
```python
def on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min passes slider changed — refresh current page."""
    _refresh_current_page(state)
```

With:
```python
def on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min passes slider changed — refresh current page."""
    # Explicitly set so Taipy marks it as callback-changed and includes it
    # in the state push.  Without this, the slider snaps back during render.
    state.min_passes = int(var_value)
    _refresh_current_page(state)
```

- [ ] **Step 2: Harden `on_min_minutes_change`**

Replace lines 579-581:
```python
def on_min_minutes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min minutes slider changed — refresh current page."""
    _refresh_current_page(state)
```

With:
```python
def on_min_minutes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min minutes slider changed — refresh current page."""
    state.min_minutes = int(var_value)
    _refresh_current_page(state)
```

---

### Task 6: Slider state-preservation — Pass Timing callbacks

**Files:**
- Modify: `hf_taipy_app/src/state/pass_timing.py:315-327`

- [ ] **Step 1: Harden all 3 Pass Timing slider callbacks**

Replace lines 315-327:
```python
def pt_on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter aggregate rankings when slider changes."""
    _refresh_data(state)


def pt_on_min_minutes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter aggregate rankings when minutes slider changes."""
    _refresh_data(state)


def pt_on_per_match_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter per-match rankings when slider changes."""
    _refresh_data(state)
```

With:
```python
def pt_on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter aggregate rankings when slider changes."""
    # Explicitly set so Taipy marks it as callback-changed and includes it
    # in the state push.  Without this, the slider snaps back during render.
    state.pt_min_passes_with_value = int(var_value)
    _refresh_data(state)


def pt_on_min_minutes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter aggregate rankings when minutes slider changes."""
    state.pt_min_minutes = int(var_value)
    _refresh_data(state)


def pt_on_per_match_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter per-match rankings when slider changes."""
    state.pt_per_match_min_passes = int(var_value)
    _refresh_data(state)
```

---

### Task 7: Slider state-preservation — Player Similarity callback

**Files:**
- Modify: `hf_taipy_app/src/state/player_similarity.py:342-346`

- [ ] **Step 1: Harden `on_ps_min_matches_change`**

Replace lines 342-346:
```python
def on_ps_min_matches_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min matches slider changed — reload player list."""
    _clear_results(state)
    state.ps_selected_player = None
    _load_player_list(state)
```

With:
```python
def on_ps_min_matches_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min matches slider changed — reload player list."""
    # Explicitly set so Taipy marks it as callback-changed and includes it
    # in the state push.  Without this, the slider snaps back during render.
    state.ps_min_matches = int(var_value)
    _clear_results(state)
    state.ps_selected_player = None
    _load_player_list(state)
```

---

### Task 8: Add `change_delay=300` to all 7 slider widgets missing it

**Files:**
- Modify: `hf_taipy_app/src/template.py` (7 widget definitions)

- [ ] **Step 1: Add `change_delay=300` to `min_passes` slider (line 565-574)**

After `slider_range_labels=("1", "10"),` (line 573), add:
```python
        change_delay=300,
```

- [ ] **Step 2: Add `change_delay=300` to `min_minutes` Player Impact/Comparison slider (line 576-588)**

After `slider_range_labels=("0", "2000"),` (line 585), add:
```python
        change_delay=300,
```

- [ ] **Step 3: Add `change_delay=300` to `min_minutes` GK slider (line 622-634)**

After `slider_range_labels=("0", "2000"),` (line 631), add:
```python
        change_delay=300,
```

- [ ] **Step 4: Add `change_delay=300` to `pt_per_match_min_passes` slider (line 746-756)**

After `slider_range_labels=("1", "50"),` (line 754), add:
```python
        change_delay=300,
```

- [ ] **Step 5: Add `change_delay=300` to `pt_min_passes_with_value` slider (line 757-767)**

After `slider_range_labels=("1", "200"),` (line 765), add:
```python
        change_delay=300,
```

- [ ] **Step 6: Add `change_delay=300` to `pt_min_minutes` slider (line 768-779)**

After `slider_range_labels=("0", "1000"),` (line 777), add:
```python
        change_delay=300,
```

- [ ] **Step 7: Add `change_delay=300` to `ps_min_matches` slider (line 1106-1115)**

After `slider_range_labels=("1", "50"),` (line 1113), add:
```python
        change_delay=300,
```

---

### Task 9: Add constant-sync drift guard test

**Files:**
- Create: `src/tests/test_taipy_lov_sync.py`

- [ ] **Step 1: Write sync test**

Create `src/tests/test_taipy_lov_sync.py`:

```python
"""Drift guard: shared.py _MATCH_REQUIRED_PAGE_NAMES must stay in sync with template.py."""

import sys
from pathlib import Path

# hf_taipy_app/src is on pyright extraPaths but not on pytest's default sys.path.
_TAIPY_SRC = str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src")
if _TAIPY_SRC not in sys.path:
    sys.path.insert(0, _TAIPY_SRC)

from state.shared import _MATCH_REQUIRED_PAGE_NAMES  # noqa: E402
from template import _MATCH_REQUIRED_PAGES  # noqa: E402


def test_match_required_page_names_in_sync() -> None:
    """_MATCH_REQUIRED_PAGE_NAMES (shared.py) must equal _MATCH_REQUIRED_PAGES (template.py).

    These are duplicated across files to avoid a circular import.  If a future
    PR adds a match-required page to template.py but forgets shared.py, the
    page-aware fallback in on_team_change silently doesn't fire for that page.
    """
    assert _MATCH_REQUIRED_PAGE_NAMES == set(_MATCH_REQUIRED_PAGES), (
        f"Drift detected: shared.py has {_MATCH_REQUIRED_PAGE_NAMES}, "
        f"template.py has {set(_MATCH_REQUIRED_PAGES)}"
    )
```

- [ ] **Step 2: Run the test**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_taipy_lov_sync.py -v`
Expected: PASS

---

### Task 10: Add `hf_taipy_app/src` to pytest testpaths

**Files:**
- Modify: `pyproject.toml:282`
- Create: `hf_taipy_app/src/conftest.py`

- [ ] **Step 1: Add testpath to pyproject.toml**

In `pyproject.toml`, line 282, change:
```toml
testpaths = ["src/tests"]
```
to:
```toml
testpaths = ["src/tests", "hf_taipy_app/src"]
```

- [ ] **Step 2: Create conftest.py to exclude test_render.py from collection**

Create `hf_taipy_app/src/conftest.py`:

```python
"""Pytest collection config for Taipy app tests.

test_render.py is the Taipy app entry point (calls gui.run()), not a pytest
test.  Exclude it from collection to prevent pytest from importing it and
launching the GUI server.
"""

collect_ignore = ["test_render.py"]
```

- [ ] **Step 3: Verify Taipy tests are discovered**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest hf_taipy_app/src/ --collect-only -q 2>&1 | tail -5`
Expected: Tests from `test_tier_a_canon.py`, `test_build_warning.py`, etc. appear. `test_render.py` does NOT appear.

- [ ] **Step 4: Verify test_render.py is excluded**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest hf_taipy_app/src/ --collect-only -q 2>&1 | grep test_render`
Expected: No output (test_render.py is excluded)

---

### Task 11: Update roadmap and TODO

**Files:**
- Modify: `docs/ui-cycles/ui-consistency-roadmap.md`
- Modify: `TODO.md`

- [ ] **Step 1: Close findings #10 and #31 in roadmap**

In `docs/ui-cycles/ui-consistency-roadmap.md`, delete the finding #10 row from the High table and the finding #31 row from the High table. Update the "Last updated" line at the bottom to:

```
2026-05-14 — UI-2 PR shipped findings #10, #31 (filter cascade decouple + match optionality + slider hardening). Remaining: #21, #32.
```

Update the Critical section note if needed (should remain "(None remaining...)").

- [ ] **Step 2: Update TODO.md**

In `TODO.md`, update the UI-2 row in the On Deck table. Change the task description to indicate it shipped, or remove the row if that's the convention (check how UI-3/UI-5 were handled — they were removed when shipped per memory `project_ui_ux_bundle_complete.md`). Remove the UI-2 row.

Update the "Last updated" date line to `2026-05-14`.

---

### Task 12: Run quality checks

- [ ] **Step 1: Run ruff**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check hf_taipy_app/src/state/shared.py hf_taipy_app/src/template.py hf_taipy_app/src/test_render.py hf_taipy_app/src/state/pass_timing.py hf_taipy_app/src/state/player_similarity.py src/tests/test_taipy_lov_sync.py`
Expected: No errors

- [ ] **Step 2: Run pyright on modified files**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pyright hf_taipy_app/src/state/shared.py hf_taipy_app/src/template.py hf_taipy_app/src/test_render.py hf_taipy_app/src/state/pass_timing.py hf_taipy_app/src/state/player_similarity.py src/tests/test_taipy_lov_sync.py`
Expected: 0 errors

- [ ] **Step 3: Run pytest (unit tests only — no DB)**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/ -v -x --ignore=src/tests/test_marts_kimball_contracts.py --ignore=src/tests/test_marts_live_schema.py -k "not live and not lakebase" 2>&1 | head -50`
Expected: All tests pass (or pre-existing failures only per `project_known_pretest_failures_on_main_2026_05_04.md`)

---

### Task 13: Commit

- [ ] **Step 1: Stage and commit**

```bash
git add \
  hf_taipy_app/src/state/shared.py \
  hf_taipy_app/src/template.py \
  hf_taipy_app/src/test_render.py \
  hf_taipy_app/src/conftest.py \
  hf_taipy_app/src/state/pass_timing.py \
  hf_taipy_app/src/state/player_similarity.py \
  src/tests/test_taipy_lov_sync.py \
  pyproject.toml \
  docs/ui-cycles/ui-consistency-roadmap.md \
  docs/superpowers/specs/2026-05-14-ui2-filter-cascade-and-slider-hardening-design.md \
  docs/superpowers/plans/2026-05-14-ui2-filter-cascade-and-slider-hardening.md \
  TODO.md
```

Commit message:
```
fix(taipy): decouple filter cascade + match optionality + slider hardening (UI-2)

- Preserve match selection on team change when match is still in narrowed
  LOV; page-aware fallback to first match on required pages (#10)
- Remove "All" from match dropdown on match-required pages via
  match_lov_required LOV variable (#31)
- Gate GK min_minutes slider to Rankings tab only
- Apply Taipy slider snap-back fix (explicit state assignment) to 6
  callbacks missing it
- Add change_delay=300 to 7 slider widgets missing it
- Add hf_taipy_app/src to pytest testpaths (9 test files were never
  collected); exclude test_render.py (app entry point) via conftest
- Add drift guard test for _MATCH_REQUIRED_PAGE_NAMES constant sync
```
