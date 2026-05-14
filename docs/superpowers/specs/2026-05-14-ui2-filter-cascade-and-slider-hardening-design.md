# UI-2: Filter Cascade Decouple + Match Optionality + Slider Hardening

**Date:** 2026-05-14
**Status:** Design-locked
**Source:** TODO.md UI-2 + cognitive-interface audit findings #10, #31, #32 (slider sub-class)
**Roadmap:** `docs/ui-cycles/ui-consistency-roadmap.md`

---

## Problem Statement

Three related filter/widget UX issues accumulate cognitive interface debt:

1. **Finding #10 (High):** `on_team_change()` unconditionally resets `selected_match = "All"` and narrows `match_lov` by team. On pages where team is optional (Match Summary, Shot Map, Heat Map, Player Impact, Player Comparison), changing team clears the user's match selection even though the page doesn't need team for its primary query. Users must re-select their match after every team change.

2. **Finding #31 (High):** The `match_lov` always starts with `"All"` on every page, but Pass-Map, Pass-Network, and Match-Summary produce empty states when `"All"` is selected (match is required). The `RequiredFilter` mechanism shows guidance text, but "All" remains a dead-end selection that the widget shouldn't offer.

3. **Slider bugs (mixed severity):**
   - GK `min_minutes` slider appears on all 3 Goalkeeper-Analytics tabs (Rankings, Shot Stopping, Distribution), but Shot Stopping and Distribution primary content ignores it.
   - 6 slider callbacks lack the explicit state-preservation pattern (`state.var = int(var_value)` before refresh) that prevents Taipy's slider snap-back bug. Only `pc_elapsed_seconds` and `ts_elapsed_seconds` have the fix.
   - Same 6 sliders lack `change_delay=300`, causing rapid re-renders on drag.

---

## Design

### Section 1: Conditional match reset in `on_team_change` (Finding #10)

**File:** `hf_taipy_app/src/state/shared.py`

**Current:** Line 467 unconditionally sets `state.selected_match = _ALL_LABEL` before re-fetching matches.

**Change:** After building the new `match_lov` from `fetch_matches(comp_key, team_id)`, check whether `state.selected_match` is still in the new list. If yes, preserve it. If no (match dropped out of narrowed list), fall back appropriately based on page type.

**Page-aware fallback (M1 review fix):** `on_team_change` is a shared callback — when the user's match drops out of the narrowed list, the fallback depends on whether the current page uses `match_lov_required` (no "All") or `match_lov` (has "All"). A module-level constant `_MATCH_REQUIRED_PAGE_NAMES` in `shared.py` mirrors `template.py`'s `_MATCH_REQUIRED_PAGES` tuple for this check. On required pages, the fallback is the first match in `match_lov_required` (if non-empty); on optional pages, the fallback is `_ALL_LABEL`.

```python
# Module-level constant (mirrors template.py _MATCH_REQUIRED_PAGES):
_MATCH_REQUIRED_PAGE_NAMES = frozenset(("Pass-Map", "Pass-Network", "Match-Summary"))

# In on_team_change(), REPLACE:
#   state.selected_match = _ALL_LABEL
# WITH (after match_lov is rebuilt):
matches = fetch_matches(comp_key, team_id)
_match_map = {label: (mk, mid) for label, mk, mid in matches}
new_labels = [_ALL_LABEL] + [label for label, _mk, _mid in matches]
required_labels = [label for label, _mk, _mid in matches]
state.match_lov = new_labels
state.match_lov_required = required_labels

# Preserve match selection if still valid; reset with page-aware fallback.
if state.selected_match not in new_labels:
    if state.current_page in _MATCH_REQUIRED_PAGE_NAMES and required_labels:
        state.selected_match = required_labels[0]
    else:
        state.selected_match = _ALL_LABEL
```

**Duplication note:** `_MATCH_REQUIRED_PAGE_NAMES` duplicates `template.py`'s `_MATCH_REQUIRED_PAGES`. This avoids a circular import (`template.py` cannot import from `shared.py`). The set is small and stable (3 page names). If page names are ever refactored, both constants must be updated — grep for `_MATCH_REQUIRED` to find them.

**`on_competition_change` is unchanged** — competition switch always means a new match universe, so unconditional reset is correct. It does still populate `match_lov_required` alongside `match_lov`.

### Section 2: Remove "All" from match LOV on required pages (Finding #31)

**Files:** `hf_taipy_app/src/state/shared.py`, `hf_taipy_app/src/template.py`

**New state variable:** `match_lov_required: list[str] = []` — same as `match_lov` but without the `_ALL_LABEL` prefix.

**Population:** Both `on_competition_change()` and `on_team_change()` set `state.match_lov_required` alongside `state.match_lov`:

```python
state.match_lov = [_ALL_LABEL] + [label for label, _mk, _mid in matches]
state.match_lov_required = [label for label, _mk, _mid in matches]
```

**Widget wiring:** In `template.py`, the match-required `SidebarWidget` changes `lov="match_lov"` to `lov="match_lov_required"`. The match-optional widget keeps `lov="match_lov"`.

**Edge case:** When `match_lov_required` is empty (no matches), the dropdown is empty. `RequiredFilter` guidance text already covers this.

### Section 3: Slider hardening

#### 3a. GK slider tab gating

**File:** `hf_taipy_app/src/template.py`

Change the GK `min_minutes` slider condition from:
```python
condition=f"current_page in {_GK_PAGES}"
```
to:
```python
condition=f'current_page in {_GK_PAGES} and selected_sub_view == "Rankings"'
```

Shot Stopping and Distribution tabs no longer show the misleading slider.

**String-literal coupling note (L2 review):** This introduces a string dependency on the sub-view name `"Rankings"`. The existing codebase already has this coupling — `template.py` line 614 uses `!= "Rankings"` for the GK player dropdown condition. If GK tab names are refactored, grep for `"Rankings"` in `condition=` expressions across `template.py`.

#### 3b. State-preservation pattern

Apply the documented Taipy snap-back fix to 6 slider callbacks. Each gets explicit `state.var = int(var_value)` before calling refresh:

| Callback | Variable | File |
|----------|----------|------|
| `on_min_passes_change` | `min_passes` | `state/shared.py` |
| `on_min_minutes_change` | `min_minutes` | `state/shared.py` |
| `pt_on_per_match_min_passes_change` | `pt_per_match_min_passes` | `state/pass_timing.py` |
| `pt_on_min_passes_change` | `pt_min_passes_with_value` | `state/pass_timing.py` |
| `pt_on_min_minutes_change` | `pt_min_minutes` | `state/pass_timing.py` |
| `on_ps_min_matches_change` | `ps_min_matches` | `state/player_similarity.py` |

Pattern (same as `pc_on_seconds_change`):
```python
def on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min passes slider changed — refresh current page."""
    # Explicitly set so Taipy marks it as callback-changed and includes it
    # in the state push. Without this, the slider snaps back during render.
    state.min_passes = int(var_value)
    _refresh_current_page(state)
```

#### 3c. `change_delay=300` on all sliders missing it

Add `change_delay=300` to 6 slider `SidebarWidget` definitions in `template.py`:
- `min_passes` (Pass Network)
- `min_minutes` (GK variant)
- `min_minutes` (Player Impact/Comparison variant)
- `pt_per_match_min_passes` (Pass Timing)
- `pt_min_passes_with_value` (Pass Timing)
- `pt_min_minutes` (Pass Timing)
- `ps_min_matches` (Player Similarity)

That's 7 widget definitions (the `min_minutes` variable has two separate `SidebarWidget` entries for different page conditions).

---

## Files Changed

| File | Changes |
|------|---------|
| `hf_taipy_app/src/state/shared.py` | Conditional match reset in `on_team_change`; `match_lov_required` population in both cascade callbacks; state-preservation in `on_min_passes_change` and `on_min_minutes_change` |
| `hf_taipy_app/src/template.py` | Match-required widget LOV swap; GK slider tab gating; `change_delay=300` on 7 slider widgets |
| `hf_taipy_app/src/state/pass_timing.py` | State-preservation in 3 slider callbacks |
| `hf_taipy_app/src/state/player_similarity.py` | State-preservation in 1 slider callback |
| `docs/ui-cycles/ui-consistency-roadmap.md` | Close findings #10, #31; update last-updated |
| `TODO.md` | Update UI-2 entry |

---

## Out of Scope

- **Finding #32** (multi-select backend search for `selected_players_multi`) — tracked as TODO UI-1, separate PR.
- **Finding #21** (deep linking / URL-encoded filter state) — feature-level, scope TBD.
- **TODO.md #30** (shared-state race on team-change-after-comp-change) — Taipy framework-level, different bug class.
- **Query-level `min_minutes` filtering for GK Shot Stopping/Distribution** — the slider is hidden on those tabs (Section 3a), not extended to their queries. Adding `min_minutes` to `fetch_gk_shots`/`fetch_gk_passes` would change analytical semantics (career threshold applied to per-match shot data) and belongs in a separate discussion.

---

## Acceptance Criteria

1. On Match Summary: select competition, select match, change team — match selection is preserved if it's still in the narrowed list.
2. On a match-required page (e.g., Pass-Network): select comp, select match, change team such that the match drops out — the first available match is auto-selected (not "All").
3. On Pass-Map / Pass-Network / Match-Summary: "All" does not appear in the match dropdown.
4. On Conversion-Funnel / Player-Impact: "All" still appears in the match dropdown and produces valid aggregate data.
5. GK `min_minutes` slider only appears on the Rankings tab.
6. All 9 sliders have `change_delay` and the explicit state-preservation pattern (see inventory below).
7. No slider snaps back to its initial value during page refresh.

### Slider inventory (9 total)

| # | Variable | Widget condition pages | `change_delay` | State-preservation | Status before this PR |
|---|----------|----------------------|-----------------|--------------------|-----------------------|
| 1 | `pc_elapsed_seconds` | Pitch Control | 300 | Yes | Already fixed |
| 2 | `ts_elapsed_seconds` | Team Shape (Snapshot) | 300 | Yes | Already fixed |
| 3 | `min_passes` | Pass Network | **Missing** | **Missing** | Needs fix |
| 4 | `min_minutes` (GK) | Goalkeeper-Analytics | **Missing** | **Missing** | Needs fix |
| 5 | `min_minutes` (Player) | Player Impact, Player Comparison | **Missing** | **Missing** | Needs fix |
| 6 | `pt_per_match_min_passes` | Pass Timing | **Missing** | **Missing** | Needs fix |
| 7 | `pt_min_passes_with_value` | Pass Timing | **Missing** | **Missing** | Needs fix |
| 8 | `pt_min_minutes` | Pass Timing | **Missing** | **Missing** | Needs fix |
| 9 | `ps_min_matches` | Player Similarity | **Missing** | **Missing** | Needs fix |

Sliders #4 and #5 share the same state variable (`min_minutes`) and callback (`on_min_minutes_change`) but are separate `SidebarWidget` entries with different `condition` tuples. The callback fix (Section 3b) covers both; the `change_delay` fix (Section 3c) touches both widget definitions.
