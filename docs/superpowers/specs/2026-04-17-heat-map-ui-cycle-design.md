# Heat Map UI cycle — design spec

| | |
|---|---|
| **Date** | 2026-04-17 |
| **Branch** | `ui/heat-map-context-and-filters` |
| **Author** | Karsten Nielsen (with Claude Opus 4.7) |
| **Status** | Implemented — with mid-flight amendment (see below) |
| **Supersedes** | — |
| **Related** | Cognitive audit (transcript 2026-04-17); **ADR-008 Tier A canonical UI pattern** (shipped implementation); ADR-002 silent-swallow elimination (tangential) |

## Amendment — 2026-04-17 (post-implementation)

Sections §5.1, §5.3, §5.4, §5.7, and §5.8 below describe an HTML-injection approach using Taipy's `|text|raw|` flag. **That approach does not work.** Live testing against Taipy 4.1.1 revealed that `|text|raw|` escapes HTML entities (`<span>` renders as `&lt;span&gt;` literal text in the DOM). Implementation pivoted to a Taipy-native pattern:

- New `ScopeDim(label, value_var)` dataclass on `page_template.py`
- `PageConfig.scope_dims: list[ScopeDim]` — canonical scope line (primary)
- `PageConfig.scope_vars: list[str]` — secondary plain-text lines (unchanged)
- Per-dimension state variables (`hm_scope_comp`, `hm_scope_team`, `hm_scope_player`; `ms_scope_comp`, `ms_scope_team`, `ms_scope_match`) in place of a single HTML-laden var
- CSS pseudo-element `.ll-scope-dim-label:not(:first-of-type)::before` draws the vertical divider between dimensions — zero separator markup in the emitted Taipy Markdown
- `filters.build_scope_label` removed; `filters.build_scope_label_plain` retained for alt attributes

The shipped design and the alternatives considered are documented in `docs/superpowers/adrs/ADR-008-tier-a-canonical-ui-pattern.md`, which supersedes the relevant spec sections below. All other sections (scope, approved decisions, CSS additions, lightbox JS rewrite, testing strategy, migration plan, success criteria) remain accurate.

The rest of this document is the original pre-implementation design, preserved for audit trail.

## 1. Goal

Establish canonical template-layer patterns for context clarity, filter labelling, and image overlay behaviour on all Tier A (StatsBomb event) pages. Heat Map and Match Summary adopt the patterns in this branch; the other 7 Tier A pages migrate in follow-up PRs. Close 4 Critical and 8 High severity findings from the cognitive-interface audit run on 2026-04-17.

The work targets the "flagship page" bar — a first-time visitor (kiosk user) should be able to read a page, open any diagram in the overlay, and answer "what am I looking at?" without guessing.

## 2. Scope

### In this branch

- Template-layer changes in `hf_taipy_app/src/page_template.py`, `hf_taipy_app/src/filters.py`, `hf_taipy_app/src/style_v2.css`, `hf_taipy_app/src/main.py` (lightbox JS), `hf_taipy_app/src/template.py` (sidebar widget marking migration).
- Page wiring in `hf_taipy_app/src/pages/heat_map.py`, `hf_taipy_app/src/pages/match_summary.py`, `hf_taipy_app/src/state/heat_map.py`, `hf_taipy_app/src/state/match_summary.py`.
- Heat Map glossary population in `hf_taipy_app/src/template.py::PAGE_TERMS`.
- Tests: unit tests for template helpers + Puppeteer test for the lightbox.
- Deferred-tracking artefact: `docs/ui-cycles/ui-consistency-roadmap.md`.

### Explicitly out of scope (tracked in roadmap doc)

- Migration of the 7 other Tier A pages (Shot-Map, Pass-Map, Pass-Network, Player-Impact, Player-Comparison, Goalkeeper-Analytics, Conversion-Funnel) to the new `build_scope_label` helper — one mechanical follow-up PR each.
- Match Summary's `Team → Match` cascade coupling (page-level bug; needs separate impact analysis).
- Broad `except Exception:` cleanup in refresh callbacks (ADR-002).
- Lightbox injection via Flask `after_request` + CSP hardening (security-adjacent, not UI).
- `prefers-reduced-motion` guard on `.ll-spin` (a11y sweep).
- Responsive layout below 768px (responsive audit).
- Deep linking / URL-encoded filter state (feature-level).
- Programmatic CVD audit with `colorspacious` (tool/CI investment).

## 3. Approved design decisions

All five resolved during brainstorming on 2026-04-17:

| # | Decision | Choice |
|---|----------|--------|
| 1 | Separator glyph | CSS-rendered 1px vertical divider (`<span class="ll-sep"></span>`, 1px × 0.9em, `rgba(255,255,255,0.45)`) |
| 2 | Empty / warning state wording | `build_warning(domain, suggestions)` helper with canonical shape `"No {domain} found for this selection. Try {joined}."` |
| 3 | Required / optional marker | `required: bool = True` field on `SidebarWidget`; `" (optional)"` auto-suffix when `False`; no extra visual treatment |
| 4 | Figcaption injection | Clone-and-render; scope-only (chart title is in the PNG); read from `[data-role="page-scope"]` at click time |
| 5 | Scope-line presentation | Small-caps dimension labels (`COMPETITION England — Premier League │ TEAM All teams │ PLAYER Jens Lehmann`) |

## 4. Canonical contract — Tier A pages after this cycle

After this branch and the 7 follow-up migration PRs, every Tier A page MUST satisfy:

1. Scope line lists **only the dimensions that page exposes as a filter**, in filter-cascade order, using the canonical separator, with small-caps dimension labels.
2. Filter widgets are marked `required=True` / `required=False` explicitly on their `SidebarWidget` definition; the template renders the marker in the label.
3. Empty states are produced by `build_warning(domain, suggestions)` — never hand-written strings.
4. Every image has a scope-aware `alt` text that carries both chart title and scope.
5. Image clicks open an accessible overlay with: close button, Escape key, focus trap, `role="dialog"`, scope figcaption cloned from the page scope element.
6. Every page's `PAGE_TERMS` entry has at least one glossary term, or an explicit code comment explaining why not.

A lint check or test asserts these contracts across all Tier A pages — see Section 10.

## 5. Architecture

### 5.1 `filters.py` — two new helpers, one backward-compatible

**New:**

```python
def build_scope_label(pairs: list[tuple[str, str]], *, plain: bool = False) -> str:
    """Render a canonical scope label line.

    Args:
        pairs: List of (dimension_label, value) tuples in display order.
            Example: [("Competition", "England — Premier League"), ("Team", "All teams"), ("Player", "Jens Lehmann")]
            Dimensions the page does NOT filter by should be omitted entirely.
        plain: When True, emit plain text (no HTML tags) suitable for the `alt`
            attribute of images and screen-reader consumers. When False
            (default), emit HTML with the canonical span classes for page
            rendering.

    Returns:
        HTML (plain=False):
            '<span class="ll-scope-dim-label">COMPETITION</span>England — Premier League<span class="ll-sep"></span>...'
        Plain (plain=True):
            'Competition: England — Premier League · Team: All teams · Player: Jens Lehmann'

        HTML values are html.escape'd. Dimension labels are uppercase'd on
        emission; the input tuple passes them in title-case.
    """
```

```python
def build_warning(domain: str, suggestions: list[str]) -> str:
    """Render a canonical 'no data' warning message.

    Args:
        domain: Plural domain noun (e.g., 'actions', 'match data', 'passes').
        suggestions: Short human-phrased suggestions; 0-3 entries recommended.
            Example: ['removing the team filter', 'choosing a different player']

    Returns:
        'No {domain} found for this selection. Try {joined}.' with suggestions
        joined with 'or' before the last entry and commas between earlier ones.
        If suggestions is empty, returns 'No {domain} found for this selection.
        Try adjusting your filters.'
    """
```

**Backward-compat kept:** `fetch_scope_label(comp_id, team_id) -> str` continues to exist with its current signature. Marked with a module-level comment that new code should use `build_scope_label()`. Retired when the 7 follow-up PRs have each migrated their caller — at which point the function is deleted (no deprecation period needed in a single-maintainer repo).

### 5.2 `page_template.py` — dataclass extensions

**`SidebarWidget`:**

```python
@dataclass(frozen=True)
class SidebarWidget:
    # ... existing fields ...
    required: bool = True  # NEW — False → " (optional)" auto-appended to label
```

`_build_sidebar_widget` constructs the effective label:

```python
effective_label = w.label if w.required else f"{w.label} (optional)"
```

Widgets that currently have hand-suffixed `"(optional)"` get migrated to `required=False` with the bare label. Migration list (confirmed from current `template.py`):

- `selected_player` → `"Player"` + `required=False`
- `pt_selected_team` → `"Team"` + `required=False`
- `pt_selected_player` → `"Player"` + `required=False`
- `dv_selected_team` → `"Team"` + `required=False`

Existing widgets without `(optional)` that should be marked `required=False` (based on behavioural analysis):

- `selected_team` → `required=False` on Heat-Map / Shot-Map / Player-Impact / Player-Comparison (it does not functionally gate those pages). NOTE: Heat-Map and Match-Summary are what this branch touches; the other 5 stay unchanged on this branch — their `required=False` gets applied in their own migration PR.

**`ContentBlock`:**

```python
@dataclass(frozen=True)
class ContentBlock:
    # ... existing fields ...
    alt_var: str = ""  # NEW — state variable for image alt text (falls back to label=page_title)
```

`_build_content_block` for `kind == "image"`:

```python
if block.alt_var:
    parts.append(f"<|{{{block.var}}}|image|label={{{block.alt_var}}}|width=100%|>")
else:
    parts.append(f"<|{{{block.var}}}|image|label={page_title}|width=100%|>")
```

The fallback preserves behaviour for all pages that don't opt in.

### 5.3 Scope-label rendering in `_build_standard_page`

Current (simplified):

```python
for sv in cfg.scope_vars:
    parts.append(f"<|part|render={{len({sv}) > 0}}|")
    parts.append(f"<|{{{sv}}}|text|>")
    parts.append("|>")
```

**Contract clarification:** `scope_vars[0]` is always the canonical scope label (emitted by `build_scope_label` with HTML markup). `scope_vars[1:]` are secondary plain-text lines (e.g., Match Summary's `ms_league_averages`).

New — the first `scope_var` is rendered inside a marked wrapper that the lightbox JS can find:

```python
if cfg.scope_vars:
    primary = cfg.scope_vars[0]
    # Use class_name="ll-page-scope" as the primary selector (Taipy emits class on div)
    # AND raw HTML span with data-role="page-scope" inside for belt-and-braces.
    parts.append(f'<|part|class_name=ll-page-scope|render={{len({primary}) > 0}}|')
    parts.append(f'<|{{{primary}}}|text|raw|class_name=ll-scope-label|>')
    parts.append('|>')
    # Remaining scope_vars render as normal
    for sv in cfg.scope_vars[1:]:
        parts.append(f"<|part|render={{len({sv}) > 0}}|")
        parts.append(f"<|{{{sv}}}|text|>")
        parts.append("|>")
```

Lightbox JS queries `.ll-page-scope` first, falls back to `[data-role="page-scope"]` if present. The `|raw|` Taipy flag is needed because `build_scope_label` emits HTML (span tags for dimension labels and separators).

### 5.4 `build_scope_label` output format

```
<span class="ll-scope-dim-label">COMPETITION</span>England — Premier League<span class="ll-sep"></span><span class="ll-scope-dim-label">TEAM</span>All teams<span class="ll-sep"></span><span class="ll-scope-dim-label">PLAYER</span>Jens Lehmann
```

HTML-safety: values are `html.escape`d before insertion. Dimension labels are controlled (developer-supplied constants), not escaped but allowlisted.

When cloned into the overlay figcaption by JS, the same span classes are preserved — overlay CSS styles them to be readable on the dark semi-transparent background.

### 5.5 `style_v2.css` additions

```css
/* ── Canonical scope label (page-level scope + lightbox figcaption) ── */

.ll-page-scope {
    margin-bottom: 0.75rem;
}

.ll-scope-label {
    font-style: italic;
    font-size: 0.9rem;
    color: rgba(255, 255, 255, 0.65);
    line-height: 1.6;
}

.ll-scope-dim-label {
    font-style: normal;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: rgba(255, 255, 255, 0.35);
    margin-right: 0.35em;
    font-weight: 600;
}

.ll-sep {
    display: inline-block;
    width: 1px;
    height: 0.9em;
    background: rgba(255, 255, 255, 0.45);
    margin: 0 0.75em;
    vertical-align: -1px;
}

/* ── Lightbox accessibility upgrade ── */

.ll-lightbox-overlay {
    /* existing rules kept, plus: */
    outline: none;
}

.ll-lightbox-overlay figure {
    margin: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.75rem;
    max-width: 90vw;
    max-height: 90vh;
}

.ll-lightbox-overlay figure img {
    max-width: 100%;
    max-height: calc(90vh - 4rem);
    border-radius: 8px;
    box-shadow: 0 4px 40px rgba(0, 0, 0, 0.5);
}

.ll-lightbox-caption {
    font-family: 'Source Sans Pro', -apple-system, sans-serif;
    color: rgba(255, 255, 255, 0.85);
    background: rgba(14, 17, 23, 0.85);
    padding: 0.5rem 1rem;
    border-radius: 6px;
    font-size: 0.9rem;
    max-width: 90vw;
    text-align: center;
}

.ll-lightbox-close {
    position: fixed;
    top: 1.25rem;
    right: 1.5rem;
    background: rgba(14, 17, 23, 0.85);
    color: rgba(255, 255, 255, 0.85);
    border: 1px solid rgba(255, 255, 255, 0.3);
    border-radius: 50%;
    width: 2.5rem;
    height: 2.5rem;
    font-size: 1.25rem;
    line-height: 1;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
}

.ll-lightbox-close:hover,
.ll-lightbox-close:focus {
    border-color: var(--color-primary);
    color: var(--color-primary);
    outline: none;
}

/* Keyboard-focusable images in content rows */
.ll-content-row img:focus {
    outline: 2px solid var(--color-primary);
    outline-offset: 2px;
}
```

### 5.6 `main.py` — lightbox JS rewrite

```javascript
(function(){
  let _previouslyFocused = null;
  let _overlay = null;

  function trapFocus(e) {
    if (!_overlay) return;
    if (e.key === 'Escape') { e.preventDefault(); closeOverlay(); return; }
    if (e.key !== 'Tab') return;
    const focusables = _overlay.querySelectorAll('button, [tabindex="0"]');
    if (!focusables.length) return;
    const first = focusables[0], last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  }

  function closeOverlay() {
    if (!_overlay) return;
    document.removeEventListener('keydown', trapFocus);
    _overlay.remove();
    _overlay = null;
    if (_previouslyFocused) { _previouslyFocused.focus(); _previouslyFocused = null; }
  }

  function openOverlay(img) {
    _previouslyFocused = document.activeElement;

    const scopeEl = document.querySelector('.ll-page-scope, [data-role="page-scope"]');
    const scopeHtml = scopeEl ? scopeEl.innerHTML : '';

    _overlay = document.createElement('div');
    _overlay.className = 'll-lightbox-overlay';
    _overlay.setAttribute('role', 'dialog');
    _overlay.setAttribute('aria-modal', 'true');
    _overlay.setAttribute('aria-label', 'Enlarged chart view');

    const closeBtn = document.createElement('button');
    closeBtn.className = 'll-lightbox-close';
    closeBtn.setAttribute('aria-label', 'Close (Escape)');
    closeBtn.textContent = '\u00d7';
    closeBtn.addEventListener('click', closeOverlay);

    const fig = document.createElement('figure');
    const clone = document.createElement('img');
    clone.src = img.src;
    clone.alt = img.alt || '';
    fig.appendChild(clone);

    if (scopeHtml) {
      const caption = document.createElement('figcaption');
      caption.className = 'll-lightbox-caption';
      caption.innerHTML = scopeHtml;
      fig.appendChild(caption);
    }

    _overlay.appendChild(closeBtn);
    _overlay.appendChild(fig);

    _overlay.addEventListener('click', function(e) {
      if (e.target === _overlay) closeOverlay();
    });

    document.body.appendChild(_overlay);
    document.addEventListener('keydown', trapFocus);
    closeBtn.focus();
  }

  document.addEventListener('click', function(e) {
    const img = e.target;
    if (img.tagName !== 'IMG') return;
    if (!img.closest('.ll-content-row')) return;
    if (_overlay) return;
    e.stopPropagation();
    openOverlay(img);
  });

  document.addEventListener('keydown', function(e) {
    if (e.key !== 'Enter') return;
    const el = document.activeElement;
    if (el && el.tagName === 'IMG' && el.closest('.ll-content-row')) {
      e.preventDefault();
      openOverlay(el);
    }
  });

  // Make content images keyboard-focusable
  const observer = new MutationObserver(function() {
    document.querySelectorAll('.ll-content-row img:not([tabindex])').forEach(function(img) {
      img.setAttribute('tabindex', '0');
    });
  });
  observer.observe(document.body, { childList: true, subtree: true });
})();
```

Replaces the existing 15-line script. ~85 lines vanilla JS, no external deps.

### 5.7 Page wiring — Heat Map

`state/heat_map.py::hm_refresh` — after computing metrics, before the logger.info at the end:

```python
# Resolve labels for scope
from state.shared import _ALL_LABEL

comp_label = state.selected_competition or ""
team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
player_label = state.selected_player if state.selected_player not in (None, _ALL_LABEL) else "All players"

scope_pairs = [
    ("Competition", comp_label),
    ("Team", team_label),
    ("Player", player_label),
]
state.hm_scope_label = build_scope_label(scope_pairs)          # HTML for page render
scope_plain = build_scope_label(scope_pairs, plain=True)       # plain text for alt attrs

# Coverage context for EID — simple f-string; no helper needed
n_matches = int(actions["match_id"].nunique()) if not actions.empty else 0
state.hm_scope_coverage = (
    f"{metrics['total']} actions across {n_matches} match{'es' if n_matches != 1 else ''}"
    if n_matches > 0 else ""
)

# Alt strings for each of the 4 images
state.hm_pass_bubbles_alt = f"Pass Distribution — {scope_plain}"
state.hm_shot_bubbles_alt = f"Shot Distribution — {scope_plain}"
state.hm_pass_focus_alt = f"Pass Hotspots (Top 5) — {scope_plain}"
state.hm_shot_focus_alt = f"Shot Hotspots (Top 5) — {scope_plain}"
```

Warning now uses `build_warning`:

```python
state.hm_warning_text = build_warning(
    domain="actions",
    suggestions=["removing the team filter", "choosing a different player"]
)
```

`pages/heat_map.py` gains `scope_vars=["hm_scope_label", "hm_scope_coverage"]` and `alt_var=` on each `ContentBlock`:

```python
ContentBlock("image", "hm_pass_bubbles", alt_var="hm_pass_bubbles_alt"),
# ... etc.
```

### 5.8 Page wiring — Match Summary

Parallel to Heat Map. `ms_refresh`:

```python
from state.shared import _ALL_LABEL

comp_label = state.selected_competition or ""
team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
match_label = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "—"

scope_pairs = [
    ("Competition", comp_label),
    ("Team", team_label),
    ("Match", match_label),
]
state.ms_scope_label = build_scope_label(scope_pairs)          # HTML for page render
scope_plain = build_scope_label(scope_pairs, plain=True)       # plain text for alt attrs

state.ms_shooting_chart_alt = f"Shooting — {scope_plain}"
state.ms_passing_chart_alt = f"Passing — {scope_plain}"
state.ms_possession_chart_alt = f"Possession — {scope_plain}"
state.ms_ppda_chart_alt = f"Pressing (PPDA) — {scope_plain}"
```

Warning:

```python
state.ms_warning_text = build_warning(
    domain="match data",
    suggestions=["choosing a different match"]
)
```

**League averages line:** kept as-is but migrated to the canonical separator. Two options:

- **(a) Keep as plain text `<|{var}|text|>` and use the Unicode middle dot `·`** — simplest; separator consistency is softer here because this is a secondary info line, not the primary scope.
- **(b) Emit with `<span class="ll-sep">` tags and render via `|raw|` flag** — matches the canonical scope rendering exactly.

**Choose (a).** The scope *line* is the canonical surface this branch standardizes; secondary text lines like `ms_league_averages` stay plain to avoid proliferating `|raw|` / HTML-injection surfaces. If cross-page consistency for secondary separators becomes an issue, it's a trivial follow-up.

`pages/match_summary.py` gains `alt_var=` on each `ContentBlock`; `scope_vars=["ms_scope_label", "ms_league_averages"]` unchanged except `ms_scope_label` is now the canonical first slot.

### 5.9 Heat Map glossary population

`template.py::PAGE_TERMS["Heat-Map"]` gains three entries. The corresponding `GLOSSARY` keys are added (before `PAGE_TERMS` uses them):

- `"Bubble Map"` — "Action density visualization where bubble area at each pitch zone represents the number of actions. Per Anzer & Bauer (2021)."
- `"Hotspot"` — "A pitch zone with the highest action density. Heat Map highlights the top 5 zones with gold rings."
- `"Action Type"` — "The kind of on-ball action — pass, shot, dribble, etc. Heat Map separates passes and shots into distinct panels."

## 6. Data flow

### 6.1 On filter change

1. User changes a filter. Taipy calls `on_*_change` in `state/shared.py`.
2. `_refresh_current_page(state)` dispatches to the page's registered refresh function.
3. Refresh resolves labels for each dimension from the current filter state, calls `build_scope_label(pairs)`, sets `state.{prefix}_scope_label`.
4. Refresh computes per-image alt strings and sets them in state.
5. Taipy re-renders the page. Scope label element carries `class="ll-page-scope"`. Each `<|image|>` picks up its new `label` attribute (which Taipy maps to the `alt` attribute on the resulting `<img>`).

### 6.2 On image click

1. User clicks `<img>` inside `.ll-content-row`.
2. Lightbox JS intercepts, reads `.ll-page-scope` `innerHTML` at that moment.
3. JS constructs `<figure><img/><figcaption/></figure>` overlay, adds close button, sets ARIA attributes, focuses the close button.
4. Keydown listener traps focus inside overlay; Escape or close-button-click triggers `closeOverlay()`.
5. On close, overlay removed, focus restored to the original image.

### 6.3 On re-filter while overlay is open

- Current behaviour: overlay blocks all page interaction (click-outside closes it). User can't change filters while overlay is open.
- After this branch: same — focus trap keeps input on the overlay; filter widgets behind the overlay are visually occluded and focus-unreachable. Escape/close brings them back.

## 7. Error handling

| Scenario | Behaviour |
|----------|-----------|
| `build_scope_label([])` — no dimensions | Returns empty string; `|>render={len(var)>0}|` wrapper suppresses empty element |
| `build_scope_label` with HTML-unsafe values | Values passed through `html.escape`; labels are controlled allowlist |
| Lightbox opens with no page-scope element | Overlay renders image + close button, no figcaption |
| Lightbox opens with empty page-scope | Same — empty `innerHTML` → no figcaption element |
| Multiple rapid image clicks | Second click suppressed by `if (_overlay) return;` guard |
| Escape outside overlay | Keydown listener only registered while overlay is open |
| Refresh throws | Existing pattern: logger.exception + cleared state vars. Not changed by this branch. |
| `build_warning` with empty `domain` | Caller error; test asserts non-empty; raise `ValueError` at entry |
| `SidebarWidget` with `required=False` and `depends_on` set | Both render conditions apply (label gets `(optional)`, widget hidden until parent set) |

## 8. Testing strategy

### 8.1 Unit tests — `hf_taipy_app/src/tests/test_scope_label.py` (new)

```python
def test_build_scope_label_empty(): ...
def test_build_scope_label_single_pair(): ...
def test_build_scope_label_three_pairs_contains_canonical_separators(): ...
def test_build_scope_label_escapes_html_in_values(): ...
def test_build_scope_label_emits_ll_scope_dim_label_spans(): ...
def test_build_scope_label_plain_mode_has_no_html_tags(): ...
def test_build_scope_label_plain_mode_uses_middle_dot_separator(): ...
```

### 8.2 Unit tests — `hf_taipy_app/src/tests/test_build_warning.py` (new)

```python
def test_build_warning_no_suggestions_fallback(): ...
def test_build_warning_single_suggestion(): ...
def test_build_warning_two_suggestions_uses_or(): ...
def test_build_warning_three_suggestions_comma_and_or(): ...
def test_build_warning_empty_domain_raises(): ...
```

### 8.3 Unit tests — `hf_taipy_app/src/tests/test_sidebar_widget_marker.py` (new)

```python
def test_sidebar_widget_required_true_label_unchanged(): ...
def test_sidebar_widget_required_false_appends_optional(): ...
def test_sidebar_widget_build_produces_expected_markdown(): ...
```

### 8.4 Unit tests — `hf_taipy_app/src/tests/test_content_block_alt.py` (new)

```python
def test_content_block_image_with_alt_var_binds_state(): ...
def test_content_block_image_without_alt_var_falls_back_to_page_title(): ...
```

### 8.5 Contract test — `hf_taipy_app/src/tests/test_tier_a_canon.py` (new)

Scope: the Tier A pages that have been migrated to the canonical patterns. This branch adds `Heat-Map` and `Match-Summary` to the migrated set; each follow-up PR adds one page to the set until all 9 Tier A pages are covered.

```python
MIGRATED_TIER_A = {"Heat-Map", "Match-Summary"}  # grows per follow-up PR

# Asserts for every page in MIGRATED_TIER_A:
def test_migrated_tier_a_page_has_non_empty_glossary(): ...
def test_migrated_tier_a_page_scope_vars_first_entry_is_scope_label(): ...
def test_migrated_tier_a_page_content_blocks_have_alt_var(): ...
def test_migrated_tier_a_page_warning_uses_build_warning_shape(): ...
```

This test is the guardrail that prevents the "partial pattern application" consistency violation from reopening as the migration progresses. Adding a new page to `MIGRATED_TIER_A` is the contract an author must satisfy during a migration PR.

### 8.6 Puppeteer integration test — `hf_taipy_app/src/tests/test_lightbox.py` (new)

Launches the local Taipy server, navigates to Heat Map, asserts:

1. Clicking an image opens the overlay with `role="dialog"`, `aria-modal="true"`.
2. Overlay contains a `<figure>` with `<img>` and `<figcaption>`.
3. Figcaption text contains the current scope dimensions.
4. Escape key closes the overlay and focus returns to the image.
5. Tab inside overlay stays inside overlay (focus trap).
6. After changing the player filter and clicking a different image, the new figcaption reflects the updated scope.

Uses the existing `mcp__puppeteer` suite per repo conventions.

### 8.7 Manual verification checklist

Before declaring the branch done:

- Load Heat Map in local Taipy, step through each of the 6 user-reported items from the original audit and confirm each is resolved.
- Load Match Summary, confirm scope line shows 3 dimensions, league averages line uses `.ll-sep`.
- Screen-reader smoke test with Windows Narrator on both pages.
- Visual diff of the 8 images vs a pre-branch screenshot — they must not change.

### 8.8 CI checks

- `uv run ruff check` — zero new violations.
- `uv run pyright hf_taipy_app/src` — zero new violations (existing warnings tolerated).
- `uv run pytest hf_taipy_app/src/tests/ -v` — all new tests green; existing tests unchanged.

## 9. Migration plan

### 9.1 This branch — single commit

File touches (approximate count):

- `hf_taipy_app/src/filters.py` — 2 new functions, `fetch_scope_label` kept
- `hf_taipy_app/src/page_template.py` — 2 dataclass fields, 1 scope-rendering change
- `hf_taipy_app/src/template.py` — ~4 widget migrations (`(optional)` → `required=False`)
- `hf_taipy_app/src/style_v2.css` — 4 new classes + lightbox style updates
- `hf_taipy_app/src/main.py` — lightbox JS rewrite
- `hf_taipy_app/src/pages/heat_map.py` — `scope_vars` + `alt_var` on blocks + `empty_message` pattern
- `hf_taipy_app/src/pages/match_summary.py` — same as Heat Map
- `hf_taipy_app/src/state/heat_map.py` — `hm_refresh` rewrite (scope + warning + alts)
- `hf_taipy_app/src/state/match_summary.py` — `ms_refresh` rewrite (scope + warning + alts + league-avg separator migration)
- `hf_taipy_app/src/tests/test_scope_label.py` — new
- `hf_taipy_app/src/tests/test_build_warning.py` — new
- `hf_taipy_app/src/tests/test_sidebar_widget_marker.py` — new
- `hf_taipy_app/src/tests/test_content_block_alt.py` — new
- `hf_taipy_app/src/tests/test_tier_a_canon.py` — new
- `hf_taipy_app/src/tests/test_lightbox.py` — new
- `docs/ui-cycles/ui-consistency-roadmap.md` — new

One squash-friendly commit. Commit message structure per repo convention.

### 9.2 Follow-up PRs — Tier A migration

Each of the 7 remaining Tier A pages gets one PR:

1. Migrate refresh callback from `fetch_scope_label` to `build_scope_label`.
2. Mark filter widgets `required=False` where applicable.
3. Migrate warning message to `build_warning`.
4. Add `alt_var` to each `ContentBlock` image.
5. Populate or explain empty `PAGE_TERMS` entry.

Targets: Shot-Map, Pass-Map, Pass-Network, Player-Impact, Player-Comparison, Goalkeeper-Analytics, Conversion-Funnel. Order of priority is by user demand; not a sequence constraint.

After all 7 PRs land, `fetch_scope_label` is deleted.

### 9.3 Roadmap doc — `docs/ui-cycles/ui-consistency-roadmap.md`

Format — one table per severity. Columns: Finding · Files · Framework · Target PR. Initial content populated from the 11 deferred findings documented in the original audit. Future cycles delete rows as items close; add rows as audits surface new issues. `git log` is the audit trail.

## 10. Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Taipy `|raw|` flag sanitizes our scope HTML spans | Medium | Test during implementation; fallback is to render scope via `<|part|content={var}|>` with a RawHtml content provider (pattern exists in `main.py` for workflow DAG) |
| `data-role="page-scope"` stripped by Taipy | Medium | CSS class `.ll-page-scope` selector as primary; `data-role` is belt-and-braces |
| Focus trap fails on synthetic focus events | Low | Puppeteer test covers Tab and Shift+Tab paths |
| MutationObserver for `tabindex` injection races with Taipy rerender | Low | Observer is idempotent — `img:not([tabindex])` selector prevents reapplication |
| Lightbox `innerHTML` injection of scope label is an XSS vector | Medium | Values in `build_scope_label` are `html.escape`d at source; only developer-controlled dimension labels are raw HTML |
| Breaking change to `ContentBlock` or `SidebarWidget` surfaces in non-Tier-A pages | Low | New fields default to safe values; existing callers unchanged |
| Visual regression on non-Tier-A pages | Low | CSS rules are additive; lightbox change affects all pages equally and is correctness-improvement |

## 11. Rollback

Single commit; rollback is a single `git revert`. Roadmap doc survives the revert (separate path, can be kept or reverted independently).

## 12. Open questions — resolved

1. **`data-role` vs `class_name` selector for lightbox source** — use both (class_name primary, data-role fallback). Resolved.
2. **Roadmap doc format** — table per severity with columns (Finding, Files, Framework, Target PR). Resolved.
3. **`<figure>` in normal DOM vs only overlay** — only overlay (matches Decision 4 hybrid). Resolved.
4. **Alt text carrier** — new `ContentBlock.alt_var` field. Resolved.
5. **`fetch_scope_label` fate** — keep backward-compatible; delete after all 7 follow-up PRs land. Resolved.

## 13. Success criteria

Branch is done when:

- All 6 user-reported items from the original audit resolve visibly on both pages.
- All new tests pass; no existing tests regress.
- Ruff + Pyright clean.
- Manual checklist in §8.7 passes.
- `docs/ui-cycles/ui-consistency-roadmap.md` committed and enumerates all deferred items.
- Single commit, ready for user-approved PR.

Flagship bar is met when: a kiosk user loading Heat Map for the first time, filtering to a specific player, and clicking any diagram can read the overlay without ever returning to the page header to remember what they filtered.
