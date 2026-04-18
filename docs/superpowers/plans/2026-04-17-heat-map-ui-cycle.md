# Heat Map UI cycle — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish canonical template-layer patterns (context clarity, filter labelling, accessible image overlay) on Heat Map + Match Summary; set up the template mechanism so the remaining 7 Tier A pages can migrate in follow-up PRs.

**Architecture:** Template-first — `page_template.py` + `filters.py` + `style_v2.css` + `main.py` (lightbox JS) get the new canonical helpers and classes; Heat Map and Match Summary wire themselves to the canon in this branch; a contract test locks in the pattern for future migrations.

**Tech Stack:** Python 3.10, Taipy 4.1, matplotlib/mplsoccer, dark-theme CSS, vanilla JS.

---

## Repo-specific rules in force

- **No per-task commits.** The repo convention (CLAUDE.md + user standing instruction) is one squash-friendly commit per branch, created only on explicit user approval at the end. Each task ends without committing. A final "commit gate" task at the bottom of the plan waits for user approval.
- **Flat test files.** Existing Taipy-app tests live in `hf_taipy_app/src/test_*.py` (flat), not a subdirectory. This plan follows that convention — the spec's `hf_taipy_app/src/tests/...` paths are remapped to `hf_taipy_app/src/test_*.py` here.
- **Three-strikes investigation protocol.** If a test fails unexpectedly or Taipy renders something unexpected, investigate the first time — do not retry.

## Reference documents

- Design spec: `docs/superpowers/specs/2026-04-17-heat-map-ui-cycle-design.md` — authoritative source for contracts, CSS, and JS. Read it alongside this plan.
- Cognitive audit findings: in the conversation transcript of 2026-04-17.

## File map

**New files (5):**

- `hf_taipy_app/src/test_scope_label.py` — unit tests for `build_scope_label`
- `hf_taipy_app/src/test_build_warning.py` — unit tests for `build_warning`
- `hf_taipy_app/src/test_sidebar_widget_marker.py` — unit tests for `required` field
- `hf_taipy_app/src/test_content_block_alt.py` — unit tests for `alt_var` field
- `hf_taipy_app/src/test_tier_a_canon.py` — contract test for migrated Tier A pages
- `hf_taipy_app/src/test_lightbox.py` — Puppeteer integration test (optional if MCP puppeteer unavailable — gated by local Taipy server)
- `docs/ui-cycles/ui-consistency-roadmap.md` — deferred-findings tracking doc

**Modified files (9):**

- `hf_taipy_app/src/filters.py` — add `build_scope_label`, `build_warning` helpers
- `hf_taipy_app/src/page_template.py` — add `SidebarWidget.required`, `ContentBlock.alt_var`, update `_build_standard_page` scope rendering
- `hf_taipy_app/src/template.py` — migrate 4 widgets + add 3 glossary entries
- `hf_taipy_app/src/style_v2.css` — add canonical CSS classes; update lightbox styles
- `hf_taipy_app/src/main.py` — rewrite `_LIGHTBOX_SCRIPT`
- `hf_taipy_app/src/pages/heat_map.py` — add `alt_var` + `scope_vars`
- `hf_taipy_app/src/pages/match_summary.py` — add `alt_var`
- `hf_taipy_app/src/state/heat_map.py` — rewrite `hm_refresh` (scope pairs, alts, warning)
- `hf_taipy_app/src/state/match_summary.py` — rewrite `ms_refresh` (scope pairs, alts, warning)

---

## Task 1: Scaffold `build_scope_label` with tests

**Files:**
- Create: `hf_taipy_app/src/test_scope_label.py`
- Modify: `hf_taipy_app/src/filters.py` (add import, add function)

- [ ] **Step 1: Write the failing tests**

Create `hf_taipy_app/src/test_scope_label.py`:

```python
"""Unit tests for build_scope_label helper in filters.py."""

from __future__ import annotations

import pytest

from filters import build_scope_label


def test_build_scope_label_empty() -> None:
    """Empty pairs list returns empty string."""
    assert build_scope_label([]) == ""


def test_build_scope_label_single_pair_html() -> None:
    """Single pair emits one dim-label span and one value."""
    result = build_scope_label([("Competition", "England — Premier League")])
    assert '<span class="ll-scope-dim-label">COMPETITION</span>' in result
    assert "England — Premier League" in result
    assert "ll-sep" not in result  # no separator with only one pair


def test_build_scope_label_three_pairs_canonical_separators() -> None:
    """Three pairs emit exactly two separator spans between them."""
    result = build_scope_label(
        [
            ("Competition", "England — Premier League"),
            ("Team", "All teams"),
            ("Player", "Jens Lehmann"),
        ]
    )
    assert result.count('<span class="ll-sep"></span>') == 2
    assert '<span class="ll-scope-dim-label">COMPETITION</span>' in result
    assert '<span class="ll-scope-dim-label">TEAM</span>' in result
    assert '<span class="ll-scope-dim-label">PLAYER</span>' in result


def test_build_scope_label_escapes_html_in_values() -> None:
    """HTML-unsafe characters in values are escaped."""
    result = build_scope_label([("Team", "<script>alert('xss')</script>")])
    assert "<script>" not in result
    assert "&lt;script&gt;" in result


def test_build_scope_label_emits_ll_scope_dim_label_spans() -> None:
    """Dimension labels are uppercased and wrapped in ll-scope-dim-label."""
    result = build_scope_label([("competition", "Value")])
    assert '<span class="ll-scope-dim-label">COMPETITION</span>' in result


def test_build_scope_label_plain_mode_has_no_html_tags() -> None:
    """plain=True emits plain text suitable for alt attributes."""
    result = build_scope_label(
        [("Competition", "England — PL"), ("Team", "All teams")],
        plain=True,
    )
    assert "<" not in result
    assert ">" not in result


def test_build_scope_label_plain_mode_uses_middle_dot_separator() -> None:
    """plain=True uses a human-readable separator, not the HTML span token."""
    result = build_scope_label(
        [("Competition", "England — PL"), ("Team", "All teams")],
        plain=True,
    )
    assert "Competition: England — PL" in result
    assert "Team: All teams" in result
    # The two pairs are joined by " \u00b7 " or similar; assert a separator is present
    assert " · " in result or " \u00b7 " in result
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_scope_label.py -v
```

Expected: all tests fail with `ImportError: cannot import name 'build_scope_label' from 'filters'`.

- [ ] **Step 3: Implement `build_scope_label` in `filters.py`**

In `hf_taipy_app/src/filters.py`, add `import html` at the top (with stdlib imports) and add this function at the end of the "Scope label & data freshness" section (before the last `@ttl_cache` block or after `fetch_scope_label`):

```python
def build_scope_label(
    pairs: list[tuple[str, str]],
    *,
    plain: bool = False,
) -> str:
    """Render a canonical scope label line.

    Args:
        pairs: List of (dimension_label, value) tuples in display order.
            Example: [("Competition", "England — Premier League"), ("Team", "All teams")]
            Dimensions the page does NOT filter by should be omitted entirely.
        plain: When True, emit plain text (no HTML tags) for use as alt text
            or in screen-reader contexts. Default False emits HTML.

    Returns:
        HTML (plain=False): '<span class="ll-scope-dim-label">COMPETITION</span>{value}<span class="ll-sep"></span>...'
        Plain (plain=True): 'Competition: {value} \u00b7 Team: {value}'
    """
    if not pairs:
        return ""
    if plain:
        parts = [f"{label}: {value}" for label, value in pairs]
        return " \u00b7 ".join(parts)
    segments: list[str] = []
    for label, value in pairs:
        dim = f'<span class="ll-scope-dim-label">{label.upper()}</span>'
        escaped_value = html.escape(value)
        segments.append(f"{dim}{escaped_value}")
    return '<span class="ll-sep"></span>'.join(segments)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_scope_label.py -v
```

Expected: all 7 tests pass.

---

## Task 2: Scaffold `build_warning` with tests

**Files:**
- Create: `hf_taipy_app/src/test_build_warning.py`
- Modify: `hf_taipy_app/src/filters.py`

- [ ] **Step 1: Write the failing tests**

Create `hf_taipy_app/src/test_build_warning.py`:

```python
"""Unit tests for build_warning helper in filters.py."""

from __future__ import annotations

import pytest

from filters import build_warning


def test_build_warning_no_suggestions_fallback() -> None:
    """Empty suggestions list uses the canonical fallback sentence."""
    assert (
        build_warning("actions", [])
        == "No actions found for this selection. Try adjusting your filters."
    )


def test_build_warning_single_suggestion() -> None:
    """One suggestion renders without 'or'."""
    assert (
        build_warning("actions", ["choosing a different player"])
        == "No actions found for this selection. Try choosing a different player."
    )


def test_build_warning_two_suggestions_uses_or() -> None:
    """Two suggestions are joined with 'or'."""
    result = build_warning("actions", ["removing the team filter", "choosing a different player"])
    assert result == (
        "No actions found for this selection. "
        "Try removing the team filter or choosing a different player."
    )


def test_build_warning_three_suggestions_comma_and_or() -> None:
    """Three suggestions: comma between first two, 'or' before last."""
    result = build_warning("passes", ["broadening the match", "another team", "different player"])
    assert result == (
        "No passes found for this selection. "
        "Try broadening the match, another team or different player."
    )


def test_build_warning_empty_domain_raises() -> None:
    """Empty or whitespace domain is a caller error."""
    with pytest.raises(ValueError, match="domain must be non-empty"):
        build_warning("", ["x"])
    with pytest.raises(ValueError, match="domain must be non-empty"):
        build_warning("   ", ["x"])
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_build_warning.py -v
```

Expected: ImportError on `build_warning`.

- [ ] **Step 3: Implement `build_warning` in `filters.py`**

Add after `build_scope_label`:

```python
def build_warning(domain: str, suggestions: list[str]) -> str:
    """Render a canonical 'no data' warning message.

    Args:
        domain: Plural domain noun (e.g., 'actions', 'match data', 'passes').
        suggestions: 0-3 short human-phrased next steps.
            Example: ['removing the team filter', 'choosing a different player']

    Returns:
        'No {domain} found for this selection. Try {joined}.'
        Joining: single → bare; two → 'a or b'; three+ → 'a, b or c'.
        Empty suggestions → 'Try adjusting your filters.'
    """
    if not domain or not domain.strip():
        msg = "domain must be non-empty"
        raise ValueError(msg)
    if not suggestions:
        tail = "Try adjusting your filters."
    elif len(suggestions) == 1:
        tail = f"Try {suggestions[0]}."
    elif len(suggestions) == 2:
        tail = f"Try {suggestions[0]} or {suggestions[1]}."
    else:
        head = ", ".join(suggestions[:-1])
        tail = f"Try {head} or {suggestions[-1]}."
    return f"No {domain} found for this selection. {tail}"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_build_warning.py -v
```

Expected: all 5 tests pass.

---

## Task 3: Add `required` field to `SidebarWidget` with tests

**Files:**
- Create: `hf_taipy_app/src/test_sidebar_widget_marker.py`
- Modify: `hf_taipy_app/src/page_template.py`

- [ ] **Step 1: Write the failing tests**

Create `hf_taipy_app/src/test_sidebar_widget_marker.py`:

```python
"""Tests for the SidebarWidget.required field + (optional) label auto-suffix."""

from __future__ import annotations

from page_template import SidebarWidget, _build_sidebar_widget


def test_sidebar_widget_required_defaults_true() -> None:
    """New field has safe default — existing widgets unchanged."""
    w = SidebarWidget("dropdown", "selected_team", "Team", "on_team_change", lov="team_lov")
    assert w.required is True


def test_sidebar_widget_required_false_label_gets_optional_suffix() -> None:
    """required=False appends ' (optional)' to the rendered label."""
    w = SidebarWidget(
        "dropdown",
        "selected_player",
        "Player",
        "on_player_change",
        lov="player_lov",
        required=False,
    )
    md = _build_sidebar_widget(w, f=False)
    assert "label=Player (optional)" in md


def test_sidebar_widget_required_true_label_unchanged() -> None:
    """Bare label is preserved when required=True."""
    w = SidebarWidget(
        "dropdown",
        "selected_competition",
        "Competition",
        "on_competition_change",
        lov="competition_lov",
        required=True,
    )
    md = _build_sidebar_widget(w, f=False)
    assert "label=Competition|" in md
    assert "(optional)" not in md
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_sidebar_widget_marker.py -v
```

Expected: `TypeError: __init__() got an unexpected keyword argument 'required'`.

- [ ] **Step 3: Add `required` field + label rendering logic**

In `hf_taipy_app/src/page_template.py`, in the `SidebarWidget` dataclass definition, add after the existing `filterable: bool = False` line:

```python
    required: bool = True  # False → " (optional)" auto-appended to label
```

Then in `_build_sidebar_widget(w: SidebarWidget, f: bool) -> str`, replace the label used for dropdown rendering. Find this existing block (currently near line 127):

```python
    if w.kind in ("dropdown", "dropdown_multi"):
        multi = "|multiple" if w.kind == "dropdown_multi" else ""
        filter_attr = "|filter" if getattr(w, "filterable", False) else ""
        parts.append(
            f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}{filter_attr}|dropdown|label={w.label}|on_change={w.on_change}|>"
        )
```

Replace with:

```python
    effective_label = w.label if w.required else f"{w.label} (optional)"

    if w.kind in ("dropdown", "dropdown_multi"):
        multi = "|multiple" if w.kind == "dropdown_multi" else ""
        filter_attr = "|filter" if getattr(w, "filterable", False) else ""
        parts.append(
            f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}{filter_attr}|dropdown|label={effective_label}|on_change={w.on_change}|>"
        )
```

(Note: the `effective_label` must be computed before the `if w.kind` branch so it's available for the dropdown label. It is intentionally NOT used for slider/toggle because those use `filter_box_label` which has its own "required"-agnostic contract.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_sidebar_widget_marker.py -v
```

Expected: all 3 tests pass.

---

## Task 4: Add `alt_var` field to `ContentBlock` with tests

**Files:**
- Create: `hf_taipy_app/src/test_content_block_alt.py`
- Modify: `hf_taipy_app/src/page_template.py`

- [ ] **Step 1: Write the failing tests**

Create `hf_taipy_app/src/test_content_block_alt.py`:

```python
"""Tests for ContentBlock.alt_var field."""

from __future__ import annotations

from page_template import ContentBlock, _build_content_block


def test_content_block_image_without_alt_var_falls_back_to_page_title() -> None:
    """When alt_var is empty, image label is the page_title (existing behaviour)."""
    block = ContentBlock("image", "hm_pass_bubbles")
    md = _build_content_block(block, page_title="Heat Map")
    assert "|image|label=Heat Map|" in md


def test_content_block_image_with_alt_var_binds_state() -> None:
    """When alt_var is set, image label binds to a state variable."""
    block = ContentBlock("image", "hm_pass_bubbles", alt_var="hm_pass_bubbles_alt")
    md = _build_content_block(block, page_title="Heat Map")
    assert "|image|label={hm_pass_bubbles_alt}|" in md


def test_content_block_alt_var_default_is_empty_string() -> None:
    """Unset alt_var defaults to empty string."""
    block = ContentBlock("image", "hm_pass_bubbles")
    assert block.alt_var == ""
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_content_block_alt.py -v
```

Expected: `TypeError: __init__() got an unexpected keyword argument 'alt_var'`.

- [ ] **Step 3: Add `alt_var` field + update image rendering**

In `hf_taipy_app/src/page_template.py`, in the `ContentBlock` dataclass definition, add after the existing `table_cell_class_name: dict[str, str] | None = None` line:

```python
    alt_var: str = ""  # state variable for image alt text; falls back to label=page_title
```

In `_build_content_block`, find:

```python
    if block.kind == "image":
        parts.append(f"<|{{{block.var}}}|image|label={page_title}|width=100%|>")
```

Replace with:

```python
    if block.kind == "image":
        if block.alt_var:
            parts.append(f"<|{{{block.var}}}|image|label={{{block.alt_var}}}|width=100%|>")
        else:
            parts.append(f"<|{{{block.var}}}|image|label={page_title}|width=100%|>")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_content_block_alt.py -v
```

Expected: all 3 tests pass.

---

## Task 5: Update `_build_standard_page` scope-label rendering

**Files:**
- Modify: `hf_taipy_app/src/page_template.py`

This change has no unit test of its own (it's an integration-level concern validated via the Puppeteer test later), but we verify by inspecting the generated markdown.

- [ ] **Step 1: Update `_build_standard_page`**

In `hf_taipy_app/src/page_template.py`, find the block inside `_build_standard_page(parts, cfg)` that iterates `cfg.scope_vars`:

```python
    # Scope variables
    for sv in cfg.scope_vars:
        parts.append(f"<|part|render={{len({sv}) > 0}}|")
        parts.append(f"<|{{{sv}}}|text|>")
        parts.append("|>")
        parts.append("")
```

Replace with:

```python
    # Scope variables — scope_vars[0] is the canonical scope label (HTML, rendered via |raw|);
    # scope_vars[1:] are secondary plain-text lines (e.g., league averages).
    if cfg.scope_vars:
        primary = cfg.scope_vars[0]
        parts.append(f"<|part|class_name=ll-page-scope|render={{len({primary}) > 0}}|")
        parts.append(f"<|{{{primary}}}|text|raw|class_name=ll-scope-label|>")
        parts.append("|>")
        parts.append("")
        for sv in cfg.scope_vars[1:]:
            parts.append(f"<|part|render={{len({sv}) > 0}}|")
            parts.append(f"<|{{{sv}}}|text|>")
            parts.append("|>")
            parts.append("")
```

- [ ] **Step 2: Run existing tests to ensure no regression**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/ -v
```

Expected: all 4 new test files pass; existing `test_admin_api.py` and `test_render.py` still pass.

---

## Task 6: Add canonical CSS classes

**Files:**
- Modify: `hf_taipy_app/src/style_v2.css`

- [ ] **Step 1: Add new CSS rules**

In `hf_taipy_app/src/style_v2.css`, append to the end of the file (after the last rule):

```css
/* ── Canonical scope label (page-level + lightbox figcaption) ─────────── */

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

/* ── Lightbox accessibility upgrade ──────────────────────────────────── */

.ll-lightbox-overlay {
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

.ll-content-row img:focus {
    outline: 2px solid var(--color-primary);
    outline-offset: 2px;
}
```

- [ ] **Step 2: Verify CSS parses**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
python -c "import pathlib; css = pathlib.Path('src/style_v2.css').read_text(encoding='utf-8'); opens = css.count('{'); closes = css.count('}'); print(f'opens={opens} closes={closes}'); assert opens == closes, 'unbalanced braces'"
```

Expected: `opens=N closes=N` where N matches.

---

## Task 7: Rewrite lightbox JS in `main.py`

**Files:**
- Modify: `hf_taipy_app/src/main.py`

- [ ] **Step 1: Replace the `_LIGHTBOX_SCRIPT` block**

In `hf_taipy_app/src/main.py`, find the existing `_LIGHTBOX_SCRIPT = """<script>...</script>"""` assignment (around line 154). Replace with:

```python
    _LIGHTBOX_SCRIPT = """<script>
(function(){
  let _previouslyFocused = null;
  let _overlay = null;

  function trapFocus(e) {
    if (!_overlay) return;
    if (e.key === 'Escape') { e.preventDefault(); closeOverlay(); return; }
    if (e.key !== 'Tab') return;
    const focusables = _overlay.querySelectorAll('button, [tabindex="0"]');
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
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
    closeBtn.textContent = '\\u00d7';
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

  const observer = new MutationObserver(function() {
    document.querySelectorAll('.ll-content-row img:not([tabindex])').forEach(function(img) {
      img.setAttribute('tabindex', '0');
    });
  });
  observer.observe(document.body, { childList: true, subtree: true });
})();
</script>"""
```

**Note on the `\\u00d7`:** this is a JavaScript escape for the `×` glyph, and it must be double-backslashed because it is inside a Python triple-quoted string.

- [ ] **Step 2: Run a syntax check on main.py**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python -c "import ast; ast.parse(open('src/main.py').read()); print('OK')"
```

Expected: `OK`.

---

## Task 8: Update `state/heat_map.py::hm_refresh`

**Files:**
- Modify: `hf_taipy_app/src/state/heat_map.py`

- [ ] **Step 1: Update imports**

At the top of `hf_taipy_app/src/state/heat_map.py`, replace the existing `from filters import ...` line with:

```python
from filters import build_scope_label, build_warning, fetch_data_freshness
```

(`fetch_scope_label` is dropped — this module no longer calls it. The function stays in `filters.py` for the 7 other Tier A pages that haven't migrated yet.)

Also add `_ALL_LABEL` to the imports from `state.shared`:

```python
from state.shared import _ALL_LABEL, get_comp_id, get_match_id, get_player_id, get_team_id, register_page_refresher
```

- [ ] **Step 2: Add new exported state variables**

At the top of the module, in the "Exported state variables" section, add:

```python
hm_scope_coverage: str = ""
hm_pass_bubbles_alt: str = ""
hm_shot_bubbles_alt: str = ""
hm_pass_focus_alt: str = ""
hm_shot_focus_alt: str = ""
```

And extend the `__all__` list to include these 5 new names (in alphabetical order within the existing list).

- [ ] **Step 3: Rewrite the body of `hm_refresh`**

Replace the existing body of `hm_refresh(state: Any) -> None` with:

```python
def hm_refresh(state: Any) -> None:
    """Fetch aggregated actions, compute metrics, render heatmap.

    Competition is required; team, player, and match are optional filters.
    """
    comp_id = get_comp_id(state.selected_competition)

    def _clear_all() -> None:
        state.hm_total = "--"
        state.hm_passes = "--"
        state.hm_shots = "--"
        state.hm_pass_bubbles = ""
        state.hm_shot_bubbles = ""
        state.hm_pass_focus = ""
        state.hm_shot_focus = ""
        state.hm_warning_text = ""
        state.hm_scope_label = ""
        state.hm_scope_coverage = ""
        state.hm_data_freshness = ""
        state.hm_pass_bubbles_alt = ""
        state.hm_shot_bubbles_alt = ""
        state.hm_pass_focus_alt = ""
        state.hm_shot_focus_alt = ""

    if comp_id is None:
        _clear_all()
        return

    team_id = get_team_id(state.selected_team)
    player_id = get_player_id(state.selected_player)
    match_id = get_match_id(state.selected_match)

    # Resolve display labels for scope line (dimensions the page filters by)
    comp_label = state.selected_competition or ""
    team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    player_label = state.selected_player if state.selected_player not in (None, _ALL_LABEL) else "All players"

    scope_pairs = [
        ("Competition", comp_label),
        ("Team", team_label),
        ("Player", player_label),
    ]
    state.hm_scope_label = build_scope_label(scope_pairs)
    scope_plain = build_scope_label(scope_pairs, plain=True)

    try:
        actions = fetch_heatmap_actions(comp_id, team_id, player_id, match_id)
    except Exception:
        logger.exception("Failed to fetch heatmap actions for comp=%d", comp_id)
        state.hm_pass_bubbles = ""
        state.hm_shot_bubbles = ""
        state.hm_pass_focus = ""
        state.hm_shot_focus = ""
        state.hm_scope_coverage = ""
        state.hm_data_freshness = ""
        return

    if actions.empty:
        state.hm_total = "0"
        state.hm_passes = "0"
        state.hm_shots = "0"
        state.hm_pass_bubbles = ""
        state.hm_shot_bubbles = ""
        state.hm_pass_focus = ""
        state.hm_shot_focus = ""
        state.hm_warning_text = build_warning(
            domain="actions",
            suggestions=["removing the team filter", "choosing a different player"],
        )
        state.hm_scope_coverage = ""
        state.hm_data_freshness = ""
        return

    state.hm_warning_text = ""
    metrics = _compute_metrics(actions)
    state.hm_total = metrics["total"]
    state.hm_passes = metrics["passes"]
    state.hm_shots = metrics["shots"]

    # Coverage context for EID — simple f-string
    n_matches = int(actions["match_id"].nunique()) if "match_id" in actions.columns else 0
    state.hm_scope_coverage = (
        f"{metrics['total']} actions across {n_matches} match{'es' if n_matches != 1 else ''}"
        if n_matches > 0 else ""
    )

    # Alt strings (scope-aware) for each of the 4 images
    state.hm_pass_bubbles_alt = f"Pass Distribution — {scope_plain}"
    state.hm_shot_bubbles_alt = f"Shot Distribution — {scope_plain}"
    state.hm_pass_focus_alt = f"Pass Hotspots (Top 5) — {scope_plain}"
    state.hm_shot_focus_alt = f"Shot Hotspots (Top 5) — {scope_plain}"

    # Split by action type
    pass_actions = actions.loc[actions["action_type"] == "pass"]
    shot_actions = actions.loc[actions["action_type"] == "shot"]

    state.hm_pass_bubbles = _render_bubble_map(pass_actions, "Pass Distribution", "Blues", "Pass Count")
    state.hm_shot_bubbles = _render_bubble_map(shot_actions, "Shot Distribution", "OrRd", "Shot Count")
    state.hm_pass_focus = _render_bubble_focus_map(pass_actions, "Pass Hotspots (Top 5)", "Blues", "Pass Count")
    state.hm_shot_focus = _render_bubble_focus_map(shot_actions, "Shot Hotspots (Top 5)", "OrRd", "Shot Count")

    state.hm_data_freshness = fetch_data_freshness()

    logger.info("Heat map rendered: %s total actions", metrics["total"])
```

- [ ] **Step 4: Syntax check + run any relevant tests**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python -c "import ast; ast.parse(open('src/state/heat_map.py').read()); print('OK')"
```

Expected: `OK`.

---

## Task 9: Update `pages/heat_map.py`

**Files:**
- Modify: `hf_taipy_app/src/pages/heat_map.py`

- [ ] **Step 1: Update `page_config` — scope_vars + alt_var on each image block**

In `hf_taipy_app/src/pages/heat_map.py`, replace the entire `page_config` assignment with:

```python
page_config = PageConfig(
    title="Heat Map",
    icon="local_fire_department",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Action density visualization using bin statistics. "
        "Spatial analysis approach per Anzer & Bauer (2021) "
        '"A goal scoring probability model based on tracking data." '
        "Rendered via mplsoccer."
    ),
    citations=[
        Citation("Anzer & Bauer (2021)", "https://doi.org/10.1007/s10994-021-06011-5"),
        Citation("mplsoccer", "https://mplsoccer.readthedocs.io/"),
    ],
    content=[
        ContentRow(
            [
                ContentBlock("image", "hm_pass_bubbles", alt_var="hm_pass_bubbles_alt"),
                ContentBlock("image", "hm_shot_bubbles", alt_var="hm_shot_bubbles_alt"),
            ],
            columns=2,
            condition="len(hm_pass_bubbles) > 0",
        ),
        ContentRow(
            [
                ContentBlock("image", "hm_pass_focus", alt_var="hm_pass_focus_alt"),
                ContentBlock("image", "hm_shot_focus", alt_var="hm_shot_focus_alt"),
            ],
            columns=2,
            condition="len(hm_pass_focus) > 0",
        ),
    ],
    empty_message="Select a competition to begin.",
    empty_condition="len(hm_pass_bubbles) == 0 and len(competition_lov) > 0",
    warning_var="hm_warning_text",
    scope_vars=["hm_scope_label", "hm_scope_coverage"],
    freshness_var="hm_data_freshness",
    metrics=[
        Metric(
            "Total Actions",
            "hm_total",
            "Total number of on-ball actions (passes, shots, dribbles, etc.) in the selected scope.",
        ),
        Metric("Passes", "hm_passes", "Number of pass actions in the selected scope."),
        Metric("Shots", "hm_shots", "Number of shot actions in the selected scope."),
    ],
)
page_md = build_page(page_config)
```

- [ ] **Step 2: Syntax check**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python -c "import ast; ast.parse(open('src/pages/heat_map.py').read()); print('OK')"
```

Expected: `OK`.

---

## Task 10: Update `state/match_summary.py::ms_refresh`

**Files:**
- Modify: `hf_taipy_app/src/state/match_summary.py`

- [ ] **Step 1: Update imports**

At the top of `hf_taipy_app/src/state/match_summary.py`, replace the existing `from filters import ...` line with:

```python
from filters import build_scope_label, build_warning, fetch_data_freshness
```

(`fetch_scope_label` is dropped — this module no longer calls it. The function stays in `filters.py` for the 7 other Tier A pages that haven't migrated yet.)

Also add `_ALL_LABEL` to the `state.shared` import:

```python
from state.shared import _ALL_LABEL, get_comp_id, get_match_id, register_page_refresher
```

- [ ] **Step 2: Add new exported state variables**

In the "Exported state variables" section:

```python
ms_shooting_chart_alt: str = ""
ms_passing_chart_alt: str = ""
ms_possession_chart_alt: str = ""
ms_ppda_chart_alt: str = ""
```

Extend `__all__` with these 4 names.

- [ ] **Step 3: Update `ms_refresh` body**

Replace the scope-label resolution block in `ms_refresh`. Find:

```python
    # Scope label
    if comp_id is not None:
        state.ms_scope_label = fetch_scope_label(comp_id, None)
    else:
        state.ms_scope_label = ""
```

Replace with:

```python
    # Scope label — canonical build_scope_label with the 3 dimensions MS filters by
    comp_label = state.selected_competition or ""
    team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    match_label = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "—"
    scope_pairs = [
        ("Competition", comp_label),
        ("Team", team_label),
        ("Match", match_label),
    ]
    state.ms_scope_label = build_scope_label(scope_pairs)
    scope_plain = build_scope_label(scope_pairs, plain=True)
```

Also find the warning-setting block:

```python
        state.ms_warning_text = "No match data for this selection. Try choosing a different competition or match."
```

Replace with:

```python
        state.ms_warning_text = build_warning(
            domain="match data",
            suggestions=["choosing a different match"],
        )
```

And add the 4 alt-var assignments immediately after the successful-load `state.ms_warning_text = ""` line (around line 152):

```python
    # Alt strings for each of the 4 charts
    state.ms_shooting_chart_alt = f"Shooting — {scope_plain}"
    state.ms_passing_chart_alt = f"Passing — {scope_plain}"
    state.ms_possession_chart_alt = f"Possession — {scope_plain}"
    state.ms_ppda_chart_alt = f"Pressing (PPDA) — {scope_plain}"
```

Extend the clear-state-when-no-match branch (around line 108) to clear the new alt vars too:

```python
        state.ms_shooting_chart_alt = ""
        state.ms_passing_chart_alt = ""
        state.ms_possession_chart_alt = ""
        state.ms_ppda_chart_alt = ""
```

(Place these alongside the existing `state.ms_shooting_chart = ""` lines in both clear-state branches — the no-match branch and the empty-match-data branch.)

- [ ] **Step 4: Syntax check**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python -c "import ast; ast.parse(open('src/state/match_summary.py').read()); print('OK')"
```

Expected: `OK`.

---

## Task 11: Update `pages/match_summary.py`

**Files:**
- Modify: `hf_taipy_app/src/pages/match_summary.py`

- [ ] **Step 1: Add `alt_var` on each image block**

In `hf_taipy_app/src/pages/match_summary.py`, replace the `content=[...]` list with:

```python
    content=[
        ContentRow(
            [
                ContentBlock("image", "ms_shooting_chart", alt_var="ms_shooting_chart_alt"),
                ContentBlock("image", "ms_passing_chart", alt_var="ms_passing_chart_alt"),
            ],
            columns=2,
            condition="len(ms_home_name) > 0",
        ),
        ContentRow(
            [
                ContentBlock("image", "ms_possession_chart", alt_var="ms_possession_chart_alt"),
                ContentBlock(
                    "image",
                    "ms_ppda_chart",
                    alt_var="ms_ppda_chart_alt",
                    caption="PPDA: Passes Per Defensive Action. Under 10 = aggressive pressing, over 15 = passive.",
                ),
            ],
            columns=2,
            condition="len(ms_home_name) > 0",
        ),
    ],
```

(No other changes to the file. `scope_vars=["ms_scope_label", "ms_league_averages"]` stays as-is.)

- [ ] **Step 2: Syntax check**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python -c "import ast; ast.parse(open('src/pages/match_summary.py').read()); print('OK')"
```

Expected: `OK`.

---

## Task 12: Migrate widget `(optional)` markers in `template.py`

**Files:**
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Migrate 4 widgets to `required=False` with bare label**

In `hf_taipy_app/src/template.py`, locate and update each of these 4 `SidebarWidget(...)` entries inside `_FILTER_WIDGETS`:

Find (around line 394):
```python
    SidebarWidget(
        "dropdown",
        "selected_player",
        "Player (optional)",
        "on_player_change",
        condition=f"current_page in {_PLAYER_PAGES}",
        lov="player_lov",
        depends_on="selected_competition",
        help="Optional player filter. Leave blank to see all players in the competition.",
    ),
```

Replace with:
```python
    SidebarWidget(
        "dropdown",
        "selected_player",
        "Player",
        "on_player_change",
        condition=f"current_page in {_PLAYER_PAGES}",
        lov="player_lov",
        depends_on="selected_competition",
        required=False,
        filterable=True,
        help="Optional player filter. Leave blank to see all players in the competition.",
    ),
```

(Note: we're also adding `filterable=True` here as part of the typeahead fix from the audit — Finding #4 in the spec. This closes Critical finding "dropdown with up to 500 items lacks `filterable=True`".)

Find (around line 577):
```python
    SidebarWidget(
        "dropdown",
        "pt_selected_team",
        "Team (optional)",
        "pt_on_team_change",
        condition=f"current_page in {_PASS_TIMING_PAGES}",
        lov="pt_team_lov",
        depends_on="pt_selected_match",
        help="Filter to a specific team's passes. Leave blank for all.",
    ),
```

Replace with:
```python
    SidebarWidget(
        "dropdown",
        "pt_selected_team",
        "Team",
        "pt_on_team_change",
        condition=f"current_page in {_PASS_TIMING_PAGES}",
        lov="pt_team_lov",
        depends_on="pt_selected_match",
        required=False,
        help="Filter to a specific team's passes. Leave blank for all.",
    ),
```

Find (around line 586):
```python
    SidebarWidget(
        "dropdown",
        "pt_selected_player",
        "Player (optional)",
        "pt_on_player_change",
        condition=f"current_page in {_PASS_TIMING_PAGES}",
        lov="pt_player_lov",
        depends_on="pt_selected_match",
        help="Filter to a specific player's passes. Leave blank for all.",
    ),
```

Replace with:
```python
    SidebarWidget(
        "dropdown",
        "pt_selected_player",
        "Player",
        "pt_on_player_change",
        condition=f"current_page in {_PASS_TIMING_PAGES}",
        lov="pt_player_lov",
        depends_on="pt_selected_match",
        required=False,
        filterable=True,
        help="Filter to a specific player's passes. Leave blank for all.",
    ),
```

Find (around line 649):
```python
    SidebarWidget(
        "dropdown",
        "dv_selected_team",
        "Team (optional)",
        "dv_on_team_change",
        condition=f"current_page in {_DEFCON_PAGES}",
        lov="dv_team_lov",
        depends_on="dv_selected_comp",
        help="Filter rankings or breakdown to a specific team.",
    ),
```

Replace with:
```python
    SidebarWidget(
        "dropdown",
        "dv_selected_team",
        "Team",
        "dv_on_team_change",
        condition=f"current_page in {_DEFCON_PAGES}",
        lov="dv_team_lov",
        depends_on="dv_selected_comp",
        required=False,
        help="Filter rankings or breakdown to a specific team.",
    ),
```

- [ ] **Step 2: Add typeahead to the remaining 4 sensitive dropdowns**

Apply `filterable=True` (not the `required` change — just typeahead, from audit Finding #4) to:

In the `gk_selected_player` widget (around line 466):
```python
    SidebarWidget(
        "dropdown",
        "gk_selected_player",
        "Goalkeeper",
        "gk_on_gk_player_change",
        condition=f"current_page in {_GK_PAGES} and selected_sub_view != 'Rankings'",
        lov="gk_player_lov",
        depends_on="selected_competition",
        filterable=True,
        help="Select a goalkeeper to view their shot stopping and distribution. Only goalkeepers with GK stats are listed.",
    ),
```

In each of `dv_selected_breakdown_player` (around line 658), `dv_selected_timeline_player` (around line 670), `tac_selected_player` (around line 876), and `tac_selected_compare_player` (around line 886) — add `filterable=True` as a keyword argument inside the existing `SidebarWidget(...)` call, preserving all other fields. Example delta for one widget:

```python
# Before
    SidebarWidget(
        "dropdown",
        "dv_selected_breakdown_player",
        "Player",
        "dv_on_breakdown_player_change",
        condition=f"current_page in {_DEFCON_PAGES}",
        lov="dv_breakdown_player_lov",
        depends_on="dv_current_view",
        depends_value="Breakdown",
        depends_lov_populated=True,
        help="Select a player to see their DEFCON credit breakdown...",
    ),
# After — just one new kwarg
    SidebarWidget(
        "dropdown",
        "dv_selected_breakdown_player",
        "Player",
        "dv_on_breakdown_player_change",
        condition=f"current_page in {_DEFCON_PAGES}",
        lov="dv_breakdown_player_lov",
        depends_on="dv_current_view",
        depends_value="Breakdown",
        depends_lov_populated=True,
        filterable=True,
        help="Select a player to see their DEFCON credit breakdown...",
    ),
```

- [ ] **Step 3: Add 3 Heat Map glossary terms**

In `hf_taipy_app/src/template.py`, in the `GLOSSARY` dict, add the following keys (alphabetically or at the end of the dict, consistent with existing style):

```python
    "Bubble Map": (
        "Action density visualization where bubble area at each pitch zone represents "
        "the number of actions. Spatial binning approach per Anzer & Bauer (2021)."
    ),
    "Hotspot": (
        "A pitch zone with the highest action density. Heat Map highlights the top 5 zones "
        "with gold rings in the 'Hotspots' view."
    ),
    "Action Type": (
        "The kind of on-ball action — pass, shot, dribble, etc. Heat Map separates passes "
        "and shots into distinct panels."
    ),
```

And update `PAGE_TERMS["Heat-Map"]`:

```python
    "Heat-Map": ["Bubble Map", "Hotspot", "Action Type"],
```

- [ ] **Step 4: Syntax check**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python -c "import ast; ast.parse(open('src/template.py').read()); print('OK')"
```

Expected: `OK`.

---

## Task 13: Write Tier A canonical contract test

**Files:**
- Create: `hf_taipy_app/src/test_tier_a_canon.py`

- [ ] **Step 1: Write the contract test**

Create `hf_taipy_app/src/test_tier_a_canon.py`:

```python
"""Tier A page canon contract test.

Asserts the canonical patterns on the set of Tier A pages that have been
migrated (MIGRATED_TIER_A). Add a page to the set once its migration PR
is merged. Prevents the 'partial pattern application' consistency violation
from reopening.
"""

from __future__ import annotations

from pages.heat_map import page_config as heat_map_config
from pages.match_summary import page_config as match_summary_config
from template import GLOSSARY, PAGE_TERMS

MIGRATED_TIER_A: dict[str, object] = {
    "Heat-Map": heat_map_config,
    "Match-Summary": match_summary_config,
}


def test_migrated_tier_a_page_has_non_empty_glossary() -> None:
    """Every migrated Tier A page has at least one domain term in PAGE_TERMS."""
    for page_key in MIGRATED_TIER_A:
        terms = PAGE_TERMS.get(page_key, [])
        assert terms, f"PAGE_TERMS[{page_key!r}] is empty; Tier A migration requires at least one term"
        for t in terms:
            assert t in GLOSSARY, f"PAGE_TERMS[{page_key!r}] references undefined GLOSSARY key {t!r}"


def test_migrated_tier_a_page_scope_vars_first_entry_is_scope_label() -> None:
    """scope_vars[0] must follow the {prefix}_scope_label naming convention."""
    for page_key, cfg in MIGRATED_TIER_A.items():
        scope_vars = getattr(cfg, "scope_vars", [])
        assert scope_vars, f"{page_key} has no scope_vars"
        first = scope_vars[0]
        assert first.endswith("_scope_label"), (
            f"{page_key} scope_vars[0]={first!r} must end with '_scope_label'"
        )


def test_migrated_tier_a_page_content_blocks_have_alt_var() -> None:
    """Every image ContentBlock on a migrated page must have alt_var set."""
    for page_key, cfg in MIGRATED_TIER_A.items():
        for row in getattr(cfg, "content", []):
            for block in row.blocks:
                if block.kind == "image":
                    assert block.alt_var, (
                        f"{page_key}: image ContentBlock with var={block.var!r} has empty alt_var"
                    )


def test_migrated_tier_a_page_uses_build_scope_label_shape() -> None:
    """Sanity: the page has warning_var wired (its refresh uses build_warning)."""
    for page_key, cfg in MIGRATED_TIER_A.items():
        assert getattr(cfg, "warning_var", ""), f"{page_key} has no warning_var"
```

- [ ] **Step 2: Run the test**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/test_tier_a_canon.py -v
```

Expected: all 4 tests pass.

If a test fails: investigate — it means one of the preceding tasks missed a step. Don't rubber-stamp by adding to `MIGRATED_TIER_A`.

---

## Task 14: Puppeteer integration test for the lightbox (optional)

**Files:**
- Create: `hf_taipy_app/src/test_lightbox.py`

**Note on scope:** This test requires a running local Taipy server + `mcp__puppeteer` tool availability. If Puppeteer is unavailable or Taipy cannot be launched in CI, skip this task — the unit tests + manual checklist in Task 16 cover the main risks. Mark this task `- [ ] SKIP — deferred to follow-up` if Puppeteer is unavailable.

- [ ] **Step 1: Start local Taipy server in background**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python src/main.py
```

Run in background via `run_in_background: true`. Wait for "Running on http://0.0.0.0:7860" in the output.

- [ ] **Step 2: Write the Puppeteer-driven test scenario**

Create `hf_taipy_app/src/test_lightbox.py` — a pytest file that uses `mcp__puppeteer` through the Claude Code tool system. Because this pattern is not yet established in the repo, implement it as a manual test harness with a clear scenario document:

```python
"""Manual / MCP Puppeteer scenario for the lightbox rewrite.

This file documents the steps to execute via mcp__puppeteer when running
the Heat Map UI cycle verification. It is NOT a standalone pytest test
because the puppeteer MCP tool operates at the Claude-tool layer, not
the pytest layer. It is here as the canonical checklist.

Steps (execute via mcp__puppeteer in order):

1. puppeteer_navigate to http://localhost:7860/Heat-Map
2. Wait for .ll-page-scope to appear — evaluate document.querySelectorAll('.ll-page-scope').length === 1
3. Click any .ll-content-row img — expect .ll-lightbox-overlay to appear
4. Evaluate:
     const overlay = document.querySelector('.ll-lightbox-overlay');
     overlay.getAttribute('role') === 'dialog' &&
     overlay.getAttribute('aria-modal') === 'true' &&
     overlay.querySelector('figure') !== null &&
     overlay.querySelector('figcaption') !== null
   Expect true.
5. Evaluate document.querySelector('.ll-lightbox-caption').textContent includes the current competition label.
6. Send Escape key — expect overlay to be removed.
7. Repeat click → verify overlay reappears.
8. Tab key inside overlay — focus must not leave the overlay.
9. Close overlay via close button click — expect focus returns to original image.
10. Change filter (competition), click a different image → figcaption reflects new scope.
"""
```

This is the minimum canonical scenario. Actual execution happens during Task 16 manual verification. If/when a Puppeteer pytest harness is added to the repo, this scenario can be ported.

---

## Task 15: Create roadmap doc

**Files:**
- Create: `docs/ui-cycles/ui-consistency-roadmap.md`

- [ ] **Step 1: Create the directory and the file**

```bash
mkdir -p D:/Development/karstenskyt__luxury-lakehouse-d32/docs/ui-cycles
```

Create `docs/ui-cycles/ui-consistency-roadmap.md`:

```markdown
# UI consistency roadmap — Tier A pages

Living tracker of UI consistency findings across Tier A (StatsBomb event) pages. Rows are added when audits surface new issues, and deleted when the fix lands. `git log` is the audit trail.

**Tier A pages (9):** Heat-Map, Match-Summary, Shot-Map, Pass-Map, Pass-Network, Player-Impact, Player-Comparison, Goalkeeper-Analytics, Conversion-Funnel.

**Initial population:** 2026-04-17, from the cognitive-interface audit at the start of branch `ui/heat-map-context-and-filters`.

**How to use this file:**

- Fixed items get DELETED (not struck through) — the spec rule is to keep the file forward-looking.
- Each row lists a target PR or describes why it is deferred.
- Severity labels (Critical/High/Medium/Low) follow the cognitive-interface-audit rubric.

## Migration ripple — Tier A pages pending adoption of the canon

Each page below needs a mechanical follow-up PR that:
1. Migrates its refresh callback from `fetch_scope_label` to `build_scope_label`.
2. Marks shared filter widgets `required=False` where they are optional (requires taking the shared widget or cloning it per-page).
3. Uses `build_warning` for the warning state.
4. Adds `alt_var` to each `ContentBlock` image.
5. Populates or explains empty `PAGE_TERMS` entry.
6. Adds itself to `MIGRATED_TIER_A` in `test_tier_a_canon.py`.

| Page | Target PR | Notes |
|------|-----------|-------|
| Shot-Map | — | Open |
| Pass-Map | — | Open |
| Pass-Network | — | Open |
| Player-Impact | — | Open; multi-view page, inspect each sub-view |
| Player-Comparison | — | Open; multi-player radar, scope needs all selected players |
| Goalkeeper-Analytics | — | Open; coverage-aware Team filter complicates scope labelling |
| Conversion-Funnel | — | Open; currently disabled at the `main.py` registry — skip until re-enabled |

When all 7 have migrated, `fetch_scope_label` can be deleted from `filters.py`.

## Deferred findings — from 2026-04-17 cognitive audit

### Critical

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 5 | Lightbox `<script>` injected via Flask `after_request`; no CSP header | `hf_taipy_app/src/main.py:154-177` | Security-adjacent | Separate security branch |

### High

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 10 | Match Summary `depends_on="selected_team"` forces Team cascade even though `ms_refresh` only needs `match_id` | `hf_taipy_app/src/template.py` | Gulf of Execution | `ui/match-summary-cascade-decouple` (new branch) |
| 11 | Broad `except Exception:` in refresh callbacks — ADR-002 | `hf_taipy_app/src/state/heat_map.py:281`, `hf_taipy_app/src/state/match_summary.py:249` | ADR-002 | Separate observability cleanup branch |

### Medium

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 19 | `.ll-spin` animation has no `prefers-reduced-motion` guard | `hf_taipy_app/src/style_v2.css:704-712` | WCAG 2.3.3 | `ui/a11y-sweep` (future) |
| 21 | No deep linking / URL-encoded filter state | app-wide | Pirolli & Card between-patch | Feature-level branch, scope TBD |
| 22 | 4fr/1fr grid collapses below 768px | `hf_taipy_app/src/style_v2.css` | WCAG 1.4.10 | `ui/responsive-audit` (future) |
| 23 | Lightbox CSP surface | `hf_taipy_app/src/main.py:172-177` | Security-adjacent | Linked to #5 |

### Low

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 24 | `_TOP_N_LABELS = 25` may cause label collisions on dense 96-bin grid | `hf_taipy_app/src/state/heat_map.py:64` | Cleveland/McGill | Low-priority polish |
| 25 | No programmatic CVD audit with `colorspacious` | CI | Olson & Brewer 1997 | Tooling investment |
| 26 | Redundant `matplotlib.use("Agg")` in `state/heat_map.py` | `hf_taipy_app/src/state/heat_map.py:24` | Code quality | Cleanup |
| 28 | Dead-code fallback `m.get("home_team_name", "Home")` | `hf_taipy_app/src/state/match_summary.py:155-156` | Code quality | Cleanup |

## Last updated

2026-04-17 — initial population, concurrent with branch `ui/heat-map-context-and-filters`.
```

- [ ] **Step 2: Verify the file exists and is well-formed Markdown**

```bash
test -f D:/Development/karstenskyt__luxury-lakehouse-d32/docs/ui-cycles/ui-consistency-roadmap.md && echo "OK"
```

Expected: `OK`.

---

## Task 16: Full test suite + lint + manual verification

**Files:** none modified; verification only.

- [ ] **Step 1: Run all tests under `hf_taipy_app/src/`**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run pytest src/ -v
```

Expected: all new test files (5 from this plan) pass; existing `test_admin_api.py` and `test_render.py` pass unchanged.

- [ ] **Step 2: Run Ruff lint**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32
uv run ruff check hf_taipy_app/src/
```

Expected: zero new violations.

- [ ] **Step 3: Run Pyright**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32
uv run pyright hf_taipy_app/src/
```

Expected: existing warnings tolerated; no NEW errors.

- [ ] **Step 4: Launch local Taipy and run the manual checklist from spec §8.7**

Launch Taipy in background (run_in_background: true):

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32/hf_taipy_app
uv run python src/main.py
```

Wait for "Running on http://0.0.0.0:7860". Then verify in a browser:

1. Navigate to `/Heat-Map`.
2. Select Competition = "England — Premier League". Verify scope line shows `COMPETITION England — Premier League │ TEAM All teams │ PLAYER All players` with small-caps labels and CSS-divider separators.
3. Select a Team. Verify scope line updates: `... TEAM Aston Villa ...`. Verify sidebar Team label is plain "Team" (required); Player label is "Player (optional)".
4. Click the player dropdown — **typeahead should be active** (type a letter and see filter).
5. Select a Player. Verify scope line updates.
6. Click the Pass Distribution image — verify overlay opens with `<figure>` + `<figcaption>` showing the current scope.
7. Press Escape — verify overlay closes and focus returns to the image.
8. Tab through — verify focus-visible outline on images.
9. Click image, then click overlay background (not image) — verify overlay closes.
10. Click image, then click the × close button — verify overlay closes.
11. Clear all filters, select a filter combination that returns no data. Verify warning reads: `"No actions found for this selection. Try removing the team filter or choosing a different player."`
12. Navigate to `/Match-Summary`. Repeat steps 1-10 for that page (scope has 3 dimensions: Competition, Team, Match). Match Summary warning should read `"No match data found for this selection. Try choosing a different match."`
13. Visually compare the 8 images against a pre-branch screenshot — they must not change (matplotlib output unchanged by this branch).

If any step fails: investigate, fix, re-run from step 1. Do not proceed to commit.

---

## Task 17: Commit gate — user approval required

**Files:** none modified. This task documents the commit boundary.

- [ ] **Step 1: Summarize the diff**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32
git status --short
git diff --stat
```

- [ ] **Step 2: Present the summary to the user and ASK for explicit commit approval**

Do NOT auto-commit. Per repo rule and user standing instruction, every commit requires separate explicit approval. Present:

- The file change list (`git status --short`)
- Line counts per file (`git diff --stat`)
- A proposed single-commit message
- Confirm all tests + lint + manual checklist passed

Wait for the user's explicit "commit approved" or equivalent. On approval:

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32
git add hf_taipy_app/src/filters.py hf_taipy_app/src/page_template.py hf_taipy_app/src/template.py hf_taipy_app/src/style_v2.css hf_taipy_app/src/main.py hf_taipy_app/src/pages/heat_map.py hf_taipy_app/src/pages/match_summary.py hf_taipy_app/src/state/heat_map.py hf_taipy_app/src/state/match_summary.py hf_taipy_app/src/test_scope_label.py hf_taipy_app/src/test_build_warning.py hf_taipy_app/src/test_sidebar_widget_marker.py hf_taipy_app/src/test_content_block_alt.py hf_taipy_app/src/test_tier_a_canon.py hf_taipy_app/src/test_lightbox.py docs/ui-cycles/ui-consistency-roadmap.md docs/superpowers/specs/2026-04-17-heat-map-ui-cycle-design.md docs/superpowers/plans/2026-04-17-heat-map-ui-cycle.md

git commit -m "$(cat <<'EOF'
feat(ui): Heat Map + Match Summary consistency cycle

Establishes canonical template patterns for context clarity, filter
labelling, and accessible image overlay on Tier A event pages. Heat
Map and Match Summary adopt the patterns in this branch; follow-up
PRs migrate the remaining 7 Tier A pages.

Closes audit findings 1-4 (Critical), 6-9 and 12-13 (High), 15-17 and
20 (Medium) from the 2026-04-17 cognitive-interface audit.

Key changes:
- filters.py: new build_scope_label + build_warning helpers
- page_template.py: SidebarWidget.required + ContentBlock.alt_var
- template.py: 4 widgets migrated to required=False; 7 widgets gain
  filterable=True for typeahead; Heat Map glossary populated
- main.py: lightbox JS rewrite (Escape + close button + focus trap +
  ARIA dialog + figcaption from .ll-page-scope)
- style_v2.css: .ll-sep, .ll-scope-*, .ll-lightbox-caption, .ll-lightbox-close
- pages/heat_map.py + state/heat_map.py: wire canon
- pages/match_summary.py + state/match_summary.py: wire canon
- docs/ui-cycles/ui-consistency-roadmap.md: new deferred-findings tracker
- Tests: 5 new unit/contract test files

Design spec: docs/superpowers/specs/2026-04-17-heat-map-ui-cycle-design.md
Plan: docs/superpowers/plans/2026-04-17-heat-map-ui-cycle.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Run git status to verify the commit**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse-d32
git status --short
git log --oneline -5
```

Expected: clean working tree; top commit is the one just created.

Do **NOT** push to remote. Push requires a separate explicit user approval.

---

## Completion criteria

Branch is done when:

- [ ] All 16 preceding tasks complete.
- [ ] All 5 new test files pass.
- [ ] Ruff + Pyright clean (no new violations).
- [ ] Manual checklist in Task 16 Step 4 all green.
- [ ] Roadmap doc committed.
- [ ] Single squash-friendly commit created with explicit user approval.
- [ ] Working tree clean, branch ready for user-approved PR.
