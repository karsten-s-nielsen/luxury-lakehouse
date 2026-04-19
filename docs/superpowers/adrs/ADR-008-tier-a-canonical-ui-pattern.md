# ADR-008: Tier A canonical UI pattern (ScopeDim + MIGRATED_TIER_A contract)

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The Taipy application ships with 17 user-facing pages organised into three tiers by filter regime. The 9 Tier A pages (Heat-Map, Match-Summary, Shot-Map, Pass-Map, Pass-Network, Player-Impact, Player-Comparison, Goalkeeper-Analytics, Conversion-Funnel) all share the same `Competition → Team → Match → Player` filter cascade, render mplsoccer / matplotlib images, and go through the shared `_build_standard_page` template path. A cognitive-interface audit (2026-04-17) surfaced that every Tier A page had drifted in how it rendered the scope context line, empty states, warnings, filter labels, and image overlays — with each page hand-rolling its own version of the same concepts. The drift produced four Critical and eight High severity findings: the overlay was inaccessible (no Escape handler, no focus trap, no ARIA), the scope line never exposed the player filter, the middle-dot separator fell below WCAG contrast, and the player dropdown had lost its typeahead.

The forcing function was the Heat Map redesign branch (`ui/heat-map-context-and-filters`), whose goal was to make Heat Map the "flagship" — a first-time kiosk visitor must be able to answer "what am I looking at?" from the page alone and from any image overlay. A one-page fix would not prevent the same drift from reopening across the other 8 Tier A pages.

An initial design attempted to render the scope line as HTML injected via Taipy's `|text|raw|` flag, with helpers in `filters.py` emitting `<span class="ll-scope-dim-label">COMPETITION</span>England — Premier League<span class="ll-sep"></span>...`. Live testing revealed that Taipy 4.1's `|text|raw|` flag **escapes HTML entities** rather than emitting them — the `<span>` tags rendered as literal `&lt;span&gt;` text in the DOM. This forced a pivot to a Taipy-native pattern during implementation.

## Decision

Establish a single canonical UI contract for all Tier A pages, enforced by a contract test that each page opts into on migration.

**Structural primitives** (in `page_template.py`):

- `ScopeDim(label: str, value_var: str)` dataclass — each dimension the page filters by is declared as a ScopeDim; `label` is the static human-readable name (rendered as small-caps), `value_var` names the per-dimension state variable holding the current resolved value.
- `PageConfig.scope_dims: list[ScopeDim]` — canonical scope line, rendered above the content grid.
- `PageConfig.scope_vars: list[str]` — secondary plain-text scope lines (e.g. league averages), rendered below.
- `SidebarWidget.required: bool = True` — `False` auto-appends `" (optional)"` to the rendered dropdown label.
- `ContentBlock.alt_var: str = ""` — optional per-image state variable supplying scope-aware alt text; falls back to `page_title`.

**Rendering pattern** (in `_build_standard_page`):

- Emit one `<span class="ll-scope-dim-label">` + one `<span class="ll-scope-value">` per declared dimension via standard Taipy `|text|` bindings (no `|raw|`, no HTML injection).
- Separators between dimensions are drawn by a CSS pseudo-element (`.ll-scope-dim-label:not(:first-of-type)::before`) — zero separator markup in the emitted Taipy Markdown.
- The entire scope line sits inside a `.ll-page-scope` wrapper that the lightbox JS queries via `document.querySelector('.ll-page-scope')` and clones into the overlay figcaption.

**Migration contract** (in `test_tier_a_canon.py`):

- `MIGRATED_TIER_A: dict[str, PageConfig]` enumerates pages that have adopted the canon.
- Four tests assert: non-empty `PAGE_TERMS` glossary, non-empty `scope_dims` with labels and value_vars, `alt_var` set on every image `ContentBlock`, non-empty `warning_var`.
- Each follow-up migration PR adds its page to `MIGRATED_TIER_A`. Until a page is in the set, no contract is enforced on it — this is deliberate so that existing Tier A pages that have not yet migrated continue to pass CI.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. HTML injection via `|text|raw|` + `html.escape` on values | Single scope state var per page; helper-first design; minimal new dataclasses | Taipy 4.1 `|text|raw|` **escapes HTML entities** rather than emitting them (verified live at `/Heat-Map`, DOM contained literal `&lt;span&gt;COMPETITION`); the `|raw|` flag controls Markdown pre-processing, not HTML escaping | Blocked by framework behaviour |
| B. `RawHtml` content-provider iframes (same pattern as the Workflows DAG) | Existing pattern in the codebase; fully supports arbitrary HTML | Each scope line becomes a 60+ pixel iframe with its own document and CSS scope; cannot cascade page CSS into the iframe; the lightbox JS cannot clone iframe content cross-document | Heavy-handed for one-line inline text; breaks the lightbox figcaption hand-off |
| C. `ScopeDim` dataclass + per-dimension Taipy state vars + CSS pseudo-element separators (chosen) | Entirely Taipy-native; lightbox clones the whole DOM subtree cleanly; CSS controls separators and small-caps labels; adds zero iframes; each dim-value is a plain state var that Taipy debounces and dirty-tracks natively | Introduces 3–4 new state vars per page (one per filter dimension); page config gets a `scope_dims=[...]` block | — |
| D. Parallel hand-written scope line per page | Zero framework fight; maximum per-page flexibility | Preserves the exact drift the audit identified; no enforcement surface; every new page hand-rolls its own version | Defeats the purpose of the cycle |

## Consequences

### Positive

- All four Critical and eight High-severity audit findings closed on Heat Map and Match Summary.
- The `MIGRATED_TIER_A` contract gives the next 7 migration PRs a mechanical checklist and a CI gate that prevents regressions.
- The overlay figcaption cloning works with zero JS fight — the lightbox reads whatever is inside `.ll-page-scope` and moves it into the overlay unchanged, so separator styling, small-caps labels, and italic values are preserved without re-implementation.
- The separator is a CSS pseudo-element, so the transition from U+00B7 middle dot to a WCAG-compliant 1px vertical divider required zero changes to the page markdown. Future separator tweaks require a single CSS edit.
- `filterable=True` on the shared `selected_player` widget restored typeahead across all pages that render it, closing the player-dropdown scrollability finding.

### Negative

- Each migrated page declares 3–4 new state variables (one per filter dimension) instead of a single `{prefix}_scope_label` string. The Heat Map state module grew from ~330 to ~388 lines; Match Summary from ~271 to ~323 lines.
- The 7 remaining Tier A pages are now inconsistent with Heat Map and Match Summary until their migration PRs land. The roadmap doc in `docs/ui-cycles/ui-consistency-roadmap.md` tracks this.
- Documentation drift: the design spec (`docs/superpowers/specs/2026-04-17-heat-map-ui-cycle-design.md`) describes the Option A approach that was abandoned mid-flight. An amendment note has been added to the spec; the plan (`docs/superpowers/plans/2026-04-17-heat-map-ui-cycle.md`) retains its original step-by-step for audit-trail purposes.

### Neutral

- The pattern is a "flexible" canon, not a rigid one — a new Tier A page can opt out of the contract test temporarily by staying out of `MIGRATED_TIER_A`, which allows exploratory or transitional pages without fighting the gate.
- User item #1 from the audit ("Team is optional on Heat Map but required on Match Summary") is only partially resolved: the `selected_team` widget is **shared across all pages** that list it, so a single `required=False` would apply to every _TEAM_PAGE including Match Summary (where Team gates Match visibility via `depends_on`). Fully resolving item #1 requires either page-scoped widget cloning or decoupling Match Summary's cascade — both scope-creep for this branch and tracked in the roadmap.

## CLAUDE.md Amendment

None. The decision extends existing UI architecture rules in CLAUDE.md (template-first architecture, `Metric` requires `help_text`, etc.) without carving out any exceptions.

## Related

- **Specs:** `docs/superpowers/specs/2026-04-17-heat-map-ui-cycle-design.md`
- **Plans:** `docs/superpowers/plans/2026-04-17-heat-map-ui-cycle.md`
- **Roadmap:** `docs/ui-cycles/ui-consistency-roadmap.md`
- **Cognitive audit transcript:** 2026-04-17 (branch `ui/heat-map-context-and-filters`)
- **Framework reference:** Taipy 4.1.1 — `|text|raw|` flag escapes HTML; RawHtml content provider renders as iframe (`state/workflows_dag.py::RawHtml`)

## Notes

The `|raw|` escape behaviour was verified in the Puppeteer MCP browser against a live Taipy 4.1.1 server at `http://localhost:7860/Heat-Map`. The DOM returned:

```html
<span class="taipy-text ll-scope-label taipy-text-raw ll-scope-label-raw" aria-label="">
  &lt;span class="ll-scope-dim-label"&gt;COMPETITION&lt;/span&gt;England — Premier League...
</span>
```

— where `&lt;` / `&gt;` are the escaped `<` / `>` characters, rendered as literal text in the page. This is the evidence that forced the pivot from Option A to Option C.
