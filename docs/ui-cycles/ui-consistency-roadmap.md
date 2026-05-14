# UI consistency roadmap — Tier A pages

Living tracker of UI consistency findings across Tier A (StatsBomb event) pages. Rows are added when audits surface new issues, and deleted when the fix lands. `git log` is the audit trail.

**Tier A pages (9):** Heat-Map, Match-Summary, Shot-Map, Pass-Map, Pass-Network, Player-Impact, Player-Comparison, Goalkeeper-Analytics, Conversion-Funnel.

**Initial population:** 2026-04-17, from the cognitive-interface audit at the start of branch `ui/heat-map-context-and-filters`.

**How to use this file:**

- Fixed items get DELETED (not struck through) — the spec rule is to keep the file forward-looking.
- Each row lists a target PR or describes why it is deferred.
- Severity labels (Critical/High/Medium/Low) follow the cognitive-interface-audit rubric.

## Migration ripple — Tier A pages pending adoption of the canon

All 9 Tier A pages now use the `scope_dims` + `alt_var` + `build_warning` canon. Migration was completed on branch `ui/heat-map-context-and-filters` (2026-04-17/18) and verified live on staging against the 6-item matrix (labels, scope line, image alt, lightbox, CSS, backend search where applicable). See `test_tier_a_canon.py::MIGRATED_TIER_A` for the authoritative list.

`fetch_scope_label` in `filters.py` is now dead code — delete whenever the cycle commits.

## Deferred findings — from 2026-04-17 cognitive audit

### Critical

(None remaining — #5 lightbox CSP shipped 2026-05-12 in UI/UX bundle PR.)

### High

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 32 | `selected_players_multi` (Player Comparison) has the same "load all 500 players into the browser" anti-pattern as the 8 single-select player dropdowns fixed in this cycle. Client-side filter only — no backend search input. With a 500-player LOV and multi-select UX, users either scroll a long list or mentally precompute an alpha prefix. Backend search for multi-select is more complex (need to preserve already-selected players across re-queries, avoid dropping them when the search narrows) so it was scoped out of the single-select cycle. | `hf_taipy_app/src/template.py:438`, `hf_taipy_app/src/state/shared.py` (`player_lov_multi`) | Pirolli & Card information foraging | `ui/multi-select-backend-search` (new branch) |

### Medium

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 21 | No deep linking / URL-encoded filter state | app-wide | Pirolli & Card between-patch | Feature-level branch, scope TBD |

### Low

(None remaining — #24 not a real issue on the regular 12×8 grid; #28 dead-code fallback removed in UI/UX bundle PR.)

## Last updated

2026-05-14 — UI-2 bundle shipped findings #10 (conditional match reset), #31 (match_lov_required for required pages), plus slider hardening (GK tab gating, state-preservation, change_delay). Remaining: #21, #32.
