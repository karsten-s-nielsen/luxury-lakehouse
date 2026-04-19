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

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 5 | Lightbox `<script>` injected via Flask `after_request`; no CSP header | `hf_taipy_app/src/main.py:154-177` | Security-adjacent | Separate security branch |

### High

| # | Finding | Files | Framework | Target PR |
|---|---------|-------|-----------|-----------|
| 10 | Match Summary `depends_on="selected_team"` forces Team cascade even though `ms_refresh` only needs `match_id` | `hf_taipy_app/src/template.py` | Gulf of Execution | `ui/match-summary-cascade-decouple` (new branch) |
| 11 | Broad `except Exception:` in refresh callbacks — ADR-002 | `hf_taipy_app/src/state/heat_map.py:281`, `hf_taipy_app/src/state/match_summary.py:249` | ADR-002 | Separate observability cleanup branch |
| 31 | Shared `selected_match` widget lacks per-page optionality posture. The `match_lov` starts with `All`, so picking `All` is a valid state, but functionally Pass-Map/Pass-Network/Match-Summary produce empty/partial states without a specific match_id, while Conversion-Funnel now (D58) computes cross-match aggregates from `fct_funnel_stages_agg` and IS genuinely optional. Adding `required=False` to the shared widget would incorrectly tag 4 pages as optional; leaving it blank tags Conversion-Funnel as required. Same architectural problem PA1 hit with `selected_game_state`. Observed on staging matrix 2026-04-18. | `hf_taipy_app/src/template.py:399` | Gulf of Execution | `ui/match-widget-per-page-optionality` (new branch — requires either cloning the widget per page OR extending `SidebarWidget` with per-page required overrides) |
| 32 | `selected_players_multi` (Player Comparison) has the same "load all 500 players into the browser" anti-pattern as the 8 single-select player dropdowns fixed in this cycle. Client-side filter only — no backend search input. With a 500-player LOV and multi-select UX, users either scroll a long list or mentally precompute an alpha prefix. Backend search for multi-select is more complex (need to preserve already-selected players across re-queries, avoid dropping them when the search narrows) so it was scoped out of the single-select cycle. | `hf_taipy_app/src/template.py:438`, `hf_taipy_app/src/state/shared.py` (`player_lov_multi`) | Pirolli & Card information foraging | `ui/multi-select-backend-search` (new branch) |

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

2026-04-18 — staging matrix run against the full Heat Map UI cycle (8 player-dropdown backend-search migration + scope_dims canon rollout). Findings #31 (Match widget per-page optionality) and #32 (Player-Comparison multi-select backend search) added from the live audit. Prior findings #10–#28 unchanged. Fix 1+2 from the audit (`cf_selected_game_state` + `gk_selected_team` → `required=False`) landed in this cycle's working tree. 9/9 Tier A pages confirmed migrated; `fetch_scope_label` is now dead code.
