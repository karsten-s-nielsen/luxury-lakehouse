# Match Summary redesign — design spec

| | |
|---|---|
| **Date** | 2026-04-19 |
| **Branch** | `ui/match-summary-redesign` |
| **Author** | Karsten Nielsen (with Claude Opus 4.7) |
| **Status** | Draft — awaiting user review |
| **Supersedes** | — |
| **Related** | ADR-008 Tier A canonical UI pattern; `docs/superpowers/specs/2026-04-17-heat-map-ui-cycle-design.md`; `reference_silly_kicks_vaep` memory |

## 1. Goal

Replace the current Match Summary page — four grouped horizontal bar charts that present stats without hierarchy or narrative — with a scorecard-native design that delivers an editorial insight about each match ("what mattered, how it unfolded, why") in a form a visitor can read in ten seconds.

The current page fails Donnelly's insight test: a user who did not watch the match can read every bar and still not know *what happened*. The redesign corrects this by adopting the same "overview → focus" cognitive pattern that made the Heat Map redesign succeed, adapted from the spatial-coverage data shape of Heat Map to the temporal+event data shape of a match.

## 2. Scope

### In this branch

- Page-level changes in `hf_taipy_app/src/pages/match_summary.py` and `hf_taipy_app/src/state/match_summary.py`.
- New rendering helpers in the state module for: xG race chart, decisive-moment cards (Big Story + secondary), ranked delta table.
- New SQL queries (in `hf_taipy_app/src/queries/match.py` or a new adjacent module) to fetch VAEP top-N actions, shot-level xG timeline, red/yellow card events per match.
- Glossary additions for new domain terms (VAEP, xG race, Big Story, Our Verdict).
- `NOTICE` additions for the new Decroos and silly-kicks citations.
- Match Summary `PAGE_TERMS` entry updated in `hf_taipy_app/src/template.py`.
- Tests: unit tests for verdict-phrase derivation, moment-ranking logic, delta-table sorting; Puppeteer E2E test on the deployed staging Space for the full page render with a known-good match.

### Explicitly out of scope

- Click-through linking from moment cards to Shot Map / Pass Map / Pressing pages (deferred to v2 — see §15).
- Replacing `|xG Δ|` ranking with VAEP ranking on any *other* page (Shot Map etc. keep their current choices; this spec is Match Summary only).
- Extending the "Big Story" editorial pattern to other pages (wait for a second use case before elevating to cross-cutting — per pattern ledger, §6).
- Aggregated scope handling (competition-level or team-level summaries). Match Summary remains strictly single-match; scope filters exist only to help the user select a match. Aggregated summaries are a separate page concern if ever needed.
- ARCHITECTURE.md row on Anzer & Bauer (compound errors; requires separate investigation — unrelated to this redesign).

## 3. Approved design decisions

All six resolved during brainstorming on 2026-04-19:

| # | Decision | Choice | Brainstorm Q |
|---|----------|--------|--------------|
| 1 | Primary editorial takeaway | Decisive moments + narrative arc (ranked by impact, then timeline) | Q1 → B,D |
| 2 | Row ordering | D→B (moments on top, narrative below) | Q2 → Y |
| 3 | Layout family | Dashboard (`stats` top tiles + full-width content) instead of Standard (sidebar metrics) | Q3 → Q |
| 4 | Decisive-moment data source | `fct_action_values` ranked by `|vaep_value|`, with red cards auto-included | Q4 + scope revision |
| 5 | Narrative chart form | Combined stepped xG race + shot ticks + event markers (FBref-standard) | Q5 → C |
| 6 | Diagnostic (Row 3) form | Ranked delta table sorted by `|Δ|` (editorial voice over visual gap) | Q6 → C |

Five additional refinements approved during synthesis from research + chart-choice-audit:

| # | Refinement | Source |
|---|-----------|--------|
| R1 | Verdict vocabulary mapped to xG-delta thresholds (Our Verdict: *Fully merited / Fair result / Fortunate / Smash & grab / Flattered by scoreline*) | Agent 2 — The Athletic, Opta Analyst, Football365 |
| R2 | Row 1 "Big Story" hierarchy — 1 hero card + 2 secondary, visually weighted | Agent 2 — Opta Analyst convention |
| R3 | Row 2 richer annotations — goal icons + red-card markers + half-time divider separate from VAEP gold rings | Agent 1 — FBref dual-line standard |
| R4 | Row 3 as ranked delta **table** (chosen) over mirrored bar chart | Chart-choice-audit Kirk matrix + user decision |
| R5 | VAEP used for Row 1 ranking from v1 (not deferred); caveat scoped to "on-ball actions only" | Agent 3 — academic grounding; memory `reference_silly_kicks_vaep` |

## 4. Non-goals / acknowledged limitations

- **Off-ball runs and positioning are not valued.** VAEP values on-ball actions only. A defender's diagonal run that creates space for a teammate's pass receives no VAEP credit. This limitation is surfaced in the Row 1 caption.
- **Match Summary does not attempt season-arc context.** "What does this match mean for the season?" is a separate editorial angle that belongs on a Team page or a Standings page, not here.
- **The page does not rank non-shot defensive events** beyond what VAEP already captures (tackles, interceptions, pressures). A post-match disciplinary narrative (subs, tactical shifts, formation changes) requires human editorial judgement that `fct_action_values` cannot produce automatically.
- **League-average comparison lives in Row 3 only.** Not on the tile strip. Reason: tiles must remain at the 4-tile cap (discipline commitment from Q3); league-average context belongs as reference lines on per-stat rows.

## 5. Page structure

### 5.1 Layout

Dashboard layout (`_build_dashboard_page`), triggered by `stats: list[StatCard]` in `PageConfig`. Desktop structure top-to-bottom (mobile layout in §5.7):

```
┌─────────────────────────────────────────────────────────────┐
│ Scope line (Competition · Team · Match)                     │
├─────────────────────────────────────────────────────────────┤
│ [Tile 1] [Tile 2] [Tile 3] [Tile 4]                         │
│  Final    Home xG  Away xG  Our Verdict                     │
├─────────────────────────────────────────────────────────────┤
│ Row 1 — Big Story (hero card) + 2 secondary cards           │
├─────────────────────────────────────────────────────────────┤
│ Row 2 — xG race + shot ticks (full width)                   │
├─────────────────────────────────────────────────────────────┤
│ Row 3 — Ranked delta table (home vs away, sorted by |Δ|)    │
├─────────────────────────────────────────────────────────────┤
│ Data freshness + citations footer                           │
└─────────────────────────────────────────────────────────────┘
```

Empty state (no match selected): placeholder message in scroll wrapper. Warning state (data gap for a selected match): `build_warning(domain, suggestions)` box with ADR-002 rules.

### 5.2 Tile strip

Four `StatCard` entries in `PageConfig.stats`:

| Position | Label | Var | Value format | Help text (tooltip) |
|----------|-------|-----|--------------|---------------------|
| 1 | `Final` | `ms_final_score` | `"H — A"` (e.g. `"2 — 1"`) | "Full-time score. Home team listed first." |
| 2 | `Home xG` | `ms_home_xg` | `"H.HH"` (e.g. `"2.40"`) + delta indicator (+0.40 vs actual) | "Expected goals from shot locations and context. Delta shows goals minus xG: positive = overperformed, negative = underperformed." |
| 3 | `Away xG` | `ms_away_xg` | Same format as Home xG | Same help text |
| 4 | `Our Verdict` | `ms_verdict_phrase` | One phrase from the vocabulary table in §8 | "Editorial interpretation of whether the scoreline reflected the run of play, by xG margin. Hover any phrase for its xG threshold." |

Tile 4 uses `detail_var="ms_verdict_detail"` to surface the xG gap in the tile body (e.g. `"+1.6 xG gap"`).

### 5.3 Row 1 — Big Story + 2 secondary decisive actions

Full-width content row rendered as HTML via `ContentBlock("html", "ms_moments_html", height_var="ms_moments_height", container_class="ll-match-moments")`.

**Hero card** (top, 60-65% width): the single highest-|VAEP| action of the match. Fields:
- Minute + period (e.g. `23' · 1st half`)
- Player name + team badge/colour
- Action-type prose (e.g. `"Line-breaking pass into half-space"`, `"Shot from 8 yards"`, `"Tackle, possession regained"`)
- VAEP value (`"+0.18 VAEP"` with direction indicator)
- One-sentence editorial framing (derived from action type + result + outcome)

**Secondary cards** (below hero, 2 columns): the 2nd and 3rd highest-|VAEP| actions. Same fields, more compact.

**Red-card auto-inclusion**: if a red card was issued during the match, it is appended as an additional card (fourth card if hero + 2 secondary; otherwise takes the third slot). Red-card cards render with a distinct colour accent.

**Caption below Row 1**:
> "Decisive on-ball actions ranked by VAEP impact (Decroos et al. 2019, computed via silly-kicks). Off-ball runs and positioning are not valued."

**Design rationale**: the "Big Story" pattern is borrowed from Opta Analyst's match-report convention. The visual hierarchy (hero vs secondary) mirrors Heat Map's top-5 focus pattern where one bubble is bigger/brighter than the rest — cross-page consistency of the "this is the one that matters" visual grammar.

### 5.4 Row 2 — xG race + shot ticks + event markers

Rendered as a **Plotly** chart (not a matplotlib PNG) so that native hover tooltips work. Embedded via `ContentBlock("chart", "ms_xg_race_fig", chart_height="320px")`. This aligns with `conversion_funnel.py`'s pattern.

**Chart elements** (in z-order, back to front — Plotly trace order):

1. **Half-time divider** — Plotly `shape` (vertical line) at minute 45 (or actual half-time minute if stoppage extends it). Dashed, grey.
2. **Cumulative xG stepped traces** — one Plotly `Scatter(mode="lines", line_shape="hv")` per team, coloured `HOME_COLOR` and `AWAY_COLOR`. Each shot contributes a step up on its team's line. Y-axis is cumulative xG; x-axis is minutes 0–90+.
3. **Shot ticks** — thin vertical `Scatter(mode="markers", marker_symbol="line-ns-open")` or narrow bars at each shot's minute, below the xG lines. Sized proportionally to per-shot xG. Team-coloured.
4. **Gold decisive-action rings** — `Scatter(mode="markers", marker_symbol="circle-open", marker_size=14, marker_line_color=AMBER, marker_line_width=2, marker_color=<team>)` at each Row 1 decisive action's minute. Visual link to Row 1.
5. **Goal icons** — `Scatter(mode="markers", marker_symbol="star")` at each goal's minute on the corresponding team's cumulative xG line. Distinct from gold rings (a goal is always annotated; a gold ring marks VAEP decisiveness which may or may not coincide with a goal).
6. **Red-card markers** — `Scatter(mode="markers", marker_symbol="square-open")` with red colour at red-card minute(s), positioned on a dedicated annotation row below the x-axis or at chart top.

**Hover behaviour** (in-scope per user direction):

- **xG stepped line hover** — shows the minute, team name, cumulative xG at that point, and the shot that caused the last step (`"52' — Chelsea cumulative xG 1.45 (last shot: Palmer 52', xG 0.18)"`).
- **Shot tick hover** — shows minute, team, shooter, xG of that individual shot.
- **Gold ring hover** — shows the full Row 1 card content (minute, action type, player, VAEP value). This explicitly doubles up the Row 1 information so the chart is self-sufficient for a visitor who skimmed past the cards.
- **Goal icon hover** — `"{minute}' — {team} goal, {scorer}, xG of that shot: {xg}"`.
- **Red-card hover** — `"{minute}' — {team} red card, {player}"`.

Implemented via Plotly's `hovertemplate` per trace with pre-formatted strings built in the Python state module (no client-side JS).

**Legend** below chart: xG line colour key, tick explanation ("tall = high xG"), marker glossary. Plotly native legend with custom positioning.

**Alt text**: scope-aware `"xG race and decisive moments — {scope_plain}"` — set via `layout.title` plus an `aria-label` attribute if Taipy's chart widget surfaces it; otherwise adjacent caption provides the scope line.

### 5.5 Row 3 — Ranked delta table

Full-width table rendered as an HTML block via `ContentBlock("html", "ms_delta_table_html", height_var="ms_delta_table_height")`. Taipy's native table widget does not support the editorial row styling this design requires (gold-star accent on the top row, directional `↑`/`↓` arrows coloured by winner team, magnitude-sorted rows). HTML block gives full control. State variable is therefore `ms_delta_table_html: str`, produced by `_render_delta_table_html()`.

**Columns**:

| Column | Type | Notes |
|--------|------|-------|
| Dimension | string | Metric name (e.g. `"xG"`, `"Progressive passes"`, `"PPDA"`) |
| Home | numeric | Home value, coloured `HOME_COLOR` |
| Away | numeric | Away value, coloured `AWAY_COLOR` |
| Δ | numeric + direction arrow | Signed delta (home − away), gold-starred on top row |
| Direction | label | Plain-English direction cue (e.g. `"Chelsea pressed higher"`, `"Chelsea created more"`) |

**Rows** (approx 6, final count to be confirmed during implementation based on data availability):

1. xG (top row, gold-starred as the match's highest-signal metric)
2. Progressive passes
3. Shots
4. PPDA (pressing intensity — *lower* = more aggressive press; direction label explains this explicitly)
5. Possession %
6. Pass completion %

**Sort order**: descending by `|Δ|`. The metric with the largest absolute delta surfaces at the top. This produces the editorial reading effect (top row is the match's defining gap).

**Direction label for each metric**: must indicate whether a higher number is better or worse for the team that "led" on that metric. PPDA is the canonical trap — a lower PPDA means more aggressive pressing, not worse pressing. The direction label disambiguates.

**League-average reference**: shown as a small grey tick or text annotation on each row (e.g. `"League avg: 1.3"`). Pulled from `fct_league_averages` or equivalent (already wired on current Match Summary via `ms_league_averages`).

### 5.6 Footer

Unchanged structure — scope line already rendered above tiles by `_build_dashboard_page`; data-freshness text and citation footer rendered by existing template.

### 5.7 Mobile / responsive layout (breakpoint at 768px)

Match Summary is the first page to receive purpose-built mobile responsiveness; the CSS pattern established here becomes the template other Tier A pages migrate to in follow-up PRs (tracked separately in `docs/ui-cycles/ui-consistency-roadmap.md`). CSS additions land in `hf_taipy_app/src/style_v2.css` and are page-agnostic where possible.

**Breakpoint**: `@media (max-width: 768px)`.

**Layout behaviour per section**:

| Section | Desktop (≥768px) | Mobile (<768px) |
|---------|------------------|-----------------|
| Tile strip | 4-column grid | 2×2 grid; tile font sizes unchanged (cards scale, not shrink) |
| Row 1 Big Story | Hero card ~60% width + 2 secondary cards in a 2-column sub-grid below | Hero full width; secondary cards stack vertically below |
| Row 2 xG race | Full-width Plotly chart at `320px` height | Plotly chart at `280px` height; Plotly handles responsive scaling natively. Hover becomes tap-to-show on touch. Legend position may auto-collapse to top |
| Row 3 delta table | 6 rows × 5 columns | Table retains layout with reduced horizontal padding; the "Direction" column collapses to an icon (`↑` / `↓` / `=`) with a tooltip carrying the full text on tap |
| Footer | Unchanged | Unchanged |

**Implementation notes**:

- `ll-stats-bar` (the tile strip container in `page_template.py`) needs a `@media (max-width: 768px)` rule to switch from `grid-template-columns: repeat(4, 1fr)` to `repeat(2, 1fr)`. This is a platform-level CSS change; once in place it affects all dashboard pages (Conversion Funnel, Workflows). Desktop appearance unchanged.
- `.ll-match-moments` (new class for Row 1 HTML block) gets a page-specific CSS rule defining the hero + secondary layout at each breakpoint.
- `.ll-delta-table` (new class for Row 3 HTML block) gets responsive padding + direction-column collapse CSS.
- Plotly chart is responsive by default via `config: {responsive: true}` at chart creation.
- Verify no regression on existing dashboard pages (Conversion Funnel, Workflows) when the 4-tile-strip breakpoint kicks in; test at 768px and 375px (iPhone SE baseline).

## 6. Pattern ledger reference

This design adopts the following from the Heat Map redesign (platform-level):

- Template-driven (PageConfig + `build_page`)
- Scope dim labels via `ScopeDim`
- Empty + warning state via `build_warning`
- Data freshness footer
- Scope-aware alt text per visual
- ColorBrewer + `HOME_COLOR`/`AWAY_COLOR` palette discipline
- **Gold-ring focus marker** (visual echo from Heat Map top-5 → Row 2 decisive rings)
- **Overview → focus hierarchy** (structural echo from Heat Map rows 1/2 → Match Summary tile strip + Rows 1–3)

Deliberately NOT reused (page-specific to Heat Map):

- 2×2 bubble grid layout — wrong data shape
- `bin_statistic` density aggregation — not spatial
- Blues/OrRd sequential cmaps — Match Summary uses team colours
- `mplsoccer.Pitch` overlay — Match Summary is not pitch-based

New patterns introduced (page-first, do NOT propagate to other pages until a second use case emerges):

- **Verdict tile** (editorial interpretive label)
- **Big Story hierarchy** (hero + secondary cards)
- **Combined xG race + shot ticks + layered event markers** (temporal matchup visualization)
- **Ranked delta table** with editorial direction labels
- **Auto-inclusion of structural events** (red cards into a VAEP-ranked list)

## 7. Data contract

### 7.1 Tables / marts consumed

| Mart / table | Used for | Notes |
|-------------|---------|-------|
| `soccer_analytics.dev_gold.fct_match_summary` | Tile strip score + match-level xG + existing metrics | Already consumed by current state module |
| `soccer_analytics.dev_gold.fct_action_values` | Row 1 VAEP decisive actions (per-match query) | 9.53M rows; indexed on `match_id` via liquid clustering. Query: `WHERE match_id = ? ORDER BY ABS(vaep_value) DESC LIMIT 3` |
| `soccer_analytics.dev_gold.fct_shots` (or equivalent shot-level mart) | Row 2 xG race — cumulative xG per team over time + per-shot xG for ticks | Exact mart name to be confirmed during implementation. Required fields: `match_id`, `minute`, `second`, `period`, `team_id`, `xg`, `is_goal` |
| `soccer_analytics.dev_gold.fct_discipline_events` (new, via Lakebase sync `fct_discipline_events_synced`) | Red and second-yellow cards for Row 1 auto-include + Row 2 red-card markers | Query: `WHERE match_id = ? AND card_name IN ('Red Card', 'Second Yellow')`. Card data is not flattened by statsbombpy — the new `stg_statsbomb__events.card_name` column coalesces `_raw_extra_json -> bad_behaviour.card.name` with `_raw_extra_json -> foul_committed.card.name` (cards can be issued on either event type). Mart grain: one row per discipline event. Data source: statsbomb only (Wyscout bronze lacks card metadata). |
| `soccer_analytics.dev_gold.fct_league_averages` | Row 3 per-metric league-average reference | Same mart as current page |

### 7.2 New state variables

All prefixed `ms_` per convention:

```python
# Tile strip
ms_final_score: str = "--"            # "2 — 1"
ms_home_name: str = ""
ms_away_name: str = ""
ms_home_xg: str = "--"                # "2.40"
ms_away_xg: str = "--"
ms_home_xg_delta: str = ""            # "+0.40 vs actual"
ms_away_xg_delta: str = ""
ms_verdict_phrase: str = ""           # "Fully merited" / "Smash & grab" / etc.
ms_verdict_detail: str = ""           # "+1.6 xG gap"

# Row 1
ms_moments_html: str = ""             # rendered Big Story + secondary cards HTML
ms_moments_height: str = "280px"
ms_moments_caption: str = ""          # scope caveat text

# Row 2
ms_xg_race_fig: object = None         # Plotly Figure object (NOT a PNG path)
ms_xg_race_alt: str = ""               # alt text / caption for a11y fallback

# Row 3
ms_delta_table_html: str = ""         # rendered Row 3 HTML
ms_delta_table_height: str = "260px"

# Shared
ms_scope_comp: str = ""
ms_scope_team: str = ""
ms_scope_match: str = ""
ms_data_freshness: str = ""
ms_warning_text: str = ""
ms_league_averages: str = ""
```

Variables **removed** from current state:
- `ms_shooting_chart`, `ms_passing_chart`, `ms_possession_chart`, `ms_ppda_chart` (and their `_alt` counterparts) — the 4 bar charts disappear
- `ms_home_score`, `ms_away_score` — absorbed into `ms_final_score`

### 7.3 New rendering helper contracts

Proposed signatures (final details during implementation):

```python
def _derive_verdict(home_xg: float, away_xg: float,
                    home_score: int, away_score: int) -> tuple[str, str]:
    """Return (phrase, detail). Phrase is one of the vocabulary set in §8.
    Detail is a compact xG-gap annotation like '+1.6 xG gap' or 'xG 0.8 vs 1.5'.
    """

def _render_moments_html(hero: dict, secondary: list[dict],
                          red_cards: list[dict], scope_plain: str) -> str:
    """Generate the full Row 1 HTML block."""

def _render_xg_race_chart(shots: pd.DataFrame, goals: pd.DataFrame,
                          red_cards: pd.DataFrame, decisive_actions: pd.DataFrame,
                          home_name: str, away_name: str, home_color: str,
                          away_color: str) -> plotly.graph_objects.Figure:
    """Build the Plotly figure for the xG race — stepped traces per team,
    shot ticks, decisive-action gold rings, goal icons, red-card markers,
    and the half-time divider. Returns a plotly.graph_objects.Figure."""

def _build_delta_rows(home_stats: dict, away_stats: dict,
                      league_avgs: dict, home_name: str, away_name: str) -> pd.DataFrame:
    """Produce a DataFrame with columns Dimension/Home/Away/Delta/Direction,
    sorted descending by |Delta|."""
```

### 7.4 New SQL queries

Added to `hf_taipy_app/src/queries/match.py` (or a new adjacent module — final location during implementation):

```python
def fetch_vaep_decisive_actions(match_id: int, n: int = 3) -> pd.DataFrame: ...
def fetch_shots_timeline(match_id: int) -> pd.DataFrame: ...
def fetch_discipline_events(match_id: int) -> pd.DataFrame: ...  # red + 2nd yellow
```

All queries must use `LIMIT` clauses per project rules; all must validate `match_id` is non-null before execution.

## 8. Verdict vocabulary

Derived from cross-publication editorial convention (Agent 2 research). Deterministic mapping from xG delta to phrase:

| Condition | Phrase | Tooltip |
|-----------|--------|---------|
| Winner's xG ≥ loser's xG + 0.5 | `Fully merited` | "Winner outperformed on xG by ≥0.5" |
| `abs(home_xg - away_xg) < 0.3` | `Fair result` | "Teams finished within 0.3 xG — evenly matched on chances" |
| Winner's xG < loser's xG (i.e. underdog on xG won) | `Fortunate` | "Winner's xG below loser's — result against the run of play" |
| Loser's xG ≥ winner's xG + 1.5 | `Smash & grab` | "Loser created ≥1.5 more xG than winner — standout xG-vs-scoreline gap" |
| Winner's xG ≥ 2× winner's goals scored | `Flattered by scoreline` | "Winner's xG suggests they should have scored more than they did" |

**Resolution order**: Smash & grab > Flattered by scoreline > Fortunate > Fair result > Fully merited. First match wins. A draw (both teams equal score) uses `Fair result` if xG is within 0.3, otherwise falls through to appropriate descriptor based on which team's xG was higher.

**Edge cases** (locked during spec review):
- 0–0 match with `|xG Δ| < 0.3` → `Fair result` applies cleanly (both teams failed to create, evenly matched).
- 0–0 match with large xG gap → `Fair result` phrase with the xG-gap detail annotation carrying the nuance (e.g. `"Fair result"` + detail `"Chelsea dominated xG 2.1 vs 0.4 but couldn't convert"`). Keeps the vocabulary set closed at five phrases — no new phrase introduced, reader load unchanged.
- Very low-scoring match with tiny xG gap → `Fair result` takes precedence over any aggressive descriptor.

The derivation is pure (no external data), unit-testable, covered in §12.1.

## 9. Citations changes

`PageConfig.citations` on `hf_taipy_app/src/pages/match_summary.py`:

**Keep**:
- `Citation("Robberechts & Davis (2020) — How Data Availability Affects the Ability to Learn Good xG Models", "https://dtai.cs.kuleuven.be/sports/blog/how-data-availability-affects-the-ability-to-learn-good-xg-models")` — blog, verified URL
- `Citation("Trainor & Chassy (2021)", "https://doi.org/10.3389/fpsyg.2020.531688")` — PPDA reference, not touched here (separate verification if concerned)

**Add**:
- `Citation("Decroos et al. (2019) — Actions Speak Louder than Goals: Valuing Player Actions in Soccer", "https://doi.org/10.1145/3292500.3330758")` — ACM KDD 2019, VAEP methodology
- `Citation("silly-kicks", "https://github.com/karsten-s-nielsen/silly-kicks")` — VAEP implementation library

`NOTICE` additions/verifications needed in this PR:

- **Decroos et al.** — existing text-only NOTICE entry at lines 61–65. Text should already reference "Proceedings of the 25th ACM SIGKDD"; confirm unchanged during implementation. No DOI to update (entry has no hyperlink).
- **silly-kicks** — verify whether NOTICE already has a silly-kicks entry under Third-Party Libraries. If not, add one.
- **Match Summary page** adding a `silly-kicks` Citation triggers the "every Citation must have a NOTICE entry" rule. This PR must satisfy that rule (add silly-kicks NOTICE entry if not present).

**Not in scope for this PR:** the NOTICE parity gap for Anzer & Bauer (Heat Map citation without a corresponding NOTICE entry) — Match Summary does not cite Anzer & Bauer, so that gap stays tracked under `project_notice_citation_sweep_pending` and will be closed by the separate NOTICE sweep PR.

## 10. Glossary additions

`PAGE_TERMS` entry for `"Match-Summary"` in `hf_taipy_app/src/template.py` updated to include (at minimum):

- **xG** (expected goals) — probability of a shot resulting in a goal
- **xG delta** — goals scored minus xG; positive = overperformed
- **VAEP** — Valuing Actions by Estimating Probabilities; values every on-ball action by its effect on scoring probability
- **PPDA** — Passes Per Defensive Action; pressing-intensity metric, *lower* = more aggressive press
- **Progressive pass** — pass that advances the ball substantially toward goal (definition follows StatsBomb/SPADL convention)
- **Our Verdict** — editorial interpretation of whether the scoreline reflected the run of play
- **Big Story** — the single most decisive action of the match by VAEP impact
- **Smash & grab / Fully merited / Fair result / Fortunate / Flattered by scoreline** — verdict vocabulary phrases (may be grouped under "Our Verdict")

## 11. Research summary

Findings informing the design (stored in `.superpowers/brainstorm/4501-1776603236/content/` for this session, not committed):

- **Agent 1 (FBref / Understat audit)**: neither competitor uses a scannable tile strip or verdict label → our 4-tile commitment fills a genuine gap. Neither ranks decisive moments (they list chronologically) → our Row 1 is novel editorial territory. FBref's dual-line + shot ticks + event markers is the industry reference form → our Row 2 matches it. Understat's mirrored delta bars inspired the Row 3 alternative (ultimately chose ranked delta table for editorial voice).
- **Agent 2 (Athletic / Opta / StatsBomb editorial conventions)**: "Our Verdict" is The Athletic's canonical closing label; verdict vocabulary in §8 is derived from cross-publication usage. "Big Story" pattern is borrowed from Opta Analyst.
- **Agent 3 (academic)**: `|xG Δ|` ranks shots only; VAEP ranks all on-ball actions. Given silly-kicks + `fct_action_values` already exist in-repo, VAEP is a query not a pipeline-build — adopted from v1.

## 12. Chart-choice-audit outcome

Applied `mad-skills:chart-choice-audit` to current `state/match_summary.py`:

- **Finding 1 (Medium, recurring ×4)**: Grouped horizontal bar charts for 2-series comparison. Kirk's nominated upgrade for 2-value-per-category editorial questions is the connected dot plot (or diverging bar chart, or ranked delta table). **Redesign resolves this finding**: Row 3 replaces all 4 grouped bar charts with a single ranked delta table (editorial voice variant of the connected-dot-plot / diverging-bar family).
- **Finding 2 (Low)**: PPDA chart was a degenerate 1-metric × 2-series case. **Redesign resolves**: PPDA becomes one row in Row 3, no longer a dedicated chart.

Both findings addressed by the redesign. Full audit output archived in the brainstorm session directory.

## 13. Testing plan

### 13.1 Unit tests

New test file: `src/tests/test_match_summary_redesign.py` (or extend existing Taipy state tests).

- `test_verdict_vocabulary_all_branches` — parameterise over every condition in §8, assert correct phrase.
- `test_verdict_resolution_order` — when multiple conditions match, correct priority returned.
- `test_verdict_edge_cases` — 0–0 matches, tiny xG, extreme xG gaps.
- `test_moment_ranking_by_abs_vaep` — top-3 correctly sorted; ties broken by minute.
- `test_red_card_auto_inclusion` — single red-card match produces 4-card output; no red card produces 3-card; two red cards both appear.
- `test_delta_table_sorted_by_abs_delta` — DataFrame output sorted descending by `|Δ|`.
- `test_delta_table_direction_labels` — direction strings contain the correct team name and metric interpretation (including PPDA's inverted scale).

### 13.2 Integration tests

- Query against Lakebase synced table or Databricks SQL: fetch a known match's VAEP actions; assert top-3 order is stable and reproducible.
- Query against shots timeline: assert cumulative xG matches `home_xg` / `away_xg` scalars from `fct_match_summary` at full time (no drift).

### 13.3 E2E (staging deploy + Puppeteer)

Before commit, deploy to staging (`scripts/manage_space.py deploy staging`) and run a Puppeteer-driven test from `.superpowers/brainstorm/` scripts or a new dedicated test:

- Load staging Space; select competition / team / match (known-good match, e.g. one with a red card to exercise the auto-include path).
- Assert all 4 tiles render populated values (not `--`).
- Assert Row 1 card count = 3 (or 4 if red card).
- Assert Row 2 image loads and is non-empty.
- Assert Row 3 table has 6 rows sorted correctly.
- Assert verdict phrase matches expected for the test match's xG delta.
- Screenshot captured for manual visual review.

Per CLAUDE.md UI-testing rule: if staging test cannot be executed (e.g. LAKEBASE env var missing), say so explicitly rather than claiming success.

### 13.4 Pre-commit local checks

Before commit:
```bash
uv run ruff check hf_taipy_app/src/pages/match_summary.py hf_taipy_app/src/state/match_summary.py
uv run ruff format --check hf_taipy_app/src/pages/match_summary.py hf_taipy_app/src/state/match_summary.py
uv run pyright hf_taipy_app/src/pages/match_summary.py hf_taipy_app/src/state/match_summary.py
uv run pytest src/tests/test_match_summary_redesign.py -v
```

All must pass before commit. Staging E2E run separately (slower).

## 14. Success criteria

- [ ] Every design decision from §3 implemented as specified.
- [ ] All tests in §13 pass locally.
- [ ] Staging deploy renders the page correctly for at least 3 known-good matches covering: (a) a decisive xG win, (b) a close result, (c) a red-card match.
- [ ] Visual verification: hero Big Story card is clearly weighted above the two secondary cards; gold decisive-action rings align with Row 1 minutes on the xG race; delta table top row gold-starred.
- [ ] Reader test: a first-time visitor presented with a random match can state "what mattered and how it unfolded" after 10 seconds.
- [ ] CI green on the redesign PR.

## 15. Deferred to v2

Captured here so they do not leak into this branch:

- **Click-through linking** — click a moment card → jump to Shot Map with that event highlighted; click delta-table row → drill into the stat's dedicated page. Hover tooltips on the xG race chart are IN scope for v1 (see §5.4).
- **Verdict vocabulary expansion** — tracked as `TODO.md` item **U7** (On-Deck, Dunkin'). Scope analysis deferred because the MVP 5-phrase set covers the common 80%+ of matches; extensions are purely additive. Specific candidates surveyed during spec review (any of these can graduate into scope if needed):
  - `Against the run of play` — split `Fortunate` into two tiers: winner xG below loser but loser xG < 2.0 → `Fortunate`; loser xG ≥ 2.0 → `Against the run of play`. Pure-function, no new data.
  - `Late winner` prefix — append to existing phrase when the goal that moved the score past a draw happened after minute 80. Requires: last-decisive-goal minute (available from shot/event data).
  - `Won on set pieces` — winner scored ≥60% of goals from set pieces. Requires: per-goal provenance (corner / free kick / penalty tag). May not be available without an events-table enrichment step.
  - `Comeback win` — winner trailed at some point. Requires: score trajectory over time (available from running score already used in `fct_action_values`).
  - `Defensive masterclass` — winner conceded < 0.5 xG. Pure-function; data already on tile strip.
  - `Man-advantage winner` — red card to losing team ≤ minute 60. Requires: red-card minute + losing-team inference (available from events + final score).
  - **Expansion effort**: 2–3 additional phrases + logic + tests ≈ half a day. All six ≈ 2–3 days (compound phrases like `"Late-winner, fully merited"` have resolution-order complexity that expands test matrix rapidly). Data availability for set-piece / comeback / man-advantage requires verifying the underlying marts.
  - **Why deferred**: reader cognitive load. Current 5 phrases are at the upper bound of what a visitor can learn from a tooltip. Each additional phrase trades vocabulary richness against reader fluency. Better to ship 5, observe what users actually need, add incrementally.
- **VAEP model refresh cadence** — beyond this spec's scope; tracked in model-card governance.
- **Editorial prose generator** — auto-write a 2-sentence "Our Verdict" paragraph instead of a single phrase (would require LLM integration; v3 or later).

## 16. ADR consideration

This redesign does NOT warrant a new ADR. Per `CLAUDE.md` ADR rules, an ADR is required for cross-cutting dependencies, schema ownership, platform workarounds, naming conventions, or security boundaries. This redesign:

- Uses existing dependencies (silly-kicks, VAEP mart — already in production).
- Introduces no new schema ownership.
- Adds no platform workaround.
- Introduces page-specific patterns (Big Story, Verdict tile, ranked delta table) that are **explicitly page-first** — if any graduates to cross-page pattern after a second use case, THAT promotion gets the ADR.

## 17. Rollout

Per the repo's single/minimal-commit rule, commits happen only at natural testing milestones — docs by themselves do not warrant a commit. The spec lives on-disk from the moment it is drafted and becomes part of the final commit bundle alongside implementation code.

- Draft spec → user review (this step).
- Approved spec lives uncommitted until implementation is ready to test.
- Implementation plan via `writing-plans` skill → lives uncommitted alongside the spec (working artifact).
- Implementation executes → `uv run ruff` + `uv run pyright` + `uv run pytest` locally → staging deploy via `scripts/manage_space.py deploy staging` → Puppeteer E2E test against staging.
- Only after staging verification passes do we stage everything (spec + plan + implementation) into a single commit.
- PR opens for review; squash-merges on approval.
- Post-merge: verify production Space renders correctly on the next refresh.

---

**Draft complete. Awaiting user review.**
