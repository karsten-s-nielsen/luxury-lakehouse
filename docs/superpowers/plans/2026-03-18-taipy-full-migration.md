# Taipy Full Migration Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port all 12 Streamlit pages to Taipy with full filter cascade parity, deployed to `luxury-lakehouse/staging`.

**Architecture:** Template-based Taipy app in `taipy_spike/`. State decomposed into `state/` modules with mandatory prefix naming. Shared filter queries in `filters.py`. Each page provides data functions + layout markdown. `main.py` is a thin orchestrator (<150 lines).

**Tech Stack:** Taipy 4.1.1, mplsoccer, Plotly, psycopg2-binary, Pydantic, HF Spaces Docker SDK.

**Spec:** `docs/superpowers/specs/2026-03-18-taipy-full-migration-design.md`

---

## File Map

```
taipy_spike/src/
├── main.py              # Thin orchestrator: imports, page registry, gui.run()
├── template.py           # Root page: navbar, conditional sidebar, glossary, footer
├── style.css             # Dark + amber theme
├── db.py                 # Lakebase connection (already exists from spike)
├── config.py             # Pydantic settings (already exists from spike)
├── filters.py            # ALL filter query functions (12 functions)
├── render.py             # Shared rendering helpers (pitch-to-file, chart-to-file)
├── state/
│   ├── __init__.py       # Empty
│   ├── shared.py         # Shared filter state + cascade callbacks + current_page
│   ├── shot_map.py       # sm_ prefixed state + callbacks
│   ├── match_summary.py  # ms_ prefixed state + callbacks
│   ├── pass_map.py       # pm_ prefixed state + callbacks
│   ├── heat_map.py       # hm_ prefixed state + callbacks
│   ├── pass_network.py   # pn_ prefixed state + callbacks
│   ├── action_values.py  # av_ prefixed state + callbacks
│   ├── player_radar.py   # pr_ prefixed state + callbacks
│   ├── player_similarity.py  # ps_ prefixed state + callbacks
│   ├── movement_analysis.py  # ma_ prefixed state + callbacks
│   ├── pitch_control.py  # pc_ prefixed state + callbacks
│   ├── pass_timing.py    # pt_ prefixed state + callbacks
│   └── defensive_valuation.py  # dv_ prefixed state + callbacks
└── pages/                # Layout markdown ONLY (no logic)
    ├── shot_map.py
    ├── match_summary.py
    ├── pass_map.py
    ├── heat_map.py
    ├── pass_network.py
    ├── action_values.py
    ├── player_radar.py
    ├── player_similarity.py
    ├── movement_analysis.py
    ├── pitch_control.py
    ├── pass_timing.py
    └── defensive_valuation.py
```

---

## Task 1: Foundation — Filters, State, Template

**Files:**
- Create: `taipy_spike/src/filters.py`
- Create: `taipy_spike/src/render.py`
- Create: `taipy_spike/src/state/__init__.py`
- Create: `taipy_spike/src/state/shared.py`
- Rewrite: `taipy_spike/src/template.py`
- Rewrite: `taipy_spike/src/main.py`

This task establishes the foundation that all 12 pages depend on. It must be completed before any page task.

- [ ] **Step 1: Write `filters.py`**

All 12 filter query functions. Each returns `list[tuple[str, id_type]]` with human-readable labels. Port SQL exactly from `src/streamlit_app/components/filters.py` and page-specific queries.

Functions:
- `fetch_competitions()` — from `dim_competitions_synced`, label: `"country — competition_name"`
- `fetch_teams(competition_id)` — UNION on `fct_match_summary_synced` home/away, joined to `dim_teams_synced`
- `fetch_matches(competition_id, team_id)` — from `fct_match_summary_synced`, label: `"YYYY-MM-DD — Home X-Y Away"`. When `team_id is None`, returns all matches.
- `fetch_players(competition_id, team_id)` — from `fct_player_stats_synced` + `dim_players_synced`
- `fetch_tracking_matches(provider)` — recursive CTE on `fct_tracking_frames_synced`, joined to `fct_match_summary_synced` for labels
- `fetch_defcon_competitions()` — competitions with rows in `fct_defcon_pressure_synced`
- `fetch_defcon_teams(competition_id)` — teams from `fct_defcon_pressure_synced` via `fct_match_summary_synced`
- `fetch_pausa_matches()` — matches from `fct_pausa_values_synced` joined to `fct_match_summary_synced`
- `fetch_pausa_teams(match_id)` — teams from `fct_pausa_values_synced`
- `fetch_pausa_players(match_id, team)` — players from `fct_pausa_values_synced` joined to `dim_players_synced`
- `fetch_embedding_players(competition_id, min_matches, table, count_col)` — recursive CTE on embedding table. `table` and `count_col` MUST be validated against `_ALLOWED_EMBEDDING_TABLES` and `_ALLOWED_COUNT_COLUMNS` allowlists before interpolation into SQL (same security pattern as Player Similarity's `_ALLOWED_VECTOR_COLUMNS`). Signature expanded from spec's `(competition_id)` to support min-matches slider and career/season table routing.
- `fetch_action_value_players(competition_id, team_id)` — from `fct_action_values_synced` joined to `dim_players_synced`. Not in the original spec; added to support Player Impact Breakdown sub-view's inline player dropdown.

**Note:** Steps 1–5 must be completed sequentially within this task: `filters.py` → `render.py` → `state/__init__.py` → `state/shared.py` (depends on filters) → `template.py` (depends on shared state) → `main.py` (depends on all of the above).

- [ ] **Step 2: Write `render.py`**

Shared rendering helpers:
- `pitch_to_file(fig, name)` — saves matplotlib figure to `/tmp/{name}.png` at 150 DPI, returns path
- `chart_to_file(fig, name)` — same for non-pitch matplotlib charts
- `PITCH_BG_COLOR = "#1a1a2e"`, `PITCH_LINE_COLOR = "#e0e0e0"`, `AMBER = "#f59e0b"`

- [ ] **Step 3: Write `state/__init__.py`**

Empty file.

- [ ] **Step 4: Write `state/shared.py`**

Shared filter state variables + cascade callbacks:
```python
# Exported state variables
current_page: str = "Shot-Map"
selected_competition: str | None = None
selected_team: str | None = None
selected_match: str | None = None
selected_player: str | None = None
selected_players_multi: list[str] = []
competition_lov: list[str] = []
team_lov: list[str] = []
match_lov: list[str] = []
player_lov: list[str] = []

# Internal maps (not exported via __all__)
_comp_map: dict[str, int] = {}
_team_map: dict[str, int] = {}
_match_map: dict[str, int] = {}
_player_map: dict[str, int] = {}

# Callbacks: on_init, on_navigate, on_competition_change, on_team_change,
# on_match_change, on_player_change
```

Cascade logic: competition change resets team/match/player and reloads all dependent lists. Team change resets match/player. Each callback calls the page-specific refresh function for the current page.

- [ ] **Step 5: Rewrite `template.py`**

Root page with conditional sidebar using `render={current_page in (...)}` for each filter. Complete sidebar control list with visibility rules:

| Control | `render=` condition |
|---------|-------------------|
| Competition dropdown | `current_page in ("Shot-Map","Pass-Map","Heat-Map","Pass-Network","Match-Summary","Action-Values","Player-Radar","Movement")` |
| Team dropdown | `current_page in ("Shot-Map","Pass-Map","Heat-Map","Pass-Network","Match-Summary","Action-Values","Player-Radar")` |
| Match dropdown | `current_page in ("Pass-Map","Pass-Network","Match-Summary","Action-Values")` |
| Player dropdown (single) | `current_page in ("Shot-Map","Heat-Map")` |
| Player dropdown (multi) | `current_page == "Player-Radar"` |
| xG model selector | `current_page == "Shot-Map"` |
| Min passes slider | `current_page == "Pass-Network"` |
| Min minutes slider | `current_page in ("Action-Values","Player-Radar")` |
| Provider selector | `current_page in ("Movement","Pitch-Control")` |
| Tracking match selector | `current_page in ("Movement","Pitch-Control")` |
| Sub-view selector | `current_page in ("Action-Values","Movement")` |

**Page-content controls (NOT sidebar):** Pitch Control's half selector, velocity toggle, time slider, and model radio live in the page markdown (they are frame-level controls, not filter-level). Defensive Impact, Pass Timing, and Player Similarity have entirely custom filter UIs in page content.

Include Getting Started (collapsible), Glossary (collapsible), and footer.

- [ ] **Step 6: Rewrite `main.py`**

Thin orchestrator (<150 lines):
```python
from state.shared import *
from state.shot_map import *
from state.match_summary import *
# ... all 12 state modules
from pages.shot_map import page_md as shot_map_page
# ... all 12 page layouts
from template import root_page

pages = {"/": root_page, "Shot-Map": shot_map_page, ...}
gui = Gui(pages=pages, css_file="style.css")
gui.run(...)
```

- [ ] **Step 7: Update `style.css`**

Refine metric cards, sidebar, footer, expandable sections for production quality.

- [ ] **Step 8: Deploy foundation and verify**

Deploy to staging. Verify: navbar shows all 12 page links, sidebar filters appear/hide based on current page, Getting Started + Glossary are collapsible.

---

## Task 2: Shot Map + Match Summary (upgrade spike)

**Files:**
- Rewrite: `taipy_spike/src/state/shot_map.py`
- Rewrite: `taipy_spike/src/pages/shot_map.py`
- Rewrite: `taipy_spike/src/state/match_summary.py`
- Rewrite: `taipy_spike/src/pages/match_summary.py`

These two pages already exist from the spike. Upgrade to use the new state/ decomposition and full feature parity.

- [ ] **Step 1: Write `state/shot_map.py`** — all `sm_` prefixed variables, `on_xg_model_change` callback, `_refresh_shot_map()` helper. Port `_compute_brier_score`, `_join_xg_predictions` from Streamlit.

- [ ] **Step 2: Write `pages/shot_map.py`** — layout markdown with 6 metric cards (Total Shots, Goals, Total xG + delta, Conversion, xG/Shot + delta, Brier + delta), conditional pitch image, citation footer.

- [ ] **Step 3: Write `state/match_summary.py`** — all `ms_` prefixed variables, `on_ms_match_change` callback. Port match data fetching, stat bar rendering.

- [ ] **Step 4: Write `pages/match_summary.py`** — layout markdown with scorecard (4 metric cards), 4 stat bar chart images, citation footer.

- [ ] **Step 5: Deploy and verify both pages**

---

## Task 3: Pass Map + Heat Map

**Files:**
- Create: `taipy_spike/src/state/pass_map.py`
- Create: `taipy_spike/src/pages/pass_map.py`
- Create: `taipy_spike/src/state/heat_map.py`
- Create: `taipy_spike/src/pages/heat_map.py`

- [ ] **Step 1: Write `state/pass_map.py`** — `pm_` prefixed. Fetch passes query, pass categorization, pitch rendering. 5 metrics (Total, Completed, Progressive, Line-Breaking, Completion %). Two checkboxes (highlight progressive, highlight line-breaking).

- [ ] **Step 2: Write `pages/pass_map.py`** — layout with pitch image, 5 metric cards, pass highlight toggles, citation.

- [ ] **Step 3: Write `state/heat_map.py`** — `hm_` prefixed. Server-side aggregation UNION query, zone classification. 4 metrics (Total, Passes, Shots, Most Active Zone).

- [ ] **Step 4: Write `pages/heat_map.py`** — layout with heatmap image, 4 metric cards, citation.

- [ ] **Step 5: Deploy and verify both pages**

---

## Task 4: Pass Network

**Files:**
- Create: `taipy_spike/src/state/pass_network.py`
- Create: `taipy_spike/src/pages/pass_network.py`

- [ ] **Step 1: Write `state/pass_network.py`** — `pn_` prefixed. Fetch completed passes with passer/receiver names. `_build_network(passes, min_pair_count)` for nodes + edges. 3 metrics (Total Passes, Unique Connections, Top Pair). `pn_min_passes` slider state (1-10, default 3). Render network as single matplotlib figure (pitch background + edges + nodes).

- [ ] **Step 2: Write `pages/pass_network.py`** — layout with network image, 3 metric cards, min_passes slider in sidebar, citation.

- [ ] **Step 3: Deploy and verify**

---

## Task 5: Player Impact (Action Values — 3 sub-views)

**Files:**
- Create: `taipy_spike/src/state/action_values.py`
- Create: `taipy_spike/src/pages/action_values.py`

Most complex standard page — 3 sub-views with different filter footprints.

- [ ] **Step 1: Write `state/action_values.py`** — `av_` prefixed. Sub-view selector state. Rankings query with min_minutes. Breakdown query with dynamic WHERE. Timeline query with match filter. 3 sets of metrics. Plotly timeline scatter + matplotlib breakdown bars.

- [ ] **Step 2: Write `pages/action_values.py`** — conditional layout per sub-view using `render={av_current_view == "Rankings"}` etc. Rankings dataframe, breakdown chart + metrics, timeline chart + metrics. Citation.

- [ ] **Step 3: Deploy and verify all 3 sub-views**

---

## Task 6: Player Comparison (Radar)

**Files:**
- Create: `taipy_spike/src/state/player_radar.py`
- Create: `taipy_spike/src/pages/player_radar.py`

- [ ] **Step 1: Write `state/player_radar.py`** — `pr_` prefixed. Multi-select player state (1-3 players). Metrics selector (11 default + 2 physical). Radar rendering via mplsoccer Radar. Stats fetch with ROW_NUMBER pattern.

- [ ] **Step 2: Write `pages/player_radar.py`** — layout with radar image centered, metric selector, player multi-select, stats expander. Citation.

- [ ] **Step 3: Deploy and verify**

---

## Task 7: Defensive Impact (DEFCON — 3 tabs)

**Files:**
- Create: `taipy_spike/src/state/defensive_valuation.py`
- Create: `taipy_spike/src/pages/defensive_valuation.py`

- [ ] **Step 1: Write `state/defensive_valuation.py`** — `dv_` prefixed. Custom DEFCON competition + team filters. Rankings query. Breakdown with 4 credit metrics + Plotly grouped bar. Timeline with per-action dataframe. Inline player selectors per tab.

- [ ] **Step 2: Write `pages/defensive_valuation.py`** — conditional layout for 3 views (rankings dataframe, breakdown chart + metrics, timeline dataframe). Custom DEFCON filters in page content (not template sidebar). Citation.

- [ ] **Step 3: Deploy and verify all 3 views**

---

## Task 8: Movement & Pressing (3 sub-views)

**Files:**
- Create: `taipy_spike/src/state/movement_analysis.py`
- Create: `taipy_spike/src/pages/movement_analysis.py`

- [ ] **Step 1: Write `state/movement_analysis.py`** — `ma_` prefixed. Provider selector + tracking match filter. Physical stats query + metric selector. PPDA query + bar chart. Off-Ball xT filtered view. 3 sub-view selector.

- [ ] **Step 2: Write `pages/movement_analysis.py`** — conditional layout per sub-view. Physical bars + metrics, PPDA bars + metrics, Off-Ball xT bars + metrics. Citations.

- [ ] **Step 3: Deploy and verify all 3 sub-views**

---

## Task 9: Pitch Control

**Files:**
- Create: `taipy_spike/src/state/pitch_control.py`
- Create: `taipy_spike/src/pages/pitch_control.py`

- [ ] **Step 1: Write `state/pitch_control.py`** — `pc_` prefixed. Provider + tracking match + half selector. Integer-second slider (converted to frame number). Frame data query. Physics-based pitch control computation via `analytics.pitch_control`. Voronoi and physics rendering modes.

- [ ] **Step 2: Write `pages/pitch_control.py`** — layout with model selector radio, half radio, velocity toggle, time slider with mm:ss display, pitch image, control metrics. Citation.

- [ ] **Step 3: Deploy and verify** — test frame slider, physics vs Voronoi modes, velocity arrows toggle.

---

## Task 10: Pass Timing (PAUSA)

**Files:**
- Create: `taipy_spike/src/state/pass_timing.py`
- Create: `taipy_spike/src/pages/pass_timing.py`

- [ ] **Step 1: Write `state/pass_timing.py`** — `pt_` prefixed. Custom PAUSA filter cascade (match → team → player from `fct_pausa_values_synced`). Summary metrics (4 metrics). Individual passes query. Plotly scatter (temporal vs spatial with quadrant lines). Plotly density heatmap (receiver locations). Rankings dataframe with DFL ID caption.

- [ ] **Step 2: Write `pages/pass_timing.py`** — layout with custom filters (page-level, not sidebar), 4 metric cards, 2 Plotly charts side-by-side, rankings dataframe. Citations.

- [ ] **Step 3: Deploy and verify**

---

## Task 11: Player Similarity

**Files:**
- Create: `taipy_spike/src/state/player_similarity.py`
- Create: `taipy_spike/src/pages/player_similarity.py`

- [ ] **Step 1: Write `state/player_similarity.py`** — `ps_` prefixed. Search mode selector (style/statistical). Optional competition filter. Min-matches slider. Player dropdown from embeddings table. pgvector cosine distance query. Results dataframe. Compare-with radar chart. Distance threshold labels. `_ALLOWED_VECTOR_COLUMNS` and `_ALLOWED_COUNT_COLUMNS` allowlists for column interpolation security.

- [ ] **Step 2: Write `pages/player_similarity.py`** — layout with custom filters (search mode, competition toggle, min matches, player selector, result count), results dataframe, compare radar. Citations.

- [ ] **Step 3: Deploy and verify** — test both search modes, competition filter toggle, pgvector query.

---

## Task 12: Final Polish + Verification

**Files:**
- Modify: `taipy_spike/src/style.css`
- Modify: `taipy_spike/src/template.py`
- Modify: `taipy_spike/requirements.txt` (if new deps needed)

- [ ] **Step 1: Cross-page navigation test** — navigate through all 12 pages, verify filter state persists where expected and resets where expected.

- [ ] **Step 2: Styling pass** — ensure all metric cards, charts, and layouts are visually consistent. Amber accent visible. Dark theme coherent.

- [ ] **Step 3: CHI audit parity check** — verify "please select" guidance on every page, "no data" warnings, human-readable labels everywhere, no raw IDs.

- [ ] **Step 4: Deploy final version to staging**

- [ ] **Step 5: Update spec with results** — record any deviations, workarounds, or known limitations.
