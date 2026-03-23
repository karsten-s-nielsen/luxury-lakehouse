# Taipy Full Migration Design

**Date:** 2026-03-18
**Branch:** `spike/taipy-proof-of-concept`
**Status:** Design approved (rev 2 — 7 review issues addressed)

## Objective

Port all 12 Streamlit pages to Taipy, deployed to `luxury-lakehouse/staging` HF Space. Same data, same filters, same metrics. Template-based architecture (proven in spike). Production Space stays Streamlit until explicit approval to switch.

## Pages — Per-Page Filter Specification

### Standard filter pages

| Page | Competition | Team | Match | Player | Extra Controls |
|------|:-----------:|:----:|:-----:|:------:|----------------|
| Shot Map | Yes | Optional | No | Optional | xG model selector (radio) |
| Pass Map | Yes | Yes | Yes | No | — |
| Heat Map | Yes | Optional | Optional (allow_all) | Optional | — |
| Pass Network | Yes | Yes | Yes | No | `min_passes` slider (1-10) |
| Match Summary | Yes | Optional | Yes | No | — |
| Player Impact (Rankings) | Yes | No | No | No | `min_minutes` slider, sub-view selector |
| Player Impact (Breakdown) | Yes | Yes | No | Yes (inline from `fct_action_values_synced`) | Sub-view selector |
| Player Impact (Timeline) | Yes | Yes | Yes | No | Sub-view selector |
| Player Comparison | Yes | Optional | No | Multi-select (1-3) | Metric multi-select |
| Defensive Impact | DEFCON-filtered | DEFCON-filtered | No | No (from rankings table) | — |

### Tracking filter pages

| Page | Provider | Tracking Match | Extra Controls |
|------|:--------:|:--------------:|----------------|
| Movement & Pressing | Yes (All/metrica/idsse/skillcorner) | Yes | Sub-view selector (Physical/PPDA/Off-Ball xT) |
| Pitch Control | From match list | Yes | Half selector, frame slider (integer seconds), velocity toggle |

### Special filter pages

| Page | Filters | Source Table |
|------|---------|-------------|
| Pass Timing | Match → Team → Player (from `fct_pausa_values_synced`) | PAUSA-specific cascade |
| Player Similarity | Competition (optional), Player (search from embeddings) | `fct_player_embeddings_*_synced` |

## Architecture

```
taipy_spike/
├── src/
│   ├── main.py              # Page registry, on_init, on_navigate, thin orchestrator
│   ├── state/               # Per-page state + callbacks (keeps main.py thin)
│   │   ├── shared.py        # Shared filter state variables + cascade callbacks
│   │   ├── shot_map.py      # Shot Map state vars + callbacks
│   │   ├── match_summary.py
│   │   ├── pass_map.py
│   │   ├── heat_map.py
│   │   ├── pass_network.py
│   │   ├── action_values.py
│   │   ├── player_radar.py
│   │   ├── player_similarity.py
│   │   ├── movement_analysis.py
│   │   ├── pitch_control.py
│   │   ├── pass_timing.py
│   │   └── defensive_valuation.py
│   ├── template.py           # Root page chrome (sidebar, nav, glossary, footer)
│   ├── style.css             # Dark + amber theme
│   ├── db.py                 # Lakebase connection (self-contained copy)
│   ├── config.py             # Pydantic settings (self-contained copy)
│   ├── filters.py            # ALL filter query functions
│   └── pages/                # Page layout markdown only (no logic)
│       ├── shot_map.py
│       ├── pass_map.py
│       ├── heat_map.py
│       ├── pass_network.py
│       ├── match_summary.py
│       ├── action_values.py
│       ├── player_radar.py
│       ├── player_similarity.py
│       ├── movement_analysis.py
│       ├── pitch_control.py
│       ├── pass_timing.py
│       └── defensive_valuation.py
├── Dockerfile
├── requirements.txt
└── .dockerignore
```

### State decomposition (Issue 5 fix)

`main.py` is a thin orchestrator (~100 lines):
- Imports all state modules from `state/`
- Registers pages
- Runs `gui.run()`

`state/shared.py` owns:
- Shared filter variables (`selected_competition`, `selected_team`, etc.)
- Filter cascade callbacks (`on_competition_change`, `on_team_change`, etc.)
- `current_page` variable (maintained by `on_navigate` callback)

`state/<page>.py` owns:
- Page-specific state variables (prefixed: `sm_total_shots`, `ms_home_name`, etc.)
- Page-specific callbacks (`on_xg_model_change`, etc.)
- Data fetching + computation + rendering functions

**Taipy binding caveat:** All state variables must be importable into `main.py`'s module namespace for Taipy to bind them. Each `state/<page>.py` exports its variables at module level, and `main.py` does `from state.shot_map import *` (star import is intentional here — Taipy requires flat module-level names). Each state module defines `__all__` to control exports.

**Mandatory naming contract:** Every exported state variable MUST be prefixed with its page abbreviation to prevent star-import collisions: `sm_` (shot map), `ms_` (match summary), `pm_` (pass map), `hm_` (heat map), `pn_` (pass network), `av_` (action values), `pr_` (player radar), `ps_` (player similarity), `ma_` (movement analysis), `pc_` (pitch control), `pt_` (pass timing), `dv_` (defensive valuation). `__all__` in each module must only contain prefixed names. Shared filter variables (unprefixed) are defined only in `state/shared.py`.

**`current_page` initial value:** `current_page: str = "Shot-Map"` in `state/shared.py`. This ensures sidebar filter visibility conditions resolve correctly on first load before any navigation occurs.

## Key Design Decisions

### 1. Filter module (`filters.py`) — complete function list

**Standard filters:**
- `fetch_competitions()` → `list[tuple[str, int]]`
- `fetch_teams(competition_id)` → `list[tuple[str, int]]`
- `fetch_matches(competition_id, team_id)` → `list[tuple[str, int]]` (when `team_id` is `None`, returns all matches for the competition — supports Heat Map's `allow_all` mode)
- `fetch_players(competition_id, team_id)` → `list[tuple[str, int]]`

**Tracking filters:**
- `fetch_tracking_matches(provider)` → `list[tuple[str, str]]`

**DEFCON-specific filters:**
- `fetch_defcon_competitions()` → `list[tuple[str, int]]` (competitions with DEFCON data)
- `fetch_defcon_teams(competition_id)` → `list[tuple[str, int]]` (teams from `fct_defcon_pressure_synced`)

**PAUSA-specific filters:**
- `fetch_pausa_matches()` → `list[tuple[str, str]]` (matches from `fct_pausa_values_synced`)
- `fetch_pausa_teams(match_id)` → `list[tuple[str, str]]` (teams from `fct_pausa_values_synced`)
- `fetch_pausa_players(match_id, team)` → `list[tuple[str, str]]` (players from `fct_pausa_values_synced`)

**Embedding-specific filters:**
- `fetch_embedding_players(competition_id)` → `list[tuple[str, str]]` (from `fct_player_embeddings_*_synced`)

All return `(human_readable_label, id)` tuples. No raw IDs reach the user.

### 2. Conditional sidebar filters (Issue 3 fix)

The template sidebar uses `render={condition}` to show/hide filters based on the current page. A `current_page` state variable is maintained by an `on_navigate` callback:

```python
def on_navigate(state, page_name):
    state.current_page = page_name
```

Filter visibility rules in template:
```
render={current_page in ("Shot-Map", "Pass-Map", "Heat-Map", ...)}  # competition
render={current_page in ("Shot-Map", "Pass-Map", ...)}               # team
render={current_page in ("Pass-Map", "Pass-Network", ...)}           # match
render={current_page in ("Shot-Map", "Heat-Map", ...)}               # player
render={current_page == "Shot-Map"}                                   # xG model
render={current_page in ("Movement", "Pitch-Control")}               # provider
render={current_page == "Pass-Network"}                               # min_passes slider
```

Pages with custom filters (Defensive Impact, Pass Timing, Player Similarity) have their filter controls embedded in the page markdown itself, not in the template sidebar. The template sidebar shows "no filters for this page" or hides the filter section entirely.

### 3. Multi-select player state (Issue 1 fix — Player Comparison)

Two separate state variables:
- `selected_player: str | None` — single-select (most pages)
- `selected_players_multi: list[str]` — multi-select (Player Comparison only)

The Player Comparison page uses `selected_players_multi` with Taipy's `selector` in `multiple` mode. Other pages use `selected_player`.

### 4. Filter cascade logic (exact CHI audit parity)

Identical to Streamlit:
- Changing competition resets team, match, player, multi-player
- Changing team resets match, player
- "Please select" guidance via conditional text (`render={selected_competition is None}`)
- "No data" warnings via `notify(state, "warning", ...)`
- Human-readable labels on all dropdowns (never raw IDs)

### 5. Caching strategy (Issue 4 fix)

**Pattern:** `functools.lru_cache` on data-fetch functions in `filters.py` with a TTL wrapper. Page-specific data (shots, match summaries, etc.) is cached in per-page state modules keyed by `(comp_id, team_id, player_id)` tuples.

**Invalidation rules:**
- Filter change → clear page-specific cache for that page
- Competition change → clear ALL page caches (fresh data for new competition)
- Page navigation → do NOT clear caches (persist across page switches)
- Temp PNG files → overwrite in place (same file path per page, no accumulation)

### 6. Visualization strategy

| Viz type | Approach | Pages |
|----------|----------|-------|
| mplsoccer pitch | Temp file PNG at 150 DPI | Shot Map, Pass Map, Heat Map, Pitch Control |
| Pass Network | Temp file PNG (mplsoccer pitch background + matplotlib network overlay) | Pass Network |
| Plotly interactive | `tgb.chart(figure="{var}")` | Action Values, DEFCON, Player Radar, Player Similarity, Pass Timing |
| Matplotlib bar charts | Temp file PNG at 150 DPI | Match Summary |

**Pass Network (Issue 7):** The Streamlit app's `plot_pass_network_interactive()` uses Plotly on top of mplsoccer. For Taipy, render the full network (pitch + edges + nodes) as a single matplotlib figure saved to PNG. Interactive hover is sacrificed — acceptable for the migration since the pitch background is the primary value. If Plotly-on-pitch proves necessary later, evaluate Plotly's `add_layout_image` for pitch background.

### 7. Pitch Control frame slider (Issue 6 fix)

Use an integer slider over elapsed seconds (0 to max_seconds). Convert to frame number in the callback: `frame = min_frame + int(seconds * fps)`. This avoids Taipy's lack of `datetime.time` slider support. Display format: `mm:ss` computed in a text element bound to the slider value.

### 8. Per-page academic citations

Every page includes a markdown citation footer matching the Streamlit app exactly.

### 9. Styling target

"Recognizably the same app" (level B):
- Dark theme with amber (`#f59e0b`) accent
- Metric cards with amber left border
- Sidebar with border separator
- Collapsible Getting Started + Glossary
- Not pixel-perfect vs Streamlit, but clearly the same product

## Deployment

- Target: `luxury-lakehouse/staging` (private, owner-only)
- Production (`soccer-analytics-app`) stays Streamlit until explicit approval
- `hf_streamlit_app/` stays in repo as rollback safety net
- Same HF Space secrets as production

## Migration order

Cross-cutting concerns (Issues 3, 4, 5) resolved before page 5:

1. **`state/` directory + `main.py` thin orchestrator** — establish the state decomposition pattern
2. **`filters.py`** — all filter query functions (standard + DEFCON + PAUSA + embedding)
3. **`template.py`** — conditional sidebar with `current_page` + `on_navigate`
4. **Shot Map** — upgrade spike to full parity (6 metrics, xG models, deltas)
5. **Match Summary** — upgrade spike to full parity (scorecard, stat bars)
6. **Pass Map** — pitch + match filter
7. **Heat Map** — pitch + optional filters
8. **Pass Network** — pitch + network overlay + `min_passes` slider
9. **Player Impact** — 3 sub-views (most complex standard page)
10. **Player Comparison** — multi-select players + radar chart
11. **Defensive Impact** — custom DEFCON filters
12. **Movement & Pressing** — tracking filters + 3 sub-views
13. **Pitch Control** — tracking filter + integer-second slider
14. **Pass Timing** — custom PAUSA cascade
15. **Player Similarity** — embedding search + special filter

## Success criteria

- All 12 pages render correctly on `luxury-lakehouse/staging`
- All filter cascades work identically to Streamlit (CHI audit compliance)
- All metrics match Streamlit values for the same data selection
- Dark theme with amber accent is visually coherent
- No broken images, no empty dropdowns, no stale state on page navigation
- `main.py` stays under 150 lines (thin orchestrator)
