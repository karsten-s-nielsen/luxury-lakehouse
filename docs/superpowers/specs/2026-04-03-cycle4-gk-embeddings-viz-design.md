# Cycle 4 Design: GK Analytics, Embedding Explorer & Shape Graph Visualization

**Date:** 2026-04-03
**Branch:** `feature/cycle4-gk-embeddings-viz`
**Status:** Design approved, pending implementation plan

## Overview

Cycle 4 delivers three user-facing surfaces and a data pipeline fix:

| # | Deliverable | Type | Location |
|---|---|---|---|
| 1 | OBSO pipeline fix + D39 dbt + GK stat vector | Data pipeline | `src/ingestion/`, `dbt_project/`, Terraform |
| 2 | Goalkeeper Analytics | New Taipy page (#15) | `hf_taipy_app/` |
| 3 | Tactical Positions | New Taipy page (#16) | `hf_taipy_app/` |
| 4 | Embedding Explorer | New standalone HF Space | `luxury-lakehouse/embedding-explorer` |
| 5 | Player Similarity enhancement | Embedded Atlas widget | `hf_taipy_app/` (existing page) |

The Taipy app goes from 14 → 16 pages. The HF org gets a new static Space. The Player Similarity page gains an embedded 2D embedding neighborhood visualization.

## Execution Order

Phases are sequenced so data is ready before UI is built. Merge points are chosen to minimize conflict with concurrent D32 (ScoutGPT) work in a parallel session.

```
Phase 1: Data Pipeline Fixes ──► Merge Point 1 (zero D32 conflict)
Phase 2: Taipy Pages (GK + TP) ──► Merge Point 2 (all new files, zero D32 conflict)
Phase 3: Embedding Explorer + Player Similarity enhancement ──► Merge Point 3 (D32 conflict zone)
Phase 4: Polish & Verification
```

## D32 Conflict Analysis

D32 (ScoutGPT, parallel session) modifies `player_similarity.py` (model selector) and potentially `player_embeddings_common.py`. Merge points are designed to defer the conflict to Merge Point 3:

| Our change | D32 change | Risk | Merge point |
|---|---|---|---|
| `player_embeddings_common.py` (GK 13d vector) | May add ScoutGPT type | Medium | 1 (we merge first) |
| `player_similarity.py` state (Atlas widget) | Model selector | High | 3 |
| `player_similarity.py` page (Atlas ContentBlock) | New sub-view or metric | High | 3 |
| New Taipy pages (GK, TP) | No overlap | None | 2 |
| `template.py` PAGE_TERMS | Additive, different keys | Low | 2 or 3 |

---

## Phase 1: Data Pipeline Fixes

### 1a. OBSO Pipeline Fix

**Problem:** Three structural issues prevent OBSO data from flowing:

1. `compute_obso_hf.py` publishes output to HF Hub (`luxury-lakehouse/obso-pausa-values`), but `import_obso_results.py` reads from UC Volume (`/Volumes/soccer_analytics/dev_gold/model_weights/obso/`). No bridge step exists.
2. `import_obso_results` Terraform task has no `depends_on` — floats free in the DAG.
3. `compute_obso_hf.py` has never been manually triggered on HF Jobs.

**Solution:**

**Extend `import_obso_results.py`** to download from HF Hub first:
- Add `--hf-repo` argument (default: `luxury-lakehouse/obso-pausa-values`)
- Download `data/pausa_raw_scores.parquet` from HF Hub to UC Volume staging path via `huggingface_hub.hf_hub_download`
- Proceed with existing Volume → bronze Delta write logic
- One entry point, one Terraform task — no separate bridge script

**Fix Terraform** (`terraform/modules/workflows/main.tf`):
- Add `depends_on` to `import_obso_results` task. Since the HF Jobs compute is manual (not a Databricks task), `depends_on` should point to the latest upstream that guarantees OBSO inputs exist — likely `compute_elastic_sync` (which populates data the OBSO script consumes). This prevents the import from running before inputs are ready on subsequent automated runs.
- Add explicit `--volume-path` parameter

**Manual trigger:**
- Verify `luxury-lakehouse/obso-pausa-inputs` dataset is populated
- Run `compute_obso_hf.py` on HF Jobs (`a10g-large`, manual)
- Verify output at `luxury-lakehouse/obso-pausa-values`

### 1b. D39 dbt Model Update

**File:** `dbt_project/models/marts/fct_goalkeeper_stats.sql`

Replace 5 `cast(null as ...)` stubs with actual data from two distinct sources:

**Source A — Sweeper metrics (inline, no import needed):**
- `avg_defensive_action_distance` and `actions_outside_box_per_90` are computable from the existing `gk_actions` CTE in the model. Add a `sweeper_stats` CTE: `avg(start_x)` for defensive action distance, count of actions where `start_x < 16.5` (penalty area) divided by minutes * 90 for outside-box rate. Reference implementation: `compute_sweeper_metrics()` in `src/analytics/goalkeeper.py` lines 395–432.

**Source B — PSxG columns (requires import + dbt source):**
- `psxg_faced`, `goals_conceded`, `goals_prevented` require `bronze.psxg_predictions`
- **Missing dbt artifact:** Create `dbt_project/models/staging/psxg/_psxg__sources.yml` defining the `psxg_predictions` source table
- **Missing staging model:** Create `stg_psxg__predictions.sql` (dedup by `event_id`, latest `_ingested_at`)
- **Join complexity:** `psxg_predictions.player_id` is the *shooter*, not the GK. The dbt model must resolve the opposing GK via: shots against team → GK of that team in that match (from `gk_matches` CTE). Aggregate `sum(psxg)` as `psxg_faced`, `count(case when outcome = 'Goal')` as `goals_conceded`, derive `goals_prevented = psxg_faced - goals_conceded`
- **Data availability:** PSxG model trained (Brier 0.129), predictions published to `luxury-lakehouse/psxg-predictions`. `import_psxg_predictions` is one of the 29 successful tasks — `bronze.psxg_predictions` likely has data. Verify before building the JOIN.

**Also fix:** `import_psxg_predictions` Terraform task has no `depends_on` (same structural bug as `import_obso_results`). Add appropriate dependency.

Contract in `_marts__models.yml` unchanged (columns already declared with correct types).

### 1c. GK Stat Vector Extension

**File:** `src/ingestion/player_embeddings_common.py`

Extend `STAT_FEATURES_BY_GROUP["Goalkeeper"]` from 4 → 13 features:

Current (4):
```
save_pct, gk_xt_per_pass, launch_rate, claim_success_rate
```

Extended (13):
```
save_pct, gk_xt_per_pass, launch_rate, claim_success_rate,
goals_prevented_per_90, psxg_per_shot_faced, avg_defensive_action_distance,
actions_outside_box_per_90, clean_sheet_pct, saves_per_90,
distribution_passes_per_90, gk_xt_delta_total_per_90, punches_per_90
```

All 13 features are available from `fct_goalkeeper_stats` (now fully populated after 1b). The `_load_goalkeeper_stats` query needs updating to fetch the new columns. The existing pgvector `vector(13)` stat index works without changes — dimensional parity with outfield.

### 1d. Verification

- `dbt build --select fct_goalkeeper_stats` with `goalkeeper_enabled=true`
- Refresh `fct_goalkeeper_stats_synced`
- Re-run player embeddings pipeline with updated GK vector
- Verify non-NULL D39 columns in Lakebase
- Verify 13d GK stat vectors in `fct_player_embeddings_career_synced`

**Merge Point 1:** All Phase 1 changes verified. Zero D32 overlap.

---

## Phase 2: Taipy Pages

### 2a. Goalkeeper Analytics Page

**New files:**
- `hf_taipy_app/src/queries/goalkeepers.py`
- `hf_taipy_app/src/state/goalkeeper.py`
- `hf_taipy_app/src/pages/goalkeeper.py`

**Edits:**
- `hf_taipy_app/src/main.py` — PageEntry in Player Analysis section, after Player Similarity
- `hf_taipy_app/src/template.py` — PAGE_TERMS + page tuple membership

**State prefix:** `gk_`

**Sidebar widgets:** Competition selector, team selector (filtered to teams with GK data), min minutes slider (default 90).

#### Sub-view: Rankings

Sortable table of all GKs matching filters. Columns:

| Column | Source | Help text |
|---|---|---|
| Player | `dim_players_synced` | — |
| Team | `dim_players_synced` | — |
| Competition | JOIN | — |
| Minutes | `fct_goalkeeper_stats_synced` | — |
| Saves | same | Total saves |
| Save% | same | Saves / shots on target faced |
| xT/Pass | same | Expected Threat delta per GK distribution pass (0–1, higher = better) |
| Launch Rate | same | % of GK passes that are long (over 60m) |
| Claim Success% | same | Successful claims / total claim attempts |
| Goals Prevented | same | PSxG faced minus goals conceded (positive = above average) |
| PSxG Faced | same | Sum of Post-Shot xG on shots faced |
| Def. Action Distance | same | Average distance from goal of defensive actions (meters) |
| Actions Outside Box/90 | same | Defensive actions outside penalty area per 90 minutes |

`scope_vars` for data coverage label. No right-column metrics (table is the content). `warning_var` for empty state.

#### Sub-view: Shot Stopping

**Goalmouth scatter** (Plotly `go.Scatter`):
- x-axis: goal width (StatsBomb y: 36–44, normalized to 0–8m)
- y-axis: goal height (StatsBomb z: 0–8m)
- Dots colored by outcome (saved vs goal), sized by PSxG probability
- Player selector in sidebar to filter to one GK or show all

**Goals Prevented bar chart** (Plotly `go.Bar`):
- Horizontal bars, one per GK, sorted by `goals_prevented` descending
- Color: green (positive — better than expected) vs red (negative)

Metrics: PSxG Faced, Goals Prevented, Save% (all with `help_text`).

Citation: Butcher et al. (2025) "An Expected Goals On Target (xGOT) Model"

#### Sub-view: Distribution

**Pitch figure** (mplsoccer):
- GK pass origins on half-pitch, colored by xT delta (green positive, red negative)
- Pass destinations shown as endpoints

**Breakdown metrics** (right column): Short%, Medium%, Long%, Launch Rate, xT per Pass, Total xT Added.

Player selector to view one GK at a time.

Citation: Lamberts (2025) "Goalkeeper Value Model"

#### Queries (`queries/goalkeepers.py`)

```
fetch_gk_rankings(competition_id, team_id, min_minutes) → DataFrame
fetch_gk_shots(competition_id, player_id) → DataFrame
fetch_gk_passes(competition_id, player_id) → DataFrame
```

All `@ttl_cache()`, `t("table_name")`, `%s` params.

#### Glossary terms

PSxG, Goals Prevented, Launch Rate, xT Delta (Distribution), Claim Success Rate, Sweeper Keeper.

---

### 2b. Tactical Positions Page

**New files:**
- `hf_taipy_app/src/queries/tactical_positions.py`
- `hf_taipy_app/src/state/tactical_positions.py`
- `hf_taipy_app/src/pages/tactical_positions.py`

**Edits:**
- `hf_taipy_app/src/main.py` — PageEntry in Advanced section, after Team Shape
- `hf_taipy_app/src/template.py` — PAGE_TERMS + tracking-scoped page tuple membership

**State prefix:** `tp_`

**Sidebar widgets:** Tracking provider selector (All/metrica/idsse/skillcorner), tracking match selector, team selector (home/away), half selector (1/2/Full Match).

**Scope constraint:** Tracking-only data (20 matches). Inherits existing tracking match selector pattern. Empty state message when no tracking matches available.

#### Sub-view: Position Plots

**Position time series** (Plotly heatmap or scatter):
- x-axis: match time (minutes)
- y-axis: one row per player, stacked by team
- Color: vertical level (B/DM/M/AM/F) mapped to 5-color sequential palette
- Horizontal level as text annotation or secondary encoding
- Annotation overlays: goals, substitutions, half-time

Metrics: Most Common Formation, Formation Stability (% unchanged windows).

Citation: Sotudeh (2026), thesis Ch. 6.1

#### Sub-view: Formation Comparison

**Dual-detector timeline** (Plotly):
- Two horizontal swim lanes: EFPI (top), Shape Graph (bottom)
- Formation labels as colored segments (window_start_s to window_end_s)
- Same formation label = same color across both lanes
- Vertical bands highlight disagreement windows

Metrics: Agreement Rate, Formation Changes (EFPI), Formation Changes (Shape Graph), Dominant Formation per detector.

Citation: Sotudeh (2026) for shape graph, EFPI source reference.

#### Sub-view: Position Maps

**5×5 grid** (Plotly heatmap):
- Rows: vertical levels (B/DM/M/AM/F)
- Columns: horizontal levels (L/LC/C/RC/R)
- Cell intensity: `pct_time` (0–100)
- One grid per selected player
- Comparison mode: two grids side-by-side for two players
- Phase toggle: All / In-Possession / Out-of-Possession (future-ready, only "all" exists now)

Metrics: Primary Position (highest pct_time cell), Position Versatility (count of cells > 10%), Vertical Range (distinct vertical levels > 5%).

Citation: Sotudeh (2026), thesis Ch. 4

#### Queries (`queries/tactical_positions.py`)

```
fetch_position_timeline(match_id, team) → DataFrame
fetch_formation_labels_dual(match_id, team) → DataFrame
fetch_position_maps(match_id, team, player_id) → DataFrame
fetch_tp_players(match_id, team) → DataFrame
```

All `@ttl_cache()`, `t("table_name")`, `%s` params.

#### Glossary terms

Shape Graph, Position Label, Vertical Level, Horizontal Level, EFPI (Exhaustive Formation Pattern Index), Position Map, Formation Detector.

**Merge Point 2:** Both new pages verified locally. All new files, zero D32 conflict.

---

## Phase 3: Embedding Explorer & Player Similarity Enhancement

### 3a. Embedding Export Script

**New file:** `scripts/export_embedding_atlas_data.py`

- Reads from `fct_player_embeddings_season_synced` JOIN `dim_players_synced` via Lakebase
- Pre-computes UMAP 2D projection locally (avoids WASM UMAP latency on first load)
- Produces two Parquet files:
  - Season-level: `player_id`, `player_name`, `team`, `competition`, `season`, `position_group`, `matches_in_sample`, `data_source`, `behavioral_vector` (128d), `umap_x`, `umap_y`
  - Career-level: same schema with aggregated vectors
- Uploads to `luxury-lakehouse/embedding-atlas-data` on HF Hub
- Entry point registered in `pyproject.toml`
- Manual trigger — not automated

### 3b. Embedding Explorer Space

**New directory:** `embedding-explorer/` at project root.

**Files:**
- `embedding-explorer/index.html` — single-page app
- `embedding-explorer/README.md` — HF Space YAML frontmatter (`sdk: static`)

**Tech stack:** Pure HTML/JS, no Python runtime.
- Embedding Atlas widget from npm CDN
- DuckDB-WASM for client-side Parquet queries
- Data: Parquet from `luxury-lakehouse/embedding-atlas-data` on HF Hub

**UI features:**
- 2D scatter of ~87K season-level dots (pre-computed UMAP of 128d behavioral vectors)
- Color by: position group (default), competition, data source
- Filter by: position group, competition, season
- Toggle: season-level (default) vs career-level (collapsed)
- Hover: player name, team, competition, season, matches in sample, position
- Click: "View in Dashboard" link to Taipy Player Similarity page
- Automatic density contours and clustering from Embedding Atlas

**Deployment:** Static Space via `huggingface_hub.upload_folder` or extended `manage_space.py`.

### 3c. Player Similarity Enhancement

**Files modified:**
- `hf_taipy_app/src/state/player_similarity.py`
- `hf_taipy_app/src/pages/player_similarity.py`

**Embedded Atlas neighborhood widget:**
- `RawHtml` content block (same iframe pattern as DAG visualization)
- Self-contained HTML: loads Embedding Atlas from CDN, DuckDB-WASM queries the HF Hub Parquet filtered to ~50 nearest neighbors
- Placed below results table, above radar comparison
- `ContentBlock("html", "ps_atlas_neighborhood")`, condition: only renders when results populated
- Fixed height ~400px via `height_var`
- "Explore full map" link to standalone Embedding Explorer Space
- Graceful absence if HF Hub Parquet unreachable (no error state, widget just doesn't render)

**Cross-links (all surfaces):**
- Player Similarity → Embedding Explorer: "Explore full map" link in Atlas widget
- Embedding Explorer → Player Similarity: click handler opens Taipy URL with `?player_id=X`
- GK Rankings → Player Similarity: row action "Compare" navigates with GK pre-selected

**Merge Point 3:** All Phase 3 changes. D32 conflict zone — `player_similarity.py`. Coordinate with D32 session on merge order.

---

## Phase 4: Polish & Verification

### Template Updates

**`template.py`:**
- Add all glossary terms (GK: 6 terms, Tactical: 7 terms)
- Add `PAGE_TERMS` entries for both new pages
- Add page routes to `_COMP_PAGES`, `_PLAYER_PAGES`, `_TRACKING_PAGES` tuples as appropriate

### Workflow Card Updates

- `wf-obso-pausa.yaml` — update dependencies and execution notes for fixed pipeline
- `wf-import-obso.yaml` — update to reflect HF Hub → Volume bridge pattern
- No new workflow card for embedding export (manual script, not orchestrated)

### CSS

No new CSS. Both pages use existing `ll-grid-3-1`, metric, table, chart classes. Atlas iframe uses `ll-html-content`. Position map heatmap is Plotly colorscale, not custom CSS.

### Verification Sequence

1. `uv run ruff check src/ scripts/` + `uv run ruff format --check src/ scripts/`
2. `uv run pyright src/`
3. `uv run pytest src/tests/ -v`
4. Local Taipy launch — verify both new pages with live Lakebase data
5. Puppeteer local (16 pages load, no console errors, GK Rankings populated, Tactical Positions shows tracking data)
6. Deploy staging → verify RUNNING → Puppeteer on staging
7. Deploy production after staging verified
8. Verify Embedding Explorer Space is RUNNING on HF

---

## Out of Scope

- OBSO visualization on other pages (data flows to bronze, future pages can use it)
- Position map phase variants beyond "all" (toggle is future-ready)
- 360 embedding model in Embedding Explorer (future addition)
- Automated embedding export pipeline (manual trigger)
- ScoutGPT embeddings in Explorer (D32 ships separately)
- M1 PAT rotation (explicitly excluded from this cycle)
