# Shape, Search & Baselines — Design Spec

**Branch:** `feature/shape-search-baselines`
**Date:** 2026-03-25
**Scope:** D20 (EFPI Formation Detection) + D21 (Team Shape Taipy Page) + O3 (Pipeline Performance Baselines) + U5 (Server-Side Player Search)

---

## D20: EFPI Formation Detection Pipeline

### Summary

Add `unravelsports` as a main dependency and create a Databricks serverless pipeline that runs EFPI (Elastic Formation and Position Identification) template matching on tracking data. Writes formation labels per 5-minute window to a new Delta table.

### Dependency

- Add `unravelsports>=0.5` to `pyproject.toml` main dependencies (MPL 2.0).
- `unravelsports` depends on `kloppy` (BSD-3) — acceptable. Verify the full dependency tree does not pull in anything heavyweight or GPL-incompatible.

### Pipeline: `compute_formations`

- **Entry point:** `compute_formations = "ingestion.formations:main"` in `pyproject.toml`.
- **Module:** `src/ingestion/formations.py`.
- **Decorator:** `@workflow("wf-formations", phase="heuristic")` — registers in the `WorkflowRegistry` singleton so the lifecycle runner dispatches `on_start`/`on_complete`/`on_skip`/`on_error` hooks.
- **Hook:** `CostEstimateHook(spark, catalog, schema, cost_schema="observability")` — writes cost data to `{catalog}.observability.workflow_cost_live` via Delta MERGE, same as all other pipelines.
- **Input:** `fct_tracking_frames` (Delta table via Spark SQL), filtered to outfield players (exclude GK from formation detection — GK position is handled separately by `defensive_line_height` in `team_shape.py`).
- **Processing:**
  1. Group tracking frames by `(match_id, period)`.
  2. For each group, resample to 1fps (formation detection does not need 10-25fps resolution).
  3. Segment into 5-minute windows (configurable via `FormationParams` Pydantic model).
  4. For each window, extract mean (x, y) per player → pass to EFPI template matching.
  5. EFPI returns formation label (e.g., "4-3-3") and confidence score per window.
- **Skip guard:** Check `fct_formation_labels` for existing `match_id` values before processing. Strategy: `skip-guard` with key `[match_id]`.
- **Output schema (`fct_formation_labels`):**

  | Column | Type | Description |
  |--------|------|-------------|
  | `match_id` | string | Match identifier |
  | `period` | int | Match period (1, 2, etc.) |
  | `team` | string | "home" or "away" |
  | `window_start_s` | float | Window start in elapsed seconds |
  | `window_end_s` | float | Window end in elapsed seconds |
  | `formation_label` | string | EFPI result (e.g., "4-3-3", "4-4-2") |
  | `confidence` | float | EFPI template match confidence (0-1) |
  | `_ingested_at` | timestamp | UTC audit column |

- **Write:** `replaceWhere` on `match_id` for idempotent writes. Liquid clustering on `match_id`.
- **Coordinate system:** EFPI needs positions in a consistent coordinate system. The `fct_tracking_frames` staging layer normalizes all providers to 120×80 StatsBomb coordinates. Pass these directly — EFPI's template matching is scale-invariant (it normalizes internally).

### dbt Model

- New mart: `fct_formation_labels.sql` — passthrough from the bronze/gold Delta table with contract enforcement.
- Add to `_marts__models.yml` with `contract: {enforced: true}` and explicit `data_type` on every column.

### Synced Table & Index

- New synced table: `fct_formation_labels_synced` (user creates manually via Databricks UI).
- Composite btree index: `(match_id, team)` — primary access pattern from Taipy.

### Workflow Card (`workflow-cards/wf-formations.yaml`)

Full workflow card following the `WorkflowCard` Pydantic model schema. This is the authoritative manifest — the pipeline implementation must match it exactly.

```yaml
---
name: EFPI Formation Detection
id: wf-formations
version: "1.0"
status: production
type: heuristic
domain: formation-detection
owners:
  - karsten
tags:
  - formation
  - team-shape
  - tracking
  - efpi

references:
  - citation: "Bekkers & Dabadghao (2025), arXiv:2506.23843 — EFPI: Elastic Formation and Position Identification"
    role: methodology
  - citation: "Bialkowski et al. (2014), IEEE ICDM — Role assignment via Hungarian algorithm"
    role: algorithm

inputs:
  datasets:
    - id: "{catalog}.gold.fct_tracking_frames"
      source: delta-table
      description: "Tracking frames with player positions (120×80 coordinate system)"

outputs:
  tables:
    - id: "{catalog}.gold.fct_formation_labels"
      destination: delta-table
      mart: fct_formation_labels
      synced: fct_formation_labels_synced

execution:
  inference:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: compute_formations
    module: ingestion.formations
    distribution: applyInPandas
    partition_key: match_id
    schedule: "daily 06:00 UTC"
    timeout: "600s"
    environment: analytics

depends_on:
  - wf-pitch-control  # shares tracking data dependency

idempotency:
  strategy: skip-guard
  key:
    - match_id
  description: "Checks existing formation labels by match_id before processing."

performance:
  inference_timeout: "600s"
  memory_ceiling: "16 GB driver, 1 GB UDF executor"

cost:
  inference:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 5
    typical_cost_usd: 0.35

monitoring:
  freshness_sla_hours: 168

links:
  source_code:
    - "src/ingestion/formations.py"
    - "src/analytics/team_shape.py"
---

## Overview

EFPI (Elastic Formation and Position Identification) detects team formations from tracking
data using the Hungarian algorithm for template matching against 65 formation templates.
Outputs a formation label and confidence score per team per 5-minute window.

## Algorithm

For each time window, mean player positions are computed from 1fps-resampled tracking frames.
Outfield players (GK excluded) are matched against formation templates using linear sum
assignment (scipy.optimize.linear_sum_assignment) with scale normalization. The template
with the lowest assignment cost is selected as the formation label.
```

The card integrates with:
- **AI/ML Workflows page**: Automatically appears in the DAG visualization and dashboard table (reads from `workflow-cards/*.yaml`).
- **`WorkflowRegistry`**: The `@workflow("wf-formations", phase="heuristic")` decorator links the runtime to the card via matching `id`.
- **`CostEstimateHook`**: Cost data written to `{catalog}.observability.workflow_cost_live` appears in the Workflows page cost column.
- **CI validation**: `validate_workflow_cards` entry point validates the YAML against the `WorkflowCard` Pydantic model.
- **`depends_on`**: Declaring `wf-pitch-control` as upstream ensures the DAG renders the dependency edge correctly.

---

## D21: Team Shape Taipy Page

### Summary

New "Team Shape" page in the Taipy app with two sub-views (Snapshot and Timeline), following the `SubView`-based multi-view pattern used by Defensive Impact and Movement & Pressing.

### Page Config (`hf_taipy_app/src/pages/team_shape.py`)

```python
PageConfig(
    title="Team Shape",
    icon="polyline",
    nav_section=NAV_ADVANCED,
    freshness_var="ts_data_freshness",
    description="Formation detection (EFPI) and spatial metrics from tracking data. ...",
    citations=[
        Citation("Bekkers & Dabadghao (2025)", "https://arxiv.org/abs/2506.23843"),
        Citation("Frencken et al. (2011)", "..."),
        Citation("Bourbousson et al. (2010)", "..."),
    ],
    sub_views=[SubView("Snapshot"), SubView("Timeline")],
)
```

### Sub-views

**Snapshot** (`selected_sub_view == "Snapshot"`):
- **Content:** Single `ContentRow` with `ContentBlock("chart", "ts_snapshot_figure")`.
  - Plotly scatter on a proper pitch (touchlines, penalty areas, center circle, arcs — drawn with Plotly shapes, same technique as Shot Map).
  - Player dots with jersey numbers (hover: player name, position).
  - Dashed formation lines connecting players within defensive/midfield/attack line clusters.
  - Semi-transparent filled convex hull polygons per team (`fill='toself'`, `opacity=0.15`).
  - Home/away colors consistent with other pages.
- **Caption var:** `ts_snapshot_caption` — "Frame 1234 · 15:42 · 1st Half" or "Phase Average · In Possession · 1st Half".
- **Two modes** controlled by `ts_phase_average` toggle:
  - **Single Frame** (default): Frame slider (`ts_elapsed_seconds`) scrubs through match. Shape computed at that frame via `compute_team_shape()`.
  - **Phase Average**: Mean positions over selected phase (in-possession / out-of-possession / full half). Shape computed from averaged positions.
- **Metrics (right column):**
  - Formation (`ts_formation`, help: "EFPI template match — most common formation in the selected window")
  - Team Length (`ts_team_length`, help: "<30m = compact, >40m = stretched (Fradua et al. 2013)", delta_var: `ts_length_delta`)
  - Team Width (`ts_team_width`, help: ">38m in possession = good width creation", delta_var: `ts_width_delta`)
  - Defensive Line Height (`ts_def_line_height`, help: "0% = own goal line, 100% = opponent goal. >50% = high press, <35% = deep block")
  - Hull Area (`ts_hull_area`, help: "Convex hull area in m². ~1,000 m² defending, ~1,500 m² attacking (Frencken et al. 2011)")
  - Stretch Index (`ts_stretch_index`, help: "Mean distance from team centroid in meters (Bourbousson et al. 2010). Lower = more compact")
  - Inter-Line Gaps (`ts_inter_line_gaps`, help: "Distance between def↔mid and mid↔att line centroids. <12m = compact, >18m = exposed")
- **Delta vars:** Deltas computed against match average (e.g., "▼ 2.1m vs avg").

**Timeline** (`selected_sub_view == "Timeline"`):
- **Content:** Three `ContentRow`s stacked:
  1. `ContentBlock("chart", "ts_timeline_figure", header="Shape Metrics Over Time")` — Plotly line chart with team length, width, defensive line height over match time. Annotated with goals, substitutions (vertical dashed lines with labels). Half-time marker.
  2. `ContentBlock("chart", "ts_formation_figure", header="Formation Labels")` — Plotly horizontal bar/strip showing formation label per 5-minute window, color-coded by formation type. Data from `fct_formation_labels`.
  3. `ContentBlock("table", "ts_phase_comparison", header="Phase Comparison")` — DataFrame table with in-possession vs out-of-possession averages for all shape metrics, plus delta column.
- **Metrics (right column):**
  - Avg Length (`ts_avg_length`, help: "Match average team length")
  - Avg Width (`ts_avg_width`, help: "Match average team width")
  - Formation Changes (`ts_formation_changes`, help: "Number of distinct formation transitions detected")
  - Compactness Delta (`ts_compactness_delta`, help: "Change in team length between 1st and 2nd half. Positive = more stretched", delta_var: `ts_compactness_delta_fmt`)

### Sidebar Widgets

Shared filters (already exist — reused via shared state):
- Provider dropdown (`selected_provider`)
- Tracking match dropdown (`selected_tracking_match`)

Page-specific widgets registered in `state/team_shape.py`:
- **Team** (`ts_selected_team`): dropdown, lov=`ts_team_lov` (["Home", "Away"]), depends_on=`selected_tracking_match`.
- **Half** (`ts_selected_half`): dropdown, lov=`ts_half_lov` (["1st Half", "2nd Half", "Full Match"]), depends_on=`selected_tracking_match`.
- **Phase Average** toggle (`ts_phase_average`): condition=`selected_sub_view == "Snapshot"`. Default: False.
- **Time (s)** slider (`ts_elapsed_seconds`): condition=`selected_sub_view == "Snapshot" and not ts_phase_average`. min/max from frame range query. `change_delay=300` for debounce.
- **Show Hull** toggle (`ts_show_hull`): condition=`selected_sub_view == "Snapshot"`. Default: True.
- **Show Formation Lines** toggle (`ts_show_formation_lines`): condition=`selected_sub_view == "Snapshot"`. Default: True.

### State Module (`hf_taipy_app/src/state/team_shape.py`)

- Prefix: `ts_`.
- **Fetch functions** (all `@ttl_cache`):
  - `_fetch_tracking_data(match_id, period)` → tracking frames from Lakebase.
  - `_fetch_formation_labels(match_id, team)` → formation labels from Lakebase.
  - `_fetch_match_events(match_id)` → goals/subs for timeline annotations.
- **Refresh callback:** `ts_refresh(state)` — registered via `register_page_refresher("Team-Shape", ts_refresh)`.
- **Render functions:**
  - `_render_snapshot(state)` → builds Plotly figure with pitch, players, hulls, lines.
  - `_render_timeline(state)` → builds Plotly time-series + formation strip + phase table.
- **On-change callbacks:** `ts_on_team_change`, `ts_on_half_change`, `ts_on_seconds_change`, `ts_on_phase_toggle`, `ts_on_hull_toggle`, `ts_on_formation_lines_toggle`.

### Pitch Rendering

The Plotly pitch is drawn using `plotly.graph_objects` shapes:
- Touchlines, halfway line, center circle, penalty areas, goal areas, penalty spots, corner arcs.
- Coordinate system: 120×80 (matching `fct_tracking_frames`).
- Background: dark green (`#0d4d0d`), lines white with `opacity=0.3`.
- Reusable function: `_draw_pitch_shapes() -> list[go.layout.Shape]` in `state/team_shape.py` (or extracted to a shared `plotting.py` if Shot Map already has one).

### Registration

- **`main.py`:** Import `page_config` and `page_md`, add `PageEntry("Team-Shape", page_config, page_md)` to `PAGE_REGISTRY`.
- **`template.py`:** Add glossary terms and `PAGE_TERMS["Team-Shape"]` entry.

### Glossary Terms

Add to `GLOSSARY` in `template.py`:
- "Team Shape" — Spatial metrics describing how a team is spread across the pitch.
- "Convex Hull" — Smallest polygon enclosing all outfield players. Area indicates territorial extent.
- "Stretch Index" — Mean distance of all outfield players from the team centroid (Bourbousson et al. 2010).
- "EFPI" — Elastic Formation and Position Identification. Template matching algorithm for automatic formation detection (Bekkers & Dabadghao 2025).
- "Defensive Line Height" — Average position of the defensive line as a percentage of pitch length. 0% = own goal, 100% = opponent goal.
- "Inter-Line Gaps" — Distance between defensive-midfield and midfield-attack line centroids.
- "Team Length" — Distance between the deepest and most advanced outfield players along the goal-to-goal axis.
- "Team Width" — Distance between the widest outfield players along the touchline-to-touchline axis.

`PAGE_TERMS["Team-Shape"]`: all of the above.

---

## O3: Pipeline Performance Baselines

### Summary

Fill in all TBD cells in `docs/performance-baselines.md` by running function benchmarks locally and pipeline jobs on Databricks serverless.

### Function Benchmarks (Local)

- Run `uv run pytest src/tests/test_benchmarks.py -v --benchmark-only` to capture p95 percentiles.
- Add new benchmark for `compute_team_shape()` (D19 module) and `compute_team_shape_frame()` — both are critical-path for D21 Taipy page responsiveness.
- Update `docs/performance-baselines.md` with all measured p95 values.

### Pipeline Timings (Databricks Serverless)

Trigger each pipeline via Databricks Jobs API (`databricks jobs run-now`), record wall-clock from job run metadata:

| Pipeline | Entry Point | Method |
|----------|------------|--------|
| `compute_off_ball_xt` | `ingestion.off_ball_xt:main` | Jobs API |
| `compute_defcon_lite` | `ingestion.defcon_lite:main` | Jobs API |
| `compute_spadl_vaep` | `ingestion.spadl_vaep:main` | Jobs API |
| `compute_embeddings` | `ingestion.player_embeddings:main` | Jobs API |
| `compute_formations` | `ingestion.formations:main` | Jobs API (new, from D20) |

- Use existing Databricks workflow job (single task override per run).
- Record `start_time`, `end_time`, compute `duration_seconds` from run metadata.
- All runs on serverless compute (no cluster configuration needed).
- Update `docs/performance-baselines.md` pipeline timing table.

### Performance Budget for D19/D21

- `compute_team_shape()`: budget ≤ 1ms per frame for 10 outfield players (pure NumPy/scipy).
- `compute_team_shape_frame()`: budget ≤ 2ms per frame for both teams.
- Add these to `docs/performance-baselines.md` and CLAUDE.md performance budgets section.

---

## U5: Server-Side Player Search

### Summary

Enable native typeahead filtering on all Taipy dropdown selectors (1-line template change), and replace the `LIMIT 500` player query in Player Similarity with server-side `ILIKE` prefix matching against the full player corpus.

### Global: Enable `|filter|` on All Dropdowns

In `page_template.py`, add `|filter` to the selector widget rendering:

```python
# Before:
f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}|dropdown|label={w.label}|on_change={w.on_change}|>"

# After:
f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}|filter|dropdown|label={w.label}|on_change={w.on_change}|>"
```

Note: `{multi}` expands to `|multiple` or empty, so the attribute chain is `|filter|dropdown|` or `|filter|multiple|dropdown|`.

This restores the Streamlit-like type-to-filter behavior on every dropdown across all 13+ pages. Taipy v4.1.1 supports `filter` on dropdown selectors (resolved in v4.0.0.dev2, GitHub issue #1829).

### Player Similarity: Remove LIMIT 500

In `hf_taipy_app/src/filters.py`, modify `fetch_embedding_players()`:
- Remove the `LIMIT 500` cap.
- Replace with `LIMIT 2000` (reasonable upper bound for a single competition's player pool with embeddings).
- With `|filter|` enabled, users can type to narrow the full 2000-entry list client-side.

This is simpler than full server-side `ILIKE` search and sufficient for the current data scale (~11,918 players total, but filtered by competition and min_matches).

### Database Index

Add btree index in `scripts/create_indexes.py`:

```python
("idx_dim_players_display_name", "dim_players_synced",
 "(player_display_name)"),
```

This supports future server-side search if scale demands it, and improves `ORDER BY player_display_name` performance on the existing queries.

### Scope Decision

Full server-side `ILIKE` search (text input → PG query → dynamic LOV) is deferred. The `|filter|` + increased LIMIT approach solves the immediate UX problem (users couldn't find players) with minimal complexity. Server-side search becomes necessary only at 100K+ players, which requires a commercial data tier.

---

## Cross-Cutting

### Files Modified

| Area | Files |
|------|-------|
| **D20** | `pyproject.toml`, `src/ingestion/formations.py` (new), `dbt_project/models/marts/fct_formation_labels.sql` (new), `dbt_project/models/marts/_marts__models.yml`, `workflow-cards/wf-formations.yaml` (new), `scripts/create_indexes.py` |
| **D21** | `hf_taipy_app/src/pages/team_shape.py` (new), `hf_taipy_app/src/state/team_shape.py` (new), `hf_taipy_app/src/main.py`, `hf_taipy_app/src/template.py` |
| **O3** | `docs/performance-baselines.md`, `src/tests/test_benchmarks.py`, `CLAUDE.md` (performance budgets) |
| **U5** | `hf_taipy_app/src/page_template.py`, `hf_taipy_app/src/filters.py`, `scripts/create_indexes.py` |
| **Shared** | `TODO.md`, `ROADMAP.md` |

### New Synced Table (User Action Required)

- `fct_formation_labels_synced` — created via Databricks UI after `fct_formation_labels` Delta table exists.

### New PG Indexes

- `(match_id, team)` on `fct_formation_labels_synced` (D20)
- `(player_display_name)` on `dim_players_synced` (U5)
