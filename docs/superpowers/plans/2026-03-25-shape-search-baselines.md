# Shape, Search & Baselines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add EFPI formation detection pipeline (D20), Team Shape Taipy page (D21), pipeline performance baselines (O3), and native dropdown search across all pages (U5).

**Architecture:** D20 writes formation labels to a new Delta table via a `@workflow`-decorated Databricks pipeline with a full workflow card. D21 adds a multi-SubView Taipy page (Snapshot + Timeline) following the Defensive Impact / Movement & Pressing patterns exactly. O3 fills TBD cells in performance-baselines.md. U5 adds `|filter|` to all dropdown selectors globally.

**Tech Stack:** unravelsports (EFPI), Plotly (pitch + charts), Taipy 4.1.1 (page template), pytest-benchmark, Databricks Jobs API.

**Spec:** `docs/superpowers/specs/2026-03-25-shape-search-baselines-design.md`

---

## File Map

### D20 — EFPI Formation Detection Pipeline
| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/ingestion/formations.py` | Pipeline entry point, `@workflow("wf-formations", phase="heuristic")` |
| Create | `workflow-cards/wf-formations.yaml` | Workflow card manifest |
| Create | `dbt_project/models/marts/fct_formation_labels.sql` | dbt mart model |
| Modify | `dbt_project/models/marts/_marts__models.yml` | Add contract for new model |
| Modify | `pyproject.toml` | Add `unravelsports` dep + `compute_formations` entry point |
| Modify | `scripts/create_indexes.py` | Add btree index on `fct_formation_labels_synced` |
| Create | `src/tests/test_formations.py` | Unit tests |

### D21 — Team Shape Taipy Page
| Action | File | Responsibility |
|--------|------|---------------|
| Create | `hf_taipy_app/src/pages/team_shape.py` | PageConfig with two SubViews |
| Create | `hf_taipy_app/src/state/team_shape.py` | State module (ts_ prefix), fetch/render/callbacks |
| Modify | `hf_taipy_app/src/main.py` | Import + register page |
| Modify | `hf_taipy_app/src/template.py` | Glossary terms, PAGE_TERMS, page tuples, sidebar widgets |

### O3 — Pipeline Performance Baselines
| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `docs/performance-baselines.md` | Fill TBD cells |
| Modify | `src/tests/test_benchmarks.py` | Add team_shape benchmarks |
| Modify | `CLAUDE.md` | Add team_shape performance budgets |

### U5 — Server-Side Player Search
| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `hf_taipy_app/src/page_template.py` | Add `|filter|` to selector widget |
| Modify | `hf_taipy_app/src/filters.py` | Increase LIMIT 500 → 2000 |
| Modify | `scripts/create_indexes.py` | Add btree on `dim_players_synced(player_display_name)` |

### Shared
| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `TODO.md` | Mark D20, D21, O3, U5 complete |
| Modify | `ROADMAP.md` | Update Team Shape status |

---

### Task 1: Add `unravelsports` dependency and `compute_formations` entry point

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add dependency and entry point to pyproject.toml**

In `pyproject.toml`, add `"unravelsports>=0.5"` to the `dependencies` list (alphabetical order), and add `compute_formations` to `[project.scripts]`:

```toml
# In dependencies list (alphabetical):
"unravelsports>=0.5",

# In [project.scripts]:
compute_formations = "ingestion.formations:main"
```

- [ ] **Step 2: Sync environment**

Run: `uv sync`
Expected: Dependencies resolve, `unravelsports` installed.

- [ ] **Step 3: Verify import**

Run: `uv run python -c "from unravelsports import efpi; print('OK')"`
Expected: `OK` (or investigate the actual EFPI import path — the package API may differ).

---

### Task 2: Create workflow card `wf-formations.yaml`

**Files:**
- Create: `workflow-cards/wf-formations.yaml`

- [ ] **Step 1: Write the workflow card**

Create `workflow-cards/wf-formations.yaml` with the exact content from the spec (the full YAML block in the "Workflow Card" section of `docs/superpowers/specs/2026-03-25-shape-search-baselines-design.md`).

- [ ] **Step 2: Validate**

Run: `uv run validate_workflow_cards --validate workflow-cards/`
Expected: `wf-formations.yaml: OK` (all cards pass).

---

### Task 3: Write formation detection unit tests

**Files:**
- Create: `src/tests/test_formations.py`

- [ ] **Step 1: Write failing tests for the formation detection analytics function**

```python
"""Tests for EFPI formation detection wrapper."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.team_shape import compute_team_shape


def _make_formation_positions(formation: str) -> pd.DataFrame:
    """Create synthetic outfield player positions for a known formation.

    Returns a DataFrame with columns (player_id, team, x, y) for 10 outfield players
    arranged in a recognizable formation pattern on a 120x80 pitch.
    """
    # 4-3-3: 4 defenders, 3 midfielders, 3 forwards
    if formation == "4-3-3":
        positions = [
            # Back 4 (y ~20)
            ("p1", 15, 20), ("p2", 35, 20), ("p3", 45, 20), ("p4", 65, 20),
            # Midfield 3 (y ~45)
            ("p5", 25, 45), ("p6", 40, 45), ("p7", 55, 45),
            # Forward 3 (y ~70)
            ("p8", 20, 70), ("p9", 40, 70), ("p10", 60, 70),
        ]
    elif formation == "4-4-2":
        positions = [
            # Back 4
            ("p1", 15, 20), ("p2", 35, 20), ("p3", 45, 20), ("p4", 65, 20),
            # Midfield 4
            ("p5", 15, 45), ("p6", 30, 45), ("p7", 50, 45), ("p8", 65, 45),
            # Forward 2
            ("p9", 30, 70), ("p10", 50, 70),
        ]
    else:
        msg = f"Unknown formation: {formation}"
        raise ValueError(msg)

    return pd.DataFrame(
        [(pid, "home", x, y) for pid, x, y in positions],
        columns=["player_id", "team", "x", "y"],
    )


class TestFormationDetection:
    """Tests for the EFPI formation detection wrapper function."""

    def test_detect_known_433(self) -> None:
        """EFPI should detect a clear 4-3-3 arrangement."""
        from ingestion.formations import detect_formation_window

        df = _make_formation_positions("4-3-3")
        result = detect_formation_window(df[["x", "y"]].values)
        assert result is not None
        label, confidence = result
        assert isinstance(label, str)
        assert len(label) > 0  # e.g., "4-3-3" or similar
        assert 0.0 <= confidence <= 1.0

    def test_detect_known_442(self) -> None:
        """EFPI should detect a clear 4-4-2 arrangement."""
        from ingestion.formations import detect_formation_window

        df = _make_formation_positions("4-4-2")
        result = detect_formation_window(df[["x", "y"]].values)
        assert result is not None
        label, confidence = result
        assert isinstance(label, str)
        assert 0.0 <= confidence <= 1.0

    def test_too_few_players_returns_none(self) -> None:
        """Fewer than 3 players should return None (can't detect a formation)."""
        from ingestion.formations import detect_formation_window

        positions = np.array([[40.0, 40.0], [60.0, 40.0]])
        result = detect_formation_window(positions)
        assert result is None

    def test_output_schema(self) -> None:
        """Batch processing function should produce correct output columns."""
        from ingestion.formations import process_match_formations

        # Create a minimal tracking DataFrame for one match
        rng = np.random.default_rng(42)
        n_frames = 300  # 5 minutes at 1fps
        rows = []
        for frame in range(n_frames):
            for pid in range(10):
                rows.append({
                    "match_id": "test_match",
                    "period": 1,
                    "team": "home",
                    "player_id": f"p{pid}",
                    "timestamp_seconds": float(frame),
                    "x": rng.uniform(10, 110),
                    "y": rng.uniform(5, 75),
                })
        df = pd.DataFrame(rows)

        result = process_match_formations(df, team="home", window_seconds=300)
        assert isinstance(result, pd.DataFrame)
        expected_cols = {
            "match_id", "period", "team", "window_start_s",
            "window_end_s", "formation_label", "confidence",
        }
        assert expected_cols.issubset(set(result.columns))
        assert len(result) >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_formations.py -v`
Expected: FAIL (module `ingestion.formations` does not exist yet).

---

### Task 4: Implement formation detection pipeline

**Files:**
- Create: `src/ingestion/formations.py`

- [ ] **Step 1: Implement the pipeline module**

Create `src/ingestion/formations.py`. Key elements:

1. `detect_formation_window(positions: np.ndarray) -> tuple[str, float] | None` — wrapper around `unravelsports` EFPI. Takes an (N, 2) array of outfield player mean positions, returns `(label, confidence)` or `None` if too few players.
2. `process_match_formations(df: pd.DataFrame, team: str, window_seconds: int = 300) -> pd.DataFrame` — splits tracking data into time windows, computes mean positions per player per window, calls `detect_formation_window`.
3. `_run_pipeline(spark, catalog, schema)` — the `@workflow`-decorated function that reads `fct_tracking_frames`, applies `process_match_formations` per `(match_id, team)` group via `applyInPandas`, writes to `fct_formation_labels` with `replaceWhere` on `match_id`.
4. `main()` — CLI entry point with `parse_ingestion_args`, `get_spark_session`, `CostEstimateHook` registration.

Follow the exact patterns from `src/ingestion/line_breaking.py`:
- `parse_ingestion_args()` for CLI
- `get_spark_session()` for Spark
- `register_hook(CostEstimateHook(...))` for cost tracking
- Skip guard: query existing `match_id` values from output table
- `@workflow("wf-formations", phase="heuristic")` decorator
- `merge_delta_table()` or `replaceWhere` for idempotent writes

**Important:** Investigate the actual `unravelsports` EFPI API during implementation. The package may expose formation detection differently than assumed. Read its source/docs and adapt `detect_formation_window` accordingly.

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_formations.py -v`
Expected: All 4 tests PASS.

- [ ] **Step 3: Run linting**

Run: `uv run ruff check src/ingestion/formations.py src/tests/test_formations.py`
Run: `uv run pyright src/ingestion/formations.py`
Expected: 0 errors.

---

### Task 5: Create dbt mart model for formation labels

**Files:**
- Create: `dbt_project/models/marts/fct_formation_labels.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Create the dbt model**

Create `dbt_project/models/marts/fct_formation_labels.sql`:

```sql
{{
    config(
        materialized='incremental',
        unique_key='formation_label_id',
        on_schema_change='fail',
        liquid_clustered_by='match_id',
        tblproperties={
            'delta.autoOptimize.autoCompact': 'true',
            'delta.autoOptimize.optimizeWrite': 'true',
        },
    )
}}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'team', 'window_start_s']) }} as formation_label_id,
        match_id,
        period,
        team,
        window_start_s,
        window_end_s,
        formation_label,
        confidence,
        _ingested_at
    from {{ source('bronze', 'formation_labels') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
)

select * from source
```

**Note:** If `formation_labels` is written directly to gold (not bronze), adjust the source reference. The pipeline writes to `{catalog}.gold.fct_formation_labels` — so this may be a simple passthrough or the pipeline writes to bronze and dbt promotes to gold. Follow the pattern established by `fct_line_breaking_results` or `fct_pausa_values`.

- [ ] **Step 2: Add contract to `_marts__models.yml`**

Add to `dbt_project/models/marts/_marts__models.yml`:

```yaml
  - name: fct_formation_labels
    description: "EFPI formation detection results per team per 5-minute window"
    config:
      contract:
        enforced: true
    columns:
      - name: formation_label_id
        data_type: string
        description: "Surrogate key (match_id + period + team + window_start_s)"
        tests:
          - unique
          - not_null
      - name: match_id
        data_type: string
        tests:
          - not_null
      - name: period
        data_type: int
        tests:
          - not_null
      - name: team
        data_type: string
        tests:
          - not_null
          - accepted_values:
              values: ['home', 'away']
      - name: window_start_s
        data_type: double
      - name: window_end_s
        data_type: double
      - name: formation_label
        data_type: string
        tests:
          - not_null
      - name: confidence
        data_type: double
      - name: _ingested_at
        data_type: timestamp
```

- [ ] **Step 3: Validate dbt model compiles**

Run: `cd dbt_project && dbt compile --select fct_formation_labels`
Expected: Compiles without errors.

---

### Task 6: Add PG indexes for formation labels and player search

**Files:**
- Modify: `scripts/create_indexes.py`

- [ ] **Step 1: Add indexes to BTREE_INDEXES list**

In `scripts/create_indexes.py`, add to the `BTREE_INDEXES` list:

```python
# D20: Formation labels — primary access pattern from Taipy (match + team filter)
("idx_formation_labels_match_team", "fct_formation_labels_synced",
 "(match_id, team)"),

# U5: Player name search — supports ORDER BY and future ILIKE prefix matching
("idx_dim_players_display_name", "dim_players_synced",
 "(player_display_name)"),
```

- [ ] **Step 2: Verify script runs (dry run)**

Run: `uv run python scripts/create_indexes.py --verify`
Expected: New indexes listed as "missing" (synced tables don't exist locally). No errors in the script itself.

---

### Task 7: Enable `|filter|` on all dropdown selectors (U5)

**Files:**
- Modify: `hf_taipy_app/src/page_template.py`

- [ ] **Step 1: Add `|filter|` to dropdown selector rendering**

In `hf_taipy_app/src/page_template.py`, find the dropdown rendering block (around line 128-130):

```python
# Before (line 128-130):
    if w.kind in ("dropdown", "dropdown_multi"):
        multi = "|multiple" if w.kind == "dropdown_multi" else ""
        parts.append(
            f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}|dropdown|label={w.label}|on_change={w.on_change}|>"
        )

# After:
    if w.kind in ("dropdown", "dropdown_multi"):
        multi = "|multiple" if w.kind == "dropdown_multi" else ""
        parts.append(
            f"<|{lb}{w.var}{rb}|selector|lov={lb}{w.lov}{rb}{multi}|filter|dropdown|label={w.label}|on_change={w.on_change}|>"
        )
```

This single change adds type-to-filter to every dropdown selector across all 13+ pages.

- [ ] **Step 2: Run linting**

Run: `uv run ruff check hf_taipy_app/src/page_template.py`
Expected: 0 errors.

---

### Task 8: Increase player query LIMIT (U5)

**Files:**
- Modify: `hf_taipy_app/src/filters.py`

- [ ] **Step 1: Change LIMIT 500 to LIMIT 2000 in `fetch_embedding_players`**

In `hf_taipy_app/src/filters.py`, find the query at line 370:

```python
# Before (line 370):
        f"ORDER BY p.player_display_name LIMIT 500",

# After:
        f"ORDER BY p.player_display_name LIMIT 2000",
```

- [ ] **Step 2: Run linting**

Run: `uv run ruff check hf_taipy_app/src/filters.py`
Expected: 0 errors.

---

### Task 9: Create Team Shape page config (D21)

**Files:**
- Create: `hf_taipy_app/src/pages/team_shape.py`

- [ ] **Step 1: Write the PageConfig**

```python
"""Team Shape page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_ADVANCED,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    SubView,
    build_page,
)

page_config = PageConfig(
    title="Team Shape",
    icon="polyline",
    nav_section=NAV_ADVANCED,
    freshness_var="ts_data_freshness",
    description=(
        "Formation detection (EFPI template matching) and spatial metrics from tracking data. "
        "Tracking data available for ~20 matches from Metrica, IDSSE, and SkillCorner."
    ),
    citations=[
        Citation("Bekkers & Dabadghao (2025)", "https://arxiv.org/abs/2506.23843"),
        Citation("Frencken et al. (2011)"),
        Citation("Bourbousson et al. (2010)"),
    ],
    sub_views=[
        SubView(
            condition='selected_sub_view == "Snapshot"',
            content=[
                ContentRow([ContentBlock("chart", "ts_snapshot_figure", condition="ts_snapshot_figure is not None")]),
            ],
            warning_var="ts_warning_text",
            empty_message="Select a tracking match to begin.",
            empty_condition="ts_snapshot_figure is None and len(tracking_match_lov) > 0",
            fallback_empty_message=(
                "No tracking data available. This page requires tracking data (available for ~20 matches)."
            ),
            fallback_empty_condition="ts_snapshot_figure is None and len(tracking_match_lov) == 0",
            scope_vars=["ts_snapshot_caption"],
            metrics=[
                Metric(
                    "Formation",
                    "ts_formation",
                    "EFPI template match — most common formation in the selected window (Bekkers & Dabadghao 2025).",
                ),
                Metric(
                    "Team Length",
                    "ts_team_length",
                    "Distance between deepest and most advanced outfield players. <30m = compact, >40m = stretched (Fradua et al. 2013).",
                    delta_var="ts_length_delta",
                ),
                Metric(
                    "Team Width",
                    "ts_team_width",
                    ">38m in possession = good width creation. Distance between widest outfield players.",
                    delta_var="ts_width_delta",
                ),
                Metric(
                    "Defensive Line",
                    "ts_def_line_height",
                    "Average position of the defensive line as % of pitch length. 0% = own goal, 100% = opponent. >50% = high press, <35% = deep block.",
                ),
                Metric(
                    "Hull Area",
                    "ts_hull_area",
                    "Convex hull area in m². ~1,000 m² defending, ~1,500 m² attacking (Frencken et al. 2011).",
                ),
                Metric(
                    "Stretch Index",
                    "ts_stretch_index",
                    "Mean distance from team centroid in meters (Bourbousson et al. 2010). Lower = more compact.",
                ),
                Metric(
                    "Inter-Line Gaps",
                    "ts_inter_line_gaps",
                    "Distance between def↔mid and mid↔att line centroids. <12m = compact, >18m = exposed.",
                ),
            ],
        ),
        SubView(
            condition='selected_sub_view == "Timeline"',
            content=[
                ContentRow([
                    ContentBlock(
                        "chart", "ts_timeline_figure",
                        header="Shape Metrics Over Time",
                        condition="ts_timeline_figure is not None",
                    ),
                ]),
                ContentRow([
                    ContentBlock(
                        "chart", "ts_formation_figure",
                        header="Formation Labels (5-min windows)",
                        condition="ts_formation_figure is not None",
                    ),
                ]),
                ContentRow([
                    ContentBlock(
                        "table", "ts_phase_comparison",
                        header="Phase Comparison",
                    ),
                ]),
            ],
            warning_var="ts_warning_text",
            empty_message="Select a tracking match to begin.",
            empty_condition="ts_timeline_figure is None and len(tracking_match_lov) > 0",
            fallback_empty_message=(
                "No tracking data available. This page requires tracking data (available for ~20 matches)."
            ),
            fallback_empty_condition="ts_timeline_figure is None and len(tracking_match_lov) == 0",
            metrics=[
                Metric("Avg Length", "ts_avg_length", "Match average team length in meters."),
                Metric("Avg Width", "ts_avg_width", "Match average team width in meters."),
                Metric(
                    "Formation Changes",
                    "ts_formation_changes",
                    "Number of distinct formation transitions detected across the match.",
                ),
                Metric(
                    "Compactness Δ",
                    "ts_compactness_delta",
                    "Change in team length between 1st and 2nd half. Positive = more stretched.",
                    delta_var="ts_compactness_delta_fmt",
                ),
            ],
        ),
    ],
)
page_md = build_page(page_config)
```

- [ ] **Step 2: Run linting**

Run: `uv run ruff check hf_taipy_app/src/pages/team_shape.py`
Expected: 0 errors.

---

### Task 10: Create Team Shape state module (D21)

**Files:**
- Create: `hf_taipy_app/src/state/team_shape.py`

- [ ] **Step 1: Implement the state module**

This is the largest single file. Follow the patterns from `state/pitch_control.py` exactly:

1. **Module-level state variables** (all `ts_` prefixed):
   - Controls: `ts_selected_team`, `ts_selected_half`, `ts_phase_average`, `ts_elapsed_seconds`, `ts_show_hull`, `ts_show_formation_lines`
   - LOVs: `ts_team_lov`, `ts_half_lov`
   - Metrics: `ts_formation`, `ts_team_length`, `ts_team_width`, `ts_def_line_height`, `ts_hull_area`, `ts_stretch_index`, `ts_inter_line_gaps`, `ts_length_delta`, `ts_width_delta`
   - Timeline metrics: `ts_avg_length`, `ts_avg_width`, `ts_formation_changes`, `ts_compactness_delta`, `ts_compactness_delta_fmt`
   - Charts: `ts_snapshot_figure`, `ts_timeline_figure`, `ts_formation_figure`
   - Tables: `ts_phase_comparison`
   - Status: `ts_warning_text`, `ts_snapshot_caption`, `ts_data_freshness`
   - Slider bounds: `ts_max_seconds`

2. **Fetch functions** (all `@ttl_cache`):
   - `_fetch_tracking_data(match_id, period)` → query `fct_tracking_frames_synced`
   - `_fetch_formation_labels(match_id, team)` → query `fct_formation_labels_synced`
   - `_fetch_match_events(match_id)` → goals/subs for timeline annotations

3. **Pitch rendering** — `_draw_pitch_shapes()` returns a list of `plotly.graph_objects.layout.Shape` for touchlines, penalty areas, center circle, arcs. Coordinate system: 120×80.

4. **Snapshot rendering** — `_render_snapshot(state)`:
   - Get tracking data for selected frame or phase average
   - Filter to selected team
   - Compute `compute_team_shape()` from `analytics.team_shape`
   - Build Plotly figure with pitch shapes, player scatter, hull polygon (`fill='toself'`, opacity 0.15), formation lines (dashed)
   - Update metric state vars

5. **Timeline rendering** — `_render_timeline(state)`:
   - Compute shape metrics at 1fps across full match
   - Build Plotly time-series figure (length, width, def line height)
   - Build formation strip figure from `fct_formation_labels`
   - Build phase comparison DataFrame
   - Update metric state vars

6. **Callbacks**: `ts_on_team_change`, `ts_on_half_change`, `ts_on_seconds_change`, `ts_on_phase_toggle`, `ts_on_hull_toggle`, `ts_on_formation_lines_toggle`.

7. **Registration**: `register_page_refresher("Team-Shape", ts_refresh)`.

- [ ] **Step 2: Run linting**

Run: `uv run ruff check hf_taipy_app/src/state/team_shape.py`
Run: `uv run pyright hf_taipy_app/src/state/team_shape.py`
Expected: 0 errors.

---

### Task 11: Register Team Shape page in main.py and template.py

**Files:**
- Modify: `hf_taipy_app/src/main.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Add imports and PageEntry in main.py**

In `hf_taipy_app/src/main.py`, add:

```python
# With other page imports (after pitch_control):
from pages.team_shape import page_config as team_shape_config
from pages.team_shape import page_md as team_shape_page

# With other state imports (after pitch_control):
from state.team_shape import *  # noqa: F403

# In PAGE_REGISTRY, after Pitch-Control entry:
    PageEntry("Team-Shape", team_shape_config, team_shape_page),
```

- [ ] **Step 2: Add glossary terms and PAGE_TERMS in template.py**

In `hf_taipy_app/src/template.py`, add to `GLOSSARY` dict:

```python
"Team Shape": "Spatial metrics describing how a team is spread across the pitch — length, width, hull area, stretch index.",
"Convex Hull": "Smallest polygon enclosing all outfield players. Area indicates territorial extent (~1,000 m² defending, ~1,500 m² attacking).",
"Stretch Index": "Mean distance of all outfield players from the team centroid (Bourbousson et al. 2010). Lower = more compact.",
"EFPI": "Elastic Formation and Position Identification — template matching algorithm for automatic formation detection (Bekkers & Dabadghao 2025).",
"Defensive Line Height": "Average position of the defensive line as % of pitch length. 0% = own goal, 100% = opponent goal.",
"Inter-Line Gaps": "Distance between defensive-midfield and midfield-attack line centroids. <12m = compact, >18m = exposed.",
"Team Length": "Distance between deepest and most advanced outfield players along the goal-to-goal axis.",
"Team Width": "Distance between widest outfield players along the touchline-to-touchline axis.",
```

Add to `PAGE_TERMS`:

```python
"Team-Shape": [
    "Team Shape", "Convex Hull", "Stretch Index", "EFPI",
    "Defensive Line Height", "Inter-Line Gaps", "Team Length", "Team Width",
],
```

Add `"Team-Shape"` to the relevant page tuples:
- `_TRACKING_PROVIDER_PAGES` — add `"Team-Shape"` (needs Provider + Tracking Match filters)
- `_SUB_VIEW_PAGES` — add `"Team-Shape"` (uses sub_view selector)
- `_FILTER_HEADER_PAGES` — add `"Team-Shape"`

Add Team Shape-specific sidebar widgets to `_FILTER_WIDGETS`:

```python
# Team Shape page-specific widgets (after Pitch Control section)
_TEAM_SHAPE_PAGES = ("Team-Shape",)

SidebarWidget(
    "dropdown",
    "ts_selected_team",
    "Team",
    "ts_on_team_change",
    condition=f"current_page in {_TEAM_SHAPE_PAGES}",
    lov="ts_team_lov",
    depends_on="selected_tracking_match",
    help="Select home or away team for shape analysis.",
),
SidebarWidget(
    "dropdown",
    "ts_selected_half",
    "Half",
    "ts_on_half_change",
    condition=f"current_page in {_TEAM_SHAPE_PAGES}",
    lov="ts_half_lov",
    depends_on="selected_tracking_match",
    help="Filter by match period.",
),
SidebarWidget(
    "toggle",
    "ts_phase_average",
    "Phase Average",
    "ts_on_phase_toggle",
    condition=f'current_page in {_TEAM_SHAPE_PAGES} and selected_sub_view == "Snapshot"',
    help="Toggle between single-frame view and phase-averaged positions.",
),
SidebarWidget(
    "slider",
    "ts_elapsed_seconds",
    "Time",
    "ts_on_seconds_change",
    condition=f'current_page in {_TEAM_SHAPE_PAGES} and selected_sub_view == "Snapshot" and not ts_phase_average and ts_max_seconds > 1',
    slider_min="0",
    slider_max="ts_max_seconds",
    change_delay=300,
    help="Scrub through match time to view team shape at a specific moment.",
),
SidebarWidget(
    "toggle",
    "ts_show_hull",
    "Show Hull",
    "ts_on_hull_toggle",
    condition=f'current_page in {_TEAM_SHAPE_PAGES} and selected_sub_view == "Snapshot"',
    help="Toggle convex hull overlay showing team territorial extent.",
),
SidebarWidget(
    "toggle",
    "ts_show_formation_lines",
    "Show Lines",
    "ts_on_formation_lines_toggle",
    condition=f'current_page in {_TEAM_SHAPE_PAGES} and selected_sub_view == "Snapshot"',
    help="Toggle dashed formation lines connecting players by detected line clusters.",
),
```

Also add Team Shape Provider/Tracking Match widgets with page-specific conditions (same pattern as Movement-Pressing duplicates):

```python
SidebarWidget(
    "dropdown",
    "selected_provider",
    "Provider",
    "on_provider_change",
    condition=f"current_page in {_TEAM_SHAPE_PAGES}",
    lov="provider_lov",
),
SidebarWidget(
    "dropdown",
    "selected_tracking_match",
    "Tracking Match",
    "on_tracking_match_change",
    condition=f"current_page in {_TEAM_SHAPE_PAGES}",
    lov="tracking_match_lov",
    depends_on="selected_provider",
),
```

- [ ] **Step 3: Run linting on both files**

Run: `uv run ruff check hf_taipy_app/src/main.py hf_taipy_app/src/template.py`
Expected: 0 errors.

---

### Task 12: Add team shape benchmarks (O3)

**Files:**
- Modify: `src/tests/test_benchmarks.py`

- [ ] **Step 1: Add benchmark test for compute_team_shape**

Add to `src/tests/test_benchmarks.py` in the `TestBenchmarks` class:

```python
def test_bench_team_shape(self, benchmark: Any, players_df: pd.DataFrame) -> None:
    """Team shape computation: budget <=1ms for 10 outfield players."""
    from analytics.team_shape import TeamShapeParams, compute_team_shape

    home_df = players_df[players_df["team"] == "home"].copy()
    params = TeamShapeParams()

    result = benchmark(compute_team_shape, home_df, params)
    assert result is not None
    assert result.convex_hull_area > 0

def test_bench_team_shape_frame(self, benchmark: Any, players_df: pd.DataFrame) -> None:
    """Both teams shape: budget <=2ms for 22 players."""
    from analytics.team_shape import TeamShapeParams, compute_team_shape_frame

    params = TeamShapeParams()

    result = benchmark(compute_team_shape_frame, players_df, params)
    assert "home" in result
    assert "away" in result
```

- [ ] **Step 2: Run benchmarks**

Run: `uv run pytest src/tests/test_benchmarks.py -v --benchmark-only`
Expected: All benchmarks pass, including the two new team shape benchmarks.

---

### Task 13: Fill performance baselines (O3)

**Files:**
- Modify: `docs/performance-baselines.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Run full benchmark suite and capture p95**

Run: `uv run pytest src/tests/test_benchmarks.py -v --benchmark-only --benchmark-json=benchmark_results.json`

Parse p95 from the JSON output and update `docs/performance-baselines.md` function benchmark table with measured values.

- [ ] **Step 2: Trigger Databricks pipeline jobs and record timings**

Use Databricks CLI or Jobs API to trigger each pipeline:

```bash
# Get the job ID
databricks jobs list --output JSON | python -c "import sys,json; [print(j['job_id'], j['settings']['name']) for j in json.load(sys.stdin)['jobs']]"

# Run each pipeline (use the actual job ID and task key)
databricks jobs run-now --job-id <JOB_ID> --python-params '["--catalog", "soccer_analytics", "--schema", "dev_gold"]'
```

Record `start_time` and `end_time` from `databricks jobs get-run --run-id <RUN_ID>`. Update `docs/performance-baselines.md` pipeline timing table.

- [ ] **Step 3: Add team shape performance budgets to CLAUDE.md**

Add to the Performance Budgets section in `CLAUDE.md`:

```markdown
- **Team shape computation**: ≤1ms per frame for 10 outfield players (benchmark baseline)
- **Team shape frame (both teams)**: ≤2ms per frame for 22 players (benchmark baseline)
```

---

### Task 14: Update TODO.md and ROADMAP.md

**Files:**
- Modify: `TODO.md`
- Modify: `ROADMAP.md`

- [ ] **Step 1: Move completed items in TODO.md**

Move D20, D21, O3, U5 from "On Deck" to "Completed On-Deck Items" with resolution descriptions.

- [ ] **Step 2: Update ROADMAP.md**

Update "Team Shape Analysis" section status to reflect D20 (EFPI) and D21 (Taipy page) completion. Stage 1 is now complete. Stage 2 remains blocked on SkillCorner DoD.

---

### Task 15: Full lint, type check, and test pass

**Files:** All modified files.

- [ ] **Step 1: Run full quality checks**

```bash
uv run ruff check src/
uv run ruff format --check src/
uv run pyright src/
uv run pytest src/tests/ -v
```

Expected: 0 lint errors, 0 type errors, all tests pass.

- [ ] **Step 2: Run Taipy app linting**

```bash
uv run ruff check hf_taipy_app/src/
uv run pyright hf_taipy_app/src/
```

Expected: 0 errors.

---

### Task 16: Puppeteer verification of Team Shape page

After deploying to staging, verify the Team Shape page renders correctly with live data:

- [ ] **Step 1: Navigate to Team Shape page**
- [ ] **Step 2: Select a tracking match (Metrica or IDSSE)**
- [ ] **Step 3: Verify Snapshot view renders pitch with players, hull, formation lines**
- [ ] **Step 4: Verify Timeline view renders shape metrics chart, formation strip, phase table**
- [ ] **Step 5: Verify dropdown filters have typeahead (type a letter → list filters)**
- [ ] **Step 6: Take screenshots as evidence**
