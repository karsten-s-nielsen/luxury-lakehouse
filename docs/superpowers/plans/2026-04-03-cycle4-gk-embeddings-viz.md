# Cycle 4: GK Analytics, Embedding Explorer & Shape Graph Visualization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver two new Taipy pages (Goalkeeper Analytics, Tactical Positions), a standalone Embedding Explorer HF Space, and fix the broken OBSO pipeline — with data pipeline fixes done first so all downstream UI has complete data.

**Architecture:** Four sequential phases with three merge points designed to minimize conflict with concurrent D32 (ScoutGPT) work. Phase 1 fixes data pipelines (OBSO import, D39 dbt stubs, GK stat vector). Phase 2 builds two new Taipy sub-view pages following the established template pattern. Phase 3 creates the Embedding Explorer Space and embeds an Atlas neighborhood widget in Player Similarity. Phase 4 polishes and verifies.

**Tech Stack:** Python 3.10, PySpark, dbt (Databricks), Taipy, Plotly, mplsoccer, Embedding Atlas (JS), DuckDB-WASM, huggingface_hub, UMAP, Terraform HCL.

**Design spec:** `docs/superpowers/specs/2026-04-03-cycle4-gk-embeddings-viz-design.md`

---

## Phase 1: Data Pipeline Fixes

### Task 1: Fix `import_obso_results.py` — Add HF Hub Download Bridge

**Files:**
- Modify: `src/ingestion/import_obso_results.py`
- Test: `src/tests/test_import_obso_results.py` (create if absent, or add to existing test)

The script currently reads directly from a UC Volume path. The HF Jobs script (`compute_obso_hf.py`) publishes to HF Hub, not the Volume. We need to add a download step, following the pattern in `import_psxg_predictions.py` (lines 81–93).

- [ ] **Step 1: Read current import_obso_results.py and import_psxg_predictions.py**

Study both files to understand the current patterns. Key difference: `import_psxg_predictions.py` has `HF_REPO` constant and uses `hf_hub_download` + `shutil.copy2` to stage from HF Hub to Volume. `import_obso_results.py` has no HF download — just reads directly from Volume.

- [ ] **Step 2: Add HF Hub download constants and import**

At the top of `src/ingestion/import_obso_results.py`, add:

```python
from huggingface_hub import hf_hub_download

HF_REPO = "luxury-lakehouse/obso-pausa-values"
PAUSA_TABLE = "pausa_raw_scores"
OBSO_TABLE = "obso_surfaces"
```

- [ ] **Step 3: Add `_download_from_hf` helper function**

Before `run_pipeline`, add a download function following the PSxG pattern (`import_psxg_predictions.py` lines 81–93):

```python
def _download_from_hf(repo_id: str, filename: str, volume_path: str) -> Path:
    """Download a file from HF Hub to UC Volume staging path."""
    local_path = hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        repo_type="dataset",
    )
    volume_file = Path(volume_path) / Path(filename).name
    volume_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_path, volume_file)
    logger.info("Staged %s → %s", filename, volume_file)
    return volume_file
```

Add `import shutil` and `from pathlib import Path` to the imports.

- [ ] **Step 4: Add `--hf-repo` argument to argparse**

In `main()`, add a new argument after `--volume-path`:

```python
parser.add_argument(
    "--hf-repo",
    default=HF_REPO,
    help="HF Hub dataset repo to download OBSO results from (default: %(default)s)",
)
```

- [ ] **Step 5: Add download step at the start of `run_pipeline`**

At the beginning of `run_pipeline`, before reading from Volume, add:

```python
# Download from HF Hub to UC Volume staging path
_download_from_hf(hf_repo, "data/pausa_raw_scores.parquet", volume_path)
```

Update the `run_pipeline` signature to accept `hf_repo: str` parameter, and update the `main()` call to pass `args.hf_repo`.

- [ ] **Step 6: Update workflow card `wf-import-obso.yaml`**

Change `inputs.datasets[0]` from `source: uc-volume` to `source: huggingface`, and update the `id` to `luxury-lakehouse/obso-pausa-values`. This reflects the new data flow.

- [ ] **Step 7: Run linter and type checker**

```bash
uv run ruff check src/ingestion/import_obso_results.py
uv run pyright src/ingestion/import_obso_results.py
```

Expected: PASS with zero violations.

---

### Task 2: Fix Terraform `depends_on` for Import Tasks

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

Both `import_obso_results` (line 672) and `import_psxg_predictions` (line 711) have no `depends_on` block. They float free in the DAG.

- [ ] **Step 1: Add `depends_on` to `import_obso_results` task**

At `terraform/modules/workflows/main.tf`, inside the `import_obso_results` task block (after line 673), add:

```hcl
depends_on {
  task_key = "compute_elastic_sync"
}
```

This ensures OBSO inputs exist before the import runs. The HF Jobs compute is manual, but this prevents the import from firing before the data it needs is ready on automated runs.

- [ ] **Step 2: Add explicit `--volume-path` to `import_obso_results` parameters**

Replace the parameters block (lines 681–684) with:

```hcl
parameters = [
  "--catalog", var.catalog_name,
  "--schema", "bronze",
  "--volume-path", "/Volumes/${var.catalog_name}/dev_gold/model_weights/obso"
]
```

- [ ] **Step 3: Add `depends_on` to `import_psxg_predictions` task**

Inside the `import_psxg_predictions` task block (after line 712), add:

```hcl
depends_on {
  task_key = "export_shots_on_target"
}
```

This ensures the shots-on-target export (which feeds the PSxG training pipeline) has run before import.

- [ ] **Step 4: Verify Terraform validates**

```bash
cd terraform/environments/dev && terraform validate
```

Expected: `Success! The configuration is valid.`

---

### Task 3: Create dbt Source + Staging Model for PSxG Predictions

**Files:**
- Create: `dbt_project/models/staging/psxg/_psxg__sources.yml`
- Create: `dbt_project/models/staging/psxg/stg_psxg__predictions.sql`

The `bronze.psxg_predictions` table exists (written by `import_psxg_predictions`) but has no dbt source definition or staging model. `fct_goalkeeper_stats.sql` needs this to JOIN PSxG data.

- [ ] **Step 1: Create source YAML**

Create `dbt_project/models/staging/psxg/_psxg__sources.yml`:

```yaml
version: 2

sources:
  - name: psxg
    description: >
      Post-Shot Expected Goals predictions from the PSxG logistic regression model.
      Imported from HF Hub dataset luxury-lakehouse/psxg-predictions via
      import_psxg_predictions entry point.
    database: soccer_analytics
    schema: bronze
    loader: python_wheel
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 168, period: hour}
        error_after: {count: 336, period: hour}

    tables:
      - name: psxg_predictions
        description: >
          Per-shot PSxG probability predictions. One row per shot event.
          event_id is the shot's surrogate key (matches fct_shots.shot_id).
          player_id is the SHOOTER (not the goalkeeper).
          psxg is NaN for off-target shots.
        columns:
          - name: event_id
            description: Shot event surrogate key (FK to fct_shots.shot_id)
          - name: match_id
            description: Match identifier (string)
          - name: player_id
            description: Shooter player_id (NOT the goalkeeper)
          - name: psxg
            description: Post-shot xG probability (0-1, NaN for off-target)
          - name: _ingested_at
            description: UTC timestamp of ingestion
```

- [ ] **Step 2: Create staging model**

Create `dbt_project/models/staging/psxg/stg_psxg__predictions.sql`:

```sql
-- stg_psxg__predictions.sql
-- Deduplicate PSxG predictions by event_id (latest _ingested_at wins).
-- Source: bronze.psxg_predictions (imported from HF Hub).

with source as (
    select * from {{ source('psxg', 'psxg_predictions') }}
),

deduplicated as (
    select
        event_id,
        match_id,
        player_id,
        psxg,
        _ingested_at,
        row_number() over (
            partition by event_id
            order by _ingested_at desc
        ) as _rn
    from source
)

select
    cast(event_id as string)   as event_id,
    cast(match_id as string)   as match_id,
    cast(player_id as string)  as player_id,
    cast(psxg as double)       as psxg,
    cast(_ingested_at as timestamp) as _ingested_at
from deduplicated
where _rn = 1
```

- [ ] **Step 3: Verify dbt compiles**

```bash
cd dbt_project && dbt compile --select stg_psxg__predictions
```

Expected: Compiles without error.

---

### Task 4: Update `fct_goalkeeper_stats.sql` — Replace D39 NULL Stubs

**Files:**
- Modify: `dbt_project/models/marts/fct_goalkeeper_stats.sql`

Replace 5 `cast(null as ...)` stubs with actual data from two sources: sweeper metrics computed inline from existing CTEs, and PSxG columns from the new staging model.

- [ ] **Step 1: Add sweeper_stats CTE**

After the existing `save_stats` CTE in `fct_goalkeeper_stats.sql`, add a new CTE that computes sweeper metrics from the `gk_actions` CTE (which already exists in the model). Reference implementation: `src/analytics/goalkeeper.py` lines 395–432.

```sql
sweeper_stats as (
    select
        ga.player_id,
        ga.match_id,
        avg(ga.start_x) as avg_defensive_action_distance,
        case
            when m.minutes_played > 0 then
                sum(case when ga.start_x < 16.5 then 0 else 1 end)
                * (90.0 / m.minutes_played)
            else null
        end as actions_outside_box_per_90
    from gk_actions ga
    inner join player_match_minutes m
        on ga.player_id = m.player_id
        and ga.match_id = m.match_id
    where ga.action_type in (
        'tackle', 'interception', 'clearance', 'block'
    )
    group by ga.player_id, ga.match_id, m.minutes_played
),
```

Note: The exact action types for "defensive actions" should match `compute_sweeper_metrics()` in `goalkeeper.py`. Verify the filter against the Python implementation before finalising.

- [ ] **Step 2: Add psxg_agg CTE**

Add a CTE that aggregates PSxG predictions per goalkeeper per match. The join is through shots — `psxg_predictions.event_id` matches shot events, and we need to resolve the *opposing* GK (the GK facing the shot, not the shooter).

```sql
psxg_agg as (
    select
        gm.player_id as gk_player_id,
        gm.match_id,
        sum(p.psxg) as psxg_faced,
        sum(case when s.shot_outcome = 'Goal' then 1 else 0 end) as goals_conceded
    from gk_matches gm
    inner join {{ ref('dim_players') }} dp
        on gm.player_id = dp.player_id
    inner join {{ ref('fct_shots') }} s
        on gm.match_id = s.match_id
        and s.team_id != dp.team_id  -- shots AGAINST the GK's team
    left join {{ ref('stg_psxg__predictions') }} p
        on s.shot_id = p.event_id
    where p.psxg is not null  -- exclude off-target (NaN)
    group by gm.player_id, gm.match_id
),
```

Note: The exact join path depends on how `fct_shots` exposes `team_id` and `shot_outcome`. Verify column names against `dbt_project/models/marts/fct_shots.sql` before finalising. The `gk_matches` CTE (already in the model) provides the GK's `player_id` and `match_id`.

- [ ] **Step 3: Replace NULL stubs in final SELECT**

Replace lines 298–303 (the 5 NULL stubs) with JOINs to the new CTEs:

```sql
pa.psxg_faced,
pa.goals_conceded,
pa.psxg_faced - pa.goals_conceded as goals_prevented,
sw.avg_defensive_action_distance,
sw.actions_outside_box_per_90
```

Add LEFT JOINs in the FROM clause:

```sql
left join psxg_agg pa
    on gm.player_id = pa.gk_player_id
    and gm.match_id = pa.match_id
left join sweeper_stats sw
    on gm.player_id = sw.player_id
    and gm.match_id = sw.match_id
```

- [ ] **Step 4: Normalise `goals_conceded` type**

The live SELECT currently casts `goals_conceded` as `int` but the empty-schema fallback uses `bigint`. Normalise both to `int` for consistency with the contract in `_marts__models.yml`.

- [ ] **Step 5: Verify dbt builds**

```bash
cd dbt_project && dbt build --select fct_goalkeeper_stats --vars '{"goalkeeper_enabled": true}'
```

Expected: Model builds successfully. Verify non-NULL values for all 5 previously-stubbed columns.

---

### Task 5: Extend GK Stat Vector to 13 Features

**Files:**
- Modify: `src/ingestion/player_embeddings_common.py`
- Test: `src/tests/test_player_embeddings.py`

Extend `STAT_FEATURES_BY_GROUP["Goalkeeper"]` from 4 → 13 features, matching outfield dimensionality. All 13 features are now available from `fct_goalkeeper_stats` (populated after Task 4).

- [ ] **Step 1: Write failing test**

Add to `src/tests/test_player_embeddings.py`:

```python
def test_goalkeeper_stat_features_has_13_dimensions():
    """GK stat vector must match outfield dimensionality for pgvector index compatibility."""
    from ingestion.player_embeddings_common import STAT_FEATURES_BY_GROUP

    gk_features = STAT_FEATURES_BY_GROUP["Goalkeeper"]
    outfield_features = STAT_FEATURES_BY_GROUP["Defender"]
    assert len(gk_features) == len(outfield_features) == 13
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest src/tests/test_player_embeddings.py::test_goalkeeper_stat_features_has_13_dimensions -v
```

Expected: FAIL — `assert 4 == 13`.

- [ ] **Step 3: Update STAT_FEATURES_BY_GROUP["Goalkeeper"]**

In `src/ingestion/player_embeddings_common.py` (line 30), replace the 4-feature tuple:

```python
"Goalkeeper": (
    "save_pct",
    "gk_xt_per_pass",
    "launch_rate",
    "claim_success_rate",
    "goals_prevented_per_90",
    "psxg_per_shot_faced",
    "avg_defensive_action_distance",
    "actions_outside_box_per_90",
    "clean_sheet_pct",
    "saves_per_90",
    "distribution_passes_per_90",
    "gk_xt_delta_total_per_90",
    "punches_per_90",
),
```

- [ ] **Step 4: Update `_load_goalkeeper_stats` query**

In the same file, update the `_load_goalkeeper_stats` function (line 293+) to SELECT the new columns. The SQL query builds column list dynamically from the `features` parameter, so the new column names must match columns in `fct_goalkeeper_stats`. Some new features are derived (per-90 rates) — these need to be computed in the SQL:

```sql
SELECT
    dp.canonical_player_id,
    gk.competition_id,
    gk.season_id,
    AVG(gk.save_pct) as save_pct,
    AVG(gk.gk_xt_per_pass) as gk_xt_per_pass,
    AVG(gk.launch_rate) as launch_rate,
    AVG(gk.claim_success_rate) as claim_success_rate,
    AVG(CASE WHEN gk.minutes_played > 0
        THEN gk.goals_prevented / gk.minutes_played * 90
        ELSE NULL END) as goals_prevented_per_90,
    AVG(CASE WHEN gk.psxg_faced > 0
        THEN gk.psxg_faced / NULLIF(gk.saves + gk.goals_conceded, 0)
        ELSE NULL END) as psxg_per_shot_faced,
    AVG(gk.avg_defensive_action_distance) as avg_defensive_action_distance,
    AVG(gk.actions_outside_box_per_90) as actions_outside_box_per_90,
    AVG(CASE WHEN gk.goals_conceded = 0 THEN 1.0 ELSE 0.0 END) as clean_sheet_pct,
    AVG(CASE WHEN gk.minutes_played > 0
        THEN gk.saves / gk.minutes_played * 90
        ELSE NULL END) as saves_per_90,
    AVG(CASE WHEN gk.minutes_played > 0
        THEN gk.distribution_passes / gk.minutes_played * 90
        ELSE NULL END) as distribution_passes_per_90,
    AVG(CASE WHEN gk.minutes_played > 0
        THEN gk.gk_xt_delta_total / gk.minutes_played * 90
        ELSE NULL END) as gk_xt_delta_total_per_90,
    AVG(CASE WHEN gk.minutes_played > 0
        THEN gk.punches / gk.minutes_played * 90
        ELSE NULL END) as punches_per_90
FROM ...
```

Note: The current `_load_goalkeeper_stats` builds the SELECT dynamically using `AVG(gk.{f}) as {f}` from the features list. The per-90 derived columns break this pattern — they need explicit CASE expressions. Refactor the query builder to handle a `FEATURE_SQL_MAP` dict that maps feature name → SQL expression, falling back to `AVG(gk.{f})` for simple columns.

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest src/tests/test_player_embeddings.py::test_goalkeeper_stat_features_has_13_dimensions -v
```

Expected: PASS.

- [ ] **Step 6: Run full test suite + linting**

```bash
uv run ruff check src/ingestion/player_embeddings_common.py && uv run pyright src/ingestion/player_embeddings_common.py && uv run pytest src/tests/test_player_embeddings.py -v
```

Expected: All pass.

---

### Task 6: Phase 1 Verification

**Files:** None (verification only)

This task is manual verification that all Phase 1 data flows end-to-end. The user must trigger the HF Jobs compute and dbt builds.

- [ ] **Step 1: Verify `luxury-lakehouse/obso-pausa-inputs` is populated**

Check HF Hub for the dataset. If empty, run `notebooks/publish_obso_data.py` first.

- [ ] **Step 2: Trigger OBSO compute on HF Jobs**

```bash
hf jobs uv run scripts/compute_obso_hf.py --flavor a10g-large --timeout 60m
```

Monitor with `hf jobs ps` and `hf jobs logs`. Verify output at `luxury-lakehouse/obso-pausa-values`.

- [ ] **Step 3: Run dbt build for GK stats**

```bash
python scripts/ensure_warehouse.py -- dbt build --select stg_psxg__predictions fct_goalkeeper_stats --vars '{"goalkeeper_enabled": true}'
```

- [ ] **Step 4: Refresh synced tables**

```bash
python scripts/refresh_synced_tables.py --table fct_goalkeeper_stats
```

- [ ] **Step 5: Verify non-NULL D39 columns in Lakebase**

Query `fct_goalkeeper_stats_synced` via psql or the app's DB connection to confirm `psxg_faced`, `goals_conceded`, `goals_prevented`, `avg_defensive_action_distance`, `actions_outside_box_per_90` have non-NULL values.

- [ ] **Step 6: Re-run player embeddings pipeline**

Trigger the embeddings pipeline to regenerate GK stat vectors with the new 13d features. Verify that `fct_player_embeddings_career_synced` contains 13d stat vectors for GK players.

**MERGE POINT 1:** All Phase 1 changes verified. Zero D32 conflict. Stage all changed files and commit (pending user approval).

---

## Phase 2: Taipy Pages

### Task 7: Goalkeeper Analytics — Query Module

**Files:**
- Create: `hf_taipy_app/src/queries/goalkeepers.py`

Follow the pattern from `queries/shots.py`: `@ttl_cache()`, `t()` for table names, `%s` params, `execute_query()`, return `pd.DataFrame`.

- [ ] **Step 1: Create query module**

Create `hf_taipy_app/src/queries/goalkeepers.py`:

```python
"""Goalkeeper analytics queries.

Extracted from state/goalkeeper.py. All queries hit Lakebase synced tables.
"""

from __future__ import annotations

import pandas as pd

from queries.common import execute_query, t, ttl_cache


@ttl_cache()
def fetch_gk_rankings(
    competition_id: int | None,
    team_id: int | None,
    min_minutes: int,
) -> pd.DataFrame:
    """Fetch GK rankings with all four-pillar stats.

    Expected columns: player_id, player_display_name, team_name, competition_name,
    minutes_played, saves, save_pct, gk_xt_per_pass, launch_rate,
    claim_success_rate, goals_prevented, psxg_faced, goals_conceded,
    avg_defensive_action_distance, actions_outside_box_per_90,
    distribution_passes, gk_xt_delta_total, punches, keeper_pick_ups.
    """
    where_parts = ["gk.minutes_played >= %s"]
    params: list[object] = [min_minutes]

    if competition_id is not None:
        where_parts.append("gk.competition_id = %s")
        params.append(competition_id)
    if team_id is not None:
        where_parts.append("p.team_id = %s")
        params.append(team_id)

    where = " AND ".join(where_parts)

    sql = f"""
        SELECT
            gk.player_id,
            p.player_display_name,
            p.team_name,
            c.competition_name,
            gk.minutes_played,
            gk.saves,
            gk.save_pct,
            gk.gk_xt_per_pass,
            gk.launch_rate,
            gk.claim_success_rate,
            gk.goals_prevented,
            gk.psxg_faced,
            gk.goals_conceded,
            gk.avg_defensive_action_distance,
            gk.actions_outside_box_per_90,
            gk.distribution_passes,
            gk.gk_xt_delta_total,
            gk.punches,
            gk.keeper_pick_ups
        FROM {t('fct_goalkeeper_stats_synced')} gk
        JOIN {t('dim_players_synced')} p ON gk.player_id = p.player_id
        LEFT JOIN {t('dim_competitions_synced')} c
            ON gk.competition_id = c.competition_id
            AND gk.season_id = c.season_id
        WHERE {where}
        ORDER BY gk.minutes_played DESC
        LIMIT 500
    """  # noqa: S608

    return execute_query(sql, tuple(params))


@ttl_cache()
def fetch_gk_shots(
    competition_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch on-target shots faced by a GK for goalmouth scatter.

    Expected columns: event_id, match_id, end_y, end_z, shot_outcome, psxg,
    shooter_name.
    """
    where_parts = ["s.shot_on_target = true"]
    params: list[object] = []

    if competition_id is not None:
        where_parts.append("s.competition_id = %s")
        params.append(competition_id)
    if player_id is not None:
        where_parts.append("s.team_id != (SELECT team_id FROM {t('dim_players_synced')} WHERE player_id = %s LIMIT 1)".format_map({"t": t}))
        params.append(player_id)

    where = " AND ".join(where_parts)

    sql = f"""
        SELECT
            s.shot_id as event_id,
            s.match_id,
            s.end_location_y as end_y,
            s.end_location_z as end_z,
            s.shot_outcome,
            p_psxg.psxg,
            shooter.player_display_name as shooter_name
        FROM {t('fct_shots_synced')} s
        LEFT JOIN {t('stg_psxg__predictions_synced')} p_psxg
            ON s.shot_id = p_psxg.event_id
        LEFT JOIN {t('dim_players_synced')} shooter
            ON s.player_id = shooter.player_id
        WHERE {where}
        ORDER BY s.match_id, s.period, s.minute
        LIMIT 2000
    """  # noqa: S608

    return execute_query(sql, tuple(params))


@ttl_cache()
def fetch_gk_passes(
    competition_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch GK distribution passes for pitch figure.

    Expected columns: match_id, player_id, start_x, start_y, end_x, end_y,
    result, action_type.
    """
    where_parts = [
        "a.action_type IN ('goalkick', 'pass')",
        "dp.position_group = 'Goalkeeper'",
        "a.player_id = dp.player_id",
    ]
    params: list[object] = []

    if competition_id is not None:
        where_parts.append("a.competition_id = %s")
        params.append(competition_id)
    if player_id is not None:
        where_parts.append("a.player_id = %s")
        params.append(player_id)

    where = " AND ".join(where_parts)

    sql = f"""
        SELECT
            a.match_id,
            a.player_id,
            a.start_x,
            a.start_y,
            a.end_x,
            a.end_y,
            a.result,
            a.action_type
        FROM {t('fct_action_values_synced')} a
        JOIN {t('dim_players_synced')} dp ON a.player_id = dp.player_id
        WHERE {where}
        ORDER BY a.match_id, a.period, a.time_seconds
        LIMIT 5000
    """  # noqa: S608

    return execute_query(sql, tuple(params))
```

Note: The exact column names (`end_location_y`, `end_location_z`, `shot_on_target`, `shot_outcome`, `team_id`) must be verified against the actual `fct_shots_synced` and `dim_players_synced` schemas before finalising. The `stg_psxg__predictions_synced` table may not exist as a synced table — if not, the PSxG join would need to go through a different path or the synced table needs to be created.

- [ ] **Step 2: Run linter**

```bash
uv run ruff check hf_taipy_app/src/queries/goalkeepers.py
```

Expected: PASS.

---

### Task 8: Goalkeeper Analytics — State Module

**Files:**
- Create: `hf_taipy_app/src/state/goalkeeper.py`

Follow the pattern from `state/action_values.py`: prefix `gk_`, sub-view LOV, dispatch refresh function, `register_page_refresher` at bottom.

- [ ] **Step 1: Create state module skeleton**

Create `hf_taipy_app/src/state/goalkeeper.py` with:

- Module docstring naming prefix `gk_` and three sub-views: Rankings, Shot Stopping, Distribution
- Imports: `logging`, `pandas`, `plotly.graph_objects`, `matplotlib`, `mplsoccer`
- Query imports from `queries.goalkeepers`
- Shared state imports: `get_comp_id`, `get_team_id`, `get_player_id`, `register_page_refresher`
- Filter imports: `fetch_data_freshness`, `fetch_scope_label`
- Render imports as needed

Module-level state variables (all `gk_` prefixed):

```python
GK_SUB_VIEW_LOV = ["Rankings", "Shot Stopping", "Distribution"]

# Rankings
gk_rankings_df: pd.DataFrame = pd.DataFrame()
gk_scope_label: str = ""
gk_warning_text: str = ""

# Shot Stopping
gk_goalmouth_figure: go.Figure | None = None
gk_goals_prevented_figure: go.Figure | None = None
gk_psxg_faced: str = "—"
gk_goals_prevented_val: str = "—"
gk_save_pct_val: str = "—"

# Distribution
gk_distribution_image: bytes | None = None
gk_short_pct: str = "—"
gk_medium_pct: str = "—"
gk_long_pct: str = "—"
gk_launch_rate_val: str = "—"
gk_xt_per_pass_val: str = "—"
gk_xt_total_val: str = "—"

# Freshness
gk_data_freshness: str = ""
```

- [ ] **Step 2: Implement `_refresh_rankings`**

Private function that calls `fetch_gk_rankings(comp_id, team_id, min_minutes)`, formats the DataFrame for display (renames columns to human-readable, rounds floats), sets `state.gk_rankings_df` and `state.gk_scope_label`.

- [ ] **Step 3: Implement `_refresh_shot_stopping`**

Private function that:
1. Calls `fetch_gk_shots(comp_id, player_id)` — player_id from sidebar GK selector
2. Builds goalmouth scatter (`go.Scatter`): x = `(end_y - 36)` (normalise to 0–8m goal width), y = `end_z` (0–8m height), color by `shot_outcome`, size by `psxg`
3. Builds goals prevented bar chart (`go.Bar`): from rankings data, horizontal, sorted, colored green/red
4. Sets metric state variables

- [ ] **Step 4: Implement `_refresh_distribution`**

Private function that:
1. Calls `fetch_gk_passes(comp_id, player_id)`
2. Builds mplsoccer half-pitch figure with pass origins colored by xT delta
3. Computes short/medium/long percentages using thresholds from `analytics.goalkeeper` (`_SHORT_THRESHOLD = 32.0`, `_LONG_THRESHOLD = 60.0`)
4. Sets metric state variables

- [ ] **Step 5: Implement `gk_refresh` dispatcher**

```python
def gk_refresh(state: Any) -> None:
    state.sub_view_lov = GK_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in GK_SUB_VIEW_LOV:
        state.selected_sub_view = GK_SUB_VIEW_LOV[0]

    view = state.selected_sub_view
    if view == "Rankings":
        _refresh_rankings(state)
    elif view == "Shot Stopping":
        _refresh_shot_stopping(state)
    elif view == "Distribution":
        _refresh_distribution(state)

    state.gk_data_freshness = fetch_data_freshness("fct_goalkeeper_stats_synced")


register_page_refresher("Goalkeeper-Analytics", gk_refresh)
```

- [ ] **Step 6: Add GK Rankings → Player Similarity cross-link**

Add a table `on_action` callback to the rankings table that navigates to Player Similarity with the selected GK pre-selected. In the state module:

```python
def gk_on_rankings_action(state: Any, var_name: str, payload: dict) -> None:
    """Handle row click in GK rankings table — navigate to Player Similarity."""
    idx = payload.get("index")
    if idx is None:
        return
    row = state.gk_rankings_df.iloc[idx]
    player_id = row.get("player_id")
    if player_id is not None:
        state.selected_player = str(player_id)
        navigate(state, "Player-Similarity")
```

Import `navigate` from `taipy.gui`. Wire the callback in the page config's `ContentBlock("table", ...)` via `on_action="gk_on_rankings_action"`.

- [ ] **Step 7: Define `__all__`**

Export all module-level state variables, the refresh function, and the `gk_on_rankings_action` callback in `__all__`.

- [ ] **Step 8: Run linter and type checker**

```bash
uv run ruff check hf_taipy_app/src/state/goalkeeper.py && uv run pyright hf_taipy_app/src/state/goalkeeper.py
```

---

### Task 9: Goalkeeper Analytics — Page Config

**Files:**
- Create: `hf_taipy_app/src/pages/goalkeeper.py`

Follow the exact pattern from `pages/action_values.py`: pure config, zero logic.

- [ ] **Step 1: Create page config**

Create `hf_taipy_app/src/pages/goalkeeper.py`:

```python
"""Goalkeeper Analytics page configuration.

Four-pillar GK evaluation: shot stopping, distribution, collection, sweeper.
Sub-views: Rankings, Shot Stopping, Distribution.
"""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    SubView,
    build_page,
)

page_config = PageConfig(
    title="Goalkeeper Analytics",
    icon="sports_soccer",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Four-pillar goalkeeper evaluation: shot stopping (PSxG), "
        "distribution (xT delta), cross collection, and sweeper positioning."
    ),
    freshness_var="gk_data_freshness",
    citations=[
        Citation(
            label="Butcher et al. (2025)",
            url="https://doi.org/10.3390/math13020226",
        ),
        Citation(
            label="Lamberts (2025)",
            url="",
        ),
    ],
    empty_message="Select a competition to view goalkeeper statistics.",
    empty_condition="selected_competition is None",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Rankings"',
            content=[
                ContentRow([ContentBlock("table", "gk_rankings_df", on_action="gk_on_rankings_action")]),
            ],
            empty_message="No goalkeeper data found for the selected filters.",
            empty_condition="len(gk_rankings_df) == 0",
            scope_vars=["gk_scope_label"],
            warning_var="gk_warning_text",
        ),
        SubView(
            condition='selected_sub_view == "Shot Stopping"',
            content=[
                ContentRow([ContentBlock("chart", "gk_goalmouth_figure", header="Goalmouth Scatter")]),
                ContentRow([ContentBlock("chart", "gk_goals_prevented_figure", header="Goals Prevented")]),
            ],
            metrics=[
                Metric("PSxG Faced", "gk_psxg_faced", help_text="Sum of Post-Shot xG on all on-target shots faced. Higher = more dangerous shots faced."),
                Metric("Goals Prevented", "gk_goals_prevented_val", help_text="PSxG faced minus goals conceded. Positive = saved more than expected (0-1 scale per shot)."),
                Metric("Save %", "gk_save_pct_val", help_text="Saves divided by shots on target faced (0-100%)."),
            ],
            empty_message="Select a goalkeeper to view shot stopping analysis.",
            empty_condition="gk_goalmouth_figure is None",
        ),
        SubView(
            condition='selected_sub_view == "Distribution"',
            content=[
                ContentRow([ContentBlock("image", "gk_distribution_image", header="Pass Distribution")]),
            ],
            metrics=[
                Metric("Short %", "gk_short_pct", help_text="Percentage of GK passes under 32m."),
                Metric("Medium %", "gk_medium_pct", help_text="Percentage of GK passes between 32-60m."),
                Metric("Long %", "gk_long_pct", help_text="Percentage of GK passes over 60m (launches)."),
                Metric("Launch Rate", "gk_launch_rate_val", help_text="Percentage of all GK passes that are long (over 60m). Higher = more direct distribution."),
                Metric("xT / Pass", "gk_xt_per_pass_val", help_text="Average Expected Threat delta per distribution pass (0-1, higher = better). Measures quality of distribution."),
                Metric("Total xT", "gk_xt_total_val", help_text="Cumulative Expected Threat added through all distribution passes."),
            ],
            empty_message="Select a goalkeeper to view distribution analysis.",
            empty_condition="gk_distribution_image is None",
        ),
    ],
)

page_md = build_page(page_config)
```

- [ ] **Step 2: Run linter**

```bash
uv run ruff check hf_taipy_app/src/pages/goalkeeper.py
```

---

### Task 10: Register Goalkeeper Analytics Page

**Files:**
- Modify: `hf_taipy_app/src/main.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Add imports to main.py**

After the existing Player Similarity imports (around line 38), add:

```python
from pages.goalkeeper import page_config as goalkeeper_config
from pages.goalkeeper import page_md as goalkeeper_page
```

After the existing Player Similarity star-import (around line 48), add:

```python
from state.goalkeeper import *  # noqa: F403
```

- [ ] **Step 2: Add PageEntry to PAGE_REGISTRY**

After the Player-Similarity entry (line 105), add:

```python
PageEntry("Goalkeeper-Analytics", goalkeeper_config, goalkeeper_page),
```

- [ ] **Step 3: Add PAGE_TERMS entry in template.py**

In the `PAGE_TERMS` dict (around line 148), add:

```python
"Goalkeeper-Analytics": [
    "PSxG (Post-Shot Expected Goals)",
    "Goals Prevented",
    "Launch Rate",
    "xT Delta (Distribution)",
    "Claim Success Rate",
    "Sweeper Keeper",
],
```

Add glossary definitions for any new terms not already in `GLOSSARY`.

- [ ] **Step 4: Add page to page tuples in template.py**

Add `"Goalkeeper-Analytics"` to: `_COMP_PAGES`, `_TEAM_PAGES`, `_PLAYER_PAGES`, `_MIN_MINUTES_PAGES`, `_SUB_VIEW_PAGES`, and `_FILTER_HEADER_PAGES`.

- [ ] **Step 5: Run linter on both files**

```bash
uv run ruff check hf_taipy_app/src/main.py hf_taipy_app/src/template.py
```

---

### Task 11: Tactical Positions — Query Module

**Files:**
- Create: `hf_taipy_app/src/queries/tactical_positions.py`

Follow the tracking query pattern from `queries/team_shape.py`.

- [ ] **Step 1: Create query module**

Create `hf_taipy_app/src/queries/tactical_positions.py` with four functions:

```python
"""Tactical Positions queries.

Shape graph position data from fct_player_positions_synced,
fct_formation_labels_synced, and fct_position_maps_synced.
All tracking-only (20 matches).
"""

from __future__ import annotations

import pandas as pd

from queries.common import execute_query, t, ttl_cache


@ttl_cache()
def fetch_position_timeline(match_id: str, team: str) -> pd.DataFrame:
    """Per-frame position labels for all players in a team.

    Expected columns: frame_id, player_id, position_label,
    vertical_level, horizontal_level.
    """
    sql = f"""
        SELECT
            pp.frame_id,
            pp.player_id,
            dp.player_display_name,
            pp.position_label,
            pp.vertical_level,
            pp.horizontal_level
        FROM {t('fct_player_positions_synced')} pp
        JOIN {t('dim_players_synced')} dp ON pp.player_id = dp.player_id
        WHERE pp.match_id = %s AND pp.team = %s
        ORDER BY pp.frame_id, dp.player_display_name
        LIMIT 50000
    """  # noqa: S608

    return execute_query(sql, (match_id, team))


@ttl_cache()
def fetch_formation_labels_dual(match_id: str, team: str) -> pd.DataFrame:
    """Formation labels from both EFPI and shape_graph detectors.

    Expected columns: period, window_start_s, window_end_s,
    formation_label, cost, detector.
    """
    sql = f"""
        SELECT
            fl.period,
            fl.window_start_s,
            fl.window_end_s,
            fl.formation_label,
            fl.cost,
            fl.detector
        FROM {t('fct_formation_labels_synced')} fl
        WHERE fl.match_id = %s AND fl.team = %s
        ORDER BY fl.detector, fl.period, fl.window_start_s
        LIMIT 500
    """  # noqa: S608

    return execute_query(sql, (match_id, team))


@ttl_cache()
def fetch_position_maps(
    match_id: str, team: str, player_id: str | None,
) -> pd.DataFrame:
    """5x5 position map (pct_time per tactical role).

    Expected columns: player_id, player_display_name, position_label,
    vertical_level, horizontal_level, pct_time, phase.
    """
    where_parts = ["pm.match_id = %s", "pm.team = %s"]
    params: list[object] = [match_id, team]

    if player_id is not None:
        where_parts.append("pm.player_id = %s")
        params.append(player_id)

    where = " AND ".join(where_parts)

    sql = f"""
        SELECT
            pm.player_id,
            dp.player_display_name,
            pm.position_label,
            pm.vertical_level,
            pm.horizontal_level,
            pm.pct_time,
            pm.phase
        FROM {t('fct_position_maps_synced')} pm
        JOIN {t('dim_players_synced')} dp ON pm.player_id = dp.player_id
        WHERE {where}
        ORDER BY dp.player_display_name, pm.pct_time DESC
        LIMIT 2000
    """  # noqa: S608

    return execute_query(sql, tuple(params))


@ttl_cache()
def fetch_tp_players(match_id: str, team: str) -> pd.DataFrame:
    """Player list for a match/team from position data.

    Expected columns: player_id, player_display_name.
    """
    sql = f"""
        SELECT DISTINCT
            pm.player_id,
            dp.player_display_name
        FROM {t('fct_position_maps_synced')} pm
        JOIN {t('dim_players_synced')} dp ON pm.player_id = dp.player_id
        WHERE pm.match_id = %s AND pm.team = %s
        ORDER BY dp.player_display_name
        LIMIT 50
    """  # noqa: S608

    return execute_query(sql, (match_id, team))
```

- [ ] **Step 2: Run linter**

```bash
uv run ruff check hf_taipy_app/src/queries/tactical_positions.py
```

---

### Task 12: Tactical Positions — State Module

**Files:**
- Create: `hf_taipy_app/src/state/tactical_positions.py`

Follow the tracking-page pattern from `state/team_shape.py`: uses tracking match selector, `get_tracking_match_id()`.

- [ ] **Step 1: Create state module skeleton**

Create `hf_taipy_app/src/state/tactical_positions.py` with:

- Module docstring naming prefix `tp_` and three sub-views: Position Plots, Formation Comparison, Position Maps
- Imports from `queries.tactical_positions` (all 4 functions)
- Shared state: `get_tracking_match_id`, `register_page_refresher`

Module-level state variables:

```python
TP_SUB_VIEW_LOV = ["Position Plots", "Formation Comparison", "Position Maps"]

# Position Plots
tp_position_plot_figure: go.Figure | None = None
tp_most_common_formation: str = "—"
tp_formation_stability: str = "—"

# Formation Comparison
tp_formation_comparison_figure: go.Figure | None = None
tp_agreement_rate: str = "—"
tp_efpi_changes: str = "—"
tp_sg_changes: str = "—"
tp_efpi_dominant: str = "—"
tp_sg_dominant: str = "—"

# Position Maps
tp_position_map_figure: go.Figure | None = None
tp_position_map_compare_figure: go.Figure | None = None
tp_primary_position: str = "—"
tp_position_versatility: str = "—"
tp_vertical_range: str = "—"

# Player selection for Position Maps
tp_player_lov: list[str] = []
tp_selected_player: str | None = None
tp_compare_player_lov: list[str] = []
tp_selected_compare_player: str | None = None

# Freshness + scope
tp_data_freshness: str = ""
tp_scope_label: str = ""
tp_warning_text: str = ""
```

- [ ] **Step 2: Implement `_refresh_position_plots`**

Build a Plotly heatmap/scatter:
- x-axis: time (frame_id converted to minutes via fps)
- y-axis: one row per player (use `player_display_name`)
- Color: vertical level mapped to 5-color palette (B=blue, DM=cyan, M=green, AM=orange, F=red)
- Use `fetch_position_timeline(match_id, team)`

- [ ] **Step 3: Implement `_refresh_formation_comparison`**

Build a dual-lane Plotly timeline:
- Two swim lanes (EFPI top, Shape Graph bottom) using `go.Bar` with base/width for segments
- Color by formation_label (consistent mapping across both lanes)
- Compute agreement rate = % of overlapping time where both detectors agree
- Use `fetch_formation_labels_dual(match_id, team)`

- [ ] **Step 4: Implement `_refresh_position_maps`**

Build a 5×5 Plotly heatmap:
- Rows: vertical levels (B/DM/M/AM/F), Columns: horizontal levels (L/LC/C/RC/R)
- Cell values: `pct_time`, color intensity via a sequential colorscale
- Two grids side-by-side in comparison mode (`make_subplots(rows=1, cols=2)`)
- Use `fetch_position_maps(match_id, team, player_id)`
- Compute metrics: primary position (max pct_time), versatility (count cells > 10%), vertical range

- [ ] **Step 5: Implement `tp_refresh` dispatcher + registration**

Same pattern as GK page. End with:

```python
register_page_refresher("Tactical-Positions", tp_refresh)
```

- [ ] **Step 6: Define `__all__` and run linter**

```bash
uv run ruff check hf_taipy_app/src/state/tactical_positions.py && uv run pyright hf_taipy_app/src/state/tactical_positions.py
```

---

### Task 13: Tactical Positions — Page Config

**Files:**
- Create: `hf_taipy_app/src/pages/tactical_positions.py`

Same pure-config pattern. Three sub-views with metrics.

- [ ] **Step 1: Create page config**

Create `hf_taipy_app/src/pages/tactical_positions.py` with `PageConfig` using `NAV_ADVANCED`, three `SubView` entries (Position Plots, Formation Comparison, Position Maps), citations for Sotudeh (2026), and tracking-scoped empty/warning messages.

The position maps sub-view should include the comparison mode content blocks:

```python
SubView(
    condition='selected_sub_view == "Position Maps"',
    content=[
        ContentRow([
            ContentBlock("chart", "tp_position_map_figure", header="Position Map"),
            ContentBlock("chart", "tp_position_map_compare_figure", header="Comparison"),
        ], columns=2),
    ],
    metrics=[
        Metric("Primary Position", "tp_primary_position", help_text="Tactical role with highest time share in the 5x5 position grid."),
        Metric("Versatility", "tp_position_versatility", help_text="Number of position grid cells where player spent more than 10% of time. Higher = more positional flexibility."),
        Metric("Vertical Range", "tp_vertical_range", help_text="Number of distinct vertical levels (B/DM/M/AM/F) occupied more than 5% of time."),
    ],
    empty_message="Select a player to view their position map.",
    empty_condition="tp_position_map_figure is None",
),
```

- [ ] **Step 2: Run linter**

```bash
uv run ruff check hf_taipy_app/src/pages/tactical_positions.py
```

---

### Task 14: Register Tactical Positions Page

**Files:**
- Modify: `hf_taipy_app/src/main.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Add imports to main.py**

Add page config and star imports for the new tactical positions module, following the same pattern as Task 10.

- [ ] **Step 2: Add PageEntry to PAGE_REGISTRY**

After Team-Shape (line 110), add:

```python
PageEntry("Tactical-Positions", tactical_positions_config, tactical_positions_page),
```

- [ ] **Step 3: Add PAGE_TERMS and glossary entries in template.py**

```python
"Tactical-Positions": [
    "Shape Graph",
    "Position Label",
    "Vertical Level",
    "Horizontal Level",
    "EFPI",
    "Position Map",
    "Formation Detector",
],
```

Add glossary definitions for each new term.

- [ ] **Step 4: Add page to tracking-scoped tuples**

Add `"Tactical-Positions"` to: `_TRACKING_PAGES` (or equivalent tracking tuple), `_SUB_VIEW_PAGES`, and `_FILTER_HEADER_PAGES`. This page should NOT be in `_COMP_PAGES` or `_TEAM_PAGES` — it uses the tracking match selector instead.

- [ ] **Step 5: Run linter**

```bash
uv run ruff check hf_taipy_app/src/main.py hf_taipy_app/src/template.py
```

**MERGE POINT 2:** Both new pages verified locally (15 + 16 = 16 total pages). All new files except main.py and template.py edits. Zero D32 conflict. Stage and commit (pending user approval).

---

## Phase 3: Embedding Explorer & Player Similarity Enhancement

### Task 15: Embedding Export Script

**Files:**
- Create: `scripts/export_embedding_atlas_data.py`
- Modify: `pyproject.toml` (add entry point)

Exports season-level and career-level embedding data from Lakebase to Parquet on HF Hub, with pre-computed UMAP 2D projections.

- [ ] **Step 1: Create export script**

Create `scripts/export_embedding_atlas_data.py`:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "psycopg2-binary",
#     "pandas",
#     "pyarrow",
#     "numpy",
#     "umap-learn",
#     "huggingface-hub",
# ]
# ///
"""Export player embeddings to Parquet for Embedding Atlas visualization.

Reads season-level and career-level embeddings from Lakebase,
pre-computes UMAP 2D projections, and uploads to HF Hub.

Usage:
    python scripts/export_embedding_atlas_data.py
"""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import umap
from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

HF_REPO = "luxury-lakehouse/embedding-atlas-data"
VECTOR_DIM = 128


def _parse_vector(text: str) -> list[float]:
    """Parse pgvector text representation '[0.1,0.2,...]' to float list."""
    return json.loads(text)


def _compute_umap(vectors: np.ndarray, n_neighbors: int = 15) -> np.ndarray:
    """Compute 2D UMAP projection from high-dimensional vectors."""
    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=0.1,
        metric="cosine",
        random_state=42,
    )
    return reducer.fit_transform(vectors)


def export_embeddings(
    connection_string: str,
    level: str = "season",
) -> pd.DataFrame:
    """Query Lakebase for embeddings and compute UMAP projection."""
    import psycopg2

    table = (
        "fct_player_embeddings_season_synced"
        if level == "season"
        else "fct_player_embeddings_career_synced"
    )
    count_col = (
        "matches_in_sample" if level == "season" else "total_matches"
    )

    # Build query — join with dim_players for metadata
    sql = f"""
        SELECT
            e.canonical_player_id as player_id,
            p.player_display_name as player_name,
            p.team_name as team,
            p.position_group,
            e.behavioral_vector,
            e.{count_col} as matches_in_sample,
            e.data_sources
            {"  , e.competition_id, e.season_id" if level == "season" else ""}
        FROM {table} e
        JOIN dim_players_synced p
            ON e.canonical_player_id = p.canonical_player_id
        WHERE e.behavioral_vector IS NOT NULL
        ORDER BY e.canonical_player_id
    """

    conn = psycopg2.connect(connection_string)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        logger.warning("No embeddings found for level=%s", level)
        return df

    # Parse vectors and compute UMAP
    vectors = np.array(df["behavioral_vector"].apply(_parse_vector).tolist())
    logger.info("Computing UMAP for %d vectors (%dd)", len(vectors), vectors.shape[1])
    umap_coords = _compute_umap(vectors)
    df["umap_x"] = umap_coords[:, 0]
    df["umap_y"] = umap_coords[:, 1]

    # Drop raw vector (Atlas uses UMAP coords, not raw vectors)
    df = df.drop(columns=["behavioral_vector"])

    return df


def main() -> None:
    """Export embeddings and upload to HF Hub."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Export embeddings for Atlas")
    parser.add_argument("--connection-string", required=True, help="Lakebase connection string")
    parser.add_argument("--hf-repo", default=HF_REPO, help="Target HF Hub dataset repo")
    args = parser.parse_args()

    api = HfApi()
    api.create_repo(args.hf_repo, exist_ok=True, repo_type="dataset")

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        for level in ("season", "career"):
            logger.info("Exporting %s-level embeddings...", level)
            df = export_embeddings(args.connection_string, level=level)
            if df.empty:
                continue
            out_path = data_dir / f"embeddings_{level}.parquet"
            df.to_parquet(out_path, index=False)
            logger.info("Wrote %d rows to %s", len(df), out_path.name)

        api.upload_folder(
            repo_id=args.hf_repo,
            folder_path=str(data_dir),
            path_in_repo="data",
            repo_type="dataset",
        )
        logger.info("Uploaded to %s", args.hf_repo)


if __name__ == "__main__":
    main()
```

Note: The exact Lakebase connection string and `dim_players_synced` schema (especially `canonical_player_id` vs `player_id` join column) must be verified against the actual database schema. The competition/season metadata for season-level exports needs a JOIN to `dim_competitions_synced` for human-readable names.

- [ ] **Step 2: Add entry point to pyproject.toml**

In `pyproject.toml` `[project.scripts]` section, add:

```toml
export_embedding_atlas_data = "scripts.export_embedding_atlas_data:main"
```

Wait — this script uses `psycopg2` directly (not PySpark), so it may not belong in the wheel's entry points. Check if it's better as a standalone script run via `python scripts/export_embedding_atlas_data.py`. If standalone, skip the entry point.

- [ ] **Step 3: Run linter**

```bash
uv run ruff check scripts/export_embedding_atlas_data.py
```

---

### Task 16: Embedding Explorer Space

**Files:**
- Create: `embedding-explorer/README.md`
- Create: `embedding-explorer/index.html`

Pure HTML/JS static Space. No Python runtime.

- [ ] **Step 1: Create README.md with HF Space metadata**

Create `embedding-explorer/README.md`:

```markdown
---
title: Football Player Embedding Explorer
emoji: ⚽
colorFrom: amber
colorTo: green
sdk: static
pinned: false
license: mit
---

# Football Player Embedding Explorer

Interactive 2D visualization of ~87,000 season-level player embeddings from the
[Luxury Lakehouse](https://huggingface.co/luxury-lakehouse) soccer analytics platform.

Powered by [Embedding Atlas](https://github.com/apple/embedding-atlas) (Apple, MIT License)
with [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) for client-side Parquet queries.

## Data

- **Season-level**: One dot per player per competition per season (~87K points)
- **Career-level**: One dot per player (career mean, ~8,950 points)
- **Embeddings**: 128-dimensional football2vec v2 transformer encoder with adversarial team debiasing
- **Projection**: Pre-computed UMAP (cosine metric, 15 neighbors)

## Links

- [Soccer Analytics Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
- [Football2vec v2 Model](https://huggingface.co/luxury-lakehouse/football2vec-v2)
```

- [ ] **Step 2: Create index.html**

Create `embedding-explorer/index.html`. This is a self-contained single-page app that:

1. Loads Embedding Atlas widget from npm CDN
2. Loads DuckDB-WASM for client-side Parquet queries
3. Fetches pre-computed Parquet from `luxury-lakehouse/embedding-atlas-data` on HF Hub
4. Renders a 2D scatter with color/filter controls

The exact implementation depends on the Embedding Atlas JavaScript API. Before writing the full HTML, read the Embedding Atlas documentation at `https://github.com/apple/embedding-atlas` to understand the widget API, data format requirements, and CDN bundle URL.

The HTML should include:
- Header with title and filter controls (position group, competition, season dropdowns)
- Toggle for season-level vs career-level
- Embedding Atlas canvas container
- Tooltip on hover (player name, team, competition, season, matches)
- Click handler that opens Taipy Player Similarity page in new tab

- [ ] **Step 3: Deploy to HF Space**

```bash
python scripts/manage_space.py deploy embedding-explorer
```

Or via direct upload:

```python
from huggingface_hub import HfApi
api = HfApi()
api.create_repo("luxury-lakehouse/embedding-explorer", exist_ok=True, repo_type="space", space_sdk="static")
api.upload_folder(repo_id="luxury-lakehouse/embedding-explorer", folder_path="embedding-explorer", repo_type="space")
```

- [ ] **Step 4: Verify Space is RUNNING**

Check `https://huggingface.co/spaces/luxury-lakehouse/embedding-explorer` loads and renders the scatter plot.

---

### Task 17: Player Similarity — Embedded Atlas Neighborhood Widget

**Files:**
- Modify: `hf_taipy_app/src/state/player_similarity.py`
- Modify: `hf_taipy_app/src/pages/player_similarity.py`

Add a scoped Atlas widget showing the selected player's ~50 nearest neighbors in 2D embedding space. Uses the same `RawHtml` + content provider iframe pattern as the DAG visualization (`state/workflows_dag.py`).

- [ ] **Step 1: Add Atlas HTML builder to state module**

In `hf_taipy_app/src/state/player_similarity.py`, add a function that generates a self-contained HTML snippet for the Atlas neighborhood widget:

```python
def _build_atlas_neighborhood_html(
    player_name: str,
    player_id: int,
    neighbors: pd.DataFrame,
) -> str:
    """Build self-contained HTML for Atlas neighborhood visualization.

    Uses DuckDB-WASM to query the pre-computed Parquet from HF Hub,
    filtered to the selected player's nearest neighbors.
    """
    # Build inline JSON data from neighbors DataFrame
    # Include umap_x, umap_y, player_name, position_group, distance
    # Highlight the selected player with distinct styling
    # Include "Explore full map" link to standalone Space
    ...
```

The exact implementation depends on the Embedding Atlas JavaScript API. This function follows the pattern from `workflows_dag.py:build_dag_html()` — generates a complete HTML document string with embedded data and JS.

- [ ] **Step 2: Add state variable**

Add to module-level state:

```python
ps_atlas_neighborhood: RawHtml | None = None
```

Import `RawHtml` from `state.workflows` (where it's defined).

- [ ] **Step 3: Wire into similarity search flow**

After the existing similarity results are computed (in the search handler), call `_build_atlas_neighborhood_html` with the results and set `state.ps_atlas_neighborhood`.

- [ ] **Step 4: Add ContentBlock to page config**

In `hf_taipy_app/src/pages/player_similarity.py`, add a `ContentBlock("html", "ps_atlas_neighborhood")` after the results table block. Set `condition` to render only when results are populated.

- [ ] **Step 5: Add "Explore full map" cross-link**

The HTML generated by `_build_atlas_neighborhood_html` should include an `<a href="https://huggingface.co/spaces/luxury-lakehouse/embedding-explorer?player_id={player_id}" target="_blank">` link.

- [ ] **Step 6: Run linter**

```bash
uv run ruff check hf_taipy_app/src/state/player_similarity.py hf_taipy_app/src/pages/player_similarity.py
```

**MERGE POINT 3:** All Phase 3 changes. D32 conflict zone — coordinate merge order for `player_similarity.py`. Stage and commit (pending user approval).

---

## Phase 4: Polish & Verification

### Task 18: Workflow Card Updates

**Files:**
- Modify: `workflow-cards/wf-import-obso.yaml`
- Modify: `workflow-cards/wf-obso-pausa.yaml` (if any execution notes need updating)

- [ ] **Step 1: Update wf-import-obso.yaml**

Change `inputs.datasets[0].source` from `uc-volume` to `huggingface`. Update `id` to `luxury-lakehouse/obso-pausa-values`. Add note about the HF Hub → Volume bridge pattern.

- [ ] **Step 2: Validate workflow cards**

```bash
uv run validate_workflow_cards
```

Expected: All cards validate.

---

### Task 19: Full Verification Sequence

**Files:** None (verification only)

- [ ] **Step 1: Run full lint + type check + test suite**

```bash
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

Expected: All pass with zero violations.

- [ ] **Step 2: Local Taipy app verification**

Launch the app locally and verify:
- All 16 pages load without errors
- Goalkeeper Analytics Rankings shows populated data
- Goalkeeper Analytics Shot Stopping renders goalmouth scatter
- Goalkeeper Analytics Distribution renders pitch figure
- Tactical Positions Position Plots renders time series (with tracking match selected)
- Tactical Positions Formation Comparison shows dual-detector timeline
- Tactical Positions Position Maps renders 5×5 grid
- Player Similarity shows Atlas neighborhood widget after search

- [ ] **Step 3: Puppeteer local verification**

Run Puppeteer tests against local app — all 16 pages load, no console errors, key elements visible.

- [ ] **Step 4: Deploy staging**

```bash
python scripts/manage_space.py deploy staging
```

Wait for staging to be RUNNING.

- [ ] **Step 5: Puppeteer staging verification**

Run Puppeteer against staging URL. Verify all pages, check GK data is populated, check tracking pages render.

- [ ] **Step 6: Deploy production**

```bash
python scripts/manage_space.py deploy production
```

- [ ] **Step 7: Verify Embedding Explorer Space**

Confirm `https://huggingface.co/spaces/luxury-lakehouse/embedding-explorer` is RUNNING and renders correctly.

---

## Summary of Files

### New Files (14)
| File | Task |
|---|---|
| `dbt_project/models/staging/psxg/_psxg__sources.yml` | 3 |
| `dbt_project/models/staging/psxg/stg_psxg__predictions.sql` | 3 |
| `hf_taipy_app/src/queries/goalkeepers.py` | 7 |
| `hf_taipy_app/src/state/goalkeeper.py` | 8 |
| `hf_taipy_app/src/pages/goalkeeper.py` | 9 |
| `hf_taipy_app/src/queries/tactical_positions.py` | 11 |
| `hf_taipy_app/src/state/tactical_positions.py` | 12 |
| `hf_taipy_app/src/pages/tactical_positions.py` | 13 |
| `scripts/export_embedding_atlas_data.py` | 15 |
| `embedding-explorer/README.md` | 16 |
| `embedding-explorer/index.html` | 16 |

### Modified Files (10)
| File | Tasks |
|---|---|
| `src/ingestion/import_obso_results.py` | 1 |
| `terraform/modules/workflows/main.tf` | 2 |
| `dbt_project/models/marts/fct_goalkeeper_stats.sql` | 4 |
| `src/ingestion/player_embeddings_common.py` | 5 |
| `hf_taipy_app/src/main.py` | 10, 14 |
| `hf_taipy_app/src/template.py` | 10, 14 |
| `hf_taipy_app/src/state/player_similarity.py` | 17 |
| `hf_taipy_app/src/pages/player_similarity.py` | 17 |
| `workflow-cards/wf-import-obso.yaml` | 1, 18 |
| `pyproject.toml` | 15 (if entry point added) |

### Merge Points
| Point | After Task | D32 Risk |
|---|---|---|
| 1 | Task 6 (Phase 1 verified) | None |
| 2 | Task 14 (Phase 2 pages registered) | None |
| 3 | Task 18 (Phase 3+4 complete) | High (player_similarity.py) |
