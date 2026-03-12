# HF Hub Expansion Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand HuggingFace Hub integration from Tier 2 (partial: model weights only) to Tier 2 (complete: 4 datasets with dataset cards) and Tier 4 (demo Space), while implementing JAX pitch control vectorization, pitch control value population, and symmetry augmentation. All artifacts published to HF Hub with proper cards (dataset cards, updated model card, updated org card). Everything deployed, rebuilt, and verified end-to-end before commit.

**Architecture:** Notebook-based export pipeline (Databricks) publishes gold Delta table subsets as Parquet to HF Hub dataset repos under the `luxury-lakehouse` org. JAX pitch control adds an optional high-performance backend to the existing NumPy implementation with identical public API. Pitch control values are computed via `applyInPandas` and written to a standalone bronze table (same pattern as `off_ball_xt_results`) — `fct_tracking_frames` is NOT modified; exports JOIN at query time. Symmetry augmentation is a pure NumPy analytics module for 8x tracking data multiplication. A Gradio demo Space showcases all published artifacts with pre-cached data.

**Tech Stack:** `huggingface_hub`, `jax[cpu]`, `gradio`, NumPy, pandas, PySpark `applyInPandas`, dbt, pytest, pytest-benchmark

---

## Scope Adjustment from Investigation

The original investigation recommended OpenSTARLab as a companion item. Deeper research revealed:
- **No pre-trained weights** available (must train from scratch)
- Hard `scipy==1.10.1` pin conflicts with project's `scipy>=1.11.0`
- Requires PyTorch (heavy dependency not currently in stack)

**Replaced with:** pitch control value population (TODO #5) + symmetry augmentation. Both are immediately actionable, use existing analytics, and produce publishable artifacts.

---

## Milestones

| # | Milestone | Tasks | Unblocks | Risk |
|---|-----------|-------|----------|------|
| M1 | Dataset Publishing (3 datasets) | 1-4 | Tier 4 Space, community training, E5 versioning | Low |
| M2 | Pitch Control Value Population | 5-6 | TODO #5, 4th publishable dataset | Low |
| M3 | JAX Pitch Control Kernel | 7 | OBSO/Space Creation (TODO #14), PAUSA foundation | Medium (new dep) |
| M4 | Symmetry Augmentation | 8 | DEFCON Tier 4 data barrier, 5th publishable artifact | Low |
| M5 | Demo Space + HF Cards | 9, 11 | Portfolio showcase, community engagement | Low |
| M6 | Documentation | 10 | ROADMAP/TODO/PLAN consistency | Low |
| M7 | Deploy + E2E Verify + Commit | 12-15 | Production-ready, merged to main | Low |

M1-M2 are the "must do." M3-M6 can be deferred to a follow-up if the branch gets too large. M7 is mandatory — no code is staged or committed until everything is deployed and verified end-to-end.

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `notebooks/publish_datasets.py` | Databricks notebook: export gold tables to Parquet, publish to HF Hub |
| `docs/huggingface/dataset-cards/spadl-vaep.md` | Dataset card for SPADL/VAEP action values |
| `docs/huggingface/dataset-cards/line-breaking.md` | Dataset card for line-breaking passes |
| `docs/huggingface/dataset-cards/player-embeddings.md` | Dataset card for player embeddings (career vectors) |
| `docs/huggingface/dataset-cards/pitch-control.md` | Dataset card for pitch control values (after M2) |
| `src/ingestion/pitch_control_batch.py` | Batch pipeline: compute pitch_control_value via `applyInPandas`, write to bronze |
| `src/tests/test_pitch_control_batch.py` | Tests for batch pipeline (unit + schema) |
| `dbt_project/models/staging/pitch_control/_pitch_control__sources.yml` | Bronze source definition (matches `_off_ball_xt__sources.yml` pattern) |
| `dbt_project/models/staging/pitch_control/_pitch_control__models.yml` | Staging model YAML |
| `dbt_project/models/staging/pitch_control/stg_pitch_control__values.sql` | Staging model: dedup + cast bronze pitch control values |
| `src/analytics/symmetry.py` | Symmetry augmentation: H-flip, V-flip, team swap (pure NumPy) |
| `src/tests/test_symmetry.py` | Tests for symmetry augmentation |
| `demo_space/app.py` | Gradio demo Space entry point (outside `src/` quality gates intentionally) |
| `demo_space/requirements.txt` | Space dependencies (gradio, pandas, numpy, plotly) |
| `demo_space/data/` | Pre-cached Parquet subsets for the Space |

### Modified files

| File | Change |
|------|--------|
| `pyproject.toml` | Add `jax` optional dependency group, add `demo` optional group (gradio), add `pitch_control_batch` entry point, add `demo_space` to pyright exclude |
| `src/analytics/pitch_control.py` | Add conditional JAX backend with `jax.jit` kernels, keep NumPy fallback |
| `src/tests/test_pitch_control_model.py` | Add parametrized tests for JAX/NumPy parity |
| `src/tests/test_benchmarks.py` | Add JAX benchmark comparison |
| `docs/huggingface/model-card.md` | Update with companion dataset links, demo Space link, STAT_FEATURES sync |
| `docs/huggingface/org-card.md` | Update with new published datasets and demo Space |
| `ROADMAP.md` | Update HF Hub tiers, DL Infrastructure, Space Creation status |
| `TODO.md` | Resolve #5 (pitch_control_value), update #14 (Space Creation) |
| `PLAN.md` | Add Phase 18 summary |
| `terraform/environments/dev/main.tf` | Add `pitch_control_batch` task to ingestion workflow (if applicable) |

### NOT modified (design decision)

| File | Why |
|------|-----|
| `dbt_project/models/marts/fct_tracking_frames.sql` | `pitch_control_value` stays as `cast(null as double)`. Pitch control values live in standalone bronze table (`bronze.pitch_control_values`), same pattern as `off_ball_xt_results`. HF exports JOIN at query time. Future `--full-refresh` with LEFT JOIN can populate the gold column later. |
| `dbt_project/models/marts/_marts__models.yml` | No contract changes needed since fct_tracking_frames SQL is unchanged. |
| `dbt_project/dbt_project.yml` | No feature toggle needed. Unlike `off_ball_xt_enabled` (which gates a LEFT JOIN in a mart), `stg_pitch_control__values` is a leaf staging view with no downstream mart dependency. `dbt compile` succeeds regardless. `dbt build` tests will fail until the bronze table exists, but only if the staging model is selected — use `dbt build --exclude stg_pitch_control__values` until the pipeline has run. |

---

## Task 1: Dataset Publishing Infrastructure

**Files:**
- Create: `notebooks/publish_datasets.py`
- Create: `docs/huggingface/dataset-cards/spadl-vaep.md`

### Subtask 1.1: Create dataset card template for SPADL/VAEP

- [ ] **Step 1: Write the SPADL/VAEP dataset card**

Create `docs/huggingface/dataset-cards/spadl-vaep.md` with HF dataset card YAML frontmatter. Follow the conventions from the existing model card at `docs/huggingface/model-card.md`.

Key metadata:
```yaml
---
language: [en]
license: mit
task_categories: [tabular-classification, tabular-regression]
tags: [sports-analytics, soccer, football, spadl, vaep, action-valuation, statsbomb, wyscout]
size_categories: [1M-10M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---
```

Body sections: description (what SPADL/VAEP is, cite socceraction + Decroos et al. 2019), data fields table (all 22 columns from `fct_action_values` contract, excluding `_loaded_at`, with types and descriptions), coordinate system note (SPADL 105x68 meters), data sources (StatsBomb open data + Wyscout Figshare), row count (~9.5M), action type vocabulary (23 SPADL types), limitations (no tracking data, VAEP model is competition-agnostic), citation.

### Subtask 1.2: Create publishing notebook scaffold

- [ ] **Step 2: Write the Databricks publishing notebook**

Create `notebooks/publish_datasets.py` following the established pattern from `notebooks/train_football2vec.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Datasets to HuggingFace Hub
# MAGIC Exports gold Delta tables as Parquet and publishes to HF Hub dataset repos.

# COMMAND ----------

import os
import tempfile
from pathlib import Path

# COMMAND ----------

# Configuration
CATALOG = "soccer_analytics"
GOLD_SCHEMA = "dev_gold"
HF_ORG = "luxury-lakehouse"

# HF token from Databricks secrets (same scope as model publishing)
try:
    hf_token = dbutils.secrets.get(scope="hf", key="token")
    os.environ["HF_TOKEN"] = hf_token
except Exception:
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        raise RuntimeError("Set HF_TOKEN env var or Databricks secret scope 'hf' key 'token'")

from huggingface_hub import HfApi
api = HfApi()
```

The notebook has one cell per dataset. Each cell:
1. Runs a Spark SQL query selecting columns from the gold table
2. Writes to a temp directory as Parquet (partitioned by `data_source` or `competition_id`)
3. Copies the dataset card as `README.md` in the upload folder (this becomes the HF Hub dataset card)
4. Calls `api.create_repo(..., repo_type="dataset", exist_ok=True)` then `api.upload_folder()` to push to HF Hub

**Card publishing:** Each dataset card in `docs/huggingface/dataset-cards/` is copied into the export folder as `README.md` before `upload_folder()`. HF Hub renders this as the dataset card page. The local file is the source of truth; the notebook pushes it to HF on every run.

---

## Task 2: Publish SPADL/VAEP Action Values Dataset

**Files:**
- Modify: `notebooks/publish_datasets.py`
- HF repo: `luxury-lakehouse/spadl-vaep-action-values`

- [ ] **Step 1: Add SPADL/VAEP export cell to notebook**

SQL query (all columns from `fct_action_values` contract, excluding `_loaded_at` audit column):
```sql
SELECT action_value_id, match_id, player_id, team_id, competition_id, season_id,
       period, time_seconds, minute, second,
       start_x, start_y, end_x, end_y,
       action_type, action_result, bodypart,
       offensive_value, defensive_value, vaep_value,
       data_source, original_event_id
FROM {CATALOG}.{GOLD_SCHEMA}.fct_action_values
ORDER BY match_id, period, time_seconds
```

Export as Parquet partitioned by `data_source` (2 partitions: `statsbomb`, `wyscout`). Copy `docs/huggingface/dataset-cards/spadl-vaep.md` as `README.md` in the upload folder.

```python
api.create_repo(f"{HF_ORG}/spadl-vaep-action-values", repo_type="dataset", exist_ok=True)
api.upload_folder(
    folder_path=str(export_dir),
    repo_id=f"{HF_ORG}/spadl-vaep-action-values",
    repo_type="dataset",
    token=hf_token,
)
```

---

## Task 3: Publish Line-Breaking Passes Dataset

**Files:**
- Create: `docs/huggingface/dataset-cards/line-breaking.md`
- Modify: `notebooks/publish_datasets.py`
- HF repo: `luxury-lakehouse/line-breaking-passes`

- [ ] **Step 1: Write line-breaking dataset card**

Key differences from SPADL/VAEP card:
- Size: ~121K rows (only passes with `is_line_breaking = true` OR all passes for context)
- Decision: publish ALL passes (~5M rows) with `is_line_breaking` as a feature column, not just the 121K line-breaking ones. This is more useful for ML training (balanced dataset with positive/negative examples).
- Columns: all 30 columns from `fct_passes` contract
- Coordinate system: StatsBomb 120x80 yards
- Method description: Ward hierarchical clustering + cross-product straddle test
- Limitations: line-breaking detection only available for StatsBomb 360 + Metrica tracking matches

- [ ] **Step 2: Add export cell to notebook**

SQL:
```sql
SELECT pass_id, match_id, player_id, team_id, pass_recipient_id,
       competition_id, season_id, period, minute, second,
       start_x, start_y, end_x, end_y,
       pass_type, pass_height, body_part, pass_length, pass_angle_radians,
       pass_outcome, is_cross, is_switch, is_through_ball, is_complete, is_progressive,
       pass_direction, is_line_breaking, lines_broken, line_breaking_type,
       data_source
FROM {CATALOG}.{GOLD_SCHEMA}.fct_passes
ORDER BY match_id, period, minute, second
```

Partition by `data_source`.

---

## Task 4: Publish Player Embeddings Dataset

**Files:**
- Create: `docs/huggingface/dataset-cards/player-embeddings.md`
- Modify: `notebooks/publish_datasets.py`
- HF repo: `luxury-lakehouse/football2vec-player-embeddings`

- [ ] **Step 1: Write player embeddings dataset card**

This is a companion dataset to the existing model repo. Key details:
- Three tables exported: `fct_player_embeddings_career` (~8,950 rows), `fct_player_embeddings_season`, `fct_player_embeddings` (per-match, ~87K rows)
- Columns: `canonical_player_id`, `behavioral_vector` (32-dim), `stat_vector` (13-dim), aggregation metadata
- Vectors are pre-computed — consumers don't need gensim or the model weights
- Use case: similarity search, clustering, transfer market analysis
- Note: link to the model repo for methodology details

**Important:** `behavioral_vector` and `stat_vector` are `array<double>` in Delta. For Parquet export, these become nested arrays. HF dataset viewer handles this natively.

- [ ] **Step 2: Add export cells to notebook (3 tables)**

Career vectors (primary — most useful for consumers):
```sql
SELECT canonical_player_id, behavioral_vector, stat_vector,
       total_matches, data_sources
FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings_career
```

Season vectors:
```sql
SELECT embedding_season_id, canonical_player_id, competition_id, season_id,
       behavioral_vector, stat_vector, matches_in_sample, data_sources
FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings_season
```

Per-match vectors:
```sql
SELECT embedding_id, canonical_player_id, match_id, data_source,
       behavioral_vector, stat_vector
FROM {CATALOG}.{GOLD_SCHEMA}.fct_player_embeddings
```

Upload as three configs in one dataset repo:
```python
# Career vectors as default config
api.upload_folder(folder_path=career_dir, repo_id=repo, repo_type="dataset",
                  path_in_repo="data/career", token=hf_token)
# Season and per-match as additional configs
api.upload_folder(folder_path=season_dir, repo_id=repo, repo_type="dataset",
                  path_in_repo="data/season", token=hf_token)
api.upload_folder(folder_path=match_dir, repo_id=repo, repo_type="dataset",
                  path_in_repo="data/per_match", token=hf_token)
```

Dataset card `configs:` section lists all three with descriptions.

---

## Task 5: Pitch Control Value Population Pipeline

**Files:**
- Create: `src/ingestion/pitch_control_batch.py`
- Create: `src/tests/test_pitch_control_batch.py`
- Create: `dbt_project/models/staging/pitch_control/_pitch_control__sources.yml`
- Create: `dbt_project/models/staging/pitch_control/_pitch_control__models.yml`
- Create: `dbt_project/models/staging/pitch_control/stg_pitch_control__values.sql`
- Modify: `pyproject.toml` (entry point)

**Design decision:** `fct_tracking_frames.sql` is NOT modified. The `pitch_control_value` column stays as `cast(null as double)`. Pitch control values live in a standalone bronze table (`bronze.pitch_control_values`), exposed via a staging model. This matches the `off_ball_xt_results` pattern exactly. The HF publishing notebook JOINs at export time. A future phase can add a LEFT JOIN + `--full-refresh` to populate the gold column.

### Subtask 5.1: Write tests for the batch pipeline

- [ ] **Step 1: Write unit tests for the pitch control batch UDF**

```python
# src/tests/test_pitch_control_batch.py
"""Tests for pitch control batch computation pipeline."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


class TestPitchControlBatchUdf:
    """Test the per-frame pitch control computation UDF."""

    def test_single_frame_all_players_get_values(self):
        """Every player in a frame gets a pitch control value."""
        # 11 home + 11 away players, one frame
        # After UDF, each row should have a non-null pitch_control_value
        ...

    def test_home_player_near_goal_high_control(self):
        """Player near own goal with no nearby opponents has high control."""
        ...

    def test_values_bounded_zero_one(self):
        """All pitch control values are in [0, 1]."""
        ...

    def test_output_schema_has_required_columns(self):
        """UDF output has (tracking_id, match_id, pitch_control_value) columns."""
        ...

    def test_missing_velocity_defaults_to_contested(self):
        """NaN velocities produce 0.5 (contested) pitch control."""
        ...

    def test_empty_frame_returns_empty(self):
        """Empty input DataFrame returns empty output."""
        ...


class TestPitchControlBatchPipeline:
    """Integration-style tests for the pipeline orchestration."""

    def test_incremental_skip_guard(self):
        """Already-computed match_ids are skipped."""
        ...

    def test_output_table_name(self):
        """Pipeline writes to correct bronze table."""
        ...
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest src/tests/test_pitch_control_batch.py -v
```

Expected: ImportError or NameError (module doesn't exist yet).

### Subtask 5.2: Implement the batch pipeline

- [ ] **Step 3: Write the batch pipeline module**

`src/ingestion/pitch_control_batch.py` — follows the exact pattern of `src/ingestion/off_ball_xt.py`:

```python
"""Pitch control value batch computation pipeline.

Reads tracking frames from fct_tracking_frames, computes pitch control at
each player's position using the Spearman 2017 model, and writes results
to bronze.pitch_control_values.

Design: "Read from gold, compute, write to bronze." The gold mart provides
the standardised schema. Results exposed via dbt staging model
stg_pitch_control__values for downstream consumption.

Architecture: applyInPandas grouped by (match_id, frame_batch_id).
For each frame in the batch, computes pitch control at all N player
positions simultaneously via compute_pitch_control_at_points.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# NOTE: analytics.pitch_control imports are LAZY — they happen inside the UDF
# function body, not here. See _make_udf() and the UDF pattern note below.

_TABLE_NAME = "pitch_control_values"
_GOLD_SCHEMA = "dev_gold"
_DEFAULT_BATCH_SIZE = 500  # frames per applyInPandas group
```

**UDF pattern (critical — follows off_ball_xt.py exactly):**
- **Lazy imports (MANDATORY):** All analytics imports (`from analytics.pitch_control import ...`) happen INSIDE the UDF function body, not at module top level. Executors don't have the `src/` wheel on `PYTHONPATH` until the UDF runs. The top-level imports (numpy, pandas, logging) are safe because they're stdlib/third-party.
- `PitchControlParams` is captured via frozen dataclass in the closure (captured at action time, not definition time — see CLAUDE.md "Lazy closure capture" section).
- **`_ingested_at` column:** NOT in the UDF output schema. The `write_delta_table()` utility adds it automatically when writing to Delta. The UDF returns only `(tracking_id, match_id, pitch_control_value)`.
- UDF output schema: `StructType([StructField("tracking_id", StringType()), StructField("match_id", StringType()), StructField("pitch_control_value", DoubleType())])`

The UDF function (`_make_udf`) for each group:
1. Lazy-imports `compute_pitch_control_at_points` and `PitchControlParams` inside the function body
2. Groups the batch DataFrame by `frame` to get per-frame player sets
3. For each frame: extracts all player positions as `target_points` array `(N, 2)`
4. Calls `compute_pitch_control_at_points(frame_df, target_points, params)` — returns `(N,)` array
5. Each player's control value is at their own index in the result
6. Returns `(tracking_id, match_id, pitch_control_value)` rows

The pipeline's `main()`:
1. Reads `fct_tracking_frames` filtered to new match_ids (incremental skip guard against `bronze.pitch_control_values`)
2. Adds `frame_batch_id = (frame / batch_size).cast("int")` synthetic partition key
3. `groupBy("match_id", "frame_batch_id").applyInPandas(udf, schema)`
4. Writes to `bronze.pitch_control_values` with `replaceWhere` on `match_id`

- [ ] **Step 4: Add entry point to pyproject.toml**

```toml
[project.scripts]
# ... existing entries ...
compute_pitch_control = "ingestion.pitch_control_batch:main"
```

- [ ] **Step 5: Run tests and verify they pass**

```bash
uv run pytest src/tests/test_pitch_control_batch.py -v
```

### Subtask 5.3: Create dbt staging layer

- [ ] **Step 7: Create bronze source definition**

`dbt_project/models/staging/pitch_control/_pitch_control__sources.yml` — follows `_off_ball_xt__sources.yml` exactly:

```yaml
version: 2

sources:
  - name: pitch_control
    description: >
      Per-player per-frame pitch control values computed by the Spearman 2017
      physics-based model. Each row is one player in one frame with the home-team
      control probability at that player's position.
    database: soccer_analytics
    schema: bronze
    loader: python_wheel
    config:
      loaded_at_field: _ingested_at
      freshness:
        warn_after: {count: 24, period: hour}
        error_after: {count: 72, period: hour}

    tables:
      - name: pitch_control_values
        description: >
          Per-player per-frame pitch control probability [0,1] at each player's
          (x, y) position. Computed via applyInPandas batch pipeline.
        columns:
          - name: tracking_id
            description: "FK to fct_tracking_frames.tracking_id"
          - name: match_id
            description: "Match identifier for partition filtering"
          - name: pitch_control_value
            description: "Home-team control probability [0,1] at this player's position"
```

- [ ] **Step 8: Create staging model**

`dbt_project/models/staging/pitch_control/stg_pitch_control__values.sql` — follows `stg_off_ball_xt__results.sql`:

```sql
-- stg_pitch_control__values.sql
-- Clean and deduplicate pitch control values from the bronze layer.
-- Dedup: ROW_NUMBER partitioned by tracking_id, latest _ingested_at wins.

with source as (
    select * from {{ source('pitch_control', 'pitch_control_values') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by tracking_id
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        cast(tracking_id as string)              as tracking_id,
        cast(match_id as string)                 as match_id,
        cast(pitch_control_value as double)      as pitch_control_value
    from deduplicated
    where _row_num = 1
)

select * from cleaned
```

- [ ] **Step 9: Create staging model YAML**

`dbt_project/models/staging/pitch_control/_pitch_control__models.yml`:

```yaml
version: 2

models:
  - name: stg_pitch_control__values
    description: "Deduplicated pitch control values from bronze layer."
    columns:
      - name: tracking_id
        description: "FK to fct_tracking_frames"
        tests: [unique, not_null]
      - name: match_id
        description: "Match identifier"
        tests: [not_null]
      - name: pitch_control_value
        description: "Home-team control probability [0,1]"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.0
              max_value: 1.0
```

- [ ] **Step 10: Verify dbt compiles with new staging model**

```bash
MSYS_NO_PATHCONV=1 uv run python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['compile', '--select', 'stg_pitch_control__values', '--project-dir', 'dbt_project', '--profiles-dir', 'dbt_project'])"
```

---

## Task 6: Publish Pitch Control Dataset

**Files:**
- Create: `docs/huggingface/dataset-cards/pitch-control.md`
- Modify: `notebooks/publish_datasets.py`
- HF repo: `luxury-lakehouse/pitch-control-tracking`

- [ ] **Step 1: Write pitch control dataset card**

Key details:
- Rows: ~38M (one per player per frame per match)
- Columns: `tracking_id`, `match_id`, `player_id`, `team`, `frame`, `period`, `x`, `y`, `pitch_control_value`, `source_provider`
- 20 matches from 3 providers (Metrica, IDSSE, SkillCorner)
- Method: Spearman 2017 physics-based model (cite paper)
- Coordinate system: StatsBomb 120x80
- Limitations: only 20 matches have tracking data

- [ ] **Step 2: Add export cell to notebook**

Query JOINs `fct_tracking_frames` with pitch control values from the staging model (since `fct_tracking_frames.pitch_control_value` stays NULL):
```sql
SELECT t.tracking_id, t.match_id, t.player_id, t.team, t.period, t.frame,
       t.timestamp_seconds, t.x, t.y, t.ball_x, t.ball_y,
       t.velocity_x, t.velocity_y, t.speed_ms,
       pc.pitch_control_value,
       t.source_provider, t.frame_rate
FROM {CATALOG}.{GOLD_SCHEMA}.fct_tracking_frames t
INNER JOIN {CATALOG}.dev_silver.stg_pitch_control__values pc
  ON t.tracking_id = pc.tracking_id
ORDER BY t.match_id, t.frame, t.player_id
```

Note: Staging models are views in `dev_silver` (dbt convention: staging → silver schema). The INNER JOIN ensures only frames with computed values are exported. Partition by `source_provider`.

---

## Task 7: JAX Pitch Control Kernel

**Files:**
- Modify: `pyproject.toml` (add `jax` optional dep)
- Modify: `src/analytics/pitch_control.py` (conditional JAX backend)
- Modify: `src/tests/test_pitch_control_model.py` (parity tests)
- Modify: `src/tests/test_benchmarks.py` (JAX benchmark)

### Subtask 7.1: Add JAX dependency

- [ ] **Step 1: Add optional JAX dependency group**

```toml
[project.optional-dependencies]
# ... existing ...
jax = [
    "jax[cpu]>=0.4.35",
]
```

- [ ] **Step 2: Install and verify**

```bash
uv sync --extra jax
uv run python -c "import jax; print(jax.__version__)"
```

### Subtask 7.2: Write JAX parity tests

- [ ] **Step 4: Add parametrized tests for JAX/NumPy parity**

In `src/tests/test_pitch_control_model.py`, add:

**Important design note:** The `_USE_JAX` flag and `@jax.jit` functions are defined at import time. Monkeypatching `_USE_JAX` after import doesn't make JIT functions appear/disappear. Instead, test both backends by calling the internal `_tti_jax` / `_tti_numpy` functions directly, and test the public API with JAX installed (it auto-selects JAX when available).

```python
jax = pytest.importorskip("jax")


class TestJaxNumPyParity:
    """Ensure JAX and NumPy backends produce identical results.

    Strategy: call the internal NumPy functions explicitly (they always exist)
    and compare against the JAX functions (which exist when jax is installed).
    Public API auto-selects JAX when available — test it separately.
    """

    @pytest.fixture
    def sample_inputs(self):
        """Fixed seed inputs for reproducible parity checks."""
        rng = np.random.default_rng(42)
        player_pos = rng.uniform(0, 105, (22, 2))
        player_vel = rng.uniform(-2, 2, (22, 2))
        targets = rng.uniform(0, 105, (10, 2))
        return player_pos, player_vel, targets

    def test_tti_parity(self, sample_inputs):
        """Time-to-intercept matches between backends (atol=1e-6)."""
        from analytics.pitch_control import _tti_numpy, _tti_jax
        player_pos, player_vel, targets = sample_inputs
        params = PitchControlParams()
        np_result = _tti_numpy(player_pos, player_vel, targets, params.reaction_time, params.max_acceleration)
        jax_result = np.asarray(_tti_jax(
            jax.numpy.asarray(player_pos), jax.numpy.asarray(player_vel),
            jax.numpy.asarray(targets), params.reaction_time, params.max_acceleration,
        ))
        np.testing.assert_allclose(np_result, jax_result, atol=1e-6)

    def test_influence_parity(self, sample_inputs):
        """Team influence matches between backends."""
        ...

    def test_frame_parity(self):
        """Full frame computation via public API produces valid results.

        With JAX installed, the public API auto-selects JAX. Compare against
        known physical constraints (values in [0,1], home near own goal > 0.5).
        """
        ...

    def test_batch_points_parity(self, sample_inputs):
        """Batch point computation matches between backends."""
        ...

    def test_nan_handling_parity(self):
        """NaN velocity handling is identical."""
        ...
```

- [ ] **Step 5: Run parity tests (expect fail — `_tti_numpy`/`_tti_jax` don't exist yet)**

```bash
uv run pytest src/tests/test_pitch_control_model.py::TestJaxNumPyParity -v
```

Expected: `ImportError: cannot import name '_tti_numpy' from 'analytics.pitch_control'`. This is correct — Step 9 will extract the existing NumPy implementation into `_tti_numpy` and the JAX kernel becomes `_tti_jax`.

### Subtask 7.3: Implement JAX backend

- [ ] **Step 6: Add conditional JAX import and backend flag**

At the top of `src/analytics/pitch_control.py`:

```python
try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False
```

- [ ] **Step 7: Implement JIT-compiled TTI kernel**

```python
if _USE_JAX:
    @jax.jit
    def _tti_jax(
        player_pos_m: jax.Array,   # (n_players, 2)
        player_vel_m: jax.Array,   # (n_players, 2)
        target_m: jax.Array,       # (n_targets, 2)
        reaction_time: float,
        max_acceleration: float,
    ) -> jax.Array:                # (n_players, n_targets)
        displacement = target_m[jnp.newaxis, :, :] - player_pos_m[:, jnp.newaxis, :]
        distance = jnp.sqrt(jnp.sum(displacement**2, axis=2))
        safe_distance = jnp.maximum(distance, 1e-10)
        direction = displacement / safe_distance[:, :, jnp.newaxis]
        v_proj = jnp.sum(player_vel_m[:, jnp.newaxis, :] * direction, axis=2)
        discriminant = v_proj**2 + 2.0 * max_acceleration * distance
        tti = reaction_time + (-v_proj + jnp.sqrt(discriminant)) / max_acceleration
        return jnp.maximum(tti, reaction_time)
```

- [ ] **Step 8: Implement JIT-compiled influence kernel**

```python
if _USE_JAX:
    @jax.jit
    def _influence_jax(
        team_tti: jax.Array,          # (n_players, n_targets)
        opponent_min_tti: jax.Array,  # (n_targets,)
        sigma: float,
    ) -> jax.Array:                   # (n_targets,)
        k = jnp.pi / jnp.sqrt(3.0) / sigma
        exponent = -k * (opponent_min_tti[jnp.newaxis, :] - team_tti)
        individual = 1.0 / (1.0 + jnp.exp(jnp.clip(exponent, -50.0, 50.0)))
        return jnp.sum(individual, axis=0)
```

- [ ] **Step 9: Refactor existing NumPy internals and wire JAX dispatch**

Rename existing NumPy TTI/influence implementations to `_tti_numpy` / `_influence_numpy` (extract the computation from `_compute_time_to_intercept` into a standalone function). This makes both backends testable independently. Then add dispatch:

```python
def _tti_numpy(
    player_pos_m: np.ndarray,
    player_vel_m: np.ndarray,
    target_m: np.ndarray,
    reaction_time: float,
    max_acceleration: float,
) -> np.ndarray:
    """NumPy TTI kernel — extracted from _compute_time_to_intercept."""
    # ... existing NumPy broadcasting implementation, unchanged ...


def _compute_time_to_intercept(player_pos_m, player_vel_m, target_m, params):
    """Dispatch to JAX or NumPy TTI kernel."""
    if _USE_JAX:
        result = _tti_jax(
            jnp.asarray(player_pos_m), jnp.asarray(player_vel_m),
            jnp.asarray(target_m), params.reaction_time, params.max_acceleration,
        )
        return np.asarray(result)  # Convert back to numpy for callers
    return _tti_numpy(player_pos_m, player_vel_m, target_m, params.reaction_time, params.max_acceleration)
```

Apply the same pattern to `_compute_team_influence` → `_influence_numpy` + dispatch.

The public API remains unchanged — callers get numpy arrays regardless of backend. Both `_tti_numpy` and `_tti_jax` are importable for direct parity testing.

- [ ] **Step 10: Add full-grid vmap function (the OBSO unlock)**

```python
def compute_pitch_control_grid_fast(
    players_df: pd.DataFrame,
    grid_cells_x: int = 104,
    grid_cells_y: int = 68,
    params: PitchControlParams | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute pitch control over a dense grid using JAX vmap.

    Like compute_pitch_control_frame but for much larger grids
    (e.g., 104x68 = 7,072 points). Requires JAX.

    Returns:
        grid_x: 1-D array of x coordinates (shape ``(grid_cells_x,)``)
        grid_y: 1-D array of y coordinates (shape ``(grid_cells_y,)``)
        surface: 2-D array of pitch control values (shape ``(grid_cells_y, grid_cells_x)``)
            where ``surface[j, i]`` is the home-team control at ``(grid_x[i], grid_y[j])``.

    Return order matches ``compute_pitch_control_frame``: ``(grid_x, grid_y, surface)``.

    Raises:
        ImportError: If JAX is not installed.
    """
    if not _USE_JAX:
        raise ImportError("JAX required for compute_pitch_control_grid_fast. Install with: pip install jax[cpu]")
    # ... use jax.vmap over grid points ...
```

- [ ] **Step 11: Run parity tests**

```bash
uv run pytest src/tests/test_pitch_control_model.py -v
```

- [ ] **Step 12: Run full test suite + linting**

```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

### Subtask 7.4: Add benchmarks

- [ ] **Step 14: Add JAX benchmark to test_benchmarks.py**

```python
class TestJaxBenchmarks:
    @pytest.mark.skipif(not _USE_JAX, reason="JAX not installed")
    def test_bench_jax_batched_pitch_control(self, benchmark, players_df, target_points_22, pitch_control_params):
        """JAX backend: 22 target points."""
        # Warm up JIT
        compute_pitch_control_at_points(players_df, target_points_22, pitch_control_params)
        result = benchmark(compute_pitch_control_at_points, players_df, target_points_22, pitch_control_params)
        assert result.shape == (22,)

    @pytest.mark.skipif(not _USE_JAX, reason="JAX not installed")
    def test_bench_jax_dense_grid(self, benchmark, players_df, pitch_control_params):
        """JAX vmap: 104x68 = 7,072 target points (OBSO-scale)."""
        # Warm up JIT
        compute_pitch_control_grid_fast(players_df, 104, 68, pitch_control_params)
        grid_x, grid_y, surface = benchmark(compute_pitch_control_grid_fast, players_df, 104, 68, pitch_control_params)
        assert grid_x.shape == (104,)
        assert grid_y.shape == (68,)
        assert surface.shape == (68, 104)  # (grid_cells_y, grid_cells_x)
```

- [ ] **Step 15: Run benchmarks**

```bash
uv run pytest src/tests/test_benchmarks.py -v --benchmark-only
```

---

## Task 8: Symmetry Augmentation Module

**Files:**
- Create: `src/analytics/symmetry.py`
- Create: `src/tests/test_symmetry.py`

### Subtask 8.1: Write tests

- [ ] **Step 1: Write symmetry augmentation tests**

```python
# src/tests/test_symmetry.py
"""Tests for symmetry augmentation (TacticAI-inspired 8x multiplier)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.symmetry import (
    flip_horizontal,
    flip_vertical,
    swap_teams,
    augment_tracking_frame,
    AugmentationConfig,
)


class TestFlipHorizontal:
    def test_x_coordinates_mirrored(self):
        """x -> pitch_length - x for StatsBomb 120x80."""

    def test_velocity_x_negated(self):
        """velocity_x -> -velocity_x."""

    def test_ball_x_mirrored(self):
        """ball_x -> pitch_length - ball_x."""

    def test_y_unchanged(self):
        """y coordinates are NOT affected by H-flip."""


class TestFlipVertical:
    def test_y_coordinates_mirrored(self):
        """y -> pitch_width - y for StatsBomb 120x80."""

    def test_velocity_y_negated(self):

    def test_x_unchanged(self):


class TestSwapTeams:
    def test_home_becomes_away(self):
        """team column: 'home' -> 'away', 'away' -> 'home'."""

    def test_other_columns_unchanged(self):


class TestAugmentTrackingFrame:
    def test_eight_variants_produced(self):
        """Original + 7 augmentations = 8 total."""

    def test_all_variants_same_shape(self):

    def test_original_included_unchanged(self):

    def test_round_trip_double_flip(self):
        """H-flip(H-flip(frame)) == original."""
```

- [ ] **Step 2: Run tests (expect fail)**

```bash
uv run pytest src/tests/test_symmetry.py -v
```

### Subtask 8.2: Implement symmetry module

- [ ] **Step 3: Write the symmetry augmentation module**

```python
# src/analytics/symmetry.py
"""Symmetry augmentation for tracking data (TacticAI, DeepMind 2024).

Produces up to 8x data from H-flip, V-flip, and team swap combinations.
All operations are pure NumPy on pandas DataFrames. No side effects.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AugmentationConfig:
    """Pitch dimensions for coordinate mirroring."""
    pitch_length: float = 120.0   # StatsBomb x-axis
    pitch_width: float = 80.0     # StatsBomb y-axis
    x_col: str = "x"
    y_col: str = "y"
    vx_col: str = "velocity_x"
    vy_col: str = "velocity_y"
    ball_x_col: str = "ball_x"
    ball_y_col: str = "ball_y"
    team_col: str = "team"


def flip_horizontal(df: pd.DataFrame, config: AugmentationConfig | None = None) -> pd.DataFrame:
    """Mirror pitch left-to-right: x -> pitch_length - x, vx -> -vx."""

def flip_vertical(df: pd.DataFrame, config: AugmentationConfig | None = None) -> pd.DataFrame:
    """Mirror pitch top-to-bottom: y -> pitch_width - y, vy -> -vy."""

def swap_teams(df: pd.DataFrame, config: AugmentationConfig | None = None) -> pd.DataFrame:
    """Swap home/away labels."""

def augment_tracking_frame(
    df: pd.DataFrame,
    config: AugmentationConfig | None = None,
    include_original: bool = True,
) -> list[pd.DataFrame]:
    """Generate all 8 symmetry variants of a tracking frame.

    Combinations: {identity, H-flip} x {identity, V-flip} x {identity, team-swap}
    = 2 x 2 x 2 = 8 variants.

    Returns list of 8 (or 7 if include_original=False) DataFrames.
    Each has an 'augmentation' column indicating the transform applied.
    """
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest src/tests/test_symmetry.py -v
```

- [ ] **Step 5: Run full quality checks**

```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

---

## Task 9: Demo Space (Tier 4)

**Files:**
- Create: `demo_space/app.py`
- Create: `demo_space/requirements.txt`
- Create: `demo_space/data/` (pre-cached Parquet subsets)
- HF Space: `luxury-lakehouse/soccer-analytics-demo`

### Design Decisions

- **Framework: Gradio** — first-class HF Spaces support, automatic API generation, better for model demos than Streamlit. Does NOT reuse Databricks Streamlit code (different data access: static Parquet vs live Lakebase).
- **Data: pre-cached Parquet** — no live database connectivity from HF Spaces. Export representative subsets (1-2 matches per source, top 100 players by match count).
- **Tabs:** Player Similarity (cosine search on career vectors), Shot Map (xG scatter), Pass Quality (line-breaking + VAEP overlay), Pitch Control (if populated).

- [ ] **Step 1: Exclude `demo_space/` from quality gates**

`demo_space/` is a standalone Gradio app deployed to HF Spaces — it has its own `requirements.txt` and doesn't import from `src/`. Exclude from ruff, pyright, and pytest:

In `pyproject.toml`, add to `[tool.pyright]`:
```toml
[tool.pyright]
exclude = [
    # ... existing excludes ...
    "demo_space",
]
```

In `pyproject.toml`, add to `[tool.ruff]`:
```toml
[tool.ruff]
exclude = [
    # ... existing excludes ...
    "demo_space",
]
```

Verify: `uv run ruff check src/` and `uv run pyright src/` should not scan `demo_space/`.

- [ ] **Step 2: Create Space directory structure**

```
demo_space/
  app.py              # Gradio app
  requirements.txt    # gradio, pandas, numpy, plotly
  data/
    career_embeddings.parquet  # Top 100 players
    sample_shots.parquet       # 1 competition subset
    sample_passes.parquet      # 1 competition subset
```

- [ ] **Step 3: Write Gradio app with Player Similarity tab**

Core pattern: load Parquet at startup, compute cosine distances in NumPy, display with Gradio `DataFrame` and `Plot` components.

```python
import gradio as gr
import numpy as np
import pandas as pd

embeddings = pd.read_parquet("data/career_embeddings.parquet")

def find_similar(player_name: str, top_k: int = 10) -> pd.DataFrame:
    """Find most similar players by cosine distance on behavioral vectors."""
    # ...

demo = gr.Blocks(title="Soccer Analytics Demo")
with demo:
    gr.Markdown("# Soccer Analytics Explorer")
    with gr.Tab("Player Similarity"):
        # ...
    with gr.Tab("Shot Map"):
        # ...
    with gr.Tab("Pass Quality"):
        # ...

demo.launch()
```

- [ ] **Step 4: Add data export cells to publishing notebook**

Add cells to `notebooks/publish_datasets.py` that export small subsets for the Space (top 100 players, 1 competition of shots/passes).

- [ ] **Step 5: Test Space locally**

```bash
cd demo_space && pip install -r requirements.txt && python app.py
```

- [ ] **Step 6: Publish Space to HF Hub**

```python
api.create_repo(f"{HF_ORG}/soccer-analytics-demo", repo_type="space",
                space_sdk="gradio", exist_ok=True)
api.upload_folder(folder_path="demo_space/", repo_id=f"{HF_ORG}/soccer-analytics-demo",
                  repo_type="space", token=hf_token)
```

---

## Task 10: Documentation Updates

**Files:**
- Modify: `ROADMAP.md`
- Modify: `TODO.md`
- Modify: `PLAN.md`
- Modify: `docs/huggingface/org-card.md`

- [ ] **Step 1: Update ROADMAP.md HF Hub section**

- Tier 2 status: "COMPLETE" (model + 3-4 datasets published)
- Tier 4 status: "COMPLETE" (demo Space live)
- Resolve open questions #5 (Gradio chosen) and #6 (defer HF Jobs comparison)
- Add JAX vmap to DL Infrastructure as "COMPLETE (CPU pitch control)"
- Update Space Creation: "Partially unblocked — JAX vmap enables OBSO computation"

- [ ] **Step 2: Update TODO.md**

- Resolve #5: `pitch_control_value` populated for 20 tracking matches
- Update #14: Space Creation partially unblocked (JAX vmap available)
- Update #17: Note OpenEvolve as next step for xT grid evolution
- Add note about OpenSTARLab findings (no pre-trained weights, deferred)

- [ ] **Step 3: Update PLAN.md**

Add Phase 18 to completed phases table:
```
| **18** | HF Hub Expansion | 3-4 HF dataset repos published (SPADL/VAEP, line-breaking, embeddings, pitch control), JAX pitch control backend, pitch_control_value populated (38M frames), symmetry augmentation module, Gradio demo Space |
```

- [ ] **Step 4: Update org card**

Add new published datasets to `docs/huggingface/org-card.md`.

---

## Task 11: HF Hub Cards and Model Card Update

**Files:**
- Modify: `docs/huggingface/model-card.md`
- Modify: `docs/huggingface/org-card.md`

The publishing notebook (Task 1) pushes dataset cards as `README.md` inside each dataset repo. This task handles the **model card update** and **org card update** which are separate from the per-dataset cards.

- [ ] **Step 1: Update existing model card**

Update `docs/huggingface/model-card.md` with:
- Reference to the new companion datasets (SPADL/VAEP, line-breaking, player embeddings)
- Updated `STAT_FEATURES` list if it changed since Phase 17
- Link to the demo Space as an interactive way to explore the model's outputs

- [ ] **Step 2: Update org card with new published artifacts**

Update `docs/huggingface/org-card.md` with:
- New dataset repos listed with descriptions
- Demo Space link
- Updated project overview reflecting Tier 2 complete + Tier 4

- [ ] **Step 3: Push model card to HF Hub**

The publishing notebook should include a cell that pushes the updated model card:
```python
# Push updated model card to existing model repo
api.upload_file(
    path_or_fileobj="docs/huggingface/model-card.md",
    path_in_repo="README.md",
    repo_id=f"{HF_ORG}/football2vec-statsbomb-wyscout",
    repo_type="model",
    token=hf_token,
)
```

Note: Org card must still be pasted manually via HF web UI (Settings > Organization card) — no API endpoint exists for org profile READMEs.

---

## Task 12: Local Quality Gate

Run all local quality checks before any git operations.

- [ ] **Step 1: Ruff lint + format**

```bash
uv run ruff check src/ && uv run ruff format --check src/
```

- [ ] **Step 2: Pyright type check**

```bash
uv run pyright src/
```

- [ ] **Step 3: Full test suite**

```bash
uv run pytest src/tests/ -v
```

- [ ] **Step 4: dbt compile (SQL validity)**

```bash
MSYS_NO_PATHCONV=1 uv run python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['compile', '--select', 'stg_pitch_control__values', '--project-dir', 'dbt_project', '--profiles-dir', 'dbt_project'])"
```

- [ ] **Step 5: Benchmarks (if JAX installed)**

```bash
uv run pytest src/tests/test_benchmarks.py -v --benchmark-only
```

All checks must pass with zero violations before proceeding.

---

## Task 13: Deploy and Run Pipelines on Databricks

**Prerequisites:** All code from Tasks 1-11 is written and passes local quality gate (Task 12).

### Subtask 13.1: Deploy pitch control batch pipeline

- [ ] **Step 1: Add `compute_pitch_control` task to Databricks workflow**

Either via Terraform (`terraform/environments/dev/main.tf` — add task to ingestion workflow) or manually via Databricks UI. The task should:
- Use the `analytics` environment (same as `compute_off_ball_xt`)
- Entry point: `compute_pitch_control`
- Depends on: tracking data tasks (Metrica, IDSSE, SkillCorner)

```bash
cd terraform/environments/dev && AWS_PROFILE=devops-agent terraform plan
# Review, then:
AWS_PROFILE=devops-agent terraform apply
```

- [ ] **Step 2: Run pitch control batch pipeline**

Trigger the pipeline on Databricks for all 20 tracking matches. Monitor job status:

```bash
databricks jobs run-now --job-id $(terraform -chdir=terraform/environments/dev output -raw ingestion_job_id) --profile OAUTH
```

Verify: `bronze.pitch_control_values` table should contain ~38M rows (one per player per frame per match across 20 matches).

### Subtask 13.2: Rebuild dbt

- [ ] **Step 3: Run dbt build with staging model**

```bash
MSYS_NO_PATHCONV=1 uv run python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--select', 'stg_pitch_control__values', '--project-dir', 'dbt_project', '--profiles-dir', 'dbt_project'])"
```

Verify: staging view created in `dev_silver`, data tests (unique, not_null, between 0-1) all pass.

- [ ] **Step 4: Run full dbt build to verify no regressions**

```bash
MSYS_NO_PATHCONV=1 uv run python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--project-dir', 'dbt_project', '--profiles-dir', 'dbt_project'])"
```

All existing tests must continue to pass.

### Subtask 13.3: Publish to HF Hub

- [ ] **Step 5: Upload publishing notebook to Databricks workspace**

```bash
databricks workspace import notebooks/publish_datasets.py /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/notebooks/publish_datasets.py --format SOURCE --language PYTHON --overwrite --profile OAUTH
```

- [ ] **Step 6: Run publishing notebook on Databricks**

Execute the notebook interactively or via job. This will:
1. Export SPADL/VAEP action values as Parquet → push to `luxury-lakehouse/spadl-vaep-action-values`
2. Export line-breaking passes as Parquet → push to `luxury-lakehouse/line-breaking-passes`
3. Export player embeddings (3 configs) → push to `luxury-lakehouse/football2vec-player-embeddings`
4. Export pitch control tracking data → push to `luxury-lakehouse/pitch-control-tracking`
5. Push updated model card to `luxury-lakehouse/football2vec-statsbomb-wyscout`
6. Export demo Space data subsets to `demo_space/data/`

- [ ] **Step 7: Verify all HF dataset repos have working viewers**

For each dataset repo, verify:
- Dataset card renders correctly (description, columns table, metadata)
- Dataset viewer shows Parquet data with correct column types
- Download links work
- `configs` section shows all configurations (for player embeddings)

Check: `https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values`
Check: `https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes`
Check: `https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings`
Check: `https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking`
Check: `https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout` (model card updated)

### Subtask 13.4: Deploy demo Space

- [ ] **Step 8: Push demo Space to HF Hub**

From the publishing notebook or locally:
```python
api.create_repo(f"{HF_ORG}/soccer-analytics-demo", repo_type="space",
                space_sdk="gradio", exist_ok=True)
api.upload_folder(folder_path="demo_space/", repo_id=f"{HF_ORG}/soccer-analytics-demo",
                  repo_type="space", token=hf_token)
```

- [ ] **Step 9: Verify demo Space loads and all tabs work**

Check: `https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo`
- Player Similarity tab: search a player, get top-10 results
- Shot Map tab: renders scatter plot
- Pass Quality tab: renders line-breaking overlay

### Subtask 13.5: Update org card manually

- [ ] **Step 10: Paste updated org card via HF web UI**

Go to `https://huggingface.co/luxury-lakehouse` > Settings > Organization card. Paste the contents of `docs/huggingface/org-card.md`.

---

## Task 14: End-to-End Verification

Final verification that everything works together.

- [ ] **Step 1: Verify pitch control values in Databricks**

```sql
-- Check row count
SELECT count(*) FROM soccer_analytics.bronze.pitch_control_values;
-- Expected: ~38M rows

-- Check value range
SELECT min(pitch_control_value), max(pitch_control_value), avg(pitch_control_value)
FROM soccer_analytics.bronze.pitch_control_values;
-- Expected: min ~0.0, max ~1.0, avg ~0.5

-- Check staging view works
SELECT count(*) FROM soccer_analytics.dev_silver.stg_pitch_control__values;
-- Should match bronze count (after dedup)
```

- [ ] **Step 2: Verify HF Hub dataset download works**

```python
from datasets import load_dataset

# Test each dataset loads correctly
ds = load_dataset("luxury-lakehouse/spadl-vaep-action-values")
assert len(ds["train"]) > 1_000_000

ds = load_dataset("luxury-lakehouse/line-breaking-passes")
assert len(ds["train"]) > 100_000

ds = load_dataset("luxury-lakehouse/football2vec-player-embeddings", "career")
assert len(ds["train"]) > 5_000

ds = load_dataset("luxury-lakehouse/pitch-control-tracking")
assert len(ds["train"]) > 1_000_000
```

- [ ] **Step 3: Verify Gradio Space is responsive**

Load the Space URL, interact with each tab, verify no errors in the build logs.

- [ ] **Step 4: Run full local test suite one final time**

```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

---

## Task 15: Git Stage, Commit, and PR

Only after ALL previous tasks pass.

- [ ] **Step 1: Stage all changes**

```bash
git add \
  notebooks/publish_datasets.py \
  docs/huggingface/dataset-cards/ \
  docs/huggingface/model-card.md \
  docs/huggingface/org-card.md \
  src/ingestion/pitch_control_batch.py \
  src/tests/test_pitch_control_batch.py \
  src/analytics/pitch_control.py \
  src/tests/test_pitch_control_model.py \
  src/tests/test_benchmarks.py \
  src/analytics/symmetry.py \
  src/tests/test_symmetry.py \
  demo_space/ \
  dbt_project/models/staging/pitch_control/ \
  pyproject.toml uv.lock \
  terraform/environments/dev/main.tf \
  ROADMAP.md TODO.md PLAN.md \
  docs/superpowers/plans/2026-03-11-hf-hub-expansion.md
```

- [ ] **Step 2: Commit**

Single commit with comprehensive message:

```bash
git commit -m "feat: HF Hub expansion — 4 datasets, JAX pitch control, symmetry augmentation, demo Space (Phase 18)"
```

- [ ] **Step 3: Push and create PR**

```bash
git push -u origin feat/hf-hub-expansion
gh pr create --title "feat: HF Hub expansion (Phase 18)" --body "..."
```

---

## Risk Register

| Risk | Mitigation |
|------|-----------|
| JAX not compatible with Databricks serverless executors | JAX is optional — pitch control batch pipeline uses NumPy by default. JAX only needed for OBSO-scale grids. |
| HF dataset upload exceeds 10 GB free limit | SPADL/VAEP is the largest (~2 GB Parquet). Well within limit. |
| Pitch control batch pipeline OOM on 38M frames | Already mitigated by `applyInPandas` with 500-frame batches. Each group: 500 frames × 22 players × 8 bytes × ~10 cols ≈ 880 KB. Well under 1 GB. |
| Staging model fails before bronze table exists | `stg_pitch_control__values` is a view — dbt compile succeeds even if source table is empty. Only `dbt build --select stg_pitch_control__values` needs the bronze table. |
| Gradio Space data staleness | Data is pre-cached Parquet snapshots. Documented as "point-in-time export" in Space description. |
