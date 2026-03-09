# Phase 15+16: pgvector Player Embeddings with HuggingFace Integration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add pgvector player embeddings (behavioral + statistical) with HuggingFace Hub integration and a Player Similarity Streamlit page.

**Architecture:** Retrain football2vec (gensim Doc2Vec) on ~4,900 StatsBomb+Wyscout matches to produce 32-dim behavioral embeddings per player-match. Compute 13-dim z-score normalized stat vectors from fct_player_stats. Store both in Delta, sync to Lakebase, query via pgvector HNSW indexes. Publish model to HuggingFace Hub.

**Tech Stack:** gensim (Doc2Vec), huggingface_hub, MLflow (custom pyfunc), pgvector (HNSW), dbt-databricks, Streamlit, mplsoccer

**Date:** 2026-03-08
**Branch:** `feature/player-embeddings-hf`
**Status:** Design approved, implementation pending

---

## Decisions

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | Vector types | Both behavioral (32-dim Doc2Vec) + statistical (13-dim per-90) | Captures playing style and output separately; user chooses search axis |
| 2 | Training data | Retrain on our ~3,000 SB + ~1,900 WY matches | Richer corpus than original ~900; produces novel publishable artifact |
| 3 | Aggregation grain | Per-match (aggregate downstream) | Store raw signal, derive season/career via mean pooling; enterprise-ready |
| 4 | Scope | Phase 15+16 together | Can't validate embeddings without similarity page |
| 5 | Minutes threshold | None (filter in UI) | Store everything; Streamlit defaults to min 5 matches |
| 6 | HF org | `luxury-lakehouse` | Matches repo branding |
| 7 | Wyscout events | Included in training | Maximizes player coverage (~11,900 vs ~8,300) |
| 8 | Index type | HNSW expression indexes | Better recall than IVFFlat; holds at enterprise scale for career/season tables |
| 9 | Deployment | Full E2E on Databricks before any git commit | No partial commits |

---

## Architecture

```
                         TRAINING PIPELINE (one-time + periodic)
StatsBomb bronze events --+
                          +--> Shared Tokenizer --> gensim Doc2Vec --> Model Artifacts
Wyscout bronze events ----+      (event_type +         |
                                  pitch grid)          +--> UC Volume (primary)
                                                       +--> HF Hub (publish)

                         INFERENCE PIPELINE (Databricks workflow task)
UC Volume / HF Hub --> Load Model --> Infer per-player-per-match --+
                                                                    |
fct_player_stats --> Normalize per-90 metrics ----------------------+
                                                                    |
                                                          +---------+
                                                          v
                                              bronze.player_embeddings_raw
                                                          |
                                                     dbt build
                                                          |
                                              +-----------+-----------+
                                              v           v           v
                                    fct_player      fct_player    fct_player
                                    _embeddings     _embeddings   _embeddings
                                    (per-match)     _season       _career
                                              |
                                         Synced Table
                                              |
                                    Lakebase PG 17 (pgvector)
                                              |
                                    Streamlit: Player Similarity
```

---

## Components

| # | Component | File | New/Modified |
|---|-----------|------|-------------|
| 1 | Shared tokenizer + training | `src/analytics/football2vec.py` | New |
| 2 | MLflow custom pyfunc wrapper | `src/analytics/football2vec.py` | New |
| 3 | Inference + stat vector pipeline | `src/ingestion/player_embeddings.py` | New |
| 4 | Staging model | `dbt_project/models/staging/embeddings/stg_player_embeddings.sql` | New |
| 5 | Per-match mart | `dbt_project/models/marts/fct_player_embeddings.sql` | Redesign |
| 6 | Season aggregation | `dbt_project/models/marts/fct_player_embeddings_season.sql` | New |
| 7 | Career aggregation | `dbt_project/models/marts/fct_player_embeddings_career.sql` | New |
| 8 | Player Similarity page | `src/streamlit_app/pages/player_similarity.py` | New |
| 9 | pgvector setup + indexes | `scripts/create_indexes.py` | Modified |
| 10 | Terraform workflow | `terraform/modules/workflows/main.tf` | Modified |
| 11 | HF setup guide | `docs/huggingface-setup.md` | New |

---

## HuggingFace Integration (Minimum Viable)

Establishes Tiers 1+2 from ROADMAP. Carries forward to all future HF work.

| Item | Detail |
|------|--------|
| HF Org | `luxury-lakehouse` on huggingface.co (free) |
| Python dep | `huggingface_hub` (pure Python, no torch) |
| `HF_HOME` | Databricks: `/Volumes/soccer_analytics/dev_gold/model_weights/hf_cache` |
| `HF_TOKEN` | Env var for uploads; not needed for public downloads |
| Published repo | `luxury-lakehouse/football2vec-statsbomb-wyscout` |

### Published artifact structure

```
luxury-lakehouse/football2vec-statsbomb-wyscout/
  action2vec.model            # gensim Word2Vec
  player2vec.model            # gensim Doc2Vec
  tokenizer_config.json       # Grid resolution, event mapping, vocabulary
  README.md                   # Model card
  training_metadata.json      # Corpus stats, hyperparams, MLflow run ID
```

### MLflow integration

Custom pyfunc wrapper: `Football2VecModel(mlflow.pyfunc.PythonModel)` with `load_context()` + `predict()`. Registered in Unity Catalog as `soccer_analytics.dev_gold.football2vec` with `@Champion`/`@Challenger` aliases.

### Deferred (correctly)

- PyTorch/transformers (not needed for gensim)
- HF Jobs / GPU training (Tier 3)
- Public demo Space (Tier 4)
- Dataset publishing (can do later)

---

## Tokenizer Design

Each event becomes a token: `{action_type}_{grid_x}_{grid_y}`

**Pitch grid:** 12 columns x 8 rows (matches xT grid). Each cell = 10x10 yards on the 120x80 coordinate system.

### Unified event type mapping (14 types)

| Token | StatsBomb `event_type` | Wyscout `event_type` + `sub_event_type` |
|-------|----------------------|---------------------------------------|
| `pass` | Pass | Pass (all sub-types) |
| `shot` | Shot | Shot |
| `carry` | Carry | (no equivalent) |
| `duel` | Duel | Duel (all sub-types) |
| `interception` | Interception | Others / Interception |
| `foul` | Foul Committed | Foul |
| `clearance` | Clearance | Free Kick (clearance sub-type) |
| `cross` | Pass (pass_cross=true) | Pass / Cross |
| `take_on` | Dribble | Others / Acceleration |
| `goalkeeper` | Goalkeeper | Goalkeeper leaving line |
| `free_kick` | Free Kick (set piece starts) | Free Kick |
| `corner` | Pass (from corner pattern) | Corner |
| `throw_in` | Pass (from throw-in pattern) | Others / Touch |
| `other` | All remaining | All remaining |

~14 action types x 96 grid cells = ~1,344 vocabulary.

---

## Training Pipeline

1. Read StatsBomb + Wyscout bronze events from Delta
2. Tokenize: event -> `"{action_type}_{grid_x}_{grid_y}"`
3. Group by (canonical_player_id, match_id) -> "document" (ordered token sequence)
4. Train gensim Doc2Vec (vector_size=32, window=5, min_count=2, epochs=20, dm=1)
5. Save model artifacts to UC Volume
6. Log to MLflow with custom pyfunc wrapper
7. Publish to HF Hub

Uses `canonical_player_id` from `dim_players` (Phase 14). Players in both sources get all match documents under one ID.

---

## Inference Pipeline

**Entry point:** `src/ingestion/player_embeddings.py` -> `compute_embeddings`

### Phase A: Behavioral embeddings

Load trained model, tokenize events, `infer_vector()` per (player, match). CPU-only, <30s for ~19K combinations.

### Phase B: Stat vector

13-dim z-score normalized vector from `fct_player_stats`:

1. `goals_per_90`
2. `xg_per_90`
3. `shots_per_90`
4. `passes_per_90`
5. `pass_completion_pct`
6. `progressive_passes_per_90`
7. `line_breaking_per_90`
8. `vaep_per_90`
9. `offensive_vaep_per_90`
10. `defensive_vaep_per_90`
11. `defcon_per_90` (nullable)
12. `intercept_per_90` (nullable)
13. `deter_per_90` (nullable)

DEFCON features (#11-13) are NULL for non-360 matches. Normalization params (mean, std) saved with model artifacts.

### Bronze output: `bronze.player_embeddings_raw`

| Column | Type |
|--------|------|
| `canonical_player_id` | STRING |
| `match_id` | STRING |
| `data_source` | STRING |
| `behavioral_vector` | ARRAY<DOUBLE> (32) |
| `stat_vector` | ARRAY<DOUBLE> (13, nullable) |
| `_ingested_at` | TIMESTAMP |

Idempotency: `replaceWhere` on `(data_source, match_id)`.

---

## dbt Models

### `stg_player_embeddings` (new, view)

Silver layer: dedup by (canonical_player_id, match_id), latest _ingested_at wins.

### `fct_player_embeddings` (redesigned, table)

Per-match grain. Surrogate key on (canonical_player_id, match_id). Joins to dim_players.

### `fct_player_embeddings_season` (new, table)

Per player-competition-season. Element-wise mean of behavioral vectors. Stat vector pass-through. Includes `matches_in_sample` count.

### `fct_player_embeddings_career` (new, table)

Per player. Element-wise mean across all matches. Includes `total_matches` count.

### Feature toggle

```yaml
vars:
  embeddings_enabled: false
```

---

## pgvector & Lakebase

### Type handling

Delta `ARRAY<DOUBLE>` syncs to PG `double precision[]`. Cast to pgvector type in queries: `::vector(32)`.

### Extension setup

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

### HNSW expression indexes (4 new, 31 total)

```sql
-- Career table
CREATE INDEX idx_embeddings_career_behavioral_hnsw
  ON fct_player_embeddings_career_synced
  USING hnsw ((behavioral_vector::vector(32)) vector_cosine_ops);

CREATE INDEX idx_embeddings_career_stat_hnsw
  ON fct_player_embeddings_career_synced
  USING hnsw ((stat_vector::vector(13)) vector_cosine_ops);

-- Season table
CREATE INDEX idx_embeddings_season_behavioral_hnsw
  ON fct_player_embeddings_season_synced
  USING hnsw ((behavioral_vector::vector(32)) vector_cosine_ops);

CREATE INDEX idx_embeddings_season_stat_hnsw
  ON fct_player_embeddings_season_synced
  USING hnsw ((stat_vector::vector(13)) vector_cosine_ops);
```

No `ON ONLY` — indexes cascade to child partitions.

---

## Streamlit: Player Similarity Page

**Controls:** Search by (Behavioral/Statistical), player dropdown, optional competition filter, min matches (default 5), result count.

**Query:** Career table by default, season table when competition filter applied. `::vector() <=> %s::vector()` cosine distance with parameterized values.

**Radar comparison:** Click result row to render mplsoccer radar (selected player vs match). Reuses existing `charts.py` radar component.

---

## Synced Tables

| Table | PK | Status |
|-------|------|--------|
| `fct_player_embeddings_synced` | `embedding_id` | Recreate (schema change) |
| `fct_player_embeddings_season_synced` | surrogate key | New (UI + import) |
| `fct_player_embeddings_career_synced` | `canonical_player_id` | New (UI + import) |

---

## Infrastructure

### Dependencies (pyproject.toml)

```toml
[project.optional-dependencies]
embeddings = ["gensim>=4.3.0", "huggingface_hub>=0.25.0"]

[project.scripts]
compute_embeddings = "ingestion.player_embeddings:main"
```

### Terraform

New `embeddings` environment with gensim + huggingface_hub. New `compute_embeddings` task depending on `resolve_players`.

### Tests (~40-50 new, ~350+ total)

- `test_football2vec.py`: Tokenizer, training, inference, pyfunc wrapper
- `test_player_embeddings.py`: Pipeline, stat normalization, Delta writes
- `test_player_similarity_page.py`: Page rendering, query construction

### Scripts

- `create_indexes.py`: pgvector extension + 4 HNSW indexes
- `lakebase_grants.sql`: 2 new synced tables
- `import_synced_tables.sh`: 2 new import commands

---

## Documentation

- `docs/huggingface-setup.md`: Fork-friendly HF setup guide
- `README.md`: HF in tech stack, Player Similarity in analytics list
- `PLAN.md`: Phase 15+16 completion
- `CLAUDE.md`: HF conventions

---

## Deployment Order

1. Write all code locally
2. Build wheel, deploy to Databricks
3. Run training pipeline (gensim Doc2Vec on SB+WY events)
4. Run inference pipeline (behavioral + stat vectors to bronze)
5. dbt build (staging -> per-match -> season -> career)
6. Recreate/create synced tables (1 recreate + 2 new)
7. Terraform import synced tables
8. Run create_indexes.py (pgvector extension + HNSW)
9. Run lakebase_grants.sql
10. Deploy Streamlit app, verify Player Similarity page
11. Verify end-to-end (search, radar comparison, both vector types)
12. Stage and commit only after full E2E validation

---
---

# Implementation Plan

**Constraint:** No git commits until full E2E validation on Databricks. All local quality checks (ruff, pyright, pytest) must pass before deploying to Databricks.

**Local verification command (run after each task):**
```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

---

## Task 1: Dependencies & Configuration

**Files:**
- Modify: `pyproject.toml`
- Modify: `dbt_project/dbt_project.yml`

**Step 1: Add embeddings optional dependency group to pyproject.toml**

Add after the existing `sdk` group (~line 48):
```toml
embeddings = [
    "gensim>=4.3.0",
    "huggingface_hub>=0.25.0",
]
```

**Step 2: Add entry point to pyproject.toml**

Add after `resolve_players` entry (~line 74):
```toml
compute_embeddings = "ingestion.player_embeddings:main"
```

**Step 3: Add embeddings_enabled feature toggle to dbt_project.yml**

Add after `entity_resolution_enabled` (~line 68):
```yaml
    embeddings_enabled: false
```

**Step 4: Install new dependencies locally**

Run: `uv sync --extra embeddings --extra analytics --extra app --extra dev`

**Verify:** `uv run python -c "import gensim; import huggingface_hub; print('OK')"`

---

## Task 2: Analytics Module — Tokenizer & Training

**Files:**
- Create: `src/analytics/football2vec.py`
- Create: `src/tests/test_football2vec.py`

This is the core analytics module. It contains:
1. `TokenizerConfig` dataclass — grid dimensions, event type mapping
2. `tokenize_event()` — single event → token string
3. `tokenize_match_events()` — DataFrame of events → list of token sequences grouped by player
4. `TrainingConfig` dataclass — Doc2Vec hyperparameters
5. `train_model()` — token sequences → trained Doc2Vec model
6. `infer_vectors()` — token sequences → embedding vectors
7. `Football2VecModel` — MLflow custom pyfunc wrapper

**Step 1: Write tests first**

Create `src/tests/test_football2vec.py` with tests covering:

- **TestTokenizerConfig**: Default grid (12x8), coordinate mapping (0→0, 119→11, 79→7), edge coordinates
- **TestTokenizeEvent**: StatsBomb pass → `"pass_6_4"`, StatsBomb shot → `"shot_11_4"`, Wyscout pass → `"pass_6_4"`, unknown type → `"other_*"`, null coordinates → skip, cross detection (pass_cross=True → `"cross_*"`)
- **TestTokenizeMatchEvents**: Groups by (player_id, match_id), orders by event index, returns dict of `{(player_id, match_id): [tokens]}`
- **TestTrainingConfig**: Default hyperparams (vector_size=32, window=5, min_count=2, epochs=20, dm=1)
- **TestTrainModel**: Trains on small corpus (3 players, 5 events each), returns Doc2Vec model, vocabulary populated, model.dv contains document vectors
- **TestInferVectors**: Infers vector for unseen token sequence, returns 32-dim array, vectors are reproducible for same input
- **TestFootball2VecModel**: MLflow pyfunc `load_context()` loads model files, `predict()` returns vectors for input DataFrame

Target: ~25 tests.

**Step 2: Implement `src/analytics/football2vec.py`**

Follow the pattern from `src/analytics/entity_resolution.py`:
- Frozen dataclasses for config
- Pure functions taking DataFrames, returning DataFrames/dicts
- No Spark dependencies (pure pandas/numpy/gensim)
- Type annotations on all public functions

Key implementation details:

**Tokenizer:**
- Grid mapping: `grid_x = min(int(x / (120 / grid_cols)), grid_cols - 1)`
- Grid mapping: `grid_y = min(int(y / (80 / grid_rows)), grid_rows - 1)`
- StatsBomb event type mapping dict with special cases:
  - Pass with `pass_cross=True` → `"cross"`
  - Pass with `play_pattern="From Corner"` → `"corner"`
  - Pass with `play_pattern="From Throw In"` → `"throw_in"`
  - Dribble → `"take_on"`
  - Foul Committed → `"foul"`
- Wyscout mapping dict (event_type + sub_event_type combinations)
- Skip events with NULL coordinates

**Training:**
- `gensim.models.doc2vec.Doc2Vec` with `TaggedDocument(words=tokens, tags=[f"{player_id}_{match_id}"])`
- Build vocabulary, then train
- Save model with `model.save(path)`

**Inference:**
- `model.infer_vector(tokens, epochs=20)` returns numpy array
- Convert to Python list for Delta storage

**MLflow pyfunc:**
- `load_context`: loads gensim model from `context.artifacts["model_dir"]`
- `predict`: takes DataFrame with `tokens` column (list of token lists), returns DataFrame with `vector` column

**Step 3: Run tests**

Run: `uv run pytest src/tests/test_football2vec.py -v`
Expected: All ~25 tests pass.

---

## Task 3: Ingestion Module — Embedding Pipeline

**Files:**
- Create: `src/ingestion/player_embeddings.py`
- Create: `src/tests/test_player_embeddings.py`

**Step 1: Write tests first**

Create `src/tests/test_player_embeddings.py` with tests covering:

- **TestStatVectorFeatures**: Correct 13 feature columns selected, z-score normalization (mean≈0, std≈1), NULL handling for DEFCON features
- **TestNormalizationParams**: Mean/std computation, serialization to JSON, reproducibility
- **TestBuildBronzeDataFrame**: Correct schema (canonical_player_id, match_id, data_source, behavioral_vector, stat_vector, _ingested_at), vector dimensions (32, 13)
- **TestStatVectorGrainJoin**: Stat vector correctly joined at player-competition-season grain, same stat vector for all matches in a competition-season
- **TestMainFunction**: CLI args parsed, mock Spark + Delta writes, replaceWhere pattern used

Target: ~15 tests.

**Step 2: Implement `src/ingestion/player_embeddings.py`**

Follow the pattern from `src/ingestion/entity_resolution.py`:
- `main()` entry point with `parse_ingestion_args()`
- `configure_logging("player_embeddings")`
- `get_spark_session()`

Key functions:
- `_load_events(spark, catalog, schema)` — reads StatsBomb + Wyscout bronze events, selects (match_id, player_id, event_type, x, y, index/ordering, data_source, play_pattern, pass_cross). Uses `canonical_player_id` from dim_players join.
- `_compute_behavioral_vectors(events_pdf, model_path)` — loads gensim model, tokenizes, infers per (player, match)
- `_compute_stat_vectors(spark, catalog, schema)` — reads fct_player_stats, selects 13 features, z-score normalizes, returns per player-competition-season
- `_merge_vectors(behavioral_df, stat_df)` — joins stat vector to each match row via player+competition+season
- `main()` — orchestrates: load events → compute behavioral → compute stat → merge → validate → write Delta

Delta write: `write_delta_table(df, catalog, schema, "player_embeddings_raw", mode="overwrite", replace_where=f"data_source = '{source}'", ...)` per source for idempotency.

Model path: `/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/` (UC Volume).

**Step 3: Run tests**

Run: `uv run pytest src/tests/test_player_embeddings.py -v`
Expected: All ~15 tests pass.

---

## Task 4: dbt Models — Staging & Marts

**Files:**
- Create: `dbt_project/models/staging/embeddings/_embeddings__sources.yml`
- Create: `dbt_project/models/staging/embeddings/stg_player_embeddings.sql`
- Modify: `dbt_project/models/marts/fct_player_embeddings.sql` (full redesign)
- Create: `dbt_project/models/marts/fct_player_embeddings_season.sql`
- Create: `dbt_project/models/marts/fct_player_embeddings_career.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (add new model docs + tests)

**Step 1: Create source definition**

Create `_embeddings__sources.yml` following the `_entity_resolution__sources.yml` pattern:
- Source name: `embeddings`
- Database: `{{ var('catalog', 'soccer_analytics') }}`
- Schema: `bronze`
- Table: `player_embeddings_raw`
- Columns: canonical_player_id, match_id, data_source, behavioral_vector, stat_vector, _ingested_at
- Freshness: warn_after 48 hours, error_after 168 hours

**Step 2: Create staging model**

Create `stg_player_embeddings.sql`:
- Materialized as view, enabled by `var('embeddings_enabled', false)`
- Dedup by (canonical_player_id, match_id) using ROW_NUMBER + _ingested_at DESC
- QUALIFY _row_num = 1
- Select: canonical_player_id, match_id, data_source, behavioral_vector, stat_vector

**Step 3: Redesign fct_player_embeddings**

Replace the entire current file. New model:
- Materialized as table, enabled by `var('embeddings_enabled', false)`
- Surrogate key: `dbt_utils.generate_surrogate_key(['canonical_player_id', 'match_id'])`
- Source: `ref('stg_player_embeddings')`
- Columns: embedding_id, canonical_player_id, match_id, data_source, behavioral_vector, stat_vector
- Header comment explaining the dual-vector design and football2vec methodology

**Step 4: Create fct_player_embeddings_season**

New model:
- Materialized as table, enabled by `var('embeddings_enabled', false)`
- Surrogate key: `dbt_utils.generate_surrogate_key(['canonical_player_id', 'competition_id', 'season_id'])`
- Join fct_player_embeddings to a match→competition+season mapping (from stg_statsbomb__matches or fct_match_summary)
- Element-wise mean of behavioral_vector using Databricks SQL `TRANSFORM` + `AGGREGATE`:
  ```sql
  TRANSFORM(
    SEQUENCE(0, 31),
    i -> (
      SELECT AVG(behavioral_vector[i])
      FROM fct_player_embeddings sub
      WHERE sub.canonical_player_id = main.canonical_player_id
        AND sub.competition_id = main.competition_id
        AND sub.season_id = main.season_id
    )
  ) as behavioral_vector
  ```
  (Exact SQL may need adjustment — Databricks SQL array aggregation syntax will be validated during dbt build.)
- `ANY_VALUE(stat_vector)` for stat vector (same within competition-season)
- `COUNT(*) as matches_in_sample`

**Step 5: Create fct_player_embeddings_career**

New model:
- Same pattern as season but grouped by canonical_player_id only
- Mean across all matches for both vectors
- `COUNT(*) as total_matches`
- `COLLECT_SET(data_source) as data_sources` (list of sources contributing)

**Step 6: Add model documentation and tests to _marts__models.yml**

Add entries for all three new models with:
- Column descriptions
- `unique` and `not_null` tests on primary keys
- `dbt_expectations.expect_column_values_to_not_be_null` on behavioral_vector
- `accepted_values` test on data_source ('statsbomb', 'wyscout')
- Relationship tests to dim_players (canonical_player_id)

---

## Task 5: Streamlit Page — Player Similarity

**Files:**
- Create: `src/streamlit_app/pages/player_similarity.py`
- Modify: `src/streamlit_app/app.py` (register new page)
- Modify: `src/tests/test_streamlit_components.py` (add page tests)

**Step 1: Create player_similarity.py**

Follow the `player_radar.py` pattern:
- `def page() -> None:` function
- Sidebar controls:
  - `st.radio("Search by", ["Behavioral (playing style)", "Statistical (output metrics)"])`
  - Competition filter (reuse `render_competition_filter()` from filters.py)
  - Player selectbox (reuse `render_player_filter()` with multiselect=False)
  - `st.slider("Min. matches", 1, 50, 5)`
  - `st.selectbox("Results", [5, 10, 20], index=1)`
- Main area:
  - Fetch target player's vector from career (or season if competition selected) synced table
  - Run pgvector cosine distance query
  - Display results in `st.dataframe()` with player name, distance, matches, data sources
  - On row selection: render radar comparison using existing `plot_player_radar()` from charts.py

**pgvector query pattern:**
```python
# Fetch target vector
target_query = (
    f"SELECT behavioral_vector, stat_vector "
    f"FROM {table} WHERE canonical_player_id = %s"
)
# Similarity search
search_query = (
    f"SELECT e.canonical_player_id, p.player_name, p.data_sources, "
    f"  e.total_matches, "
    f"  e.{vector_col}::vector <=> %s::vector AS distance "
    f"FROM {table} e "
    f"JOIN {t('dim_players_synced')} p "
    f"  ON e.canonical_player_id = p.canonical_player_id "
    f"WHERE e.total_matches >= %s "
    f"  AND e.canonical_player_id != %s "
    f"ORDER BY distance LIMIT %s"
)
```

Vector passed as pgvector literal: `"[0.1,0.2,...]"` string format.

**Step 2: Register page in app.py**

Add import and `st.Page` entry after defensive_valuation (~line 55):
```python
from pages import player_similarity
# ...
st.Page(player_similarity.page, title="Player Similarity", icon="🔍", url_path="player-similarity"),
```

**Step 3: Add tests**

Add to `test_streamlit_components.py` or create `test_player_similarity_page.py`:
- Test vector string formatting (list → pgvector literal)
- Test query construction with correct casts
- Test empty results handling
- Test competition filter switching between career/season tables

Target: ~10 tests.

---

## Task 6: Infrastructure — Terraform & Scripts

**Files:**
- Modify: `terraform/modules/workflows/main.tf`
- Modify: `terraform/modules/synced_tables/main.tf`
- Modify: `scripts/create_indexes.py`
- Modify: `scripts/lakebase_grants.sql`

**Step 1: Add Terraform workflow environment and task**

Add `embeddings` environment after `tracking` (~line 294):
```hcl
environment {
  environment_key = "embeddings"
  spec {
    client = "1"
    dependencies = concat(
      [var.wheel_path],
      [
        "gensim>=4.3.0",
        "huggingface_hub>=0.25.0",
      ]
    )
  }
}
```

Add `compute_embeddings` task after `resolve_players` (~line 247):
```hcl
task {
  task_key = "compute_embeddings"
  environment_key = "embeddings"

  python_wheel_task {
    package_name = "ingestion"
    entry_point  = "compute_embeddings"
    parameters = [
      "--catalog", var.catalog_name,
      "--schema", "bronze",
    ]
  }

  depends_on {
    task_key = "resolve_players"
  }

  timeout_seconds = 3600
  max_retries     = 1
}
```

**Step 2: Add synced table definitions**

Add after `fct_player_embeddings` resource (~line 117):
```hcl
resource "databricks_database_synced_database_table" "fct_player_embeddings_season" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_season_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"
  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_season"
    primary_key_columns    = ["embedding_season_id"]
    scheduling_policy      = "SNAPSHOT"
  }
  lifecycle { ignore_changes = all }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings_career" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_career_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"
  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_career"
    primary_key_columns    = ["canonical_player_id"]
    scheduling_policy      = "SNAPSHOT"
  }
  lifecycle { ignore_changes = all }
}
```

**Step 3: Add pgvector extension and HNSW indexes to create_indexes.py**

Add `CREATE EXTENSION IF NOT EXISTS vector` at the start of `_create_indexes()`.

Add 4 HNSW index definitions to the INDEXES list (~line 104):
```python
# pgvector HNSW indexes for embedding similarity search
("idx_embeddings_career_behavioral_hnsw",
 "fct_player_embeddings_career_synced",
 "USING hnsw ((behavioral_vector::vector(32)) vector_cosine_ops)"),
("idx_embeddings_career_stat_hnsw",
 "fct_player_embeddings_career_synced",
 "USING hnsw ((stat_vector::vector(13)) vector_cosine_ops)"),
("idx_embeddings_season_behavioral_hnsw",
 "fct_player_embeddings_season_synced",
 "USING hnsw ((behavioral_vector::vector(32)) vector_cosine_ops)"),
("idx_embeddings_season_stat_hnsw",
 "fct_player_embeddings_season_synced",
 "USING hnsw ((stat_vector::vector(13)) vector_cosine_ops)"),
```

Note: HNSW indexes use a different `CREATE INDEX` syntax than btree. The script's index creation loop will need a small refactor to handle the `USING hnsw ((...) ops)` syntax vs the current `(col1, col2)` btree syntax.

Add verification query for pgvector:
```python
("SELECT canonical_player_id FROM {schema}.fct_player_embeddings_career_synced "
 "ORDER BY behavioral_vector::vector(32) <=> '[0.1,0.1,...,0.1]'::vector(32) LIMIT 5",
 "fct_player_embeddings_career_synced"),
```

**Step 4: Update lakebase_grants.sql**

No changes needed — the existing `ALTER DEFAULT PRIVILEGES ... GRANT SELECT ON TABLES` (line 27-28) automatically covers new synced tables created by the sync process.

---

## Task 7: Documentation

**Files:**
- Create: `docs/huggingface-setup.md`
- Modify: `README.md`
- Modify: `PLAN.md`
- Modify: `CLAUDE.md`

**Step 1: Create HF setup guide**

Create `docs/huggingface-setup.md` with sections:
1. **Using pre-trained model** (for forks): `pip install huggingface_hub` → `snapshot_download("luxury-lakehouse/football2vec-statsbomb-wyscout")`
2. **Retraining on your data**: How to configure HF_HOME, run training pipeline, publish
3. **HF org creation**: Step-by-step account + org + write token
4. **Databricks integration**: UC Volume path, MLflow registry, workflow env

**Step 2: Update README.md**

- Add `huggingface_hub` to Tech Stack table
- Add "Player Similarity" to Analytics list (remove "planned" tag)
- Update Status section: "Phase 16 complete — 11 Streamlit pages, 16 synced tables"
- Add Phase 15 and 16 rows to status table

**Step 3: Update PLAN.md**

- Move Phase 15 and 16 from §8 (Future Work) to §7 (Completed Phases)
- Update status line at top
- Add synced table entries
- Add Streamlit page entry

**Step 4: Update CLAUDE.md**

Add HF conventions to project conventions section:
- HF org: `luxury-lakehouse`
- Model artifacts: UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/`
- `HF_HOME` env var for cache location
- `huggingface_hub` for model publish/download (no torch)

---

## Task 8: Local Quality Gate

**Before deploying to Databricks, ALL checks must pass locally.**

**Step 1: Lint**
```bash
uv run ruff check src/
uv run ruff format --check src/
```
Expected: Zero violations.

**Step 2: Type check**
```bash
uv run pyright src/
```
Expected: Zero errors (basic mode). Note: gensim may need type stubs or `# type: ignore` for untyped imports.

**Step 3: Unit tests**
```bash
uv run pytest src/tests/ -v
```
Expected: All tests pass (~350+).

**Step 4: Wheel build**
```bash
uv build
```
Expected: Produces installable wheel in `dist/`.

---

## Task 9: Manual — HuggingFace Org & Repo Creation

**This is a manual step. Claude will guide you through it.**

**Step 1: Create HuggingFace account** (if needed)
- Go to https://huggingface.co/join
- Create account

**Step 2: Create organization**
- Go to https://huggingface.co/organizations/new
- Name: `luxury-lakehouse`
- Type: Community (free)

**Step 3: Create model repository**
- Go to https://huggingface.co/new (under `luxury-lakehouse` org)
- Repo name: `football2vec-statsbomb-wyscout`
- License: MIT (matching football2vec's license)
- Visibility: Public

**Step 4: Generate write token**
- Go to https://huggingface.co/settings/tokens
- Create token: Name "luxury-lakehouse-write", Type "Write"
- Save token securely — needed as `HF_TOKEN` env var

---

## Task 10: Databricks Deployment — Training Pipeline

**Step 1: Upload wheel to Databricks**
```bash
databricks sync . /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse --profile OAUTH
```

**Step 2: Create UC Volume for model weights** (if not exists)
```sql
CREATE VOLUME IF NOT EXISTS soccer_analytics.dev_gold.model_weights;
```

**Step 3: Run training pipeline**

Run from a Databricks notebook or job:
```python
from analytics.football2vec import TokenizerConfig, TrainingConfig, train_model, tokenize_match_events

# Load events from bronze
sb_events = spark.sql("SELECT * FROM soccer_analytics.bronze.statsbomb_events").toPandas()
wy_events = spark.sql("SELECT * FROM soccer_analytics.bronze.wyscout_events").toPandas()

# Join to dim_players for canonical_player_id
dim_players = spark.sql("SELECT player_id, canonical_player_id FROM soccer_analytics.dev_gold.dim_players").toPandas()

# Tokenize
config = TokenizerConfig()
sb_docs = tokenize_match_events(sb_events, dim_players, config, source="statsbomb")
wy_docs = tokenize_match_events(wy_events, dim_players, config, source="wyscout")
all_docs = {**sb_docs, **wy_docs}

# Train
training_config = TrainingConfig()
model = train_model(all_docs, training_config)

# Save to UC Volume
model.save("/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/player2vec.model")
```

**Step 4: Verify training output**
- Check model file exists in UC Volume
- Verify vocabulary size (~1,344 tokens)
- Verify document count matches expected player-match combinations
- Test inference on a known player

**Step 5: Log to MLflow and publish to HF Hub**

```python
import mlflow
from huggingface_hub import HfApi

# Log to MLflow
mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run(run_name="football2vec-v1"):
    mlflow.pyfunc.log_model(
        artifact_path="football2vec",
        python_model=Football2VecModel(),
        artifacts={"model_dir": "/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/"},
        registered_model_name="soccer_analytics.dev_gold.football2vec",
    )

# Publish to HF Hub
api = HfApi()
api.upload_folder(
    folder_path="/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/",
    repo_id="luxury-lakehouse/football2vec-statsbomb-wyscout",
    token=os.environ["HF_TOKEN"],
)
```

---

## Task 11: Databricks Deployment — Inference Pipeline

**Step 1: Run the compute_embeddings workflow task**

Either trigger via Databricks UI (run the ingestion job with only `compute_embeddings` task) or run from a notebook:
```python
from ingestion.player_embeddings import main
import sys
sys.argv = ["compute_embeddings", "--catalog", "soccer_analytics", "--schema", "bronze"]
main()
```

**Step 2: Verify bronze output**
```sql
SELECT COUNT(*) FROM soccer_analytics.bronze.player_embeddings_raw;
-- Expected: ~19K rows

SELECT data_source, COUNT(*) FROM soccer_analytics.bronze.player_embeddings_raw GROUP BY data_source;
-- Expected: statsbomb ~X, wyscout ~Y

SELECT SIZE(behavioral_vector), SIZE(stat_vector)
FROM soccer_analytics.bronze.player_embeddings_raw LIMIT 5;
-- Expected: 32, 13
```

---

## Task 12: Databricks Deployment — dbt Build

**Step 1: Enable the embeddings toggle**

In `dbt_project.yml`, set `embeddings_enabled: true` temporarily for the build (or pass via CLI):
```bash
MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--select', 'tag:embeddings', '--vars', '{embeddings_enabled: true}'])"
```

Or build all models:
```bash
MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--vars', '{embeddings_enabled: true, entity_resolution_enabled: true, defcon_enabled: true, off_ball_xt_enabled: true}'])"
```

**Step 2: Verify gold tables**
```sql
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings;
-- Expected: ~19K

SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings_season;
-- Expected: ~12K

SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings_career;
-- Expected: ~11,918 (one per dim_players row with events)

-- Check vector dimensions
SELECT SIZE(behavioral_vector), SIZE(stat_vector)
FROM soccer_analytics.dev_gold.fct_player_embeddings_career LIMIT 5;
-- Expected: 32, 13
```

**Step 3: Verify dbt tests pass**
```bash
MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['test', '--select', 'tag:embeddings', '--vars', '{embeddings_enabled: true}'])"
```

---

## Task 13: Databricks Deployment — Synced Tables & pgvector

**Step 1: Delete old fct_player_embeddings_synced**

From `terraform/environments/dev/`:
```bash
AWS_PROFILE=devops-agent terraform destroy \
  -target='module.synced_tables.databricks_database_synced_database_table.fct_player_embeddings' \
  -auto-approve
```

**Step 2: Drop PG ghost table**

Via psycopg2 with OAuth credential:
```sql
DROP TABLE IF EXISTS dev_gold.fct_player_embeddings_synced CASCADE;
```

**Step 3: Recreate fct_player_embeddings_synced via Databricks UI**

Catalog → soccer_analytics → dev_gold → fct_player_embeddings → Create synced table:
- Project: soccer-analytics-dev
- Branch: production
- Logical DB: databricks_postgres
- Scheduling: SNAPSHOT

**Step 4: Create 2 new synced tables via Databricks UI**

Same procedure for:
- `fct_player_embeddings_season` → `fct_player_embeddings_season_synced` (PK: embedding_season_id)
- `fct_player_embeddings_career` → `fct_player_embeddings_career_synced` (PK: canonical_player_id)

**Step 5: Terraform import**

```bash
AWS_PROFILE=devops-agent terraform import \
  'module.synced_tables.databricks_database_synced_database_table.fct_player_embeddings' \
  'soccer_analytics.dev_gold.fct_player_embeddings_synced'

AWS_PROFILE=devops-agent terraform import \
  'module.synced_tables.databricks_database_synced_database_table.fct_player_embeddings_season' \
  'soccer_analytics.dev_gold.fct_player_embeddings_season_synced'

AWS_PROFILE=devops-agent terraform import \
  'module.synced_tables.databricks_database_synced_database_table.fct_player_embeddings_career' \
  'soccer_analytics.dev_gold.fct_player_embeddings_career_synced'
```

**Step 6: Trigger SNAPSHOT refresh**
```bash
python scripts/refresh_synced_tables.py --tables fct_player_embeddings_synced,fct_player_embeddings_season_synced,fct_player_embeddings_career_synced --wait
```

**Step 7: Create pgvector extension and HNSW indexes**
```bash
.venv/Scripts/python.exe scripts/create_indexes.py
```

**Step 8: Verify indexes**
```bash
.venv/Scripts/python.exe scripts/create_indexes.py --verify
```

Expected: All 31 indexes verified, including 4 new HNSW indexes. EXPLAIN ANALYZE shows Index Scan for pgvector queries.

---

## Task 14: Databricks Deployment — Streamlit App

**Step 1: Deploy app**
```bash
databricks sync . /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse --profile OAUTH
databricks apps deploy soccer-analytics-dashboard-dev \
  --source-code-path /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse \
  --profile OAUTH
```

**Step 2: Verify Player Similarity page**

Open app URL and navigate to Player Similarity:
1. Select a well-known player (e.g., Messi, Neymar)
2. Verify behavioral similarity results make sense (stylistically similar players ranked high)
3. Switch to statistical search — verify output-similar players differ from behavioral
4. Apply competition filter — verify it switches to season table
5. Adjust min matches slider — verify result count changes
6. Click a result — verify radar comparison renders
7. Test with both StatsBomb-only and cross-source players

**Step 3: Smoke test all other pages**

Verify existing 10 pages still work (no regressions from schema changes).

---

## Task 15: Git Commit

**Only after ALL of the above is verified on Databricks.**

**Step 1: Final local quality gate**
```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

**Step 2: Update auto-memory**

Update `MEMORY.md` with Phase 15+16 completion details.

**Step 3: Stage and commit**

Stage specific files (no secrets, no .env):
```bash
git add src/analytics/football2vec.py \
        src/ingestion/player_embeddings.py \
        src/tests/test_football2vec.py \
        src/tests/test_player_embeddings.py \
        src/streamlit_app/pages/player_similarity.py \
        src/streamlit_app/app.py \
        dbt_project/models/staging/embeddings/ \
        dbt_project/models/marts/fct_player_embeddings.sql \
        dbt_project/models/marts/fct_player_embeddings_season.sql \
        dbt_project/models/marts/fct_player_embeddings_career.sql \
        dbt_project/models/marts/_marts__models.yml \
        dbt_project/dbt_project.yml \
        terraform/modules/workflows/main.tf \
        terraform/modules/synced_tables/main.tf \
        scripts/create_indexes.py \
        pyproject.toml uv.lock \
        docs/plans/2026-03-08-player-embeddings-hf.md \
        docs/huggingface-setup.md \
        README.md PLAN.md CLAUDE.md TODO.md ROADMAP.md
```

Commit message:
```
feat: pgvector player embeddings with HuggingFace Hub (Phase 15+16)
```

---

## Dependency Graph

```
Task 1 (deps/config)
  ├── Task 2 (analytics module + tests)
  │     └── Task 3 (ingestion module + tests)
  │           └── Task 4 (dbt models)
  │                 └── Task 5 (Streamlit page + tests)
  ├── Task 6 (infrastructure — can parallel with 2-5)
  └── Task 7 (documentation — can parallel with 2-6)

Task 8 (local quality gate) — after all of 1-7

Task 9 (manual HF setup) — anytime before Task 10

Task 10 (training on Databricks) — after 8, 9
  └── Task 11 (inference on Databricks)
        └── Task 12 (dbt build)
              └── Task 13 (synced tables + pgvector)
                    └── Task 14 (Streamlit deploy + verify)
                          └── Task 15 (git commit)
```
